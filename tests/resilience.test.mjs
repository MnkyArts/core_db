// core_db/tests/resilience.test.mjs — review R1 C1/H3 (error classes), L7 (redaction), L1 (a checked-out client
// losing its backend), M1 (half-open links: query_timeout, flush watchdog), L2 (session SETs never leak), L6
// (scalar answers).
import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { createRequire } from 'node:module';
import { setup, fakeFivem, sleep, eventsNamed, pg, BASE_URL, createCoreDb, INTERNAL } from './helpers.mjs';

const require = createRequire(import.meta.url);
const { classify, redact, describe } = require('../src/errors.js');

const err = (code, message = 'x') => Object.assign(new Error(message), { code });

test('error classes: connection by driver signals and 08000/1/3/4/6, 57P01–03; 08P01 is a statement error', () => {
    for (const code of ['08000', '08001', '08003', '08004', '08006', '57P01', '57P02', '57P03', '53300', 'ECONNRESET', 'EPIPE']) {
        assert.equal(classify(err(code)), 'connection', code);
    }
    for (const code of ['57014', '40P01', '40001', '55P03', '53100', '53200', '25006']) assert.equal(classify(err(code)), 'retryable', code);
    for (const code of ['08P01', '08007', '23505', '42P01', '22P02']) assert.equal(classify(err(code)), 'statement', code);
    assert.equal(classify(new Error('Query read timeout')), 'connection');
    assert.equal(classify(new Error('Connection terminated unexpectedly')), 'connection');
    assert.equal(classify(new Error('Client has encountered a connection error and is not queryable')), 'connection');
    assert.equal(classify(new Error('circular reference detected')), 'statement', 'a JS bug is not an outage');
    assert.equal(classify(Object.assign(new Error('no free connection'), { coreDb: 'timeout' })), 'saturated');
    assert.equal(describe(err('08P01', 'bind message supplies 1 parameters, but prepared statement "" requires 2')),
        '08P01 bind message supplies 1 parameters, but prepared statement "" requires 2');
});

test('L7: values in Postgres messages are redacted before logs, events and status (the caller keeps its answer)', async (t) => {
    assert.equal(redact('22P02 invalid input syntax for type integer: "4111 1111 1111 1111"'), '22P02 invalid input syntax for type integer: "…"');
    assert.equal(redact('22003 value "99999999999" is out of range for type integer'), '22003 value "…" is out of range for type integer');
    assert.equal(redact('22007 invalid input syntax for type timestamp with time zone: "yesterday-ish"'), '22007 invalid input syntax for type timestamp with time zone: "…"');
    assert.equal(redact('23505 duplicate key value violates unique constraint "vehicles_plate_key"'), '23505 duplicate key value violates unique constraint "vehicles_plate_key"');
    assert.equal(redact('42P01 relation "nope" does not exist'), '42P01 relation "nope" does not exist');

    const ctx = await setup('t_resil1');
    t.after(ctx.teardown);
    await ctx.call('query', 'CREATE TABLE r_items (id text PRIMARY KEY, n int)', [], null);
    ctx.syncAs('core', 'enqueue', [{ t: 'sql', sql: "INSERT INTO r_items VALUES ('a', 'secret-value'::int)" }]);
    await ctx.flush();
    await sleep(5);
    const [event] = eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed');
    assert.equal(event[3], '22P02 invalid input syntax for type integer: "…"');
    assert.ok(!ctx.fivem.logs.some(([, m]) => m.includes('secret-value')), 'no log carries the value');
    const [, status] = await ctx.call('status');
    assert.ok(!status.queue.lastError.includes('secret-value'));
    const [answer] = await ctx.call('query', "SELECT 'mine'::int", [], null);
    assert.match(answer, /"mine"/, "the caller's own answer is complete");
});

test('L1: a checked-out client whose backend dies is contained (no crash), the tx fails, the client is destroyed', async (t) => {
    const ctx = await setup('t_resil2');
    t.after(ctx.teardown);
    const [, txId] = await ctx.call('txBegin', {});
    const [, rows] = await ctx.call('txQuery', txId, 'SELECT pg_backend_pid() AS pid', []);
    await ctx.sql('SELECT pg_terminate_backend($1)', [rows[0].pid]);
    await sleep(300);
    const [e1] = await ctx.call('txQuery', txId, 'SELECT 1', []);
    assert.ok(e1 === 'unavailable' || e1 === 'tx_aborted', `answered ${e1}`);
    const [e2] = await ctx.call('txEnd', txId, true);
    assert.ok(e2 === 'tx_aborted' || e2 === 'tx_unknown', `answered ${e2}`);
    await sleep(50);
    const [, status] = await ctx.call('status');
    assert.equal(status.pool.total - status.pool.idle, 0, 'nothing checked out');
    assert.deepEqual(await ctx.call('query', 'SELECT 1 AS one', [], null), [null, [{ one: 1 }], 1], 'the pool recovered');
});

test('L2 + L6: a raw session SET is reset before the client is reused; scalar answers by field order', async (t) => {
    const ctx = await setup('t_resil3', { poolSize: 1 });
    t.after(ctx.teardown);
    const [, before] = await ctx.call('query', 'SHOW search_path', [], null);
    assert.deepEqual(await ctx.call('query', 'SET search_path TO pg_catalog', [], null), [null, [], 0]);
    const [, after] = await ctx.call('query', 'SHOW search_path', [], null);
    assert.deepEqual(after, before, 'RESET ALL ran before the (only) client was reused');

    assert.deepEqual(await ctx.call('query', 'SELECT 2 AS b, 1 AS a', [], { scalar: true }), [null, 2, 1]);
    assert.deepEqual(await ctx.call('query', 'SELECT 1 AS a WHERE false', [], { scalar: true }), [null, undefined, 0]);
    const [, txId] = await ctx.call('txBegin', {});
    assert.deepEqual(await ctx.call('txQuery', txId, "SELECT 'x' AS z, 5 AS y", [], { scalar: true }), [null, 'x', 1]);
    await ctx.call('txEnd', txId, false);
});

/** A TCP proxy in front of Postgres that can go silent (a half-open link: no RST, no FIN). */
async function freezableProxy() {
    const proxy = { frozen: false, sockets: [] };
    proxy.server = net.createServer((client) => {
        const upstream = net.connect(5432, '127.0.0.1');
        proxy.sockets.push(client, upstream);
        client.on('data', (d) => { if (!proxy.frozen) upstream.write(d); });
        upstream.on('data', (d) => { if (!proxy.frozen) client.write(d); });
        client.on('error', () => {});
        upstream.on('error', () => {});
    });
    await new Promise((resolve) => proxy.server.listen(0, '127.0.0.1', resolve));
    proxy.port = proxy.server.address().port;
    proxy.close = () => {
        for (const s of proxy.sockets) s.destroy();
        proxy.server.close();
    };
    return proxy;
}

async function viaProxy(t, schema, core) {
    const proxy = await freezableProxy();
    const admin = new pg.Client(BASE_URL);
    await admin.connect();
    await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE; CREATE SCHEMA ${schema}; CREATE TABLE ${schema}.items (id text PRIMARY KEY, n int)`);
    const url = new URL(BASE_URL);
    url.port = `${proxy.port}`;
    url.searchParams.set('options', `-c search_path=${schema}`);
    const fivem = fakeFivem({ invoker: 'core' });
    const api = createCoreDb(fivem, { url: url.toString(), flushMs: 0, testMode: true, ...core });
    await api[INTERNAL].catalog.reload();
    t.after(async () => {
        await api[INTERNAL].close();
        proxy.close();
        await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
        await admin.end();
    });
    const call = (name, ...args) => new Promise((resolve) => api[name](...args, (...a) => resolve(a)));
    return { proxy, api, fivem, admin, call, schema };
}

test('M1: a statement on a silently dead link fails after statement timeout + margin; the client is dropped', async (t) => {
    const x = await viaProxy(t, 't_resil4', { statementTimeoutMs: 400, queryTimeoutMarginMs: 300 });
    assert.deepEqual(await x.call('query', 'SELECT 1 AS one', [], null), [null, [{ one: 1 }], 1]);
    x.proxy.frozen = true;
    const started = Date.now();
    const [e] = await x.call('query', 'SELECT 1', [], null);
    assert.equal(e, 'unavailable');
    assert.ok(Date.now() - started < 2000, `answered after ${Date.now() - started} ms`);
    assert.equal(x.api.isHealthy(), false);
    x.proxy.frozen = false;
    assert.deepEqual(await x.call('query', 'SELECT 2 AS two', [], null), [null, [{ two: 2 }], 1], 'a fresh connection works');
    assert.equal(x.api.isHealthy(), true);
});

test('M1: the flush watchdog drops a stuck connection and the batch is requeued', async (t) => {
    const x = await viaProxy(t, 't_resil5', { statementTimeoutMs: 20000, watchdogMs: 300 });
    x.api.enqueue([{ t: 'save', table: 'items', row: { id: 'warm', n: 1 } }]);
    await x.api[INTERNAL].queue.flushNow(); // the system pool now holds an idle client through the proxy
    x.proxy.frozen = true;
    x.api.enqueue([{ t: 'save', table: 'items', row: { id: 'a', n: 1 } }]);
    const started = Date.now();
    await x.api[INTERNAL].queue.flushNow();
    assert.ok(Date.now() - started < 3000, `the flush gave up after ${Date.now() - started} ms`);
    const [, st] = await x.call('status');
    assert.equal(st.queue.pending, 1, 'requeued, not dropped');
    assert.equal(st.queue.dropped, 0);
    assert.ok(x.fivem.logs.some(([l, m]) => l === 'warn' && m.startsWith('flush watchdog')));
    x.proxy.frozen = false;
    await x.api[INTERNAL].queue.flushNow();
    assert.deepEqual((await x.admin.query(`SELECT id FROM ${x.schema}.items ORDER BY id`)).rows.map((r) => r.id), ['a', 'warm']);
});
