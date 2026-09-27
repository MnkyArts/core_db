// core_db/tests/pool.test.mjs — type parsers (§56.2.4), error strings, health and the slow-query log (§56.2.5).
import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { setup, eventsNamed, sleep } from './helpers.mjs';

const require = createRequire(import.meta.url);
const { parseTimestamp, parseInt8, describe, isConnectionError } = require('../src/pool.js');

test('parsers: int8, timestamps (unit)', () => {
    assert.equal(parseInt8('42'), 42);
    assert.equal(parseInt8('-9007199254740992'), -9007199254740992);
    assert.equal(parseInt8('9007199254740992'), 9007199254740992);
    assert.equal(parseInt8('9007199254740993'), '9007199254740993');
    assert.equal(parseTimestamp('2026-09-27 12:00:00+00'), Date.UTC(2026, 8, 27, 12) / 1000);
    assert.equal(parseTimestamp('2026-09-27 12:00:00.999+02'), Date.UTC(2026, 8, 27, 10) / 1000);
    assert.equal(parseTimestamp('2026-09-27 12:00:00-05:30'), Date.UTC(2026, 8, 27, 17, 30) / 1000);
    assert.equal(parseTimestamp('1969-12-31 23:59:59.5+00'), -1);
    const bc = new Date(Date.UTC(2000, 0, 1));
    bc.setUTCFullYear(0);
    assert.equal(parseTimestamp('0001-01-01 00:00:00+00 BC'), bc.getTime() / 1000);
    assert.equal(parseTimestamp('infinity'), 'infinity');
});

test('errors: describe and classification (unit)', () => {
    assert.equal(describe(Object.assign(new Error('duplicate key value'), { code: '23505' })), '23505 duplicate key value');
    assert.equal(describe(Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:1'), { code: 'ECONNREFUSED' })), 'unavailable');
    assert.equal(describe(new Error('Connection terminated unexpectedly')), 'unavailable');
    assert.equal(describe('timeout'), 'timeout');
    assert.equal(isConnectionError(Object.assign(new Error('x'), { code: '08006' })), true);
    assert.equal(isConnectionError(Object.assign(new Error('x'), { code: '57P01' })), true);
    assert.equal(isConnectionError(Object.assign(new Error('x'), { code: '53300' })), true);
    assert.equal(isConnectionError(Object.assign(new Error('x'), { code: '23505' })), false);
    assert.equal(isConnectionError(Object.assign(new Error('EPIPE'), { code: 'EPIPE' })), true);
    assert.equal(describe(Object.assign(new Error('write EPIPE'), { code: 'EPIPE' })), 'unavailable');
});

test('pool: values come back as §56.2.4 says', async (t) => {
    const ctx = await setup('t_pool');
    t.after(ctx.teardown);

    const [err, rows, count] = await ctx.call('query', `SELECT
        1::int8 AS small, 9007199254740993::int8 AS big, 12.5::numeric AS num, '2026-09-27 12:00:00.75+00'::timestamptz AS tz,
        '2026-09-27 12:00:00'::timestamp AS ts, '2026-09-27'::date AS d, '{"a":[1,{"b":null}]}'::jsonb AS j,
        '[1,2]'::json AS js, true AS yes, NULL::text AS nothing, ARRAY[1,2]::int8[] AS i8s,
        ARRAY['2026-09-27 00:00:00+00'::timestamptz, NULL] AS tzs, ARRAY['2026-01-02'::date] AS ds,
        ARRAY['a','b'] AS texts, 3::int4 AS i4`, [], null);
    assert.equal(err, null);
    assert.equal(count, 1);
    const r = rows[0];
    assert.equal(r.small, 1);
    assert.equal(r.big, '9007199254740993');
    assert.equal(r.num, 12.5);
    assert.equal(r.tz, Date.UTC(2026, 8, 27, 12) / 1000);
    assert.equal(r.ts, Date.UTC(2026, 8, 27, 12) / 1000);
    assert.equal(r.d, '2026-09-27');
    assert.deepEqual(r.j, { a: [1, { b: null }] });
    assert.deepEqual(r.js, [1, 2]);
    assert.equal(r.yes, true);
    assert.equal(r.nothing, null);
    assert.deepEqual(r.i8s, [1, 2]);
    assert.deepEqual(r.tzs, [Date.UTC(2026, 8, 27) / 1000, null]);
    assert.deepEqual(r.ds, ['2026-01-02']);
    assert.deepEqual(r.texts, ['a', 'b']);
    assert.equal(r.i4, 3);
});

test('pool: raw params — NULL markers, arrays, objects', async (t) => {
    const ctx = await setup('t_pool2');
    t.after(ctx.teardown);
    const [err, rows] = await ctx.call('query',
        'SELECT $1::text IS NULL AS isnull, $2::text[] AS arr, $3::jsonb AS obj, $4::int AS n, 5 = ANY($5) AS hit',
        [{ __null: true }, ['a', { __null: true }], { k: { __null: true }, v: 1 }, 7, [4, 5]], null);
    assert.equal(err, null);
    assert.deepEqual(rows[0], { isnull: true, arr: ['a', null], obj: { k: null, v: 1 }, n: 7, hit: true });

    // A Lua list with holes the lib did not pad arrives as a map with integer keys.
    const [err2, rows2] = await ctx.call('query', 'SELECT $1::int AS a, $2::int IS NULL AS b, $3::int AS c', { 1: 1, 3: 3 }, null);
    assert.equal(err2, null);
    assert.deepEqual(rows2[0], { a: 1, b: true, c: 3 });

    const [err3] = await ctx.call('query', 'SELECT 1; SELECT 2', [], null);
    assert.match(err3, /^42601 /, 'one statement per query (extended protocol)');
    const [err4] = await ctx.call('query', 'SELECT * FROM no_such_table', [], null);
    assert.match(err4, /^42P01 relation "no_such_table" does not exist/);
    const [err5] = await ctx.call('query', '', [], null);
    assert.equal(err5, 'invalid: sql must be a non-empty string');
});

test('pool: health flips on connection-class failures and back, with dbStatus events', async (t) => {
    const ctx = await setup('t_pool3');
    t.after(ctx.teardown);
    assert.equal(ctx.api.isHealthy(), true);
    ctx.api.testFail('SELECT 4242', 'ECONNREFUSED');
    const [err] = await ctx.call('query', 'SELECT 4242', [], null);
    assert.equal(err, 'unavailable');
    assert.equal(ctx.api.isHealthy(), false);
    const [err2] = await ctx.call('query', 'SELECT 4242', [], null);
    assert.equal(err2, 'unavailable');
    const [, status] = await ctx.call('status');
    assert.equal(status.healthy, false);
    ctx.api.testFail(null);
    const [err3] = await ctx.call('query', 'SELECT 4242', [], null);
    assert.equal(err3, null);
    assert.equal(ctx.api.isHealthy(), true);
    await sleep(5);
    const flips = eventsNamed(ctx.fivem, 'core:hook:dbStatus');
    assert.equal(flips.length, 2, 'one event per flip');
    assert.equal(flips[0][0], false);
    assert.match(flips[0][1], /ECONNREFUSED/);
    assert.deepEqual(flips[1], [true, 'ok']);
    assert.equal(ctx.fivem.logs.filter(([l, m]) => l === 'error' && m.startsWith('database unavailable')).length, 1, 'logged once');

    // A statement error is not a connection failure.
    ctx.api.testFail('SELECT 4343', '23505 duplicate key value');
    const [err4] = await ctx.call('query', 'SELECT 4343', [], null);
    assert.equal(err4, '23505 duplicate key value');
    assert.equal(ctx.api.isHealthy(), true);
});

test('pool: a database that refuses connections answers unavailable', async () => {
    const { createCoreDb, INTERNAL } = require('../src/core.js');
    const { fakeFivem } = await import('./helpers.mjs');
    const fivem = fakeFivem();
    const api = createCoreDb(fivem, { url: 'postgres://core_test:core_test@127.0.0.1:1/core_test', flushMs: 0, testMode: true });
    const answer = await new Promise((resolve) => api.query('SELECT 1', [], null, (...a) => resolve(a)));
    assert.equal(answer[0], 'unavailable');
    assert.equal(api.isHealthy(), false);
    const [, status] = await new Promise((resolve) => api.status((...a) => resolve(a)));
    assert.equal(status.healthy, false);
    assert.ok(!fivem.logs.some(([, m]) => m.includes('core_test:core_test')), 'the URL never reaches a log');
    await api[INTERNAL].close();
});

test('pool: slow queries log owner and SQL, never parameters; per-call timeoutMs', async (t) => {
    const ctx = await setup('t_pool4', { slowMs: 50 });
    t.after(ctx.teardown);
    const [err] = await ctx.callAs('some_plugin', 'query', 'SELECT pg_sleep(0.08), $1::text AS secret', ['hunter2'], null);
    assert.equal(err, null);
    const slow = ctx.fivem.logs.filter(([l, m]) => l === 'warn' && m.startsWith('slow query'));
    assert.equal(slow.length, 1);
    assert.match(slow[0][1], /^slow query \d+ ms \(some_plugin\): SELECT pg_sleep/);
    assert.ok(!ctx.fivem.logs.some(([, m]) => m.includes('hunter2')), 'parameters are never logged');

    // statement_timeout = min(configured, what is left of the caller's timeoutMs − 250 ms slack)
    const started = Date.now();
    const [err2] = await ctx.call('query', 'SELECT pg_sleep(2)', [], { timeoutMs: 600 });
    assert.match(err2, /^57014 /, 'statement timeout');
    assert.ok(Date.now() - started < 600, 'cancelled before the caller gives up');
    assert.deepEqual(await ctx.call('query', 'SELECT 1', [], { timeoutMs: 100 }), ['timeout'], 'no statement starts without time left');
    const [err3, rows] = await ctx.call('query', 'SHOW statement_timeout', [], null);
    assert.equal(err3, null);
    assert.equal(rows[0].statement_timeout, '10s', 'the per-call timeout does not leak to the pooled client');
});
