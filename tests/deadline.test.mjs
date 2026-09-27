// core_db/tests/deadline.test.mjs — review R1 M4 + R3a (a call never starts after its caller's deadline; every
// statement's statement_timeout ends before it), M6 (transactions per owner), L13 (txEnd checks ownership first),
// L3 (a fake onResourceStop is ignored), L4 (minimum flush interval outside test mode).
import test from 'node:test';
import assert from 'node:assert/strict';
import { setup, queueTables, sleep } from './helpers.mjs';

test('M4: a call whose deadline passes behind the migration barrier never runs', async (t) => {
    const ctx = await queueTables(t, 't_deadline1');
    ctx.syncAs('slowplug', 'migrate', [{ version: 1, sql: 'SELECT pg_sleep(0.8)' }]);
    const started = Date.now();
    const answer = await ctx.callAs('slowplug', 'crud', 'insert', 'q_items', { values: { id: 'late_insert', n: 1 }, timeoutMs: 300 }, 0);
    assert.deepEqual(answer, ['timeout']);
    assert.ok(Date.now() - started < 700, 'answered at the deadline, not after the barrier');
    assert.deepEqual(await ctx.callAs('slowplug', 'awaitMigrations', 5000), [null]);
    await sleep(100);
    assert.deepEqual(await ctx.items(), [], 'the insert never ran after the caller gave up');

    // the same for a read-your-writes wait that outlives the deadline
    ctx.api.testFail('^BEGIN$', 'ECONNRESET');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'queued', n: 1 } }]);
    const [e2] = await ctx.call('query', "INSERT INTO q_items (id) VALUES ('raw')", [], { sync: true, timeoutMs: 300 });
    assert.equal(e2, 'timeout');
    ctx.api.testFail(null);
    await ctx.flush();
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['queued']);
});

test('R3a: statement budgets — nextId/txQuery take opts.timeoutMs; tx statements end before the tx deadline', async (t) => {
    const ctx = await setup('t_deadline2');
    t.after(ctx.teardown);
    await ctx.call('batch', [
        { sql: 'CREATE TABLE core_counters (name text PRIMARY KEY, value bigint NOT NULL DEFAULT 0)' },
        { sql: 'CREATE TABLE d_items (id text PRIMARY KEY)' },
    ], null);
    assert.deepEqual(await ctx.call('nextId', 'n', { timeoutMs: 100 }), ['timeout'], 'less than the slack left: refused');
    assert.deepEqual(await ctx.call('nextId', 'n'), [null, 1], 'the old (name, cb) form still works; nothing was counted before');

    // the tx deadline bounds its statements
    const [, txId] = await ctx.call('txBegin', { timeoutMs: 700 });
    const started = Date.now();
    const [e1] = await ctx.call('txQuery', txId, 'SELECT pg_sleep(2)', []);
    assert.match(e1, /^57014 /);
    assert.ok(Date.now() - started < 700, `cancelled after ${Date.now() - started} ms, inside the tx deadline`);
    assert.deepEqual(await ctx.call('txEnd', txId, true), ['tx_aborted']);

    // a txQuery refused for its own deadline never runs
    const [, tx2] = await ctx.call('txBegin', {});
    assert.deepEqual(await ctx.call('txQuery', tx2, "INSERT INTO d_items VALUES ('late')", [], { timeoutMs: 100 }), ['timeout']);
    await ctx.call('txEnd', tx2, true);
    assert.deepEqual(await ctx.sql('SELECT * FROM d_items'), []);

    // a lowered statement_timeout inside a tx does not stick to later statements with more time
    const [, tx3] = await ctx.call('txBegin', { timeoutMs: 60000 });
    await ctx.call('txQuery', tx3, 'SELECT 1', [], { timeoutMs: 1000 });
    const [, rows] = await ctx.call('txQuery', tx3, 'SHOW statement_timeout', []);
    assert.equal(rows[0].statement_timeout, '10s');
    await ctx.call('txEnd', tx3, false);
});

test('M6 + L13 + L3: tx cap per owner, txEnd checks ownership first, fake stop events are ignored', async (t) => {
    const ctx = await setup('t_deadline3', { poolSize: 8 });
    t.after(ctx.teardown);
    const ids = [];
    for (let i = 0; i < 4; i += 1) ids.push((await ctx.callAs('plugin_a', 'txBegin', {}))[1]);
    assert.deepEqual(await ctx.callAs('plugin_a', 'txBegin', {}), ['tx_limit']);
    const [, other] = await ctx.callAs('plugin_b', 'txBegin', {});
    assert.equal(typeof other, 'number', 'another owner is not affected');
    await ctx.callAs('plugin_a', 'txEnd', ids.pop(), false);
    const [, again] = await ctx.callAs('plugin_a', 'txBegin', {});
    assert.equal(typeof again, 'number', 'a slot freed up');
    ids.push(again);

    // L13: a foreign txEnd neither ends the tx nor clears its DDL flag
    await ctx.callAs('plugin_a', 'txQuery', ids[0], 'CREATE TABLE d_new (id int PRIMARY KEY)', []);
    assert.deepEqual(await ctx.callAs('plugin_b', 'txEnd', ids[0], true), ['tx_unknown']);
    assert.deepEqual(await ctx.callAs('plugin_a', 'txEnd', ids[0], true), [null]);
    assert.ok(ctx.internal.catalog.table('d_new'), 'the owner\'s commit reloaded the catalog');

    // L3: plugin_b fires onResourceStop('plugin_a') while plugin_a runs: ignored
    ctx.internal.hooks.onResourceStop('plugin_a');
    assert.deepEqual(await ctx.callAs('plugin_a', 'txQuery', ids[1], 'SELECT 1 AS one', []), [null, [{ one: 1 }], 1]);
    assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'warn' && m.includes('ignored onResourceStop for "plugin_a"')));
    ctx.fivem.states.plugin_a = 'stopping';
    ctx.internal.hooks.onResourceStop('plugin_a');
    await sleep(50);
    assert.deepEqual(await ctx.callAs('plugin_a', 'txQuery', ids[1], 'SELECT 1', []), ['tx_unknown'], 'a genuine stop rolls back');
    await ctx.callAs('plugin_b', 'txEnd', other, false);
});

test('L4: outside test mode the flush interval is at least 50 ms (flushMs 0 does not switch the timer off)', async (t) => {
    const ctx = await queueTables(t, 't_deadline4', { testMode: false, flushMs: 0 });
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'timer', n: 1 } }]);
    await sleep(250);
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['timer']);
});
