// core_db/tests/tx.test.mjs — interactive transactions: commit, rollback, abort, deadline, ownership (§56.2.3).
import test from 'node:test';
import assert from 'node:assert/strict';
import { setup, sleep } from './helpers.mjs';

async function withTable(t, schema) {
    const ctx = await setup(schema);
    t.after(ctx.teardown);
    const [err] = await ctx.call('batch', [
        { sql: 'CREATE TABLE tx_items (id text PRIMARY KEY, n int NOT NULL DEFAULT 0)' },
        { sql: 'CREATE TABLE tx_parent (id text PRIMARY KEY)' },
        { sql: 'CREATE TABLE tx_child (id text PRIMARY KEY, parent text NOT NULL REFERENCES tx_parent (id) DEFERRABLE INITIALLY DEFERRED)' },
    ], null);
    assert.equal(err, null);
    return ctx;
}

const count = async (ctx, id) => (await ctx.sql('SELECT count(*)::int AS n FROM tx_items WHERE id = $1', [id]))[0].n;

test('tx: commit, rollback, crud inside, isolation', async (t) => {
    const ctx = await withTable(t, 't_tx1');
    const [e1, id] = await ctx.call('txBegin', {});
    assert.equal(e1, null);
    assert.equal(typeof id, 'number');
    assert.deepEqual(await ctx.call('txQuery', id, 'INSERT INTO tx_items (id) VALUES ($1)', ['a']), [null, [], 1]);
    const [e2, row] = await ctx.call('crud', 'insert', 'tx_items', { values: { id: 'b', n: 2 } }, id);
    assert.equal(e2, null);
    assert.deepEqual(row, { id: 'b', n: 2 });
    assert.deepEqual(await ctx.call('crud', 'insert', 'tx_items', { values: { nope: 1 } }, id), ['invalid: unknown column tx_items.nope'], 'a validation error does not abort');
    assert.equal(await count(ctx, 'a'), 0, 'not visible before COMMIT');
    assert.deepEqual(await ctx.call('crud', 'count', 'tx_items', {}, id), [null, 2], 'visible inside the tx');
    assert.deepEqual(await ctx.call('txEnd', id, true), [null]);
    assert.equal(await count(ctx, 'a'), 1);
    assert.equal(await count(ctx, 'b'), 1);
    assert.deepEqual(await ctx.call('txEnd', id, true), ['tx_unknown'], 'an ended tx is gone');

    const [, id2] = await ctx.call('txBegin', null);
    await ctx.call('txQuery', id2, "INSERT INTO tx_items (id) VALUES ('c')", []);
    assert.deepEqual(await ctx.call('txEnd', id2, false), [null]);
    assert.equal(await count(ctx, 'c'), 0, 'rolled back');
    assert.deepEqual(await ctx.call('txQuery', 9999, 'SELECT 1', []), ['tx_unknown']);
});

test('tx: a statement error aborts; commit of an aborted tx rolls back and answers tx_aborted', async (t) => {
    const ctx = await withTable(t, 't_tx2');
    const [, id] = await ctx.call('txBegin', {});
    await ctx.call('txQuery', id, "INSERT INTO tx_items (id) VALUES ('a')", []);
    const [e1] = await ctx.call('txQuery', id, "INSERT INTO tx_items (id) VALUES ('a')", []);
    assert.match(e1, /^23505 /);
    assert.deepEqual(await ctx.call('txQuery', id, 'SELECT 1', []), ['tx_aborted']);
    assert.deepEqual(await ctx.call('crud', 'count', 'tx_items', {}, id), ['tx_aborted']);
    assert.deepEqual(await ctx.call('txEnd', id, true), ['tx_aborted']);
    assert.equal(await count(ctx, 'a'), 0);
    const [, status] = await ctx.call('status');
    assert.equal(status.pool.total - status.pool.idle, 0, 'the client went back to the pool');

    // A deferred FK fails at COMMIT: the error comes back from txEnd.
    const [, id2] = await ctx.call('txBegin', {});
    await ctx.call('txQuery', id2, "INSERT INTO tx_child (id, parent) VALUES ('k', 'missing')", []);
    const [e2] = await ctx.call('txEnd', id2, true);
    assert.match(e2, /^23503 /);
});

test('tx: the deadline rolls back and frees the client; later calls answer tx_expired', async (t) => {
    const ctx = await withTable(t, 't_tx3');
    const [, id] = await ctx.call('txBegin', { timeoutMs: 150 });
    await ctx.call('txQuery', id, "INSERT INTO tx_items (id) VALUES ('late')", []);
    await sleep(300);
    assert.deepEqual(await ctx.call('txQuery', id, 'SELECT 1', []), ['tx_expired']);
    assert.deepEqual(await ctx.call('txEnd', id, true), ['tx_expired']);
    assert.equal(await count(ctx, 'late'), 0);
    const [, status] = await ctx.call('status');
    assert.equal(status.pool.total - status.pool.idle, 0);
    assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'warn' && /passed its 150 ms deadline/.test(m)));
});

test('tx: belongs to the invoker; a stopping owner is rolled back', async (t) => {
    const ctx = await withTable(t, 't_tx4');
    const [, id] = await ctx.callAs('plugin_a', 'txBegin', {});
    assert.deepEqual(await ctx.callAs('plugin_b', 'txQuery', id, 'SELECT 1', []), ['tx_unknown']);
    assert.deepEqual(await ctx.callAs('plugin_b', 'txEnd', id, false), ['tx_unknown']);
    await ctx.callAs('plugin_a', 'txQuery', id, "INSERT INTO tx_items (id) VALUES ('p')", []);
    ctx.fivem.states.plugin_a = 'stopping';
    ctx.internal.hooks.onResourceStop('plugin_a');
    await sleep(50);
    assert.deepEqual(await ctx.callAs('plugin_a', 'txQuery', id, 'SELECT 1', []), ['tx_unknown']);
    assert.equal(await count(ctx, 'p'), 0);
    const [, status] = await ctx.call('status');
    assert.equal(status.pool.total - status.pool.idle, 0);
});
