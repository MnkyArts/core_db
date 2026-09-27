// core_db/tests/queue_failures.test.mjs — the queue under failure and in time (§56.3.3–§56.3.5): poison →
// safe mode, connection loss → requeue + backoff, sync / dropped accounting, owner barriers, flush triggers,
// stop hooks.
import test from 'node:test';
import assert from 'node:assert/strict';
import { queueTables, sleep, eventsNamed, traced } from './helpers.mjs';

test('poison: safe mode drops exactly the bad entries, commits the rest, reports them', async (t) => {
    const ctx = await queueTables(t, 't_qfail1');
    ctx.enqueue([
        { t: 'save', table: 'q_items', row: { id: 'ok1', n: 1 } },
        { t: 'save', table: 'q_items', row: { id: 'bad', n: -1 } },
        { t: 'save', table: 'q_items', row: { id: 'ok2', n: 2 } },
        { t: 'save', table: 'q_child', row: { id: 'orphan', parent: 'missing' } },
        { t: 'append', table: 'q_log', row: { msg: 'kept' } },
    ]);
    ctx.enqueue([{ t: 'sql', sql: 'INSERT INTO no_such_table VALUES (1)' }], 'plugin_b');
    const seen = await traced(ctx, () => ctx.flush());
    assert.ok(seen.includes('SET CONSTRAINTS ALL IMMEDIATE'), 'safe mode ran');
    // runs in original order: [ok1 bad ok2] [orphan] [kept] [sql]; the first is bisected: [ok1] [bad ok2] [bad] [ok2]
    assert.equal(seen.filter((s) => s === 'SAVEPOINT e').length, 8, 'one savepoint per run and per bisection');
    assert.deepEqual(await ctx.items(), [{ id: 'ok1', n: 1, v: null }, { id: 'ok2', n: 2, v: null }]);
    assert.equal((await ctx.sql('SELECT count(*)::int AS n FROM q_log'))[0].n, 1);
    assert.deepEqual(await ctx.sql('SELECT * FROM q_child'), []);

    await sleep(5);
    const failed = eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed');
    assert.equal(failed.length, 3);
    assert.deepEqual(failed[0].slice(0, 3), ['core', 'save', 'q_items']);
    assert.match(failed[0][3], /^23514 /);
    assert.deepEqual(failed[1].slice(0, 3), ['core', 'save', 'q_child']);
    assert.match(failed[1][3], /^23503 /);
    assert.deepEqual(failed[2].slice(0, 3), ['plugin_b', 'sql', null]);
    assert.match(failed[2][3], /^42P01 /);
    assert.equal(failed[0][4], 'bad', 'the dropped row\'s primary key rides along');
    assert.equal(failed[1][4], 'orphan');
    assert.equal(failed[2][4], null, 'a raw statement has no key');
    assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'error' && /^dropped queued save on q_items by core: 23514 /.test(m)));
    const [, status] = await ctx.call('status');
    assert.equal(status.queue.dropped, 3);
    assert.match(status.queue.lastError, /^42P01 /);
    assert.equal(status.healthy, true, 'statement errors are not outages');

    assert.deepEqual(await ctx.call('sync', null, 1000), [null, { dropped: 2 }], "the caller's own drops");
    assert.deepEqual(await ctx.call('sync', null, 1000), [null, { dropped: 0 }], 'reported once');
    assert.deepEqual(await ctx.callAs('plugin_b', 'sync', null, 1000), [null, { dropped: 1 }]);
});

test('connection loss: the batch is requeued in order, coalescing resumes, backoff doubles, nothing dropped', async (t) => {
    const ctx = await queueTables(t, 't_qfail2');
    ctx.api.testFail('^BEGIN$', 'ECONNRESET');
    ctx.enqueue([
        { t: 'save', table: 'q_items', row: { id: 'a', n: 1 } },
        { t: 'append', table: 'q_log', row: { msg: 'x' } },
    ]);
    await ctx.internal.queue.flushNow();
    let [, status] = await ctx.call('status');
    assert.equal(status.queue.pending, 2, 'back in the queue');
    assert.equal(status.queue.lastError, 'unavailable');
    assert.equal(status.queue.backoffMs, 1000);
    assert.equal(status.queue.dropped, 0);
    assert.equal(ctx.api.isHealthy(), false);
    ctx.enqueue([{ t: 'patch', table: 'q_items', key: 'a', changes: { v: 'merged' } }]);
    assert.equal((await ctx.call('status'))[1].queue.pending, 2, 'the patch coalesced into the requeued save');
    await ctx.internal.queue.flushNow();
    [, status] = await ctx.call('status');
    assert.equal(status.queue.backoffMs, 2000, '1 s, 2 s, 4 s … capped at 30 s');
    assert.deepEqual(await ctx.call('sync', null, 150), ['timeout']);

    ctx.api.testFail(null);
    await ctx.internal.queue.flushNow();
    [, status] = await ctx.call('status');
    assert.equal(status.queue.pending, 0);
    assert.equal(status.queue.backoffMs, 0);
    assert.deepEqual(await ctx.items(), [{ id: 'a', n: 1, v: 'merged' }]);
    assert.equal(ctx.api.isHealthy(), true);
    await sleep(5);
    assert.deepEqual(eventsNamed(ctx.fivem, 'core:hook:dbStatus').map((e) => e[0]), [false, true]);
    assert.deepEqual(eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed'), []);
});

test('connection loss: the backoff timer retries by itself', async (t) => {
    const ctx = await queueTables(t, 't_qfail3');
    ctx.api.testFail('INSERT INTO "q_items"', '08006 connection failure');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'r', n: 1 } }]);
    await ctx.internal.queue.flushNow();
    assert.equal((await ctx.call('status'))[1].queue.pending, 1);
    ctx.api.testFail(null);
    await sleep(1300);
    assert.deepEqual(await ctx.items(), [{ id: 'r', n: 1, v: null }], 'retried after 1 s');
});

test('sync: waits for everything up to seq (merged entries included), times out, kicks a flush', async (t) => {
    const ctx = await queueTables(t, 't_qfail4');
    const first = ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'a', n: 1 } }]).seq;
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'a', n: 2 } }]);
    const [err, answer] = await ctx.call('sync', first, 1000);
    assert.equal(err, null, 'no timer runs (flushMs 0): the waiting sync kicked the flush');
    assert.deepEqual(answer, { dropped: 0 });
    assert.deepEqual(await ctx.items(), [{ id: 'a', n: 2, v: null }]);
    assert.deepEqual(await ctx.call('sync', 999, 1000), [null, { dropped: 0 }], 'a future seq is clamped');
    assert.deepEqual(await ctx.call('sync', 'x', 1000), ['invalid: seq must be a number']);
});

test('owner barriers: held entries wait, other owners flush around them, a settle flushes them', async (t) => {
    const ctx = await queueTables(t, 't_qfail5');
    assert.deepEqual(ctx.syncAs('slowplug', 'migrate', [{ version: 1, sql: 'SELECT pg_sleep(0.4)' }]), { ok: true });
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'held', n: 1 } }], 'slowplug');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'free', n: 1 } }], 'core');
    await ctx.internal.queue.flushNow();
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['free']);
    assert.equal((await ctx.call('status'))[1].queue.pending, 1);
    assert.deepEqual(await ctx.callAs('slowplug', 'awaitMigrations', 5000), [null]);
    await sleep(100);
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['free', 'held'], 'flushed after the barrier settled');

    // testFlush (the bridge's write-through) waits for a pending barrier itself.
    ctx.syncAs('slowplug', 'migrate', [{ version: 1, sql: 'SELECT 1' }, { version: 2, sql: 'SELECT pg_sleep(0.3)' }]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'held2', n: 1 } }], 'slowplug');
    await ctx.flush();
    assert.ok((await ctx.items()).some((r) => r.id === 'held2'));
});

test('flush triggers: more than 2000 pending, the timer, another resource stopping, own stop', async (t) => {
    const ctx = await queueTables(t, 't_qfail6');
    const many = Array.from({ length: 2001 }, (_, i) => ({ t: 'append', table: 'q_log', row: { msg: `${i}` } }));
    ctx.enqueue(many);
    await sleep(400);
    assert.equal((await ctx.sql('SELECT count(*)::int AS n FROM q_log'))[0].n, 2001, 'flushed on the next tick');

    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'stop', n: 1 } }]);
    ctx.fivem.states.some_plugin = 'stopping';
    ctx.internal.hooks.onResourceStop('some_plugin');
    await sleep(200);
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['stop'], "another resource's stop starts a flush");
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'shutdown', n: 1 } }]);
    ctx.internal.hooks.onShutdown();
    await sleep(200);
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['shutdown', 'stop']);

    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'lost', n: 1 } }]);
    ctx.fivem.states.core_db = 'stopping';
    ctx.internal.hooks.onResourceStop('core_db');
    assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'error' && /^1 queued write\(s\) were not committed and are lost/.test(m)));
});

test('the flush timer commits within core_db_flush_ms', async (t) => {
    const ctx = await queueTables(t, 't_qfail7', { flushMs: 50 });
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'tick', n: 1 } }]);
    await sleep(300);
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['tick']);
    const [, status] = await ctx.call('status');
    assert.ok(status.queue.flushes >= 1);
    assert.equal(typeof status.queue.lastFlushMs, 'number');
});
