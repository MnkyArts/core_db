// core_db/tests/queue_retry.test.mjs — review R1 C1/H3/M6: 08P01 is a statement error, retryable errors requeue
// (bounded), repeated failures isolate the batch in safe mode, pool saturation has no exponential backoff, a
// waiting sync cancels the backoff once the database answers again.
import test from 'node:test';
import assert from 'node:assert/strict';
import { queueTables, sleep, eventsNamed, traced, pg, BASE_URL } from './helpers.mjs';

test('C1: 08P01 (wrong parameter count) is a statement error — dropped, never requeued for ever', async (t) => {
    const ctx = await queueTables(t, 't_qretry1', { flushMs: 50 });
    const answer = ctx.enqueue([{ t: 'sql', sql: 'UPDATE q_items SET v = $1 WHERE id = $2', params: ['x'] }], 'buggy_plugin');
    assert.ok(answer.seq);
    assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'warn' && /queued sql by buggy_plugin uses \$2 but has 1 param/.test(m)), 'warned at enqueue');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'money_row', n: 100 } }], 'core');
    await sleep(600);
    const [, st] = await ctx.call('status');
    assert.equal(st.healthy, true);
    assert.equal(st.queue.pending, 0);
    assert.equal(st.queue.dropped, 1);
    assert.match(st.queue.lastError, /^08P01 /);
    assert.deepEqual(await ctx.items(), [{ id: 'money_row', n: 100, v: null }]);
    assert.equal(eventsNamed(ctx.fivem, 'core:hook:dbStatus').length, 0, 'health never flapped');
    const [err] = await ctx.call('query', 'SELECT $1::int, $2::int', [1], null);
    assert.match(err, /^08P01 /);
    assert.equal(ctx.api.isHealthy(), true);
});

test('H3: a lock timeout requeues with backoff (nothing dropped, health untouched), then commits', async (t) => {
    const ctx = await queueTables(t, 't_qretry2', { statementTimeoutMs: 400, core: { retryFirstMs: 100 } });
    await ctx.sql("INSERT INTO q_items (id, n, v) VALUES ('c1', 0, 'start'), ('c2', 0, 'start')");
    const holder = new pg.Client(BASE_URL);
    await holder.connect();
    await holder.query('SET search_path TO t_qretry2, public');
    await holder.query('BEGIN');
    await holder.query("SELECT * FROM q_items WHERE id = 'c1' FOR UPDATE");
    ctx.enqueue([
        { t: 'patch', table: 'q_items', key: 'c1', changes: { v: 'autosave' } },
        { t: 'patch', table: 'q_items', key: 'c2', changes: { v: 'autosave' } },
    ]);
    const started = Date.now();
    await ctx.internal.queue.flushNow();
    assert.ok(Date.now() - started < 1500, 'lock_timeout (200 ms here) ends the wait, not the statement timeout');
    let [, st] = await ctx.call('status');
    assert.equal(st.queue.pending, 2, 'requeued');
    assert.match(st.queue.lastError, /^55P03 /);
    assert.equal(st.queue.dropped, 0);
    assert.equal(st.healthy, true, 'a lock timeout is not an outage');
    await holder.query('COMMIT');
    await holder.end();
    await sleep(400); // the retry timer (100 ms, then 200 ms) commits both
    [, st] = await ctx.call('status');
    assert.equal(st.queue.pending, 0);
    assert.deepEqual(await ctx.sql('SELECT id, v FROM q_items ORDER BY id'), [{ id: 'c1', v: 'autosave' }, { id: 'c2', v: 'autosave' }]);
    assert.deepEqual(eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed'), []);
});

test('H3 + C1: an entry that keeps failing is isolated in safe mode and finally dropped; the rest commits', async (t) => {
    const ctx = await queueTables(t, 't_qretry3', { statementTimeoutMs: 400 });
    await ctx.sql("INSERT INTO q_items (id, n, v) VALUES ('c1', 0, 'start'), ('c2', 0, 'start')");
    // (the schema drop of the teardown would wait for the holder's lock: release it inside the test)
    const holder = new pg.Client(BASE_URL);
    await holder.connect();
    await holder.query('SET search_path TO t_qretry3, public');
    await holder.query('BEGIN');
    await holder.query("SELECT * FROM q_items WHERE id = 'c1' FOR UPDATE");
    ctx.enqueue([
        { t: 'patch', table: 'q_items', key: 'c1', changes: { v: 'stuck' } },
        { t: 'patch', table: 'q_items', key: 'c2', changes: { v: 'ok' } },
    ]);
    let safe = 0;
    try {
        for (let i = 0; i < 12; i += 1) {
            const seen = await traced(ctx, () => ctx.internal.queue.flushNow());
            if (seen.includes('SET CONSTRAINTS ALL IMMEDIATE')) safe += 1;
            if ((await ctx.call('status'))[1].queue.pending === 0) break;
        }
    } finally {
        await holder.query('ROLLBACK').catch(() => {});
        await holder.end().catch(() => {});
    }
    assert.equal(safe, 3, '5 failed batches → isolation; the stubborn entry is dropped on its 3rd safe-mode attempt');
    assert.deepEqual(await ctx.sql('SELECT id, v FROM q_items ORDER BY id'), [{ id: 'c1', v: 'start' }, { id: 'c2', v: 'ok' }]);
    await sleep(5);
    const failed = eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed');
    assert.equal(failed.length, 1);
    assert.equal(failed[0][4], 'c1');
    assert.match(failed[0][3], /^55P03 /);
});

test('C1: five failed batches (connection) isolate the batch in safe mode — still nothing dropped', async (t) => {
    const ctx = await queueTables(t, 't_qretry4');
    ctx.api.testFail('^COMMIT$', 'ECONNRESET');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'a', n: 1 } }]);
    for (let i = 0; i < 5; i += 1) await ctx.internal.queue.flushNow();
    assert.equal((await ctx.call('status'))[1].queue.pending, 1);
    ctx.api.testFail(null);
    const seen = await traced(ctx, () => ctx.internal.queue.flushNow());
    assert.ok(seen.includes('SET CONSTRAINTS ALL IMMEDIATE'), 'isolated');
    assert.deepEqual(await ctx.items(), [{ id: 'a', n: 1, v: null }]);
    assert.equal((await ctx.call('status'))[1].queue.dropped, 0);
});

test('M6: pool saturation retries at a fixed short delay; a waiting sync cancels the backoff once healthy', async (t) => {
    const ctx = await queueTables(t, 't_qretry5');
    ctx.api.testFail('^BEGIN$', 'timeout');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'a', n: 1 } }]);
    await ctx.internal.queue.flushNow();
    await ctx.internal.queue.flushNow();
    assert.equal((await ctx.call('status'))[1].queue.backoffMs, 250, 'no doubling for saturation');
    ctx.api.testFail(null);
    await sleep(400);
    assert.equal((await ctx.call('status'))[1].queue.pending, 0);

    ctx.api.testFail('^BEGIN$', 'ECONNRESET');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'b', n: 1 } }]);
    await ctx.internal.queue.flushNow();
    assert.equal((await ctx.call('status'))[1].queue.backoffMs, 1000);
    ctx.api.testFail(null);
    const started = Date.now();
    const waiting = ctx.call('sync', null, 5000);
    await sleep(50);
    await ctx.call('query', 'SELECT 1', [], null); // the database answers → health back → the waiter's flush runs now
    assert.deepEqual(await waiting, [null, { dropped: 0 }]);
    assert.ok(Date.now() - started < 800, 'did not sit out the 1 s backoff');
});
