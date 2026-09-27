// core_db/tests/queue_merge.test.mjs — review R1/R4 fixes around coalescing and waiting: H1 (a key at most once per
// bulk statement), M2 (merge semantics), L8 (drop accounting after merges), L14 (no merge across a pending
// barrier; order kept), M3 (sync ignores other owners' held entries), R4 (row-precise read-your-writes).
import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { queueTables, traced, sleep, eventsNamed } from './helpers.mjs';

const require = createRequire(import.meta.url);
const { buildStatements } = require('../src/sqlgen.js');

const KEYED = { t: 'sql', sql: "UPDATE q_items SET data = '{\"x\":1}'::jsonb WHERE id = 'c1'", key: 'k:c1' };

test('H1 unit: a run ends where a row identity would repeat', () => {
    const e = (id, v) => ({ t: 'patch', table: 'q_items', pk: ['id'], data: { id, v }, cols: ['id', 'v'], sig: 'patch|id,v', identity: `q_items|${id}`, owner: 'core' });
    const out = buildStatements([e('a', 1), e('b', 1), e('a', 2), e('c', 1), e('b', 2)]);
    assert.deepEqual(out.map((s) => s.entries.map((x) => `${x.data.id}${x.data.v}`)), [['a1', 'b1'], ['a2', 'c1', 'b2']]);
});

test('H1: two patches of one row never share one UPDATE (large ANALYZEd table, keyed-sql splice and requeue)', async (t) => {
    const ctx = await queueTables(t, 't_qmerge1');
    await ctx.sql("INSERT INTO q_items (id, n, v) SELECT 'r' || g, 0, 'x' FROM generate_series(1, 50000) g");
    await ctx.sql("INSERT INTO q_items (id, n, v) VALUES ('c1', 0, 'start')");
    await ctx.sql('ANALYZE q_items');
    ctx.enqueue([{ t: 'patch', table: 'q_items', key: 'c1', changes: { v: 'OLD' } }, KEYED]);
    ctx.enqueue([{ t: 'patch', table: 'q_items', key: 'c1', changes: { v: 'NEW' } }, KEYED]);
    const seen = await traced(ctx, () => ctx.flush());
    assert.equal(seen.filter((s) => s.startsWith('UPDATE "q_items" AS x')).length, 2, 'one UPDATE per patch of c1');
    assert.deepEqual(await ctx.sql("SELECT v FROM q_items WHERE id = 'c1'"), [{ v: 'NEW' }]);
    assert.equal((await ctx.call('status'))[1].queue.dropped, 0);

    await ctx.sql("UPDATE q_items SET v = 'start' WHERE id = 'c1'");
    ctx.api.testFail('^BEGIN$', 'ECONNRESET');
    ctx.enqueue([{ t: 'patch', table: 'q_items', key: 'c1', changes: { v: 'OLD2' } }, KEYED]);
    await ctx.internal.queue.flushNow(); // fails, requeued
    ctx.enqueue([{ t: 'patch', table: 'q_items', key: 'c1', changes: { v: 'NEW2' } }, KEYED]);
    ctx.api.testFail(null);
    await ctx.internal.queue.flushNow();
    assert.deepEqual(await ctx.sql("SELECT v FROM q_items WHERE id = 'c1'"), [{ v: 'NEW2' }], 'after an outage and a requeue');

    // two saves of one key in one run used to fail with 21000 and fall into safe mode
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 's1', v: 'a' } }, KEYED]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 's1', v: 'b' } }, KEYED]);
    const seen2 = await traced(ctx, () => ctx.flush());
    assert.ok(!seen2.includes('SET CONSTRAINTS ALL IMMEDIATE'), 'no safe mode');
    assert.deepEqual(await ctx.sql("SELECT v FROM q_items WHERE id = 's1'"), [{ v: 'b' }]);
});

test('M2: save after remove keeps the DELETE (cascade, defaults); saves and patches merge columns', async (t) => {
    const ctx = await queueTables(t, 't_qmerge2');
    await ctx.call('batch', [
        { sql: 'CREATE TABLE fm (character_id text PRIMARY KEY, faction text NOT NULL, rank int NOT NULL DEFAULT 0, joined_at timestamptz NOT NULL DEFAULT now())' },
        { sql: 'CREATE TABLE fm_perm (id text PRIMARY KEY, character_id text NOT NULL REFERENCES fm (character_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)' },
        { sql: "INSERT INTO fm VALUES ('c1', 'police', 5, '2020-01-01')" },
        { sql: "INSERT INTO fm_perm VALUES ('p1', 'c1')" },
    ], null);
    ctx.enqueue([
        { t: 'remove', table: 'fm', key: 'c1' },
        { t: 'save', table: 'fm', row: { character_id: 'c1', faction: 'mafia' } },
        { t: 'patch', table: 'fm', key: 'c1', changes: { rank: 1 } }, // merges into the save behind the DELETE
    ]);
    assert.equal((await ctx.call('status'))[1].queue.pending, 2);
    await ctx.flush();
    const [row] = await ctx.sql('SELECT faction, rank, joined_at > now() - interval \'1 minute\' AS fresh FROM fm');
    assert.deepEqual(row, { faction: 'mafia', rank: 1, fresh: true }, 'defaults again, not the old row');
    assert.deepEqual(await ctx.sql('SELECT * FROM fm_perm'), [], 'the DELETE cascaded');
});

test('L8 + L14: merged contributions are all accounted; merges never cross a pending barrier; row order kept', async (t) => {
    const ctx = await queueTables(t, 't_qmerge3');
    // L8: A's save and B's (bad) patch merge into one entry; dropping it reports to BOTH owners
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'm', n: 1 } }], 'plug_a');
    ctx.enqueue([{ t: 'patch', table: 'q_items', key: 'm', changes: { n: -1 } }], 'plug_b');
    assert.equal((await ctx.call('status'))[1].queue.pending, 1, 'merged');
    await ctx.flush();
    await sleep(5);
    assert.deepEqual(eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed').map((e) => e[0]).sort(), ['plug_a', 'plug_b']);
    assert.deepEqual(await ctx.callAs('plug_a', 'sync', null, 1000), [null, { dropped: 1 }]);
    assert.deepEqual(await ctx.callAs('plug_b', 'sync', null, 1000), [null, { dropped: 1 }]);

    // L14: slowplug's save of r is held behind its pending barrier; core's newer save of r does not merge
    // into it — and waits behind it, so the row ends with core's value (program order).
    ctx.syncAs('slowplug', 'migrate', [{ version: 1, sql: 'SELECT pg_sleep(0.4)' }]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'r', n: 1, v: 'slowplug' } }], 'slowplug');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'r', n: 2, v: 'core' } }, { t: 'save', table: 'q_items', row: { id: 'free', n: 1 } }], 'core');
    assert.equal((await ctx.call('status'))[1].queue.pending, 3, 'not merged across the pending barrier');
    await ctx.internal.queue.flushNow();
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['free'], 'r waits behind the held entry of its row');
    await ctx.flush(); // testFlush waits for the barrier
    assert.deepEqual(await ctx.sql("SELECT n, v FROM q_items WHERE id = 'r'"), [{ n: 2, v: 'core' }]);
});

test('M3: a sync never waits for another owner\'s entries held behind its pending barrier', async (t) => {
    const ctx = await queueTables(t, 't_qmerge4');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'plugin_final', n: 1 } }], 'slowplug');
    ctx.syncAs('slowplug', 'migrate', [{ version: 1, sql: 'SELECT pg_sleep(1.5)' }]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'held_new', n: 1 } }], 'slowplug');
    let started = Date.now();
    assert.deepEqual(await ctx.callAs('core', 'query', 'SELECT 1 AS one', [], { sync: true, timeoutMs: 1000 }), [null, [{ one: 1 }], 1]);
    assert.ok(Date.now() - started < 500, 'core did not wait for slowplug');
    assert.deepEqual(await ctx.callAs('core', 'sync', null, 1000), [null, { dropped: 0 }]);
    assert.ok(Date.now() - started < 900);
    started = Date.now();
    assert.deepEqual(await ctx.callAs('slowplug', 'sync', null, 5000), [null, { dropped: 0 }], 'the owner itself waits');
    assert.ok(Date.now() - started >= 500);
    assert.equal((await ctx.items()).length, 2);
});

test('R4: row-precise read-your-writes for crud reads that pin the whole primary key', async (t) => {
    const ctx = await queueTables(t, 't_qmerge5');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'mine', n: 7 } }, { t: 'save', table: 'q_items', row: { id: 'other', n: 1 } }]);
    // nothing queued for 'nobody': answered at once, no flush forced
    const flushes = (await ctx.call('status'))[1].queue.flushes;
    assert.deepEqual(await ctx.call('crud', 'first', 'q_items', { where: { id: 'nobody' }, sync: true }, 0), [null, null]);
    await sleep(20);
    let [, st] = await ctx.call('status');
    assert.equal(st.queue.flushes, flushes, 'no flush forced');
    assert.equal(st.queue.pending, 2);
    // the pinned row waits for its own entry
    const [err, row] = await ctx.call('crud', 'first', 'q_items', { where: { id: 'mine' }, sync: true }, 0);
    assert.equal(err, null);
    assert.equal(row.n, 7);
    // a read that does not pin the key keeps the global semantics
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'later', n: 1 } }]);
    const [, rows] = await ctx.call('crud', 'select', 'q_items', { where: { n: 1 }, sync: true }, 0);
    assert.deepEqual(rows.map((r) => r.id).sort(), ['later', 'other']);
    [, st] = await ctx.call('status');
    assert.equal(st.queue.pending, 0);
    // composite keys: both columns must be pinned
    ctx.enqueue([{ t: 'save', table: 'q_pairs', row: { a: 'x', b: 1, v: 'v' } }]);
    assert.deepEqual(await ctx.call('crud', 'first', 'q_pairs', { where: { a: 'x', b: 1 }, sync: true }, 0), [null, { a: 'x', b: 1, v: 'v' }]);
});
