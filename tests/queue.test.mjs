// core_db/tests/queue.test.mjs — the write-behind queue (§56.3): validation, the coalescing matrix, run
// grouping, bulk chunking, deferred FKs across tables, value conversion in the payload.
// Failures, sync accounting, barriers and timing: queue_failures.test.mjs.
import test from 'node:test';
import assert from 'node:assert/strict';
import { queueTables, traced } from './helpers.mjs';

const NULL = { __null: true };

test('enqueue: synchronous validation, all or nothing', async (t) => {
    const ctx = await queueTables(t, 't_queue1');
    const bad = [
        [{ t: 'save', table: 'nope', row: { id: 1 } }, /^invalid: unknown table nope$/],
        [{ t: 'save', table: 'Q_items', row: { id: 1 } }, /^invalid: table name/],
        [{ t: 'save', table: 'q_items', row: { id: 'a', zzz: 1 } }, /^invalid: unknown column q_items.zzz$/],
        [{ t: 'save', table: 'q_items', row: { n: 1 } }, /save on q_items needs primary key column id/],
        [{ t: 'save', table: 'q_items', row: { id: NULL } }, /needs primary key column id/],
        [{ t: 'save', table: 'q_req', row: { id: 'r' } }, /save on q_req needs column must \(NOT NULL without a default\)/],
        [{ t: 'save', table: 'q_req', row: { id: 'r', must: NULL } }, /needs column must/],
        [{ t: 'save', table: 'q_items', row: 'x' }, /row must be a map/],
        [{ t: 'patch', table: 'q_items', key: 'a', changes: { id: 'b' } }, /patch cannot change primary key column q_items.id/],
        [{ t: 'patch', table: 'q_pairs', key: 'a', changes: { v: 'x' } }, /key must be the primary key value or \{ a, b \}/],
        [{ t: 'remove', table: 'q_pairs', key: { a: 'x' } }, /key needs primary key column q_pairs.b/],
        [{ t: 'remove', table: 'q_pairs', key: { a: 'x', b: 1, v: 2 } }, /key column q_pairs.v is not part of the primary key/],
        [{ t: 'remove', table: 'q_nopk', key: 1 }, /table q_nopk has no primary key/],
        [{ t: 'save', table: 'q_items', row: { id: 'a', at: true } }, /q_items.at needs Unix seconds/],
        [{ t: 'save', table: 'q_items', row: { id: 'a', tags: { x: 1 } } }, /q_items.tags needs a list/],
        [{ t: 'drop', table: 'q_items' }, /unknown kind "drop"/],
        [{ t: 'sql', sql: '  ' }, /sql must be a non-empty string/],
        [{ t: 'sql', sql: 'SELECT 1', key: { a: 1 } }, /key must be a string or a number/],
        ['nope', /entry 2 must be a table/],
    ];
    for (const [entry, re] of bad) {
        const answer = ctx.enqueue([{ t: 'append', table: 'q_log', row: { msg: 'first' } }, entry]);
        assert.match(answer.error || '', re, JSON.stringify(entry));
    }
    assert.deepEqual(ctx.enqueue('x'), { error: 'invalid: entries must be a list' });
    assert.equal(await ctx.pending(), 0, 'a refused call queues nothing');
    assert.deepEqual(ctx.enqueue([]), { seq: 0 });
    assert.deepEqual(ctx.enqueue([{ t: 'patch', table: 'q_items', key: 'a', changes: [] }]), { seq: 1 }, 'an empty patch is a no-op');
    assert.equal(await ctx.pending(), 0);
    assert.deepEqual(ctx.enqueue([{ t: 'append', table: 'q_log', row: { msg: 'a' } }, { t: 'sql', sql: 'SELECT $1::int', params: [1] }]), { seq: 3 });
    assert.equal(await ctx.pending(), 2);
});

test('coalescing matrix (§56.3.2)', async (t) => {
    const ctx = await queueTables(t, 't_queue2');
    await ctx.sql("INSERT INTO q_items (id, n, v) VALUES ('pp', 1, 'old'), ('pr', 1, 'old'), ('rp', 1, 'old'), ('rs', 1, 'old')");
    const q = (entries) => assert.ok(ctx.enqueue(entries).seq);
    q([{ t: 'save', table: 'q_items', row: { id: 'ss', n: 1, v: 'first' } }, { t: 'save', table: 'q_items', row: { id: 'ss', n: 2 } }]);
    q([{ t: 'save', table: 'q_items', row: { id: 'sp', n: 1 } }, { t: 'patch', table: 'q_items', key: 'sp', changes: { v: 'patched' } }]);
    q([{ t: 'patch', table: 'q_items', key: 'pp', changes: { n: 5 } }, { t: 'patch', table: 'q_items', key: { id: 'pp' }, changes: { v: 'new', n: 6 } }]);
    q([{ t: 'patch', table: 'q_items', key: 'ps', changes: { v: 'lost' } }, { t: 'save', table: 'q_items', row: { id: 'ps', n: 3 } }]);
    q([{ t: 'save', table: 'q_items', row: { id: 'sr', n: 1 } }, { t: 'remove', table: 'q_items', key: 'sr' }]);
    q([{ t: 'patch', table: 'q_items', key: 'pr', changes: { n: 9 } }, { t: 'remove', table: 'q_items', key: 'pr' }]);
    q([{ t: 'remove', table: 'q_items', key: 'rs' }, { t: 'save', table: 'q_items', row: { id: 'rs', n: 4 } }]);
    q([{ t: 'remove', table: 'q_items', key: 'rp' }, { t: 'patch', table: 'q_items', key: 'rp', changes: { n: 7 } }]);
    q([{ t: 'append', table: 'q_log', row: { msg: 'a' } }, { t: 'append', table: 'q_log', row: { msg: 'a' } }]);
    q([{ t: 'sql', sql: "INSERT INTO q_pairs VALUES ('k', 1, 'one')", key: 'k' }, { t: 'sql', sql: "INSERT INTO q_pairs VALUES ('k', 2, 'two')", key: 'k' }]);
    q([{ t: 'save', table: 'q_pairs', row: { a: 'x', b: 1, v: 'one' } }, { t: 'save', table: 'q_pairs', row: { b: 1, a: 'x', v: 'two' } }]);
    assert.ok(ctx.syncAs('other', 'enqueue', [{ t: 'sql', sql: "INSERT INTO q_pairs VALUES ('k', 3, 'three')", key: 'k' }]).seq);
    // 9 row identities (rs twice: the save after its remove is a new entry) + 2 appends + 1 keyed sql (core)
    // + 1 keyed sql (other owner, not coalesced)
    assert.equal(await ctx.pending(), 14);
    await ctx.flush();
    assert.deepEqual(await ctx.items(), [
        { id: 'pp', n: 6, v: 'new' },          // patch + patch → merged, newer wins
        { id: 'ps', n: 3, v: 'lost' },         // save after patch → the columns merge (R1 M2)
        { id: 'rs', n: 4, v: null },           // save after remove → DELETE, then INSERT: defaults again (R1 M2)
        { id: 'sp', n: 1, v: 'patched' },      // patch after save → merged into the row
        { id: 'ss', n: 2, v: 'first' },        // save after save → the columns merge, newer wins (R1 M2)
    ]);                                        // sr, pr: remove after save/patch; rp: patch after remove dropped
    assert.equal((await ctx.sql('SELECT count(*)::int AS n FROM q_log'))[0].n, 2, 'append never coalesces');
    assert.deepEqual((await ctx.sql('SELECT a, b, v FROM q_pairs ORDER BY a, b')), [
        { a: 'k', b: 2, v: 'two' }, { a: 'k', b: 3, v: 'three' }, { a: 'x', b: 1, v: 'two' },
    ]);
    const [, status] = await ctx.call('status');
    assert.equal(status.queue.dropped, 0, 'coalescing never counts as a drop');
});

test('a merged entry keeps the OLDER position and the newer content', async (t) => {
    const ctx = await queueTables(t, 't_queue3');
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'a', v: 'v1' } }, { t: 'save', table: 'q_items', row: { id: 'b', v: 'b1' } }]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'a', v: 'v2' } }]);
    await ctx.flush();
    assert.deepEqual(await ctx.items(), [{ id: 'a', n: 0, v: 'v2' }, { id: 'b', n: 0, v: 'b1' }]);
});

test('a raw statement is a coalescing barrier: program order survives (§56.3.2)', async (t) => {
    const ctx = await queueTables(t, 't_queue3b');
    ctx.enqueue([
        { t: 'save', table: 'q_items', row: { id: 'a', v: 'v1' } },
        { t: 'sql', sql: "UPDATE q_items SET v = v || '!' WHERE id = 'a'" },
    ]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'a', v: 'v2' } }]);
    // the lost-write case: save → DELETE → save must end with the row present
    ctx.enqueue([
        { t: 'save', table: 'q_items', row: { id: 'd', v: 'd1' } },
        { t: 'sql', sql: "DELETE FROM q_items WHERE id = 'd'" },
        { t: 'save', table: 'q_items', row: { id: 'd', v: 'd2' } },
    ]);
    // a keyed statement that is replaced runs at its NEWEST position (after the save queued in between)
    ctx.enqueue([{ t: 'sql', sql: "UPDATE q_items SET v = 'k1' WHERE id = 'k'", key: 'k' }]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'k', v: 'saved' } }]);
    ctx.enqueue([{ t: 'sql', sql: "UPDATE q_items SET v = 'k2' WHERE id = 'k'", key: 'k' }]);
    await ctx.flush();
    assert.deepEqual(await ctx.items(), [
        { id: 'a', n: 0, v: 'v2' },
        { id: 'd', n: 0, v: 'd2' },
        { id: 'k', n: 0, v: 'k2' },
    ]);
});

test('runs, bulk statements ≤ 1000 rows, one transaction per flush', async (t) => {
    const ctx = await queueTables(t, 't_queue4');
    const entries = [];
    for (let i = 0; i < 2500; i += 1) entries.push({ t: 'append', table: 'q_log', row: { msg: `m${i}` } });
    entries.push({ t: 'save', table: 'q_items', row: { id: 'a', n: 1 } });
    entries.push({ t: 'save', table: 'q_pairs', row: { a: 'p', b: 1 } });
    entries.push({ t: 'save', table: 'q_items', row: { id: 'b', n: 2 } });
    entries.push({ t: 'save', table: 'q_items', row: { id: 'c', v: 'x' } });
    entries.push({ t: 'patch', table: 'q_items', key: 'a', changes: { v: 'p' } });
    ctx.enqueue(entries);
    const seen = await traced(ctx, () => ctx.flush());
    const flushStatements = seen.filter((s) => !/^SELECT|^RESET/.test(s));
    assert.equal(flushStatements.filter((s) => s === 'BEGIN').length, 1, 'one transaction');
    assert.equal(flushStatements.filter((s) => s === 'COMMIT').length, 1);
    assert.equal(flushStatements.filter((s) => s.startsWith('INSERT INTO "q_log"')).length, 3, '2500 rows → 1000 + 1000 + 500');
    const items = flushStatements.filter((s) => s.includes('"q_items"'));
    assert.equal(items.length, 3, 'saves {id,n} ×2 as one run, save {id,v} its own run, the patch merged into a');
    assert.equal((await ctx.sql('SELECT count(*)::int AS n FROM q_log'))[0].n, 2500);
    assert.deepEqual(await ctx.items(), [{ id: 'a', n: 1, v: 'p' }, { id: 'b', n: 2, v: null }, { id: 'c', n: 0, v: 'x' }]);
    const ids = await ctx.sql('SELECT min(id)::int AS lo, max(id)::int AS hi FROM q_log');
    assert.deepEqual(ids, [{ lo: 1, hi: 2500 }]);
    assert.equal((await ctx.sql("SELECT msg FROM q_log WHERE id = 1001"))[0].msg, 'm1000', 'append order is kept');
});

test('deferred FKs let a child be written before its parent across tables', async (t) => {
    const ctx = await queueTables(t, 't_queue5');
    ctx.enqueue([
        { t: 'save', table: 'q_child', row: { id: 'c1', parent: 'p1' } },
        { t: 'save', table: 'q_parent', row: { id: 'p1' } },
    ]);
    const seen = await traced(ctx, () => ctx.flush());
    const order = seen.filter((s) => s.startsWith('INSERT')).map((s) => /INSERT INTO "(\w+)"/.exec(s)[1]);
    assert.deepEqual(order, ['q_child', 'q_parent'], 'tables in order of first appearance');
    assert.deepEqual(await ctx.sql('SELECT id, parent FROM q_child'), [{ id: 'c1', parent: 'p1' }]);
    const [, status] = await ctx.call('status');
    assert.equal(status.queue.dropped, 0);
});

test('payload conversion: timestamps, jsonb, arrays, NULL markers, identity', async (t) => {
    const ctx = await queueTables(t, 't_queue6');
    ctx.enqueue([
        { t: 'save', table: 'q_items', row: { id: 'a', at: 1790000000.5, data: { k: [1, { z: NULL }], s: 'x' }, tags: ['a', NULL], v: NULL } },
        { t: 'save', table: 'q_items', row: { id: 'b', at: '2026-01-01T00:00:00Z', data: 'just a string' } },
        { t: 'append', table: 'q_log', row: { msg: 'auto id' } },
        { t: 'append', table: 'q_log', row: { id: 500, msg: 'explicit id' } },
        { t: 'sql', sql: 'UPDATE q_items SET v = $1 WHERE id = $2 AND $3::text IS NULL', params: ['by sql', 'b', NULL] },
    ]);
    await ctx.flush();
    const rows = await ctx.sql("SELECT id, extract(epoch from at)::float AS at, data, tags, v FROM q_items ORDER BY id");
    assert.deepEqual(rows, [
        { id: 'a', at: 1790000000.5, data: { k: [1, { z: null }], s: 'x' }, tags: ['a', null], v: null },
        { id: 'b', at: 1767225600, data: 'just a string', tags: null, v: 'by sql' },
    ]);
    assert.deepEqual(await ctx.sql('SELECT id::int, msg FROM q_log ORDER BY id'), [{ id: 1, msg: 'auto id' }, { id: 500, msg: 'explicit id' }]);
});
