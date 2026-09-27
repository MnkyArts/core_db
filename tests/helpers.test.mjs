// core_db/tests/helpers.test.mjs — the table helpers behind `crud` (§56.5.3), batch, nextId, read-your-writes.
import test from 'node:test';
import assert from 'node:assert/strict';
import { setup } from './helpers.mjs';

const NULL = { __null: true };
const op = (name, value, value2) => ({ __op: name, value, value2 });

async function withTables(t, schema) {
    const ctx = await setup(schema);
    t.after(ctx.teardown);
    const [err] = await ctx.call('batch', [
        { sql: `CREATE TABLE h_items (
            id text PRIMARY KEY, owner text, n int NOT NULL DEFAULT 0, tags text[] NOT NULL DEFAULT '{}',
            data jsonb NOT NULL DEFAULT '{}', at timestamptz, day date, created_at timestamptz NOT NULL DEFAULT now())` },
        { sql: 'CREATE TABLE h_log (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, msg text NOT NULL)' },
        { sql: 'CREATE TABLE h_pairs (a text, b int, v text, PRIMARY KEY (a, b))' },
        { sql: 'CREATE TABLE core_counters (name text PRIMARY KEY, value bigint NOT NULL DEFAULT 0)' },
    ], null);
    assert.equal(err, null);
    return ctx;
}

test('crud: insert / insertMany with conversions, defaults, returning', async (t) => {
    const ctx = await withTables(t, 't_helpers1');
    const crud = (...args) => ctx.call('crud', ...args);

    const [err, row] = await crud('insert', 'h_items', {
        values: { id: 'a', tags: ['x', 'y'], data: { k: [1, 2], z: NULL }, at: 1790000000.75, day: '2026-01-02', owner: NULL },
    }, 0);
    assert.equal(err, null);
    assert.equal(row.id, 'a');
    assert.equal(row.n, 0, 'column default');
    assert.deepEqual(row.tags, ['x', 'y']);
    assert.deepEqual(row.data, { k: [1, 2], z: null });
    assert.equal(row.at, 1790000000, 'Unix seconds in, integer seconds out');
    assert.equal(row.day, '2026-01-02');
    assert.equal(row.owner, null);
    assert.equal(typeof row.created_at, 'number');

    assert.deepEqual(await crud('insert', 'h_items', { values: { id: 'b' }, opts: { returning: false } }, 0), [null, true]);
    assert.deepEqual(await crud('insert', 'h_items', { values: { id: 'c' }, returning: ['id', 'n'] }, null), [null, { id: 'c', n: 0 }]);
    assert.deepEqual(await crud('insert', 'h_items', [{ id: 'd' }, { returning: false }], 0), [null, true], 'positional args');
    const [, logRow] = await crud('insert', 'h_log', { values: { msg: 'hi' } }, 0);
    assert.equal(logRow.id, 1);

    const [e2, n] = await crud('insertMany', 'h_items', { rows: [{ id: 'm1', n: 5 }, { id: 'm2', owner: 'o' }, { id: 'm3' }] }, 0);
    assert.equal(e2, null);
    assert.equal(n, 3);
    const got = await ctx.sql("SELECT id, n, owner FROM h_items WHERE id LIKE 'm%' ORDER BY id");
    assert.deepEqual(got, [{ id: 'm1', n: 5, owner: null }, { id: 'm2', n: 0, owner: 'o' }, { id: 'm3', n: 0, owner: null }], 'missing keys take DEFAULT, not NULL');
    assert.deepEqual(await crud('insertMany', 'h_items', { rows: [] }, 0), [null, 0]);
    const big = Array.from({ length: 2100 }, (_, i) => ({ msg: `m${i}` }));
    assert.deepEqual(await crud('insertMany', 'h_log', { rows: big }, 0), [null, 2100]);

    const [e3] = await crud('insertMany', 'h_items', { rows: [{ id: 'dup' }, { id: 'dup' }] }, 0);
    assert.match(e3, /^23505 /);
    assert.equal((await ctx.sql("SELECT count(*)::int AS n FROM h_items WHERE id = 'dup'"))[0].n, 0);
});

test('crud: select / first / count with where, ops, order, paging', async (t) => {
    const ctx = await withTables(t, 't_helpers2');
    const crud = (...args) => ctx.call('crud', ...args);
    await crud('insertMany', 'h_items', { rows: [
        { id: 'a', n: 1, owner: 'x', tags: ['red'], at: 100 },
        { id: 'b', n: 2, owner: 'y', tags: ['red', 'blue'], at: 200 },
        { id: 'c', n: 3, tags: ['green'], at: 300, data: { kind: 'car', seats: 2 } },
    ] }, 0);

    const ids = async (where, opts) => (await crud('select', 'h_items', { where, opts: { orderBy: 'id', ...opts } }, 0))[1].map((r) => r.id);
    assert.deepEqual(await ids({}), ['a', 'b', 'c']);
    assert.deepEqual(await ids({ owner: 'x' }), ['a']);
    assert.deepEqual(await ids({ owner: NULL }), ['c']);
    assert.deepEqual(await ids({ n: [1, 3] }), ['a', 'c']);
    assert.deepEqual(await ids({ n: op('>', 1) }), ['b', 'c']);
    assert.deepEqual(await ids({ n: op('between', 2, 3) }), ['b', 'c']);
    assert.deepEqual(await ids({ n: op('not_in', [2]) }), ['a', 'c']);
    assert.deepEqual(await ids({ owner: op('not_null') }), ['a', 'b']);
    assert.deepEqual(await ids({ id: op('like', '%b%') }), ['b']);
    assert.deepEqual(await ids({ id: op('ilike', 'B') }), ['b']);
    assert.deepEqual(await ids({ tags: op('contains', ['red']) }), ['a', 'b']);
    assert.deepEqual(await ids({ tags: op('overlaps', ['blue', 'green']) }), ['b', 'c']);
    assert.deepEqual(await ids({ data: op('contains', { kind: 'car' }) }), ['c']);
    assert.deepEqual(await ids({ at: op('>=', 200) }), ['b', 'c'], 'Unix seconds compare with timestamptz');
    assert.deepEqual(await ids({ n: [] }), []);
    assert.deepEqual(await ids({}, { orderBy: 'n DESC', limit: 2, offset: 1 }), ['b', 'a']);

    const [, rows] = await crud('select', 'h_items', { where: { id: 'a' }, columns: ['id', 'n'] }, 0);
    assert.deepEqual(rows, [{ id: 'a', n: 1 }]);
    assert.deepEqual(await crud('first', 'h_items', { where: { id: 'zz' } }, 0), [null, null]);
    assert.equal((await crud('first', 'h_items', [{ owner: 'y' }], 0))[1].id, 'b');
    assert.deepEqual(await crud('count', 'h_items', { where: { tags: op('contains', ['red']) } }, 0), [null, 2]);
    assert.deepEqual(await crud('count', 'h_items', [], 0), [null, 3]);
});

test('crud: update / delete / upsert', async (t) => {
    const ctx = await withTables(t, 't_helpers3');
    const crud = (...args) => ctx.call('crud', ...args);
    await crud('insertMany', 'h_items', { rows: [{ id: 'a' }, { id: 'b' }, { id: 'c' }] }, 0);

    assert.deepEqual(await crud('update', 'h_items', { set: { n: 7, owner: 'z' }, where: { id: ['a', 'b'] } }, 0), [null, 2]);
    assert.deepEqual(await crud('update', 'h_items', [{ owner: NULL }, { id: 'a' }], 0), [null, 1], 'positional');
    assert.deepEqual(await crud('update', 'h_items', { set: { n: 1 }, where: {} }, 0), ['invalid: update needs a non-empty where']);
    assert.deepEqual(await crud('delete', 'h_items', { where: { id: 'c' } }, 0), [null, 1]);
    assert.deepEqual(await crud('delete', 'h_items', {}, 0), ['invalid: delete needs a non-empty where']);
    assert.deepEqual(await ctx.sql('SELECT id, n, owner FROM h_items ORDER BY id'), [{ id: 'a', n: 7, owner: null }, { id: 'b', n: 7, owner: 'z' }]);

    const [e1, r1] = await crud('upsert', 'h_pairs', { values: { a: 'x', b: 1, v: 'one' }, conflict: ['a', 'b'] }, 0);
    assert.equal(e1, null);
    assert.deepEqual(r1, { a: 'x', b: 1, v: 'one' });
    const [, r2] = await crud('upsert', 'h_pairs', { values: { a: 'x', b: 1, v: 'two' }, conflict: ['a', 'b'] }, 0);
    assert.equal(r2.v, 'two');
    const [, r3] = await crud('upsert', 'h_pairs', { values: { a: 'x', b: 1, v: 'three' }, conflict: ['a', 'b'], opts: { update: [] } }, 0);
    assert.equal(r3.v, 'two', 'update = {} → DO NOTHING, the existing row comes back');
    assert.deepEqual(await crud('upsert', 'h_items', [{ id: 'a', n: 9, owner: 'q' }, 'id', { update: ['n'], returning: false }], 0), [null, true]);
    assert.deepEqual(await ctx.sql("SELECT n, owner FROM h_items WHERE id = 'a'"), [{ n: 9, owner: null }]);
});

test('crud: refusals', async (t) => {
    const ctx = await withTables(t, 't_helpers4');
    const crud = (...args) => ctx.call('crud', ...args);
    assert.deepEqual(await crud('select', 'no_such', {}, 0), ['invalid: unknown table no_such']);
    assert.match((await crud('select', 'H_items', {}, 0))[0], /^invalid: table name/);
    assert.match((await crud('select', 'h_items; DROP TABLE h_items', {}, 0))[0], /^invalid: table name/);
    assert.deepEqual(await crud('insert', 'h_items', { values: { id: 'a', nope: 1 } }, 0), ['invalid: unknown column h_items.nope']);
    assert.deepEqual(await crud('select', 'h_items', { where: { 'id = id OR TRUE': 1 } }, 0), ['invalid: unknown column h_items.id = id OR TRUE']);
    assert.match((await crud('select', 'h_items', { license: 'x' }, 0))[0], /^invalid: crud args key "license"/);
    assert.match((await crud('explode', 'h_items', {}, 0))[0], /^invalid: unknown crud op/);
    assert.match((await crud('insert', 'h_items', { values: { id: 'a', at: 'not a time' } }, 0))[0], /^22007 /);
    assert.equal((await ctx.sql('SELECT count(*)::int AS n FROM h_items'))[0].n, 0);
});

test('batch, nextId, read-your-writes (opts.sync)', async (t) => {
    const ctx = await withTables(t, 't_helpers5');
    const [e1, results] = await ctx.call('batch', [
        { sql: "INSERT INTO h_items (id) VALUES ('x')" },
        ['UPDATE h_items SET n = $1 WHERE id = $2 RETURNING n', [5, 'x']],
    ], { rows: true });
    assert.equal(e1, null);
    assert.deepEqual(results, [{ rowCount: 1, rows: [] }, { rowCount: 1, rows: [{ n: 5 }] }]);
    assert.deepEqual((await ctx.call('batch', [{ sql: 'SELECT 1' }], null))[1], [{ rowCount: 1 }]);
    const [e2] = await ctx.call('batch', [
        { sql: "INSERT INTO h_items (id) VALUES ('y')" },
        { sql: "INSERT INTO h_items (id) VALUES ('x')" },
    ], null);
    assert.match(e2, /^23505 /);
    assert.equal((await ctx.sql("SELECT count(*)::int AS n FROM h_items WHERE id = 'y'"))[0].n, 0, 'one transaction: all or nothing');
    assert.deepEqual(await ctx.call('batch', [], null), [null, []]);

    assert.deepEqual(await ctx.call('nextId', 'things'), [null, 1]);
    assert.deepEqual(await ctx.call('nextId', 'things'), [null, 2]);
    assert.deepEqual(await ctx.call('nextId', 'other'), [null, 1]);
    assert.deepEqual(await ctx.call('nextId', ''), ['invalid: nextId needs a counter name']);

    assert.deepEqual(ctx.syncAs('core', 'enqueue', [{ t: 'save', table: 'h_items', row: { id: 'q', n: 3 } }]), { seq: 1 });
    const [e3, rows] = await ctx.call('crud', 'first', 'h_items', { where: { id: 'q' }, opts: { sync: true } }, 0);
    assert.equal(e3, null);
    assert.equal(rows.n, 3, 'sync = true waits for the queued save');
    ctx.syncAs('core', 'enqueue', [{ t: 'patch', table: 'h_items', key: 'q', changes: { n: 4 } }]);
    const [, qrows] = await ctx.call('query', 'SELECT n FROM h_items WHERE id = $1', ['q'], { sync: true });
    assert.equal(qrows[0].n, 4);
});
