// core_db/tests/sqlgen.test.mjs — identifiers, WHERE ops, ORDER BY, helper SQL, conversions and the queue's
// bulk statements (§56.3.3). Pure: no database.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const sqlgen = require('../src/sqlgen.js');
const values = require('../src/values.js');
const { buildTables } = require('../src/catalog.js');

function col(tbl, name, typ, extra = {}) {
    return { tbl, relkind: 'r', col: name, typ, cat: typ.startsWith('_') ? 'A' : 'S', elem: typ.startsWith('_') ? typ.slice(1) : null,
        notnull: false, hasdef: false, ident: '', gen: '', pkpos: null, ...extra };
}

const tables = buildTables([
    col('things', 'id', 'text', { notnull: true, pkpos: 0 }),
    col('things', 'n', 'int4'),
    col('things', 'name', 'text', { notnull: true }),
    col('things', 'data', 'jsonb', { notnull: true, hasdef: true }),
    col('things', 'tags', '_text'),
    col('things', 'at', 'timestamptz'),
    col('things', 'day', 'date'),
    col('things', 'stamps', '_timestamptz'),
    col('things', 'twice', 'int4', { gen: 's', hasdef: true }),
    col('things', 'seq', 'int8', { notnull: true, ident: 'a' }),
    col('pairs', 'b', 'int4', { notnull: true, pkpos: 1 }),
    col('pairs', 'a', 'text', { notnull: true, pkpos: 0 }),
    col('pairs', 'v', 'text'),
]);
const things = tables.get('things');
const pairs = tables.get('pairs');

test('catalog meta: pk order, required, kinds', () => {
    assert.deepEqual(pairs.pk, ['a', 'b']);
    assert.deepEqual(things.pk, ['id']);
    assert.deepEqual(things.required, ['id', 'name']);
    assert.equal(things.columns.get('data').kind, 'json');
    assert.equal(things.columns.get('tags').kind, 'array');
    assert.equal(things.columns.get('stamps').elemKind, 'timestamptz');
    assert.equal(things.columns.get('seq').identityAlways, true);
    assert.equal(things.columns.get('seq').hasDefault, true);
    assert.equal(things.columns.get('twice').generated, true);
});

test('identifiers: shape, catalog, quoting', () => {
    for (const bad of ['Things', 'a;drop table x', '1abc', '', 'a-b', 'a b', null, 5, '"x"']) {
        assert.throws(() => sqlgen.checkTableName(bad), /^Error: invalid: table name/);
    }
    assert.equal(sqlgen.checkTableName('ok_name_1'), 'ok_name_1');
    assert.equal(sqlgen.quoteIdent('we"ird'), '"we""ird"');
    assert.throws(() => sqlgen.column(things, 'nope'), /invalid: unknown column things.nope/);
    assert.throws(() => sqlgen.column(things, '1; DROP'), /invalid: unknown column/);
    assert.throws(() => sqlgen.buildInsert(things, { twice: 1 }), /is generated/);
});

test('where: values, NULL, lists and every operator', () => {
    const params = [];
    const sql = sqlgen.buildWhere(things, {
        id: 'x', name: { __null: true }, n: [1, 2], tags: ['a'], at: { __op: '>=', value: 0 },
    }, params);
    assert.equal(sql, ' WHERE "id" = $1 AND "name" IS NULL AND "n" = ANY($2) AND "tags" = $3 AND "at" >= $4');
    assert.deepEqual(params, ['x', [1, 2], ['a'], '1970-01-01T00:00:00.000Z']);

    const ops = [
        [{ __op: '=', value: 1 }, '"n" = $1', [1]],
        [{ __op: '=', value: { __null: true } }, '"n" IS NULL', []],
        [{ __op: '<>', value: { __null: true } }, '"n" IS NOT NULL', []],
        [{ __op: '<>', value: 1 }, '"n" <> $1', [1]],
        [{ __op: '<', value: 1 }, '"n" < $1', [1]],
        [{ __op: '<=', value: 1 }, '"n" <= $1', [1]],
        [{ __op: '>', value: 1 }, '"n" > $1', [1]],
        [{ __op: 'in', value: [1, 2] }, '"n" = ANY($1)', [[1, 2]]],
        [{ __op: 'in', value: [] }, 'FALSE', []],
        [{ __op: 'not_in', value: [3] }, '"n" <> ALL($1)', [[3]]],
        [{ __op: 'not_in', value: [] }, 'TRUE', []],
        [{ __op: 'between', value: 1, value2: 5 }, '"n" BETWEEN $1 AND $2', [1, 5]],
        [{ __op: 'is_null' }, '"n" IS NULL', []],
        [{ __op: 'not_null' }, '"n" IS NOT NULL', []],
    ];
    for (const [cond, expected, expectedParams] of ops) {
        const p = [];
        assert.equal(sqlgen.buildWhere(things, { n: cond }, p), ` WHERE ${expected}`, JSON.stringify(cond));
        assert.deepEqual(p, expectedParams, JSON.stringify(cond));
    }
    const p = [];
    assert.equal(sqlgen.buildWhere(things, {
        name: { __op: 'like', value: 'a%' }, id: { __op: 'ilike', value: 'B%' },
    }, p), ' WHERE "name" LIKE $1 AND "id" ILIKE $2');
    const p2 = [];
    assert.equal(sqlgen.buildWhere(things, { name: { __op: 'not_like', value: 'x' }, tags: { __op: 'contains', value: ['a'] }, data: { __op: 'contains', value: { k: 1 } } }, p2),
        ' WHERE "name" NOT LIKE $1 AND "tags" @> $2 AND "data" @> $3');
    assert.deepEqual(p2, ['x', ['a'], '{"k":1}']);
    const p3 = [];
    assert.equal(sqlgen.buildWhere(things, { tags: { __op: 'overlaps', value: ['a', 'b'] } }, p3), ' WHERE "tags" && $1');
    const p4 = [];
    assert.equal(sqlgen.buildWhere(things, { n: [] }, p4), ' WHERE FALSE');
    assert.equal(sqlgen.buildWhere(things, [], []), '', 'an empty Lua table is an empty map');
    assert.equal(sqlgen.buildWhere(things, null, []), '');

    assert.throws(() => sqlgen.buildWhere(things, { n: { __op: 'drop' } }, []), /unknown operator/);
    assert.throws(() => sqlgen.buildWhere(things, { n: { __op: '<' } }, []), /needs a value/);
    assert.throws(() => sqlgen.buildWhere(things, { n: { __op: 'between', value: 1 } }, []), /needs two values/);
    assert.throws(() => sqlgen.buildWhere(things, { name: { __op: 'like', value: 5 } }, []), /needs a string/);
    assert.throws(() => sqlgen.buildWhere(things, { n: { a: 1 } }, []), /must be a value/);
    assert.throws(() => sqlgen.buildWhere(things, { tags: { __op: 'in', value: [['a']] } }, []), /not supported on an array column/);
    assert.throws(() => sqlgen.buildWhere(things, 'id = 1', []), /where must be a map/);
    assert.throws(() => sqlgen.buildWhere(things, { 'id = 1 OR 1': 1 }, []), /unknown column/);
});

test('orderBy, columns, limit/offset', () => {
    assert.equal(sqlgen.buildOrderBy(things, 'n DESC, name asc nulls last, id'), ' ORDER BY "n" DESC, "name" ASC NULLS LAST, "id"');
    assert.equal(sqlgen.buildOrderBy(things, ['n', 'id desc']), ' ORDER BY "n", "id" DESC');
    assert.throws(() => sqlgen.buildOrderBy(things, 'n; DROP TABLE x'), /invalid: orderBy/);
    assert.throws(() => sqlgen.buildOrderBy(things, 'random()'), /invalid: orderBy/);
    assert.throws(() => sqlgen.buildOrderBy(things, 'nope'), /unknown column/);
    const s = sqlgen.buildSelect(things, { id: 'a' }, { columns: ['id', 'n'], orderBy: 'n', limit: 5, offset: 10 });
    assert.equal(s.text, 'SELECT "id", "n" FROM "things" WHERE "id" = $1 ORDER BY "n" LIMIT $2 OFFSET $3');
    assert.deepEqual(s.values, ['a', 5, 10]);
    assert.equal(sqlgen.buildSelect(things, null, {}, true).text, 'SELECT * FROM "things" LIMIT 1');
    assert.throws(() => sqlgen.buildSelect(things, null, { limit: -1 }), /limit must be a non-negative integer/);
    assert.throws(() => sqlgen.buildSelect(things, null, { limit: 1.5 }), /limit/);
    assert.throws(() => sqlgen.buildSelect(things, null, { columns: ['id', 'x'] }), /unknown column things.x/);
});

test('helper statements', () => {
    const ins = sqlgen.buildInsert(things, { id: 'a', data: { x: [] }, at: 1.5, tags: ['t'], name: { __null: true } }, {});
    assert.equal(ins.text, 'INSERT INTO "things" ("id", "data", "at", "tags", "name") VALUES ($1, $2, $3, $4, $5) RETURNING *');
    assert.deepEqual(ins.values, ['a', '{"x":[]}', '1970-01-01T00:00:01.500Z', ['t'], null]);
    assert.equal(sqlgen.buildInsert(things, [], { returning: false }).text, 'INSERT INTO "things" DEFAULT VALUES');
    assert.equal(sqlgen.buildInsert(things, { id: 'a' }, { returning: ['id'] }).text, 'INSERT INTO "things" ("id") VALUES ($1) RETURNING "id"');
    assert.match(sqlgen.buildInsert(things, { seq: 5 }, {}).text, /OVERRIDING SYSTEM VALUE/);

    const many = sqlgen.buildInsertMany(things, [{ id: 'a', n: 1 }, { id: 'b', name: 'x' }]);
    assert.equal(many.length, 1);
    assert.equal(many[0].text, 'INSERT INTO "things" ("id", "n", "name") VALUES ($1, $2, DEFAULT), ($3, DEFAULT, $4)');
    assert.deepEqual(many[0].values, ['a', 1, 'b', 'x']);
    assert.equal(sqlgen.buildInsertMany(things, Array.from({ length: 2500 }, (_, i) => ({ id: `${i}` }))).length, 3, '<= 1000 rows each');

    const up = sqlgen.buildUpdate(things, { n: 2, at: '2026-01-01T00:00:00Z' }, { id: 'a' });
    assert.equal(up.text, 'UPDATE "things" SET "n" = $1, "at" = $2 WHERE "id" = $3');
    assert.throws(() => sqlgen.buildUpdate(things, { n: 2 }, []), /update needs a non-empty where/);
    assert.throws(() => sqlgen.buildUpdate(things, [], { id: 'a' }), /at least one column/);
    assert.throws(() => sqlgen.buildDelete(things, {}), /delete needs a non-empty where/);
    assert.equal(sqlgen.buildDelete(things, { id: 'a' }).text, 'DELETE FROM "things" WHERE "id" = $1');
    assert.equal(sqlgen.buildCount(things, { n: 1 }).text, 'SELECT count(*) AS n FROM "things" WHERE "n" = $1');

    const ups = sqlgen.buildUpsert(things, { id: 'a', n: 1, name: 'x' }, 'id', {});
    assert.equal(ups.text, 'INSERT INTO "things" ("id", "n", "name") VALUES ($1, $2, $3) ON CONFLICT ("id") DO UPDATE SET "n" = EXCLUDED."n", "name" = EXCLUDED."name" RETURNING *');
    assert.equal(ups.fallback, null);
    const only = sqlgen.buildUpsert(pairs, { a: 'x', b: 1 }, ['a', 'b'], { returning: ['v'] });
    assert.match(only.text, /DO NOTHING RETURNING "v"$/);
    assert.equal(only.fallback.text, 'SELECT "v" FROM "pairs" WHERE "a" = $1 AND "b" = $2');
    assert.match(sqlgen.buildUpsert(things, { id: 'a', n: 1, name: 'x' }, 'id', { update: ['n'] }).text, /DO UPDATE SET "n" = EXCLUDED."n" RETURNING/);
    assert.throws(() => sqlgen.buildUpsert(things, { n: 1 }, 'id', {}), /conflict column things.id is missing/);
    assert.throws(() => sqlgen.buildUpsert(things, { id: 'a' }, [], {}), /upsert needs conflict/);
});

test('value conversion (§56.2.4)', () => {
    const at = things.columns.get('at');
    assert.equal(values.toParam(at, 1790000000), '2026-09-21T14:13:20.000Z');
    assert.equal(values.toParam(at, 1790000000.25), '2026-09-21T14:13:20.250Z');
    assert.equal(values.toParam(at, '2026-01-01T00:00:00Z'), '2026-01-01T00:00:00Z');
    assert.throws(() => values.toParam(at, true, 'things'), /invalid: things.at needs Unix seconds/);
    assert.equal(values.toParam(things.columns.get('day'), 0), '1970-01-01');
    assert.equal(values.toParam(things.columns.get('data'), 'text'), '"text"');
    assert.equal(values.toParam(things.columns.get('data'), { a: { __null: true } }), '{"a":null}');
    assert.throws(() => values.toParam(things.columns.get('tags'), { a: 1 }), /needs a list/);
    assert.deepEqual(values.toParam(things.columns.get('stamps'), [0, { __null: true }]), ['1970-01-01T00:00:00.000Z', null]);
    assert.equal(values.toParam(things.columns.get('n'), { __null: true }), null);
    assert.deepEqual(values.toJson(things.columns.get('stamps'), [1]), ['1970-01-01T00:00:01.000Z']);
    assert.deepEqual(values.toJson(things.columns.get('data'), { a: 1 }), { a: 1 });
    assert.deepEqual(values.rawParams([{ __null: true }, [1, { __null: true }], { a: 1 }, 'x']), [null, [1, null], '{"a":1}', 'x']);
    assert.deepEqual(values.rawParams(null), []);
    assert.deepEqual(values.rawParams({ 2: 'b' }), [null, 'b']);
    assert.throws(() => values.rawParams({ a: 1 }), /params must be a list/);
});

function entry(t, table, pk, data, extra = {}) {
    const cols = Object.keys(data).sort();
    return { t, table, pk, data, cols, sig: `${t}|${cols.join(',')}`, override: false, owner: 'core', ...extra };
}

test('bulk statements: barriers, per-table grouping, runs, chunking (§56.3.3)', () => {
    const e = [
        entry('save', 'things', ['id'], { id: 'a', name: 'x' }),
        entry('save', 'pairs', ['a', 'b'], { a: 'p', b: 1, v: 'v' }),
        entry('save', 'things', ['id'], { id: 'b', name: 'y' }),
        entry('patch', 'things', ['id'], { id: 'c', n: 1 }),
        entry('save', 'things', ['id'], { id: 'd', name: 'z' }),
        { t: 'sql', sql: 'SELECT 1', params: [], owner: 'core', table: null },
        entry('remove', 'pairs', ['a', 'b'], { a: 'p', b: 2 }),
        entry('append', 'things', ['id'], { id: 'e', name: 'w' }),
        entry('save', 'pairs', ['a', 'b'], { a: 'p', b: 3 }),
    ];
    const out = sqlgen.buildStatements(e);
    assert.deepEqual(out.map((s) => s.entries.length), [2, 1, 1, 1, 1, 1, 1, 1]);
    assert.equal(out[0].text, 'INSERT INTO "things" ("id", "name") SELECT "id", "name" FROM jsonb_populate_recordset(NULL::"things", $1::jsonb) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"');
    assert.deepEqual(JSON.parse(out[0].values[0]), [{ id: 'a', name: 'x' }, { id: 'b', name: 'y' }]);
    assert.equal(out[1].text, 'UPDATE "things" AS x SET "n" = v."n" FROM jsonb_populate_recordset(NULL::"things", $1::jsonb) AS v WHERE x."id" = v."id"');
    assert.equal(out[2].entries[0].data.id, 'd', 'a new run after the patch');
    assert.match(out[3].text, /^INSERT INTO "pairs" \("a", "b", "v"\).*ON CONFLICT \("a", "b"\) DO UPDATE SET "v" = EXCLUDED."v"$/);
    assert.equal(out[4].text, 'SELECT 1', 'sql entries are barriers in their position');
    assert.equal(out[5].text, 'DELETE FROM "pairs" AS x USING jsonb_populate_recordset(NULL::"pairs", $1::jsonb) AS v WHERE x."a" = v."a" AND x."b" = v."b"');
    assert.match(out[6].text, /ON CONFLICT \("a", "b"\) DO NOTHING$/, 'only pk columns: DO NOTHING');
    assert.equal(out[7].text, 'INSERT INTO "things" ("id", "name") SELECT "id", "name" FROM jsonb_populate_recordset(NULL::"things", $1::jsonb)');

    const many = Array.from({ length: 2345 }, (_, i) => entry('append', 'things', ['id'], { id: `${i}` }));
    assert.deepEqual(sqlgen.buildStatements(many).map((s) => s.entries.length), [1000, 1000, 345]);
    const marker = sqlgen.buildStatements([entry('save', 'things', ['id'], { id: 'a', name: { __null: true }, data: { k: { __null: true } } })]);
    assert.deepEqual(JSON.parse(marker[0].values[0]), [{ id: 'a', name: null, data: { k: null } }]);
});
