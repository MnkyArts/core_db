// core_db/tests/catalog.test.mjs — introspection of the current schema, invalidation after DDL, miss reloads.
import test from 'node:test';
import assert from 'node:assert/strict';
import { setup, sleep } from './helpers.mjs';

test('catalog: tables, columns, primary keys of the CURRENT schema only', async (t) => {
    const ctx = await setup('t_catalog', { secondSchema: 't_catalog_other' });
    t.after(async () => {
        await ctx.sql('DROP SCHEMA IF EXISTS t_catalog_other CASCADE');
        await ctx.teardown();
    });
    await ctx.sql(`CREATE TABLE t_catalog.cat_a (
        k2 int, k1 text, v jsonb NOT NULL, stamps timestamptz[], d date, t timestamp,
        g int GENERATED ALWAYS AS (k2 * 2) STORED, i bigint GENERATED ALWAYS AS IDENTITY, need text NOT NULL,
        PRIMARY KEY (k1, k2))`);
    await ctx.sql('CREATE VIEW t_catalog.cat_view AS SELECT k1 FROM t_catalog.cat_a');
    await ctx.sql('DROP SCHEMA IF EXISTS t_catalog_other CASCADE');
    await ctx.sql('CREATE SCHEMA t_catalog_other');
    await ctx.sql('CREATE TABLE t_catalog_other.other_only (id int)');
    await ctx.internal.catalog.reload();
    const [, found] = await ctx.call('query', 'SELECT count(*) AS n FROM other_only', [], null);
    assert.equal(found[0].n, 0, 'the second schema is on the search path');

    const a = ctx.internal.catalog.table('cat_a');
    assert.deepEqual(a.pk, ['k1', 'k2']);
    assert.deepEqual(a.required, ['k2', 'k1', 'v', 'need']);
    assert.equal(a.columns.get('v').kind, 'json');
    assert.equal(a.columns.get('stamps').kind, 'array');
    assert.equal(a.columns.get('stamps').elemKind, 'timestamptz');
    assert.equal(a.columns.get('d').kind, 'date');
    assert.equal(a.columns.get('t').kind, 'timestamp');
    assert.equal(a.columns.get('g').generated, true);
    assert.equal(a.columns.get('i').identityAlways, true);
    assert.equal(ctx.internal.catalog.table('cat_view').view, true);
    assert.equal(ctx.internal.catalog.table('other_only'), null, 'tables of other schemas are invisible');
});

test('catalog: DDL through query reloads before answering; a miss reloads once', async (t) => {
    const ctx = await setup('t_catalog2');
    t.after(ctx.teardown);
    assert.equal(ctx.internal.catalog.table('cat_new'), null);
    const [err] = await ctx.call('query', '-- a comment first\nCREATE TABLE cat_new (id int PRIMARY KEY, v text)', [], null);
    assert.equal(err, null);
    assert.ok(ctx.internal.catalog.table('cat_new'), 'reloaded before the answer');
    assert.deepEqual(ctx.syncAs('core', 'enqueue', [{ t: 'save', table: 'cat_new', row: { id: 1, v: 'x' } }]), { seq: 1 });

    // Created behind core_db's back: the sync lookup misses (and schedules a reload), the async helper finds it.
    await ctx.sql('CREATE TABLE t_catalog2.cat_behind (id int PRIMARY KEY)');
    await sleep(1100);
    const [err2, rows] = await ctx.call('crud', 'select', 'cat_behind', {}, 0);
    assert.equal(err2, null);
    assert.deepEqual(rows, []);

    await ctx.sql('ALTER TABLE t_catalog2.cat_new ADD COLUMN extra int');
    const miss = ctx.syncAs('core', 'enqueue', [{ t: 'save', table: 'cat_new', row: { id: 2, extra: 1 } }]);
    assert.equal(miss.error, 'invalid: unknown column cat_new.extra');
    await ctx.internal.catalog.reload();
    assert.ok(ctx.syncAs('core', 'enqueue', [{ t: 'save', table: 'cat_new', row: { id: 2, extra: 1 } }]).seq);
});
