// core_db/tests/migrate_retry.test.mjs — review R1 H2/H3/L2 for the migration runner: a connection-class failure
// keeps the barrier PENDING and retries; entries keep the barrier they were queued under; a migration waits for
// locks only `core_db_migration_lock_timeout_ms` and then retries; a migration's session SET never leaks.
import test from 'node:test';
import assert from 'node:assert/strict';
import { queueTables, sleep, eventsNamed, pg, BASE_URL } from './helpers.mjs';

test('H2: the database blips while core re-registers — the barrier stays pending, nothing is dropped', async (t) => {
    const ctx = await queueTables(t, 't_mretry1', { core: { retryFirstMs: 100 } });
    ctx.syncAs('core', 'migrate', [{ version: 1, sql: 'SELECT 1' }]);
    assert.deepEqual(await ctx.callAs('core', 'awaitMigrations', 5000), [null]);
    // `restart core`: its final saves are queued, then the database refuses connections while core starts again
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'final_money', n: 500 } }], 'core');
    ctx.api.testFail('.', 'ECONNREFUSED');
    await ctx.internal.queue.flushNow();
    ctx.syncAs('core', 'migrate', [{ version: 1, sql: 'SELECT 1' }]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'after_restart', n: 1 } }], 'core');
    assert.deepEqual(await ctx.callAs('core', 'awaitMigrations', 300), ['timeout'], 'still pending, not failed');
    assert.equal((await ctx.call('status'))[1].migrations.core.state, 'pending');
    assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'warn' && /^migrations of core wait for the database \(ECONNREFUSED/.test(m)));

    ctx.api.testFail(null); // the database is back: the job retries by itself
    assert.deepEqual(await ctx.callAs('core', 'awaitMigrations', 5000), [null]);
    await ctx.flush();
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['after_restart', 'final_money']);
    const [, st] = await ctx.call('status');
    assert.equal(st.queue.dropped, 0);
    assert.deepEqual(eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed'), []);
    assert.deepEqual(await ctx.callAs('core', 'query', 'SELECT 1 AS one', [], null), [null, [{ one: 1 }], 1]);
});

test('H2: entries keep the barrier they were queued under — a failing re-registration drops only its own', async (t) => {
    const ctx = await queueTables(t, 't_mretry2');
    ctx.syncAs('plug', 'migrate', [{ version: 1, sql: 'SELECT 1' }]);
    await ctx.callAs('plug', 'awaitMigrations', 5000);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'before', n: 1 } }], 'plug');
    ctx.syncAs('plug', 'migrate', [{ version: 1, sql: 'SELECT 1' }, { version: 2, name: 'broken', sql: 'SELEC 1' }]);
    ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: 'after', n: 1 } }], 'plug');
    assert.match((await ctx.callAs('plug', 'awaitMigrations', 5000))[0], /^migrations_failed: plug v2 broken: 42601 /);
    await ctx.flush();
    assert.deepEqual((await ctx.items()).map((r) => r.id), ['before'], 'the entry of the older registration flushed');
    await sleep(5);
    const failed = eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed');
    assert.equal(failed.length, 1);
    assert.equal(failed[0][4], 'after');
});

test('H3: a migration waits for a lock at most core_db_migration_lock_timeout_ms, then retries', async (t) => {
    const ctx = await queueTables(t, 't_mretry3', { core: { migrationLockTimeoutMs: 150, retryFirstMs: 100 } });
    const holder = new pg.Client(BASE_URL);
    await holder.connect();
    await holder.query('SET search_path TO t_mretry3, public');
    await holder.query('BEGIN');
    await holder.query('LOCK TABLE q_pairs IN ACCESS SHARE MODE');
    try {
        ctx.syncAs('plug', 'migrate', [{ version: 1, name: 'alter', sql: 'ALTER TABLE q_pairs ADD COLUMN extra int' }]);
        await sleep(400);
        assert.equal((await ctx.call('status'))[1].migrations.plug.state, 'pending');
        assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'warn' && /^migrations of plug \(v1 alter\) wait for the database \(55P03 /.test(m)));
    } finally {
        await holder.query('ROLLBACK');
        await holder.end();
    }
    assert.deepEqual(await ctx.callAs('plug', 'awaitMigrations', 5000), [null]);
    assert.ok(ctx.internal.catalog.table('q_pairs').columns.has('extra'));
});

test('L2: a SET inside a migration does not leak into the next user of its pooled client', async (t) => {
    const ctx = await queueTables(t, 't_mretry4');
    ctx.syncAs('plug', 'migrate', [{ version: 1, name: 'dirty', sql: "SET search_path TO pg_catalog; SET application_name = 'dirty'; SELECT 1" }]);
    assert.deepEqual(await ctx.callAs('plug', 'awaitMigrations', 5000), [null]);
    for (let i = 0; i < 3; i += 1) { // the flush runs on the same (system) pool, most recently released client first
        ctx.enqueue([{ t: 'save', table: 'q_items', row: { id: `r${i}`, n: 1 } }]);
        await ctx.flush();
    }
    assert.equal((await ctx.items()).length, 3);
    assert.equal((await ctx.call('status'))[1].queue.dropped, 0);
});

test('addendum: a lock timeout while taking the migration lock is retried — the barrier never fails for it', async (t) => {
    const ctx = await queueTables(t, 't_mretry5', { core: { retryFirstMs: 50 } });
    ctx.api.testFail('pg_advisory_xact_lock', '55P03 canceling statement due to lock timeout');
    ctx.syncAs('plug', 'migrate', [{ version: 1, sql: 'SELECT 1' }]);
    await sleep(1000); // 50, 100, 200, 400 ms … far more than MAX_RETRYABLE attempts for other retryable errors
    assert.equal((await ctx.call('status'))[1].migrations.plug.state, 'pending');
    ctx.api.testFail(null);
    assert.deepEqual(await ctx.callAs('plug', 'awaitMigrations', 5000), [null]);
});

test('addendum: six cores in six schemas migrate core\'s real schema at the same time', async (t) => {
    const path = await import('node:path');
    const { fileURLToPath } = await import('node:url');
    const { setup } = await import('./helpers.mjs');
    const coreDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'core');
    const ctxs = await Promise.all([1, 2, 3, 4, 5, 6].map((i) => setup(`t_mretry_many${i}`, { core: { migrationLockTimeoutMs: 1000, retryFirstMs: 100 } })));
    t.after(() => Promise.all(ctxs.map((c) => c.teardown())));
    for (const c of ctxs) {
        c.api.testMapResource('core', coreDir);
        assert.deepEqual(c.syncAs('core', 'migrate', ['sql/0001_core_schema.sql']), { ok: true });
    }
    const answers = await Promise.all(ctxs.map((c) => c.callAs('core', 'awaitMigrations', 60000)));
    assert.deepEqual(answers, ctxs.map(() => [null]));
    for (const c of ctxs) {
        assert.ok(c.internal.catalog.table('accounts'), 'every schema has core\'s tables');
        assert.deepEqual(await c.call('nextId', 'x'), [null, 1]);
    }
});
