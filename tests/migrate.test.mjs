// core_db/tests/migrate.test.mjs — the migration runner (§56.4): order, checksum, barrier, failure, advisory
// lock, out-of-order, prefix warning, validation, and core's real 0001 schema through the runner.
import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { setup, pg, BASE_URL, sleep, eventsNamed } from './helpers.mjs';

const RESOURCES = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const md5 = (s) => crypto.createHash('md5').update(s, 'utf8').digest('hex');
const warns = (ctx, re) => ctx.fivem.logs.filter(([l, m]) => l === 'warn' && re.test(m));

test('migrate: order, barrier, idempotence, checksum warning, status', async (t) => {
    const ctx = await setup('t_migrate1', { files: {
        'plug/sql/0001_init.sql': 'CREATE TABLE plug_things (id int PRIMARY KEY); SELECT pg_sleep(0.2);',
    } });
    t.after(ctx.teardown);
    const list = ['sql/0001_init.sql', { version: 2, name: 'more', sql: 'ALTER TABLE plug_things ADD COLUMN v text' }];
    assert.deepEqual(ctx.syncAs('plug', 'migrate', list), { ok: true });

    // The owner's calls wait for its barrier; another owner is not held.
    const started = Date.now();
    const [err, rows] = await ctx.callAs('plug', 'query', 'SELECT id, v FROM plug_things', [], null);
    assert.equal(err, null, 'the query ran after the migrations');
    assert.deepEqual(rows, []);
    assert.ok(Date.now() - started >= 150);
    assert.deepEqual(await ctx.callAs('plug', 'awaitMigrations', 1000), [null]);
    assert.deepEqual(await ctx.callAs('nobody', 'awaitMigrations', 1000), [null], 'no barrier → at once');

    const applied = await ctx.sql('SELECT owner, version, name, checksum, duration_ms FROM core_migrations ORDER BY version');
    assert.deepEqual(applied.map((r) => [r.owner, r.version, r.name, r.checksum]), [
        ['plug', 1, 'init', md5('CREATE TABLE plug_things (id int PRIMARY KEY); SELECT pg_sleep(0.2);')],
        ['plug', 2, 'more', md5('ALTER TABLE plug_things ADD COLUMN v text')],
    ]);
    assert.ok(applied[0].duration_ms >= 150);
    assert.ok(ctx.internal.catalog.table('plug_things').columns.has('v'), 'catalog invalidated after the job');

    // Registering again applies nothing; a changed applied migration warns once and never re-runs.
    ctx.fivem.files['plug/sql/0001_init.sql'] = 'CREATE TABLE plug_things (id int PRIMARY KEY, changed int);';
    ctx.syncAs('plug', 'migrate', list);
    assert.deepEqual(await ctx.callAs('plug', 'awaitMigrations', 5000), [null]);
    ctx.syncAs('plug', 'migrate', list);
    assert.deepEqual(await ctx.callAs('plug', 'awaitMigrations', 5000), [null]);
    assert.equal(warns(ctx, /plug v1 init changed after it was applied/).length, 1);
    assert.equal((await ctx.sql('SELECT count(*)::int AS n FROM core_migrations'))[0].n, 2);
    const [, status] = await ctx.call('status');
    assert.deepEqual(status.migrations.plug, { version: 2, state: 'applied' });
});

test('migrate: a failure fails the barrier, rolls the version back, drops the owner\'s queued writes', async (t) => {
    const ctx = await setup('t_migrate2');
    t.after(ctx.teardown);
    await ctx.call('query', 'CREATE TABLE bad_plug_rows (id int PRIMARY KEY)', [], null);
    const broken = { version: 2, name: 'broken', sql: 'CREATE TABLE bad_plug_half (id int); SELEC 1;' };
    assert.deepEqual(ctx.syncAs('bad_plug', 'migrate', [{ version: 1, sql: 'CREATE TABLE bad_plug_ok (id int)' }, broken]), { ok: true });
    assert.deepEqual(ctx.syncAs('bad_plug', 'enqueue', [{ t: 'save', table: 'bad_plug_rows', row: { id: 1 } }]), { seq: 1 });

    const [err] = await ctx.callAs('bad_plug', 'query', 'SELECT 1', [], null);
    assert.match(err, /^migrations_failed: bad_plug v2 broken: 42601 syntax error at or near "SELEC"/);
    assert.deepEqual(await ctx.callAs('bad_plug', 'awaitMigrations', 1000), [err]);
    assert.deepEqual(await ctx.callAs('other', 'query', 'SELECT 1 AS x', [], null), [null, [{ x: 1 }], 1], 'other owners are not affected');
    const tables = (await ctx.sql("SELECT tablename FROM pg_tables WHERE schemaname = 't_migrate2' ORDER BY 1")).map((r) => r.tablename);
    assert.deepEqual(tables, ['bad_plug_ok', 'bad_plug_rows', 'core_migrations'], 'v1 stays, v2 rolled back as a whole');
    assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'error' && m.startsWith('migrations failed: bad_plug v2 broken')));

    await ctx.flush();
    assert.deepEqual(await ctx.sql('SELECT * FROM bad_plug_rows'), [], 'the failed owner\'s queued write was dropped');
    assert.equal(ctx.fivem.logs.filter(([l, m]) => l === 'error' && /^dropped 1 queued write\(s\) by bad_plug: migrations_failed/.test(m)).length, 1);
    await sleep(5);
    assert.equal(eventsNamed(ctx.fivem, 'core:hook:dbWriteFailed').length, 1);
    const [, synced] = await ctx.callAs('other', 'status');
    assert.equal(synced.migrations.bad_plug.state, 'failed');

    // Calling migrate again (fixed) clears the failure.
    assert.deepEqual(ctx.syncAs('bad_plug', 'migrate', [{ version: 1, sql: 'CREATE TABLE bad_plug_ok (id int)' },
        { version: 2, name: 'fixed', sql: 'CREATE TABLE bad_plug_half (id int)' }]), { ok: true });
    assert.deepEqual(await ctx.callAs('bad_plug', 'query', 'SELECT count(*)::int AS n FROM bad_plug_half', [], null), [null, [{ n: 0 }], 1]);
});

test('migrate: validation is synchronous and fails closed', async (t) => {
    const ctx = await setup('t_migrate3', { files: { 'v/sql/0003_x.sql': 'SELECT 1', 'v/sql/0004_empty.sql': '   ' } });
    t.after(ctx.teardown);
    const bad = [
        [[{ version: 2, sql: 'SELECT 1' }, { version: 2, sql: 'SELECT 1' }], /strictly increasing/],
        [[{ version: 3, sql: 'SELECT 1' }, { version: 1, sql: 'SELECT 1' }], /strictly increasing/],
        [[{ version: 0, sql: 'SELECT 1' }], /version must be an integer >= 1/],
        [[{ version: 1.5, sql: 'SELECT 1' }], /version must be an integer/],
        [['sql/../../secret.sql'], /bad path/],
        [['/etc/passwd.sql'], /bad path/],
        [['sql/0001_missing.sql'], /missing or empty/],
        [['sql/0004_empty.sql'], /missing or empty/],
        [['sql/init.sql'], /does not start with its version number/],
        [[{ version: 1 }], /needs sql or file/],
        [[42], /must be a path or a table/],
        ['nope', /migrate needs a list/],
    ];
    for (const [list, re] of bad) {
        const answer = ctx.syncAs('v', 'migrate', list);
        assert.match(answer.error, re, JSON.stringify(list));
        assert.match(answer.error, /^invalid: /);
    }
    const [err] = await ctx.callAs('v', 'query', 'SELECT 1', [], null);
    assert.match(err, /^migrations_failed: v: invalid: /, 'a refused registration fails closed');
    assert.deepEqual(ctx.syncAs('v', 'migrate', ['sql/0003_x.sql', { version: 5, file: 'sql/0003_x.sql' }]), { ok: true });
    assert.deepEqual(await ctx.callAs('v', 'awaitMigrations', 5000), [null]);
    assert.deepEqual((await ctx.sql("SELECT version, name FROM core_migrations WHERE owner = 'v' ORDER BY 1")), [{ version: 3, name: 'x' }, { version: 5, name: 'x' }]);
    assert.deepEqual(ctx.syncAs('w', 'migrate', []), { ok: true }, 'an empty list is fine');
    assert.deepEqual(await ctx.callAs('w', 'awaitMigrations', 5000), [null]);
});

test('migrate: out-of-order, prefix warning, FIFO, advisory lock', async (t) => {
    const ctx = await setup('t_migrate4');
    t.after(ctx.teardown);
    ctx.syncAs('ooo', 'migrate', [{ version: 1, sql: 'SELECT 1' }, { version: 3, sql: 'SELECT 3' }]);
    await ctx.callAs('ooo', 'awaitMigrations', 5000);
    ctx.syncAs('ooo', 'migrate', [{ version: 1, sql: 'SELECT 1' }, { version: 2, name: 'late', sql: 'SELECT 2' }, { version: 3, sql: 'SELECT 3' }]);
    assert.deepEqual(await ctx.callAs('ooo', 'awaitMigrations', 5000), [null]);
    assert.equal(warns(ctx, /ooo v2 late is applied out of order \(v3 is already applied\)/).length, 1);

    ctx.syncAs('myplug', 'migrate', [{ version: 1, sql: 'CREATE TABLE myplug_ok (id int); CREATE TABLE IF NOT EXISTS stray_things (id int)' }]);
    ctx.syncAs('core', 'migrate', [{ version: 1, sql: 'CREATE TABLE core_like_table (id int)' }]);
    await ctx.callAs('core', 'awaitMigrations', 5000);
    assert.deepEqual(warns(ctx, /without the ".*_" prefix/).map(([, m]) => m.replace(/ \(DESIGN.*$/, '')),
        ['migration myplug v1 v1 creates table stray_things without the "myplug_" prefix'], 'FIFO: myplug finished before core; core is exempt');

    // Another session holds the migration lock: the job waits for it.
    const holder = new pg.Client(BASE_URL);
    await holder.connect();
    t.after(() => holder.end());
    await holder.query("SELECT pg_advisory_lock(hashtext('core_db:migrate:t_migrate4'))");
    ctx.syncAs('locked', 'migrate', [{ version: 1, sql: 'SELECT 1' }]);
    await sleep(200);
    assert.equal((await ctx.call('status'))[1].migrations.locked.state, 'pending');
    assert.deepEqual(await ctx.callAs('locked', 'awaitMigrations', 50), ['timeout']);
    await holder.query("SELECT pg_advisory_unlock(hashtext('core_db:migrate:t_migrate4'))");
    assert.deepEqual(await ctx.callAs('locked', 'awaitMigrations', 5000), [null]);
});

test('migrate: core/sql/0001_core_schema.sql applies through the runner (DO blocks, trigger function)', async (t) => {
    const ctx = await setup('t_migrate_core');
    t.after(ctx.teardown);
    // Keep pg_trgm out of the test schema: if it is missing, create it in public first (a no-op when present).
    const ext = await ctx.sql("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'");
    if (!ext.length) await ctx.sql('CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public');
    assert.equal(ctx.api.testMapResource('core', path.join(RESOURCES, 'core')), true);
    assert.deepEqual(ctx.syncAs('core', 'migrate', ['sql/0001_core_schema.sql']), { ok: true });
    assert.deepEqual(await ctx.callAs('core', 'awaitMigrations', 60000), [null]);

    const tables = (await ctx.sql("SELECT tablename FROM pg_tables WHERE schemaname = 't_migrate_core' ORDER BY 1")).map((r) => r.tablename);
    for (const name of ['accounts', 'audit_log', 'bans', 'characters', 'core_counters', 'core_migrations', 'factions', 'map_journal', 'scene_nodes', 'vehicles']) {
        assert.ok(tables.includes(name), `${name} exists`);
    }
    assert.ok(ctx.internal.catalog.table('accounts'), 'catalog knows the new tables');
    assert.deepEqual(await ctx.call('nextId', 'scene_nodes'), [null, 1]);

    // The trigger function and the deferred FKs work through the queue.
    ctx.syncAs('core', 'enqueue', [
        { t: 'save', table: 'characters', row: { id: 'c1', account_id: 'a1', name: 'Liam' } },
        { t: 'save', table: 'accounts', row: { id: 'a1', license: 'license:1', last_seen: 1790000000 } },
    ]);
    await ctx.flush();
    const [, acc] = await ctx.call('crud', 'first', 'accounts', { where: { license: 'license:1' } }, 0);
    assert.equal(acc.last_seen, 1790000000);
    await ctx.sql("UPDATE accounts SET name = 'x', updated_at = '2000-01-01' WHERE id = 'a1'");
    await ctx.sql("UPDATE accounts SET name = 'y' WHERE id = 'a1'");
    const [, after] = await ctx.call('crud', 'first', 'accounts', { where: { id: 'a1' } }, 0);
    assert.ok(after.updated_at > 946684800, 'core_touch_updated_at set updated_at = now()');
    const [, trgm] = await ctx.call('query', "SELECT count(*)::int AS n FROM pg_indexes WHERE schemaname = 't_migrate_core' AND indexname = 'audit_log_search_idx'", [], null);
    assert.equal(trgm[0].n, 1, 'the pg_trgm index was built');
});
