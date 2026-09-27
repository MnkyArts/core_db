// core_db/tests/bridge.test.mjs — tests/bridge.mjs speaks the §56.10.2 protocol over two FIFOs, the way
// core/tests/pgbridge.lua drives it, and isolates every run in its own schema (CORE_TEST_PG_SCHEMA or
// t_<pid>), dropped on a clean exit; stale t_<pid> schemas of dead runs are swept.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import readline from 'node:readline';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { pg, BASE_URL } from './helpers.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(HERE, 'bridge.mjs');
const DEAD_PID_SCHEMA = 't_99999998'; // above any pid_max: that process cannot exist

async function schemaExists(admin, name) {
    const r = await admin.query('SELECT count(*)::int AS n FROM pg_namespace WHERE nspname = $1', [name]);
    return r.rows[0].n === 1;
}

async function startBridge(t, schema, dbUrl = BASE_URL) {
    const admin = new pg.Client(BASE_URL);
    await admin.connect();
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'core_db-bridge-'));
    const fifoIn = path.join(dir, 'in');
    const fifoOut = path.join(dir, 'out');
    execFileSync('mkfifo', [fifoIn, fifoOut]);
    const env = { ...process.env, CORE_TEST_PG_URL: dbUrl };
    delete env.CORE_TEST_PG_SCHEMA;
    if (schema) env.CORE_TEST_PG_SCHEMA = schema;
    const child = spawn(process.execPath, [BRIDGE, fifoIn, fifoOut], { env, stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    child.stderr.on('data', (d) => { stderr += d; });
    const exited = new Promise((resolve) => child.on('exit', (code) => resolve(code)));
    // Same order as a blocking Lua client: the request FIFO first, then the answer FIFO. Both driven as
    // event-loop pipes (a fs read stream on a FIFO blocks a thread-pool thread, and exit waits for it).
    const openFifo = (file, flags) => new Promise((resolve, reject) => fs.open(file, flags, (e, fd) => (e ? reject(e) : resolve(fd))));
    const writer = new net.Socket({ fd: await openFifo(fifoIn, 'w'), readable: false, writable: true });
    const lines = readline.createInterface({ input: new net.Socket({ fd: await openFifo(fifoOut, 'r'), readable: true, writable: false }), crlfDelay: Infinity });
    const inbox = [];
    let waiting = null;
    lines.on('line', (line) => {
        if (waiting) {
            const w = waiting;
            waiting = null;
            w(line);
        } else inbox.push(line);
    });
    const nextLine = () => (inbox.length ? Promise.resolve(inbox.shift()) : new Promise((resolve) => { waiting = resolve; }));
    let id = 0;
    async function request(invoker, fn, ...args) {
        id += 1;
        writer.write(`${JSON.stringify({ id, invoker, fn, args })}\n`);
        const got = [];
        for (;;) {
            const message = JSON.parse(await nextLine());
            got.push(message);
            if (message.done === id) return got;
        }
    }
    t.after(async () => {
        child.kill();
        await exited;
        await admin.query(`DROP SCHEMA IF EXISTS ${schema || `t_${child.pid}`} CASCADE`).catch(() => {});
        await admin.end();
        fs.rmSync(dir, { recursive: true, force: true });
    });
    return { request, writer, exited, stderr: () => stderr, admin, pid: child.pid };
}

test('bridge: the line protocol end to end', async (t) => {
    const b = await startBridge(t, 't_bridge');
    const cb = (k) => ({ $cb: k });

    assert.deepEqual(await b.request('core', 'query', 'SELECT 1 AS x', [], null, cb(1)), [
        { cb: 1, args: [null, [{ x: 1 }], 1] },
        { done: 1 },
    ], 'callback first, then done without ret (query returns nothing)');

    assert.deepEqual(await b.request('tests', '$sql', { sql: 'CREATE TABLE br_items (id text PRIMARY KEY, n int NOT NULL DEFAULT 0 CHECK (n >= 0))' }),
        [{ done: 2, ret: [] }]);
    assert.deepEqual(await b.request('tests', '$sql', 'SELECT $1::int AS v', [5]), [{ done: 3, ret: [{ v: 5 }] }]);
    const [bad] = await b.request('tests', '$sql', 'SELECT * FROM nope', []);
    assert.match(bad.ret.error, /^42P01 /);

    // enqueue is write-through: the row is there as soon as `done` arrives.
    assert.deepEqual(await b.request('core', 'enqueue', [{ t: 'save', table: 'br_items', row: { id: 'a', n: 1 } }]), [{ done: 5, ret: { seq: 1 } }]);
    assert.deepEqual((await b.request('tests', '$sql', 'SELECT id, n FROM br_items', []))[0].ret, [{ id: 'a', n: 1 }]);

    // A dropped queued write shows up as an event line before `done`.
    const dropped = await b.request('core', 'enqueue', [{ t: 'save', table: 'br_items', row: { id: 'bad', n: -1 } }]);
    assert.equal(dropped.length, 2);
    assert.equal(dropped[0].event, 'core:hook:dbWriteFailed');
    assert.deepEqual(dropped[0].args.slice(0, 3), ['core', 'save', 'br_items']);
    assert.match(dropped[0].args[3], /^23514 /);
    assert.deepEqual(dropped[1], { done: 7, ret: { seq: 2 } });
    assert.deepEqual(await b.request('core', 'sync', null, 1000, cb(9)), [{ cb: 9, args: [null, { dropped: 1 }] }, { done: 8 }]);

    assert.deepEqual(await b.request('core', 'isHealthy'), [{ done: 9, ret: true }]);
    assert.deepEqual(await b.request('core', 'enqueue', [{ t: 'save', table: 'nope', row: {} }]), [{ done: 10, ret: { error: 'invalid: unknown table nope' } }]);

    // crud with a NULL marker and an op, answered with the row; JSON null ↔ SQL NULL.
    const [first] = await b.request('core', 'crud', 'first', 'br_items', { where: { n: { __op: '>=', value: 1 } } }, 0, cb(3));
    assert.deepEqual(first, { cb: 3, args: [null, { id: 'a', n: 1 }] });
    const [none] = await b.request('core', 'crud', 'first', 'br_items', { where: { id: 'zz' } }, null, cb(4));
    assert.deepEqual(none, { cb: 4, args: [null, null] });

    // $fail / unfail
    assert.deepEqual(await b.request('tests', '$fail', 'FROM br_items', '08006 simulated'), [{ done: 13, ret: true }]);
    const failed = await b.request('core', 'query', 'SELECT * FROM br_items', [], null, cb(5));
    assert.equal(failed.find((m) => m.cb === 5).args[0], '08006 simulated', 'a SQLSTATE keeps its form; health still flips');
    assert.deepEqual(failed.find((m) => m.event === 'core:hook:dbStatus').args[0], false);
    await b.request('tests', '$fail');
    const healed = await b.request('core', 'query', 'SELECT count(*)::int AS n FROM br_items', [], null, cb(6));
    assert.deepEqual(healed.find((m) => m.cb === 6).args, [null, [{ n: 1 }], 1]);
    assert.deepEqual(healed.find((m) => m.event).args, [true, 'ok']);

    // migrate from a mapped fixture resource, then awaitMigrations
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), 'core_db-fixture-'));
    t.after(() => fs.rmSync(fixture, { recursive: true, force: true }));
    fs.mkdirSync(path.join(fixture, 'sql'));
    fs.writeFileSync(path.join(fixture, 'sql', '0001_init.sql'), 'CREATE TABLE fixture_things (id int PRIMARY KEY);');
    assert.equal((await b.request('tests', '$mapResource', 'fixture', fixture))[0].ret, true);
    assert.deepEqual((await b.request('fixture', 'migrate', ['sql/0001_init.sql']))[0].ret, { ok: true });
    const awaited = await b.request('fixture', 'awaitMigrations', 5000, cb(7));
    assert.deepEqual(awaited[0], { cb: 7, args: [null] });

    // $reset truncates (migrations stay), $recreate drops and re-creates the current schema.
    assert.equal((await b.request('tests', '$reset'))[0].ret, true);
    assert.deepEqual((await b.request('tests', '$sql', 'SELECT count(*)::int AS n FROM br_items', []))[0].ret, [{ n: 0 }]);
    assert.deepEqual((await b.request('tests', '$sql', 'SELECT owner, version FROM core_migrations', []))[0].ret, [{ owner: 'fixture', version: 1 }]);
    assert.equal((await b.request('tests', '$recreate'))[0].ret, true);
    const gone = await b.admin.query("SELECT count(*)::int AS n FROM pg_tables WHERE schemaname = 't_bridge'");
    assert.equal(gone.rows[0].n, 0);
    assert.deepEqual((await b.request('fixture', 'migrate', ['sql/0001_init.sql']))[0].ret, { ok: true });
    assert.deepEqual((await b.request('fixture', 'awaitMigrations', 5000, cb(8)))[0], { cb: 8, args: [null] });

    const [unknown] = await b.request('core', 'nope');
    assert.match(unknown.ret.error, /unknown function nope/);

    // EOF on the request FIFO: the bridge exits cleanly and drops its schema.
    assert.equal(await schemaExists(b.admin, 't_bridge'), true);
    b.writer.end();
    assert.equal(await b.exited, 0);
    assert.equal(await schemaExists(b.admin, 't_bridge'), false, 'the run leaves nothing behind');
    assert.ok(!/unreachable/.test(b.stderr()));
});

test('bridge: default schema t_<pid>, stale runs swept at start, --gc', async (t) => {
    const admin = new pg.Client(BASE_URL);
    await admin.connect();
    t.after(async () => {
        await admin.query(`DROP SCHEMA IF EXISTS ${DEAD_PID_SCHEMA} CASCADE`).catch(() => {});
        await admin.query('DROP SCHEMA IF EXISTS t_keep_me CASCADE').catch(() => {});
        await admin.end();
    });
    await admin.query(`CREATE SCHEMA IF NOT EXISTS ${DEAD_PID_SCHEMA}`);
    await admin.query(`CREATE TABLE IF NOT EXISTS ${DEAD_PID_SCHEMA}.leftover (id int)`);
    await admin.query('CREATE SCHEMA IF NOT EXISTS t_keep_me');

    const b = await startBridge(t, null);
    const [where] = await b.request('tests', '$sql', 'SELECT current_schema() AS s', []);
    assert.deepEqual(where.ret, [{ s: `t_${b.pid}` }]);
    assert.equal(await schemaExists(admin, DEAD_PID_SCHEMA), false, 'a dead run\'s schema was swept');
    assert.equal(await schemaExists(admin, 't_keep_me'), true, 'only t_<digits> names are touched');
    await b.request('tests', '$sql', 'CREATE TABLE here (id int)', []);
    assert.equal((await admin.query(`SELECT count(*)::int AS n FROM pg_tables WHERE schemaname = 't_${b.pid}'`)).rows[0].n, 1);
    b.writer.end();
    assert.equal(await b.exited, 0);
    assert.equal(await schemaExists(admin, `t_${b.pid}`), false);

    await admin.query(`CREATE SCHEMA IF NOT EXISTS ${DEAD_PID_SCHEMA}`);
    const printed = execFileSync(process.execPath, [BRIDGE, '--gc'], { env: { ...process.env, CORE_TEST_PG_URL: BASE_URL } }).toString();
    assert.match(printed, new RegExp(`dropped \\d+ stale schema\\(s\\): .*${DEAD_PID_SCHEMA}`));
    assert.equal(await schemaExists(admin, DEAD_PID_SCHEMA), false);
});

test('bridge L12: without its own schema the bridge answers every request with the error — never public', async (t) => {
    const unreachable = new URL(BASE_URL);
    unreachable.port = '1';
    const b = await startBridge(t, 't_bridge_fatal', unreachable.toString());
    const [answer] = await b.request('core', 'query', 'SELECT 1', [], null, { $cb: 1 });
    assert.match(answer.ret.error, /^bridge: cannot create the run's schema t_bridge_fatal/);
    const [reset] = await b.request('tests', '$reset');
    assert.match(reset.ret.error, /cannot create the run's schema/);
    b.writer.end();
    assert.equal(await b.exited, 0);
    assert.match(b.stderr(), /cannot create the run's schema/);
});
