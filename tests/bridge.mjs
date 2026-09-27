// core_db/tests/bridge.mjs <fifoIn> <fifoOut> — the Lua test bridge (DESIGN §56.10.2).
//      bridge.mjs --gc                        — only drop stale per-run schemas (see "Isolation") and exit
//
// Runs the REAL core_db code (createCoreDb, test mode, no flush timer) with a fake FiveM adapter and speaks a
// JSON-lines protocol over two FIFOs. core/tests/pgbridge.lua writes requests and reads answers:
//
//   Lua → Node  { "id": n, "invoker": "core", "fn": "query", "args": [ ..., { "$cb": k } ] }
//   Node → Lua  { "cb": k, "args": [...] }          each callback, as it fires
//               { "event": name, "args": [...] }     every emitted local event (dbStatus, dbWriteFailed)
//               { "done": n, "ret": value }          once the call RETURNED and every callback it got FIRED
//                                                     (ret omitted when the export returned nothing)
//
// `enqueue` awaits a flush (testFlush) before `done`: queued writes are visible at once (write-through).
// Bridge functions (answered with `done` only): $reset, $recreate, $sql, $fail, $mapResource.
// JSON has no NULL-vs-absent distinction: undefined is omitted (trailing callback arguments trimmed,
// undefined object fields dropped), null stays null.
//
// Isolation: several suites run bridges AT THE SAME TIME against the one test database, so every bridge
// process works in ITS OWN schema — env CORE_TEST_PG_SCHEMA, else `t_<pid>` — created at start, used through
// `search_path = <schema>, public` on every pooled connection (public stays second so pg_trgm's operator
// classes resolve; every CREATE lands in the run's schema), and dropped on a clean exit (input EOF, SIGTERM,
// SIGINT; CORE_TEST_PG_KEEP=1 keeps it). At start, schemas named `t_<digits>` whose process is gone are
// dropped (a crashed run). `$reset` truncates, and `$recreate` drops + re-creates, ONLY the run's schema.
//
// The database is CORE_TEST_PG_URL (default postgres://core_test:core_test@127.0.0.1:5432/core_test) — a
// throwaway test database, never production.

import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import readline from 'node:readline';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const pg = require('pg');
const { createCoreDb, INTERNAL } = require('../src/core.js');
const { quoteIdent } = require('../src/sqlgen.js');

const HERE = path.dirname(fileURLToPath(import.meta.url));
const RESOURCES_DIR = path.resolve(HERE, '..', '..');
const DB_URL = process.env.CORE_TEST_PG_URL || 'postgres://core_test:core_test@127.0.0.1:5432/core_test';
const VERBOSE = !!process.env.CORE_DB_BRIDGE_VERBOSE;
const CALL_TIMEOUT_MS = Number(process.env.CORE_DB_BRIDGE_TIMEOUT_MS) || 120000;
const SCHEMA_RE = /^[a-z_][a-z0-9_]*$/;
const RUN_SCHEMA_RE = /^t_([0-9]+)$/;

function stderr(level, message) {
    process.stderr.write(`[core_db bridge] ${level}: ${message}\n`);
}

function openFifo(file, flags) {
    return new Promise((resolve, reject) => {
        fs.open(file, flags, (err, fd) => (err ? reject(err) : resolve(fd)));
    });
}

// ---- the run's schema -----------------------------------------------------------------------------------------

function runSchema() {
    const wanted = process.env.CORE_TEST_PG_SCHEMA;
    if (wanted) {
        if (SCHEMA_RE.test(wanted)) return wanted;
        stderr('warn', `CORE_TEST_PG_SCHEMA "${wanted}" is not ^[a-z_][a-z0-9_]*$ — using t_${process.pid}`);
    }
    return `t_${process.pid}`;
}

function urlFor(schema) {
    const url = new URL(DB_URL);
    url.searchParams.set('options', `-c search_path=${schema},public`);
    return url.toString();
}

async function withAdmin(fn) {
    const client = new pg.Client(DB_URL);
    await client.connect();
    try {
        return await fn(client);
    } finally {
        await client.end().catch(() => {});
    }
}

function alive(pid) {
    try {
        process.kill(pid, 0);
        return true;
    } catch (err) {
        return err.code !== 'ESRCH'; // EPERM: it exists, it just is not ours
    }
}

/** Drops `t_<pid>` schemas whose process is gone (never `keep`, never a live pid). */
async function sweep(client, keep) {
    const { rows } = await client.query("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname ~ '^t_[0-9]+$'");
    const dropped = [];
    for (const { nspname } of rows) {
        const m = RUN_SCHEMA_RE.exec(nspname);
        if (!m || nspname === keep) continue;
        const pid = Number(m[1]);
        if (pid === process.pid || alive(pid)) continue;
        try {
            await client.query(`DROP SCHEMA IF EXISTS ${quoteIdent(nspname)} CASCADE`);
            dropped.push(nspname);
        } catch (err) {
            stderr('warn', `could not drop stale schema ${nspname}: ${err.message}`);
        }
    }
    return dropped;
}

const argv = process.argv.slice(2);
if (argv[0] === '--gc') {
    withAdmin((client) => sweep(client, null)).then((dropped) => {
        process.stdout.write(`dropped ${dropped.length} stale schema(s)${dropped.length ? `: ${dropped.join(', ')}` : ''}\n`);
        process.exit(0);
    }, (err) => {
        stderr('error', `the test database is unreachable — run scripts/test-db.sh up (${err.message})`);
        process.exit(1);
    });
} else {
    runBridge(argv[0], argv[1]);
}

// ---- the bridge -------------------------------------------------------------------------------------------------

function runBridge(fifoIn, fifoOut) {
    if (!fifoIn || !fifoOut) {
        stderr('error', 'usage: node core_db/tests/bridge.mjs <fifoIn> <fifoOut>   |   node core_db/tests/bridge.mjs --gc');
        process.exit(2);
    }
    const SCHEMA = runSchema();

    // Both FIFOs are opened asynchronously (a thread-pool open blocks until Lua opens the other end, so the
    // order in which Lua opens them cannot deadlock) and then driven as event-loop pipes (net.Socket over the
    // fd): a fs read stream would park a thread-pool thread in a blocking read(), and process exit waits for
    // it — a bridge told to stop (SIGTERM) could never exit.
    let out = null;
    const unsent = [];
    openFifo(fifoOut, 'w').then((fd) => {
        out = new net.Socket({ fd, readable: false, writable: true });
        out.on('error', () => shutdown()); // the reader (Lua) is gone
        for (const line of unsent.splice(0)) out.write(line);
    }, (err) => {
        stderr('error', `cannot open ${fifoOut}: ${err.message}`);
        process.exit(1);
    });

    /** undefined → omitted (trailing array slots trimmed, object fields dropped), null → null. */
    function send(message) {
        const line = `${JSON.stringify(message)}\n`;
        if (out) out.write(line);
        else unsent.push(line);
    }

    function trimArgs(list) {
        const args = [...list];
        while (args.length && args[args.length - 1] === undefined) args.pop();
        return args;
    }

    let currentInvoker = 'tests';

    const fivem = {
        resourceName: () => 'core_db',
        invoker: () => currentInvoker,
        registerExport() {},
        getConvar: (_name, fallback) => fallback,
        loadResourceFile(res, file) {
            try {
                return fs.readFileSync(path.join(RESOURCES_DIR, res, file), 'utf8');
            } catch (_err) {
                return null;
            }
        },
        resourceState: () => 'started',
        dependsOnCore: () => true,
        on() {},
        emit: (event, ...args) => send({ event, args: trimArgs(args) }),
        defer: (fn) => fn(),
        log(level, message) {
            if (VERBOSE || level === 'error' || level === 'warn') stderr(level, message);
        },
    };

    let api = null;
    let fatal = null; // the run's schema could not be made: every request answers this (never fall back to public)

    // The schema must exist BEFORE the pool's first connection: current_schema() is the first EXISTING schema
    // of the search_path, and the catalog introspects current_schema().
    const ready = (async () => {
        try {
            await withAdmin(async (client) => {
                const stale = await sweep(client, SCHEMA).catch(() => []);
                if (stale.length && VERBOSE) stderr('info', `dropped stale schemas ${stale.join(', ')}`);
                await client.query(`CREATE SCHEMA IF NOT EXISTS ${quoteIdent(SCHEMA)}`);
                // pg_trgm belongs in public: an extension created by a migration inside a run's schema would die
                // with that schema and take every other run's trigram indexes with it.
                await client.query('CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public').catch(() => {});
            });
            // search_path lists public second: without the run's schema every table would land in public.
            const check = await withAdmin((client) => client.query('SELECT count(*)::int AS n FROM pg_namespace WHERE nspname = $1', [SCHEMA]));
            if (check.rows[0].n !== 1) throw new Error(`schema ${SCHEMA} does not exist after CREATE SCHEMA`);
        } catch (err) {
            fatal = `bridge: cannot create the run's schema ${SCHEMA} — the test database is unreachable or refuses it (run scripts/test-db.sh up): ${err.message}`;
            stderr('error', fatal);
            return;
        }
        api = createCoreDb(fivem, { url: urlFor(SCHEMA), testMode: true, flushMs: 0 });
        await api[INTERNAL].catalog.reload().catch(() => {});
    })();

    /** Calls an api function (callback last) as `invoker`; resolves the callback's arguments. */
    function callback(name, invoker, ...args) {
        return new Promise((resolve) => {
            currentInvoker = invoker;
            try {
                api[name](...args, (...answer) => resolve(answer));
            } finally {
                currentInvoker = 'tests';
            }
        });
    }

    async function runSql(invoker, sql, params) {
        const [err, rows] = await callback('query', invoker, sql, params == null ? [] : params, null);
        return err ? { error: err } : rows;
    }

    async function special(fn, args, invoker) {
        switch (fn) {
            case '$reset': {
                const [err] = await callback('testReset', 'tests');
                return err ? { error: err } : true;
            }
            case '$recreate': {
                // DROP and re-create the RUN's schema (never public), in one transaction under the migration lock.
                const schema = quoteIdent(SCHEMA);
                const [batchErr] = await callback('batch', 'tests', [
                    { sql: "SELECT pg_advisory_xact_lock(hashtext('core_db:migrate:' || current_schema()))" },
                    { sql: `DROP SCHEMA IF EXISTS ${schema} CASCADE` },
                    { sql: `CREATE SCHEMA ${schema}` },
                ], null);
                if (batchErr) return { error: batchErr };
                const [err] = await callback('testReset', 'tests');
                return err ? { error: err } : true;
            }
            case '$sql': {
                const one = args[0];
                const request = one !== null && typeof one === 'object' && !Array.isArray(one) ? one : { sql: one, params: args[1] };
                return runSql(invoker || 'tests', request.sql, request.params);
            }
            case '$fail':
                api.testFail(args[0] == null ? null : args[0], args[1] == null ? null : args[1]);
                return true;
            case '$mapResource':
                return api.testMapResource(args[0], args[1] == null ? null : args[1]);
            default:
                return { error: `unknown bridge function ${fn}` };
        }
    }

    async function handle(request) {
        const { id } = request;
        const fn = `${request.fn}`;
        const invoker = typeof request.invoker === 'string' && request.invoker ? request.invoker : 'tests';
        const args = Array.isArray(request.args) ? request.args : [];
        if (fatal) {
            send({ done: id, ret: { error: fatal } });
            return;
        }
        if (fn.startsWith('$')) {
            send({ done: id, ret: await special(fn, args, invoker) });
            return;
        }
        const exported = api[fn];
        if (typeof exported !== 'function') {
            send({ done: id, ret: { error: `unknown function ${fn}` } });
            return;
        }
        let open = 0;
        let expired = false;
        let allFired;
        const fired = new Promise((resolve) => { allFired = resolve; });
        const callArgs = args.map((a) => {
            if (a === null || typeof a !== 'object' || Array.isArray(a) || a.$cb === undefined) return a;
            const k = a.$cb;
            let called = false;
            open += 1;
            return (...answer) => {
                if (called || expired) return;
                called = true;
                send({ cb: k, args: trimArgs(answer) });
                open -= 1;
                if (open === 0) allFired();
            };
        });
        if (open === 0) allFired();
        let ret;
        currentInvoker = invoker;
        try {
            ret = exported(...callArgs);
        } finally {
            currentInvoker = 'tests';
        }
        if (fn === 'enqueue') await callback('testFlush', 'tests');
        let timer;
        const timedOut = await Promise.race([
            fired.then(() => false),
            new Promise((resolve) => { timer = setTimeout(() => resolve(true), CALL_TIMEOUT_MS); }),
        ]);
        clearTimeout(timer);
        if (timedOut) {
            expired = true;
            send({ done: id, ret: { error: `bridge: a callback of ${fn} did not fire within ${CALL_TIMEOUT_MS} ms` } });
            return;
        }
        send(ret === undefined ? { done: id } : { done: id, ret });
    }

    // ---- the request loop: one request at a time, in order --------------------------------------------------

    const backlog = [];
    let working = false;
    let closing = false;

    async function pump() {
        if (working) return;
        working = true;
        await ready;
        while (backlog.length) {
            const line = backlog.shift();
            let request;
            try {
                request = JSON.parse(line);
            } catch (_err) {
                stderr('error', `not JSON: ${line.slice(0, 200)}`);
                continue;
            }
            try {
                await handle(request);
            } catch (err) {
                send({ done: request.id, ret: { error: `bridge: ${err && err.message ? err.message : err}` } });
            }
        }
        working = false;
        if (closing) shutdown();
    }

    let shuttingDown = false;
    async function shutdown() {
        if (shuttingDown) return;
        shuttingDown = true;
        setTimeout(() => process.exit(0), 5000).unref();
        await ready.catch(() => {});
        if (api) await api[INTERNAL].close().catch(() => {});
        if (!process.env.CORE_TEST_PG_KEEP && !fatal) {
            await withAdmin((client) => client.query(`DROP SCHEMA IF EXISTS ${quoteIdent(SCHEMA)} CASCADE`))
                .catch((err) => stderr('warn', `could not drop schema ${SCHEMA}: ${err.message}`));
        }
        if (out && !out.destroyed) out.end(() => process.exit(0));
        else process.exit(0);
    }

    openFifo(fifoIn, 'r').then((fd) => {
        const input = new net.Socket({ fd, readable: true, writable: false });
        input.on('error', () => {}); // 'close' follows
        const lines = readline.createInterface({ input, crlfDelay: Infinity });
        lines.on('line', (line) => {
            if (!line.trim()) return;
            backlog.push(line);
            pump();
        });
        lines.on('close', () => {
            closing = true;
            if (!working) shutdown();
        });
    }, (err) => {
        stderr('error', `cannot open ${fifoIn}: ${err.message}`);
        process.exit(1);
    });
    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);

    // Say it loudly (stderr) when the test database is not there — suites must fail, never skip silently.
    ready.then(() => (api ? callback('query', 'tests', 'SELECT 1', [], null) : [null])).then(([err]) => {
        if (err) stderr('error', `the test database is unreachable — run scripts/test-db.sh up (${err})`);
    });
}
