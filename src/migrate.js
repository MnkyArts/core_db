'use strict';
// core_db/src/migrate.js — the migration runner and the per-owner barriers (DESIGN §56.4, §56.2.3).
//
// migrate(owner, list) validates SYNCHRONOUSLY (files are read with LoadResourceFile), installs the owner's
// barrier and queues a job. Jobs run one at a time, FIFO, on the SYSTEM pool. Per version, on one client:
//   BEGIN; SET LOCAL statement_timeout = 0; SELECT pg_advisory_xact_lock(hashtext('core_db:migrate:' ||
//   current_schema()))  — keyed by schema: one lock in production, no contention between test schemas;
//   SET LOCAL lock_timeout = <core_db_migration_lock_timeout_ms>; re-check; <the SQL text, simple query
//   protocol>; RESET ALL; INSERT INTO core_migrations ...; COMMIT — and the client's session is RESET again when
//   it goes back to the pool (a SET inside the migration outlives its COMMIT).
// A connection-class failure (the database is not up yet at boot, a blip during `restart core`) or a lock conflict
// (55P03 lock timeout, 40P01 deadlock, 40001 serialization) keeps the barrier PENDING and retries the job with
// backoff for as long as it takes; other retryable errors retry MAX_RETRYABLE times; only a statement error
// fails the barrier (review R1 H2/H3). Barriers are objects: every queue entry keeps the
// barrier that was current when it was queued, so a re-registration never holds or drops older entries.

const crypto = require('crypto');
const { describe, reasonOf, coreError, classify } = require('./pool');
const { invalid, isPlainObject, isMapLike } = require('./values');

const PATH_RE = /^[A-Za-z0-9_\-/.]+\.sql$/;
const CREATE_TABLE_RE = /\bcreate\s+(?:(?:global\s+|local\s+)?(?:temp|temporary|unlogged)\s+)?table\s+(?:if\s+not\s+exists\s+)?((?:"[^"]+"|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:"[^"]+"|[A-Za-z_][\w$]*))?)/gi;
const EXEMPT_PREFIX = new Set(['core', 'core_db']);
const RETRY_MAX_MS = 30000;
const MAX_RETRYABLE = 5;
const LOCK_CONFLICTS = new Set(['55P03', '40P01', '40001']); // retried without limit: the lock frees eventually
const LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext('core_db:migrate:' || current_schema()))";

const MIGRATIONS_TABLE = `CREATE TABLE IF NOT EXISTS core_migrations (
    owner       text        NOT NULL,
    version     integer     NOT NULL,
    name        text        NOT NULL,
    checksum    text        NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    duration_ms integer     NOT NULL,
    PRIMARY KEY (owner, version)
)`;

function md5(text) {
    return crypto.createHash('md5').update(text, 'utf8').digest('hex');
}

/** Table names a migration creates (for the plugin prefix warning of §56.4.3). */
function createdTables(sql) {
    const names = [];
    for (const m of sql.matchAll(CREATE_TABLE_RE)) {
        const parts = m[1].split('.').map((p) => p.trim().replace(/^"|"$/g, ''));
        names.push(parts[parts.length - 1]);
    }
    return names;
}

/**
 * @param {{ pool, catalog, log, loadFile: (res, path) => string|null, onSettle: (owner) => void }} o
 */
function createMigrator(o) {
    const barriers = new Map();   // owner → { state, reason, promise, resolve, version }
    const warned = new Set();
    let chain = Promise.resolve();
    let running = 0;

    function checkPath(path, where) {
        if (typeof path !== 'string' || !PATH_RE.test(path) || path.includes('..') || path.startsWith('/')) {
            throw invalid(`${where}: bad path "${`${path}`.slice(0, 80)}"`);
        }
    }

    function readFile(owner, path, where) {
        checkPath(path, where);
        const text = o.loadFile(owner, path);
        if (typeof text !== 'string' || !text.trim()) throw invalid(`${where}: ${path} is missing or empty in ${owner}`);
        return text;
    }

    function fromFileName(path) {
        const base = path.slice(path.lastIndexOf('/') + 1).replace(/\.sql$/, '');
        const m = /^(\d+)[_-]?(.*)$/.exec(base);
        return m ? { version: Number(m[1]), name: m[2] || base } : { version: null, name: base };
    }

    /** list → [{ version, name, sql, checksum }] or a throw with `invalid: ...`. */
    function validate(owner, list) {
        let items = list;
        if (!Array.isArray(items)) {
            if (isMapLike(items) && !Object.keys(items).length) items = [];
            else throw invalid('migrate needs a list');
        }
        const out = [];
        items.forEach((item, i) => {
            const where = `migration ${i + 1}`;
            let version;
            let name;
            let sql;
            if (typeof item === 'string') {
                checkPath(item, where);
                ({ version, name } = fromFileName(item));
                if (version == null) throw invalid(`${where}: ${item} does not start with its version number`);
                sql = readFile(owner, item, where);
            } else if (isPlainObject(item)) {
                version = item.version;
                if (typeof item.sql === 'string') {
                    if (!item.sql.trim()) throw invalid(`${where}: empty sql`);
                    sql = item.sql;
                    name = typeof item.name === 'string' && item.name ? item.name : `v${version}`;
                } else if (item.file != null) {
                    sql = readFile(owner, item.file, where);
                    name = typeof item.name === 'string' && item.name ? item.name : fromFileName(item.file).name;
                } else {
                    throw invalid(`${where}: needs sql or file`);
                }
            } else {
                throw invalid(`${where}: must be a path or a table`);
            }
            if (!Number.isInteger(version) || version < 1) throw invalid(`${where}: version must be an integer >= 1`);
            if (out.length && version <= out[out.length - 1].version) {
                throw invalid(`${where}: versions must be unique and strictly increasing (${version} after ${out[out.length - 1].version})`);
            }
            out.push({ version, name, sql, checksum: md5(sql) });
        });
        return out;
    }

    function newBarrier(owner) {
        const barrier = { owner, state: 'pending', reason: null, version: 0, resolve: null, promise: null };
        barrier.promise = new Promise((resolve) => { barrier.resolve = resolve; });
        const previous = barriers.get(owner);
        if (previous) barrier.version = previous.version;
        barriers.set(owner, barrier);
        return barrier;
    }

    function settle(owner, barrier, state, reason) {
        barrier.state = state;
        barrier.reason = reason;
        barrier.resolve();
        o.onSettle(owner); // held entries (of this barrier, current or not) flush on the next tick
    }

    // A test reset cancels jobs that wait for a retry (the database may be gone for good in a test).
    let epoch = 0;
    const sleepers = new Set();
    function pause(ms, myEpoch) {
        return new Promise((resolve) => {
            const s = { resolve, timer: null };
            s.timer = setTimeout(() => {
                sleepers.delete(s);
                resolve(myEpoch === epoch);
            }, ms);
            if (typeof s.timer.unref === 'function') s.timer.unref();
            sleepers.add(s);
        });
    }

    async function applyOne(owner, m, highest) {
        return o.pool.withClient(async (client) => {
            const meta = { owner, queryTimeoutMs: 'none', internal: true };
            o.pool.markDirty(client); // a SET inside the migration outlives its COMMIT: RESET ALL on release (L2)
            try {
                await o.pool.exec(client, 'BEGIN', [], meta);
                await o.pool.exec(client, 'SET LOCAL statement_timeout = 0', [], meta);
                await o.pool.exec(client, LOCK_SQL, [], meta);
                await o.pool.exec(client, "SELECT set_config('lock_timeout', $1, true)", [`${o.lockTimeoutMs}`], meta);
                const again = await o.pool.exec(client, 'SELECT 1 FROM core_migrations WHERE owner = $1 AND version = $2', [owner, m.version], meta);
                if (again.rows.length) {
                    await o.pool.exec(client, 'COMMIT', [], meta);
                    return false;
                }
                if (m.version < highest) o.log('warn', `migration ${owner} v${m.version} ${m.name} is applied out of order (v${highest} is already applied)`);
                if (!EXEMPT_PREFIX.has(owner)) {
                    for (const table of createdTables(m.sql)) {
                        if (!table.startsWith(`${owner}_`)) {
                            o.log('warn', `migration ${owner} v${m.version} ${m.name} creates table ${table} without the "${owner}_" prefix (DESIGN §56.4.3)`);
                        }
                    }
                }
                const started = Date.now();
                await o.pool.exec(client, m.sql, null, { owner, simple: true, queryTimeoutMs: 'none', internal: true });
                // the SQL may have SET search_path & co.: the bookkeeping below must see the session defaults
                await o.pool.exec(client, 'RESET ALL', [], meta);
                await o.pool.exec(client,
                    'INSERT INTO core_migrations (owner, version, name, checksum, duration_ms) VALUES ($1, $2, $3, $4, $5)',
                    [owner, m.version, m.name, m.checksum, Date.now() - started], meta);
                await o.pool.exec(client, 'COMMIT', [], meta);
                o.log('info', `applied migration ${owner} v${m.version} ${m.name} (${Date.now() - started} ms)`);
                return true;
            } catch (err) {
                try {
                    await client.query('ROLLBACK');
                } catch (_e) {
                    // connection gone
                }
                throw err;
            }
        }, 'system');
    }

    async function runJob(owner, list, barrier, myEpoch) {
        let attempt = 0;
        let retryable = 0;
        let changed = false;
        for (;;) {
            const outcome = await attemptJob(owner, list, barrier, () => { changed = true; }, () => changed);
            if (outcome === null) return;
            const { err, current } = outcome;
            const kind = classify(err);
            const lockConflict = kind === 'retryable' && LOCK_CONFLICTS.has(err.code);
            if (kind === 'retryable' && !lockConflict) retryable += 1;
            const transient = kind === 'connection' || kind === 'saturated' || lockConflict
                || (kind === 'retryable' && retryable < MAX_RETRYABLE);
            if (transient && myEpoch === epoch) {
                attempt += 1;
                const delay = Math.min(o.retryFirstMs * 2 ** (attempt - 1), RETRY_MAX_MS);
                o.log('warn', `migrations of ${owner}${current ? ` (v${current.version} ${current.name})` : ''} wait for the database (${reasonOf(err)}): retry in ${delay} ms`);
                if (await pause(delay, myEpoch)) continue;
            }
            if (myEpoch !== epoch) {
                settle(owner, barrier, 'failed', `${owner}: reset`);
                return;
            }
            const reason = current
                ? `${owner} v${current.version} ${current.name}: ${describe(err)}`
                : `${owner}: ${describe(err)}`;
            o.log('error', `migrations failed: ${reasonOf(reason)}`);
            if (current) o.catalog.invalidate();
            settle(owner, barrier, 'failed', reason);
            return;
        }
    }

    /** One pass over the list: null when applied (the barrier is settled), else { err, current }. */
    async function attemptJob(owner, list, barrier, markChanged, wasChanged) {
        let current = null;
        try {
            const sys = { owner: 'core_db', internal: true };
            await o.pool.withClient((client) => o.pool.exec(client, MIGRATIONS_TABLE, [], sys), 'system');
            const res = await o.pool.withClient((client) => o.pool.exec(client,
                'SELECT version, name, checksum FROM core_migrations WHERE owner = $1', [owner], sys), 'system');
            const applied = new Map(res.rows.map((r) => [r.version, r]));
            let highest = res.rows.reduce((max, r) => Math.max(max, r.version), 0);
            barrier.version = highest;
            const listed = new Set(list.map((m) => m.version));
            for (const r of res.rows) {
                if (!listed.has(r.version)) o.log('debug', `migration ${owner} v${r.version} ${r.name} is applied but not listed`);
            }
            for (const m of list) {
                current = m;
                const done = applied.get(m.version);
                if (done) {
                    const key = `${owner}:${m.version}:${m.checksum}`;
                    if (done.checksum !== m.checksum && !warned.has(key)) {
                        warned.add(key);
                        o.log('warn', `migration ${owner} v${m.version} ${m.name} changed after it was applied (checksum differs) — never edit an applied migration, add a new version`);
                    }
                    continue;
                }
                if (await applyOne(owner, m, highest)) markChanged();
                highest = Math.max(highest, m.version);
                barrier.version = highest;
            }
            current = null;
            if (wasChanged()) await o.catalog.reload();
            settle(owner, barrier, 'applied', null);
            return null;
        } catch (err) {
            return { err, current };
        }
    }

    return {
        /** Synchronous: { ok = true } | { error }. A registration that fails validation leaves a FAILED barrier. */
        migrate(owner, list) {
            let items;
            try {
                items = validate(owner, list);
            } catch (err) {
                const reason = describe(err);
                const barrier = newBarrier(owner);
                settle(owner, barrier, 'failed', `${owner}: ${reason}`);
                return { error: reason };
            }
            const barrier = newBarrier(owner);
            const myEpoch = epoch;
            running += 1;
            chain = chain.then(() => runJob(owner, items, barrier, myEpoch)).finally(() => { running -= 1; });
            return { ok: true };
        },
        /** The owner's current barrier object (queue entries keep it), or null. */
        current: (owner) => barriers.get(owner) || null,
        state: (owner) => (barriers.has(owner) ? barriers.get(owner).state : undefined),
        reason: (owner) => (barriers.has(owner) ? barriers.get(owner).reason : null),
        /** Resolves null (no barrier / applied) or the error string; timeoutMs null = no limit. */
        async gate(owner, timeoutMs) {
            const barrier = barriers.get(owner);
            if (!barrier) return null;
            if (barrier.state === 'pending') {
                if (timeoutMs == null) await barrier.promise;
                else {
                    let timer;
                    const expired = await Promise.race([
                        barrier.promise.then(() => false),
                        new Promise((resolve) => { timer = setTimeout(() => resolve(true), timeoutMs); }),
                    ]);
                    clearTimeout(timer);
                    if (expired) throw coreError('timeout');
                }
            }
            return barrier.state === 'failed' ? `migrations_failed: ${barrier.reason}` : null;
        },
        wait: (owner) => (barriers.has(owner) ? barriers.get(owner).promise : Promise.resolve()),
        status() {
            const out = {};
            for (const [owner, b] of barriers) out[owner] = { version: b.version, state: b.state };
            return out;
        },
        /** Resolves once no job is queued or running. */
        async idle() {
            while (running > 0) await chain;
        },
        /** Test reset: jobs waiting for a retry give up; barriers and warnings are forgotten. */
        cancelRetries() {
            epoch += 1;
            for (const s of sleepers) {
                clearTimeout(s.timer);
                s.resolve(false);
            }
            sleepers.clear();
        },
        reset() {
            barriers.clear();
            warned.clear();
        },
    };
}

module.exports = { createMigrator, createdTables, md5, MIGRATIONS_TABLE, LOCK_SQL };
