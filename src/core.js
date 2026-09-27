'use strict';
// core_db/src/core.js — createCoreDb(fivem, options) → api (DESIGN §56.2.1, §56.2.3).
//
// The api object's keys are EXACTLY the export names of §56.2.3 (plus testReset/testFlush/testFail/
// testMapResource in test mode). All FiveM globals are reached through the injected adapter, so the node
// tests and tests/bridge.mjs run this very code. Internals for index.js and the tests hang off the
// non-enumerable symbol INTERNAL (hooks, close) and never become exports. The export functions themselves are
// api.js; options, argument shapes and deadlines are calls.js.

const fs = require('fs');
const path = require('path');
const { createPool, describe } = require('./pool');
const { createCatalog } = require('./catalog');
const { createQueue } = require('./queue');
const { createExecutor } = require('./flush');
const { createMigrator } = require('./migrate');
const { createTxManager } = require('./tx');
const { buildApi } = require('./api');
const { quoteIdent } = require('./sqlgen');
const C = require('./calls');

const INTERNAL = Symbol.for('core_db.internal');
const EXPORTS = ['query', 'batch', 'crud', 'nextId', 'txBegin', 'txQuery', 'txEnd', 'enqueue', 'sync', 'migrate',
    'awaitMigrations', 'status', 'isHealthy'];
const TEST_EXPORTS = ['testReset', 'testFlush', 'testFail', 'testMapResource'];
// A resource state in which its onResourceStop cannot be genuine (a plugin may TriggerEvent it, L3).
const RUNNING_STATES = new Set(['started', 'starting']);

function createCoreDb(fivem, options) {
    const opts = C.normalizeOptions(options);
    const own = fivem.resourceName();
    const log = (level, message) => {
        try {
            fivem.log(level, message);
        } catch (_err) {
            // logging must never throw into a caller
        }
    };
    const emit = (name, ...args) => fivem.defer(() => {
        try {
            fivem.emit(name, ...args);
        } catch (err) {
            log('warn', `event ${name} failed: ${err && err.message ? err.message : err}`);
        }
    });

    // ---- the invoker gate (§56.1) ----------------------------------------------------------------------

    const allowCache = new Map();
    const refused = new Set();

    function invokerName() {
        const inv = fivem.invoker();
        return typeof inv === 'string' && inv !== '' ? inv : own;
    }

    function allowed(res) {
        if (opts.testMode || res === 'core' || res === own) return true;
        let ok = allowCache.get(res);
        if (ok === undefined) {
            try {
                ok = fivem.dependsOnCore(res) === true;
            } catch (_err) {
                ok = false;
            }
            allowCache.set(res, ok);
        }
        if (!ok && !refused.has(res)) {
            refused.add(res);
            log('warn', `refused a call from "${res}": its manifest declares neither dependency 'core' nor 'core_db'`);
        }
        return ok;
    }

    function answerer(cb) {
        let answered = false;
        return (err, ...values) => {
            if (answered) return;
            answered = true;
            if (typeof cb !== 'function') return;
            const e = err == null ? null : describe(err);
            fivem.defer(() => {
                try {
                    cb(e, ...values);
                } catch (thrown) {
                    // a callback into a stopped resource throws; nobody is listening any more
                    log('debug', `a callback threw: ${thrown && thrown.message ? thrown.message : thrown}`);
                }
            });
        };
    }

    /** true when `res` really is stopping (a fake onResourceStop from a plugin is ignored, L3). */
    function reallyStopping(res) {
        let state;
        try {
            state = fivem.resourceState(res);
        } catch (_err) {
            return true;
        }
        if (RUNNING_STATES.has(state)) {
            log('warn', `ignored onResourceStop for "${res}": it is ${state} (not a genuine stop)`);
            return false;
        }
        return true;
    }

    const hooks = {
        onResourceStart(res) {
            allowCache.delete(res);
            refused.delete(res);
        },
        onResourceStop() {},
        onShutdown() {},
    };

    // ---- unavailable mode: no URL, or the pool could not be built ------------------------------------------

    function unavailableApi() {
        const api = {};
        for (const name of EXPORTS.concat(opts.testMode ? TEST_EXPORTS : [])) {
            api[name] = (...args) => {
                const { cb } = C.splitArgs(args, 99);
                answerer(cb)('unavailable');
            };
        }
        api.enqueue = () => ({ error: 'unavailable' });
        api.migrate = () => ({ error: 'unavailable' });
        api.isHealthy = () => false;
        if (opts.testMode) {
            api.testFail = () => false;
            api.testMapResource = () => false;
        }
        Object.defineProperty(api, INTERNAL, { value: { hooks, close: async () => {} } });
        return api;
    }

    if (!opts.url) {
        log('error', "core_pg_url is not set — every database call answers 'unavailable'. Set it in core_pg.cfg (set core_pg_url \"postgres://...\") and restart core_db.");
        return unavailableApi();
    }
    let queue = null;
    let pool;
    try {
        pool = createPool({ ...opts, log, emit, onHealthy: () => queue && queue.healthBack() });
    } catch (err) {
        log('error', `could not create the Postgres pool (${err && err.message ? err.message : err}) — every database call answers 'unavailable'`);
        return unavailableApi();
    }

    // ---- components ----------------------------------------------------------------------------------------

    const mapped = new Map();
    function loadFile(res, file) {
        const dir = mapped.get(res);
        if (dir === undefined) return fivem.loadResourceFile(res, file);
        try {
            return fs.readFileSync(path.join(dir, file), 'utf8');
        } catch (_err) {
            return null;
        }
    }

    const catalog = createCatalog({ pool, log });
    const migrator = createMigrator({
        pool, catalog, log, loadFile,
        lockTimeoutMs: opts.migrationLockTimeoutMs,
        retryFirstMs: opts.retryFirstMs,
        onSettle: () => queue && queue.kick(),
    });
    queue = createQueue({
        catalog,
        log,
        emit,
        execute: createExecutor({ pool, log, lockTimeoutMs: opts.lockTimeoutMs, watchdogMs: opts.watchdogMs }),
        currentBarrier: migrator.current,
        onDdl: () => catalog.reload().catch(() => {}),
        flushMs: opts.flushMs,
        retryFirstMs: opts.retryFirstMs,
    });
    const txm = createTxManager({ pool, log, perOwner: opts.txPerOwner, statementTimeoutMs: opts.statementTimeoutMs });
    catalog.invalidate(); // first load (and the first connectivity check)

    /**
     * An awaited export: invoker read synchronously, gate, the call's deadline from its timeoutMs (counted from
     * receipt), the owner's migration barrier (bounded by that deadline), then handler(owner, d, ...params).
     */
    function asyncExport(arity, handler, { barrier = true, timeoutOf = null } = {}) {
        return (...args) => {
            const receivedAt = Date.now();
            const owner = invokerName();
            const { cb, params } = C.splitArgs(args, arity);
            const reply = answerer(cb);
            if (!allowed(owner)) {
                reply('forbidden');
                return;
            }
            let timeoutMs = 0;
            if (timeoutOf) {
                try {
                    timeoutMs = timeoutOf(params) || 0;
                } catch (_err) {
                    timeoutMs = 0;
                }
            }
            const d = C.deadline(receivedAt, timeoutMs);
            (async () => {
                if (barrier) {
                    const failed = await migrator.gate(owner, d.at ? C.waitBudget(d) : null);
                    if (failed) throw failed;
                }
                return handler(owner, d, ...params);
            })().then((values) => reply(null, ...(values || [])), (err) => reply(err));
        };
    }

    const api = buildApi({ opts, pool, catalog, queue, migrator, txm, asyncExport, invokerName, allowed });

    // ---- test mode (§56.10) --------------------------------------------------------------------------------

    async function testReset() {
        migrator.cancelRetries();
        await migrator.idle();
        await queue.reset();
        await txm.reset();
        pool.setFail(null);
        await pool.withClient(async (client) => {
            const meta = { owner: 'core_db', internal: true };
            const tables = await pool.exec(client,
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = current_schema() AND tablename <> 'core_migrations'", [], meta);
            if (tables.rows.length) {
                await pool.exec(client, `TRUNCATE ${tables.rows.map((r) => quoteIdent(r.tablename)).join(', ')} RESTART IDENTITY CASCADE`, [], meta);
            }
            const sequences = await pool.exec(client,
                'SELECT sequencename FROM pg_catalog.pg_sequences WHERE schemaname = current_schema()', [], meta);
            for (const r of sequences.rows) await pool.exec(client, `ALTER SEQUENCE ${quoteIdent(r.sequencename)} RESTART`, [], meta);
        });
        migrator.reset();
        catalog.reset();
        await catalog.reload();
    }

    if (opts.testMode) {
        api.testReset = asyncExport(1, async () => {
            await testReset();
            return [];
        }, { barrier: false });
        api.testFlush = asyncExport(1, async () => {
            await queue.flushAll();
            return [];
        }, { barrier: false });
        api.testFail = (pattern, err) => {
            pool.setFail(pattern == null ? null : pattern, err);
            return true;
        };
        api.testMapResource = (res, dir) => {
            if (typeof res !== 'string' || !res) return false;
            if (dir == null) mapped.delete(res);
            else mapped.set(res, `${dir}`);
            return true;
        };
    }

    // ---- lifecycle hooks (§56.3.5), wired by index.js ------------------------------------------------------

    hooks.onResourceStop = (res) => {
        if (typeof res !== 'string' || !reallyStopping(res)) return;
        if (res === own) {
            queue.stop();
            const lost = queue.unflushed();
            if (lost) log('error', `${lost} queued write(s) were not committed and are lost (core_db stopped — restart core, never core_db)`);
            return;
        }
        txm.releaseOwner(res);
        queue.flush();
    };
    hooks.onShutdown = () => {
        queue.flush(); // harmless even when faked: it only starts a flush
    };

    Object.defineProperty(api, INTERNAL, {
        value: {
            hooks,
            catalog,
            queue,
            migrator,
            trace: (fn) => pool.setTrace(fn),
            async close() {
                queue.stop();
                migrator.cancelRetries();
                await txm.reset().catch(() => {});
                await pool.end().catch(() => {});
            },
        },
    });
    return api;
}

module.exports = { createCoreDb, INTERNAL, EXPORTS, TEST_EXPORTS };
