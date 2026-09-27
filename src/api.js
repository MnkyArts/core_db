'use strict';
// core_db/src/api.js — the export functions of DESIGN §56.2.3 over the components core.js builds.
//
// Every awaited export is wrapped by core.js's asyncExport: the invoker is read synchronously, the gate and the
// owner's migration barrier are passed, and the handler gets the call's DEADLINE (calls.js) as its second
// argument. Optional trailing `opts` (review R1 M4): nextId(name, opts, cb) and txQuery(txId, sql, params, opts,
// cb) accept { timeoutMs } like query/batch/crud; `query` and `txQuery` also take { scalar = true } (L6).

const crud = require('./crud');
const { DDL_RE, pinnedIdentity } = require('./entries');
const { isPlainObject, rawParams, invalid } = require('./values');
const C = require('./calls');

const NEXT_ID_SQL = 'INSERT INTO core_counters (name, value) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET value = core_counters.value + 1 RETURNING value';
const READS = new Set(['select', 'first', 'count']);

/** { timeoutMs } of an opts table (0 when absent or not a map — the handler reports a bad opts itself). */
function optTimeout(o) {
    return isPlainObject(o) ? C.waitMs(o.timeoutMs, 0) : 0;
}

function rowCountOf(r) {
    return r.rowCount == null ? 0 : r.rowCount;
}

/** The first column BY FIELD ORDER of the first row (rows fetched in array mode), or nil. */
function scalarOf(r) {
    return r.rows.length && r.rows[0].length ? r.rows[0][0] : undefined;
}

/**
 * @param {{ opts, pool, catalog, queue, migrator, txm, asyncExport: Function, invokerName: Function,
 *           allowed: Function }} k
 */
function buildApi(k) {
    const { opts, pool, catalog, queue, migrator, txm, asyncExport } = k;

    async function reloadAfterDdl(texts) {
        if (texts.some((t) => DDL_RE.test(t))) await catalog.reload().catch(() => {});
    }

    /**
     * Statements on one pooled client under the call's deadline d: `tx` = always BEGIN/COMMIT, `atomic` = only
     * when there are several. Nothing starts once d passed; each statement gets the statement budget.
     */
    function runOnPool(owner, statements, { d, atomic, tx, rowMode } = {}) {
        return pool.withClient(async (client) => {
            const meta = { owner };
            const wrap = tx || (atomic && statements.length > 1);
            let lowered = false;
            const budget = async () => {
                const ms = C.statementBudget(opts.statementTimeoutMs, d);
                if (ms > 0) {
                    await pool.exec(client, "SELECT set_config('statement_timeout', $1, false)", [`${ms}`], { owner, internal: true });
                    lowered = true;
                }
                return ms;
            };
            try {
                if (wrap) {
                    C.remaining(d);
                    await pool.exec(client, 'BEGIN', [], meta);
                }
                const results = [];
                for (const s of statements) {
                    const ms = await budget();
                    results.push(await pool.exec(client, s.text, s.values, { owner, queryTimeoutMs: ms || undefined, rowMode }));
                }
                if (wrap) await pool.exec(client, 'COMMIT', [], meta);
                return results;
            } catch (err) {
                if (wrap) await client.query('ROLLBACK').catch(() => {});
                throw err;
            } finally {
                if (lowered) await client.query('RESET statement_timeout').catch(() => {});
            }
        });
    }

    /** Statements inside a transaction: statement_timeout ≤ what is left of the call's AND the tx's deadline. */
    function txRun(owner, txId, d, statements, rowMode) {
        return txm.run(owner, txId, async (client, tx) => {
            const out = [];
            for (const s of statements) {
                const ms = C.statementBudget(opts.statementTimeoutMs, d, tx.deadline);
                const want = ms || opts.statementTimeoutMs;
                if (tx.stmtTimeout !== want) {
                    await pool.exec(client, "SELECT set_config('statement_timeout', $1, true)", [`${want}`], { owner, internal: true });
                    tx.stmtTimeout = want;
                }
                out.push(await pool.exec(client, s.text, s.values, { owner, queryTimeoutMs: ms || undefined, rowMode }));
            }
            return out;
        }, () => C.remaining(d));
    }

    /**
     * opts.sync (read-your-writes, §56.2.3): a crud read whose where pins EVERY primary-key column with plain
     * equality waits only for queued entries of THAT row (no flush when there is none); anything else waits for
     * everything queued so far — except other owners' entries held behind their pending barrier (M3).
     */
    async function readYourWrites(owner, d, op, tableName, where) {
        const budget = C.waitBudget(d, C.SYNC_DEFAULT_MS);
        if (READS.has(op) && typeof tableName === 'string') {
            const table = await catalog.tableAsync(tableName).catch(() => null);
            const identity = table ? pinnedIdentity(table, where) : null;
            if (identity) return queue.waitForRow(identity, owner, budget);
        }
        return queue.waitFor(null, owner, budget, false);
    }

    function crudTimeout(params) {
        try {
            return optTimeout(crud.parseArgs(params[0], params[2]).opts);
        } catch (_err) {
            return 0;
        }
    }

    return {
        query: asyncExport(4, async (owner, d, sql, params, o) => {
            const text = C.sqlText(sql);
            const values = rawParams(params);
            const q = C.queryOpts(o);
            if (q.sync) await queue.waitFor(null, owner, C.waitBudget(d, C.SYNC_DEFAULT_MS), false);
            const [r] = await runOnPool(owner, [{ text, values }], { d, rowMode: q.scalar ? 'array' : undefined });
            await reloadAfterDdl([text]);
            return q.scalar ? [scalarOf(r), rowCountOf(r)] : [r.rows, rowCountOf(r)];
        }, { timeoutOf: (p) => optTimeout(p[2]) }),

        batch: asyncExport(3, async (owner, d, statements, o) => {
            const q = C.queryOpts(o);
            let list = statements;
            if (list == null || (isPlainObject(list) && !Object.keys(list).length)) list = [];
            if (!Array.isArray(list)) throw invalid('statements must be a list');
            list = list.map((s, i) => {
                const pair = Array.isArray(s) ? { sql: s[0], params: s[1] } : s;
                if (!isPlainObject(pair)) throw invalid(`statement ${i + 1} must be { sql, params }`);
                return { text: C.sqlText(pair.sql), values: rawParams(pair.params) };
            });
            if (!list.length) return [[]];
            if (q.sync) await queue.waitFor(null, owner, C.waitBudget(d, C.SYNC_DEFAULT_MS), false);
            const results = await runOnPool(owner, list, { d, tx: true });
            await reloadAfterDdl(list.map((s) => s.text));
            return [results.map((r) => (q.rows ? { rowCount: rowCountOf(r), rows: r.rows } : { rowCount: rowCountOf(r) }))];
        }, { timeoutOf: (p) => optTimeout(p[1]) }),

        crud: asyncExport(5, async (owner, d, op, table, args, txId) => {
            const a = crud.parseArgs(op, args);
            const q = C.queryOpts(a.opts);
            const inTx = txId != null && txId !== 0 && txId !== false;
            if (!inTx) {
                const failed = await migrator.gate(owner, d.at ? C.waitBudget(d) : null);
                if (failed) throw failed;
            }
            if (q.sync) await readYourWrites(owner, d, op, table, a.where);
            const run = inTx
                ? (statements) => txRun(owner, txId, d, statements)
                : (statements, atomic) => runOnPool(owner, statements, { d, atomic });
            return [await crud.execute(catalog, op, table, a, run)];
        }, { barrier: false, timeoutOf: crudTimeout }),

        nextId: asyncExport(3, async (owner, d, name, o) => {
            if (typeof name !== 'string' || !name || name.length > 128) throw invalid('nextId needs a counter name');
            C.queryOpts(o);
            const [r] = await runOnPool(owner, [{ text: NEXT_ID_SQL, values: [name] }], { d });
            return [r.rows[0].value];
        }, { timeoutOf: (p) => optTimeout(p[1]) }),

        txBegin: asyncExport(2, async (owner, d, o) => [await txm.begin(owner, isPlainObject(o) ? o : {})]),

        txQuery: asyncExport(5, async (owner, d, txId, sql, params, o) => {
            const text = C.sqlText(sql);
            const values = rawParams(params);
            const q = C.queryOpts(o);
            const [r] = await txRun(owner, txId, d, [{ text, values }], q.scalar ? 'array' : undefined);
            if (DDL_RE.test(text)) txm.markDdl(owner, txId);
            return q.scalar ? [scalarOf(r), rowCountOf(r)] : [r.rows, rowCountOf(r)];
        }, { barrier: false, timeoutOf: (p) => optTimeout(p[3]) }),

        txEnd: asyncExport(3, async (owner, d, txId, commit) => {
            const { ddl } = await txm.end(owner, txId, commit === true || commit === 1);
            if (ddl) await catalog.reload().catch(() => {});
            return [];
        }, { barrier: false }),

        enqueue(entries) {
            const owner = k.invokerName();
            if (!k.allowed(owner)) return { error: 'forbidden' };
            return queue.enqueue(owner, entries);
        },

        sync: asyncExport(3, async (owner, d, seq, timeoutMs) => {
            if (seq != null && (typeof seq !== 'number' || !Number.isFinite(seq))) throw invalid('seq must be a number');
            const r = await queue.waitFor(seq == null ? null : seq, owner, C.waitBudget(d, C.SYNC_DEFAULT_MS), true);
            return [{ dropped: r.dropped }];
        }, { timeoutOf: (p) => C.waitMs(p[1], 0) }),

        migrate(list) {
            const owner = k.invokerName();
            if (!k.allowed(owner)) return { error: 'forbidden' };
            return migrator.migrate(owner, list);
        },

        awaitMigrations: asyncExport(2, async (owner, d, timeoutMs) => {
            const failed = await migrator.gate(owner, C.waitMs(timeoutMs, C.SYNC_DEFAULT_MS));
            if (failed) throw failed;
            return [];
        }, { barrier: false }),

        status: asyncExport(1, async () => [{
            healthy: pool.isHealthy(),
            pool: pool.stats(),
            queue: queue.stats(),
            migrations: migrator.status(),
        }], { barrier: false }),

        isHealthy() {
            return k.allowed(k.invokerName()) ? pool.isHealthy() : false;
        },
    };
}

module.exports = { buildApi };
