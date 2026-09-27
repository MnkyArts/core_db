'use strict';
// core_db/src/calls.js — options, argument shapes and per-call deadlines shared by core.js and api.js.
//
// Deadlines (review R1 M4 + R3a): an awaited call's `timeoutMs` is the time the CALLER waits, counted from the
// moment core_db RECEIVED the call. core_db never starts a statement after it (checked after the barrier wait,
// the read-your-writes wait and the connection checkout), and every statement runs with
//   statement_timeout = min(core_db_statement_timeout_ms, remaining − DEADLINE_SLACK_MS)
// so a statement that started late is cancelled by Postgres before the Lua side gives up: a Lua 'timeout'
// means "not applied" (except a connection lost around COMMIT). Transactions do the same with their deadline.

const { coreError } = require('./pool');
const { isPlainObject, invalid } = require('./values');

const SYNC_DEFAULT_MS = 30000;
const MAX_WAIT_MS = 600000;
const DEADLINE_SLACK_MS = 250;
const MIN_FLUSH_MS = 50;

function int(v, fallback, min) {
    return typeof v === 'number' && Number.isFinite(v) && v >= min ? Math.floor(v) : fallback;
}

function normalizeOptions(options) {
    const o = options || {};
    const testMode = o.testMode === true;
    const statementTimeoutMs = int(o.statementTimeoutMs, 10000, 0);
    let flushMs = int(o.flushMs, 250, 0);
    if (!testMode && flushMs < MIN_FLUSH_MS) flushMs = MIN_FLUSH_MS; // L4: flushMs 0 (no timer) is test-only
    const defaultLock = statementTimeoutMs > 0 ? Math.max(1, Math.min(5000, Math.floor(statementTimeoutMs / 2))) : 5000;
    return {
        url: typeof o.url === 'string' ? o.url.trim() : '',
        poolSize: int(o.poolSize, 10, 1),
        flushMs,
        statementTimeoutMs,
        slowMs: int(o.slowMs, 250, 0),
        lockTimeoutMs: int(o.lockTimeoutMs, defaultLock, 0),
        migrationLockTimeoutMs: int(o.migrationLockTimeoutMs, 30000, 0),
        txPerOwner: int(o.txPerOwner, 4, 1),
        queryTimeoutMarginMs: int(o.queryTimeoutMarginMs, 5000, 0),
        watchdogMs: int(o.watchdogMs, Math.max(120000, (statementTimeoutMs + 5000) * 4), 0),
        retryFirstMs: int(o.retryFirstMs, 1000, 1),
        testMode,
    };
}

function waitMs(v, fallback) {
    return typeof v === 'number' && Number.isFinite(v) && v > 0 ? Math.min(Math.floor(v), MAX_WAIT_MS) : fallback;
}

/** opts of query/batch/crud/nextId/txQuery: { sync, timeoutMs, rows, scalar }. An empty Lua table is []. */
function queryOpts(o) {
    if (o == null || (Array.isArray(o) && !o.length)) return { sync: false, timeoutMs: 0, rows: false, scalar: false };
    if (!isPlainObject(o)) throw invalid('opts must be a map');
    return { sync: o.sync === true, timeoutMs: waitMs(o.timeoutMs, 0), rows: o.rows === true, scalar: o.scalar === true };
}

function sqlText(sql) {
    if (typeof sql !== 'string' || !sql.trim()) throw invalid('sql must be a non-empty string');
    return sql;
}

/** The callback is the LAST function argument; positions after it are treated as absent. */
function splitArgs(args, arity) {
    let at = -1;
    for (let i = args.length - 1; i >= 0; i -= 1) {
        if (typeof args[i] === 'function') {
            at = i;
            break;
        }
    }
    const params = [];
    for (let i = 0; i < arity - 1; i += 1) params.push(at >= 0 && i >= at ? undefined : args[i]);
    return { cb: at >= 0 ? args[at] : undefined, params };
}

/** A per-call deadline: `at` = epoch ms (0 = none). */
function deadline(receivedAt, timeoutMs) {
    return { at: timeoutMs > 0 ? receivedAt + timeoutMs : 0 };
}

/** ms left before the caller gives up (Infinity without a deadline); throws 'timeout' once it passed. */
function remaining(d) {
    if (!d || !d.at) return Infinity;
    const left = d.at - Date.now();
    if (left <= 0) throw coreError('timeout');
    return left;
}

/** How long a wait (barrier, sync) may take: the caller's remaining time, else `fallback`. */
function waitBudget(d, fallback) {
    const left = remaining(d);
    return left === Infinity ? fallback : Math.max(1, Math.floor(left));
}

/**
 * The statement_timeout for a statement starting NOW: 0 = the configured default applies unchanged, else
 * min(configured, remaining − slack) ms. Throws 'timeout' when nothing is left.
 */
function statementBudget(configuredMs, ...deadlines) {
    let left = Infinity;
    for (const d of deadlines) left = Math.min(left, remaining(d));
    if (left === Infinity) return 0;
    const budget = Math.floor(left - DEADLINE_SLACK_MS);
    if (budget <= 0) throw coreError('timeout');
    if (configuredMs > 0 && budget >= configuredMs) return 0;
    return budget;
}

module.exports = {
    SYNC_DEFAULT_MS,
    MAX_WAIT_MS,
    DEADLINE_SLACK_MS,
    normalizeOptions,
    waitMs,
    queryOpts,
    sqlText,
    splitArgs,
    deadline,
    remaining,
    waitBudget,
    statementBudget,
};
