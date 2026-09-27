'use strict';
// core_db/src/errors.js — error classes and error strings (DESIGN §56.2.5, review R1 C1/H3/L7).
//
//   connection  the link is gone or cannot be made: driver/network errors, 'Connection terminated',
//               'not queryable', a client-side query timeout, 08000/08001/08003/08004/08006, 57P01–03, 53300.
//               Health flips; the queue requeues and never drops. (08P01 protocol_violation is a STATEMENT
//               error: a bind with the wrong parameter count answers it.)
//   retryable   the statement may succeed later: 57014 statement timeout, 40P01 deadlock, 40001 serialization,
//               55P03 lock not available, 53100 disk full, 53200 out of memory, 25006 read-only transaction
//               (failover). Health does not flip; the queue requeues with backoff (bounded, see flush.js).
//   statement   everything else: the entry is bad (constraint, bad data, bug) and safe mode drops it.

const NET_CODES = new Set([
    'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'EPIPE', 'EHOSTUNREACH', 'ENETUNREACH',
    'EAI_AGAIN', 'ECONNABORTED', 'EHOSTDOWN', 'ENETDOWN',
]);
const CONNECTION_STATES = new Set(['08000', '08001', '08003', '08004', '08006', '57P01', '57P02', '57P03', '53300']);
const RETRYABLE_STATES = new Set(['57014', '40P01', '40001', '55P03', '53100', '53200', '25006']);
const CONNECTION_MESSAGE_RE = /connection terminated|not queryable|timeout exceeded when trying to connect|connection ended|terminated unexpectedly|query read timeout|flush watchdog|ECONN/i;

function sqlStateOf(err) {
    const code = err && err.code;
    return typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code) && !NET_CODES.has(code) ? code : null;
}

function isConnectionError(err) {
    if (!err || typeof err === 'string') return false;
    if (err.coreDb) return err.coreDb === 'unavailable';
    if (NET_CODES.has(err.code)) return true;
    const state = sqlStateOf(err);
    if (state) return CONNECTION_STATES.has(state);
    return CONNECTION_MESSAGE_RE.test(`${err.message || ''}`);
}

/** A statement error that may succeed on a later attempt (never a connection error). */
function isRetryable(err) {
    const state = err && typeof err === 'object' ? sqlStateOf(err) : null;
    return state !== null && RETRYABLE_STATES.has(state);
}

/** 'connection' | 'retryable' | 'saturated' | 'statement' */
function classify(err) {
    if (err && err.coreDb === 'timeout') return 'saturated';
    if (isConnectionError(err)) return 'connection';
    if (isRetryable(err)) return 'retryable';
    return 'statement';
}

/** One short error string (never an object, never a stack, never parameters). */
function describe(err) {
    if (err == null) return 'unknown error';
    if (typeof err === 'string') return err;
    if (err.coreDb) return err.coreDb;
    const state = sqlStateOf(err);
    if (state) return `${state} ${err.message}`;
    if (isConnectionError(err)) return 'unavailable';
    return `invalid: ${err.message || err}`;
}

/**
 * Removes VALUES from a Postgres message before it reaches a log, an event or `status` (L7): the quoted text
 * after ': ' (22P02 `invalid input syntax for type integer: "abc"`, 22007, 22008, malformed literals) and
 * `value "…" is out of range`. Identifiers (`relation "x"`, `constraint "y"`) are kept.
 */
function redact(text) {
    if (typeof text !== 'string') return text;
    return text
        .replace(/: "(?:[^"\\]|\\.|"")*"/g, ': "…"')
        .replace(/\bvalue "(?:[^"\\]|\\.|"")*" is out of range/g, 'value "…" is out of range');
}

/** A reason for logs and `dbStatus`: the driver detail, never the URL (pg never puts it in messages). */
function reasonOf(err) {
    if (typeof err === 'string') return redact(err);
    const state = sqlStateOf(err);
    if (state) return redact(`${state} ${err.message}`);
    return redact(`${err.code ? `${err.code} ` : ''}${err.message || err}`.trim());
}

/** A tagged error that `describe` passes through unchanged ('timeout', 'unavailable', ...). */
function coreError(code, message) {
    const err = new Error(message || code);
    err.coreDb = code;
    return err;
}

/** Turns a bridge.fail error string into the error object the driver would have thrown. */
function simulatedError(text) {
    const m = /^([0-9A-Z]{5}) (.*)$/.exec(text);
    if (m && !NET_CODES.has(m[1])) return Object.assign(new Error(m[2]), { code: m[1], simulated: true });
    const err = new Error(text);
    err.simulated = true;
    if (/^E[A-Z]+$/.test(text)) err.code = text;
    else if (text === 'unavailable' || text === 'timeout') err.coreDb = text;
    return err;
}

module.exports = {
    NET_CODES,
    CONNECTION_STATES,
    RETRYABLE_STATES,
    sqlStateOf,
    isConnectionError,
    isRetryable,
    classify,
    describe,
    redact,
    reasonOf,
    coreError,
    simulatedError,
};
