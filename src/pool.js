'use strict';
// core_db/src/pool.js — the pg pools, their type parsers (DESIGN §56.2.4), health tracking (§56.2.5), the
// slow-query log, and the one statement wrapper every module runs SQL through (`exec`), which also carries the
// test-mode failure injection (§56.10.2 `bridge.fail`). Error classes and strings live in errors.js.
//
// Two pools (review R1 M6): `main` (core_db_pool_size) for awaited calls and transactions, `system` (2) for the
// queue's flush and the migration runner, so a saturated main pool never stalls durability.
// Half-open links (M1): TCP keepalive, and every query carries a client-side `query_timeout` of statement
// timeout + margin; a query that outlives it is a connection failure and its client is destroyed.

const pg = require('pg');
const {
    isConnectionError, isRetryable, classify, describe, redact, reasonOf, coreError, simulatedError,
} = require('./errors');

// ---- type parsers (JS → Lua values) ---------------------------------------------------------------------

const OID = {
    INT8: 20, NUMERIC: 1700, DATE: 1082, TIMESTAMP: 1114, TIMESTAMPTZ: 1184,
    INT8_ARRAY: 1016, NUMERIC_ARRAY: 1231, DATE_ARRAY: 1182, TIMESTAMP_ARRAY: 1115, TIMESTAMPTZ_ARRAY: 1185,
};

const INT8_LIMIT = 2n ** 53n;

/** int8 → number when |v| ≤ 2^53, else the decimal string. */
function parseInt8(text) {
    if (text.length <= 15) return Number(text);
    const big = BigInt(text);
    return big <= INT8_LIMIT && big >= -INT8_LIMIT ? Number(big) : text;
}

function parseNumeric(text) {
    return Number(text);
}

// Postgres' ISO output: 'YYYY-MM-DD HH:MM:SS[.ffffff][+HH[:MM[:SS]]][ BC]'
const TIMESTAMP_RE = /^(\d{4,})-(\d\d)-(\d\d)[ T](\d\d):(\d\d):(\d\d)(?:\.\d+)?(?:([+-])(\d\d)(?::?(\d\d))?(?::?(\d\d))?)?( BC)?$/;

/** timestamp/timestamptz → integer Unix seconds (floor). 'infinity'/'-infinity' stay strings. */
function parseTimestamp(text) {
    const m = TIMESTAMP_RE.exec(text);
    if (!m) return text;
    let year = Number(m[1]);
    if (m[11]) year = 1 - year;
    const date = new Date(Date.UTC(2000, Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6])));
    date.setUTCFullYear(year);
    let ms = date.getTime();
    if (m[7]) {
        const offset = (Number(m[8]) * 3600 + Number(m[9] || 0) * 60 + Number(m[10] || 0)) * 1000;
        ms -= m[7] === '+' ? offset : -offset;
    }
    return Math.floor(ms / 1000);
}

function identity(text) {
    return text;
}

function arrayOf(parse) {
    return (text) => pg.types.arrayParser.create(text, parse).parse();
}

const PARSERS = {
    [OID.INT8]: parseInt8,
    [OID.NUMERIC]: parseNumeric,
    [OID.DATE]: identity,
    [OID.TIMESTAMP]: parseTimestamp,
    [OID.TIMESTAMPTZ]: parseTimestamp,
    [OID.INT8_ARRAY]: arrayOf(parseInt8),
    [OID.NUMERIC_ARRAY]: arrayOf(parseNumeric),
    [OID.DATE_ARRAY]: arrayOf(identity),
    [OID.TIMESTAMP_ARRAY]: arrayOf(parseTimestamp),
    [OID.TIMESTAMPTZ_ARRAY]: arrayOf(parseTimestamp),
};

/** core_db's OWN type table (passed to every client) — pg's global parsers stay untouched. */
const TYPES = {
    getTypeParser(oid, format) {
        if (!format || format === 'text') {
            const parse = PARSERS[oid];
            if (parse) return parse;
        }
        return pg.types.getTypeParser(oid, format);
    },
};

// ---- errors ----------------------------------------------------------------------------------------------

// ---- the pools -------------------------------------------------------------------------------------------

const MAX_QUERY_TIMEOUT_MS = 0x7fffffff;
// Statements that change SESSION state (not SET LOCAL / SET TRANSACTION / SET CONSTRAINTS): the client gets a
// RESET ALL before it goes back to the pool (L2).
const SESSION_RE = /^\s*(?:set\s+(?!local\b|transaction\b|constraints\b)|reset\b|discard\b)|\bset_config\s*\(/i;

/**
 * @param {{ url: string, poolSize: number, systemPoolSize?: number, statementTimeoutMs: number, slowMs: number,
 *           queryTimeoutMarginMs?: number, connectTimeoutMs?: number, log: Function, emit: Function,
 *           onHealthy?: Function }} o
 */
function createPool(o) {
    const margin = o.queryTimeoutMarginMs == null ? 5000 : o.queryTimeoutMarginMs;
    const queryTimeoutMs = o.statementTimeoutMs > 0 ? o.statementTimeoutMs + margin : 0;
    let healthy = true;
    let failRules = [];
    let trace = null; // tests: observe every statement text

    function markSuccess() {
        if (healthy) return;
        healthy = true;
        o.log('info', 'database reachable again');
        o.emit('core:hook:dbStatus', true, 'ok');
        if (o.onHealthy) o.onHealthy();
    }

    function markFailure(err) {
        if (!healthy) return;
        healthy = false;
        const reason = reasonOf(err);
        o.log('error', `database unavailable: ${reason}`);
        o.emit('core:hook:dbStatus', false, reason);
    }

    function makePool(max, name) {
        const p = new pg.Pool({
            connectionString: o.url,
            max,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: o.connectTimeoutMs || 10000,
            statement_timeout: o.statementTimeoutMs,
            query_timeout: queryTimeoutMs || undefined,
            keepAlive: true,
            keepAliveInitialDelayMillis: 10000,
            application_name: name,
            types: TYPES,
            allowExitOnIdle: true,
        });
        // An idle client that dies (server restart, network drop) emits here instead of crashing Node.
        p.on('error', (err) => {
            o.log('warn', `idle connection error: ${reasonOf(err)}`);
            if (isConnectionError(err)) markFailure(err);
        });
        return p;
    }

    const pools = { main: makePool(o.poolSize, 'core_db'), system: makePool(o.systemPoolSize || 2, 'core_db_system') };

    function failFor(text) {
        for (const rule of failRules) if (rule.re.test(text)) return simulatedError(rule.err);
        return null;
    }

    /** A checked-out client (which = 'main' | 'system'); always give it back with release(). */
    async function acquire(which) {
        const p = pools[which || 'main'];
        let client;
        try {
            client = await p.connect();
        } catch (err) {
            // A checkout that timed out while every connection was busy is saturation, not an outage.
            if (/timeout exceeded when trying to connect/i.test(`${err.message}`) && p.totalCount >= p.options.max) {
                throw coreError('timeout', 'no free connection');
            }
            // Any failure to open a connection (refused, auth, missing database) means no database.
            markFailure(err);
            const e = coreError('unavailable', reasonOf(err));
            e.atConnect = true;
            throw e;
        }
        // pg-pool listens for 'error' only while a client is idle: a checked-out client whose backend dies
        // (an open transaction, a stream, the flush) would throw an unhandled 'error' (L1).
        const onError = (err) => {
            client.coreDbBroken = client.coreDbBroken || err;
            if (isConnectionError(err)) markFailure(err);
        };
        client.coreDbOnError = onError;
        client.coreDbBroken = null;
        client.coreDbDirty = false;
        client.on('error', onError);
        return client;
    }

    /** Gives a client back: destroyed when broken, RESET ALL first when a statement changed its session. */
    async function release(client, err) {
        let broken = err || client.coreDbBroken || null;
        if (!broken && client.coreDbDirty) {
            try {
                await client.query({ text: 'RESET ALL', query_timeout: queryTimeoutMs || undefined });
            } catch (e) {
                broken = e;
            }
        }
        if (client.coreDbOnError) client.removeListener('error', client.coreDbOnError);
        client.coreDbOnError = null;
        client.release(broken ? (broken instanceof Error ? broken : new Error(describe(broken))) : undefined);
    }

    /** Runs fn(client) on a pooled client; a connection-class error destroys that client instead of reusing it. */
    async function withClient(fn, which) {
        const client = await acquire(which);
        let broken = null;
        try {
            return await fn(client);
        } catch (err) {
            if (isConnectionError(err)) broken = err instanceof Error ? err : new Error(describe(err));
            throw err;
        } finally {
            await release(client, broken);
        }
    }

    /**
     * Every statement goes through here. meta: { owner, simple, queryTimeoutMs, rowMode, internal } — `simple`
     * = the simple query protocol (several statements, no parameters: migrations only); everything else uses
     * the extended protocol, which refuses a second statement in one call. queryTimeoutMs = 'none' lifts the
     * client-side limit (migrations run with statement_timeout = 0).
     */
    async function exec(client, text, values, meta) {
        const m = meta || {};
        const owner = m.owner || 'core_db';
        if (trace) trace(text, owner);
        if (failRules.length) {
            const simulated = failFor(text);
            if (simulated) {
                if (isConnectionError(simulated)) markFailure(simulated);
                throw simulated;
            }
        }
        if (!m.internal && SESSION_RE.test(text)) client.coreDbDirty = true;
        const config = m.simple ? { text } : { text, values: values || [], queryMode: 'extended' };
        if (m.queryTimeoutMs === 'none') config.query_timeout = MAX_QUERY_TIMEOUT_MS;
        else if (m.queryTimeoutMs > 0) config.query_timeout = m.queryTimeoutMs + margin;
        if (m.rowMode) config.rowMode = m.rowMode;
        const started = Date.now();
        let result;
        try {
            result = await client.query(config);
        } catch (err) {
            if (isConnectionError(err)) markFailure(err);
            throw err;
        }
        markSuccess();
        const ms = Date.now() - started;
        if (ms >= o.slowMs) o.log('warn', `slow query ${ms} ms (${owner}): ${text.slice(0, 160)}`);
        return result;
    }

    const statsOf = (p) => ({ total: p.totalCount, idle: p.idleCount, waiting: p.waitingCount });

    return {
        acquire,
        release,
        withClient,
        exec,
        queryTimeoutMs,
        /** The next release of this client runs RESET ALL (a migration may have changed session settings). */
        markDirty(client) {
            client.coreDbDirty = true;
        },
        isHealthy: () => healthy,
        stats: () => ({ ...statsOf(pools.main), system: statsOf(pools.system) }),
        /** test mode: statements whose text matches the RegExp source answer `err`; pattern null clears all. */
        setFail(pattern, err) {
            if (pattern == null) {
                failRules = [];
                return;
            }
            const source = `${pattern}`;
            failRules = failRules.filter((rule) => rule.source !== source);
            failRules.push({ source, re: new RegExp(source), err: err == null ? 'XX000 simulated failure' : `${err}` });
        },
        setTrace(fn) {
            trace = typeof fn === 'function' ? fn : null;
        },
        end: () => Promise.all([pools.main.end(), pools.system.end()]),
    };
}

module.exports = {
    createPool,
    describe,
    redact,
    reasonOf,
    classify,
    isConnectionError,
    isRetryable,
    coreError,
    parseTimestamp,
    parseInt8,
    TYPES,
    SESSION_RE,
};
