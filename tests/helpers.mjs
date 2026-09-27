// core_db/tests/helpers.mjs — shared by the *.test.mjs suites (not a suite itself).
//
// Every suite works in ITS OWN schema (search_path '<schema>,public' through the URL's `options`), created
// fresh and dropped at the end, so suites run in parallel and never touch `public` (the Lua suites use it).
// The test database is CORE_TEST_PG_URL (DESIGN §56.10.1); unreachable → the suite FAILS with a hint.

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
export const pg = require('pg');
export const { createCoreDb, INTERNAL, EXPORTS, TEST_EXPORTS } = require('../src/core.js');

export const BASE_URL = process.env.CORE_TEST_PG_URL || 'postgres://core_test:core_test@127.0.0.1:5432/core_test';

export function schemaUrl(schema, second = 'public') {
    const url = new URL(BASE_URL);
    url.searchParams.set('options', `-c search_path=${schema},${second}`);
    return url.toString();
}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** A fake adapter like FiveM's: defer = setImmediate, events and logs recorded. */
export function fakeFivem({ invoker = 'core', files = {}, deps = {} } = {}) {
    const f = {
        current: invoker,
        fallbackInvoker: invoker,
        events: [],
        logs: [],
        files,
        deps,
        handlers: {},
        states: {},
        resourceName: () => 'core_db',
        invoker: () => f.current,
        registerExport() {},
        getConvar: (_name, fallback) => fallback,
        loadResourceFile: (res, file) => (Object.prototype.hasOwnProperty.call(f.files, `${res}/${file}`) ? f.files[`${res}/${file}`] : null),
        resourceState: (res) => f.states[res] || 'started',
        dependsOnCore: (res) => f.deps[res] === true,
        on(event, fn) {
            f.handlers[event] = fn;
        },
        emit: (event, ...args) => f.events.push([event, ...args]),
        defer: (fn) => setImmediate(fn),
        log: (level, message) => f.logs.push([level, message]),
    };
    return f;
}

/** Creates the schema and a core_db instance on it. */
export async function setup(schema, { testMode = true, flushMs = 0, poolSize = 4, slowMs = 5000, statementTimeoutMs, secondSchema, core = {}, ...fake } = {}) {
    const admin = new pg.Client(BASE_URL);
    try {
        await admin.connect();
    } catch (err) {
        throw new Error(`the test database is unreachable — run scripts/test-db.sh up (${err.message})`);
    }
    await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await admin.query(`CREATE SCHEMA ${schema}`);
    await admin.query(`SET search_path TO ${schema}, public`);
    const fivem = fakeFivem(fake);
    const api = createCoreDb(fivem, { url: schemaUrl(schema, secondSchema), flushMs, testMode, poolSize, slowMs, statementTimeoutMs, ...core });
    const internal = api[INTERNAL];
    await internal.catalog.reload();

    /** Calls an awaited export as `invoker` (default: the fake's invoker); resolves the callback's args. */
    function callAs(invoker, name, ...args) {
        return new Promise((resolve) => {
            fivem.current = invoker;
            try {
                api[name](...args, (...answer) => resolve(answer));
            } finally {
                fivem.current = fivem.fallbackInvoker;
            }
        });
    }
    const call = (name, ...args) => callAs(fivem.fallbackInvoker, name, ...args);

    /** A synchronous export as `invoker`. */
    function syncAs(invoker, name, ...args) {
        fivem.current = invoker;
        try {
            return api[name](...args);
        } finally {
            fivem.current = fivem.fallbackInvoker;
        }
    }

    async function teardown() {
        try {
            await internal.close();
            await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
        } finally {
            await admin.end().catch(() => {});
        }
    }

    return {
        api,
        fivem,
        internal,
        admin,
        call,
        callAs,
        syncAs,
        sql: async (text, params) => (await admin.query(text, params)).rows,
        flush: () => call('testFlush'),
        teardown,
    };
}

/** Collects statement texts while fn runs. */
export async function traced(ctx, fn) {
    const seen = [];
    ctx.internal.trace((text) => seen.push(text));
    try {
        await fn();
    } finally {
        ctx.internal.trace(null);
    }
    return seen;
}

export function eventsNamed(fivem, name) {
    return fivem.events.filter((e) => e[0] === name).map((e) => e.slice(1));
}

/** The queue suites' tables (in the suite's own schema) plus enqueue/pending/items shortcuts. */
export async function queueTables(t, schema, options) {
    const ctx = await setup(schema, options);
    t.after(ctx.teardown);
    const [err] = await ctx.call('batch', [
        { sql: "CREATE TABLE q_items (id text PRIMARY KEY, n int NOT NULL DEFAULT 0 CHECK (n >= 0), v text, data jsonb NOT NULL DEFAULT '{}', at timestamptz, tags text[])" },
        { sql: 'CREATE TABLE q_pairs (a text, b int, v text, PRIMARY KEY (a, b))' },
        { sql: 'CREATE TABLE q_log (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, msg text NOT NULL)' },
        { sql: 'CREATE TABLE q_parent (id text PRIMARY KEY)' },
        { sql: 'CREATE TABLE q_child (id text PRIMARY KEY, parent text NOT NULL REFERENCES q_parent (id) DEFERRABLE INITIALLY DEFERRED)' },
        { sql: 'CREATE TABLE q_req (id text PRIMARY KEY, must text NOT NULL, opt text)' },
        { sql: 'CREATE TABLE q_nopk (x int)' },
    ], null);
    assert.equal(err, null);
    ctx.enqueue = (entries, owner = 'core') => ctx.syncAs(owner, 'enqueue', entries);
    ctx.pending = async () => (await ctx.call('status'))[1].queue.pending;
    ctx.items = async () => ctx.sql('SELECT id, n, v FROM q_items ORDER BY id');
    return ctx;
}
