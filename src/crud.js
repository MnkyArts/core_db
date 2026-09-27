'use strict';
// core_db/src/crud.js — the table helpers behind the `crud` export (DESIGN §56.5.3).
//
// crud(op, table, args, txId, cb). `args` is either
//   named:      { values | row, rows, where, set, conflict, opts = { ... } }   (option keys may also sit at
//               the top level: columns orderBy limit offset returning update sync timeoutMs)
//   positional: the Lua helper's arguments after the table name, as a list:
//               insert {values, opts}   insertMany {rows, opts}   select/first/count {where, opts}
//               update {set, where, opts}   delete {where, opts}   upsert {values, conflict, opts}
// Answers: insert/upsert → row (or true with returning = false), insertMany → count, select → rows,
// first → row | nil, count → integer, update/delete → rowCount.

const sqlgen = require('./sqlgen');
const { isPlainObject, invalid } = require('./values');

const OPS = new Set(['insert', 'insertMany', 'select', 'first', 'count', 'update', 'delete', 'upsert']);
const OPTION_KEYS = ['columns', 'orderBy', 'limit', 'offset', 'returning', 'update', 'sync', 'timeoutMs'];
const NAMED_KEYS = new Set(['values', 'row', 'rows', 'where', 'set', 'conflict', 'opts', ...OPTION_KEYS]);
const POSITIONAL = {
    insert: ['values', 'opts'],
    insertMany: ['rows', 'opts'],
    select: ['where', 'opts'],
    first: ['where', 'opts'],
    count: ['where', 'opts'],
    update: ['set', 'where', 'opts'],
    delete: ['where', 'opts'],
    upsert: ['values', 'conflict', 'opts'],
};

/** args (either shape) → { values, rows, where, set, conflict, opts }. */
function parseArgs(op, args) {
    if (!OPS.has(op)) throw invalid(`unknown crud op "${`${op}`.slice(0, 32)}"`);
    const out = { opts: {} };
    if (args == null || (Array.isArray(args) && !args.length)) return out;
    if (Array.isArray(args)) {
        POSITIONAL[op].forEach((name, i) => {
            const v = args[i];
            if (v == null) return;
            if (name === 'opts') {
                if (!isPlainObject(v) && !(Array.isArray(v) && !v.length)) throw invalid('opts must be a map');
                if (isPlainObject(v)) out.opts = v;
            } else out[name] = v;
        });
        return out;
    }
    if (!isPlainObject(args)) throw invalid('crud args must be a table');
    for (const key of Object.keys(args)) {
        if (!NAMED_KEYS.has(key)) {
            throw invalid(`crud args key "${key.slice(0, 32)}" — expected { values|row, rows, where, set, conflict, opts }`);
        }
    }
    out.values = args.values !== undefined ? args.values : args.row;
    out.rows = args.rows;
    out.where = args.where;
    out.set = args.set;
    out.conflict = args.conflict;
    for (const key of OPTION_KEYS) if (args[key] !== undefined) out.opts[key] = args[key];
    if (args.opts != null) {
        if (isPlainObject(args.opts)) Object.assign(out.opts, args.opts);
        else if (!(Array.isArray(args.opts) && !args.opts.length)) throw invalid('opts must be a map');
    }
    return out;
}

/**
 * Builds and runs one helper call. `runStatements(statements, atomic)` executes the statements in order on
 * one client (inside a transaction when `atomic` and there are several) and resolves the pg results.
 */
async function execute(catalog, op, tableName, a, runStatements) {
    try {
        return await attempt(catalog, op, tableName, a, runStatements);
    } catch (err) {
        // A column added behind core_db's back: reload once (rate-limited) and build again. Nothing ran yet.
        if (!(err && typeof err.coreDb === 'string' && err.coreDb.startsWith('invalid: unknown column')) || !catalog.stale()) throw err;
        await catalog.reload();
        return attempt(catalog, op, tableName, a, runStatements);
    }
}

async function attempt(catalog, op, tableName, a, runStatements) {
    sqlgen.checkTableName(tableName);
    const table = await catalog.tableAsync(tableName);
    if (!table) throw invalid(`unknown table ${tableName}`);
    const o = a.opts;
    switch (op) {
        case 'insert': {
            const s = sqlgen.buildInsert(table, a.values == null ? [] : a.values, o);
            const [r] = await runStatements([s], false);
            return s.returning ? (r.rows[0] || null) : true;
        }
        case 'insertMany': {
            const statements = sqlgen.buildInsertMany(table, a.rows == null ? [] : a.rows);
            if (!statements.length) return 0;
            const results = await runStatements(statements, true);
            return results.reduce((n, r) => n + (r.rowCount || 0), 0);
        }
        case 'select':
        case 'first': {
            const s = sqlgen.buildSelect(table, a.where, o, op === 'first');
            const [r] = await runStatements([s], false);
            return op === 'first' ? (r.rows[0] || null) : r.rows;
        }
        case 'count': {
            const s = sqlgen.buildCount(table, a.where);
            const [r] = await runStatements([s], false);
            return Number(r.rows[0].n);
        }
        case 'update': {
            const s = sqlgen.buildUpdate(table, a.set, a.where);
            const [r] = await runStatements([s], false);
            return r.rowCount || 0;
        }
        case 'delete': {
            const s = sqlgen.buildDelete(table, a.where);
            const [r] = await runStatements([s], false);
            return r.rowCount || 0;
        }
        case 'upsert': {
            const s = sqlgen.buildUpsert(table, a.values == null ? [] : a.values, a.conflict, o);
            const statements = s.fallback ? [s, s.fallback] : [s];
            const results = await runStatements(statements, false);
            if (!s.returning) return true;
            const row = results[0].rows[0] || (results[1] && results[1].rows[0]);
            return row || null;
        }
        default:
            throw invalid(`unknown crud op "${op}"`);
    }
}

module.exports = { parseArgs, execute, OPS };
