'use strict';
// core_db/src/sqlgen.js — SQL text from catalog-checked identifiers (DESIGN §56.2.1, §56.5.3, §56.3.3).
//
// Every table and column name that reaches SQL text was found in the catalog first and is ALWAYS quoted;
// every value is a bind parameter. Nothing here talks to the database.

const {
    isPlainObject, isNullMarker, isOp, isMapLike, mapEntries, markerReplacer, toParam, invalid,
} = require('./values');

const TABLE_RE = /^[a-z_][a-z0-9_]*$/;
const ORDER_RE = /^([a-z_][a-z0-9_]*)(?:\s+(asc|desc))?(?:\s+nulls\s+(first|last))?$/i;
const MAX_PARAMS = 65535;
const MAX_BULK_ROWS = 1000;

function quoteIdent(name) {
    return `"${`${name}`.replace(/"/g, '""')}"`;
}

/** Validates a table name's shape (the catalog lookup is the caller's). */
function checkTableName(name) {
    if (typeof name !== 'string' || !TABLE_RE.test(name)) throw invalid(`table name ${typeof name === 'string' ? `"${name.slice(0, 64)}"` : typeof name}`);
    return name;
}

function column(table, name) {
    const col = typeof name === 'string' ? table.columns.get(name) : null;
    if (!col) throw invalid(`unknown column ${table.name}.${name}`);
    return col;
}

function writableColumn(table, name) {
    const col = column(table, name);
    if (col.generated) throw invalid(`column ${table.name}.${name} is generated`);
    return col;
}

function nonNegativeInt(v, what) {
    if (typeof v !== 'number' || !Number.isInteger(v) || v < 0) throw invalid(`${what} must be a non-negative integer`);
    return v;
}

function push(params, v) {
    params.push(v);
    return `$${params.length}`;
}

// ---- WHERE ---------------------------------------------------------------------------------------------

const COMPARE = new Set(['=', '<>', '<', '<=', '>', '>=']);
const LIKE = { like: 'LIKE', ilike: 'ILIKE', not_like: 'NOT LIKE' };

function listParam(table, col, list, what) {
    if (!Array.isArray(list)) throw invalid(`${what} needs a list`);
    return list.map((item) => {
        if (item == null || isNullMarker(item)) return null;
        return col.kind === 'json' ? JSON.stringify(item, markerReplacer) : toParam(col, item, table.name);
    });
}

function opCondition(table, col, q, cond, params) {
    const op = cond.__op;
    const what = `where.${col.name} (${op})`;
    const value = cond.value;
    const absent = value == null || isNullMarker(value);
    if (COMPARE.has(op)) {
        if (absent) {
            if (op === '=') return `${q} IS NULL`;
            if (op === '<>') return `${q} IS NOT NULL`;
            throw invalid(`${what} needs a value`);
        }
        return `${q} ${op} ${push(params, toParam(col, value, table.name))}`;
    }
    if (LIKE[op]) {
        if (typeof value !== 'string') throw invalid(`${what} needs a string`);
        return `${q} ${LIKE[op]} ${push(params, value)}`;
    }
    switch (op) {
        case 'in':
        case 'not_in': {
            if (col.kind === 'array') throw invalid(`${what} is not supported on an array column`);
            const list = listParam(table, col, value, what);
            if (!list.length) return op === 'in' ? 'FALSE' : 'TRUE';
            return op === 'in' ? `${q} = ANY(${push(params, list)})` : `${q} <> ALL(${push(params, list)})`;
        }
        case 'between': {
            const value2 = cond.value2;
            if (absent || value2 == null || isNullMarker(value2)) throw invalid(`${what} needs two values`);
            return `${q} BETWEEN ${push(params, toParam(col, value, table.name))} AND ${push(params, toParam(col, value2, table.name))}`;
        }
        case 'is_null':
            return `${q} IS NULL`;
        case 'not_null':
            return `${q} IS NOT NULL`;
        case 'contains':
        case 'overlaps':
            if (absent) throw invalid(`${what} needs a value`);
            return `${q} ${op === 'contains' ? '@>' : '&&'} ${push(params, toParam(col, value, table.name))}`;
        default:
            throw invalid(`unknown operator "${`${op}`.slice(0, 32)}"`);
    }
}

function condition(table, col, value, params) {
    const q = quoteIdent(col.name);
    if (value == null || isNullMarker(value)) return `${q} IS NULL`;
    if (isOp(value)) return opCondition(table, col, q, value, params);
    if (Array.isArray(value)) {
        // An array or json column compares with the whole value; any other column matches one of the list.
        if (col.kind === 'array' || col.kind === 'json') return `${q} = ${push(params, toParam(col, value, table.name))}`;
        if (!value.length) return 'FALSE';
        return `${q} = ANY(${push(params, listParam(table, col, value, `where.${col.name}`))})`;
    }
    if (isPlainObject(value) && col.kind !== 'json') throw invalid(`where.${col.name} must be a value, a list, Core.DB.NULL or Core.DB.op(...)`);
    return `${q} = ${push(params, toParam(col, value, table.name))}`;
}

/** ' WHERE a = $1 AND ...' or '' (no conditions). */
function buildWhere(table, where, params) {
    if (where == null) return '';
    if (!isMapLike(where)) throw invalid('where must be a map');
    const parts = mapEntries(where).map(([name, value]) => condition(table, column(table, name), value, params));
    return parts.length ? ` WHERE ${parts.join(' AND ')}` : '';
}

function buildOrderBy(table, orderBy) {
    if (orderBy == null || orderBy === '' || (Array.isArray(orderBy) && !orderBy.length)) return '';
    const text = Array.isArray(orderBy) ? orderBy.join(',') : orderBy;
    if (typeof text !== 'string') throw invalid('orderBy must be a string');
    const parts = text.split(',').map((part) => {
        const m = ORDER_RE.exec(part.trim());
        if (!m) throw invalid(`orderBy "${part.trim().slice(0, 64)}"`);
        column(table, m[1]);
        return `${quoteIdent(m[1])}${m[2] ? ` ${m[2].toUpperCase()}` : ''}${m[3] ? ` NULLS ${m[3].toUpperCase()}` : ''}`;
    });
    return ` ORDER BY ${parts.join(', ')}`;
}

function columnList(table, columns, what) {
    if (columns == null || (Array.isArray(columns) && !columns.length)) return null;
    const list = typeof columns === 'string' ? [columns] : columns;
    if (!Array.isArray(list)) throw invalid(`${what} must be a list of columns`);
    return list.map((name) => column(table, name).name);
}

/** opts.returning: absent/true → '*', false → none, 'col' or { 'a', 'b' } → those. */
function returningOf(table, returning) {
    if (returning === false) return { sql: '', on: false };
    const cols = returning === true || returning == null ? null : columnList(table, returning, 'returning');
    return { sql: ` RETURNING ${cols ? cols.map(quoteIdent).join(', ') : '*'}`, on: true };
}

// ---- helper statements (§56.5.3) -------------------------------------------------------------------------

function valuesOf(table, map, what) {
    if (!isMapLike(map)) throw invalid(`${what} must be a map`);
    const cols = [];
    const params = [];
    let override = false;
    for (const [name, v] of mapEntries(map)) {
        if (v === undefined) continue;
        const col = writableColumn(table, name);
        cols.push(name);
        params.push(toParam(col, v, table.name));
        if (col.identityAlways) override = true;
    }
    return { cols, params, override };
}

function buildInsert(table, values, opts) {
    const qt = quoteIdent(table.name);
    const { cols, params, override } = valuesOf(table, values, 'values');
    const ret = returningOf(table, opts && opts.returning);
    const text = cols.length
        ? `INSERT INTO ${qt} (${cols.map(quoteIdent).join(', ')})${override ? ' OVERRIDING SYSTEM VALUE' : ''} VALUES (${params.map((_, i) => `$${i + 1}`).join(', ')})`
        : `INSERT INTO ${qt} DEFAULT VALUES`;
    return { text: text + ret.sql, values: params, returning: ret.on };
}

/** One multi-row INSERT per chunk; a key missing from a row takes the column DEFAULT. */
function buildInsertMany(table, rows) {
    if (!Array.isArray(rows)) throw invalid('rows must be a list');
    if (!rows.length) return [];
    const order = [];
    const seen = new Map();
    let override = false;
    for (const [i, row] of rows.entries()) {
        if (!isMapLike(row)) throw invalid(`rows[${i + 1}] must be a map`);
        for (const [name, v] of mapEntries(row)) {
            if (v === undefined || seen.has(name)) continue;
            const col = writableColumn(table, name);
            seen.set(name, col);
            order.push(name);
            if (col.identityAlways) override = true;
        }
    }
    const qt = quoteIdent(table.name);
    if (!order.length) return rows.map(() => ({ text: `INSERT INTO ${qt} DEFAULT VALUES`, values: [] }));
    const perChunk = Math.max(1, Math.min(MAX_BULK_ROWS, Math.floor(MAX_PARAMS / order.length)));
    const head = `INSERT INTO ${qt} (${order.map(quoteIdent).join(', ')})${override ? ' OVERRIDING SYSTEM VALUE' : ''} VALUES `;
    const statements = [];
    for (let start = 0; start < rows.length; start += perChunk) {
        const params = [];
        const tuples = rows.slice(start, start + perChunk).map((row) => {
            const map = Array.isArray(row) ? {} : row;
            const cells = order.map((name) => (map[name] === undefined
                ? 'DEFAULT'
                : push(params, toParam(seen.get(name), map[name], table.name))));
            return `(${cells.join(', ')})`;
        });
        statements.push({ text: head + tuples.join(', '), values: params });
    }
    return statements;
}

function buildSelect(table, where, opts, limitOne) {
    const o = opts || {};
    const params = [];
    const cols = columnList(table, o.columns, 'columns');
    let text = `SELECT ${cols ? cols.map(quoteIdent).join(', ') : '*'} FROM ${quoteIdent(table.name)}`;
    text += buildWhere(table, where, params);
    text += buildOrderBy(table, o.orderBy);
    if (limitOne) text += ' LIMIT 1';
    else if (o.limit != null) text += ` LIMIT ${push(params, nonNegativeInt(o.limit, 'limit'))}`;
    if (o.offset != null) text += ` OFFSET ${push(params, nonNegativeInt(o.offset, 'offset'))}`;
    return { text, values: params };
}

function buildCount(table, where) {
    const params = [];
    const text = `SELECT count(*) AS n FROM ${quoteIdent(table.name)}${buildWhere(table, where, params)}`;
    return { text, values: params };
}

function buildUpdate(table, set, where) {
    const { cols, params } = valuesOf(table, set, 'set');
    if (!cols.length) throw invalid('update needs at least one column in set');
    const assignments = cols.map((name, i) => `${quoteIdent(name)} = $${i + 1}`);
    const whereSql = buildWhere(table, where, params);
    if (!whereSql) throw invalid('update needs a non-empty where');
    return { text: `UPDATE ${quoteIdent(table.name)} SET ${assignments.join(', ')}${whereSql}`, values: params };
}

function buildDelete(table, where) {
    const params = [];
    const whereSql = buildWhere(table, where, params);
    if (!whereSql) throw invalid('delete needs a non-empty where');
    return { text: `DELETE FROM ${quoteIdent(table.name)}${whereSql}`, values: params };
}

/** upsert: { text, values, returning, fallback? } — fallback re-reads the row when DO NOTHING hit a conflict. */
function buildUpsert(table, values, conflict, opts) {
    const o = opts || {};
    const target = typeof conflict === 'string' ? [conflict] : conflict;
    if (!Array.isArray(target) || !target.length) throw invalid('upsert needs conflict = column or { columns }');
    const { cols, params, override } = valuesOf(table, values, 'values');
    for (const name of target) {
        column(table, name);
        if (!cols.includes(name)) throw invalid(`upsert conflict column ${table.name}.${name} is missing from values`);
    }
    const update = o.update == null
        ? cols.filter((name) => !target.includes(name))
        : (columnList(table, o.update, 'update') || []).map((name) => writableColumn(table, name).name);
    const ret = returningOf(table, o.returning);
    const qt = quoteIdent(table.name);
    const action = update.length
        ? `DO UPDATE SET ${update.map((name) => `${quoteIdent(name)} = EXCLUDED.${quoteIdent(name)}`).join(', ')}`
        : 'DO NOTHING';
    const text = `INSERT INTO ${qt} (${cols.map(quoteIdent).join(', ')})${override ? ' OVERRIDING SYSTEM VALUE' : ''}`
        + ` VALUES (${params.map((_, i) => `$${i + 1}`).join(', ')}) ON CONFLICT (${target.map(quoteIdent).join(', ')}) ${action}${ret.sql}`;
    let fallback = null;
    if (!update.length && ret.on) {
        const fparams = target.map((name) => params[cols.indexOf(name)]);
        fallback = {
            text: `SELECT ${ret.sql.slice(' RETURNING '.length)} FROM ${qt} WHERE ${target.map((name, i) => `${quoteIdent(name)} = $${i + 1}`).join(' AND ')}`,
            values: fparams,
        };
    }
    return { text, values: params, returning: ret.on, fallback };
}

// ---- the queue's bulk statements (§56.3.3) ---------------------------------------------------------------
//
// A queue entry (queue.js) carries: t, table, pk (column names), cols (sorted column names of `data`),
// sig (t + cols), override (an identity ALWAYS column is written), data (the converted JSON row).

function bulkStatements(chunk, out) {
    const e = chunk[0];
    const qt = quoteIdent(e.table);
    const src = `jsonb_populate_recordset(NULL::${qt}, $1::jsonb)`;
    const list = e.cols.map(quoteIdent).join(', ');
    const override = e.override ? ' OVERRIDING SYSTEM VALUE' : '';
    const pkMatch = e.pk.map((c) => `x.${quoteIdent(c)} = v.${quoteIdent(c)}`).join(' AND ');
    let text;
    switch (e.t) {
        case 'save': {
            const rest = e.cols.filter((c) => !e.pk.includes(c));
            const action = rest.length
                ? `DO UPDATE SET ${rest.map((c) => `${quoteIdent(c)} = EXCLUDED.${quoteIdent(c)}`).join(', ')}`
                : 'DO NOTHING';
            text = `INSERT INTO ${qt} (${list})${override} SELECT ${list} FROM ${src} ON CONFLICT (${e.pk.map(quoteIdent).join(', ')}) ${action}`;
            break;
        }
        case 'patch': {
            const rest = e.cols.filter((c) => !e.pk.includes(c));
            text = `UPDATE ${qt} AS x SET ${rest.map((c) => `${quoteIdent(c)} = v.${quoteIdent(c)}`).join(', ')} FROM ${src} AS v WHERE ${pkMatch}`;
            break;
        }
        case 'remove':
            text = `DELETE FROM ${qt} AS x USING ${src} AS v WHERE ${pkMatch}`;
            break;
        case 'append':
            if (!e.cols.length) {
                for (const one of chunk) out.push({ text: `INSERT INTO ${qt} DEFAULT VALUES`, values: [], entries: [one] });
                return;
            }
            text = `INSERT INTO ${qt} (${list})${override} SELECT ${list} FROM ${src}`;
            break;
        default:
            throw new Error(`unknown entry kind ${e.t}`);
    }
    out.push({ text, values: [JSON.stringify(chunk.map((x) => x.data), markerReplacer)], entries: chunk });
}

/**
 * Entries (in queue order) → statements [{ text, values, entries }]:
 *   1. raw `sql` entries are barriers and run in their exact position;
 *   2. inside a segment, entries group per table (tables in order of first appearance, relative order kept)
 *      and cut into runs of consecutive entries with the same kind and column signature — and a run also ends
 *      where a row identity would repeat (review R1 H1): `UPDATE … FROM` with two source rows for one key
 *      applies an ARBITRARY one, and a bulk upsert with a repeated key fails with 21000;
 *   3. a run becomes one bulk statement per ≤ maxRows rows.
 */
function buildStatements(entries, maxRows) {
    const limit = maxRows || MAX_BULK_ROWS;
    const out = [];
    let segment = [];
    const endSegment = () => {
        const byTable = new Map();
        for (const e of segment) {
            let list = byTable.get(e.table);
            if (!list) byTable.set(e.table, (list = []));
            list.push(e);
        }
        for (const list of byTable.values()) {
            let run = [];
            let ids = new Set();
            const endRun = () => {
                for (let i = 0; i < run.length; i += limit) bulkStatements(run.slice(i, i + limit), out);
                run = [];
                ids = new Set();
            };
            for (const e of list) {
                if (run.length && (run[0].sig !== e.sig || (e.identity && ids.has(e.identity)))) endRun();
                run.push(e);
                if (e.identity) ids.add(e.identity);
            }
            endRun();
        }
        segment = [];
    };
    for (const e of entries) {
        if (e.t === 'sql') {
            endSegment();
            out.push({ text: e.sql, values: e.params, entries: [e] });
        } else {
            segment.push(e);
        }
    }
    endSegment();
    return out;
}

/**
 * Safe mode's units (review R1 M5): runs in ORIGINAL queue order — consecutive entries of one table with the
 * same kind and signature, no repeated row identity, ≤ maxRows; a raw `sql` entry is a run of its own. Safe
 * mode runs every run in a savepoint and bisects a failing one, so constraints can be IMMEDIATE (the original
 * order is the program's order: a parent queued before its child is inserted before it).
 */
function buildSafeRuns(entries, maxRows) {
    const limit = maxRows || MAX_BULK_ROWS;
    const runs = [];
    let run = [];
    let ids = new Set();
    for (const e of entries) {
        const joins = run.length && e.t !== 'sql' && run[0].t !== 'sql' && run[0].table === e.table && run[0].sig === e.sig
            && run.length < limit && !(e.identity && ids.has(e.identity));
        if (!joins && run.length) {
            runs.push(run);
            run = [];
            ids = new Set();
        }
        run.push(e);
        if (e.identity) ids.add(e.identity);
    }
    if (run.length) runs.push(run);
    return runs;
}

/** The statements of ONE safe-mode run (a raw sql entry, or a bulk statement over the run). */
function runStatements(run) {
    if (run[0].t === 'sql') return [{ text: run[0].sql, values: run[0].params, entries: run }];
    const out = [];
    bulkStatements(run, out);
    return out;
}

module.exports = {
    TABLE_RE,
    MAX_BULK_ROWS,
    quoteIdent,
    checkTableName,
    column,
    writableColumn,
    buildWhere,
    buildOrderBy,
    buildInsert,
    buildInsertMany,
    buildSelect,
    buildCount,
    buildUpdate,
    buildDelete,
    buildUpsert,
    buildStatements,
    buildSafeRuns,
    runStatements,
};
