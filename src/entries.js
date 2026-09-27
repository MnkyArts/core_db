'use strict';
// core_db/src/entries.js — validation of queue entries (DESIGN §56.3.1): synchronous, against the in-memory
// catalog. A raw entry from `enqueue` becomes { t, owner, table, pk, data, cols, sig, override, identity }
// (save/patch/remove/append; `data` is the converted JSON row of §56.2.4) or { t = 'sql', sql, params, sqlKey,
// ddl }. Every refusal throws `invalid: ...` (or 'unavailable' before the catalog was ever loaded).

const {
    isPlainObject, isNullMarker, isMapLike, mapEntries, toJson, rawParams, invalid,
} = require('./values');
const { checkTableName, writableColumn } = require('./sqlgen');
const { coreError } = require('./pool');

const DDL_RE = /^\s*(?:(?:--[^\n]*\n|\/\*[\s\S]*?\*\/)\s*)*(?:create|alter|drop|do)\b/i;
const SEP = '\u0001';

/** The coalescing identity of a row: table + its primary-key values (converted like the payload). */
function identityOf(tableName, pk, data) {
    return tableName + SEP + pk.map((c) => {
        const v = data[c];
        return v !== null && typeof v === 'object' ? JSON.stringify(v) : `${v}`;
    }).join(SEP);
}

/**
 * The identity a `where` pins with plain equality on EVERY primary-key column (row-precise read-your-writes),
 * or null when it does not (a list, an op, NULL, a missing column).
 */
function pinnedIdentity(table, where) {
    if (!table.pk.length || !isPlainObject(where)) return null;
    const data = {};
    for (const c of table.pk) {
        const v = where[c];
        if (v == null || typeof v === 'object') return null;
        try {
            data[c] = toJson(table.columns.get(c), v, table.name);
        } catch (_err) {
            return null;
        }
    }
    return identityOf(table.name, table.pk, data);
}

/** The highest $n placeholder of a statement, ignoring string literals, quoted names and comments. */
function highestPlaceholder(sql) {
    const bare = sql
        .replace(/--[^\n]*/g, ' ')
        .replace(/\/\*[\s\S]*?\*\//g, ' ')
        .replace(/\$([A-Za-z_]\w*)?\$[\s\S]*?\$\1\$/g, ' ')
        .replace(/'(?:[^']|'')*'/g, ' ')
        .replace(/"(?:[^"]|"")*"/g, ' ');
    let max = 0;
    for (const m of bare.matchAll(/\$(\d+)/g)) max = Math.max(max, Number(m[1]));
    return max;
}

/** Recomputes the column signature (runs of §56.3.3 group entries with equal kind + sorted columns). */
function finalize(e) {
    e.cols = Object.keys(e.data).sort();
    e.sig = e.t + SEP + e.cols.join(SEP);
    return e;
}

function createEntries(catalog, log) {
    const warnedPlaceholders = new Set();

    /** Optional check (R1 C1): a raw statement whose params do not match its placeholders fails at flush. */
    function checkPlaceholders(owner, sql, count) {
        const highest = highestPlaceholder(sql);
        if (highest === count || !log) return;
        const key = `${owner}${SEP}${sql}`;
        if (warnedPlaceholders.has(key)) return;
        if (warnedPlaceholders.size > 1000) warnedPlaceholders.clear();
        warnedPlaceholders.add(key);
        log('warn', `queued sql by ${owner} uses $${highest} but has ${count} param(s) — Postgres will refuse it: ${sql.slice(0, 120)}`);
    }

    function fill(e, table, map, where, what) {
        if (!isMapLike(map)) throw invalid(`${where}: ${what} must be a map`);
        for (const [name, v] of mapEntries(map)) {
            if (v === undefined) continue;
            const col = writableColumn(table, name);
            e.data[name] = toJson(col, v, table.name);
            if (col.identityAlways) e.override = true;
        }
    }

    function fillKey(e, table, key, where) {
        const pk = table.pk;
        if (isPlainObject(key) && !isNullMarker(key)) {
            for (const name of Object.keys(key)) {
                if (!pk.includes(name)) throw invalid(`${where}: key column ${table.name}.${name} is not part of the primary key`);
            }
            for (const name of pk) {
                const v = key[name];
                if (v == null || isNullMarker(v)) throw invalid(`${where}: key needs primary key column ${table.name}.${name}`);
                e.data[name] = toJson(table.columns.get(name), v, table.name);
            }
            return;
        }
        if (pk.length === 1 && key != null && !Array.isArray(key) && !isNullMarker(key)) {
            e.data[pk[0]] = toJson(table.columns.get(pk[0]), key, table.name);
            return;
        }
        throw invalid(`${where}: key must be the primary key value or { ${pk.join(', ')} }`);
    }

    /** One raw entry → a queue entry, null (a no-op), or a throw with the `invalid: ...` reason. */
    function prepare(owner, raw, index) {
        const where = `entry ${index + 1}`;
        if (!isPlainObject(raw)) throw invalid(`${where} must be a table`);
        const t = raw.t;
        if (t === 'sql') {
            if (typeof raw.sql !== 'string' || !raw.sql.trim()) throw invalid(`${where}: sql must be a non-empty string`);
            const key = raw.key;
            if (key != null && typeof key !== 'string' && typeof key !== 'number') throw invalid(`${where}: key must be a string or a number`);
            const params = rawParams(raw.params);
            checkPlaceholders(owner, raw.sql, params.length);
            return {
                t, owner, table: null, sql: raw.sql, params,
                sqlKey: key == null ? null : `${owner}${SEP}${key}`, ddl: DDL_RE.test(raw.sql),
            };
        }
        if (t !== 'save' && t !== 'patch' && t !== 'remove' && t !== 'append') {
            throw invalid(`${where}: unknown kind "${`${t}`.slice(0, 16)}"`);
        }
        const name = checkTableName(raw.table);
        const table = catalog.table(name);
        if (!table) {
            if (!catalog.loaded()) {
                catalog.invalidate();
                throw coreError('unavailable');
            }
            catalog.refreshIfStale(); // a table created behind core_db's back shows up for a later call
            throw invalid(`unknown table ${name}`);
        }
        if (t !== 'append' && !table.pk.length) throw invalid(`${where}: table ${name} has no primary key`);
        const e = { t, owner, table: name, pk: table.pk, data: {}, override: false };
        if (t === 'save' || t === 'append') {
            fill(e, table, raw.row, where, 'row');
            if (t === 'save') {
                for (const c of table.pk) if (e.data[c] == null) throw invalid(`${where}: save on ${name} needs primary key column ${c}`);
                for (const c of table.required) if (e.data[c] == null) throw invalid(`${where}: save on ${name} needs column ${c} (NOT NULL without a default)`);
            }
        } else {
            fillKey(e, table, raw.key, where);
            if (t === 'patch') {
                const keyCols = Object.keys(e.data);
                fill(e, table, raw.changes, where, 'changes');
                for (const c of table.pk) {
                    if (raw.changes && !Array.isArray(raw.changes) && Object.prototype.hasOwnProperty.call(raw.changes, c)) {
                        throw invalid(`${where}: patch cannot change primary key column ${name}.${c}`);
                    }
                }
                if (Object.keys(e.data).length === keyCols.length) return null; // nothing to change
            }
        }
        if (t !== 'append') e.identity = identityOf(name, table.pk, e.data);
        return finalize(e);
    }

    return { prepare };
}

module.exports = { createEntries, finalize, identityOf, pinnedIdentity, highestPlaceholder, DDL_RE, SEP };
