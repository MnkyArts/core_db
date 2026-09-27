'use strict';
// core_db/src/values.js — values across the hop, Lua → Postgres (DESIGN §56.2.4).
//
// What arrives over msgpack: a Lua sequence is an Array, a Lua map an object, an EMPTY Lua table an empty
// Array (it cannot say whether it was a map). `Core.DB.NULL` is the marker `{ __null: true }`,
// `Core.DB.op(name, v, v2)` is `{ __op, value, value2 }` (WHERE only).
//
// Three conversions live here:
//   toParam(column, v)    a bind parameter for a helper statement (catalog-typed)
//   toJson(column, v)     a value inside the queue's jsonb_populate_recordset payload (catalog-typed)
//   rawParams(params)     positional params of raw SQL (only markers, arrays and objects are touched)

function isPlainObject(v) {
    return v !== null && typeof v === 'object' && !Array.isArray(v) && !Buffer.isBuffer(v);
}

function isNullMarker(v) {
    return isPlainObject(v) && v.__null === true;
}

function isOp(v) {
    return isPlainObject(v) && typeof v.__op === 'string';
}

/** A Lua map as it arrives: an object, or an empty Array (the empty Lua table). */
function isMapLike(v) {
    return isPlainObject(v) || (Array.isArray(v) && v.length === 0);
}

function mapEntries(v) {
    return Array.isArray(v) ? [] : Object.entries(v);
}

/** JSON.stringify replacer: the NULL marker anywhere inside a value becomes JSON null. */
function markerReplacer(_key, value) {
    return isNullMarker(value) ? null : value;
}

function toJsonText(value) {
    return JSON.stringify(value === undefined ? null : value, markerReplacer);
}

class Invalid extends Error {
    constructor(what) {
        super(`invalid: ${what}`);
        this.coreDb = `invalid: ${what}`;
    }
}

function invalid(what) {
    return new Invalid(what);
}

/** Unix seconds (ms precision kept when fractional) or an ISO string → ISO string. */
function toTimestamp(v, where) {
    if (typeof v === 'string') return v;
    if (typeof v === 'number' && Number.isFinite(v)) {
        const date = new Date(Math.round(v * 1000));
        if (!Number.isNaN(date.getTime())) return date.toISOString();
    }
    throw invalid(`${where} needs Unix seconds or an ISO string`);
}

/** 'YYYY-MM-DD' (or Unix seconds → that day in UTC). */
function toDate(v, where) {
    if (typeof v === 'string') return v;
    if (typeof v === 'number' && Number.isFinite(v)) {
        const date = new Date(Math.round(v * 1000));
        if (!Number.isNaN(date.getTime())) return date.toISOString().slice(0, 10);
    }
    throw invalid(`${where} needs 'YYYY-MM-DD'`);
}

/** One scalar by kind; json values are left as values (the caller encodes). */
function scalarByKind(kind, v, where) {
    if (v == null || isNullMarker(v)) return null;
    if (kind === 'timestamptz' || kind === 'timestamp') return toTimestamp(v, where);
    if (kind === 'date') return toDate(v, where);
    return v;
}

function whereOf(column, tableName) {
    return `${tableName ? `${tableName}.` : ''}${column.name}`;
}

/** A bind parameter for a helper statement. */
function toParam(column, v, tableName) {
    if (v == null || isNullMarker(v)) return null;
    const where = whereOf(column, tableName);
    switch (column.kind) {
        case 'json':
            return toJsonText(v);
        case 'timestamptz':
        case 'timestamp':
            return toTimestamp(v, where);
        case 'date':
            return toDate(v, where);
        case 'array':
            if (typeof v === 'string') return v; // an array literal
            if (!Array.isArray(v)) throw invalid(`${where} needs a list`);
            return v.map((item) => (column.elemKind === 'json'
                ? (item == null || isNullMarker(item) ? null : toJsonText(item))
                : scalarByKind(column.elemKind, item, where)));
        default:
            if (isPlainObject(v)) return toJsonText(v);
            if (Array.isArray(v)) return v.map((item) => (isNullMarker(item) ? null : item));
            return v;
    }
}

/** A value inside a queue payload (the whole payload is JSON-encoded once, markers → null). */
function toJson(column, v, tableName) {
    if (v == null || isNullMarker(v)) return null;
    const where = whereOf(column, tableName);
    switch (column.kind) {
        case 'timestamptz':
        case 'timestamp':
            return toTimestamp(v, where);
        case 'date':
            return toDate(v, where);
        case 'array':
            if (!Array.isArray(v)) throw invalid(`${where} needs a list`);
            if (column.elemKind === 'json') return v;
            return v.map((item) => scalarByKind(column.elemKind, item, where));
        default:
            return v;
    }
}

/** Positional params of raw SQL: markers → NULL, arrays → Postgres arrays, objects → JSON text. */
function rawParams(params) {
    if (params == null) return [];
    let list = params;
    if (!Array.isArray(params)) {
        if (!isPlainObject(params)) throw invalid('params must be a list');
        // A Lua list with holes the lib did not pad arrives as a map with integer keys.
        const keys = Object.keys(params);
        if (keys.some((k) => !/^[1-9]\d*$/.test(k))) throw invalid('params must be a list');
        const max = keys.reduce((m, k) => Math.max(m, Number(k)), 0);
        list = [];
        for (let i = 1; i <= max; i += 1) list.push(params[i] === undefined ? null : params[i]);
    }
    return list.map((v) => {
        if (v == null || isNullMarker(v)) return null;
        if (Array.isArray(v)) return v.map((item) => (isNullMarker(item) ? null : (isPlainObject(item) ? toJsonText(item) : item)));
        if (isPlainObject(v)) return toJsonText(v);
        return v;
    });
}

module.exports = {
    isPlainObject,
    isNullMarker,
    isOp,
    isMapLike,
    mapEntries,
    markerReplacer,
    toJsonText,
    toTimestamp,
    toParam,
    toJson,
    rawParams,
    invalid,
    Invalid,
};
