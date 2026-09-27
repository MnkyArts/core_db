'use strict';
// core_db/src/catalog.js — table/column/primary-key introspection of the CURRENT schema (DESIGN §56.2.1).
//
// The whole catalog is one query and is kept in memory: `enqueue` validates synchronously against it.
// `invalidate()` reloads in the background and keeps serving the previous snapshot until the new one is in.
// An async lookup that misses reloads once (at most every MISS_RELOAD_MS) before answering "unknown", so a
// table created by raw SQL is found without an explicit invalidate.

const MISS_RELOAD_MS = 1000;

const CATALOG_SQL = `
SELECT c.relname AS tbl, c.relkind AS relkind, a.attname AS col,
       bt.typname AS typ, bt.typcategory AS cat, et.typname AS elem,
       a.attnotnull AS notnull, a.atthasdef AS hasdef, a.attidentity AS ident, a.attgenerated AS gen,
       array_position(pk.indkey::int2[], a.attnum) AS pkpos
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
  JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
  JOIN pg_catalog.pg_type bt ON bt.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END
  LEFT JOIN pg_catalog.pg_type et ON et.oid = bt.typelem AND bt.typcategory = 'A'
  LEFT JOIN pg_catalog.pg_index pk ON pk.indrelid = c.oid AND pk.indisprimary
 WHERE c.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema())
   AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
 ORDER BY c.relname, a.attnum`;

/** The conversion class of a column type (§56.2.4). */
function kindOf(typ, cat) {
    if (typ === 'json' || typ === 'jsonb') return 'json';
    if (typ === 'timestamptz' || typ === 'timestamp' || typ === 'date') return typ;
    if (cat === 'A') return 'array';
    return 'plain';
}

function buildTables(rows) {
    const tables = new Map();
    for (const r of rows) {
        let table = tables.get(r.tbl);
        if (!table) {
            table = { name: r.tbl, view: r.relkind === 'v' || r.relkind === 'm', columns: new Map(), pk: [], required: [] };
            tables.set(r.tbl, table);
        }
        const column = {
            name: r.col,
            type: r.typ,
            kind: kindOf(r.typ, r.cat),
            elemKind: r.elem ? kindOf(r.elem, null) : null,
            notNull: r.notnull === true,
            hasDefault: r.hasdef === true || r.ident !== '',
            identityAlways: r.ident === 'a',
            generated: r.gen !== '' && r.gen != null,
            pkPos: r.pkpos == null ? null : r.pkpos,
        };
        table.columns.set(r.col, column);
    }
    for (const table of tables.values()) {
        const pkCols = [...table.columns.values()].filter((c) => c.pkPos != null).sort((a, b) => a.pkPos - b.pkPos);
        table.pk = pkCols.map((c) => c.name);
        for (const c of table.columns.values()) {
            if (c.notNull && !c.hasDefault && !c.generated) table.required.push(c.name);
        }
    }
    return tables;
}

/** @param {{ pool: object, log: Function }} o */
function createCatalog(o) {
    let tables = null;
    let loading = null;
    let again = false;
    let loadedAt = 0;
    let generation = 0; // bumps on reset(), so a load started before a reset never lands after it

    async function loadOnce() {
        const gen = generation;
        const result = await o.pool.withClient((client) => o.pool.exec(client, CATALOG_SQL, [], { owner: 'core_db' }));
        if (gen !== generation) return;
        tables = buildTables(result.rows);
        loadedAt = Date.now();
    }

    /** (Re)loads the catalog; concurrent callers share one load, an invalidate during a load runs another. */
    function reload() {
        if (loading) {
            again = true;
            return loading;
        }
        loading = (async () => {
            try {
                do {
                    again = false;
                    await loadOnce();
                } while (again);
            } finally {
                loading = null;
            }
            return tables;
        })();
        return loading;
    }

    function invalidate() {
        reload().catch((err) => o.log('warn', `catalog reload failed: ${err && err.message ? err.message : err}`));
    }

    /** Synchronous lookup against the current snapshot (null when never loaded or unknown). */
    function table(name) {
        return tables ? tables.get(name) || null : null;
    }

    /** Async lookup: loads when needed, and reloads once on a miss. */
    async function tableAsync(name) {
        if (!tables) await reload();
        let found = tables.get(name);
        if (!found && Date.now() - loadedAt >= MISS_RELOAD_MS) {
            await reload();
            found = tables.get(name);
        }
        return found || null;
    }

    /** A reload at most every MISS_RELOAD_MS, for lookups that missed (tables altered behind our back). */
    function stale() {
        return Date.now() - loadedAt >= MISS_RELOAD_MS;
    }

    return {
        reload,
        invalidate,
        stale,
        refreshIfStale() {
            if (stale()) invalidate();
        },
        table,
        tableAsync,
        loaded: () => tables !== null,
        /** Drops the snapshot (test reset / schema recreate) and loads a fresh one in the background. */
        reset() {
            generation += 1;
            tables = null;
            loadedAt = 0;
            invalidate();
        },
        names: () => (tables ? [...tables.keys()] : []),
    };
}

module.exports = { createCatalog, buildTables, kindOf };
