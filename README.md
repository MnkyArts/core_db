# core_db

The Postgres runtime of the `core` framework: connection pool, write-behind queue, migration runner, table
helpers and interactive transactions. `Core.DB` (`core/lib/db/server.lua`, compiled into every server VM)
calls it through `exports.core_db:<fn>(...)`. The contract is **DESIGN §56** in `core/DESIGN.md`.

core_db is its own resource so it survives `restart core` and plugin restarts: the final writes of a stopping
resource are queued synchronously and committed after the caller is gone.

## Operations

- **Deploy:** `core_db` sits next to `core` in `[local]` (symlink). `core` declares `dependency 'core_db'`, so
  `ensure core` starts it first. `server.cfg` keeps `exec core_pg.cfg` (the `core_pg_url` convar).
- **Never `restart core_db` on a live server.** Stopping it stops `core` and every plugin in the same tick, and
  FXServer frees core_db's Node environment in that tick, so the queue's last flush dies with it (the pending
  count is logged as lost). Restart `core` instead: core_db flushes the stopping resource's writes.
- A server shutdown under txAdmin (`txAdmin:events:serverShuttingDown`) starts a flush at once.
- **Status:** `/dbstatus` (core's console command) prints the `status` export: health, pool, queue counters,
  migrations per owner.
- **Backups:** `docker exec core-postgres pg_dump -U core -Fc core > backup.dump`; restore with `pg_restore`.
  `/dbexport` and `/dbimport` are gone.
- **`legacy_documents`** (the renamed pre-§56 `core_documents`) is never dropped automatically. Once every plugin
  imported its collections and the data was checked in game: `DROP TABLE legacy_documents;`.

### Convars (read once at start)

| convar | default | meaning |
|---|---|---|
| `core_pg_url` | — (required) | the Postgres URL. A secret: never logged. Empty → one error line, every call answers `unavailable` |
| `core_db_pool_size` | `10` | max connections of the MAIN pool (awaited calls, transactions); the queue's flush and the migration runner have their own 2-connection system pool |
| `core_db_flush_ms` | `250` | queue flush interval (at least 50 ms) |
| `core_db_statement_timeout_ms` | `10000` | per-statement timeout (migrations run with `SET LOCAL statement_timeout = 0`); every query also has a client-side limit of this + 5 s (a silently dead link fails instead of hanging) |
| `core_db_lock_timeout_ms` | half the statement timeout, ≤ 5000 | how long a flush waits for a row/table lock before it retries later |
| `core_db_migration_lock_timeout_ms` | `30000` | the same inside a migration (it then retries; the barrier stays pending) |
| `core_db_tx_per_owner` | `4` | open transactions (and `Core.DB.stream`s) per resource; one more answers `tx_limit` |
| `core_db_slow_ms` | `250` | slow-query log threshold (`slow query <ms> ms (<owner>): <first 160 chars>`) |

Logs never contain parameters or the URL, and values inside Postgres messages (`invalid input syntax for type
integer: "…"`) are redacted in logs, events and `status` (the caller's own answer keeps them). Health flips
(connection-class failures and recovery) log once and emit the local event `core:hook:dbStatus (healthy, reason)`;
every dropped queued write logs `dropped queued <kind> on <table> by <owner>: <err>` and emits
`core:hook:dbWriteFailed (owner, kind, table, err, key)` — once per owner whose write was merged into it.

### Failures (review R1)

- **connection-class** (driver/network errors, `Connection terminated`, a client-side query timeout,
  `08000 08001 08003 08004 08006 57P01–03 53300`): health flips; the queue requeues the whole batch (never drops)
  with backoff 1 s … 30 s; a migration job keeps its barrier PENDING and retries. `08P01` (a wrong parameter count)
  is a statement error.
- **retryable** (`57014 40P01 40001 55P03 53100 53200 25006`): the queue requeues with backoff; a migration
  retries (lock conflicts without limit, the others 5 times). A batch whose entries failed 5 times in a row runs
  in safe mode; an entry that still fails alone with a retryable error on its 3rd safe-mode attempt is dropped.
- **statement errors**: safe mode — the batch in original order, in runs with a savepoint each, a failing run
  bisected down to the entries that fail alone, which are dropped. Pool saturation retries every 250 ms.
- A flush that runs longer than its watchdog (4 × (statement timeout + 5 s), ≥ 2 min) loses its connection and is
  requeued. Once the database answers again a waiting `sync` does not sit out the backoff.
- A connection that ran a session `SET`/`RESET`/`set_config` (raw SQL or a migration) gets `RESET ALL` before it
  is reused.

### Queue merges (§56.3.2 as revised by review R1)

A newer `save`/`patch`/`remove` of a row merges into its pending entry: save after save or after patch merges the
columns (newer wins); patch after save/patch merges; remove after anything becomes the remove; patch after
remove is dropped; **save after remove keeps the DELETE and queues the save behind it** (cascades and column
defaults happen). A raw `sql` entry is a merge barrier; a keyed one replaces the owner's older one with the same
key and runs at the newer position. Entries never merge across two different migration barriers unless both are
applied, and a newer write of a row waits behind an older one that is held by a pending barrier. Every entry keeps
the barrier that was current when it was queued: held while THAT barrier is pending, dropped only if THAT barrier
failed. One bulk statement never carries the same row twice.

## Exports (DESIGN §56.2.3)

Callbacks are the last argument, called exactly once on the next tick; `err` is `nil` or a string:
`'<SQLSTATE> <message>'` for a Postgres error, else `timeout`, `unavailable`, `forbidden`, `invalid: <what>`,
`migrations_failed: <why>`, `tx_unknown`, `tx_aborted`, `tx_expired`.

| export | signature | answer |
|---|---|---|
| `query` | `(sql, params, opts, cb)` | `cb(err, rows, rowCount)` — one statement (extended protocol) |
| `batch` | `(statements, opts, cb)` | one transaction; `statements = { { sql, params } | { sql, params as list } }`; `cb(err, { { rowCount, rows? } })` (`rows` with `opts.rows`) |
| `crud` | `(op, table, args, txId, cb)` | table helpers, see below; `txId` `0`/`nil` outside a transaction |
| `nextId` | `(name, opts?, cb)` | `cb(err, value)` from `core_counters`; `opts.timeoutMs` as for `query` |
| `txBegin` | `(opts, cb)` | `cb(err, txId)`; `opts.timeoutMs` = the tx deadline, default 10000, max 60000; `tx_limit` beyond `core_db_tx_per_owner` |
| `txQuery` | `(txId, sql, params, opts?, cb)` | like `query` on the tx's client (`opts.timeoutMs`, `opts.scalar`); every statement ends before the tx deadline |
| `txEnd` | `(txId, commit, cb)` | `cb(err)`; commit of an aborted tx rolls back and answers `tx_aborted` |
| `enqueue` | `(entries)` | synchronous: `{ seq = n }` or `{ error = '...' }` (all or nothing) |
| `sync` | `(seq, timeoutMs, cb)` | `cb(err, { dropped = n })` once every entry ≤ `seq` (nil = all so far) committed or dropped |
| `migrate` | `(list)` | synchronous: `{ ok = true }` or `{ error = '...' }` |
| `awaitMigrations` | `(timeoutMs, cb)` | `cb(err)` when the caller's barrier settles |
| `status` | `(cb)` | `cb(nil, { healthy, pool = { total, idle, waiting, system = { total, idle, waiting } }, queue = { pending, inflight, dropped, flushes, lastFlushMs, lastError, backoffMs }, migrations = { [owner] = { version, state } } })` |
| `isHealthy` | `()` | synchronous boolean |

`opts` of `query`/`batch`/`crud`/`nextId`/`txQuery`:

- `{ timeoutMs = n }` — how long the CALLER waits, counted from the moment core_db received the call. core_db
  never starts a statement after that (checked after the migration barrier, the `sync` wait and the connection
  checkout: `timeout`), and every statement runs with `statement_timeout = min(core_db_statement_timeout_ms,
  remaining − 250 ms)`, so a Lua `timeout` means "not applied" (except a connection lost around COMMIT).
  A call can no longer RAISE the statement timeout above `core_db_statement_timeout_ms`.
- `{ scalar = true }` (`query`, `txQuery`) — `cb(err, value, rowCount)`: the first column by field order of the
  first row (nil when there is none).
- `{ sync = true }` — read-your-writes. **Row-precise rule:** a `crud` `select`/`first`/`count` whose `where` pins
  EVERY primary-key column with plain equality (one value each — no list, op or NULL) waits only for queued
  entries of THAT row (save/patch/remove; raw `sql` entries are not considered) and starts no flush when there is
  none. Every other sync read waits for everything queued before the call — except other resources' entries held
  behind their pending migration barrier. A waiting sync starts a flush instead of waiting for the timer.

**Who may call:** `core`, `core_db`, and resources whose manifest declares `dependency 'core'` or
`dependency 'core_db'` (checked once per resource, re-checked when it restarts). Everyone else: `forbidden`.
The invoking resource owns the call: migrations, barriers, keyed `sql` coalescing, transactions and logs.

### `crud` arguments

`args` is one table in either shape:

- **named:** `{ values = {...} (or row), rows = {...}, where = {...}, set = {...}, conflict = 'col' | { 'a', 'b' }, opts = {...} }`
  — option keys (`columns orderBy limit offset returning update sync timeoutMs`) may also sit at the top level;
  any other key is refused (`invalid: crud args key "<k>" ...`).
- **positional:** the Lua helper's arguments after the table name, as a list: `insert {values, opts}`,
  `insertMany {rows, opts}`, `select`/`first`/`count {where, opts}`, `update {set, where, opts}`,
  `delete {where, opts}`, `upsert {values, conflict, opts}`.

Answers: `insert`/`upsert` → the row (`RETURNING *`, or the `opts.returning` columns; `returning = false` → `true`),
`insertMany` → count, `select` → rows, `first` → row or nil, `count` → integer, `update`/`delete` → rowCount.
`update`/`delete` refuse an empty `where`. An upsert whose `opts.update` is empty (`DO NOTHING`) still answers
the existing row.

`where`: `col = value`, `col = Core.DB.NULL` (IS NULL), `col = { a, b }` (`= ANY`; on an array or json column the
list is the value itself), or `Core.DB.op(name, v, v2)` with `= <> < <= > >= like ilike not_like in not_in
between is_null not_null contains overlaps`.

### Values (DESIGN §56.2.4)

In: the NULL marker `{ __null = true }` → NULL (also inside params, arrays and json); timestamp columns take Unix
seconds (fraction = ms) or ISO strings; json/jsonb take any value (JSON-encoded here); arrays take lists; an empty
Lua table is `[]` for a json column. Raw params: lists → Postgres arrays, maps → JSON text.
Out: `int8` → number when |v| ≤ 2^53 (else string), `numeric` → number, `timestamp(tz)` → integer Unix seconds,
`date` → `'YYYY-MM-DD'`, json → value, NULL → absent.

## Build

```sh
cd resources && npm run build -w core_db-build     # src/ → dist/core_db.js (esbuild, pg bundled, pg-native external)
```

`dist/core_db.js` is committed; rebuild after every `src/` change (`tests/bundle.test.mjs` fails on a stale
bundle). No `package.json` or `node_modules` at the resource root (FXServer's yarn builder; the Node sandbox
cannot read `node_modules` behind the symlink) — the build lives in `build/`.

## Tests

```sh
cd resources && node --test core_db/tests/*.test.mjs   # globs, never a directory; or: npm test -w core_db-build
```

They need the throwaway test database `CORE_TEST_PG_URL` (default
`postgres://core_test:core_test@127.0.0.1:5432/core_test`, `core/scripts/test-db.sh up`) and fail with
`the test database is unreachable — run scripts/test-db.sh up` without it. Every suite works in its own schema
(`t_<suite>`), so suites run in parallel and never touch `public`.

`fxlint core_db` → 0/0: `.fxlintrc.json` ignores C008 because fxlint reads JS built-ins (`new Map()`, `Number()`,
`Date`, …) as unknown natives. The only natives are in `src/fivem.js` (checked with `fxref show`).

### The Lua test bridge (`tests/bridge.mjs`, DESIGN §56.10.2)

`node core_db/tests/bridge.mjs <fifoIn> <fifoOut>` runs the real code in test mode behind a fake adapter and
speaks JSON lines: requests `{ id, invoker, fn, args }` (a callback is `{ "$cb": k }`), answers `{ cb, args }`,
`{ event, args }` and finally `{ done = id, ret }` once the call returned and all its callbacks fired. `enqueue`
flushes before `done`. Bridge functions: `$reset` (truncate the run's tables, keep migrations), `$recreate`
(drop and re-create the run's schema), `$sql` (`{ sql, params }` → rows or `{ error }`), `$fail` (pattern =
JS RegExp source, err string; no pattern clears), `$mapResource` (resource → directory).

Each bridge process works in its own schema (`CORE_TEST_PG_SCHEMA`, else `t_<pid>`), created at start
(`search_path = <schema>, public`), dropped on EOF/SIGTERM/SIGINT (`CORE_TEST_PG_KEEP=1` keeps it); schemas
`t_<digits>` of dead processes are swept at start, or with `node core_db/tests/bridge.mjs --gc`.
`CORE_DB_BRIDGE_VERBOSE=1` prints info logs to stderr.

## Files

| path | role |
|---|---|
| `fxmanifest.lua` | server only, `node_version '22'`, loads `dist/core_db.js` |
| `src/index.js` | entry: convars → `createCoreDb(fivem, options)`, exports, lifecycle hooks |
| `src/fivem.js` | the real FiveM adapter (the only file touching FiveM globals) |
| `src/core.js` | `createCoreDb`: assembly, invoker gate, lifecycle hooks (genuine stops only), test mode |
| `src/api.js` | the export functions |
| `src/calls.js` | options, argument shapes, per-call deadlines and statement budgets |
| `src/errors.js` | error classes (connection / retryable / statement), error strings, redaction |
| `src/pool.js` | `pg.Pool`, type parsers, health, slow log, error strings, test failure injection |
| `src/catalog.js` | tables/columns/primary keys of the current schema, cached |
| `src/sqlgen.js` | identifiers, WHERE/ORDER BY, helper statements, the queue's bulk statements |
| `src/values.js` | value conversion by column type, NULL markers, raw params |
| `src/crud.js` | the `crud` export's argument shapes and answers |
| `src/entries.js` | queue entry validation |
| `src/queue.js` | coalescing, flush scheduling, requeue + backoff, barriers, drops |
| `src/waits.js` | `sync` / read-your-writes waiters and the per-owner drop memory |
| `src/flush.js` | one batch: bulk statements in one transaction, safe mode with bisection, watchdog |
| `src/migrate.js` | the migration runner and per-owner barriers |
| `src/tx.js` | interactive transactions and their deadlines |
| `build/package.json` | the npm workspace `core_db-build` (esbuild, pg) |
| `dist/core_db.js` | the committed bundle |
| `tests/` | `*.test.mjs` suites, `helpers.mjs`, `bridge.mjs` |
