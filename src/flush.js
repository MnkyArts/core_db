'use strict';
// core_db/src/flush.js — executes one queue batch (DESIGN §56.3.3, §56.3.5; review R1 C1/H3/M1/M5/M6).
//
//   normal:    BEGIN; SET LOCAL lock_timeout; <bulk statements>; COMMIT    (one transaction, SYSTEM pool client)
//   a statement error (constraint, bad data, bug, a deferred FK at COMMIT) → safe mode:
//   safe mode: BEGIN; SET LOCAL lock_timeout; SET CONSTRAINTS ALL IMMEDIATE; the batch cut into runs in ORIGINAL
//              order (sqlgen.buildSafeRuns), each in SAVEPOINT e … RELEASE SAVEPOINT e; a failing run is rolled
//              back to its savepoint and BISECTED (≈ log2 n round trips) down to the entries that fail alone,
//              which are dropped; COMMIT
//   isolation: an entry that was part of ISOLATE_AFTER consecutive failed batches sends the batch straight to
//              safe mode, so one entry can never stall the queue for ever.
//   thrown to queue.js (the WHOLE batch is requeued, nothing dropped, backoff):
//     - a connection-class error anywhere (the database is gone), or pool saturation;
//     - a retryable error (57014, 40P01, 40001, 55P03, 53100, 53200, 25006) — in normal mode always; in safe
//       mode until the ONE entry that fails alone did so SAFE_RETRIES times, then it is dropped like a bad one.
//   A watchdog destroys the client when a flush runs longer than `watchdogMs` (a link that went silent under a
//   running statement); the in-flight statement then fails as a connection error and the batch is requeued.

const { buildStatements, buildSafeRuns, runStatements } = require('./sqlgen');
const { describe, classify } = require('./pool');

const ISOLATE_AFTER = 5;
const SAFE_RETRIES = 3;

/**
 * @param {{ pool: object, log: Function, lockTimeoutMs: number, watchdogMs: number, maxRows?: number }} o
 * @returns {(batch: object[]) => Promise<{ dropped: { entry: object, err: string }[], mode: string }>}
 */
function createExecutor(o) {
    const { pool } = o;
    const control = { internal: true };

    async function rollbackQuietly(client) {
        try {
            await client.query('ROLLBACK');
        } catch (_err) {
            // the connection is gone or no transaction is open — nothing to undo
        }
    }

    async function begin(client, safe) {
        await pool.exec(client, 'BEGIN', [], control);
        if (o.lockTimeoutMs > 0) await pool.exec(client, "SELECT set_config('lock_timeout', $1, true)", [`${o.lockTimeoutMs}`], control);
        if (safe) await pool.exec(client, 'SET CONSTRAINTS ALL IMMEDIATE', [], control);
    }

    async function run(client, statements) {
        for (const s of statements) {
            await pool.exec(client, s.text, s.values, { owner: s.entries.length ? s.entries[0].owner : 'core_db' });
        }
    }

    /** One run (or half of one) inside a savepoint; bisects on a statement error. */
    async function attempt(client, entries, dropped) {
        await pool.exec(client, 'SAVEPOINT e', [], control);
        try {
            await run(client, runStatements(entries));
            await pool.exec(client, 'RELEASE SAVEPOINT e', [], control);
            return;
        } catch (err) {
            const kind = classify(err);
            if (kind === 'connection' || kind === 'saturated') throw err;
            await pool.exec(client, 'ROLLBACK TO SAVEPOINT e', [], control);
            await pool.exec(client, 'RELEASE SAVEPOINT e', [], control);
            if (entries.length > 1) {
                const mid = entries.length >> 1;
                await attempt(client, entries.slice(0, mid), dropped);
                await attempt(client, entries.slice(mid), dropped);
                return;
            }
            const entry = entries[0];
            if (kind === 'retryable') {
                entry.transient = (entry.transient || 0) + 1;
                // Retry later with everything else (the order is kept); a stubborn entry is dropped.
                if (entry.transient < SAFE_RETRIES) throw err;
            }
            dropped.push({ entry, err: describe(err) });
        }
    }

    async function safeMode(client, batch) {
        const dropped = [];
        await begin(client, true);
        for (const entries of buildSafeRuns(batch, o.maxRows)) await attempt(client, entries, dropped);
        try {
            await pool.exec(client, 'COMMIT', [], control);
        } catch (err) {
            if (classify(err) !== 'statement') throw err;
            // Constraints are immediate here, so this should not happen; if it does nothing committed.
            await rollbackQuietly(client);
            const reason = describe(err);
            return {
                mode: 'safe',
                dropped: batch.filter((e) => !dropped.some((d) => d.entry === e)).map((entry) => ({ entry, err: reason })).concat(dropped),
            };
        }
        return { mode: 'safe', dropped };
    }

    function watchdog(client) {
        if (!(o.watchdogMs > 0)) return () => {};
        const timer = setTimeout(() => {
            o.log('warn', `flush watchdog: no answer within ${o.watchdogMs} ms — dropping the connection, the batch is requeued`);
            const err = new Error('flush watchdog');
            client.coreDbBroken = client.coreDbBroken || err;
            try {
                client.connection.stream.destroy(err);
            } catch (_e) {
                // already gone
            }
        }, o.watchdogMs);
        if (typeof timer.unref === 'function') timer.unref();
        return () => clearTimeout(timer);
    }

    return async function execute(batch) {
        const isolate = batch.some((e) => (e.fails || 0) >= ISOLATE_AFTER);
        return pool.withClient(async (client) => {
            const stop = watchdog(client);
            try {
                if (!isolate) {
                    try {
                        await begin(client, false);
                        await run(client, buildStatements(batch, o.maxRows));
                        await pool.exec(client, 'COMMIT', [], control);
                        return { mode: 'normal', dropped: [] };
                    } catch (err) {
                        await rollbackQuietly(client);
                        if (classify(err) !== 'statement') throw err;
                    }
                }
                try {
                    return await safeMode(client, batch);
                } catch (err) {
                    await rollbackQuietly(client);
                    throw err;
                }
            } finally {
                stop();
            }
        }, 'system');
    };
}

module.exports = { createExecutor, ISOLATE_AFTER, SAFE_RETRIES };
