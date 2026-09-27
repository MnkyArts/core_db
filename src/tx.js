'use strict';
// core_db/src/tx.js — interactive transactions (DESIGN §56.2.3 txBegin / txQuery / txEnd).
//
// A tx holds one pooled client from BEGIN to COMMIT/ROLLBACK and belongs to the resource that began it
// (another resource gets 'tx_unknown'). Calls on one tx run one after another. A statement error leaves the
// tx aborted (every later call answers 'tx_aborted'; txEnd rolls back). The deadline rolls it back and frees
// the client; later calls answer 'tx_expired'. A resource holds at most `perOwner` open transactions (streams
// are transactions too): one more answers 'tx_limit' (review R1 M6).

const { coreError, isConnectionError } = require('./pool');

const DEFAULT_TIMEOUT_MS = 10000;
const MAX_TIMEOUT_MS = 60000;
const REMEMBER_ENDED = 1000;
const DEFAULT_PER_OWNER = 4;

/** @param {{ pool, log, perOwner?: number, statementTimeoutMs?: number }} o */
function createTxManager(o) {
    const perOwner = o.perOwner > 0 ? o.perOwner : DEFAULT_PER_OWNER;
    const txs = new Map();     // id → { id, owner, client, state, chain, timer, broken }
    const ended = new Map();   // id → { owner, code } for expired txs (bounded)
    let nextId = 1;

    function remember(tx, code) {
        ended.set(tx.id, { owner: tx.owner, code });
        if (ended.size > REMEMBER_ENDED) ended.delete(ended.keys().next().value);
    }

    function serialize(tx, fn) {
        const run = tx.chain.then(fn);
        tx.chain = run.catch(() => {});
        return run;
    }

    /** ROLLBACK straight on the client (never through a test failure rule) and give the client back. */
    function closeLater(tx) {
        tx.chain = tx.chain.then(async () => {
            let broken = tx.broken || tx.client.coreDbBroken;
            if (!broken) {
                try {
                    await tx.client.query('ROLLBACK');
                } catch (err) {
                    broken = err;
                }
            }
            await o.pool.release(tx.client, broken || undefined);
        });
    }

    function expire(tx, code, why) {
        if (!txs.has(tx.id)) return;
        clearTimeout(tx.timer);
        txs.delete(tx.id);
        tx.state = 'expired';
        if (code) remember(tx, code);
        if (why) o.log('warn', `transaction ${tx.id} of ${tx.owner} ${why} — rolled back`);
        closeLater(tx);
    }

    function lookup(owner, id) {
        const tx = txs.get(id);
        if (tx && tx.owner === owner) return tx;
        const gone = ended.get(id);
        if (gone && gone.owner === owner) throw coreError(gone.code);
        throw coreError('tx_unknown');
    }

    function openOf(owner) {
        let n = 0;
        for (const tx of txs.values()) if (tx.owner === owner) n += 1;
        return n + (opening.get(owner) || 0);
    }
    const opening = new Map(); // owner → txBegin calls waiting for a client

    async function begin(owner, opts) {
        const wanted = opts && typeof opts.timeoutMs === 'number' && opts.timeoutMs > 0 ? opts.timeoutMs : DEFAULT_TIMEOUT_MS;
        const timeoutMs = Math.min(Math.floor(wanted), MAX_TIMEOUT_MS);
        if (openOf(owner) >= perOwner) throw coreError('tx_limit');
        opening.set(owner, (opening.get(owner) || 0) + 1);
        let client;
        try {
            client = await o.pool.acquire('main');
        } finally {
            const n = opening.get(owner) - 1;
            if (n) opening.set(owner, n);
            else opening.delete(owner);
        }
        try {
            await o.pool.exec(client, 'BEGIN', [], { owner });
        } catch (err) {
            await o.pool.release(client, isConnectionError(err) ? err : undefined);
            throw err;
        }
        const tx = {
            id: nextId, owner, client, state: 'open', chain: Promise.resolve(), timer: null, broken: null, ddl: false,
            deadline: { at: Date.now() + timeoutMs }, // statements run with statement_timeout ≤ what is left of it
            stmtTimeout: o.statementTimeoutMs || 0,   // the statement_timeout currently in force in the tx
        };
        nextId += 1;
        tx.timer = setTimeout(() => expire(tx, 'tx_expired', `passed its ${timeoutMs} ms deadline`), timeoutMs);
        if (typeof tx.timer.unref === 'function') tx.timer.unref();
        txs.set(tx.id, tx);
        return tx.id;
    }

    /**
     * Runs fn(client) inside the tx; a database error aborts the tx (a validation error does not).
     * `beforeStart` (optional) runs once the tx's earlier calls are done — it throws to refuse a call whose
     * caller already gave up (the per-call deadline, M4).
     */
    function run(owner, id, fn, beforeStart) {
        const tx = lookup(owner, id);
        return serialize(tx, async () => {
            if (tx.state === 'expired') throw coreError('tx_expired');
            if (tx.state === 'aborted') throw coreError('tx_aborted');
            if (tx.state !== 'open') throw coreError('tx_unknown');
            if (beforeStart) beforeStart();
            try {
                return await fn(tx.client, tx);
            } catch (err) {
                if (!(err && typeof err.coreDb === 'string' && err.coreDb.startsWith('invalid'))) {
                    tx.state = 'aborted';
                    if (isConnectionError(err)) tx.broken = err;
                }
                throw err;
            }
        });
    }

    /**
     * COMMIT (or ROLLBACK when commit is false or the tx is aborted — then 'tx_aborted' for a commit).
     * Ownership is checked (lookup) before anything about the tx is touched (L13). Resolves { ddl }: whether a
     * committed statement changed the schema (the catalog reloads).
     */
    function end(owner, id, commit) {
        const tx = lookup(owner, id);
        return serialize(tx, async () => {
            if (tx.state === 'expired') throw coreError('tx_expired');
            const aborted = tx.state === 'aborted';
            clearTimeout(tx.timer);
            txs.delete(tx.id);
            tx.state = 'ended';
            let broken = tx.broken || tx.client.coreDbBroken;
            try {
                if (commit && !aborted && !broken) {
                    await o.pool.exec(tx.client, 'COMMIT', [], { owner });
                    return { ddl: tx.ddl };
                }
                if (!broken) {
                    try {
                        await tx.client.query('ROLLBACK');
                    } catch (err) {
                        broken = err;
                    }
                }
                if (commit) throw coreError('tx_aborted');
                return { ddl: false };
            } catch (err) {
                if (isConnectionError(err)) broken = err;
                throw err;
            } finally {
                await o.pool.release(tx.client, broken || undefined);
            }
        });
    }

    /** A statement of this tx changed the schema: the catalog reloads when it commits. */
    function markDdl(owner, id) {
        const tx = txs.get(id);
        if (tx && tx.owner === owner) tx.ddl = true;
    }

    return {
        begin,
        run,
        end,
        markDdl,
        /** A resource stopped: roll back its open transactions now. */
        releaseOwner(owner) {
            for (const tx of [...txs.values()]) if (tx.owner === owner) expire(tx, null, null);
        },
        open: () => txs.size,
        async reset() {
            const all = [...txs.values()];
            for (const tx of all) expire(tx, null, null);
            await Promise.all(all.map((tx) => tx.chain));
            ended.clear();
        },
    };
}

module.exports = { createTxManager, DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS };
