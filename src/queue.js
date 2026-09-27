'use strict';
// core_db/src/queue.js — the write-behind queue (DESIGN §56.3): coalescing (§56.3.2), flush scheduling,
// requeue + backoff (§56.3.5), owner barriers (§56.3.4) and `sync` accounting. Validating entries (§56.3.1)
// is entries.js; executing one batch (bulk statements, safe mode) is flush.js.
//
// Review R1 changes: an entry keeps the migration BARRIER OBJECT current when it was queued (held while that
// barrier is pending, dropped only if THAT barrier failed — H2); merges never cross two different barriers
// unless both are settled fine (L14); save after save / patch MERGES columns and save after remove keeps the
// DELETE and queues the save behind it (M2); a sync never waits for another owner's entries held behind its
// pending barrier (M3); a row-precise wait serves reads that pin a whole primary key (R4); every merged
// contribution is accounted when an entry is dropped (L8); failed batches count per entry (C1 isolation).

const { isMapLike } = require('./values');
const { describe, redact, classify } = require('./pool');
const { createEntries, finalize } = require('./entries');
const { createWaits } = require('./waits');

const SOON_THRESHOLD = 2000;       // pending entries that trigger a flush on the next tick
const WARN_PENDING = 50000;        // warn (once per minute) above this
const BACKOFF_FIRST_MS = 1000;
const BACKOFF_MAX_MS = 30000;
const CONTRIB_MAX = 256;           // merged contributions remembered per entry (drop accounting)

function unref(timer) {
    if (timer && typeof timer.unref === 'function') timer.unref();
    return timer;
}

const held = (e) => !!(e.barrier && e.barrier.state === 'pending');
const doomed = (e) => !!(e.barrier && e.barrier.state === 'failed');
/** A barrier under which merging is harmless: none, or applied. */
const fine = (b) => !b || b.state === 'applied';

/**
 * @param {{ catalog, log, emit, execute: (batch) => Promise<{ dropped: { entry, err }[] }>,
 *           currentBarrier: (owner) => object|null, onDdl: () => Promise, flushMs: number, retryFirstMs?: number }} o
 */
function createQueue(o) {
    let pending = [];
    const rowIndex = new Map();   // row identity → the pending entry a newer write of that row merges into
    const sqlIndex = new Map();   // owner + key → pending sql entry
    let inflight = null;
    let seq = 0;
    let flushing = null;
    let again = false;
    let soon = false;
    let retryTimer = null;
    let retryDelay = 0;
    let backoff = 0;
    let lastWarnAt = 0;
    const stats = { dropped: 0, flushes: 0, lastFlushMs: 0, lastError: null };

    const validator = createEntries(o.catalog, o.log);
    const waits = createWaits({
        outstanding: () => ({ pending, inflight }),
        currentSeq: () => seq,
        flushSoon: () => scheduleSoon(),
    });
    const settle = () => waits.settle();

    // ---- coalescing (§56.3.2) ----------------------------------------------------------------------------

    /** Two entries may merge only under the same barrier, or when both barriers are settled fine (L14). */
    function canMerge(ex, inc) {
        return ex.barrier === inc.barrier || (fine(ex.barrier) && fine(inc.barrier));
    }

    function absorb(ex, inc) {
        ex.owner = inc.owner;
        ex.barrier = inc.barrier;
        ex.seq = Math.max(ex.seq, inc.seq);
        ex.minSeq = Math.min(ex.minSeq, inc.minSeq);
        for (const c of inc.contrib) ex.contrib.push(c);
        if (ex.contrib.length > CONTRIB_MAX) ex.contrib.splice(0, ex.contrib.length - CONTRIB_MAX);
        finalize(ex);
    }

    /** Merges `inc` into the older pending entry `ex` in place (ex keeps its position); false = queue inc. */
    function coalesce(ex, inc) {
        if (inc.t === 'patch') {
            if (ex.t === 'remove') return true; // the row is going away: the patch is dropped
            Object.assign(ex.data, inc.data); // save + patch stays a save, patch + patch a patch (newer wins)
            ex.override = ex.override || inc.override;
        } else if (inc.t === 'save') {
            if (ex.t === 'remove') return false; // keep the DELETE (cascades, defaults); the save follows it
            ex.t = 'save'; // save after save / after patch: the columns merge, newer wins
            Object.assign(ex.data, inc.data);
            ex.override = ex.override || inc.override;
        } else {
            ex.t = 'remove'; // remove after anything → the remove
            ex.data = inc.data;
            ex.override = false;
        }
        absorb(ex, inc);
        return true;
    }

    function add(e) {
        if (e.t === 'sql') {
            // A raw statement is a coalescing BARRIER (§56.3.2): it may touch any row, so a row entry queued
            // after it must never merge into an entry queued before it (a save → DELETE → save would lose the
            // second save to the DELETE).
            rowIndex.clear();
            if (e.sqlKey) {
                const ex = sqlIndex.get(e.sqlKey);
                if (ex && canMerge(ex, e)) {
                    // the newer statement supersedes the older one: drop the older, run the newer at ITS position
                    const i = pending.indexOf(ex);
                    if (i !== -1) pending.splice(i, 1);
                    e.minSeq = Math.min(ex.minSeq, e.minSeq);
                }
                sqlIndex.set(e.sqlKey, e);
            }
        } else if (e.identity) {
            const ex = rowIndex.get(e.identity);
            if (ex && canMerge(ex, e) && coalesce(ex, e)) return;
            rowIndex.set(e.identity, e);
        }
        pending.push(e);
    }

    /** Rebuilds both indexes from `pending`, honouring the raw-statement barriers of add(). */
    function reindex() {
        rowIndex.clear();
        sqlIndex.clear();
        for (const e of pending) {
            if (e.t === 'sql') {
                rowIndex.clear();
                if (e.sqlKey) sqlIndex.set(e.sqlKey, e);
            } else if (e.identity) {
                rowIndex.set(e.identity, e);
            }
        }
    }

    /** enqueue(owner, entries) → { seq } | { error } — synchronous, all or nothing. */
    function enqueue(owner, entries) {
        let list = entries;
        if (!Array.isArray(list)) {
            if (isMapLike(list) && !Object.keys(list).length) list = [];
            else return { error: 'invalid: entries must be a list' };
        }
        let prepared;
        try {
            prepared = list.map((raw, i) => validator.prepare(owner, raw, i));
        } catch (err) {
            const error = describe(err);
            if (error.startsWith('invalid: unknown column')) o.catalog.refreshIfStale();
            return { error };
        }
        const barrier = o.currentBarrier(owner);
        for (const e of prepared) {
            seq += 1;
            if (!e) continue;
            e.seq = seq;
            e.minSeq = seq;
            e.barrier = barrier;
            e.contrib = [{ owner, seq }];
            e.fails = 0;
            add(e);
        }
        if (pending.length > SOON_THRESHOLD) scheduleSoon();
        if (pending.length > WARN_PENDING && Date.now() - lastWarnAt >= 60000) {
            lastWarnAt = Date.now();
            o.log('warn', `${pending.length} queued writes are pending`);
        }
        return { seq };
    }

    // ---- drops and sync accounting -------------------------------------------------------------------------

    /** The dropped row's primary key (a value, or { col = value } for a composite key); null for sql/append. */
    function keyOf(e) {
        if (!e.pk || !e.pk.length || e.t === 'append' || !e.data) return null;
        if (e.pk.length === 1) return e.data[e.pk[0]] ?? null;
        const key = {};
        for (const c of e.pk) key[c] = e.data[c];
        return key;
    }

    /** Every contribution merged into the entry counts for its owner's next sync (L8); one event per owner. */
    function recordDrop(e, err) {
        stats.dropped += 1;
        const owners = new Set();
        for (const c of e.contrib) {
            waits.recordDrop(c.owner, c.seq);
            owners.add(c.owner);
        }
        const text = redact(err);
        for (const owner of owners) o.emit('core:hook:dbWriteFailed', owner, e.t, e.table, text, keyOf(e));
    }

    function dropOne(e, err) {
        o.log('error', `dropped queued ${e.t}${e.table ? ` on ${e.table}` : ''} by ${e.owner}: ${redact(err)}`);
        recordDrop(e, err);
    }

    /** One error log per burst (§56.3.4), one event per entry. */
    function dropBurst(owner, list, err) {
        o.log('error', `dropped ${list.length} queued write(s) by ${owner}: ${redact(err)}`);
        for (const e of list) recordDrop(e, err);
    }

    // ---- flushing (§56.3.3, §56.3.5) -----------------------------------------------------------------------

    function scheduleSoon() {
        if (soon) return;
        soon = true;
        setImmediate(() => {
            soon = false;
            if (!retryTimer) flush();
        });
    }

    function scheduleRetry(err) {
        const kind = classify(err);
        let delay;
        if (kind === 'saturated') {
            delay = Math.max(o.flushMs || 0, 250); // pool saturation is not an outage: no exponential backoff
        } else {
            backoff = backoff ? Math.min(backoff * 2, BACKOFF_MAX_MS) : (o.retryFirstMs || BACKOFF_FIRST_MS);
            delay = backoff;
        }
        o.log('warn', `flush failed (${redact(describe(err))}): ${pending.length} queued write(s) kept, retry in ${delay} ms`);
        if (retryTimer) clearTimeout(retryTimer);
        retryDelay = delay;
        retryTimer = unref(setTimeout(() => {
            retryTimer = null;
            flush();
        }, delay));
    }

    /** The batch goes back to the FRONT in its original order; newer pending entries coalesce into it. */
    function requeue(batch) {
        const merged = batch.concat(pending);
        pending = [];
        rowIndex.clear();
        sqlIndex.clear();
        for (const e of merged) add(e);
    }

    async function runFlush() {
        const batch = [];
        const keep = [];
        const lost = new Map(); // failed barrier → its entries
        const heldRows = new Set(); // a newer write of a row waits behind an older held one (order, L14)
        for (const e of pending) {
            if (held(e) || (e.identity && heldRows.has(e.identity))) {
                keep.push(e);
                if (e.identity) heldRows.add(e.identity);
            } else if (doomed(e)) {
                if (!lost.has(e.barrier)) lost.set(e.barrier, []);
                lost.get(e.barrier).push(e);
            } else batch.push(e);
        }
        pending = keep;
        reindex();
        for (const [barrier, list] of lost) dropBurst(barrier.owner, list, `migrations_failed: ${barrier.reason}`);
        if (!batch.length) {
            settle();
            return;
        }
        inflight = batch;
        const started = Date.now();
        try {
            const result = await o.execute(batch);
            stats.flushes += 1;
            stats.lastFlushMs = Date.now() - started;
            backoff = 0;
            for (const d of result.dropped) {
                dropOne(d.entry, d.err);
                stats.lastError = redact(d.err);
            }
            if (batch.some((e) => e.ddl)) await o.onDdl();
        } catch (err) {
            stats.lastError = redact(describe(err));
            // a failure to connect is not the entries' fault; anything later counts toward isolation (C1)
            if (!err || !err.atConnect) for (const e of batch) e.fails = (e.fails || 0) + 1;
            inflight = null;
            requeue(batch);
            scheduleRetry(err);
        } finally {
            inflight = null;
            settle();
        }
    }

    /** Starts a flush unless one is in flight (then another follows it). */
    function flush() {
        if (flushing) {
            again = true;
            return flushing;
        }
        flushing = runFlush()
            .catch((err) => o.log('error', `flush crashed: ${describe(err)}`))
            .finally(() => {
                flushing = null;
                if (again) {
                    again = false;
                    if (pending.length && !retryTimer) scheduleSoon();
                }
            });
        return flushing;
    }

    /** A flush that includes everything queued before the call (waits out one in flight, skips the backoff). */
    async function flushNow() {
        while (flushing) await flushing;
        if (retryTimer) {
            clearTimeout(retryTimer);
            retryTimer = null;
        }
        await flush();
    }

    let timer = null;
    if (o.flushMs > 0) {
        timer = unref(setInterval(() => {
            if (!retryTimer && pending.length) flush();
        }, o.flushMs));
    }

    return {
        enqueue,
        waitFor: waits.waitFor,
        waitForRow: waits.waitForRow,
        flush,
        flushNow,
        /** Test mode: flush, and while entries are held by a PENDING barrier wait for it and flush again. */
        async flushAll(timeoutMs) {
            const deadline = Date.now() + (timeoutMs || 30000);
            for (;;) {
                await flushNow();
                const barriers = new Set(pending.filter(held).map((e) => e.barrier));
                const left = deadline - Date.now();
                if (!barriers.size || left <= 0) return;
                await Promise.race([
                    Promise.all([...barriers].map((b) => b.promise)),
                    new Promise((resolve) => unref(setTimeout(resolve, left))),
                ]);
            }
        },
        /** A barrier settled: flush its held entries on the next tick. */
        kick() {
            if (pending.length) scheduleSoon();
        },
        /** The database answered again: a waiting sync should not sit out the backoff (M6). */
        healthBack() {
            if (retryTimer && waits.waiting()) {
                clearTimeout(retryTimer);
                retryTimer = null;
                flush();
            }
        },
        currentSeq: () => seq,
        stats: () => ({
            pending: pending.length,
            inflight: inflight ? inflight.length : 0,
            dropped: stats.dropped,
            flushes: stats.flushes,
            lastFlushMs: stats.lastFlushMs,
            lastError: stats.lastError,
            backoffMs: retryTimer ? retryDelay : 0,
        }),
        /** Entries that would be lost if the Node environment died now. */
        unflushed: () => pending.length + (inflight ? inflight.length : 0),
        /** Test reset: waits for a flush in flight, then forgets everything queued. */
        async reset() {
            while (flushing) await flushing;
            if (retryTimer) clearTimeout(retryTimer);
            retryTimer = null;
            backoff = 0;
            pending = [];
            rowIndex.clear();
            sqlIndex.clear();
            stats.lastError = null;
            waits.reset();
        },
        stop() {
            if (timer) clearInterval(timer);
            if (retryTimer) clearTimeout(retryTimer);
            timer = null;
            retryTimer = null;
        },
    };
}

module.exports = { createQueue };
