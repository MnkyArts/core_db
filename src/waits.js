'use strict';
// core_db/src/waits.js — `sync` and read-your-writes waiters of the queue (DESIGN §56.2.3, §56.3.3), and the
// per-owner memory of dropped sequence numbers a sync reports. Split out of queue.js; it reads the queue's
// outstanding entries through `o.outstanding()` and asks for a flush with `o.flushSoon()`.
//
//   waitFor(seq, owner)       every entry with sequence ≤ seq committed or dropped — entries of OTHER owners held
//                             behind their pending barrier excepted (review R1 M3)
//   waitForRow(identity, …)   no pending/inflight entry of that row left (review R4) — no flush when there is none

const { coreError } = require('./pool');

const DROP_MEMORY = 10000; // unreported drop seqs kept per owner for `sync`

function unref(timer) {
    if (timer && typeof timer.unref === 'function') timer.unref();
    return timer;
}

const held = (e) => !!(e.barrier && e.barrier.state === 'pending');
/** Another owner's entry held behind ITS pending barrier: `owner` never waits for it (M3). */
const heldFor = (e, owner) => e.owner !== owner && held(e);

/** @param {{ outstanding: () => { pending: object[], inflight: object[]|null }, currentSeq: () => number, flushSoon: Function }} o */
function createWaits(o) {
    let waiters = [];
    const drops = new Map(); // owner → [seq, ...] not yet reported by a sync

    /** A dropped contribution of `owner` (sequence `seq`) for its next sync. */
    function recordDrop(owner, seq) {
        let list = drops.get(owner);
        if (!list) drops.set(owner, (list = []));
        list.push(seq);
        if (list.length > DROP_MEMORY) list.splice(0, list.length - DROP_MEMORY);
    }

    function takeDrops(owner, upTo) {
        const list = drops.get(owner);
        if (!list || !list.length) return 0;
        const keep = list.filter((s) => s > upTo);
        const n = list.length - keep.length;
        if (keep.length) drops.set(owner, keep);
        else drops.delete(owner);
        return n;
    }

    /**
     * One pass over the outstanding entries: the smallest minSeq of entries nobody holds, the smallest one of
     * held entries per owner, and the outstanding entries of the identities row waiters wait for.
     */
    function snapshot(rowIds) {
        let free = Infinity;
        const heldMin = new Map();
        const rows = new Map();
        const visit = (e, isHeld) => {
            if (isHeld) {
                const m = heldMin.get(e.owner);
                if (m === undefined || e.minSeq < m) heldMin.set(e.owner, e.minSeq);
            } else if (e.minSeq < free) free = e.minSeq;
            if (rowIds && e.identity && rowIds.has(e.identity)) {
                if (!rows.has(e.identity)) rows.set(e.identity, []);
                rows.get(e.identity).push(e);
            }
        };
        const { pending, inflight } = o.outstanding();
        for (const e of pending) visit(e, held(e));
        if (inflight) for (const e of inflight) visit(e, false);
        return { free, heldMin, rows };
    }

    function isDone(w, snap) {
        if (w.identity) {
            const list = snap.rows.get(w.identity);
            return !list || list.every((e) => heldFor(e, w.owner));
        }
        const own = snap.heldMin.get(w.owner);
        return w.seq < (own === undefined ? snap.free : Math.min(snap.free, own));
    }

    function finish(w) {
        if (w.timer) clearTimeout(w.timer);
        w.resolve({ dropped: w.countDrops ? takeDrops(w.owner, w.seq) : 0 });
    }

    function settle() {
        if (!waiters.length) return;
        const ids = new Set();
        for (const w of waiters) if (w.identity) ids.add(w.identity);
        const snap = snapshot(ids.size ? ids : null);
        const still = [];
        for (const w of waiters) {
            if (isDone(w, snap)) finish(w);
            else still.push(w);
        }
        waiters = still;
    }

    function register(w, timeoutMs) {
        return new Promise((resolve, reject) => {
            w.resolve = resolve;
            if (isDone(w, snapshot(w.identity ? new Set([w.identity]) : null))) {
                finish(w);
                return;
            }
            if (!(timeoutMs > 0)) {
                reject(coreError('timeout'));
                return;
            }
            w.timer = unref(setTimeout(() => {
                waiters = waiters.filter((x) => x !== w);
                reject(coreError('timeout'));
            }, timeoutMs));
            waiters.push(w);
            o.flushSoon(); // somebody waits: no reason to sit out the rest of the flush interval
        });
    }

    /**
     * Resolves { dropped } once every entry with sequence ≤ seq (null = everything queued so far) was
     * committed or dropped — entries of OTHER owners held behind their pending barrier excepted (M3);
     * `dropped` counts the OWNER's entries in that range dropped since its last sync. Rejects 'timeout'.
     */
    function waitFor(target, owner, timeoutMs, countDrops) {
        const seq = o.currentSeq();
        const upTo = target == null ? seq : Math.min(target, seq);
        return register({ seq: upTo, owner, identity: null, countDrops: countDrops !== false, timer: null }, timeoutMs);
    }

    /** Resolves once no pending/inflight entry of that ROW identity is left (no flush when there is none). */
    function waitForRow(identity, owner, timeoutMs) {
        return register({ seq: o.currentSeq(), owner, identity, countDrops: false, timer: null }, timeoutMs);
    }

    return {
        recordDrop,
        waitFor,
        waitForRow,
        settle,
        waiting: () => waiters.length > 0,
        /** Test reset: forget the drops (waiters settle against the emptied queue). */
        reset() {
            drops.clear();
            settle();
        },
    };
}

module.exports = { createWaits };
