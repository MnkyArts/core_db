// core_db/tests/gate.test.mjs — the API shape (§56.2.3), the invoker gate (§56.1), unavailable mode, callbacks.
import test from 'node:test';
import assert from 'node:assert/strict';
import { setup, fakeFivem, createCoreDb, INTERNAL, EXPORTS, TEST_EXPORTS, sleep } from './helpers.mjs';

test('api keys are exactly the export names; test exports only in test mode', async (t) => {
    assert.deepEqual(EXPORTS, ['query', 'batch', 'crud', 'nextId', 'txBegin', 'txQuery', 'txEnd', 'enqueue', 'sync',
        'migrate', 'awaitMigrations', 'status', 'isHealthy']);
    const live = await setup('t_gate1', { testMode: false });
    t.after(live.teardown);
    assert.deepEqual(Object.keys(live.api), EXPORTS);
    const testing = await setup('t_gate2');
    t.after(testing.teardown);
    assert.deepEqual(Object.keys(testing.api), EXPORTS.concat(TEST_EXPORTS));
    for (const name of Object.keys(testing.api)) assert.equal(typeof testing.api[name], 'function');
});

test('invoker gate: core, core_db and dependants are allowed; the verdict is cached until the resource restarts', async (t) => {
    const ctx = await setup('t_gate3', { testMode: false, deps: { plugin_ok: true } });
    t.after(ctx.teardown);
    let asked = 0;
    const depends = ctx.fivem.dependsOnCore;
    ctx.fivem.dependsOnCore = (res) => {
        asked += 1;
        return depends(res);
    };
    assert.deepEqual(await ctx.callAs('core', 'query', 'SELECT 1 AS x', [], null), [null, [{ x: 1 }], 1]);
    assert.deepEqual(await ctx.callAs('core_db', 'query', 'SELECT 1 AS x', [], null), [null, [{ x: 1 }], 1]);
    assert.equal((await ctx.callAs('plugin_ok', 'query', 'SELECT 1 AS x', [], null))[0], null);
    assert.equal(asked, 1);

    assert.deepEqual(await ctx.callAs('random_res', 'query', 'SELECT 1', [], null), ['forbidden']);
    assert.deepEqual(await ctx.callAs('random_res', 'status'), ['forbidden']);
    assert.deepEqual(ctx.syncAs('random_res', 'enqueue', []), { error: 'forbidden' });
    assert.deepEqual(ctx.syncAs('random_res', 'migrate', []), { error: 'forbidden' });
    assert.equal(ctx.syncAs('random_res', 'isHealthy'), false);
    assert.equal(asked, 2, 'cached per resource');
    assert.equal(ctx.fivem.logs.filter(([, m]) => m.includes('refused a call from "random_res"')).length, 1, 'logged once');

    ctx.fivem.deps.random_res = true;
    assert.deepEqual(await ctx.callAs('random_res', 'query', 'SELECT 1', [], null), ['forbidden'], 'still cached');
    ctx.internal.hooks.onResourceStart('random_res');
    assert.equal((await ctx.callAs('random_res', 'query', 'SELECT 1', [], null))[0], null, 'dropped on its onResourceStart');

    // No invoking resource (a call from core_db's own runtime) counts as core_db.
    ctx.fivem.current = null;
    const own = await new Promise((resolve) => ctx.api.query('SELECT 1 AS x', [], null, (...a) => resolve(a)));
    ctx.fivem.current = 'core';
    assert.equal(own[0], null);
});

test('callbacks: last function argument, called once through defer, a throwing callback is contained', async (t) => {
    const ctx = await setup('t_gate4');
    t.after(ctx.teardown);
    const short = await new Promise((resolve) => ctx.api.query('SELECT 2 AS y', (...a) => resolve(a)));
    assert.deepEqual(short, [null, [{ y: 2 }], 1], 'trailing optional arguments may be omitted');
    let deferred = 0;
    const defer = ctx.fivem.defer;
    ctx.fivem.defer = (fn) => {
        deferred += 1;
        defer(fn);
    };
    ctx.api.query('SELECT 1', [], null, () => { throw new Error('the plugin stopped'); });
    await sleep(50);
    assert.equal(deferred, 1);
    assert.ok(ctx.fivem.logs.some(([l, m]) => l === 'debug' && m.includes('a callback threw')));
    ctx.api.query('SELECT 1', [], null); // no callback at all: nothing to answer, nothing thrown
    await sleep(20);
});

test('no core_pg_url: one clear error, every call answers unavailable', async () => {
    const fivem = fakeFivem();
    const api = createCoreDb(fivem, { url: '', testMode: true });
    assert.deepEqual(Object.keys(api), EXPORTS.concat(TEST_EXPORTS));
    assert.equal(fivem.logs.filter(([l, m]) => l === 'error' && m.startsWith('core_pg_url is not set')).length, 1);
    for (const name of ['query', 'batch', 'crud', 'nextId', 'txBegin', 'txQuery', 'txEnd', 'sync', 'awaitMigrations', 'status', 'testReset', 'testFlush']) {
        const answer = await new Promise((resolve) => api[name](1, 2, 3, 4, (...a) => resolve(a)));
        assert.deepEqual(answer, ['unavailable'], name);
    }
    assert.deepEqual(api.enqueue([]), { error: 'unavailable' });
    assert.deepEqual(api.migrate([]), { error: 'unavailable' });
    assert.equal(api.isHealthy(), false);
    await api[INTERNAL].close();
});
