'use strict';
// core_db/src/index.js — the FXServer entry (DESIGN §56.2.1). Bundled by build/ into dist/core_db.js.
//
// Reads the convars of §56.2.6 once, builds the API with the real adapter, registers every API function as
// an export, and wires the lifecycle hooks of §56.3.5. The URL is a secret: it is never logged.

const fivem = require('./fivem');
const { createCoreDb, INTERNAL } = require('./core');

function numberConvar(name, fallback) {
    const raw = fivem.getConvar(name, `${fallback}`);
    const n = Number(raw);
    if (!Number.isFinite(n) || n < 0) {
        fivem.log('warn', `convar ${name} = "${raw}" is not a non-negative number, using ${fallback === undefined ? 'the default' : fallback}`);
        return fallback;
    }
    return n;
}

/** A convar that has a derived default in core.js: undefined when it is not set. */
function optionalConvar(name) {
    const raw = fivem.getConvar(name, '');
    return raw === '' ? undefined : numberConvar(name, undefined);
}

function optionsFromConvars() {
    return {
        url: fivem.getConvar('core_pg_url', ''),
        poolSize: Math.max(1, numberConvar('core_db_pool_size', 10)),
        flushMs: numberConvar('core_db_flush_ms', 250),
        statementTimeoutMs: numberConvar('core_db_statement_timeout_ms', 10000),
        slowMs: numberConvar('core_db_slow_ms', 250),
        lockTimeoutMs: optionalConvar('core_db_lock_timeout_ms'),
        migrationLockTimeoutMs: numberConvar('core_db_migration_lock_timeout_ms', 30000),
        txPerOwner: Math.max(1, numberConvar('core_db_tx_per_owner', 4)),
        testMode: false,
    };
}

const options = optionsFromConvars();
const api = createCoreDb(fivem, options);

for (const name of Object.keys(api)) fivem.registerExport(name, api[name]);

const { hooks } = api[INTERNAL];
fivem.on('onResourceStart', (res) => hooks.onResourceStart(res));
fivem.on('onResourceStop', (res) => hooks.onResourceStop(res));
fivem.on('txAdmin:events:serverShuttingDown', () => hooks.onShutdown());

if (options.url) {
    fivem.log('info', `ready (pool ${options.poolSize}, flush ${options.flushMs} ms, statement timeout ${options.statementTimeoutMs} ms)`);
}
