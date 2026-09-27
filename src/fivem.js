'use strict';
// core_db/src/fivem.js — the real adapter over the FiveM globals (DESIGN §56.2.2).
//
// core.js reaches FiveM ONLY through this object, so the node tests and tests/bridge.mjs run the same code
// with a fake adapter. Natives (fxref-checked 2026-09-27, CFX, apiset shared): GetCurrentResourceName,
// GetInvokingResource, GetConvar, LoadResourceFile, GetResourceState, GetNumResourceMetadata,
// GetResourceMetadata. JS runtime functions: exports, on, emit.

const RESOURCE = GetCurrentResourceName();

// Inside a CommonJS module the bare name `exports` is the module object, not CitizenFX's export
// registrar (§33.1); take whichever of the two is callable.
const registrar = typeof exports === 'function' ? exports : global.exports;

const DEPENDENCY_KEY = 'dependency'; // `dependencies { ... }` is stored under the same key

module.exports = {
    resourceName() {
        return RESOURCE;
    },

    /** The resource whose runtime invoked the export — read SYNCHRONOUSLY at the top of every call. */
    invoker() {
        return GetInvokingResource();
    },

    registerExport(name, fn) {
        registrar(name, fn);
    },

    getConvar(name, fallback) {
        return GetConvar(name, fallback);
    },

    /** A file of any resource as a string, or null. */
    loadResourceFile(res, path) {
        const text = LoadResourceFile(res, path);
        return typeof text === 'string' ? text : null;
    },

    resourceState(res) {
        return GetResourceState(res);
    },

    /** true when the resource's manifest declares `dependency 'core'` or `dependency 'core_db'`. */
    dependsOnCore(res) {
        const count = GetNumResourceMetadata(res, DEPENDENCY_KEY) || 0;
        for (let i = 0; i < count; i += 1) {
            const dep = GetResourceMetadata(res, DEPENDENCY_KEY, i);
            if (dep === 'core' || dep === 'core_db') return true;
        }
        return false;
    },

    on(event, fn) {
        on(event, fn);
    },

    /** A local server event (TriggerEvent). */
    emit(event, ...args) {
        emit(event, ...args);
    },

    /** EVERY call into a Lua funcref (and every native call from a Node callback) goes through here. */
    defer(fn) {
        setImmediate(fn);
    },

    log(level, message) {
        const line = `[core_db] ${level === 'info' ? '' : `${level}: `}${message}`;
        if (level === 'error') console.error(line);
        else if (level === 'warn') console.warn(line);
        else console.log(line);
    },
};
