/**
 * The concurrent storage workload of the browser kill campaign (browser/next-storage-chaos.ts),
 * crashed by power loss at a seeded physical step of the simulated disk, over the OPFS backend
 * (simulated handles) and the IndexedDB backend (the simulated IndexedDB). A browser kill cannot
 * land inside Chromium's synchronous OPFS calls, so interruption inside interleaved operations is
 * checked here: the crash lands wherever the step count falls, while several lanes' operations
 * are under way. The record of every message is kept in-process, so the expectation is exact; a
 * second process recovers against it exactly as in the browser (root, every acknowledged log byte,
 * exact audit) and must not report a corruption.
 */
import { describe, expect, it } from "vitest";
import {
  runChaos,
  type ChaosExpect,
  type ChaosIntent,
  type ChaosMessage,
  type ChaosRecovery,
  type ChaosState,
  type ChaosStep,
} from "./chaos-workload.js";
import { createReach, STORAGE_CONCURRENT_CRASH_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { StorageBackend } from "../storage/contract.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { createOpfsBackend, type OpfsDirectoryLike } from "../storage/opfs.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";

const SEEDS = seedsFor(
  "next-storage-concurrent-crash",
  Array.from({ length: 24 }, (_, index) => 0xc4a5 + index * 7919),
);

/**
 * The backend with every asynchronous session call deferred by a task: both simulated stores
 * finish a call within one task, so without this the workload's lanes would only ever be inside
 * one operation at a time. With it, other lanes start their operations while one waits, as they do
 * over a real, asynchronous platform, and the power loss lands among interleaved operations.
 */
function yielding(backend: StorageBackend): StorageBackend {
  const task = () =>
    new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
  return {
    fence: backend.fence,
    acquire: (options) => backend.acquire(options),
    scratch: (lease) => backend.scratch(lease),
    async open(lease) {
      const session = await backend.open(lease);
      return new Proxy(session, {
        get(target, key) {
          const value: unknown = Reflect.get(target, key);
          if (typeof value !== "function") return value;
          const method = (value as (...args: unknown[]) => unknown).bind(target);
          // logLength is synchronous; everything else returns a promise.
          if (key === "logLength") return method;
          return async (...args: unknown[]) => {
            await task();
            return method(...args);
          };
        },
      });
    },
  };
}

const directoryOf = (disk: SimDisk): OpfsDirectoryLike => ({
  getFileHandle: async (name) => ({
    createSyncAccessHandle: async () => disk.open(name, true),
  }),
});

/** One process's view of the workload: what the page would know, kept exactly. */
function observer(stopAfterRecovery: boolean) {
  const view = {
    state: null as ChaosState | null,
    inFlight: new Map<number, ChaosIntent>(),
    began: new Set<number>(),
    settled: new Set<number>(),
    recovered: null as ChaosRecovery | null,
    failure: null as string | null,
  };
  let release: (() => void) | null = null;
  const recovered = new Promise<void>((resolve) => {
    release = resolve;
  });
  const hooks = {
    post: (message: ChaosMessage) => {
      if (message.chaos === "recovered") {
        view.recovered = message.recovery;
        view.state = message.recovery.state;
        release?.();
      } else if (message.chaos === "ack") view.state = message.state;
      else if (message.chaos === "intent")
        view.inFlight.set(message.intent.sequence, message.intent);
      else if (message.chaos === "began") view.began.add(message.sequence);
      else if (message.chaos === "settled") view.settled.add(message.sequence);
      else if (message.chaos === "done") view.inFlight.delete(message.sequence);
      else view.failure = message.message;
    },
    // The recovering process stops its workload once recovery is verified (no go ever comes).
    go: () =>
      stopAfterRecovery && view.recovered !== null
        ? new Promise<void>(() => undefined)
        : Promise.resolve(),
  };
  return { view, hooks, recovered };
}

describe("the concurrent storage workload under power loss at any physical step", () => {
  for (const kind of ["opfs", "indexeddb"] as const) {
    const reach = createReach(`concurrent crash ${kind}`, STORAGE_CONCURRENT_CRASH_REACH, {
      minimum: 5,
    });
    it.each(SEEDS)(
      `${kind}: recovers exactly what was acknowledged (seed %i)`,
      { timeout: 60_000 },
      async (seed) => {
        const random = mulberry32(seed);
        // A physical step is a file write or flush on OPFS, but a whole committed transaction on
        // the simulated IndexedDB (its write-ahead log record), so the ranges differ.
        const crashAtStep =
          kind === "opfs" ? 200 + Math.floor(random() * 6_000) : 10 + Math.floor(random() * 900);
        const disk = new SimDisk({ seed, crashAtStep });
        const backendOf = (): StorageBackend =>
          kind === "opfs"
            ? createOpfsBackend({ directory: directoryOf(disk), name: "db", locks: new SimLocks() })
            : createIndexedDbBackend({
                database: () => simIndexedDb(disk),
                name: "db",
                locks: new SimLocks(),
              });
        const backendFor = async () => yielding(backendOf());
        // The first process runs until the power fails under it.
        const first = observer(false);
        await expect(
          runChaos("db", seed, null, { ...first.hooks, backendFor }, { mode: "concurrent" }),
        ).rejects.toThrow();
        expect(disk.crashes, "the run ended by the planned power loss").toBe(1);
        const intents = [...first.view.inFlight.values()];
        const inOperation: ChaosStep[] = intents
          .filter((intent) => first.view.began.has(intent.sequence))
          .filter((intent) => !first.view.settled.has(intent.sequence))
          .map((intent) => intent.step);
        const entered: string[] = [];
        const has = (steps: readonly ChaosStep[]) =>
          inOperation.some((step) => steps.includes(step));
        if (inOperation.length >= 2) entered.push("two-or-more-in-operation");
        if (inOperation.length >= 3) entered.push("three-or-more-in-operation");
        if (
          has(["write", "publish", "reclaim"]) &&
          has(["log-append", "log-flush", "truncate", "reset"])
        )
          entered.push("data-with-log");
        if (inOperation.filter((step) => step === "write").length >= 2)
          entered.push("write-with-write");
        if (has(["spill"]) && inOperation.length >= 2) entered.push("spill-with-another");
        reach.recordHistory(entered);
        // A crash before the first recovery finished leaves nothing acknowledged.
        const expectation: ChaosExpect | null =
          first.view.state === null ? null : { state: first.view.state, inFlight: intents };
        // A new process recovers against everything the first acknowledged.
        const second = observer(true);
        const recovering = runChaos(
          "db",
          seed + 1,
          expectation,
          { ...second.hooks, backendFor },
          { mode: "concurrent" },
        );
        recovering.catch(() => undefined);
        await Promise.race([
          second.recovered,
          recovering.then(
            () => undefined,
            (error: unknown) => {
              throw new Error(
                `seed ${String(seed)}: recovery after the crash at step ${String(disk.steps)} failed with ${inOperation.length.toString()} operations in storage (${inOperation.join("+")}): ${String(error)}`,
                { cause: error },
              );
            },
          ),
        ]);
        expect(second.view.failure).toBeNull();
        expect(second.view.recovered).not.toBeNull();
      },
    );
    it(`${kind}: the crashes landed inside interleaved operations`, () => {
      reach.assertReached();
    });
  }
});
