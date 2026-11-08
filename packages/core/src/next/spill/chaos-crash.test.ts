/**
 * The concurrent storage workload of the browser kill campaign (browser/next-storage-chaos.ts),
 * crashed by power loss at a seeded physical step of the simulated disk, over the OPFS backend
 * (simulated handles) and the IndexedDB backend (the simulated IndexedDB). A browser kill cannot
 * stop a synchronous OPFS call halfway, so interruption inside interleaved operations is checked
 * here: the crash lands wherever the step count falls, while several lanes' operations
 * are under way. The record of every message is kept in-process, so the expectation is exact; a
 * second process recovers against it exactly as in the browser (root, every acknowledged log byte,
 * exact audit) and must not report a corruption.
 *
 * Reach (review B1-p2g finding 2) counts storage calls issued and not yet returned at the crash,
 * snapshotted at the crashing step (`SimDisk` `onCrash`): a call is counted once it has run, never
 * while it waits to start. The calls run at once and their results come back a task later, so
 * other lanes run in between.
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
import type { MetadataStore, ScratchSpace, StorageBackend } from "../storage/contract.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { createOpfsBackend, type OpfsDirectoryLike } from "../storage/opfs.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";

const SEEDS = seedsFor(
  "next-storage-concurrent-crash",
  Array.from({ length: 24 }, (_, index) => 0xc4a5 + index * 7919),
);

/** Storage calls issued and not yet returned to the workload (by method), as of now. */
export class IssuedCalls {
  readonly #active = new Map<number, string>();
  readonly #random: () => number;
  #next = 0;

  /** `random` (seeded) picks each call's latency: 1 to 3 tasks. */
  constructor(random: () => number = () => 0) {
    this.#random = random;
  }

  /** Runs `call` now and returns its result 1–3 tasks later; counted from the call until then. */
  async run<T>(method: string, call: () => Promise<T> | T): Promise<T> {
    const id = (this.#next += 1);
    this.#active.set(id, method);
    try {
      const result = await call();
      for (let tasks = 1 + Math.floor(this.#random() * 3); tasks > 0; tasks--) await task();
      return result;
    } finally {
      this.#active.delete(id);
    }
  }

  snapshot(): string[] {
    return [...this.#active.values()];
  }
}

const task = () =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });

/** Wraps every function member of `target` so its calls go through `calls.run`. */
function issuing<T extends object>(target: T, calls: IssuedCalls, prefix: string): T {
  return new Proxy(target, {
    get(inner, key) {
      const value: unknown = Reflect.get(inner, key);
      if (typeof value !== "function") return value;
      const method = (value as (...args: unknown[]) => unknown).bind(inner);
      // logLength is synchronous; everything else returns a promise.
      if (key === "logLength") return method;
      if (key === "metadata")
        return () => {
          const metadata = method() as MetadataStore | null;
          return metadata === null ? null : issuing(metadata, calls, "metadata.");
        };
      return (...args: unknown[]) =>
        calls.run(`${prefix}${String(key)}`, () => method(...args) as Promise<unknown>);
    },
  });
}

/**
 * The backend with every session and scratch call's result handed back 1–3 tasks after the call ran:
 * both simulated stores finish a call within one task, so without this the workload's lanes would
 * only ever be inside one operation at a time. With it, other lanes run while one waits for its
 * result, as over a real, asynchronous platform, and the power loss lands among interleaved calls.
 */
function yielding(backend: StorageBackend, calls: IssuedCalls): StorageBackend {
  return {
    fence: backend.fence,
    acquire: (options) => backend.acquire(options),
    scratch: (lease) => {
      const scratch = backend.scratch(lease);
      return scratch === null ? null : issuing<ScratchSpace>(scratch, calls, "spill.");
    },
    async open(lease) {
      return issuing(await backend.open(lease), calls, "");
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
      else if (message.chaos === "failure") view.failure = message.message;
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
    it.each(
      SEEDS.flatMap(
        (seed) =>
          [
            [seed, false],
            [seed, true],
          ] as const,
      ),
    )(
      `${kind}: recovers exactly what was acknowledged (seed %i, target overlap %s)`,
      { timeout: 60_000 },
      async (seed, overlap) => {
        const random = mulberry32(seed);
        // A physical step is a file write or flush on OPFS, but a whole committed transaction on
        // the simulated IndexedDB (its write-ahead log record), so the ranges differ.
        const crashAtStep =
          kind === "opfs" ? 200 + Math.floor(random() * 6_000) : 10 + Math.floor(random() * 900);
        // The first process's calls and open operations, snapshotted at the crashing step.
        const first = observer(false);
        const calls = new IssuedCalls(mulberry32(seed ^ 0x1a7e));
        const crash: { at: { issued: string[]; open: ChaosStep[] } | null } = { at: null };
        const disk: SimDisk = new SimDisk({
          seed,
          // Keep every original uniform physical cut. The added dimension waits for an
          // issued-call overlap; private metadata I/O must not make this reach accidental.
          get crashAtStep(): number {
            if (!overlap) return crashAtStep;
            const issued = calls.snapshot();
            const data = issued.some((method) =>
              ["writeData", "publish", "releaseData"].includes(method),
            );
            const log = issued.some((method) =>
              ["appendLog", "flushLog", "truncateLog", "resetLog", "writeLogHeader"].includes(
                method,
              ),
            );
            const writers = [...first.view.inFlight.values()].filter(
              (intent) =>
                intent.step === "write" &&
                first.view.began.has(intent.sequence) &&
                !first.view.settled.has(intent.sequence),
            ).length;
            const reached =
              seed % 3 === 0
                ? data && log
                : seed % 3 === 1
                  ? issued.includes("writeData") && writers >= 2
                  : issued.length >= 2 && issued.some((method) => method.startsWith("spill."));
            return reached ? disk.steps - 1 : -1;
          },
          onCrash: () => {
            crash.at = {
              issued: calls.snapshot(),
              open: [...first.view.inFlight.values()]
                .filter((intent) => first.view.began.has(intent.sequence))
                .filter((intent) => !first.view.settled.has(intent.sequence))
                .map((intent) => intent.step),
            };
          },
        });
        const backendOf = (): StorageBackend =>
          kind === "opfs"
            ? createOpfsBackend({ directory: directoryOf(disk), name: "db", locks: new SimLocks() })
            : createIndexedDbBackend({
                database: () => simIndexedDb(disk),
                name: "db",
                locks: new SimLocks(),
              });
        // The first process runs until the power fails under it.
        await expect(
          runChaos(
            "db",
            seed,
            null,
            { ...first.hooks, backendFor: async () => yielding(backendOf(), calls) },
            { mode: "concurrent" },
          ),
        ).rejects.toThrow();
        expect(disk.crashes, "the run ended by the planned power loss").toBe(1);
        const intents = [...first.view.inFlight.values()];
        const inOperation: ChaosStep[] = intents
          .filter((intent) => first.view.began.has(intent.sequence))
          .filter((intent) => !first.view.settled.has(intent.sequence))
          .map((intent) => intent.step);
        // Reach: the storage calls issued and not returned when the power failed.
        const snapshot = crash.at ?? { issued: [], open: [] };
        const issued = snapshot.issued;
        const entered: string[] = [];
        const count = (methods: readonly string[]) =>
          issued.filter((method) => methods.includes(method)).length;
        const DATA = ["writeData", "publish", "releaseData"];
        const LOG = ["appendLog", "flushLog", "truncateLog", "resetLog", "writeLogHeader"];
        if (issued.length >= 2) entered.push("two-or-more-calls-issued");
        if (issued.length >= 3) entered.push("three-or-more-calls-issued");
        if (count(DATA) >= 1 && count(LOG) >= 1) entered.push("data-call-with-log-call");
        // The space's write lock admits one data write at a time (space.ts `writeLock`), so two
        // issued data writes never coexist; the writers' interleaving is a data write issued while
        // both write operations are open.
        if (
          count(["writeData"]) >= 1 &&
          snapshot.open.filter((step) => step === "write").length >= 2
        )
          entered.push("data-write-with-two-writers-open");
        if (issued.some((method) => method.startsWith("spill.")) && issued.length >= 2)
          entered.push("spill-call-with-another");
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
          { ...second.hooks, backendFor: async () => yielding(backendOf(), new IssuedCalls()) },
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
    it(`${kind}: the crashes landed among interleaved storage calls`, () => {
      reach.assertReached();
    });
  }
});

describe("the sweep's reach counts calls that ran (review B1-p2g finding 2)", () => {
  it("a call is counted from the moment it runs until its result is returned, never before", async () => {
    const calls = new IssuedCalls();
    const ran: string[] = [];
    const session = issuing(
      {
        writeData: async () => {
          ran.push("writeData");
          // Every counted call has run: the snapshot taken inside the call includes it.
          expect(calls.snapshot()).toEqual(["writeData"]);
        },
      },
      calls,
      "",
    );
    const pending = session.writeData();
    // Synchronously after the call: it has run, and it is counted.
    expect(ran).toEqual(["writeData"]);
    expect(calls.snapshot()).toEqual(["writeData"]);
    await pending;
    expect(calls.snapshot()).toEqual([]);
  });
});
