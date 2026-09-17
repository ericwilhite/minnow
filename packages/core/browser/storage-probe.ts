import { autoStoreTestHooks, forgetStoreChoice, openAutoStore } from "../dist/engine/auto-store.js";
import { MinnowDatabase } from "@minnowdb/core";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { deleteOpfsDatabase, opfsDatabaseExists } from "@minnowdb/core/storage/opfs";
import { admitWriter } from "../dist/engine/write-coordinator.js";

interface StorageProbeResult {
  errorName: string;
  indexedDbShadowCreated: boolean;
  rows: unknown[];
}

interface IndexedDbProbeResult {
  errorName: string;
  alternativeOpened: boolean;
  rows: unknown[];
}

interface GarbageCollectionRaceResult {
  insertedAfterSnapshot: boolean;
  droppedBeforeNomination: boolean;
  provenanceRefusals: number;
  missingSegmentCount: number;
  tablesAfterReopen: string[];
  completedJobs: number;
}

interface IndexedDbSequenceResult {
  first: unknown[];
  second: unknown[];
  durableKeyMetadata: boolean;
}

interface WriteAdmissionProgressResult {
  progressingQueueOverlapped: boolean;
  progressingQueueStalls: number;
  progressingQueueWaitedPastOneInterval: boolean;
  /** The writer never ran while the frozen holder still held the lock. */
  frozenQueueBypassed: boolean;
  /** Exactly one stall report for the whole frozen episode, naming another context. */
  frozenQueueStalls: number;
  frozenQueueStallHolder: string | undefined;
  frozenQueueChurnProven: boolean;
  /** The writer ran once the holder let go, under the lock. */
  frozenQueueEnteredAfterRelease: boolean;
  /** A second waiter cancelled while the holder was frozen rejected without waiting for it. */
  frozenQueueCancelledPromptly: boolean;
}

function nativeLockHolder(lockName: string): {
  worker: Worker;
  ready: Promise<void>;
  acquired: Promise<void>;
  released: Promise<void>;
  acquire: () => void;
  release: () => void;
} {
  const worker = new Worker(new URL("./lock-holder-worker.ts", import.meta.url), {
    type: "module",
  });
  let markReady!: () => void;
  let acquire!: () => void;
  let release!: () => void;
  const failures: Array<(error: Error) => void> = [];
  const ready = new Promise<void>((resolve, reject) => {
    markReady = resolve;
    failures.push(reject);
  });
  const acquired = new Promise<void>((resolve, reject) => {
    acquire = resolve;
    failures.push(reject);
  });
  const released = new Promise<void>((resolve, reject) => {
    release = resolve;
    failures.push(reject);
  });
  worker.addEventListener(
    "message",
    (event: MessageEvent<"ready" | "acquired" | "released" | { error: string }>) => {
      if (event.data === "ready") {
        markReady();
      } else if (event.data === "acquired") {
        acquire();
      } else if (event.data === "released") {
        release();
      } else {
        const error = new Error(event.data.error);
        for (const fail of failures) fail(error);
      }
    },
  );
  worker.addEventListener("error", (event) => {
    const error = new Error(event.message);
    for (const fail of failures) fail(error);
  });
  return {
    worker,
    ready,
    acquired,
    released,
    acquire: () => worker.postMessage({ type: "acquire", lockName }),
    release: () => worker.postMessage({ type: "release" }),
  };
}

function heldClientId(snapshot: LockManagerSnapshot, lockName: string): string | null {
  return snapshot.held?.find(({ name }) => name === lockName)?.clientId ?? null;
}

async function waitForNativeLock(
  predicate: (snapshot: LockManagerSnapshot) => boolean,
  message: string,
): Promise<void> {
  const deadline = performance.now() + 10_000;
  for (;;) {
    if (predicate(await navigator.locks.query())) return;
    if (performance.now() >= deadline) throw new Error(message);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

export async function runNativeWriteAdmissionProgress(): Promise<WriteAdmissionProgressResult> {
  const channelName = `minnowdb-live:lock-progress:${crypto.randomUUID()}`;
  const lockName = `minnowdb-write:${channelName}`;
  const store = new MemoryBlockStore();
  Object.defineProperty(store, "liveQueryChannelName", { value: channelName });
  const workers: Worker[] = [];
  try {
    const identityLockName = `${lockName}:identity`;
    let pageClientId: string | undefined;
    await navigator.locks.request(identityLockName, async () => {
      pageClientId = heldClientId(await navigator.locks.query(), identityLockName) ?? undefined;
    });
    if (pageClientId === undefined) throw new Error("Native lock client identity is unavailable");

    const first = nativeLockHolder(lockName);
    const second = nativeLockHolder(lockName);
    const third = nativeLockHolder(lockName);
    const progressing = [first, second, third];
    workers.push(...progressing.map(({ worker }) => worker));
    await Promise.all(progressing.map(({ ready }) => ready));
    first.acquire();
    await first.acquired;
    const holderIds = [heldClientId(await navigator.locks.query(), lockName)];
    second.acquire();
    await waitForNativeLock(
      (snapshot) => (snapshot.pending ?? []).filter(({ name }) => name === lockName).length === 1,
      "Second native lock holder did not enter the queue",
    );
    third.acquire();
    await waitForNativeLock(
      (snapshot) => (snapshot.pending ?? []).filter(({ name }) => name === lockName).length === 2,
      "Third native lock holder did not enter the queue",
    );
    let progressingQueueStalls = 0;
    const progressingStartedAt = performance.now();
    const progressingAdmission = admitWriter(
      store,
      {
        kind: "autocommit",
        signal: new AbortController().signal,
        stallReportMs: 5_000,
        onStalled: () => {
          progressingQueueStalls += 1;
        },
      },
      async () => heldClientId(await navigator.locks.query(), lockName),
    );
    await waitForNativeLock(
      (snapshot) =>
        (snapshot.pending ?? []).some(
          ({ name, clientId }) => name === lockName && clientId === pageClientId,
        ),
      "Native write admission did not enter the queue",
    );
    const advanceHolders = (async () => {
      await new Promise((resolve) => setTimeout(resolve, 2_000));
      first.release();
      await first.released;
      await second.acquired;
      holderIds.push(heldClientId(await navigator.locks.query(), lockName));
      await new Promise((resolve) => setTimeout(resolve, 2_000));
      second.release();
      await second.released;
      await third.acquired;
      holderIds.push(heldClientId(await navigator.locks.query(), lockName));
      await new Promise((resolve) => setTimeout(resolve, 2_000));
      third.release();
      await third.released;
    })();
    const progressingEntryClientId = await progressingAdmission;
    const progressingElapsedMs = performance.now() - progressingStartedAt;
    await advanceHolders;
    if (
      holderIds.some((id) => id === null || id === pageClientId) ||
      new Set(holderIds).size !== progressing.length
    ) {
      throw new Error("Native lock holders did not advance through distinct clients");
    }

    // A holder that never lets go: the writer reports the stall once and keeps waiting. It is
    // never let through while the lock is held, and a waiter that gives up leaves at once.
    const frozen = nativeLockHolder(lockName);
    workers.push(frozen.worker);
    await frozen.ready;
    frozen.acquire();
    await frozen.acquired;
    const frozenClientId = heldClientId(await navigator.locks.query(), lockName);
    if (frozenClientId === null || frozenClientId === pageClientId) {
      throw new Error("Native frozen holder identity is invalid");
    }
    let frozenQueueStalls = 0;
    let frozenQueueStallHolder: string | undefined;
    let frozenEntryClientId: string | null | undefined;
    let enteredWhileFrozen = false;
    let frozenHeld = true;
    const frozenAdmission = admitWriter(
      store,
      {
        kind: "autocommit",
        signal: new AbortController().signal,
        stallReportMs: 5_000,
        onStalled: (stall) => {
          frozenQueueStalls += 1;
          frozenQueueStallHolder = stall.holder;
        },
      },
      async () => {
        if (frozenHeld) enteredWhileFrozen = true;
        frozenEntryClientId = heldClientId(await navigator.locks.query(), lockName);
      },
    );
    await waitForNativeLock(
      (snapshot) =>
        (snapshot.pending ?? []).some(
          ({ name, clientId }) => name === lockName && clientId === pageClientId,
        ),
      "Frozen write admission did not enter the queue",
    );
    let frozenQueueChurn = 0;
    const churnState = { stop: false };
    const churnTask = (async () => {
      const deadline = performance.now() + 7_000;
      while (!churnState.stop && performance.now() < deadline) {
        const controller = new AbortController();
        const churn = navigator.locks
          .request(lockName, { mode: "shared", signal: controller.signal }, () => undefined)
          .catch(() => undefined);
        await waitForNativeLock(
          (snapshot) =>
            (snapshot.pending ?? []).some(
              ({ name, mode }) => name === lockName && mode === "shared",
            ),
          "Native pending lock churn was not observable",
        );
        frozenQueueChurn += 1;
        await new Promise((resolve) => setTimeout(resolve, 1_200));
        controller.abort();
        await churn;
        await waitForNativeLock(
          (snapshot) =>
            !(snapshot.pending ?? []).some(
              ({ name, mode }) => name === lockName && mode === "shared",
            ),
          "Native pending lock churn did not leave the queue",
        );
        await new Promise((resolve) => setTimeout(resolve, 1_200));
      }
    })();
    await churnTask;
    // Seven seconds of churn behind a frozen holder: still waiting, reported exactly once.
    const cancel = new AbortController();
    const cancelledStartedAt = performance.now();
    const cancelled = admitWriter(
      store,
      { kind: "autocommit", signal: cancel.signal },
      async () => "should not run",
    ).then(
      () => "ran",
      (error: unknown) => (error instanceof Error ? error.message : String(error)),
    );
    setTimeout(() => cancel.abort(new Error("waiter gave up")), 100);
    const cancelledOutcome = await cancelled;
    const cancelledElapsedMs = performance.now() - cancelledStartedAt;
    frozenHeld = false;
    frozen.release();
    await frozen.released;
    await frozenAdmission;

    return {
      progressingQueueOverlapped: progressingEntryClientId !== pageClientId,
      progressingQueueStalls,
      progressingQueueWaitedPastOneInterval: progressingElapsedMs >= 5_000,
      frozenQueueBypassed: enteredWhileFrozen,
      frozenQueueStalls,
      frozenQueueStallHolder,
      frozenQueueChurnProven: frozenQueueChurn >= 3,
      frozenQueueEnteredAfterRelease: frozenEntryClientId === pageClientId,
      frozenQueueCancelledPromptly:
        cancelledOutcome === "waiter gave up" && cancelledElapsedMs < 2_000,
    };
  } finally {
    for (const worker of workers) worker.terminate();
    store.close();
  }
}

function databaseNames(): Promise<string[]> {
  if (typeof indexedDB.databases !== "function") {
    throw new Error("This browser does not expose indexedDB.databases()");
  }
  return indexedDB
    .databases()
    .then((records) => records.flatMap(({ name }) => (name === undefined ? [] : [name])));
}

function deleteIndexedDbDatabase(name: string): Promise<void> {
  return new Promise((resolve) => {
    const request = indexedDB.deleteDatabase(name);
    request.addEventListener("success", () => resolve(), { once: true });
    request.addEventListener("error", () => resolve(), { once: true });
    request.addEventListener("blocked", () => resolve(), { once: true });
  });
}

export async function runFailedStorageProbe(): Promise<StorageProbeResult> {
  const name = `probe-${crypto.randomUUID()}`;
  const root = await navigator.storage.getDirectory();
  const spawn = () =>
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  let shadow: IndexedDbBlockStore | undefined;
  let seeded: MinnowDatabaseClient | undefined;
  let verifier: MinnowDatabaseClient | undefined;
  try {
    seeded = new MinnowDatabaseClient(spawn(), { store: { kind: "opfs", name } });
    await seeded.execute("CREATE TABLE ledger(id INTEGER PRIMARY KEY, amount INTEGER)");
    await seeded.execute("INSERT INTO ledger VALUES (1, 700)");
    await seeded.close({ terminateWorker: true });
    seeded = undefined;

    const unavailableRoot = {
      getDirectoryHandle: async () => {
        throw new DOMException("OPFS backend unavailable", "UnknownError");
      },
    } as unknown as FileSystemDirectoryHandle;
    // Force the alternative adapter to IndexedDB. Before the fail-closed fix, the uncertain OPFS
    // lookup answered false and this call really opened an empty same-named IndexedDB database.
    autoStoreTestHooks.opfsAvailable = async () => false;
    let errorName = "resolved";
    try {
      const opened = await openAutoStore(
        name,
        async (kind) => {
          if (kind !== "indexeddb") throw new Error(`Unexpected auto store: ${kind}`);
          return IndexedDbBlockStore.open({ name });
        },
        {
          opfsDatabaseExists: (candidate) =>
            opfsDatabaseExists({ name: candidate, root: unavailableRoot }),
        },
      );
      shadow = opened.store;
    } catch (error) {
      errorName = error instanceof Error ? error.name : String(error);
    }

    const indexedDbShadowCreated = (await databaseNames()).includes(name);
    shadow?.close();
    shadow = undefined;
    verifier = new MinnowDatabaseClient(spawn(), { store: { kind: "opfs", name } });
    const rows = (await verifier.query("SELECT * FROM ledger")).rows;
    await verifier.close({ terminateWorker: true });
    verifier = undefined;
    return { errorName, indexedDbShadowCreated, rows };
  } finally {
    delete autoStoreTestHooks.opfsAvailable;
    shadow?.close();
    if (seeded !== undefined) {
      await seeded.close({ terminateWorker: true }).catch(() => undefined);
    }
    if (verifier !== undefined) {
      await verifier.close({ terminateWorker: true }).catch(() => undefined);
    }
    await deleteOpfsDatabase({ name, root }).catch(() => undefined);
    await forgetStoreChoice(name).catch(() => undefined);
    await deleteIndexedDbDatabase(name);
  }
}

export async function runFailedIndexedDbProbe(): Promise<IndexedDbProbeResult> {
  const name = `idb-probe-${crypto.randomUUID()}`;
  const spawn = () =>
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  try {
    const seeded = new MinnowDatabaseClient(spawn(), { store: { kind: "indexeddb", name } });
    try {
      await seeded.execute("CREATE TABLE ledger(id INTEGER PRIMARY KEY, amount INTEGER)");
      await seeded.execute("INSERT INTO ledger VALUES (1, 900)");
    } finally {
      await seeded.close({ terminateWorker: true });
    }

    // The real engine database is schema 2. Hide databases(), then make only the existence probe
    // open it as schema 1, which produces a native asynchronous VersionError. Choice-database
    // calls still reach the browser unchanged so the complete auto-store path runs.
    const probeFactory = new Proxy(indexedDB, {
      get(target, property) {
        if (property === "databases") return undefined;
        if (property === "open") {
          return (candidate: string, version?: number) => {
            if (candidate === name && version === undefined) return target.open(candidate, 1);
            return version === undefined ? target.open(candidate) : target.open(candidate, version);
          };
        }
        const value: unknown = Reflect.get(target, property, target);
        return value;
      },
    });
    autoStoreTestHooks.indexedDB = probeFactory;
    autoStoreTestHooks.opfsAvailable = async () => true;
    let alternativeOpened = false;
    let errorName = "resolved";
    try {
      await openAutoStore(
        name,
        async () => {
          alternativeOpened = true;
          throw new Error("the alternative adapter must not open after an uncertain probe");
        },
        { opfsDatabaseExists: async () => false },
      );
    } catch (error) {
      errorName = error instanceof Error ? error.name : String(error);
    }

    const verifier = new MinnowDatabaseClient(spawn(), { store: { kind: "indexeddb", name } });
    let rows: unknown[];
    try {
      rows = (await verifier.query("SELECT * FROM ledger")).rows;
    } finally {
      await verifier.close({ terminateWorker: true });
    }
    return { errorName, alternativeOpened, rows };
  } finally {
    delete autoStoreTestHooks.indexedDB;
    delete autoStoreTestHooks.opfsAvailable;
    await forgetStoreChoice(name).catch(() => undefined);
    await deleteIndexedDbDatabase(name);
  }
}

export async function runNativeIndexedDbGarbageCollectionRace(): Promise<GarbageCollectionRaceResult> {
  const name = `gc-race-${crypto.randomUUID()}`;
  const collectorStore = await IndexedDbBlockStore.open({ name });
  const writerStore = await IndexedDbBlockStore.open({ name });
  const options = { autoCollect: false, autoCompact: false } as const;
  const writer = new MinnowDatabase(writerStore, options);
  let armed = false;
  let insertedAfterSnapshot = false;
  let droppedBeforeNomination = false;
  let provenanceRefusals = 0;
  const interceptedStore = new Proxy(collectorStore, {
    get(target, property) {
      const value: unknown = Reflect.get(target, property, target);
      if (typeof value !== "function") return value;
      const method = value as (...arguments_: unknown[]) => unknown;
      if (property === "listSegmentPage") {
        return async (...arguments_: unknown[]) => {
          if (armed && !insertedAfterSnapshot) {
            insertedAfterSnapshot = true;
            await writer.execute("INSERT INTO items VALUES (1, 10)");
          }
          return method.apply(target, arguments_);
        };
      }
      if (property === "updateGarbageCollectionPlanning") {
        return async (...arguments_: unknown[]) => {
          const input = arguments_[0] as { candidateSegmentIds?: readonly string[] } | undefined;
          if (armed && !droppedBeforeNomination && (input?.candidateSegmentIds?.length ?? 0) > 0) {
            droppedBeforeNomination = true;
            await writer.execute("DROP TABLE items");
          }
          try {
            return await method.apply(target, arguments_);
          } catch (error) {
            if (
              error instanceof Error &&
              error.message.startsWith("GC segment has no provenance:")
            ) {
              provenanceRefusals += 1;
            }
            throw error;
          }
        };
      }
      return method.bind(target);
    },
  });
  const collector = new MinnowDatabase(interceptedStore, options);
  try {
    await collector.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, amount INTEGER)");
    armed = true;
    const result = await collector.collectGarbage({
      maxPlanningItems: 32,
      maxItemsPerStep: 32,
    });
    const jobs = await collectorStore.listGarbageCollectionJobs();
    await collector.close();
    await writer.close();
    collectorStore.close();
    writerStore.close();

    const reopenedStore = await IndexedDbBlockStore.open({ name });
    try {
      return {
        insertedAfterSnapshot,
        droppedBeforeNomination,
        provenanceRefusals,
        missingSegmentCount: result.missingSegmentCount,
        tablesAfterReopen: (await reopenedStore.listTables()).map(({ name }) => name),
        completedJobs: jobs.filter(({ state }) => state === "completed").length,
      };
    } finally {
      reopenedStore.close();
    }
  } finally {
    await Promise.allSettled([collector.close(), writer.close()]);
    collectorStore.close();
    writerStore.close();
    await deleteIndexedDbDatabase(name);
  }
}

export async function runNativeIndexedDbSequenceReopen(): Promise<IndexedDbSequenceResult> {
  const name = `sequence-${crypto.randomUUID()}`;
  const spawn = () =>
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
  let firstClient: MinnowDatabaseClient | undefined;
  let secondClient: MinnowDatabaseClient | undefined;
  let inspector: IndexedDbBlockStore | undefined;
  try {
    firstClient = new MinnowDatabaseClient(spawn(), { store: { kind: "indexeddb", name } });
    await firstClient.execute("CREATE SEQUENCE order_ids");
    const first = (await firstClient.query("SELECT NEXTVAL('order_ids') AS id")).rows;
    await firstClient.close({ terminateWorker: true });
    firstClient = undefined;

    secondClient = new MinnowDatabaseClient(spawn(), { store: { kind: "indexeddb", name } });
    const second = (await secondClient.query("SELECT NEXTVAL('order_ids') AS id")).rows;
    await secondClient.close({ terminateWorker: true });
    secondClient = undefined;

    inspector = await IndexedDbBlockStore.open({ name });
    const sequence = (await inspector.listTables()).find(
      (table) => table.sequence?.name === "order_ids",
    );
    return {
      first,
      second,
      durableKeyMetadata:
        sequence?.uniqueKeyColumnId !== undefined &&
        sequence.uniqueKeyColumnId === sequence.sequence?.columnId,
    };
  } finally {
    if (firstClient !== undefined) {
      await firstClient.close({ terminateWorker: true }).catch(() => undefined);
    }
    if (secondClient !== undefined) {
      await secondClient.close({ terminateWorker: true }).catch(() => undefined);
    }
    inspector?.close();
    await deleteIndexedDbDatabase(name);
  }
}
