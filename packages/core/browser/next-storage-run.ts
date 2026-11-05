/**
 * Page side of the rebuild core's storage browser suite (browser-tests/next-storage.spec.ts):
 * spawns next-storage-worker.ts workers, kills them mid-publish, and verifies from fresh ones.
 */
import { column, schema, table } from "@minnowdb/core";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import type { NextStorageRequest, PublishPhase, StoreKind } from "./next-storage-worker.js";

/** The backend every following scenario runs on (OPFS unless a test picks IndexedDB). */
let store: StoreKind = "opfs";
export function useStore(kind: StoreKind): void {
  store = kind;
}

interface StorageWorker {
  call<T>(request: NextStorageRequest): Promise<T>;
  /** The publisher's last reported phase, or null before its first round. */
  readonly progress: { readonly round: number; readonly phase: PublishPhase } | null;
  /** The highest round this worker reported as published (its publish resolved). */
  readonly acked: number;
  /** Rounds this worker published. */
  readonly published: number;
  /** Starts a publish loop; a failure other than this page's own `terminate` is kept. */
  publish(dir: string, rounds: number): void;
  /**
   * Resolves when `ready()` holds; rejects naming `what`, the last phase and any publisher
   * failure when it does not within `ms` (a stalled or failed publisher fails the test).
   */
  until(ready: () => boolean, what: string, ms?: number): Promise<void>;
  terminate(): void;
}

async function spawn(): Promise<StorageWorker> {
  const worker = new Worker(new URL("./next-storage-worker.ts", import.meta.url), {
    type: "module",
  });
  const pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();
  let next = 1;
  let progress: StorageWorker["progress"] = null;
  let acked = 0;
  let published = 0;
  let failure: Error | null = null;
  let terminated = false;
  /** Waiters checked on every progress message, so no phase can slip by between polls. */
  const watchers = new Set<() => void>();
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error("storage worker did not start"));
    }, 20_000);
    worker.addEventListener("message", (event: MessageEvent<unknown>) => {
      if (event.data === "next-storage-ready") {
        clearTimeout(timer);
        resolve();
        return;
      }
      const data = event.data as {
        id?: number;
        result?: unknown;
        error?: string;
        progress?: { round: number; phase: PublishPhase };
      };
      if (data.progress !== undefined) {
        progress = data.progress;
        if (data.progress.phase === "published") {
          acked = Math.max(acked, data.progress.round);
          published += 1;
        }
        for (const watch of [...watchers]) watch();
      }
      if (data.id === undefined) return;
      const waiter = pending.get(data.id);
      pending.delete(data.id);
      if (data.error !== undefined) waiter?.reject(new Error(data.error));
      else waiter?.resolve(data.result);
    });
    worker.addEventListener("error", (event) => {
      clearTimeout(timer);
      failure ??= new Error(`storage worker error: ${event.message}`);
      reject(failure);
    });
  });
  const call = <T>(request: NextStorageRequest) => {
    const id = next++;
    return new Promise<T>((resolve, reject) => {
      pending.set(id, { resolve: resolve, reject });
      worker.postMessage({ id, request: { store, ...request } });
    });
  };
  return {
    call,
    get progress() {
      return progress;
    },
    get acked() {
      return acked;
    },
    get published() {
      return published;
    },
    publish(dir, rounds) {
      call({ op: "publish-loop", dir, rounds }).then(
        () => {
          failure ??= new Error("the publisher stopped before it was killed");
        },
        (error: unknown) => {
          if (!terminated) failure ??= error instanceof Error ? error : new Error(String(error));
        },
      );
    },
    until(ready, what, ms = 20_000) {
      return new Promise<void>((resolve, reject) => {
        const finish = (error: Error | null) => {
          watchers.delete(check);
          clearInterval(timer);
          if (error === null) resolve();
          else reject(error);
        };
        const check = () => {
          if (failure !== null)
            finish(new Error(`publisher failed before ${what}: ${failure.message}`));
          else if (ready()) finish(null);
          else if (performance.now() > deadline)
            finish(
              new Error(
                `publisher stalled before ${what}: last phase ${JSON.stringify(progress)}, ${String(published)} rounds published`,
              ),
            );
        };
        const deadline = performance.now() + ms;
        const timer = setInterval(check, 10);
        watchers.add(check);
        check();
      });
    },
    terminate() {
      terminated = true;
      worker.terminate();
      for (const waiter of pending.values()) waiter.reject(new Error("terminated"));
      pending.clear();
    },
  };
}

const unique = (label: string) =>
  `${label}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

async function withWorker<T>(work: (worker: StorageWorker) => Promise<T>): Promise<T> {
  const worker = await spawn();
  try {
    return await work(worker);
  } finally {
    worker.terminate();
  }
}

export async function runConformance(): Promise<Array<{ name: string; error: string | null }>> {
  return withWorker((worker) => worker.call({ op: "conformance", prefix: unique("conformance") }));
}

export const PUBLISH_PHASES: readonly PublishPhase[] = ["writing", "publishing", "published"];

export interface KillResult {
  readonly iterations: Array<{
    /** The phase this iteration aimed its kill at, and where the publisher was when killed. */
    readonly target: PublishPhase;
    readonly killedRound: number;
    readonly acked: number;
    readonly round: number;
    readonly previous: number | null;
    readonly openedMs: number;
  }>;
}

/**
 * Kills a publishing worker in each phase of a round in turn (writing its extents, inside
 * `publish`, just after `publish` resolved), after a random 2–5 rounds of its own so an idle or
 * failed publisher stalls the test instead of passing it, then verifies the files from a fresh
 * worker: the root is at least the last acknowledged round, its previous root is the round
 * before, every blob of both verifies, the free map is exact (`audit`), and the next owner keeps
 * writing. Each phase is the target of every third iteration.
 */
export async function runKillDuringPublish(iterations: number, seed: number): Promise<KillResult> {
  let state = seed >>> 0;
  const random = () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state / 2 ** 32;
  };
  const dir = unique("kill");
  const results: Array<KillResult["iterations"][number]> = [];
  let acked = 0;
  for (let iteration = 0; iteration < iterations; iteration++) {
    const target = PUBLISH_PHASES[iteration % PUBLISH_PHASES.length] ?? "publishing";
    const warmup = 2 + Math.floor(random() * 4);
    const writer = await spawn();
    try {
      writer.publish(dir, 1_000_000);
      await writer.until(
        () => writer.published >= warmup && writer.progress?.phase === target,
        `the ${target} phase after ${String(warmup)} rounds`,
      );
    } finally {
      writer.terminate();
    }
    const killedRound = writer.progress?.round ?? 0;
    acked = Math.max(acked, writer.acked);
    const verified = await withWorker((worker) =>
      worker.call<{ round: number; previous: number | null; openedMs: number }>({
        op: "verify",
        dir,
        minAcked: acked,
      }),
    );
    results.push({ target, killedRound, acked, ...verified });
    // The verifier published one more round.
    acked = verified.round + 1;
  }
  await withWorker((worker) => worker.call({ op: "remove", dir }));
  return { iterations: results };
}

/** Starts a publish loop in this page's worker and returns the directory (the tab is then closed). */
export async function startPublishLoop(): Promise<string> {
  const dir = unique("tab");
  const writer = await spawn();
  writer.publish(dir, 1_000_000);
  await writer.until(() => writer.published >= 3, "three published rounds");
  return dir;
}

export async function verifyDirectory(
  dir: string,
): Promise<{ round: number; previous: number | null; openedMs: number }> {
  return withWorker((worker) => worker.call({ op: "verify", dir, minAcked: 3 }));
}

/**
 * The fence across workers: a second owner without `steal` times out; with `steal` it takes
 * over once the first worker's lease has closed its handles, and the first worker's next mutation
 * is refused.
 */
export async function runSteal(): Promise<{
  waited: { ok: boolean; error?: string };
  stolen: { ok: boolean; ms?: number; error?: string };
  loser: { lost: boolean; error: string | null };
}> {
  const dir = unique("steal");
  const first = await spawn();
  try {
    await first.call({ op: "hold", dir });
    const waited = await withWorker((worker) =>
      worker.call<{ ok: boolean; error?: string }>({
        op: "acquire",
        dir,
        steal: false,
        timeoutMs: 300,
      }),
    );
    const second = await spawn();
    try {
      const stolen = await second.call<{ ok: boolean; ms?: number; error?: string }>({
        op: "acquire",
        dir,
        steal: true,
        timeoutMs: 5_000,
      });
      const loser = await first.call<{ lost: boolean; error: string | null }>({
        op: "write-after-loss",
      });
      return { waited, stolen, loser };
    } finally {
      second.terminate();
    }
  } finally {
    first.terminate();
    await withWorker((worker) => worker.call({ op: "remove", dir }));
  }
}

export async function runSpill(): Promise<{ pages: number; inUse: number; after: number }> {
  const dir = unique("spill");
  try {
    return await withWorker((worker) => worker.call({ op: "spill", dir }));
  } finally {
    await withWorker((worker) => worker.call({ op: "remove", dir }));
  }
}

export interface BuilderBench {
  readonly appendMs: number;
  readonly sealMs: number;
  readonly publishMs: number;
  readonly bytes: number;
}

/** The page format's group-writer pattern: many column builders, page by page (or one by one). */
export async function runBuilderBench(
  alternating: boolean,
  buffered: boolean,
  columns: number,
  pagesPerColumn: number,
  pageBytes: number,
): Promise<BuilderBench> {
  const dir = unique("bench");
  try {
    return await withWorker((worker) =>
      worker.call<BuilderBench>({
        op: "bench-builders",
        dir,
        columns,
        pagesPerColumn,
        pageBytes,
        alternating,
        buffered,
      }),
    );
  } finally {
    await withWorker((worker) => worker.call({ op: "remove", dir }));
  }
}

export interface ChooseResult {
  readonly kind: string | null;
  readonly opfsRefused: string | null;
  readonly error: { name: string; store?: string; location?: string } | null;
}

/** What `chooseBackend` picks in a dedicated worker of this page (for `name`, or a fresh one). */
export async function runChoose(name?: string): Promise<ChooseResult> {
  return withWorker((worker) =>
    worker.call({ op: "choose", ...(name === undefined ? {} : { name }) }),
  );
}

/** The IndexedDB scenarios on the platform's IndexedDB and on the simulator. */
export async function runIdbDifferential(): Promise<{ real: unknown; simulated: unknown }> {
  return withWorker((worker) => worker.call({ op: "idb-scenarios" }));
}

/**
 * The pre-reset engine creates a database (in OPFS or IndexedDB) and writes a row; then
 * `chooseBackend` for the same name must refuse with `LegacyDataError` and create nothing.
 */
export async function runLegacyDetection(kind: "opfs" | "indexeddb"): Promise<ChooseResult> {
  const name = `legacy-${kind}-${crypto.randomUUID()}`;
  const store =
    kind === "indexeddb" ? { kind: "indexeddb" as const, name } : { kind: "opfs" as const, name };
  const client = new MinnowDatabaseClient(
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
    { store },
  );
  await client.migrate(schema([table("notes", { id: column.integer().unique() })]));
  await client.execute("INSERT INTO notes VALUES (1)");
  await client.close({ terminateWorker: true });
  return runChoose(name);
}

/** The IndexedDB backend's bulk write and small-commit costs, with and without batching. */
export async function runIdbBench(
  batching: boolean,
): Promise<{ bulkMs: number; commitMs: number }> {
  const dir = unique("idb-bench");
  try {
    return await withWorker((worker) =>
      worker.call({ op: "bench-idb", dir, batching, store: "indexeddb" }),
    );
  } finally {
    await withWorker((worker) => worker.call({ op: "remove", dir, store: "indexeddb" }));
  }
}
