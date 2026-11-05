/**
 * Page side of the rebuild core's storage browser suite (browser-tests/next-storage.spec.ts):
 * spawns next-storage-worker.ts workers, kills them mid-publish, and verifies from fresh ones.
 */
import type { NextStorageRequest } from "./next-storage-worker.js";

interface StorageWorker {
  call<T>(request: NextStorageRequest): Promise<T>;
  /** The last round the worker reported as published (its publish resolved). */
  readonly acked: number;
  terminate(): void;
}

async function spawn(): Promise<StorageWorker> {
  const worker = new Worker(new URL("./next-storage-worker.ts", import.meta.url), {
    type: "module",
  });
  const pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();
  let next = 1;
  let acked = 0;
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
        progress?: { acked?: number };
      };
      if (data.progress?.acked !== undefined) acked = data.progress.acked;
      if (data.id === undefined) return;
      const waiter = pending.get(data.id);
      pending.delete(data.id);
      if (data.error !== undefined) waiter?.reject(new Error(data.error));
      else waiter?.resolve(data.result);
    });
    worker.addEventListener("error", (event) => {
      clearTimeout(timer);
      reject(new Error(event.message));
    });
  });
  return {
    call<T>(request: NextStorageRequest) {
      const id = next++;
      return new Promise<T>((resolve, reject) => {
        pending.set(id, { resolve: resolve, reject });
        worker.postMessage({ id, request });
      });
    },
    get acked() {
      return acked;
    },
    terminate() {
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

export interface KillResult {
  readonly iterations: Array<{
    readonly acked: number;
    readonly round: number;
    readonly previous: number | null;
    readonly openedMs: number;
  }>;
}

/**
 * Starts a publish loop in a worker, terminates the worker after a random delay (while it is
 * publishing: each round ends in two flushes), and verifies the files from a fresh worker:
 * the root is at least the last acknowledged round, its previous root is the round before, every
 * blob of both verifies, the free map is exact (`audit`), and the next owner keeps writing.
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
    const writer = await spawn();
    void writer.call({ op: "publish-loop", dir, rounds: 1_000 }).catch(() => undefined);
    // Wait for the first acknowledgement, then a random 0–60 ms more.
    const deadline = performance.now() + 20_000;
    while (writer.acked <= acked && performance.now() < deadline)
      await new Promise((resolve) => setTimeout(resolve, 2));
    await new Promise((resolve) => setTimeout(resolve, Math.floor(random() * 60)));
    acked = Math.max(acked, writer.acked);
    writer.terminate();
    const verified = await withWorker((worker) =>
      worker.call<{ round: number; previous: number | null; openedMs: number }>({
        op: "verify",
        dir,
        minAcked: acked,
      }),
    );
    results.push({ acked, ...verified });
    acked = verified.round + 1;
  }
  await withWorker((worker) => worker.call({ op: "remove", dir }));
  return { iterations: results };
}

/** Starts a publish loop in this page's worker and returns the directory (the tab is then closed). */
export async function startPublishLoop(): Promise<string> {
  const dir = unique("tab");
  const writer = await spawn();
  void writer.call({ op: "publish-loop", dir, rounds: 100_000 }).catch(() => undefined);
  const deadline = performance.now() + 20_000;
  while (writer.acked < 3 && performance.now() < deadline)
    await new Promise((resolve) => setTimeout(resolve, 2));
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
