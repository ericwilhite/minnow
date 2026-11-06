/**
 * The IndexedDB byte space's in-memory record index (packages/core/src/next/storage/idb-bytes.ts)
 * at a million records, in Node (review B1-p2d finding 4): the cost of validating and streaming it
 * into blocks at open (pages of LOAD_PAGE entries), its resident heap, fragmented reuse (splitting records in the middle), and deleting a
 * 100,000-record prefix. Each is measured for the blocked index and for the flat sorted arrays it
 * replaced (66dc3ca). The IndexedDB requests themselves are stubbed out: this is the CPU and heap
 * the engine adds on top of the platform's `getAll`.
 *
 *   node --expose-gc node_modules/vite-node/dist/cli.mjs scripts/idb-index-bench.mts
 */
import { performance } from "node:perf_hooks";
import { ByteStore, IntervalIndex } from "../packages/core/src/next/storage/idb-bytes.ts";
import type {
  IdbDatabaseLike,
  IdbRange,
  IdbStoreLike,
  IdbTransactionLike,
} from "../packages/core/src/next/storage/idb-like.ts";

const RECORDS = 1_000_000;
const RECORD = 4096;
const gc = (globalThis as { gc?: () => void }).gc;

function report(name: string, value: number, unit: string): void {
  console.log(`${name.padEnd(52)} ${value.toFixed(value < 10 ? 2 : 0).padStart(10)} ${unit}`);
}

function heap(): number {
  gc?.();
  gc?.();
  return process.memoryUsage().heapUsed;
}

/**
 * A database whose stores answer from arrays built up front (so the timings exclude making them;
 * the platform's `getAll` is not what this measures).
 */
function stubDatabase(count: number): IdbDatabaseLike {
  const entries = Array.from({ length: count }, (_, at) => ({
    offset: at * RECORD,
    length: RECORD,
  }));
  const keys = Array.from({ length: count }, (_, at) => at * RECORD);
  /** The slice a paged request returns (keys are `at * RECORD`; open lower bounds only). */
  const page = <T,>(all: T[], range: IdbRange, limit = Infinity): T[] => {
    const from = range.lower === undefined ? 0 : Math.floor(range.lower / RECORD) + 1;
    return all.slice(from, from + Math.min(limit, all.length));
  };
  const store = (name: string): IdbStoreLike => ({
    get: (key, then) => {
      then({ offset: key, bytes: new ArrayBuffer(RECORD) });
    },
    getAll: (range, then, limit) => {
      then(name.endsWith("-index") ? page(entries, range, limit) : []);
    },
    getAllKeys: (range, then, limit) => {
      then(page(keys, range, limit));
    },
    last: (_range, then) => {
      then(undefined);
    },
    put: () => undefined,
    delete: () => undefined,
    deleteRange: () => undefined,
    clear: () => undefined,
  });
  const tx: IdbTransactionLike = { store, abort: () => undefined };
  return {
    transaction: async (_stores, _mode, _durability, body) => {
      body(tx);
    },
    close: () => undefined,
  };
}

/** The 66dc3ca index: two flat sorted arrays, spliced per insert and delete. */
class FlatIndex {
  starts: number[] = [];
  ends: number[] = [];
  #first(x: number): number {
    let low = 0;
    let high = this.ends.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if ((this.ends[middle] ?? 0) <= x) low = middle + 1;
      else high = middle;
    }
    return low;
  }
  insert(start: number, end: number): void {
    const at = this.#first(start);
    this.starts.splice(at, 0, start);
    this.ends.splice(at, 0, end);
  }
  remove(start: number): void {
    const at = this.#first(start);
    this.starts.splice(at, 1);
    this.ends.splice(at, 1);
  }
}

function time(body: () => void): number {
  const start = performance.now();
  body();
  return performance.now() - start;
}

async function main(): Promise<void> {
  if (gc === undefined) console.log("(run with --expose-gc for heap figures)");
  console.log(`${RECORDS.toLocaleString()} records of ${String(RECORD)} bytes`);

  // Open: validate and build (the getAll arrays are the platform's, freed after open).
  const db = stubDatabase(RECORDS);
  const before = heap();
  let start = performance.now();
  const space = await ByteStore.load(db, "data");
  // The stub keeps its arrays, so the heap delta is the index alone.
  report("open: validate + build blocked index", performance.now() - start, "ms");
  const resident = heap() - before;
  report("  resident heap (measured)", resident / 2 ** 20, "MiB");
  report("  charged estimate (IntervalIndex.bytes)", space.index.bytes / 2 ** 20, "MiB");
  report("  blocks", space.index.blocks, "");

  const flatBefore = heap();
  start = performance.now();
  const flat = new FlatIndex();
  for (let at = 0; at < RECORDS; at++) {
    flat.starts.push(at * RECORD);
    flat.ends.push(at * RECORD + RECORD);
  }
  report("open: flat arrays (66dc3ca, no validation)", performance.now() - start, "ms");
  report("  resident heap (measured)", (heap() - flatBefore) / 2 ** 20, "MiB");

  // Fragmented reuse: release one page in the middle of a record and write it back (one record
  // becomes three: two inserts and a delete each way), at random places.
  const OPS = 2000;
  const places = Array.from({ length: OPS }, (_, at) => ((at * 7919) % RECORDS) * RECORD);
  const blocked = IntervalIndex.from(
    Array.from({ length: RECORDS }, (_, at) => ({ offset: at * RECORD, length: RECORD })),
    null,
  );
  const blockedMs = time(() => {
    for (const offset of places) {
      blocked.removeStarts(offset, offset);
      blocked.insert(offset, offset + 1024);
      blocked.insert(offset + 1024, offset + 2048);
      blocked.insert(offset + 2048, offset + RECORD);
    }
  });
  report("fragmented reuse: blocked, per split", (blockedMs / OPS) * 1000, "µs");
  const flatMs = time(() => {
    for (const offset of places) {
      flat.remove(offset);
      flat.insert(offset, offset + 1024);
      flat.insert(offset + 1024, offset + 2048);
      flat.insert(offset + 2048, offset + RECORD);
    }
  });
  report("fragmented reuse: flat arrays, per split", (flatMs / OPS) * 1000, "µs");

  // Prefix delete of 100,000 records: one clear (two range deletes) vs one splice per record
  // (66dc3ca recursed per record, overflowing the stack long before 100,000; its splices alone
  // are timed here on the first 2,000 and scaled).
  const PREFIX = 100_000;
  const tx: IdbTransactionLike = { store: () => idleStore, abort: () => undefined };
  const fresh = await ByteStore.load(db, "data");
  const clearMs = time(() => {
    fresh.clear(tx, 0, PREFIX * RECORD, () => undefined);
  });
  fresh.commit();
  report("prefix delete of 100,000: blocked clear", clearMs, "ms");
  report("  records left", fresh.records, "");
  const SAMPLE = 2000;
  const flatPrefix = new FlatIndex();
  for (let at = 0; at < RECORDS; at++) {
    flatPrefix.starts.push(at * RECORD);
    flatPrefix.ends.push(at * RECORD + RECORD);
  }
  const sampleMs = time(() => {
    for (let at = 0; at < SAMPLE; at++) flatPrefix.remove(at * RECORD);
  });
  report("prefix delete of 100,000: flat splices (scaled)", (sampleMs / SAMPLE) * PREFIX, "ms");
}

/** A store whose requests do nothing (the clear's range deletes). */
const idleStore: IdbStoreLike = {
  get: (_key, then) => {
    then(undefined);
  },
  getAll: (_range, then) => {
    then([]);
  },
  getAllKeys: (_range, then) => {
    then([]);
  },
  last: (_range, then) => {
    then(undefined);
  },
  put: () => undefined,
  delete: () => undefined,
  deleteRange: () => undefined,
  clear: () => undefined,
};

await main();
