/**
 * Benchmarks the rebuild core's write path and key index (packages/core/src/next/write) against
 * PROPOSAL.md "Targets", owner-side, in Node, on the simulated disk and the in-memory extent space:
 *
 *   npx vite-node scripts/write-path-bench.mts [--rows 1000000] [--probes 20000]
 *
 * - keyed load: `rows` rows (id INT PRIMARY KEY, v TEXT, n INT) as 2,048-row autocommit inserts,
 *   strict, until every flush published (target: 1M rows ≤ 1.5 s durable; here the disk is memory,
 *   so this is the CPU part only).
 * - warm PK probe: key → locator through the key index (runs + memtable), one key per call, CPU
 *   time per probe, synchronous when cached (target ≤ 10 µs for a whole point read; UUID ≤ 15 µs).
 * - warm PK read: probe plus both value columns, read from the group's cached pages through the
 *   page format's chunk readers (opened once per group and column, as an executor scan would).
 * - after 2k uncompacted writes: the same probe with 2,048 newer rows in the memtable, as a ratio
 *   of the compacted probe (target ≤ 1.2×).
 * - relaxed INSERT: one-row autocommit transactions (begin, statement, insert, end, commit) under
 *   relaxed durability, owner CPU per insert (target ≤ 20 µs).
 * - resident bytes per keyed row: the write path's own memory (memtables, the resident fences,
 *   block refs and Bloom filters of the runs it opened), per row (target ≤ 4 B; pages and run
 *   blocks live in the extent space's governed cache and are not counted).
 * - allocation: heap bytes allocated per keyed-load row and per relaxed INSERT, and the share of
 *   the time spent in GC, from V8's GC profiler (allocated = the heap growth between collections).
 * Medians of several runs; the machine is shared, so compare against the targets with margin.
 * `--only keyed|uuid|insert` runs one section.
 */
import { performance } from "node:perf_hooks";
import { GCProfiler, getHeapStatistics } from "node:v8";
import { encodeKeyBatch, KeyBatch } from "../packages/core/src/next/keys/key-encoding.ts";
import { ChunkReader } from "../packages/core/src/next/page/reader.ts";
import type { ColumnChunk } from "../packages/core/src/next/format/manifest.ts";
import { allocateData, type FlatVector } from "../packages/core/src/next/vector/vector.ts";
import type { PhysicalType } from "../packages/core/src/next/types.ts";
import type { WritePathImpl } from "../packages/core/src/next/write/engine.ts";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
  TEXT,
  texts,
} from "../packages/core/src/next/write/test-support.ts";

const args = process.argv.slice(2);
const option = (name: string, fallback: number): number => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? Number(args[index + 1]) : fallback;
};
const ROWS = option("rows", 1_000_000);
const PROBES = option("probes", 20_000);
const FREE = { allocate: () => true, free: () => undefined };
const UUID: PhysicalType = { kind: "uuid" };
const ONLY = (() => {
  const index = args.indexOf("--only");
  return index >= 0 ? (args[index + 1] ?? null) : null;
})();
const runs = (section: string) => ONLY === null || ONLY === section;

/** Heap bytes allocated while `work` runs, and the milliseconds V8 spent collecting. */
async function measureAllocation(
  work: () => Promise<void>,
): Promise<{ bytes: number; gcMs: number; ms: number }> {
  const profiler = new GCProfiler();
  const before = getHeapStatistics().used_heap_size;
  const start = performance.now();
  profiler.start();
  await work();
  const result = profiler.stop();
  const ms = performance.now() - start;
  const after = getHeapStatistics().used_heap_size;
  let bytes = 0;
  let previous = before;
  let gcMs = 0;
  for (const entry of result?.statistics ?? []) {
    bytes += Math.max(0, entry.beforeGC.heapStatistics.usedHeapSize - previous);
    previous = entry.afterGC.heapStatistics.usedHeapSize;
    gcMs += entry.cost / 1000;
  }
  bytes += Math.max(0, after - previous);
  return { bytes, gcMs, ms };
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

function uuids(ids: readonly number[]): FlatVector {
  const bytes = new Uint8Array(ids.length * 16);
  ids.forEach((id, k) => {
    // A stable pseudo-random 16-byte key per id.
    let x = (id + 1) * 0x9e3779b1;
    for (let b = 0; b < 16; b++) {
      x = Math.imul(x ^ (x >>> 15), 0x2c1b3c6d) >>> 0;
      bytes[k * 16 + b] = x & 0xff;
    }
  });
  return {
    form: "flat",
    type: UUID,
    length: ids.length,
    validity: null,
    data: { layout: "fixed16", bytes },
  };
}

async function createKeyed(path: WritePathImpl, name: string, key: PhysicalType): Promise<number> {
  let id = 0;
  await autocommit(path, async (statement) => {
    id =
      (
        await statement.changeCatalog({
          kind: "createTable",
          table: tableInput(name, [column("id", key, false), column("v", TEXT), column("n", INT)]),
        })
      ).id ?? 0;
  });
  return id;
}

async function load(
  path: WritePathImpl,
  tableId: number,
  key: PhysicalType,
  from: number,
  rows: number,
): Promise<void> {
  for (let start = from; start < from + rows; start += 2048) {
    const n = Math.min(2048, from + rows - start);
    const ids = Array.from({ length: n }, (_, k) => start + k);
    const keys = key.kind === "uuid" ? uuids(ids) : ints(ids);
    await autocommit(path, (statement) =>
      statement
        .table(tableId)
        .insert(
          batch(
            keys,
            texts(ids.map((id) => `value ${String(id)}`)),
            ints(ids.map((id) => id % 1000)),
          ),
          null,
        ),
    );
  }
}

/** Times `probes` single-key probes; returns µs per probe (median of 5 rounds). */
async function probeLatency(
  path: WritePathImpl,
  tableId: number,
  key: PhysicalType,
  keys: readonly number[],
  read: boolean,
): Promise<number> {
  const view = path.pin();
  const def = path.catalog.table(tableId);
  const index = view.index(tableId, def?.primaryKey ?? 0);
  const table = await view.table(tableId, { budget: FREE });
  const keyBatch = new KeyBatch(FREE);
  const out = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
  // Chunk readers per (segment, column), opened once (an executor scan keeps them).
  const readers = new Map<string, ChunkReader>();
  const targets = (def?.columns ?? []).slice(1).map((c) => ({
    form: "flat" as const,
    type: c.type,
    length: 0,
    validity: new Uint32Array(1),
    data: allocateData(c.type, 1, 256),
  }));
  const row = new Int32Array(1);
  const one = key.kind === "uuid" ? null : ints([0]);
  const columnsOf = (def?.columns ?? []).slice(1);
  /** Reads the row's value columns: synchronously when cached, else a Promise. */
  const readRow = (locator: number): Promise<void> | undefined => {
    const segmentIndex = Math.floor(locator / 65_536);
    const position = locator % 65_536;
    const segment = table.segments[segmentIndex];
    if (segment === undefined) throw new Error("bad locator");
    if (segment.kind === "memory") {
      for (const c of columnsOf) {
        const result = segment.chunk(c.id, Math.floor(position / 2048), {
          flat: targets[0] as FlatVector,
          codes: new Int32Array(0),
          varlen: null,
        });
        if (result instanceof Promise) return result.then(() => undefined);
      }
      return undefined;
    }
    const root = segment.root();
    if (root instanceof Promise) return root.then(() => readRow(locator));
    row[0] = position;
    for (const [k, c] of columnsOf.entries()) {
      const cacheKey = `${String(segmentIndex)}:${String(c.id)}`;
      let reader = readers.get(cacheKey);
      if (reader === undefined) {
        const opened = ChunkReader.open(view.pages, segment.chunk(c.id) as ColumnChunk, {
          budget: FREE,
        });
        if (opened instanceof Promise)
          return opened.then((ready) => {
            readers.set(cacheKey, ready);
            return readRow(locator);
          });
        reader = opened;
        readers.set(cacheKey, reader);
      }
      const loaded = reader.load(reader.pagesForRows(row));
      if (loaded instanceof Promise) return loaded.then(() => readRow(locator));
      reader.gather(row, targets[k] as FlatVector, 0);
    }
    return undefined;
  };
  /** One point read; synchronous (no await, R6) when the index blocks and pages are cached. */
  const runOnce = (key_: number): Promise<void> | undefined => {
    const vector = one === null ? uuids([key_]) : one;
    if (one !== null && vector.data.layout === "i32") vector.data.values[0] = key_;
    encodeKeyBatch(
      "equality",
      [{ vector, direction: "asc", nulls: "first" }],
      { length: 1, selection: null },
      keyBatch,
    );
    const after = () => {
      if (out.count !== 1) throw new Error(`key ${String(key_)} not found`);
      return read ? readRow(out.locators[0] ?? 0) : undefined;
    };
    const result = index.probe(keyBatch, out);
    return result instanceof Promise ? result.then(after) : after();
  };
  for (const key_ of keys.slice(0, 2000)) await runOnce(key_); // warm caches
  const rounds: number[] = [];
  for (let round = 0; round < 5; round++) {
    const cpu = process.cpuUsage();
    for (const key_ of keys) {
      const pending = runOnce(key_);
      if (pending !== undefined) await pending;
    }
    const used = process.cpuUsage(cpu);
    rounds.push((used.user + used.system) / keys.length);
  }
  for (const reader of readers.values()) reader.close();
  view.release();
  return median(rounds);
}

function randomKeys(count: number, max: number): number[] {
  let x = 12345;
  return Array.from({ length: count }, () => {
    x = (Math.imul(x, 1103515245) + 12345) >>> 0;
    return x % max;
  });
}

async function main(): Promise<void> {
  const lines: string[] = [];
  const report = (label: string, value: string, target: string) => {
    lines.push(`${label.padEnd(44)} ${value.padStart(14)}   target ${target}`);
  };
  let start: number;
  if (runs("keyed")) {
    // Keyed load, strict.
    const harness = new Harness("strict", "memory");
    const path = await harness.open();
    const t = await createKeyed(path, "keyed", INT);
    const loaded = await measureAllocation(async () => {
      await load(path, t, INT, 0, ROWS);
      await path.quiesce();
    });
    report(
      `keyed load, ${String(ROWS)} rows (strict, memory disk)`,
      `${loaded.ms.toFixed(0)} ms`,
      "≤ 1,500 ms at 1M (durable)",
    );
    report("  publications", String(path.publications), "");
    report(
      "  heap allocated per row (incl. building input)",
      `${(loaded.bytes / ROWS).toFixed(0)} B`,
      "",
    );
    report("  GC share of load time", `${((100 * loaded.gcMs) / loaded.ms).toFixed(1)} %`, "");
    // Resident bytes per keyed row: memtables plus resident run parts, after the probes below
    // have opened every run (reported after them).
    // Warm PK probe and read.
    const keys = randomKeys(PROBES, ROWS);
    const probeUs = await probeLatency(path, t, INT, keys, false);
    report("warm PK probe (INT key, owner)", `${probeUs.toFixed(2)} µs`, "part of ≤ 10 µs");
    const readUs = await probeLatency(path, t, INT, keys, true);
    report("warm PK read: probe + 2 columns (INT key)", `${readUs.toFixed(2)} µs`, "≤ 10 µs");
    report(
      "resident write-path bytes per keyed row",
      `${(path.residentBytes() / ROWS).toFixed(2)} B`,
      "≤ 4 B",
    );
    report(
      "  stored bytes per row (pages, runs, metadata)",
      `${(harness.space.stats.dataBytes / ROWS).toFixed(1)} B`,
      "",
    );
    // After 2k uncompacted writes.
    await load(path, t, INT, ROWS, 2048);
    const afterUs = await probeLatency(path, t, INT, keys, false);
    report(
      "probe after 2k uncompacted writes",
      `${(afterUs / probeUs).toFixed(2)}×`,
      "≤ 1.2× compacted",
    );
    await harness.close();
  }
  if (runs("uuid")) {
    // UUID keys.
    const uuidHarness = new Harness("strict", "memory");
    const uuidPath = await uuidHarness.open();
    const u = await createKeyed(uuidPath, "uuid_keyed", UUID);
    const uuidRows = Math.min(ROWS, 1_000_000);
    start = performance.now();
    await load(uuidPath, u, UUID, 0, uuidRows);
    await uuidPath.quiesce();
    report(
      `UUID keyed load, ${String(uuidRows)} rows`,
      `${(performance.now() - start).toFixed(0)} ms`,
      "",
    );
    const uuidUs = await probeLatency(uuidPath, u, UUID, randomKeys(PROBES, uuidRows), true);
    report("warm PK read: probe + 2 columns (UUID key)", `${uuidUs.toFixed(2)} µs`, "≤ 15 µs");
    await uuidHarness.close();
  }
  if (runs("insert")) {
    // Relaxed single-row INSERT.
    const relaxed = new Harness("relaxed", "memory");
    const relaxedPath = await relaxed.open();
    const r = await createKeyed(relaxedPath, "inserts", INT);
    const one = { id: ints([0]), v: texts(["x"]), n: ints([1]) };
    const insertOne = async (id: number) => {
      if (one.id.data.layout === "i32") one.id.data.values[0] = id;
      const tx = await relaxedPath.begin();
      const statement = tx.statement({ rules });
      const inserted = statement.table(r).insert(batch(one.id, one.v, one.n), null);
      if (inserted instanceof Promise) await inserted;
      await statement.end();
      await tx.commit(null);
    };
    for (let id = 0; id < 5000; id++) await insertOne(id);
    const rounds: number[] = [];
    const allocated: number[] = [];
    const gcShare: number[] = [];
    let next = 5000;
    for (let round = 0; round < 5; round++) {
      const count = 10_000;
      let used = { user: 0, system: 0 };
      const measured = await measureAllocation(async () => {
        const cpu = process.cpuUsage();
        for (let k = 0; k < count; k++) await insertOne(next++);
        used = process.cpuUsage(cpu);
      });
      rounds.push((used.user + used.system) / count);
      allocated.push(measured.bytes / count);
      gcShare.push((100 * measured.gcMs) / measured.ms);
    }
    report("relaxed single-row INSERT (owner CPU)", `${median(rounds).toFixed(2)} µs`, "≤ 20 µs");
    report("  heap allocated per INSERT", `${median(allocated).toFixed(0)} B`, "");
    report("  GC share of INSERT time", `${median(gcShare).toFixed(1)} %`, "");
    await relaxed.close();
  }
  process.stdout.write(`${lines.join("\n")}\n`);
}

await main();
