/**
 * Review B3-p2b-1 #3: a popular long key, and merges that fail.
 * - The reviewer's probe: eight runs of 800 row ids under one 1,000-byte key merge into pieces
 *   whose fences stay under the 1 MiB blob cap, and every entry is found.
 * - In the engine: eight flushes of 800 rows sharing one 1,000-byte secondary key merge, and the
 *   index still returns every row.
 * - A merge that fails before its publish leaves the write path healthy and its inputs in place;
 *   a later merge runs.
 */
import { expect, it } from "vitest";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { compareBytes } from "../vector/varlen.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import { DEFAULT_MERGE_POLICY, mergeRuns, sortedRuns } from "./merge.js";
import { encodeRun, RunReader, runRef, type RunEntry } from "./runs.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  TEXT,
  tableInput,
  texts,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };
const MiB = 1024 * 1024;

it("eight runs of 800 row ids under one 1,000-byte key merge within the fence bound (the reviewer's probe)", async () => {
  const harness = new Harness("strict", "memory");
  await harness.open();
  const writer = harness.space.beginWrite("probe inputs");
  const readers: RunReader[] = [];
  const key = new Uint8Array(1000).fill(65);
  for (let i = 0; i < 8; i++) {
    const encoded = need(
      encodeRun([
        {
          key,
          entries: Array.from({ length: 800 }, (_, j) => ({
            rowId: i * 800 + j + 1,
            tombstone: false,
          })),
        },
      ]),
    );
    const ref = runRef(encoded, (await writer.write(encoded.parts)).blobs);
    readers.push(await RunReader.open(harness.space, ref, FREE, FREE));
  }
  const grant = await harness.governor.admit({
    label: "probe merge",
    minimumBytes: 8 * MiB,
    spillable: false,
  });
  const out = harness.space.beginWrite("probe output", { budget: grant });
  try {
    const result = await mergeRuns({
      space: harness.space,
      writer: out,
      grant,
      readers,
      level: 1,
      bottom: true,
      policy: DEFAULT_MERGE_POLICY,
      stopped: () => false,
    });
    expect(result.entriesOut).toBe(6400);
    expect(result.runs.length).toBeGreaterThan(1);
    expect(sortedRuns(result.runs, 0, result.runs.length)).toBe(1);
    const found: RunEntry[] = [];
    for (const ref of result.runs) {
      expect(ref.fences.length).toBeLessThanOrEqual(MiB);
      expect(ref.entries.length).toBeLessThanOrEqual(MiB);
      expect(compareBytes(ref.minKey, 0, ref.minKey.length, key, 0, key.length)).toBe(0);
      const reader = await RunReader.open(harness.space, ref, FREE, FREE);
      await reader.lookup(harness.space, key, found, FREE);
    }
    expect(found.map((entry) => entry.rowId)).toEqual(
      Array.from({ length: 6400 }, (_, k) => k + 1),
    );
  } finally {
    if (out.state === "open") await out.abort();
    if (writer.state === "open") await writer.abort();
    grant.close();
    await harness.close();
  }
}, 120_000);

async function hotTable(path: WritePathImpl): Promise<{ t: number; index: number }> {
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("s", TEXT)]),
        })
      ).id ?? 0;
  });
  const s = path.catalog.table(t)?.columns[1]?.id ?? 0;
  await autocommit(path, (statement) =>
    statement.changeCatalog({
      kind: "createIndex",
      index: { tableId: t, name: "t_s", columnIds: [s], unique: false, role: "secondary" },
    }),
  );
  const index = need(path.catalog.table(t)?.indexes.find((candidate) => candidate.name === "t_s"));
  return { t, index: index.id };
}

const HOT = "h".repeat(1000);

async function holders(path: WritePathImpl, t: number, index: number): Promise<number> {
  const view = path.pin();
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: texts([HOT]), direction: "asc", nulls: "first" }],
    { length: 1, selection: null },
    keys,
  );
  const out = { probes: new Int32Array(8192), locators: new Float64Array(8192), count: 0 };
  const result = await view.index(t, index).probe(keys, out);
  keys.close();
  view.release();
  expect(result.done).toBe(true);
  return out.count;
}

it("eight flushes of 800 rows under one 1,000-byte secondary key merge, and every row is found", async () => {
  const harness = new Harness("strict", "memory", 800);
  const path = await harness.open();
  const { t, index } = await hotTable(path);
  for (let flush = 0; flush < 8; flush++)
    for (let from = 0; from < 800; from += 200) {
      const ids = Array.from({ length: 200 }, (_, k) => flush * 800 + from + k);
      await autocommit(path, (s) =>
        s.table(t).insert(batch(ints(ids), texts(ids.map(() => HOT))), null),
      );
    }
  // One more row freezes the eighth memtable, so eight runs of 800 hot entries wait to merge.
  await autocommit(path, (s) => s.table(t).insert(batch(ints([6400]), texts(["other"])), null));
  await path.quiesce();
  expect(path.failure).toBeNull();
  expect(path.maintenance.merges).toBeGreaterThan(0);
  expect(path.maintenance.mergeFailures).toBe(0);
  expect(path.maintenance.splitMerges).toBeGreaterThan(0);
  expect(await holders(path, t, index)).toBe(6400);
  const reopened = await harness.reopen();
  expect(await holders(reopened, t, index)).toBe(6400);
  await harness.space.audit();
  await harness.close();
}, 300_000);

it("a merge that fails before its publish keeps its inputs and the write path healthy", async () => {
  const harness = new Harness("strict", "memory", 4, undefined, { merges: { fanIn: 2 } });
  const path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  // Every read a merge makes (its grant's label) fails until the fault is lifted.
  let failing = true;
  const read = harness.space.read.bind(harness.space);
  harness.space.read = (refs, options) => {
    const label = (options.budget as { label?: string }).label ?? "";
    if (failing && label.startsWith("merge"))
      return Promise.reject(new Error("injected read fault"));
    return read(refs, options);
  };
  for (let from = 0; from < 12; from += 4)
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(Array.from({ length: 4 }, (_, k) => from + k))), null),
    );
  await path.quiesce();
  expect(path.failure).toBeNull();
  expect(path.maintenance.mergeFailures).toBeGreaterThanOrEqual(2); // a try and its retry
  expect(path.maintenance.lastMergeFailure).toContain("injected read fault");
  expect(path.maintenance.merges).toBe(0);
  const primary = need(path.catalog.table(t)?.primaryKey);
  expect(path.state().tables.get(t)?.runs.get(primary)?.length).toBeGreaterThanOrEqual(2);
  // Writing continues; once the fault is gone, the next flush's debt merges.
  failing = false;
  for (let from = 12; from < 20; from += 4)
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(Array.from({ length: 4 }, (_, k) => from + k))), null),
    );
  await path.quiesce();
  expect(path.failure).toBeNull();
  expect(path.maintenance.merges).toBeGreaterThan(0);
  await harness.space.audit();
  await harness.close();
}, 120_000);
