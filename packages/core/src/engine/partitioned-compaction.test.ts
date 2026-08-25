/**
 * Partitioned folds of keyed tables.
 *
 * A keyed table's folded form is a run of level-one partitions, and a fold rewrites only the
 * partitions its deltas touch. These suites pin the layout invariants (ordered, bounded,
 * disjoint partitions), that a fold's cost is the partitions it touches and not the table, that
 * contents and visible order survive a long random history with the background loop folding
 * underneath, and that a fold in flight survives a restart, a cancellation, and concurrent
 * writes.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { type CompactionJobRecord, type SegmentRecord } from "../storage/types.js";
import { MinnowDatabase } from "./database.js";
import { allVisibleSegments } from "./storage-test-helpers.js";
import { seedFor } from "../testing/seeds.js";

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

interface Row {
  id: number;
  amount: number;
  label: string;
  [column: string]: string | number;
}

const PARTITION_ROWS = 16;
const LABELS = ["alpha", "bravo", "charlie", "delta"] as const;

async function visibleRecords(
  database: MinnowDatabase,
  store: MemoryBlockStore,
  tableName: string,
): Promise<SegmentRecord[]> {
  const visible = await allVisibleSegments(database, tableName);
  const records: SegmentRecord[] = [];
  for (const segment of visible) {
    const record = await store.getSegment(segment.id);
    if (record === undefined) throw new Error(`Expected segment ${segment.id}`);
    records.push(record);
  }
  return records;
}

function rowIdsOf(segment: SegmentRecord): bigint[] {
  const kind = segment.kind;
  if (kind === "update" || kind === "delete") return [];
  const spans = segment.rowIdSpans;
  const ids: bigint[] = [];
  for (const span of spans) {
    for (let offset = 0; offset < span.rowCount; offset += 1) {
      ids.push(span.rowIdStart + BigInt(offset));
    }
  }
  return ids;
}

/**
 * The layout a keyed table must keep: a prefix of level-one partitions with explicit, strictly
 * increasing logical orders, then level-zero history; row-ID footprints pairwise disjoint; and
 * — where `maxPartitionRows` is given — no partition larger than the target.
 */
function assertPartitionLayout(
  records: readonly SegmentRecord[],
  maxPartitionRows?: number,
): { partitions: SegmentRecord[]; level0: SegmentRecord[] } {
  const partitions: SegmentRecord[] = [];
  let index = 0;
  while (index < records.length && (records[index]?.level ?? 0) === 1) {
    const partition = records[index];
    if (partition === undefined) break;
    const kind = partition.kind;
    expect(kind === "base" || kind === "insert").toBe(true);
    expect(partition.partitionOrdinal).toBeUndefined();
    expect(Number.isFinite(partition.logicalOrder)).toBe(true);
    expect(partition.logicalOrder).toBeGreaterThanOrEqual(0);
    const previous = partitions[partitions.length - 1];
    if (previous !== undefined) {
      expect(partition.logicalOrder).toBeGreaterThan(previous.logicalOrder);
    }
    if (maxPartitionRows !== undefined) {
      expect(partition.rowCount).toBeLessThanOrEqual(maxPartitionRows);
    }
    partitions.push(partition);
    index += 1;
  }
  const level0 = records.slice(index);
  for (const segment of level0) expect(segment.level).toBe(0);
  const seen = new Set<bigint>();
  for (const record of records) {
    for (const rowId of rowIdsOf(record)) {
      expect(seen.has(rowId), `row ID ${String(rowId)} appears twice`).toBe(false);
      seen.add(rowId);
    }
  }
  return { partitions, level0 };
}

/** A keyed table written in thirty small commits and folded once into sixteen-row partitions. */
async function partitionedFixture(): Promise<{
  store: MemoryBlockStore;
  database: MinnowDatabase;
  reference: Map<number, Row>;
  order: number[];
  partitions: SegmentRecord[];
}> {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 8,
    compression: "raw",
    autoCompact: false,
  });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "amount", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  const reference = new Map<number, Row>();
  const order: number[] = [];
  for (let batch = 0; batch < 30; batch += 1) {
    const rows: Row[] = Array.from({ length: 8 }, (_, offset) => {
      const id = batch * 8 + offset;
      return { id, amount: id * 10, label: LABELS[id % LABELS.length] ?? "alpha" };
    });
    await database.insertBatch("items", rows);
    for (const row of rows) {
      reference.set(row.id, row);
      order.push(row.id);
    }
  }
  const result = await database.compactTable("items", {
    partitionRows: PARTITION_ROWS,
    outputCompression: "raw",
  });
  expect(result).toMatchObject({ compacted: true, rowCount: 240, sourceSegmentCount: 30 });
  const { partitions, level0 } = assertPartitionLayout(
    await visibleRecords(database, store, "items"),
    PARTITION_ROWS,
  );
  expect(level0).toHaveLength(0);
  expect(partitions).toHaveLength(240 / PARTITION_ROWS);
  expect(result.outputSegmentIds).toEqual(partitions.map((partition) => partition.id));
  return { store, database, reference, order, partitions };
}

async function expectContents(
  database: MinnowDatabase,
  reference: ReadonlyMap<number, Row>,
  order: readonly number[],
  context: string,
): Promise<void> {
  const expected = [...reference.values()].sort((left, right) => left.id - right.id);
  const rows = (
    await database.query("SELECT id, amount, label FROM items ORDER BY id", { memoize: false })
  ).rows as unknown as Row[];
  expect(rows, `${context}: ordered contents`).toEqual(expected);
  // Unordered reads keep written order: rewritten partitions stay in place, new rows append.
  const unordered = (await database.readTable("items")) as unknown as Row[];
  expect(
    unordered.map((row) => row.id),
    `${context}: visible order`,
  ).toEqual(order);
  const lookupKey = expected[Math.floor(expected.length / 2)]?.id;
  if (lookupKey !== undefined) {
    const found = await database.query("SELECT amount FROM items WHERE id = ?", {
      params: [lookupKey],
      memoize: false,
    });
    expect(found.rows, `${context}: keyed lookup`).toEqual([
      { amount: reference.get(lookupKey)?.amount },
    ]);
  }
}

function requiredRow(reference: ReadonlyMap<number, Row>, id: number): Row {
  const row = reference.get(id);
  if (row === undefined) throw new Error(`Expected reference row ${String(id)}`);
  return row;
}

async function requiredJob(store: MemoryBlockStore, jobId: string): Promise<CompactionJobRecord> {
  const job = await store.getCompactionJob(jobId);
  if (job === undefined) throw new Error(`Expected compaction job ${jobId}`);
  return job;
}

describe("partitioned folds of a keyed table", () => {
  it("splits oversized interior partitions when the target is lowered", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      rowsPerBlock: 8,
      compression: "raw",
      autoCompact: false,
    });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
        { name: "label", type: "string" },
      ],
    });
    const expected: Row[] = [];
    for (let batch = 0; batch < 12; batch += 1) {
      const rows = Array.from({ length: 10 }, (_, offset) => {
        const id = batch * 10 + offset;
        return { id, amount: id, label: "seed" } satisfies Row;
      });
      expected.push(...rows);
      await database.insertBatch("items", rows);
    }
    await database.compactTable("items", { partitionRows: 40, outputCompression: "raw" });
    const before = assertPartitionLayout(
      await visibleRecords(database, store, "items"),
      40,
    ).partitions;
    expect(before.map((partition) => partition.rowCount)).toEqual([40, 40, 40]);

    await database.updateBatch("items", { keys: [55], changes: { amount: [-55] } });
    expected[55] = { id: 55, amount: -55, label: "seed" };
    const result = await database.compactTable("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    expect(result).toMatchObject({ compacted: true, sourceSegmentCount: 4, rowCount: 120 });
    const after = assertPartitionLayout(
      await visibleRecords(database, store, "items"),
      PARTITION_ROWS,
    );
    expect(after.level0).toHaveLength(0);
    expect(after.partitions).toHaveLength(9);
    expect(after.partitions.every((partition) => partition.rowCount <= PARTITION_ROWS)).toBe(true);
    expect(await database.readTable("items")).toEqual(expected);
    store.close();
  });

  it("keeps the table as ordered, bounded, disjoint level-one partitions across folds", async () => {
    const { store, database, reference, order, partitions } = await partitionedFixture();
    const before = new Map(partitions.map((partition) => [partition.logicalOrder, partition]));

    // Deltas into four partitions, a delete, new rows, and an upsert that patches in place.
    const patch = async (id: number, amount: number): Promise<void> => {
      await database.updateBatch("items", { keys: [id], changes: { amount: [amount] } });
      const row = reference.get(id);
      if (row !== undefined) reference.set(id, { ...row, amount });
    };
    await patch(2 * PARTITION_ROWS + 3, -1);
    await patch(9 * PARTITION_ROWS + 1, -2);
    await database.deleteBatch("items", { keys: [5 * PARTITION_ROWS + 7] });
    reference.delete(5 * PARTITION_ROWS + 7);
    order.splice(order.indexOf(5 * PARTITION_ROWS + 7), 1);
    const fresh: Row[] = [1000, 1001, 1002, 1003, 1004].map((id) => ({
      id,
      amount: id,
      label: "fresh",
    }));
    await database.insertBatch("items", fresh);
    for (const row of fresh) {
      reference.set(row.id, row);
      order.push(row.id);
    }
    const upsertExisting: Row = { id: 12 * PARTITION_ROWS + 5, amount: 777, label: "upsert" };
    const upsertNew: Row = { id: 2000, amount: 2000, label: "upsert" };
    await database.upsertBatch("items", [upsertExisting, upsertNew]);
    reference.set(upsertExisting.id, upsertExisting);
    reference.set(upsertNew.id, upsertNew);
    order.push(upsertNew.id);
    await expectContents(database, reference, order, "before the second fold");

    const result = await database.compactTable("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    expect(result.compacted).toBe(true);
    const records = await visibleRecords(database, store, "items");
    const layout = assertPartitionLayout(records, PARTITION_ROWS);
    expect(layout.level0).toHaveLength(0);
    // The four touched partitions were rewritten in place; the new rows opened one partition
    // behind the rest; every other partition is the very same segment as before.
    expect(result.outputSegmentIds).toHaveLength(5);
    expect(layout.partitions).toHaveLength(partitions.length + 1);
    for (const partition of layout.partitions) {
      const previous = before.get(partition.logicalOrder);
      if (previous === undefined) {
        // The new tail partition: only the appended rows, ordered behind every old partition.
        expect(partition.rowCount).toBe(6);
        expect(partition.logicalOrder).toBeGreaterThan(
          Math.max(...partitions.map((old) => old.logicalOrder)),
        );
        continue;
      }
      if (result.outputSegmentIds?.includes(partition.id)) {
        // A rewritten partition keeps a subset of its old row IDs: patched rows in place,
        // deleted rows gone, nothing foreign.
        const oldIds = new Set(rowIdsOf(previous));
        expect(rowIdsOf(partition).every((rowId) => oldIds.has(rowId))).toBe(true);
        expect(partition.kind).toBe("base");
      } else {
        expect(partition.id).toBe(previous.id);
      }
    }
    await expectContents(database, reference, order, "after the second fold");
    store.close();
  });

  it("rewrites only the partitions the deltas touch", async () => {
    const { store, database, reference, order, partitions } = await partitionedFixture();
    const total = await storedBytes(store, partitions);

    // Two point updates into partitions 2 and 9.
    const first = 2 * PARTITION_ROWS + 4;
    const second = 9 * PARTITION_ROWS + 12;
    await database.updateBatch("items", { keys: [first], changes: { amount: [1] } });
    await database.updateBatch("items", { keys: [second], changes: { amount: [2] } });
    reference.set(first, { ...requiredRow(reference, first), amount: 1 });
    reference.set(second, { ...requiredRow(reference, second), amount: 2 });
    const deltas = (await allVisibleSegments(database, "items")).slice(partitions.length);
    expect(deltas).toHaveLength(2);

    const result = await database.compactTable("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    if (result.jobId === undefined) throw new Error("Expected a fold job");
    const job = await requiredJob(store, result.jobId);
    expect(job.sourceSegmentIds).toEqual([
      partitions[2]?.id,
      partitions[9]?.id,
      ...deltas.map((segment) => segment.id),
    ]);
    expect(job.rewritePlan.kind).toBe("merge-v1");
    if (job.rewritePlan.kind !== "merge-v1") throw new Error("Expected a merge plan");
    expect(job.rewritePlan.partitions).toEqual([
      { rowStart: 0, rowCount: PARTITION_ROWS, logicalOrder: partitions[2]?.logicalOrder },
      {
        rowStart: PARTITION_ROWS,
        rowCount: PARTITION_ROWS,
        logicalOrder: partitions[9]?.logicalOrder,
      },
    ]);
    // Two partitions of three columns: six output blocks, thirty-two rows, and bytes in
    // proportion — not the 240-row table.
    expect(result).toMatchObject({
      rowCount: 2 * PARTITION_ROWS,
      outputBlockCount: 6,
      sourceSegmentCount: 4,
    });
    expect(result.outputSegmentIds).toHaveLength(2);
    expect(result.outputStoredBytes).toBeLessThan(total / 4);
    const after = await visibleRecords(database, store, "items");
    expect(after.map((segment) => segment.id)).toEqual(
      partitions.map((partition, index) =>
        index === 2 || index === 9
          ? (result.outputSegmentIds?.[index === 2 ? 0 : 1] ?? "")
          : partition.id,
      ),
    );
    assertPartitionLayout(after, PARTITION_ROWS);
    await expectContents(database, reference, order, "after the targeted fold");

    // Pure inserts touch nothing: they open a partition behind the rest, and the last partition
    // — already at the target — is left alone.
    const fresh: Row[] = Array.from({ length: 5 }, (_, index) => ({
      id: 5000 + index,
      amount: index,
      label: "fresh",
    }));
    await database.insertBatch("items", fresh);
    await database.insertBatch("items", [{ id: 6000, amount: 6, label: "fresh" }]);
    for (const row of [...fresh, { id: 6000, amount: 6, label: "fresh" }]) {
      reference.set(row.id, row);
      order.push(row.id);
    }
    const appended = await database.compactTable("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    expect(appended).toMatchObject({ compacted: true, sourceSegmentCount: 2, rowCount: 6 });
    expect(appended.outputSegmentIds).toHaveLength(1);
    const withTail = await visibleRecords(database, store, "items");
    expect(withTail.slice(0, after.length).map((segment) => segment.id)).toEqual(
      after.map((segment) => segment.id),
    );
    expect(withTail).toHaveLength(after.length + 1);
    await expectContents(database, reference, order, "after the append fold");

    // New rows join the small tail partition while it is below the target: the tail is the
    // only partition rewritten, and it stays one partition.
    await database.insertBatch("items", [{ id: 7000, amount: 7, label: "fresh" }]);
    reference.set(7000, { id: 7000, amount: 7, label: "fresh" });
    order.push(7000);
    const absorbed = await database.compactTable("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    if (absorbed.jobId === undefined) throw new Error("Expected an absorbing fold");
    const absorbingJob = await requiredJob(store, absorbed.jobId);
    expect(absorbingJob.sourceSegmentIds[0]).toBe(withTail[withTail.length - 1]?.id);
    expect(absorbed).toMatchObject({ sourceSegmentCount: 2, rowCount: 7 });
    expect(absorbed.outputSegmentIds).toHaveLength(1);
    const settled = await visibleRecords(database, store, "items");
    expect(settled).toHaveLength(withTail.length);
    expect(settled[settled.length - 1]?.logicalOrder).toBe(
      withTail[withTail.length - 1]?.logicalOrder,
    );
    await expectContents(database, reference, order, "after the absorbing fold");
    store.close();
  });

  it("survives a restart mid-fold and a cancelled attempt", async () => {
    const { store, database, reference, order, partitions } = await partitionedFixture();
    for (const index of [1, 6, 11]) {
      const id = index * PARTITION_ROWS + 2;
      await database.updateBatch("items", { keys: [id], changes: { amount: [-index] } });
      reference.set(id, { ...requiredRow(reference, id), amount: -index });
    }
    // One block, then the "tab" goes away; a fresh database on the same store resumes it.
    const started = await database.compactTableStep("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
      maxBlocks: 1,
    });
    if (started.jobId === null) throw new Error("Expected a persisted fold job");
    expect(started.result).toBeNull();
    expect((await requiredJob(store, started.jobId)).outputBlockIds).toHaveLength(1);
    const reopened = new MinnowDatabase(store, {
      rowsPerBlock: 8,
      compression: "raw",
      autoCompact: false,
    });
    let progress = await reopened.resumeCompactionJob(started.jobId, { maxBlocks: 2 });
    while (progress.result === null) {
      progress = await reopened.resumeCompactionJob(started.jobId, { maxBlocks: 2 });
    }
    expect(progress.result.outputSegmentIds).toHaveLength(3);
    const resumed = await visibleRecords(reopened, store, "items");
    assertPartitionLayout(resumed, PARTITION_ROWS);
    expect(resumed).toHaveLength(partitions.length);
    await expectContents(reopened, reference, order, "after the resumed fold");

    // A cancelled attempt leaves the table as it was; the retry publishes under a new job.
    for (const index of [3, 8]) {
      const id = index * PARTITION_ROWS + 9;
      await reopened.updateBatch("items", { keys: [id], changes: { amount: [100 + index] } });
      reference.set(id, { ...requiredRow(reference, id), amount: 100 + index });
    }
    const attempt = await reopened.compactTableStep("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
      maxBlocks: 1,
    });
    if (attempt.jobId === null) throw new Error("Expected a cancellable fold job");
    expect(attempt.result).toBeNull();
    expect(await reopened.cancelCompactionJob(attempt.jobId)).toMatchObject({
      state: "cancelled",
    });
    const untouched = await visibleRecords(reopened, store, "items");
    expect(untouched.map((segment) => segment.id)).toEqual([
      ...resumed.map((segment) => segment.id),
      ...(await allVisibleSegments(reopened, "items")).slice(resumed.length).map((s) => s.id),
    ]);
    await expectContents(reopened, reference, order, "after the cancelled attempt");
    const retried = await reopened.compactTable("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    expect(retried.compacted).toBe(true);
    expect(retried.jobId).not.toBe(attempt.jobId);
    expect(retried.outputSegmentIds).toHaveLength(2);
    const afterRetry = await visibleRecords(reopened, store, "items");
    assertPartitionLayout(afterRetry, PARTITION_ROWS);
    expect(afterRetry).toHaveLength(partitions.length);
    for (const id of retried.outputSegmentIds ?? []) {
      expect(afterRetry.some((segment) => segment.id === id)).toBe(true);
    }
    await expectContents(reopened, reference, order, "after the retried fold");
    store.close();
  });

  it("rebases a fold in flight across concurrent writes to other partitions", async () => {
    const { store, database, reference, order, partitions } = await partitionedFixture();
    const touched = 4 * PARTITION_ROWS + 1;
    await database.updateBatch("items", { keys: [touched], changes: { amount: [-4] } });
    reference.set(touched, { ...requiredRow(reference, touched), amount: -4 });
    const started = await database.compactTableStep("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
      maxBlocks: 1,
    });
    if (started.jobId === null) throw new Error("Expected a fold job");
    expect(started.result).toBeNull();

    // While the fold is half written: a new row, an update in a partition it does not touch,
    // and a delete inside the partition it is rewriting. All three are newer than the fold's
    // sources, so they stay level-zero history on top of the published output.
    const other = 10 * PARTITION_ROWS + 3;
    await database.insertBatch("items", [{ id: 9000, amount: 9, label: "late" }]);
    await database.updateBatch("items", { keys: [other], changes: { amount: [-10] } });
    await database.deleteBatch("items", { keys: [4 * PARTITION_ROWS + 2] });
    reference.set(9000, { id: 9000, amount: 9, label: "late" });
    order.push(9000);
    reference.set(other, { ...requiredRow(reference, other), amount: -10 });
    reference.delete(4 * PARTITION_ROWS + 2);
    order.splice(order.indexOf(4 * PARTITION_ROWS + 2), 1);

    let progress = await database.resumeCompactionJob(started.jobId, { maxBlocks: 4 });
    while (progress.result === null) {
      progress = await database.resumeCompactionJob(started.jobId, { maxBlocks: 4 });
    }
    expect(progress.result.compacted).toBe(true);
    const records = await visibleRecords(database, store, "items");
    const layout = assertPartitionLayout(records, PARTITION_ROWS);
    expect(layout.partitions).toHaveLength(partitions.length);
    expect(layout.level0).toHaveLength(3);
    await expectContents(database, reference, order, "after the rebased fold");

    // The next fold absorbs the concurrent history: one rewrite of partitions 4 and 10, and
    // the late row joins the last partition only because it is still below the target.
    const next = await database.compactTable("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    expect(next.compacted).toBe(true);
    const settled = await visibleRecords(database, store, "items");
    const settledLayout = assertPartitionLayout(settled, PARTITION_ROWS);
    expect(settledLayout.level0).toHaveLength(0);
    await expectContents(database, reference, order, "after absorbing the concurrent history");
    store.close();
  });

  it("rejects a persisted plan whose partitions do not tile the output", async () => {
    const { store, database, partitions } = await partitionedFixture();
    await database.updateBatch("items", { keys: [PARTITION_ROWS + 1], changes: { amount: [1] } });
    const result = await database.compactTable("items", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    if (result.jobId === undefined) throw new Error("Expected a fold job");
    const job = await requiredJob(store, result.jobId);
    if (job.rewritePlan.kind !== "merge-v1" || job.rewritePlan.partitions === undefined) {
      throw new Error("Expected a partitioned merge plan");
    }
    expect(partitions.length).toBeGreaterThan(1);
    const plan = job.rewritePlan;
    const attempt = (partitionsValue: unknown, id: string) =>
      store.createCompactionJob({
        ...job,
        id,
        rewritePlan: { ...plan, partitions: partitionsValue } as typeof plan,
      });
    await expect(attempt([{ rowStart: 0, rowCount: 1, logicalOrder: 0 }], "short")).rejects.toThrow(
      "must cover every planned row",
    );
    await expect(
      attempt(
        [
          { rowStart: 0, rowCount: 8, logicalOrder: 5 },
          { rowStart: 8, rowCount: 8, logicalOrder: 5 },
        ],
        "equal-orders",
      ),
    ).rejects.toThrow("strictly increasing");
    await expect(
      attempt(
        [
          { rowStart: 0, rowCount: 5, logicalOrder: 1 },
          { rowStart: 5, rowCount: 11, logicalOrder: 2 },
        ],
        "straddle",
      ),
    ).rejects.toThrow("straddle");
    store.close();
  });
});

describe("partitioned rechunk folds of a keyless table", () => {
  it("publishes bounded partitions, rewrites only the tail, and rebases across an append", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      rowsPerBlock: 8,
      compression: "raw",
      autoCompact: false,
    });
    await database.createTable({
      name: "events",
      columns: [
        { name: "sequence", type: "number" },
        { name: "label", type: "string" },
      ],
    });
    const expected: Array<{ sequence: number; label: string }> = [];
    const append = async (count: number): Promise<void> => {
      const start = expected.length;
      const rows = Array.from({ length: count }, (_, offset) => ({
        sequence: start + offset,
        label: `event-${String(start + offset)}`,
      }));
      expected.push(...rows);
      await database.insertBatch("events", rows);
    };
    for (let batch = 0; batch < 20; batch += 1) await append(5);

    const first = await database.compactTable("events", {
      partitionRows: PARTITION_ROWS,
      outputCompression: "raw",
    });
    expect(first.outputSegmentIds).toHaveLength(7);
    const initial = assertPartitionLayout(
      await visibleRecords(database, store, "events"),
      PARTITION_ROWS,
    ).partitions;
    expect(initial.map((partition) => partition.rowCount)).toEqual([16, 16, 16, 16, 16, 16, 4]);
    expect(initial.every((partition) => partition.kind === "insert")).toBe(true);

    await append(26);
    const tail = await database.compactTable("events", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    expect(tail).toMatchObject({ compacted: true, sourceSegmentCount: 2, rowCount: 30 });
    const afterTail = assertPartitionLayout(
      await visibleRecords(database, store, "events"),
      PARTITION_ROWS,
    );
    expect(afterTail.partitions.slice(0, 6).map((partition) => partition.id)).toEqual(
      initial.slice(0, 6).map((partition) => partition.id),
    );
    expect(afterTail.partitions.slice(6).map((partition) => partition.rowCount)).toEqual([16, 14]);

    await append(20);
    const started = await database.compactTableStep("events", {
      partitionRows: PARTITION_ROWS,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
      maxBlocks: 1,
    });
    if (started.jobId === null) throw new Error("Expected a keyless partition job");
    expect(started.result).toBeNull();
    await append(2);
    const reopened = new MinnowDatabase(store, {
      rowsPerBlock: 8,
      compression: "raw",
      autoCompact: false,
    });
    let progress = await reopened.resumeCompactionJob(started.jobId, { maxBlocks: 2 });
    while (progress.result === null) {
      progress = await reopened.resumeCompactionJob(started.jobId, { maxBlocks: 2 });
    }
    const rebased = assertPartitionLayout(
      await visibleRecords(reopened, store, "events"),
      PARTITION_ROWS,
    );
    expect(rebased.level0).toHaveLength(1);
    expect(await reopened.readTable("events")).toEqual(expected);

    const promoted = await reopened.compactTable("events", {
      targetLevel: 2,
      minimumLevel0Segments: 1,
      maxWriteAmplification: 1_000,
      outputCompression: "raw",
    });
    expect(promoted).toMatchObject({
      compacted: true,
      sourceSegmentCount: 1,
      outputPartitionOrdinal: 0,
    });
    const afterPromotion = await visibleRecords(reopened, store, "events");
    expect(afterPromotion.slice(0, rebased.partitions.length).map((segment) => segment.id)).toEqual(
      rebased.partitions.map((segment) => segment.id),
    );
    expect(afterPromotion.at(-1)).toMatchObject({ level: 2, partitionOrdinal: 0, rowCount: 2 });
    expect(await reopened.readTable("events")).toEqual(expected);
    store.close();
  });
});

describe("partitioned folds under the background loop", () => {
  // The settle loop deliberately permits 40 seconds under slow instrumentation. On shared CI
  // runners the 36k-row workload can consume another minute while other Vitest files contend for
  // CPU, so the outer timeout leaves room for both without changing the settle deadline or any
  // correctness assertion.
  it("keeps a large keyed table equal to a reference while auto-compaction folds it", async () => {
    const seed = seedFor("partitioned-compaction-soak", 0x7a11);
    const random = mulberry32(seed);
    const store = new MemoryBlockStore();
    // Small partitions, so a 36k-row table is eighteen of them and a fold's selectivity shows.
    const SOAK_PARTITION_ROWS = 2_048;
    const database = new MinnowDatabase(store, {
      rowsPerBlock: 512,
      compaction: { partitionRows: SOAK_PARTITION_ROWS },
    });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
        { name: "label", type: "string" },
      ],
    });
    const KEY_SPACE = 40_000;
    const reference = new Map<number, Row>();
    const initial: Row[] = Array.from({ length: 36_000 }, (_, id) => ({
      id,
      amount: Math.floor(random() * 1000),
      label: LABELS[id % LABELS.length] ?? "alpha",
    }));
    await database.insertBatch("items", initial);
    for (const row of initial) reference.set(row.id, row);

    const check = async (context: string): Promise<void> => {
      const expected = [...reference.values()].sort((left, right) => left.id - right.id);
      const aggregate = (
        await database.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM items", {
          memoize: false,
        })
      ).rows[0] as { n: number; total: number | null };
      expect(aggregate.n, `${context}: count`).toBe(expected.length);
      expect(aggregate.total ?? 0, `${context}: sum`).toBe(
        expected.reduce((sum, row) => sum + row.amount, 0),
      );
      const rows = (
        await database.query("SELECT id, amount, label FROM items ORDER BY id", {
          memoize: false,
        })
      ).rows as unknown as Row[];
      expect(rows.length, `${context}: rows`).toBe(expected.length);
      for (let index = 0; index < expected.length; index += 1) {
        const want = expected[index];
        const got = rows[index];
        if (
          want === undefined ||
          got?.id !== want.id ||
          got.amount !== want.amount ||
          got.label !== want.label
        ) {
          throw new Error(
            `seed ${String(seed)}, ${context}: row ${String(index)} diverged — wanted ` +
              `${JSON.stringify(want)}, got ${JSON.stringify(got)}`,
          );
        }
      }
      const probe = expected[Math.floor(random() * expected.length)];
      if (probe !== undefined) {
        const found = await database.query("SELECT amount FROM items WHERE id = ?", {
          params: [probe.id],
          memoize: false,
        });
        expect(found.rows, `${context}: lookup ${String(probe.id)}`).toEqual([
          { amount: probe.amount },
        ]);
      }
    };

    // Most operations land on a hot two per cent of the key space, the rest anywhere — the
    // shape that lets a fold leave most partitions alone. Every operation yields a macrotask,
    // as a write from a user event would, so the background loop gets to run between them;
    // a microtask-only loop over the memory store would starve it until the writes stop.
    const OPERATIONS = 2_400;
    const HOT_KEYS = Math.floor(KEY_SPACE * 0.02);
    for (let operation = 1; operation <= OPERATIONS; operation += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      const key =
        random() < 0.7
          ? KEY_SPACE - 1 - Math.floor(random() * HOT_KEYS)
          : Math.floor(random() * KEY_SPACE);
      const roll = random();
      const exists = reference.has(key);
      if (!exists && roll < 0.5) {
        const row: Row = { id: key, amount: Math.floor(random() * 1000), label: "new" };
        await database.insertBatch("items", [row]);
        reference.set(key, row);
      } else if (exists && roll < 0.25) {
        await database.deleteBatch("items", { keys: [key] });
        reference.delete(key);
      } else if (exists && roll < 0.8) {
        const amount = Math.floor(random() * 1000);
        await database.updateBatch("items", { keys: [key], changes: { amount: [amount] } });
        reference.set(key, { ...requiredRow(reference, key), amount });
      } else if (exists) {
        const row: Row = { id: key, amount: Math.floor(random() * 1000), label: "up" };
        await database.upsertBatch("items", [row]);
        reference.set(key, row);
      }
      if (operation % 300 === 0) await check(`after operation ${String(operation)}`);
    }

    // Let the background loop settle. Planning is not a job record yet, so an unchanged segment
    // count alone is not a completion signal under slow coverage instrumentation: keep waiting
    // while the actual fold condition is still due.
    const active = async (): Promise<number> =>
      (await database.listCompactionJobs("items")).filter(
        (job) => job.state === "planned" || job.state === "running" || job.state === "ready",
      ).length;
    let stable = 0;
    let lastVisible = -1;
    for (let attempt = 0; attempt < 4_000 && stable < 30; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      const currentRecords = await visibleRecords(database, store, "items");
      const visible = currentRecords.length;
      const deltas = currentRecords.filter((segment) => {
        const kind = segment.kind;
        return kind !== "insert" && kind !== "base";
      }).length;
      const due = visible >= 48 || deltas >= 32;
      stable = (await active()) === 0 && !due && visible === lastVisible ? stable + 1 : 0;
      lastVisible = visible;
    }
    expect(await active()).toBe(0);
    await check("settled");
    // Background collection prunes old fold records, so the evidence that folds happened is the
    // shape they left: bounded partitions, and a history short of the fold threshold. How many
    // partitions each fold touched depends on how many deltas landed while the previous one
    // ran, which is load-dependent; the targeted suite above pins that selectivity exactly.
    const published = (await database.listCompactionJobs("items")).filter(
      (job) => job.state === "published",
    );
    expect(published.length).toBeGreaterThanOrEqual(1);
    const records = await visibleRecords(database, store, "items");
    const layout = assertPartitionLayout(records, SOAK_PARTITION_ROWS);
    expect(layout.partitions.length).toBeGreaterThanOrEqual(10);
    expect(layout.level0.length).toBeLessThan(48);
    store.close();
  }, 180_000);
});

async function storedBytes(
  store: MemoryBlockStore,
  segments: readonly SegmentRecord[],
): Promise<number> {
  let total = 0;
  for (const segment of segments) {
    for (const blockId of Object.values(segment.columnBlockIds).flat()) {
      total += (await store.getBlock(blockId))?.byteLength ?? 0;
    }
  }
  return total;
}
