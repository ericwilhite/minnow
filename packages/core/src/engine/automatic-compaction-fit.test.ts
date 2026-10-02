/**
 * Automatic compaction on tables whose folds do not fit the default memory budget as planned.
 *
 * A fold's working memory follows the distinct keys its deltas touch, not how many times they
 * were written or how wide the rows are; an automatic fold that still does not fit is cut to
 * fit, and the smallest fold a table allows is given the memory it needs. None of it is
 * configured: these tests open databases with default options and leave maintenance to itself.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import type { CompactionJobRecord, SegmentRecord } from "../storage/types.js";
import { CompactionMemoryBudgetError, MinnowDatabase } from "./database.js";
import { allVisibleSegments, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(240_000) });

const DEFAULT_BUDGET = 32 * 1024 * 1024;

type Row = Record<string, string | number | Date>;

/** A keyed table forty columns wide: a text key, then numbers, timestamps, and short text. */
async function wideTable(database: MinnowDatabase, name = "t"): Promise<void> {
  const columns: Array<{
    name: string;
    type: "string" | "number" | "datetime";
    nullable: boolean;
  }> = [{ name: "id", type: "string", nullable: false }];
  for (let index = 0; index < 13; index += 1) {
    columns.push({ name: `n${String(index)}`, type: "number", nullable: true });
    columns.push({ name: `d${String(index)}`, type: "datetime", nullable: true });
    columns.push({ name: `s${String(index)}`, type: "string", nullable: true });
  }
  await database.createTable({ name, uniqueKey: "id", columns });
}

function wideRows(count: number, version = 0, offset = 0): Row[] {
  return Array.from({ length: count }, (_, position) => {
    const row = offset + position;
    const record: Row = { id: `key-${String(row).padStart(7, "0")}` };
    for (let index = 0; index < 13; index += 1) {
      record[`n${String(index)}`] = row * 13 + index + version;
      record[`d${String(index)}`] = new Date(1_700_000_000_000 + row * 1_000 + index);
      record[`s${String(index)}`] = `v${String((row + version) % 97)}-${String(index)}`;
    }
    return record;
  });
}

/** A deterministic shuffle, so a refresh can arrive in an order unlike the table's. */
function shuffled<T>(values: readonly T[], seed: number): T[] {
  const result = [...values];
  let state = seed;
  for (let index = result.length - 1; index > 0; index -= 1) {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31;
    const other = state % (index + 1);
    const value = result[index];
    const swap = result[other];
    if (value === undefined || swap === undefined) continue;
    result[index] = swap;
    result[other] = value;
  }
  return result;
}

function openDatabase(store = new MemoryBlockStore()) {
  const errors: unknown[] = [];
  const database = new MinnowDatabase(store, {
    onBackgroundError: (error) => errors.push(error),
  });
  return { store, database, errors };
}

async function publishedJobs(database: MinnowDatabase, table = "t") {
  return (await database.listCompactionJobs(table)).filter((job) => job.state === "published");
}

function isActive(job: CompactionJobRecord): boolean {
  return job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted";
}

/**
 * Waits for automatic compaction to go quiet on a table: no fold in flight and the visible
 * layout unchanged for a second. Fails after two minutes with the layout it last saw.
 */
async function settled(
  database: MinnowDatabase,
  store: MemoryBlockStore,
  table = "t",
): Promise<SegmentRecord[]> {
  let previous = "";
  let quiet = 0;
  for (let attempt = 0; attempt < 2_400 && quiet < 20; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    const active = (await database.listCompactionJobs(table)).some(isActive);
    const layout = JSON.stringify(
      (await allVisibleSegments(database, table)).map((segment) => segment.id),
    );
    quiet = !active && layout === previous ? quiet + 1 : 0;
    previous = layout;
  }
  expect(quiet, `Compaction did not settle: ${previous}`).toBe(20);
  const visible = await allVisibleSegments(database, table);
  const records = await Promise.all(visible.map((segment) => store.getSegment(segment.id)));
  return records.filter((segment) => segment !== undefined);
}

async function totals(database: MinnowDatabase, table = "t") {
  return (
    await database.query(`SELECT COUNT(*) AS n, SUM("n0") AS n0, MAX("s5") AS s5 FROM "${table}"`, {
      memoize: false,
    })
  ).rows;
}

describe("automatic compaction fits its folds to memory", () => {
  it("keeps folding a wide table that full-table refreshes keep rewriting", async () => {
    const { store, database, errors } = openDatabase();
    await wideTable(database);
    const rows = wideRows(7_000);
    await database.insertBatch("t", rows);
    // An application refreshing the whole table from a server: read it back, write it again.
    for (let refresh = 1; refresh <= 36; refresh += 1) {
      await database.upsertBatch("t", wideRows(7_000, refresh));
    }
    const segments = await settled(database, store);
    expect(errors).toEqual([]);
    expect(segments.length).toBeLessThanOrEqual(2);
    expect(segments.reduce((total, segment) => total + segment.rowCount, 0)).toBeLessThanOrEqual(
      14_000,
    );
    const jobs = await publishedJobs(database);
    expect(jobs.length).toBeGreaterThan(0);
    // Thirty-six rewrites of seven thousand forty-column rows fit the default budget: a fold
    // holds each touched key once, and replaced rows read in the table's order coalesce.
    for (const job of jobs) expect(job.memoryBudgetBytes).toBe(DEFAULT_BUDGET);
    expect(await totals(database)).toEqual(await totals(await reference(wideRows(7_000, 36))));
    await database.close();
  });

  it("cuts a fold whose distinct keys outgrow the budget, then folds the rest", async () => {
    const store = new MemoryBlockStore();
    const setup = new MinnowDatabase(store, { autoCompact: false });
    await wideTable(setup);
    const rows = wideRows(36_000);
    for (let start = 0; start < rows.length; start += 6_000) {
      await setup.insertBatch("t", rows.slice(start, start + 6_000));
    }
    // Thirty-six updates of a thousand different rows each: together they touch more keys
    // than one fold can patch in the default budget, which a smaller fold does not.
    for (let batch = 0; batch < 36; batch += 1) {
      await setup.execute(`UPDATE "t" SET "n0" = "n0" + 1 WHERE "id" >= ? AND "id" < ?`, [
        `key-${String(batch * 1_000).padStart(7, "0")}`,
        `key-${String((batch + 1) * 1_000).padStart(7, "0")}`,
      ]);
    }
    await expect(setup.compactTableStep("t", { maxLevel0Segments: 256 })).rejects.toBeInstanceOf(
      CompactionMemoryBudgetError,
    );
    await setup.close();

    // Reopened with defaults, the next commit wakes automatic compaction, which has to cut the
    // fold to make progress.
    const { database, errors } = openDatabase(store);
    await database.execute(`UPDATE "t" SET "n1" = 0 WHERE "id" = ?`, ["key-0000000"]);
    const segments = await settled(database, store);
    expect(errors).toEqual([]);
    // What is left is below every trigger: fewer than thirty-two deltas, holding fewer rows
    // than the table they overlay.
    const deltas = segments.filter((segment) => segment.kind === "update");
    expect(deltas.length).toBeLessThan(32);
    expect(deltas.reduce((total, segment) => total + segment.rowCount, 0)).toBeLessThan(36_000);
    const jobs = await publishedJobs(database);
    const deltaSources = jobs.map((job) =>
      job.rewritePlan.kind === "merge-v1"
        ? job.rewritePlan.sourceSegments.filter((source) => source.kind === "update").length
        : 0,
    );
    expect(jobs.length).toBeGreaterThan(1);
    expect(Math.max(...deltaSources)).toBeLessThan(36);
    for (const job of jobs) expect(job.memoryBudgetBytes).toBe(DEFAULT_BUDGET);
    expect(
      (await database.query(`SELECT COUNT(*) AS n, SUM("n0") AS n0 FROM "t"`, { memoize: false }))
        .rows,
    ).toEqual([
      {
        n: 36_000,
        n0: rows.reduce((total, row) => total + Number(row.n0), 0) + 36_000,
      },
    ]);
    await database.close();
  });

  it("gives the smallest fold the memory it needs when even that does not fit", async () => {
    const { store, database, errors } = openDatabase();
    await wideTable(database);
    const rows = wideRows(20_000);
    await database.insertBatch("t", rows);
    // One statement updating twenty thousand forty-column rows: patching them needs more than
    // the default budget even in the smallest fold, the base and this one delta.
    await database.execute(`UPDATE "t" SET "n0" = "n0" + 1`);
    await database.execute(`UPDATE "t" SET "n1" = "n1" + 1`);
    const segments = await settled(database, store);
    expect(errors).toEqual([]);
    // Two partitions of the folded rows. The second update, alone over them but as large as
    // the table, is due on its own and folded into them too.
    expect(segments.map((segment) => [segment.level, segment.kind])).toEqual([
      [1, "base"],
      [1, "base"],
    ]);
    const jobs = await publishedJobs(database);
    expect(jobs.some((job) => job.memoryBudgetBytes > DEFAULT_BUDGET)).toBe(true);
    expect(
      (
        await database.query(`SELECT COUNT(*) AS n, SUM("n0") AS n0, SUM("n1") AS n1 FROM "t"`, {
          memoize: false,
        })
      ).rows,
    ).toEqual([
      {
        n: 20_000,
        n0: rows.reduce((total, row) => total + Number(row.n0), 0) + 20_000,
        n1: rows.reduce((total, row) => total + Number(row.n1), 0) + 20_000,
      },
    ]);
    await database.close();
  });

  it("folds refreshes written in an unrelated order as cheaply as ordered ones", async () => {
    const { store, database, errors } = openDatabase();
    await wideTable(database);
    await database.insertBatch("t", wideRows(7_000));
    // Each refresh replaces every row in an order unrelated to the table's, so every output
    // cell reads a different source row than its neighbour.
    for (let refresh = 1; refresh <= 4; refresh += 1) {
      await database.upsertBatch("t", shuffled(wideRows(7_000, refresh), refresh));
    }
    const segments = await settled(database, store);
    expect(errors).toEqual([]);
    expect(segments.length).toBeLessThanOrEqual(2);
    expect(await totals(database)).toEqual(await totals(await reference(wideRows(7_000, 4))));
    const jobs = await publishedJobs(database);
    expect(jobs.length).toBeGreaterThan(0);
    for (const job of jobs) {
      const plan = job.rewritePlan;
      if (plan.kind !== "merge-v2") throw new Error("Expected a replayed merge");
      // The record every step rewrites holds the sources and windows, not a range per cell:
      // 7,000 reordered forty-column rows would be 280,000 ranges and tens of megabytes.
      const recordBytes = JSON.stringify(job, (_key, value: unknown) =>
        typeof value === "bigint" ? String(value) : value,
      ).length;
      expect(recordBytes).toBeLessThan(128 * 1024);
      expect(plan.outputs.length).toBeLessThanOrEqual(4);
    }
    await database.close();
  });

  it("folds one delta as large as its table without waiting for another write", async () => {
    const { store, database, errors } = openDatabase();
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
      ],
    });
    const rows = (version: number) =>
      Array.from({ length: 8_000 }, (_, id) => ({ id, amount: id + version }));
    await database.insertBatch("items", rows(0));
    // A single full-table refresh: one delta, then nothing more is written.
    await database.upsertBatch("items", rows(1));
    let segments = await settled(database, store, "items");
    expect(segments.map((segment) => segment.kind)).toEqual(["base"]);
    // The next refresh is a lone delta over the folded partition, and is folded as well.
    await database.upsertBatch("items", rows(2));
    segments = await settled(database, store, "items");
    expect(segments.map((segment) => segment.kind)).toEqual(["base"]);
    expect(errors).toEqual([]);
    expect(
      (await database.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM items")).rows,
    ).toEqual([{ n: 8_000, total: (7_999 * 8_000) / 2 + 2 * 8_000 }]);
    await database.close();
  });

  it("folds deltas as large as the table they overlay without waiting for thirty-two", async () => {
    const { store, database, errors } = openDatabase();
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
      ],
    });
    const rows = (version: number) =>
      Array.from({ length: 8_000 }, (_, id) => ({ id, amount: id + version }));
    await database.insertBatch("items", rows(0));
    await database.upsertBatch("items", rows(1));
    await database.upsertBatch("items", rows(2));
    const segments = await settled(database, store, "items");
    expect(segments).toHaveLength(1);
    expect(errors).toEqual([]);

    // Small tables and point updates are left to the segment counts.
    await database.createTable({
      name: "small",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
      ],
    });
    const small = Array.from({ length: 1_000 }, (_, id) => ({ id, amount: id }));
    await database.insertBatch("small", small);
    for (let refresh = 0; refresh < 4; refresh += 1) await database.upsertBatch("small", small);
    for (let id = 0; id < 8; id += 1) {
      await database.execute(`UPDATE items SET amount = amount + 1 WHERE id = ?`, [id]);
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(await allVisibleSegments(database, "small")).toHaveLength(5);
    expect(await allVisibleSegments(database, "items")).toHaveLength(9);
    await database.close();
  });
});

describe("merge planning keeps per-row replay semantics", () => {
  it("resolves upserts, updates, and deletes of the same keys in commit order", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { autoCompact: false });
    await database.createTable({
      name: "kv",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "a", type: "string", nullable: true },
        { name: "b", type: "number", nullable: true },
      ],
    });
    await database.insertBatch(
      "kv",
      Array.from({ length: 8 }, (_, id) => ({ id, a: `a${String(id)}`, b: id })),
    );
    // Replaced out of order, so patched rows read a later source in a different order.
    await database.upsertBatch("kv", [
      { id: 5, a: "u5", b: 50 },
      { id: 1, a: "u1", b: 10 },
      { id: 3, a: "u3", b: 30 },
      { id: 9, a: "new9", b: 90 },
    ]);
    // An update on top of an upsert names one column; the rest still come from the upsert.
    await database.execute("UPDATE kv SET b = 11 WHERE id = 1");
    await database.execute("UPDATE kv SET a = 'p4' WHERE id = 4");
    // A later upsert replaces the whole row again, update and all.
    await database.upsertBatch("kv", [{ id: 4, a: "u4", b: 40 }]);
    await database.execute("UPDATE kv SET a = 'p3' WHERE id = 3");
    await database.execute("DELETE FROM kv WHERE id IN (2, 5)");
    await database.insertBatch("kv", [{ id: 2, a: "again", b: 22 }]);
    await database.upsertBatch("kv", [{ id: 9, a: "u9", b: 99 }]);
    const query = "SELECT id, a, b FROM kv ORDER BY id";
    const expected = [
      { id: 0, a: "a0", b: 0 },
      { id: 1, a: "u1", b: 11 },
      { id: 2, a: "again", b: 22 },
      { id: 3, a: "p3", b: 30 },
      { id: 4, a: "u4", b: 40 },
      { id: 6, a: "a6", b: 6 },
      { id: 7, a: "a7", b: 7 },
      { id: 9, a: "u9", b: 99 },
    ];
    expect((await database.query(query, { memoize: false })).rows).toEqual(expected);
    const beforeOrder = await database.readTable("kv");
    const result = await database.compactTable("kv");
    expect(result.compacted).toBe(true);
    expect(await allVisibleSegments(database, "kv")).toHaveLength(1);
    expect((await database.query(query, { memoize: false })).rows).toEqual(expected);
    // A replaced row keeps its place: the fold changes the layout, never the scan order.
    expect(await database.readTable("kv")).toEqual(beforeOrder);
    await database.close();
  });

  it("resumes a replayed merge elsewhere by recomputing its replay", async () => {
    const store = new MemoryBlockStore();
    const planner = new MinnowDatabase(store, { autoCompact: false });
    await wideTable(planner);
    await planner.insertBatch("t", wideRows(2_000));
    await planner.upsertBatch("t", shuffled(wideRows(2_000, 1), 3));
    await planner.execute(`UPDATE "t" SET "s3" = 'patched' WHERE "n0" % 7 = 0`);
    const expected = await planner.readTable("t");
    const first = await planner.compactTableStep("t", { maxBlocks: 2 });
    if (first.jobId === null) throw new Error("Expected a running fold");
    expect(first.result).toBeNull();
    await planner.close();

    // A second engine has never seen the replay: it recomputes it from the sources, checks it
    // against the plan, and finishes the fold from the first engine's checkpoint.
    const resumer = new MinnowDatabase(store, { autoCompact: false });
    let progress = first;
    while (progress.result === null) {
      progress = await resumer.resumeCompactionJob(first.jobId, { maxBlocks: 4 });
    }
    expect(progress.result.compacted).toBe(true);
    expect(await resumer.readTable("t")).toEqual(expected);
    await resumer.close();
  });

  it("abandons a fold whose recomputed replay does not match its plan", async () => {
    const store = new MemoryBlockStore();
    const planner = new MinnowDatabase(store, { autoCompact: false });
    await wideTable(planner);
    await planner.insertBatch("t", wideRows(500));
    await planner.upsertBatch("t", shuffled(wideRows(500, 1), 5));
    const expected = await planner.readTable("t");
    const planned = await planner.compactTableStep("t", { maxBlocks: 1 });
    if (planned.jobId === null) throw new Error("Expected a running fold");
    const job = await store.getCompactionJob(planned.jobId);
    if (job?.rewritePlan.kind !== "merge-v2") throw new Error("Expected a replayed merge");
    await planner.cancelCompactionJob(job.id);
    await planner.close();
    // The same plan with a checksum no replay of these sources produces.
    await store.createCompactionJob({
      ...job,
      id: `${job.id}/tampered`,
      outputBlockIds: [],
      outputCursor: { outputIndex: 0, columnIndex: 0, rowStart: 0 },
      processedRows: 0,
      outputStoredBytes: 0,
      outputLogicalBytes: 0,
      peakWorkingBytes: 0,
      state: "planned",
      transactionId: null,
      revision: 0,
      rewritePlan: {
        ...job.rewritePlan,
        resolution: {
          ...job.rewritePlan.resolution,
          checksum: (job.rewritePlan.resolution.checksum + 1) >>> 0,
        },
      },
    });
    const resumer = new MinnowDatabase(store, { autoCompact: false });
    await expect(resumer.resumeCompactionJob(`${job.id}/tampered`)).rejects.toThrow(
      "Compaction replay differs from its plan",
    );
    expect(await store.getCompactionJob(`${job.id}/tampered`)).toMatchObject({
      state: "aborted",
    });
    // Nothing was written; the table folds normally afterwards.
    expect((await resumer.compactTable("t")).compacted).toBe(true);
    expect(await resumer.readTable("t")).toEqual(expected);
    await resumer.close();
  });
});

async function reference(rows: Row[]): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false });
  await wideTable(database);
  await database.insertBatch("t", rows);
  return database;
}
