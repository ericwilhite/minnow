import { inspectBlock, MAX_BLOCK_ROW_COUNT } from "../block-format/index.js";
import { describe, expect, it } from "vitest";
import { MemoryBlockStore, type SegmentRecord } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { allSegmentRecords } from "./storage-test-helpers.js";
import {
  estimateCompactionRowsPerOutput,
  planAlignedWriteBlockRanges,
} from "./write-block-planner.js";

async function blockRowCounts(
  store: MemoryBlockStore,
  segment: SegmentRecord,
): Promise<Record<string, number[]>> {
  return Object.fromEntries(
    await Promise.all(
      Object.entries(segment.columnBlockIds).map(
        async ([columnId, ids]) =>
          [
            columnId,
            await Promise.all(
              ids.map(async (id) => {
                const bytes = await store.getBlock(id);
                if (bytes === undefined) throw new Error(`Missing test block: ${id}`);
                return inspectBlock(bytes).rowCount;
              }),
            ),
          ] as const,
      ),
    ),
  );
}

function expectAligned(counts: Record<string, number[]>): void {
  const [first, ...rest] = Object.values(counts);
  expect(first).toBeDefined();
  for (const candidate of rest) expect(candidate).toEqual(first);
}

describe("byte-bounded ordinary write blocks", () => {
  it("shortens every sibling column to the widest column's exact UTF-8 range", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      compression: "raw",
      rowsPerBlock: 100,
      targetBlockBytes: 64,
      autoCompact: false,
    });
    await database.execute("CREATE TABLE sized (id INTEGER PRIMARY KEY, a TEXT, b TEXT)");
    const rows = Array.from({ length: 5 }, (_, index) => ({
      id: index + 1,
      a: `${String(index)}${"😀".repeat(6)}`,
      b: `b${String(index)}`,
    }));

    const inserted = await database.insertBatch("sized", rows);
    expect(inserted.blockCount).toBe(9);
    expect(await database.readTable("sized")).toEqual(rows);

    const table = await store.getTableByName("sized");
    if (table === undefined) throw new Error("Missing sized table");
    const segment = (await allSegmentRecords(store, table.id)).find(
      (entry) => entry.kind === "insert",
    );
    if (segment === undefined) throw new Error("Missing insert segment");
    const counts = await blockRowCounts(store, segment);
    expectAligned(counts);
    expect(Object.values(counts)[0]).toEqual([2, 2, 1]);
  });

  it("allows one row over the performance target while preserving the hard format limit", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      compression: "raw",
      rowsPerBlock: 100,
      targetBlockBytes: 32,
      autoCompact: false,
    });
    await database.execute("CREATE TABLE wide_value (value TEXT)");
    const value = "x".repeat(1_024);
    const result = await database.insertBatch("wide_value", [{ value }]);

    expect(result.blockCount).toBe(1);
    expect(await database.readTable("wide_value")).toEqual([{ value }]);
  });

  it("uses aligned byte ranges for update and write-scope insert paths", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      compression: "raw",
      rowsPerBlock: 100,
      targetBlockBytes: 64,
      autoCompact: false,
    });
    await database.execute("CREATE TABLE mutations (id INTEGER PRIMARY KEY, a TEXT, b TEXT)");
    const initial = Array.from({ length: 5 }, (_, index) => ({
      id: index + 1,
      a: "a",
      b: "b",
    }));
    await database.insertBatch("mutations", initial);

    const changed = initial.map((_, index) => String(index).repeat(25));
    const updated = await database.updateBatch("mutations", {
      keys: initial.map(({ id }) => id),
      changes: { a: changed, b: changed },
    });
    expect(updated.blockCount).toBe(9);

    await database.write(async (session) => {
      await session.insertBatch("mutations", {
        columns: {
          id: [6, 7, 8, 9, 10],
          a: changed,
          b: changed,
        },
      });
    });

    const table = await store.getTableByName("mutations");
    if (table === undefined) throw new Error("Missing mutations table");
    const segments = await allSegmentRecords(store, table.id);
    for (const segment of segments.filter((entry) => entry.kind !== "delete")) {
      expectAligned(await blockRowCounts(store, segment));
    }
  });

  it("measures string rows linearly regardless of which column forces the boundary", () => {
    const rowCount = 10_000;
    const narrow = new Array<string>(rowCount).fill("");
    const wide = new Array<string>(rowCount).fill("x".repeat(3_000));
    const plan = (reverse: boolean) => {
      let measurements = 0;
      const columns = [
        { type: "string" as const, values: narrow },
        { type: "string" as const, values: wide },
      ];
      const ranges = planAlignedWriteBlockRanges(
        reverse ? [...columns].reverse() : columns,
        rowCount,
        rowCount,
        4_096,
        (value) => {
          measurements += 1;
          return value.length;
        },
      );
      return { ranges, measurements };
    };

    const forward = plan(false);
    const reverse = plan(true);
    expect(forward.ranges).toEqual(reverse.ranges);
    expect(forward.ranges).toHaveLength(rowCount);
    // Every accepted row plus the one overflow candidate at each boundary, for both columns.
    expect(forward.measurements).toBeLessThanOrEqual(rowCount * 4);
    expect(reverse.measurements).toBeLessThanOrEqual(rowCount * 4);
  });

  it("plans fixed-width ranges arithmetically without reading values", () => {
    const rowCount = 100_000;
    const values = new Proxy(
      { length: rowCount },
      {
        get(target, property, receiver) {
          if (typeof property === "string" && /^\d+$/.test(property)) {
            throw new Error("Fixed-width planning read a value");
          }
          return Reflect.get(target, property, receiver) as unknown;
        },
      },
    ) as unknown as readonly number[];
    const ranges = planAlignedWriteBlockRanges(
      [{ type: "number", values }],
      rowCount,
      MAX_BLOCK_ROW_COUNT,
      64,
    );

    expect(ranges[0]).toEqual({ start: 0, end: 7 });
    expect(ranges.at(-1)?.end).toBe(rowCount);
    expect(ranges).toHaveLength(Math.ceil(rowCount / 7));
  });

  it("rejects sparse delete and update inputs before staging any mutation", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { compression: "raw", autoCompact: false });
    await database.execute("CREATE TABLE sparse_mutations (id INTEGER PRIMARY KEY, label TEXT)");
    await database.insertBatch("sparse_mutations", [
      { id: 1, label: "one" },
      { id: 2, label: "two" },
    ]);

    const directDelete = [1];
    directDelete.length = 2;
    await expect(database.deleteBatch("sparse_mutations", { keys: directDelete })).rejects.toThrow(
      /id\[1\].*(?:number|integer)/,
    );

    const scopedDelete = [1];
    scopedDelete.length = 2;
    await expect(
      database.write((session) => session.deleteBatch("sparse_mutations", { keys: scopedDelete })),
    ).rejects.toThrow(/id\[1\].*(?:number|integer)/);

    const sparseKeys = [1];
    sparseKeys.length = 2;
    await expect(
      database.updateBatch("sparse_mutations", {
        keys: sparseKeys,
        changes: { label: ["changed", "changed"] },
      }),
    ).rejects.toThrow(/id\[1\].*(?:number|integer)/);

    const sparseChanges = ["changed"];
    sparseChanges.length = 2;
    await expect(
      database.updateBatch("sparse_mutations", {
        keys: [1, 2],
        changes: { label: sparseChanges },
      }),
    ).rejects.toThrow(/label\[1\].*string/);

    expect(await database.readTable("sparse_mutations")).toEqual([
      { id: 1, label: "one" },
      { id: 2, label: "two" },
    ]);
  });

  it("byte-bounds standalone and write-scope delete key blocks", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      compression: "raw",
      rowsPerBlock: 100,
      targetBlockBytes: 64,
      autoCompact: false,
    });
    await database.execute("CREATE TABLE removals (id INTEGER PRIMARY KEY)");
    await database.insertBatch(
      "removals",
      Array.from({ length: 20 }, (_, index) => ({ id: index + 1 })),
    );

    const direct = await database.deleteBatch("removals", {
      keys: Array.from({ length: 10 }, (_, index) => index + 1),
    });
    expect(direct.blockCount).toBe(2);
    await database.write(async (session) => {
      await session.deleteBatch("removals", {
        keys: Array.from({ length: 10 }, (_, index) => index + 11),
      });
    });

    const table = await store.getTableByName("removals");
    if (table === undefined) throw new Error("Missing removals table");
    const deletes = (await allSegmentRecords(store, table.id)).filter(
      (entry) => entry.kind === "delete",
    );
    expect(deletes).toHaveLength(2);
    for (const segment of deletes) {
      expect(Object.values(await blockRowCounts(store, segment))[0]).toEqual([7, 3]);
    }
  });

  it("validates the target byte option", () => {
    expect(() => new MinnowDatabase(new MemoryBlockStore(), { targetBlockBytes: 0 })).toThrow(
      /Target block bytes/,
    );
    expect(
      () => new MinnowDatabase(new MemoryBlockStore(), { targetBlockBytes: 64 * 1024 * 1024 + 1 }),
    ).toThrow(/Target block bytes/);
    expect(
      () => new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: MAX_BLOCK_ROW_COUNT }),
    ).not.toThrow();
    expect(
      () => new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: MAX_BLOCK_ROW_COUNT + 1 }),
    ).toThrow(/Rows per block/);
  });

  it("caps ordinary and compaction planners at the format row ceiling", () => {
    expect(() =>
      planAlignedWriteBlockRanges(
        [{ type: "boolean", values: [true] }],
        1,
        MAX_BLOCK_ROW_COUNT + 1,
        64,
      ),
    ).toThrow(/format limit/);
    expect(estimateCompactionRowsPerOutput(64 * 1024 * 1024, 1)).toBe(MAX_BLOCK_ROW_COUNT);
  });

  it("uses a Date's internal value consistently for validation, keys, and persisted bytes", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { compression: "raw", autoCompact: false });
    await database.execute("CREATE TABLE dated (at TIMESTAMP PRIMARY KEY, label TEXT)");
    const shadowed = new Date(1234);
    Object.defineProperty(shadowed, "getTime", {
      value: () => {
        throw new Error("caller-controlled getTime must not run");
      },
    });

    await database.insert("dated", { at: shadowed, label: "first" });
    expect(await database.readTable("dated")).toEqual([{ at: new Date(1234), label: "first" }]);
    await expect(
      database.insert("dated", { at: new Date(1234), label: "duplicate" }),
    ).rejects.toThrow(/Duplicate value/);
  });

  it("keeps Date persistence and keys consistent after a prototype override", async () => {
    const original = Object.getOwnPropertyDescriptor(Date.prototype, "getTime");
    if (original === undefined) throw new Error("Missing Date.prototype.getTime");
    try {
      Object.defineProperty(Date.prototype, "getTime", {
        ...original,
        value: () => 0,
      });

      const store = new MemoryBlockStore();
      const database = new MinnowDatabase(store, { compression: "raw", autoCompact: false });
      await database.execute("CREATE TABLE prototype_dated (at TIMESTAMP PRIMARY KEY, label TEXT)");
      await database.insert("prototype_dated", { at: new Date(1234), label: "first" });

      expect(await database.readTable("prototype_dated")).toEqual([
        { at: new Date(1234), label: "first" },
      ]);
      await expect(
        database.insert("prototype_dated", { at: new Date(1234), label: "duplicate" }),
      ).rejects.toThrow(/Duplicate value/);
    } finally {
      Object.defineProperty(Date.prototype, "getTime", original);
    }
  });
});
