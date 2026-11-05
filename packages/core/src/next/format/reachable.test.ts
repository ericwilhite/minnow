import { describe, expect, it } from "vitest";
import type {
  BlobRef,
  ColumnChunk,
  GroupRef,
  GroupRoot,
  IndexRoot,
  ManifestRoot,
  TableRoot,
} from "./manifest.js";
import {
  occupied,
  reachable,
  reachableExtents,
  type ManifestReader,
  type ReachableKind,
} from "./reachable.js";

let next = 0;
/** A fresh ref at the next 4 KiB slot (offsets are distinct, so each reference is traceable). */
function blob(length = 100, shift = 0): BlobRef {
  next += 4096;
  return { offset: next + shift, length, checksum: next };
}

function chunk(columnId: number, data = blob(20_000)): ColumnChunk {
  return {
    columnId,
    type: { kind: "int32", bits: 32 },
    data: { offset: data.offset, length: data.length },
    directory: { offset: data.offset + 16_000, length: 300, checksum: 1 },
    statistics: { nullCount: 0, min: null, max: null, dictionarySize: null },
    distinctSketch: { offset: data.offset + 16_200, length: 90, checksum: 2 },
  };
}

function index(indexId: number): IndexRoot {
  return {
    indexId,
    columnIds: [1],
    unique: false,
    runs: [0, 1].map((level) => ({
      level,
      entryCount: 10,
      entries: blob(),
      fences: blob(),
      bloom: blob(),
      minKey: new Uint8Array(1),
      maxKey: new Uint8Array(1),
    })),
  };
}

interface Fixture {
  readonly root: ManifestRoot;
  readonly reader: ManifestReader;
  /** Every reference the root reaches, with its kind. */
  readonly expected: Array<{ offset: number; length: number; kind: ReachableKind }>;
}

function fixture(): Fixture {
  const expected: Fixture["expected"] = [];
  const want = (ref: { offset: number; length: number }, kind: ReachableKind): void => {
    expected.push({ offset: ref.offset, length: ref.length, kind });
  };
  const directories = new Map<number, GroupRef[]>();
  const roots = new Map<number, GroupRoot>();
  const shared = blob(8_000); // a packed extent shared by two chunks of one group
  const makeGroups = (count: number, kind: ReachableKind): BlobRef => {
    const directory = blob();
    want(directory, kind);
    const groups: GroupRef[] = [];
    for (let k = 0; k < count; k++) {
      const rootRef = blob();
      const deletions = k % 2 === 0 ? blob() : null;
      const merged = k === 1;
      const rowIds = merged ? blob() : null;
      const columns =
        k === 0 ? [chunk(1, shared), chunk(2, shared), chunk(3)] : [chunk(1), chunk(2)];
      roots.set(rootRef.offset, {
        rowCount: 100,
        rowIds: rowIds === null ? { kind: "dense" } : { kind: "column", ref: rowIds },
        columns,
      });
      groups.push({
        groupId: k,
        firstRowId: k * 100 + 1,
        lastRowId: k * 100 + 100,
        rowCount: 100,
        liveRowCount: 90,
        root: rootRef,
        deletions,
      });
      want(rootRef, "group-root");
      if (deletions !== null) want(deletions, "deletions");
      if (rowIds !== null) want(rowIds, "row-ids");
      for (const column of columns) {
        want(column.data, "chunk-data");
        want(column.directory, "page-directory");
        if (column.distinctSketch !== null) want(column.distinctSketch, "distinct-sketch");
      }
    }
    directories.set(directory.offset, groups);
    return directory;
  };
  const tables: TableRoot[] = [1, 2].map((tableId) => {
    const keyIndex = index(1);
    const secondary = [index(2)];
    const statistics = tableId === 1 ? blob() : null;
    for (const run of [...keyIndex.runs, ...(secondary[0]?.runs ?? [])]) {
      want(run.entries, "index-entries");
      want(run.fences, "index-fences");
      want(run.bloom, "index-bloom");
    }
    if (statistics !== null) want(statistics, "table-statistics");
    return {
      tableId,
      schemaVersion: 1,
      nextRowId: 1000,
      flushedThroughLsn: 5,
      liveRowCount: 900,
      groups: makeGroups(tableId === 1 ? 70 : 3, "group-directory"),
      groupCount: tableId === 1 ? 70 : 3,
      staged:
        tableId === 2
          ? [
              {
                transaction: 9,
                firstRowId: 2000,
                rowCount: 200,
                groups: makeGroups(2, "staged-group-directory"),
              },
            ]
          : [],
      keyIndex,
      secondaryIndexes: secondary,
      statistics,
    };
  });
  const catalog = blob();
  want(catalog, "catalog");
  const root: ManifestRoot = {
    formatVersion: 1,
    sequence: 3,
    epoch: 1,
    log: { salt: 1, firstLsn: 1, rootSequence: 1 },
    globalLsn: 5,
    catalogVersion: 1,
    catalog,
    nextTableId: 3,
    sequences: [],
    tables,
    clients: [],
  };
  const reader: ManifestReader = {
    groups: async (ref) => directories.get(ref.offset) ?? [],
    groupRoot: async (ref) => {
      const found = roots.get(ref.offset);
      if (found === undefined) throw new Error("unknown group root");
      return found;
    },
  };
  return { root, reader, expected };
}

const key = (range: { offset: number; length: number; kind: string }): string =>
  `${range.kind}@${String(range.offset)}+${String(range.length)}`;

describe("reachability walk", () => {
  it("lists every range a root references, each with its kind", async () => {
    const { root, reader, expected } = fixture();
    const found = await reachable(root, reader, { concurrency: 8 });
    expect(found.map(key).sort()).toEqual(expected.map(key).sort());
    // Table ownership is recorded.
    expect(found.filter((range) => range.kind === "catalog").map((range) => range.tableId)).toEqual(
      [null],
    );
    expect(
      new Set(
        found
          .filter((range) => range.kind === "staged-group-directory")
          .map((range) => range.tableId),
      ),
    ).toEqual(new Set([2]));
  });

  it("merges shared and nested references into disjoint aligned ranges", async () => {
    const { root, reader } = fixture();
    const ranges = occupied(await reachable(root, reader));
    for (let k = 1; k < ranges.length; k++) {
      const previous = ranges[k - 1];
      const current = ranges[k];
      if (previous === undefined || current === undefined) continue;
      expect(current.offset).toBeGreaterThan(previous.offset + previous.length);
    }
    for (const range of ranges) {
      expect(range.offset % 4096).toBe(0);
      expect(range.length % 4096).toBe(0);
    }
    expect(
      occupied([
        { offset: 100, length: 10 },
        { offset: 4000, length: 200 },
        { offset: 8192, length: 1 },
        { offset: 0, length: 0 },
      ]),
    ).toEqual([{ offset: 0, length: 12_288 }]);
  });

  it("refuses to produce a partial walk when a metadata read fails", async () => {
    const { root, reader } = fixture();
    const failing: ManifestReader = {
      groups: (ref) => reader.groups(ref),
      groupRoot: async () => {
        throw new Error("Group root checksum mismatch");
      },
    };
    await expect(reachable(root, failing)).rejects.toThrow("checksum");
  });

  it("yields the extents the space opens with, through leased and released reads", async () => {
    const { root, reader, expected } = fixture();
    // Fake codecs: a blob's first 8 bytes name the offset it was read from.
    const groupsAt = new Map<number, readonly GroupRef[]>();
    const rootsAt = new Map<number, GroupRoot>();
    const read = async (offset: number): Promise<void> => {
      const groups = await reader.groups({ offset, length: 1, checksum: 0 });
      if (groups.length > 0) groupsAt.set(offset, groups);
    };
    for (const table of root.tables) {
      await read(table.groups.offset);
      for (const staged of table.staged) await read(staged.groups.offset);
    }
    for (const groups of groupsAt.values())
      for (const group of groups)
        rootsAt.set(group.root.offset, await reader.groupRoot(group.root));
    const offsetOf = (bytes: Uint8Array): number =>
      new DataView(bytes.buffer, bytes.byteOffset).getFloat64(0, true);
    let leased = 0;
    let budgetHeld = 0;
    const space = {
      read: async (
        refs: readonly BlobRef[],
        options: { budget: { allocate(n: number): boolean; free(n: number): void } },
      ) => {
        const total = refs.reduce((sum, ref) => sum + ref.length, 0);
        options.budget.allocate(total);
        leased++;
        return {
          bytes: refs.map((ref) => {
            const bytes = new Uint8Array(8);
            new DataView(bytes.buffer).setFloat64(0, ref.offset, true);
            return bytes;
          }),
          release: () => {
            leased--;
            options.budget.free(total);
          },
        };
      },
    };
    const budget = {
      allocate: (bytes: number): boolean => ((budgetHeld += bytes), true),
      free: (bytes: number): void => void (budgetHeld -= bytes),
    };
    const walk = reachableExtents({
      decodeRoot: () => root,
      decodeGroups: (bytes) => groupsAt.get(offsetOf(bytes)) ?? [],
      decodeGroupRoot: (bytes) => {
        const found = rootsAt.get(offsetOf(bytes));
        if (found === undefined) throw new Error("unknown group root");
        return found;
      },
    });
    const extents: Array<{ offset: number; length: number }> = [];
    for await (const extent of walk(new Uint8Array(0), space, budget)) extents.push(extent);
    expect(leased).toBe(0);
    expect(budgetHeld).toBe(0);
    const wanted = new Set(
      expected
        // A directory or sketch inside a chunk extent is covered by it; the shared 8 KB pack's
        // directories lie past it and stand on their own.
        .filter(
          (range) =>
            (range.kind !== "page-directory" && range.kind !== "distinct-sketch") ||
            !expected.some(
              (data) =>
                data.kind === "chunk-data" &&
                range.offset >= data.offset &&
                range.offset + range.length <= data.offset + data.length,
            ),
        )
        .map((range) => `${String(range.offset)}:${String(range.length)}`),
    );
    expect(
      new Set(extents.map((extent) => `${String(extent.offset)}:${String(extent.length)}`)),
    ).toEqual(wanted);
    expect(extents.length).toBe(wanted.size);
  });
});
