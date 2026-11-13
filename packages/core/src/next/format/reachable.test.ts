import { describe, expect, it } from "vitest";
import type { BlobRef, ExtentRef } from "../storage/extents.js";
import type {
  ColumnChunk,
  GroupRef,
  GroupRoot,
  IndexRoot,
  ManifestRoot,
  RunRef,
  TableRoot,
} from "./manifest.js";
import {
  reachable,
  reachableExtents,
  walkReachable,
  type Held,
  type ManifestReader,
  type ReachableKind,
} from "./reachable.js";
import type { GroupPageEntry, Parsed } from "./manifest-codec.js";

let next = 0;
/** A fresh extent at the next 8 KiB slot, so each reference is traceable. */
function extent(length = 4096): ExtentRef {
  next += 1 << 16;
  return { offset: next, length };
}
/** A blob alone in its own extent (metadata), or at `offset` inside `within`. */
function blob(within: ExtentRef = extent(100), offset = 0, length = within.length): BlobRef {
  return { extent: within, offset, length, checksum: within.offset + offset };
}

/** A chunk whose data range starts at `at` inside `data` (a nonzero `at`: a packed extent). */
function chunk(columnId: number, data: ExtentRef = extent(20_000), at = 0): ColumnChunk {
  return {
    columnId,
    type: { kind: "int32", bits: 32 },
    data: { extent: data, offset: at, length: 20_000 },
    directory: blob(data, at + 16_000, 300),
    statistics: { nullCount: 0, min: null, max: null, dictionarySize: null },
    distinctSketch: blob(data, at + 16_300, 90),
  };
}

function run(level: number): RunRef {
  return {
    level,
    entryCount: 10,
    entries: blob(),
    fences: blob(),
    bloom: blob(),
    minKey: new Uint8Array(1),
    maxKey: new Uint8Array(1),
  };
}

function index(indexId: number): IndexRoot {
  return { indexId, columnIds: [1], unique: false, runs: [run(0), run(1)] };
}

interface Fixture {
  readonly root: ManifestRoot;
  readonly reader: ManifestReader;
  /** Every reference the root reaches, with its kind. */
  readonly expected: Array<{ ref: BlobRef | ExtentRef; kind: ReachableKind }>;
  /** Decoded values handed out and not yet released. */
  readonly outstanding: () => number;
}

function fixture(): Fixture {
  const expected: Fixture["expected"] = [];
  const want = (ref: BlobRef | ExtentRef, kind: ReachableKind): void => {
    expected.push({ ref, kind });
  };
  const indexes = new Map<BlobRef, GroupPageEntry[]>();
  const pages = new Map<BlobRef, GroupRef[]>();
  const roots = new Map<BlobRef, GroupRoot>();
  const wantRuns = (runs: readonly RunRef[]): void => {
    for (const each of runs) {
      want(each.entries, "index-entries");
      want(each.fences, "index-fences");
      want(each.bloom, "index-bloom");
    }
  };
  const makeGroups = (count: number, kind: ReachableKind, base = 0): BlobRef => {
    const directory = blob();
    want(directory, kind);
    const groups: GroupRef[] = [];
    for (let k = 0; k < count; k++) {
      const rootRef = blob();
      const deletions = k % 2 === 0 ? blob() : null;
      const rowIds = k === 1 ? blob() : null;
      // Group 0 packs two small chunks into one shared extent.
      const pack = extent(40_000);
      const columns =
        k === 0 ? [chunk(1, pack, 0), chunk(2, pack, 20_000), chunk(3)] : [chunk(1), chunk(2)];
      roots.set(rootRef, {
        rowCount: 100,
        rowIds: rowIds === null ? { kind: "dense" } : { kind: "column", ref: rowIds },
        columns,
      });
      groups.push({
        groupId: k,
        firstRowId: base + k * 100 + 1,
        lastRowId: base + k * 100 + 100,
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
    // Pages of 32 groups, so a directory has several.
    const entries: GroupPageEntry[] = [];
    for (let start = 0; start < groups.length; start += 32) {
      const page = groups.slice(start, start + 32);
      const ref = blob();
      want(ref, "group-directory-page");
      pages.set(ref, page);
      entries.push({
        ref,
        firstRowId: page[0]?.firstRowId ?? 1,
        lastRowId: page[page.length - 1]?.lastRowId ?? 1,
        groupCount: page.length,
        rowCount: page.length * 100,
        liveRowCount: page.length * 90,
      });
    }
    indexes.set(directory, entries);
    return directory;
  };
  const tables: TableRoot[] = [1, 2].map((tableId) => {
    const keyIndex = index(1);
    const secondary = [index(2)];
    const statistics = tableId === 1 ? blob() : null;
    wantRuns([...keyIndex.runs, ...(secondary[0]?.runs ?? [])]);
    if (statistics !== null) want(statistics, "table-statistics");
    const stagedRuns =
      tableId === 2
        ? [
            { indexId: 1, runs: [run(0)] },
            { indexId: 2, runs: [run(0), run(1)] },
          ]
        : [];
    for (const staged of stagedRuns) wantRuns(staged.runs);
    const staged =
      tableId === 2
        ? [
            {
              transaction: 9,
              firstRowId: 301,
              rowCount: 200,
              groups: makeGroups(2, "staged-group-directory", 300),
              indexRuns: stagedRuns,
            },
          ]
        : [];
    return {
      tableId,
      schemaVersion: 1,
      // Totals agree with the directories (the walk cross-checks them).
      nextRowId: tableId === 1 ? 7001 : 501,
      flushedThroughLsn: 5,
      liveRowCount: (tableId === 1 ? 70 : 3) * 90,
      groups: makeGroups(tableId === 1 ? 70 : 3, "group-directory"),
      groupCount: tableId === 1 ? 70 : 3,
      staged,
      keyIndex,
      secondaryIndexes: secondary,
      statistics,
    };
  });
  const catalog = blob();
  want(catalog, "catalog");
  // Only the fields the walk reads are meaningful here.
  const root = {
    formatVersion: 3,
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
    extentList: [],
  } as unknown as ManifestRoot;
  let outstanding = 0;
  const held = <T>(value: T): Held<T> => {
    outstanding++;
    let released = false;
    return {
      value,
      release: () => {
        if (!released) outstanding--;
        released = true;
      },
    };
  };
  const reader: ManifestReader = {
    groupIndex: async (ref) => held(indexes.get(ref) ?? []),
    groupPage: async (ref) => held(pages.get(ref) ?? []),
    groupRoot: async (ref) => {
      const found = roots.get(ref);
      if (found === undefined) throw new Error("unknown group root");
      return held(found);
    },
  };
  return { root, reader, expected, outstanding: () => outstanding };
}

const key = (ref: BlobRef | ExtentRef, kind: string): string =>
  "extent" in ref
    ? `${kind}@${String(ref.extent.offset)}+${String(ref.offset)}:${String(ref.length)}`
    : `${kind}@${String(ref.offset)}:${String(ref.length)}`;

const extentKey = (ref: BlobRef | ExtentRef): string => {
  const owner = "extent" in ref ? ref.extent : ref;
  return `${String(owner.offset)}:${String(owner.length)}`;
};

describe("reachability walk", () => {
  it("lists every reference a root reaches, staged index runs included", async () => {
    const { root, reader, expected, outstanding } = fixture();
    const found = await reachable(root, reader, { concurrency: 8 });
    expect(outstanding()).toBe(0); // every decoded index, page and root was released
    expect(found.map(({ ref, kind }) => key(ref, kind)).sort()).toEqual(
      expected.map(({ ref, kind }) => key(ref, kind)).sort(),
    );
    // The review's probe (B2-1 #1): a staged transaction's index runs are reached.
    const staged = root.tables[1]?.staged[0]?.indexRuns.flatMap((each) => each.runs) ?? [];
    expect(staged.length).toBe(3);
    for (const each of staged)
      for (const ref of [each.entries, each.fences, each.bloom])
        expect(found.some((reached) => reached.ref === ref)).toBe(true);
    expect(found.filter((r) => r.kind === "catalog").map((r) => r.tableId)).toEqual([null]);
    // A packed chunk's data range starts inside its extent.
    expect(
      found.some((r) => r.kind === "chunk-data" && "extent" in r.ref && r.ref.offset === 20_000),
    ).toBe(true);
  });

  it("streams: it yields before it has read every group root", async () => {
    const { root, reader } = fixture();
    let rootsRead = 0;
    const counting: ManifestReader = {
      groupIndex: (ref) => reader.groupIndex(ref),
      groupPage: (ref) => reader.groupPage(ref),
      groupRoot: async (ref) => {
        rootsRead++;
        return reader.groupRoot(ref);
      },
    };
    for await (const reached of walkReachable(root, counting, { concurrency: 4 }))
      if (reached.kind === "chunk-data") break;
    expect(rootsRead).toBe(4); // one batch of the 75 group roots
  });

  it("refuses to produce a partial walk when a metadata read fails", async () => {
    const { root, reader, outstanding } = fixture();
    let calls = 0;
    const failing: ManifestReader = {
      groupIndex: (ref) => reader.groupIndex(ref),
      groupPage: (ref) => reader.groupPage(ref),
      // The fifth group root of a batch fails; the others of its batch were decoded.
      groupRoot: async (ref) => {
        if (++calls === 5) throw new Error("Group root checksum mismatch");
        return reader.groupRoot(ref);
      },
    };
    await expect(reachable(root, failing)).rejects.toThrow("checksum");
    expect(outstanding()).toBe(0); // nothing decoded stays held after the failure
  });

  it("yields every extent for the space, leasing metadata blobs only to decode them", async () => {
    const { root, reader, expected } = fixture();
    // Fake codecs: a blob's bytes hold its checksum, which names the ref.
    const byChecksum = new Map<number, BlobRef>();
    for (const { ref } of expected) if ("extent" in ref) byChecksum.set(ref.checksum, ref);
    const refOf = (bytes: Uint8Array): BlobRef => {
      const ref = byChecksum.get(new DataView(bytes.buffer, bytes.byteOffset).getFloat64(0, true));
      if (ref === undefined) throw new Error("unknown blob");
      return ref;
    };
    let leased = 0;
    let held = 0;
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
            new DataView(bytes.buffer).setFloat64(0, ref.checksum, true);
            return bytes;
          }),
          release: () => {
            leased--;
            options.budget.free(total);
          },
        };
      },
      peek: () => null,
    };
    const budget = {
      allocate: (bytes: number): boolean => ((held += bytes), true),
      free: (bytes: number): void => void (held -= bytes),
    };
    // The codecs are synchronous; resolve the fixture's maps up front.
    const parsed = <T>(value: T): Parsed<T> => {
      budget.allocate(10);
      return { value, charge: 10 };
    };
    const unwrap = async <T>(promise: Promise<Held<T>>): Promise<T> => {
      const holder = await promise;
      holder.release();
      return holder.value;
    };
    const indexOf = new Map<BlobRef, readonly GroupPageEntry[]>();
    const pageOf = new Map<BlobRef, readonly GroupRef[]>();
    const rootOf = new Map<BlobRef, GroupRoot>();
    for (const table of root.tables)
      for (const ref of [table.groups, ...table.staged.map((staged) => staged.groups)]) {
        const entries = await unwrap(reader.groupIndex(ref));
        indexOf.set(ref, entries);
        for (const entry of entries) {
          const groups = await unwrap(reader.groupPage(entry.ref));
          pageOf.set(entry.ref, groups);
          for (const group of groups)
            rootOf.set(group.root, await unwrap(reader.groupRoot(group.root)));
        }
      }
    const walker = reachableExtents({
      decodeRoot: () => parsed(root),
      decodeGroupIndex: (bytes) => parsed(indexOf.get(refOf(bytes)) ?? []),
      decodeGroupPage: (bytes) => parsed(pageOf.get(refOf(bytes)) ?? []),
      decodeGroupRoot: (bytes) => {
        const found = rootOf.get(refOf(bytes));
        if (found === undefined) throw new Error("unknown group root");
        return parsed(found);
      },
    });
    const out: Array<BlobRef | ExtentRef> = [];
    for await (const ref of walker(new Uint8Array(0), space, budget)) out.push(ref);
    expect(leased).toBe(0);
    expect(held).toBe(0);
    // The space gets BlobRefs or bare extents, never a checksum-less chunk data range.
    for (const ref of out) expect("extent" in ref ? "checksum" in ref : true).toBe(true);
    // Every extent the root reaches is covered (the space takes BlobRef.extent) ...
    expect(new Set(out.map(extentKey))).toEqual(new Set(expected.map(({ ref }) => extentKey(ref))));
    // ... and a chunk's directory and sketch, in its data extent, are not repeated.
    expect(out.length).toBeLessThan(expected.length);
  });
});
