import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { DATA_ALIGN } from "../storage/contract.js";
import type { BlobRef, ByteBudget, ExtentRef } from "../storage/extents.js";
import type { ColumnStatistics, PhysicalType, StatValue } from "../types.js";
import { ByteWriter, ManifestCorruptionError } from "./bytes.js";
import type { ClientLedgerEntry } from "./ledger.js";
import {
  decodeGroupIndex,
  decodeGroupPage,
  decodeGroupRoot,
  decodeManifestRoot,
  decodeTableStatistics,
  encodeGroupIndex,
  encodeGroupPage,
  encodeGroupRoot,
  encodeManifestRoot,
  encodeTableStatistics,
  GROUPS_PER_PAGE,
  MAX_ROOT_BYTES,
  ParseBudgetError,
  parseCharge,
  type GroupPageEntry,
} from "./manifest-codec.js";
import type {
  ChunkData,
  ColumnChunk,
  GroupRef,
  GroupRoot,
  IndexRoot,
  ManifestRoot,
  RunRef,
  TableRoot,
  TableStatistics,
} from "./manifest.js";

const SEEDS = seedsFor("next-manifest-codecs", [0x3a9e, 0x77]);
const RUNS = 60;

function tracked(): ByteBudget & { held: number } {
  return {
    held: 0,
    allocate(bytes: number): boolean {
      this.held += bytes;
      return true;
    },
    free(bytes: number): void {
      this.held -= bytes;
    },
  };
}

// ── Arbitraries ────────────────────────────────────────────────────────────────────────────────

const extent: fc.Arbitrary<ExtentRef> = fc
  .tuple(fc.integer({ min: 0, max: 2 ** 32 }), fc.integer({ min: 1, max: 2 ** 31 }))
  .map(([slot, length]) => ({ offset: slot * DATA_ALIGN, length }));

/** A pool of extents per value, so references share them (the extent table). */
const extents = fc.array(extent, { minLength: 1, maxLength: 6 });

/** A root's extent list (ruling R25): pages in strictly increasing first-offset order. */
function extentListIn(pool: readonly ExtentRef[]): fc.Arbitrary<ManifestRoot["extentList"]> {
  return fc.uniqueArray(fc.nat({ max: 2 ** 40 }), { maxLength: 6 }).chain((firsts) =>
    fc.tuple(
      ...[...firsts]
        .sort((a, b) => a - b)
        .map((first) =>
          fc.record({
            blob: blobIn(pool),
            first: fc.constant(first),
            count: fc.integer({ min: 1, max: 4096 }),
          }),
        ),
    ),
  );
}

function blobIn(pool: readonly ExtentRef[]): fc.Arbitrary<BlobRef> {
  return fc
    .tuple(
      fc.nat(),
      fc.double({ min: 0, max: 1, noNaN: true }),
      fc.double({ min: 0, max: 1, noNaN: true }),
      fc.nat({ max: 0xffff_ffff }),
    )
    .map(([pick, start, size, checksum]) => {
      const owner = pool[pick % pool.length] ?? { offset: 0, length: 1 };
      const offset = Math.floor(start * owner.length);
      const length = Math.floor(size * (owner.length - offset));
      return { extent: owner, offset, length, checksum };
    });
}

function dataIn(pool: readonly ExtentRef[]): fc.Arbitrary<ChunkData> {
  return blobIn(pool).map(({ extent: owner, offset, length }) => ({
    extent: owner,
    offset,
    length,
  }));
}

const physicalType: fc.Arbitrary<PhysicalType> = fc.letrec<{ type: PhysicalType }>((tie) => ({
  type: fc.oneof(
    { depthSize: "small", withCrossShrink: true },
    fc.constantFrom<PhysicalType>(
      { kind: "bool" },
      { kind: "int32", bits: 16 },
      { kind: "int32", bits: 32 },
      { kind: "int64" },
      { kind: "float64" },
      { kind: "float32" },
      { kind: "date" },
      { kind: "time" },
      { kind: "timestamp", zoned: true },
      { kind: "timestamp", zoned: false },
      { kind: "interval" },
      { kind: "text" },
      { kind: "bytes" },
      { kind: "json", binary: true },
      { kind: "json", binary: false },
      { kind: "uuid" },
    ),
    fc
      .record({
        precision: fc.option(fc.integer({ min: 1, max: 1000 }), { nil: null }),
        scale: fc.option(fc.integer({ min: -1000, max: 16_383 }), { nil: null }),
      })
      .map(({ precision, scale }) => ({ kind: "decimal" as const, precision, scale })),
    fc
      .record({
        name: fc.string(),
        labels: fc.array(fc.string({ unit: "grapheme" }), { maxLength: 20 }),
      })
      .map(({ name, labels }) => ({ kind: "enum" as const, name, labels })),
    tie("type").map((element) => ({ kind: "array" as const, element })),
  ),
})).type;

const statValue: fc.Arbitrary<StatValue | null> = fc.oneof(
  fc.constant(null),
  fc.boolean(),
  fc.double(),
  fc.bigInt({ min: -(2n ** 200n), max: 2n ** 200n }),
  fc.uint8Array({ maxLength: 40 }),
);

const statistics: fc.Arbitrary<ColumnStatistics> = fc.record({
  nullCount: fc.nat({ max: 65_536 }),
  min: statValue,
  max: statValue,
  dictionarySize: fc.option(fc.nat({ max: 65_536 }), { nil: null }),
});

const nonNullStat: fc.Arbitrary<StatValue> = fc.oneof(
  fc.boolean(),
  fc.double(),
  fc.bigInt({ min: -(2n ** 200n), max: 2n ** 200n }),
  fc.uint8Array({ maxLength: 40 }),
);

/** Table statistics: ascending distinct column ids; bounds exactly when bounded with values. */
const tableStatistics: fc.Arbitrary<TableStatistics> = fc
  .record({
    groupCount: fc.nat({ max: 2 ** 40 }),
    columns: fc.uniqueArray(
      fc.record({
        columnId: fc.nat({ max: 5000 }),
        nullCount: fc.nat({ max: 2 ** 45 }),
        values: fc.nat({ max: 2 ** 45 }),
        bytes: fc.nat({ max: 2 ** 50 }),
        unbounded: fc.boolean(),
        min: nonNullStat,
        max: nonNullStat,
      }),
      { selector: (column) => column.columnId, maxLength: 30 },
    ),
  })
  .map(({ groupCount, columns }) => ({
    groupCount,
    columns: [...columns]
      .sort((a, b) => a.columnId - b.columnId)
      .map((column) =>
        column.unbounded || column.values === 0 ? { ...column, min: null, max: null } : column,
      ),
  }));

const groupRoot: fc.Arbitrary<GroupRoot> = extents
  .chain((pool) =>
    fc.record({
      rowCount: fc
        .integer({ min: 65_536, max: 65_536 })
        .chain(() => fc.integer({ min: 1, max: 65_536 })),
      rowIds: fc.oneof(
        fc.constant({ kind: "dense" as const }),
        blobIn(pool).map((ref) => ({ kind: "column" as const, ref })),
      ),
      columns: fc
        .uniqueArray(fc.nat({ max: 5000 }), { maxLength: 40 })
        .chain((ids) =>
          fc.tuple(
            ...ids.map((columnId) =>
              fc.record({
                columnId: fc.constant(columnId),
                type: physicalType,
                data: dataIn(pool),
                directory: blobIn(pool),
                statistics,
                distinctSketch: fc.option(blobIn(pool), { nil: null }),
              }),
            ),
          ),
        )
        .map((columns): ColumnChunk[] => columns),
    }),
  )
  .map((root) => ({
    ...root,
    columns: root.columns.map((chunk) => ({
      ...chunk,
      statistics: {
        ...chunk.statistics,
        nullCount: Math.min(chunk.statistics.nullCount, root.rowCount),
      },
    })),
  }));

/** Groups in row-id order with gaps, each a valid shape. */
function groupsOf(pool: readonly ExtentRef[], maxLength: number): fc.Arbitrary<GroupRef[]> {
  return fc
    .array(
      fc.record({
        gap: fc.nat({ max: 1000 }),
        rows: fc.integer({ min: 1, max: 65_536 }),
        holes: fc.nat({ max: 5000 }),
        deleted: fc.nat({ max: 65_536 }),
        groupId: fc.nat({ max: 2 ** 40 }),
        root: blobIn(pool),
        deletions: fc.option(blobIn(pool), { nil: null }),
      }),
      { minLength: 1, maxLength },
    )
    .map((specs) => {
      let next = 1;
      return specs.map(({ gap, rows, holes, deleted, groupId, root, deletions }) => {
        const firstRowId = next + gap;
        const lastRowId = firstRowId + rows - 1 + holes;
        next = lastRowId + 1;
        return {
          groupId,
          firstRowId,
          lastRowId,
          rowCount: rows,
          liveRowCount: rows - Math.min(deleted, rows),
          root,
          deletions,
        };
      });
    });
}

function run(pool: readonly ExtentRef[]): fc.Arbitrary<RunRef> {
  return (
    fc
      .record({
        level: fc.nat({ max: 12 }),
        entryCount: fc.nat({ max: 2 ** 40 }),
        entries: blobIn(pool),
        fences: blobIn(pool),
        bloom: blobIn(pool),
        minKey: fc.uint8Array({ maxLength: 64 }),
        maxKey: fc.uint8Array({ maxLength: 64 }),
        continues: fc.boolean(),
      })
      // `continues` is present only when true (one encoding per value).
      .map(({ continues, ...rest }): RunRef => (continues ? { ...rest, continues: true } : rest))
  );
}

function indexRoot(pool: readonly ExtentRef[]): fc.Arbitrary<IndexRoot> {
  return fc.record({
    indexId: fc.nat({ max: 1000 }),
    columnIds: fc.array(fc.nat({ max: 1600 }), { minLength: 1, maxLength: 4 }),
    unique: fc.boolean(),
    runs: fc.array(run(pool), { maxLength: 6 }),
  });
}

const lsn = fc.nat({ max: Number.MAX_SAFE_INTEGER });

function tableRoot(pool: readonly ExtentRef[], tableId: number): fc.Arbitrary<TableRoot> {
  return fc.record({
    tableId: fc.constant(tableId),
    schemaVersion: fc.nat({ max: 1000 }),
    nextRowId: lsn,
    flushedThroughLsn: lsn,
    liveRowCount: lsn,
    groups: blobIn(pool),
    groupCount: fc.nat({ max: 2 ** 32 }),
    staged: fc.array(
      fc.record({
        transaction: lsn,
        firstRowId: lsn,
        rowCount: fc.nat(),
        groups: blobIn(pool),
        indexRuns: fc.array(
          fc.record({ indexId: fc.nat({ max: 99 }), runs: fc.array(run(pool), { maxLength: 3 }) }),
          {
            maxLength: 3,
          },
        ),
      }),
      { maxLength: 3 },
    ),
    keyIndex: fc.option(indexRoot(pool), { nil: null }),
    secondaryIndexes: fc.array(indexRoot(pool), { maxLength: 3 }),
    statistics: fc.option(blobIn(pool), { nil: null }),
  });
}

/**
 * Strings a decoder must keep byte for byte (review B2-p2a #3, #4): a leading byte-order mark,
 * which a default TextDecoder strips, and keys that name Object.prototype members.
 */
const BOM = "\ufeff";
const ledgerReach = createReach("next-manifest-codecs ledger strings", [
  "client id with a leading BOM",
  "count key __proto__",
  "count key with a leading BOM",
]);
const clientId = fc.oneof(
  fc.string({ unit: "grapheme", maxLength: 40 }),
  fc.string({ unit: "grapheme", maxLength: 40 }).map((text) => BOM + text),
);
const countKey = fc.oneof(
  { weight: 3, arbitrary: fc.string({ maxLength: 12 }) },
  fc.constantFrom("__proto__", "constructor", "toString", BOM, `${BOM}inserted`),
);

const client: fc.Arbitrary<ClientLedgerEntry> = fc.record({
  clientId,
  resolvedThrough: lsn,
  outcomes: fc.array(
    fc.record({
      sequence: lsn,
      lsn,
      counts: fc.dictionary(countKey, fc.double(), { maxKeys: 4 }),
      recordedAtMs: fc.double(),
    }),
    { maxLength: 5 },
  ),
});

/** Marks the ledger strings a generated root holds. */
function reachLedger(root: ManifestRoot): void {
  ledgerReach.beginHistory();
  for (const entry of root.clients) {
    if (entry.clientId.startsWith(BOM)) ledgerReach.reach("client id with a leading BOM");
    for (const outcome of entry.outcomes)
      for (const key of Object.keys(outcome.counts)) {
        if (key === "__proto__") ledgerReach.reach("count key __proto__");
        if (key.startsWith(BOM)) ledgerReach.reach("count key with a leading BOM");
      }
  }
}

afterAll(() => {
  ledgerReach.assertReached();
});

const manifestRoot: fc.Arbitrary<ManifestRoot> = extents.chain((pool) =>
  fc.record({
    formatVersion: fc.constant(2),
    sequence: lsn,
    epoch: lsn,
    log: fc.record({ salt: fc.nat({ max: 0xffff_ffff }), firstLsn: lsn, rootSequence: lsn }),
    globalLsn: lsn,
    catalogVersion: lsn,
    catalog: blobIn(pool),
    nextTableId: lsn,
    sequences: fc.array(
      fc.record({
        sequenceId: fc.nat(),
        reservedThrough: fc.bigInt({ min: -(2n ** 64n), max: 2n ** 64n }),
      }),
      { maxLength: 5 },
    ),
    tables: fc
      .uniqueArray(fc.nat({ max: 10_000 }), { maxLength: 6 })
      .chain((ids) => fc.tuple(...ids.map((id) => tableRoot(pool, id))))
      .map((tables): TableRoot[] => tables),
    clients: fc.array(client, { maxLength: 4 }),
    extentList: extentListIn(pool),
  }),
);

/** Decoded values compare structurally; extents decode to shared objects of equal value. */
function roundTrip<T>(
  value: T,
  encode: (value: T) => Uint8Array,
  decode: (bytes: Uint8Array, budget: ByteBudget) => { value: T; charge: number },
): void {
  const bytes = encode(value);
  const budget = tracked();
  const parsed = decode(bytes, budget);
  expect(parsed.value).toEqual(value);
  expect(parsed.charge).toBe(parseCharge(bytes.length));
  expect(budget.held).toBe(parsed.charge);
  // Re-encoding the decoded value gives the same bytes (the encoding is canonical).
  expect(encode(parsed.value)).toEqual(bytes);
}

describe.each(SEEDS)("manifest codecs round-trip (seed %i)", (seed) => {
  it("group roots", () => {
    fc.assert(
      fc.property(groupRoot, (root) => roundTrip(root, encodeGroupRoot, decodeGroupRoot)),
      {
        seed,
        numRuns: RUNS,
      },
    );
  });

  it("group directory pages and the index", () => {
    fc.assert(
      fc.property(
        extents.chain((pool) => groupsOf(pool, 300)),
        (groups) => roundTrip(groups, encodeGroupPage, decodeGroupPage),
      ),
      { seed, numRuns: RUNS },
    );
    fc.assert(
      fc.property(
        extents.chain((pool) =>
          fc.tuple(groupsOf(pool, 200), fc.array(blobIn(pool), { minLength: 200, maxLength: 200 })),
        ),
        ([groups, refs]) => {
          // One page entry per run of up to 7 groups.
          const pages: GroupPageEntry[] = [];
          for (let start = 0; start < groups.length; start += 7) {
            const slice = groups.slice(start, start + 7);
            const pageRef = refs[pages.length];
            if (pageRef === undefined) throw new Error("more pages than refs");
            pages.push({
              ref: pageRef,
              firstRowId: slice[0]?.firstRowId ?? 1,
              lastRowId: slice[slice.length - 1]?.lastRowId ?? 1,
              groupCount: slice.length,
              rowCount: slice.reduce((sum, group) => sum + group.rowCount, 0),
              liveRowCount: slice.reduce((sum, group) => sum + group.liveRowCount, 0),
            });
          }
          roundTrip(pages, encodeGroupIndex, decodeGroupIndex);
        },
      ),
      { seed, numRuns: RUNS },
    );
  });

  it("table statistics", () => {
    fc.assert(
      fc.property(tableStatistics, (value) =>
        roundTrip(value, encodeTableStatistics, decodeTableStatistics),
      ),
      { seed, numRuns: RUNS },
    );
  });

  it("manifest roots", () => {
    fc.assert(
      fc.property(manifestRoot, (root) => {
        reachLedger(root);
        roundTrip(root, encodeManifestRoot, decodeManifestRoot);
      }),
      {
        seed,
        numRuns: RUNS,
      },
    );
  });

  it("refuses every damaged byte as ManifestCorruptionError or decodes a value, never crashes", () => {
    fc.assert(
      fc.property(
        fc.oneof(
          groupRoot.map(encodeGroupRoot),
          manifestRoot.map(encodeManifestRoot),
          tableStatistics.map(encodeTableStatistics),
        ),
        (bytes) => {
          const random = mulberry32(bytes.length);
          for (let k = 0; k < Math.min(bytes.length, 200); k++) {
            const at = Math.floor(random() * bytes.length);
            const damaged = bytes.slice();
            damaged[at] = (damaged[at] ?? 0) ^ (1 + Math.floor(random() * 255));
            for (const candidate of [damaged, bytes.subarray(0, at)]) {
              const budget = tracked();
              for (const decode of [
                decodeGroupRoot,
                decodeManifestRoot,
                decodeGroupPage,
                decodeGroupIndex,
                decodeTableStatistics,
              ]) {
                try {
                  const parsed = decode(candidate, budget);
                  budget.free(parsed.charge);
                } catch (error) {
                  expect(error).toBeInstanceOf(ManifestCorruptionError);
                }
              }
              expect(budget.held).toBe(0); // a refused parse returns its charge
            }
          }
        },
      ),
      { seed, numRuns: 20 },
    );
  });
});

describe("table statistics codec", () => {
  const column = {
    columnId: 3,
    nullCount: 1,
    values: 5,
    bytes: 40,
    unbounded: false,
    min: 1,
    max: 9,
  } as const;
  it("refuses columns out of order and bounds that disagree with their column, at encode", () => {
    expect(() =>
      encodeTableStatistics({ groupCount: 1, columns: [column, { ...column, columnId: 2 }] }),
    ).toThrow(RangeError);
    expect(() =>
      encodeTableStatistics({ groupCount: 1, columns: [{ ...column, unbounded: true }] }),
    ).toThrow(RangeError);
  });
  it("refuses the same at decode, and a trailing byte", () => {
    const budget = tracked();
    const good = encodeTableStatistics({ groupCount: 1, columns: [column] });
    // Hand-written: a bounded column with values but no bounds (tags 0, 0 for min and max).
    const out = new ByteWriter();
    out.u32(0x3154_534d);
    out.u8(1);
    for (const value of [1, 1, 3, 1, 5, 40]) out.uint(value);
    out.u8(0);
    out.u8(0);
    out.u8(0);
    for (const bytes of [out.finish(), Uint8Array.from([...good, 0])])
      expect(() => decodeTableStatistics(bytes, budget)).toThrow(ManifestCorruptionError);
    expect(budget.held).toBe(0);
  });
});

describe("manifest codec rules", () => {
  const big: ExtentRef = { offset: 0, length: 1 << 20 };
  const small: ExtentRef = { offset: 1 << 20, length: 4096 };
  const ref = (owner: ExtentRef, offset = 0, length = 16): BlobRef => ({
    extent: owner,
    offset,
    length,
    checksum: 7,
  });

  it("decodes every reference into one extent to the same ExtentRef", () => {
    const owner = big;
    const root: GroupRoot = {
      rowCount: 10,
      rowIds: { kind: "dense" },
      columns: [1, 2, 3].map((columnId) => ({
        columnId,
        type: { kind: "int64" },
        data: { extent: owner, offset: columnId * 100, length: 50 },
        directory: ref(owner, columnId * 100 + 50, 20),
        statistics: { nullCount: 0, min: 1n, max: 2n, dictionarySize: null },
        distinctSketch: null,
      })),
    };
    const parsed = decodeGroupRoot(encodeGroupRoot(root), tracked()).value;
    const shared = parsed.columns[0]?.data.extent;
    for (const chunk of parsed.columns) {
      expect(chunk.data.extent).toBe(shared);
      expect(chunk.directory.extent).toBe(shared);
    }
  });

  it("keeps every blob under its cap: big group roots and roots are refused at encode", () => {
    const owner = big;
    const wide: GroupRoot = {
      rowCount: 1,
      rowIds: { kind: "dense" },
      columns: Array.from({ length: 4000 }, (_, columnId) => ({
        columnId,
        type: { kind: "text" },
        data: { extent: owner, offset: 0, length: 1 },
        directory: ref(owner),
        statistics: {
          nullCount: 0,
          min: new Uint8Array(120),
          max: new Uint8Array(120),
          dictionarySize: 1,
        },
        distinctSketch: ref(owner),
      })),
    };
    expect(() => encodeGroupRoot(wide)).toThrow(RangeError);
    const { columns } = wide;
    const fits = {
      ...wide,
      columns: columns.slice(0, 1600).map((chunk) => ({
        ...chunk,
        statistics: { ...chunk.statistics, min: new Uint8Array(32), max: new Uint8Array(32) },
      })),
    };
    expect(encodeGroupRoot(fits).length).toBeLessThan(1 << 20); // PostgreSQL's 1,600 columns, 32-byte bounds
    const groups: GroupRef[] = Array.from({ length: GROUPS_PER_PAGE + 1 }, (_, k) => ({
      groupId: k,
      firstRowId: k * 10 + 1,
      lastRowId: k * 10 + 10,
      rowCount: 10,
      liveRowCount: 10,
      root: ref(owner),
      deletions: null,
    }));
    expect(() => encodeGroupPage(groups)).toThrow(RangeError);
    expect(encodeGroupPage(groups.slice(0, GROUPS_PER_PAGE)).length).toBeLessThan(1 << 20);
    const huge: ManifestRoot = {
      formatVersion: 2,
      sequence: 1,
      epoch: 1,
      log: { salt: 1, firstLsn: 1, rootSequence: 1 },
      globalLsn: 1,
      catalogVersion: 1,
      catalog: ref(owner),
      nextTableId: 1,
      sequences: [],
      tables: [],
      clients: Array.from({ length: 20_000 }, (_, k) => ({
        clientId: `client-${String(k)}-${"x".repeat(40)}`,
        resolvedThrough: k,
        outcomes: [],
      })),
      extentList: [],
    };
    expect(() => encodeManifestRoot(huge)).toThrow(RangeError);
    expect(MAX_ROOT_BYTES).toBe(1 << 20);
  });

  it("refuses invalid shapes at encode and decode: overlapping groups, references past their extent", () => {
    const owner = small;
    const group = (first: number, last: number): GroupRef => ({
      groupId: first,
      firstRowId: first,
      lastRowId: last,
      rowCount: last - first + 1,
      liveRowCount: 0,
      root: ref(owner),
      deletions: null,
    });
    expect(() => encodeGroupPage([group(1, 10), group(5, 20)])).toThrow(RangeError);
    expect(() =>
      encodeGroupPage([group(1, 10)].map((g) => ({ ...g, root: ref(owner, 4000, 200) }))),
    ).toThrow(RangeError);
  });

  it("refuses at decode a reference that lies outside its extent", () => {
    // A hand-built directory page: one extent of 10 bytes, a group whose root is 5 + 10 bytes.
    const out = new ByteWriter();
    out.u32(0x3150_474d);
    out.u8(1);
    out.uint(1);
    out.uint(0);
    out.uint(10);
    out.uint(1); // one group
    for (const value of [1, 1, 9, 10, 10]) out.uint(value);
    out.uint(0); // extent 0
    out.uint(5);
    out.uint(10);
    out.u32(1);
    out.u8(0);
    const bytes = out.finish();
    expect(() => decodeGroupPage(bytes, tracked())).toThrow(ManifestCorruptionError);
    // The same page with a 5-byte root decodes.
    bytes[bytes.length - 6] = 5;
    expect(decodeGroupPage(bytes, tracked()).value[0]?.root.length).toBe(5);
  });

  it("charges before it parses: a refused budget is ParseBudgetError and builds nothing", () => {
    const root: GroupRoot = { rowCount: 1, rowIds: { kind: "dense" }, columns: [] };
    const bytes = encodeGroupRoot(root);
    const refusing: ByteBudget = { allocate: () => false, free: () => undefined };
    expect(() => decodeGroupRoot(bytes, refusing)).toThrow(ParseBudgetError);
    // Even a blob that would not parse is refused for budget first: nothing was built.
    expect(() => decodeGroupRoot(new Uint8Array([1, 2, 3]), refusing)).toThrow(ParseBudgetError);
    const budget = tracked();
    const damaged = bytes.slice();
    damaged[0] = 0;
    expect(() => decodeGroupRoot(damaged, budget)).toThrow(ManifestCorruptionError);
    expect(budget.held).toBe(0);
  });

  it("refuses a root written by a newer format", () => {
    const owner = big;
    const root: ManifestRoot = {
      formatVersion: 3,
      sequence: 1,
      epoch: 1,
      log: { salt: 1, firstLsn: 1, rootSequence: 1 },
      globalLsn: 1,
      catalogVersion: 1,
      catalog: ref(owner),
      nextTableId: 1,
      sequences: [],
      tables: [],
      clients: [],
      extentList: [],
    };
    expect(() => decodeManifestRoot(encodeManifestRoot(root), tracked())).toThrow(/newer/);
  });

  it("refuses a root of format 1, written before extent lists (ruling R25)", () => {
    const root: ManifestRoot = {
      formatVersion: 1,
      sequence: 1,
      epoch: 1,
      log: { salt: 1, firstLsn: 1, rootSequence: 1 },
      globalLsn: 1,
      catalogVersion: 1,
      catalog: ref(big),
      nextTableId: 1,
      sequences: [],
      tables: [],
      clients: [],
      extentList: [],
    };
    const budget = tracked();
    expect(() => decodeManifestRoot(encodeManifestRoot(root), budget)).toThrow(
      /before extent lists/,
    );
    expect(budget.held).toBe(0);
  });

  it("refuses an extent list out of offset order or with a bad page count", () => {
    const page = (first: number, count: number) => ({ blob: ref(big), first, count });
    const rootWith = (extentList: ManifestRoot["extentList"]): ManifestRoot => ({
      formatVersion: 2,
      sequence: 1,
      epoch: 1,
      log: { salt: 1, firstLsn: 1, rootSequence: 1 },
      globalLsn: 1,
      catalogVersion: 1,
      catalog: ref(big),
      nextTableId: 1,
      sequences: [],
      tables: [],
      clients: [],
      extentList,
    });
    const ok = rootWith([page(0, 1), page(4096, 4096)]);
    expect(decodeManifestRoot(encodeManifestRoot(ok), tracked()).value.extentList).toEqual(
      ok.extentList,
    );
    for (const bad of [
      [page(4096, 1), page(0, 1)],
      [page(0, 1), page(0, 1)],
      [page(0, 0)],
      [page(0, 4097)],
    ])
      expect(() => decodeManifestRoot(encodeManifestRoot(rootWith(bad)), tracked())).toThrow(
        ManifestCorruptionError,
      );
  });
});
