/**
 * The table scan (exec/table-scan.ts) against a reference: the same scan with pushdown off and
 * dense decode (decode-then-filter), and that reference against the source rows. Tables are
 * generated: groups written through the page writer (dictionary and plain text, values stored out
 * of line), memory segments of lent vectors, deletions, a column some groups predate, and
 * filters mixing pushed conjuncts (comparisons, IN, IS NULL) with residual ones (arithmetic, OR,
 * NOT). The filter kernels' semantics are judged against PostgreSQL elsewhere
 * (kernels/pglite.test.ts); here the law is that pushdown, pruning, gather, byte-bounded batches
 * and late materialization never change which rows come out or their values.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { ColumnDef } from "../access/catalog.js";
import type {
  ChunkResult,
  DeletionVector,
  GroupSegment,
  MemorySegment,
  PageSummary,
  Segment,
  TableSnapshot,
} from "../access/snapshot.js";
import type { ScalarExpr } from "../ir/ir.js";
import { Governor, type Loan } from "../memory/governor.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { ChunkReader } from "../page/reader.js";
import { writeGroup, type TestValue } from "../page/test-support.js";
import type { PhysicalType } from "../types.js";
import type { ByteBudget } from "../access/space.js";
import type { Batch, Selection, Vector } from "../vector/vector.js";
import {
  F64,
  I32,
  I64,
  TEXT,
  UNCHARGED,
  bigints,
  call,
  cmp,
  col,
  decimal,
  decimals,
  dictionary,
  lit,
  numbers,
  texts,
  valueAt,
} from "./kernels/test-support.js";
import { OutputArena } from "./kernels/vectors.js";
import { andThen } from "./pushdown.js";
import type { ChunkPages, ChunkReads, ColumnChunkRef } from "./scan.js";
import { TableScan, type ScanStats, type TableScanOptions } from "./table-scan.js";

/** `xs[i]`, which must exist. */
function at<T>(xs: readonly T[], i: number): T {
  const x = xs[i];
  if (x === undefined) throw new RangeError(`no element ${String(i)}`);
  return x;
}

const SEEDS = seedsFor("next-table-scan", [0x5ca9, 0x1d]);
const N12 = decimal(12, 2);
const encoder = new TextEncoder();
const MISSING_ID = 6;

const DEFS: ColumnDef[] = [
  [1, "i", I32],
  [2, "b", I64],
  [3, "f", F64],
  [4, "t", TEXT],
  [5, "n", N12],
  [MISSING_ID, "late", I32],
].map(([id, name, type]) => ({
  id: id as number,
  name: name as string,
  type: type as PhysicalType,
  declared: { name: "x" } as never,
  nullable: true,
  defaultSql: null,
  generatedSql: null,
  identitySequence: null,
  // Rows stored before ALTER TABLE ADD COLUMN read as 7.
  missing: id === MISSING_ID ? numbers(I32, [7]) : null,
}));

interface Row {
  readonly i: number | null;
  readonly b: bigint | null;
  readonly f: number | null;
  readonly t: string | null;
  readonly n: bigint | null;
  readonly late: number | null;
}

function rowsOf(random: () => number, count: number, longText: boolean): Row[] {
  const pick = <T>(values: readonly T[]): T => at(values, Math.floor(random() * values.length));
  return Array.from({ length: count }, () => ({
    i: random() < 0.1 ? null : Math.floor(random() * 200) - 100,
    b: random() < 0.1 ? null : pick([0n, 1n, -5n, 2n ** 60n, BigInt(Math.floor(random() * 1e6))]),
    f: random() < 0.1 ? null : pick([0, -0, NaN, Infinity, 1.5, -2.25, random() * 100]),
    t:
      random() < 0.1
        ? null
        : longText && random() < 0.05
          ? `long-${"x".repeat(1000 + Math.floor(random() * 3000))}`
          : `v${String(Math.floor(random() * 12))}`,
    n: random() < 0.1 ? null : BigInt(Math.floor(random() * 200000) - 100000),
    late: random() < 0.1 ? null : Math.floor(random() * 10),
  }));
}

/** A deletion vector over `deleted` positions. */
function deletionsOf(deleted: ReadonlySet<number>): DeletionVector | null {
  if (deleted.size === 0) return null;
  return {
    deletedCount: deleted.size,
    isDeleted: (position) => deleted.has(position),
    liveRows(from: number, length: number, input: Selection | null, out: Selection): void {
      let n = 0;
      const count = input === null ? length : input.count;
      for (let k = 0; k < count; k++) {
        const row = input === null ? k : (input.rows[k] ?? 0);
        if (!deleted.has(from + row)) out.rows[n++] = row;
      }
      out.count = n;
    },
  };
}

const NO_PAGES = (): readonly PageSummary[] => [];

/** A group segment over a written group (`late` absent when the group predates it). */
async function groupSegment(
  index: number,
  rows: readonly Row[],
  deleted: ReadonlySet<number>,
  predatesLate: boolean,
  space: MemoryExtentSpace,
): Promise<GroupSegment> {
  const columns: Array<{ type: PhysicalType; values: TestValue[]; vector?: { scale?: number } }> = [
    { type: I32, values: rows.map((r) => r.i) },
    { type: I64, values: rows.map((r) => r.b) },
    { type: F64, values: rows.map((r) => r.f) },
    { type: TEXT, values: rows.map((r) => (r.t === null ? null : encoder.encode(r.t))) },
    {
      type: N12,
      values: rows.map((r) => (r.n === null ? null : { coefficient: r.n, special: 0 })),
      vector: { scale: 2 },
    },
    { type: I32, values: rows.map((r) => r.late) },
  ];
  const written = await writeGroup(columns, { space, inlineBytes: 512, batch: 700 });
  const chunks: ColumnChunkRef[] = written.group.columns.map((chunk) => chunk);
  return {
    kind: "group",
    index,
    rowCount: rows.length,
    liveRowCount: rows.length - deleted.size,
    staged: false,
    group: {} as GroupSegment["group"],
    root: () => ({}) as Awaited<ReturnType<GroupSegment["root"]>>,
    chunk: (columnId: number) =>
      columnId === MISSING_ID && predatesLate ? null : (chunks[columnId - 1] ?? null),
    deletions: () => deletionsOf(deleted),
    statistics: () => null,
    pages: NO_PAGES,
  };
}

/** A memory segment lending one resident vector per column and chunk. */
function memorySegment(
  index: number,
  rows: readonly Row[],
  deleted: ReadonlySet<number>,
): MemorySegment {
  const chunkVector = (columnId: number, chunk: number): Vector => {
    const part = rows.slice(chunk * 2048, chunk * 2048 + 2048);
    switch (columnId) {
      case 1:
        return numbers(
          I32,
          part.map((r) => r.i),
        );
      case 2:
        return bigints(
          I64,
          part.map((r) => r.b),
        );
      case 3:
        return numbers(
          F64,
          part.map((r) => r.f),
        );
      case 4: {
        // Dictionary-encoded, as a memtable may keep codes.
        const entries = [...new Set(part.map((r) => r.t).filter((t) => t !== null))];
        return dictionary(
          texts(entries.length === 0 ? [""] : entries),
          part.map((r) => (r.t === null ? null : entries.indexOf(r.t))),
        );
      }
      case 5:
        return decimals(
          N12,
          part.map((r) => (r.n === null ? null : ([r.n, 2] as const))),
          "safe",
          2,
        );
      default:
        return numbers(
          I32,
          part.map((r) => r.late),
        );
    }
  };
  return {
    kind: "memory",
    index,
    rowCount: rows.length,
    liveRowCount: rows.length - deleted.size,
    deletions: () => deletionsOf(deleted),
    statistics: () => null,
    pages: NO_PAGES,
    chunk(columnId: number, chunk: number): ChunkResult {
      const vector = chunkVector(columnId, chunk);
      return { vector, firstRow: 0, rowCount: vector.length, complete: true, reason: null };
    },
  };
}

/** Chunk readers over the in-memory extent space (with its cache, as production). */
function readsOver(space: MemoryExtentSpace): ChunkReads {
  return {
    open: (_pages, chunk, options) => ChunkReader.open(space.view({ cache: true }), chunk, options),
  };
}

/** A budget that counts what is held. */
function counting(): ByteBudget & { held: number } {
  const budget = {
    held: 0,
    allocate(bytes: number) {
      budget.held += bytes;
      return true;
    },
    free(bytes: number) {
      budget.held -= bytes;
    },
  };
  return budget;
}

/**
 * Every emitted row, as values (locators included), in output order; `check` sees each batch and
 * whether it is a memory segment's lent chunk (zero copy, not sized by the scan).
 */
async function collect(
  scan: TableScan,
  check?: (batch: Batch, lent: boolean) => void,
): Promise<unknown[][]> {
  const out: unknown[][] = [];
  for (;;) {
    const lentBefore = scan.stats.memoryBatches;
    const item = await scan.next();
    if (item === null) break;
    const batch: Batch = item.batch;
    check?.(batch, scan.stats.memoryBatches > lentBefore);
    const rows =
      batch.selection === null
        ? Array.from({ length: batch.length }, (_, k) => k)
        : Array.from(batch.selection.rows.subarray(0, batch.selection.count));
    for (const r of rows) out.push(batch.columns.map((column) => normalize(valueAt(column, r))));
  }
  return out;
}

/**
 * Bytes of a batch as decoded (a lower bound of what the scan counts): validity, values, codes,
 * scales, limbs, specials, offsets, varlen bytes in use, locators.
 */
function batchBytes(batch: Batch): { bytes: number; varlen: number } {
  let bytes = 0;
  let varlen = 0;
  for (const vector of batch.columns) {
    if (vector.form === "constant") continue;
    const n = vector.length;
    if (vector.validity !== null) bytes += 4 * ((n + 31) >>> 5);
    if (vector.form === "dictionary") {
      bytes += 4 * n;
      continue;
    }
    const data = vector.data;
    switch (data.layout) {
      case "bool":
        bytes += n;
        break;
      case "i32":
      case "f32":
        bytes += 4 * n;
        break;
      case "f64":
      case "int64":
        bytes += 8 * n;
        break;
      case "decimal":
        bytes +=
          (data.coefficients.repr === "int128" ? 16 : 8) * n +
          (data.scales === null ? 0 : 2 * n) +
          (data.specials === null ? 0 : n);
        break;
      case "varlen": {
        const used = (data.offsets[n] ?? 0) - (data.offsets[0] ?? 0);
        bytes += 4 * (n + 1) + used;
        varlen += used;
        break;
      }
      case "fixed16":
      case "interval":
        bytes += 16 * n;
        break;
    }
  }
  return { bytes, varlen };
}

function normalize(value: unknown): unknown {
  if (typeof value === "number" && Number.isNaN(value)) return "NaN";
  if (typeof value === "number" && Object.is(value, -0)) return "-0";
  return value;
}

/** Source rows as the scan emits them (decimals as [coefficient, 2]). */
function expectedRow(r: Row, late: boolean): unknown[] {
  return [r.i, r.b, normalize(r.f), r.t, r.n === null ? null : [r.n, 2], late ? 7 : r.late].map(
    normalize,
  );
}

// ── Filters ─────────────────────────────────────────────────────────────────────────────────

const C = (index: number): ScalarExpr => col(index, at(DEFS, index).type);
const conjunctArb: fc.Arbitrary<ScalarExpr> = fc.oneof(
  fc.integer({ min: -100, max: 100 }).map((k) => cmp("<", C(0), lit(numbers(I32, [k])))),
  fc.integer({ min: -100, max: 100 }).map((k) => cmp(">=", lit(numbers(I32, [k])), C(0))),
  fc.constantFrom(0n, 1n, 2n ** 60n).map((k) => cmp("=", C(1), lit(bigints(I64, [k])))),
  fc.constantFrom(0, 1.5, NaN, -0).map((x) => cmp(">", C(2), lit(numbers(F64, [x])))),
  fc.integer({ min: 0, max: 12 }).map((k) => cmp("=", C(3), lit(texts([`v${String(k)}`])))),
  fc.constant<ScalarExpr>({ op: "isNull", negated: false, arg: C(3), type: { kind: "bool" } }),
  fc.constant<ScalarExpr>({ op: "isNull", negated: true, arg: C(1), type: { kind: "bool" } }),
  fc
    .array(fc.integer({ min: -100, max: 100 }), { minLength: 0, maxLength: 3 })
    .map((ks): ScalarExpr => ({
      op: "inList",
      negated: false,
      arg: C(0),
      list: [...ks.map((k) => lit(numbers(I32, [k]))), lit(numbers(I32, [null]))],
      collation: null,
      type: { kind: "bool" },
    })),
  fc
    .bigInt({ min: -100000n, max: 100000n })
    .map((c) => cmp("<=", C(4), lit(decimals(N12, [[c, 2]], "safe", 2)))),
  fc.integer({ min: 0, max: 9 }).map((k) => cmp("=", C(5), lit(numbers(I32, [k])))),
  // Residual: arithmetic, OR, NOT.
  fc
    .integer({ min: -50, max: 50 })
    .map((k) => cmp(">", call("add", I32, C(0), C(5)), lit(numbers(I32, [k])))),
  fc.constant<ScalarExpr>({
    op: "or",
    args: [cmp("=", C(3), lit(texts(["v1"]))), cmp(">", C(0), lit(numbers(I32, [50])))],
    type: { kind: "bool" },
  }),
  fc.constant<ScalarExpr>({
    op: "not",
    arg: cmp("<", C(2), lit(numbers(F64, [0]))),
    type: { kind: "bool" },
  }),
);

function and(conjuncts: readonly ScalarExpr[]): ScalarExpr | null {
  if (conjuncts.length === 0) return null;
  if (conjuncts.length === 1) return at(conjuncts, 0);
  return { op: "and", args: conjuncts, type: { kind: "bool" } };
}

function scanOf(
  table: TableSnapshot,
  space: MemoryExtentSpace,
  filter: ScalarExpr | null,
  options: Partial<TableScanOptions> = {},
  encoded = true,
): TableScan {
  return new TableScan({
    table,
    pages: { read: () => Promise.reject(new Error("unused")), peek: () => null },
    reads: readsOver(space),
    encodedPushdown: encoded,
    budget: options.budget ?? UNCHARGED,
    columns: DEFS.map((d) => d.id),
    filter,
    emitLocator: options.emitLocator ?? false,
    context: { arena: new OutputArena(UNCHARGED, 2048), params: { slots: [] }, input: [] },
    ...options,
  });
}

describe("table scan: pushdown, gather and late materialization never change the answer", () => {
  it.each(SEEDS)(
    "generated tables and filters (seed %i)",
    async (seed) => {
      const reach = createReach("next-table-scan", [
        "gather batch",
        "dense batch",
        "residual",
        "memory segment",
        "byte-bounded split",
        "out-of-line value",
        "deleted rows",
        "group predates a column",
        "group pruned",
        "encoded-page pushdown",
        "decoded fallback",
        "batch bounded by fixed-width bytes",
        "yield without output",
      ] as const);
      await fc.assert(
        fc.asyncProperty(
          fc.array(
            fc.record({
              kind: fc.constantFrom("group", "group", "memory"),
              rows: fc.oneof(
                fc.integer({ min: 1, max: 300 }),
                fc.integer({ min: 2000, max: 5000 }),
              ),
              deleteShare: fc.constantFrom(0, 0.1, 0.6),
              predatesLate: fc.boolean(),
              longText: fc.boolean(),
            }),
            { minLength: 1, maxLength: 3 },
          ),
          fc.array(conjunctArb, { minLength: 0, maxLength: 3 }),
          fc.boolean(),
          fc.constantFrom(1 << 20, 16 * 1024, 4096),
          fc.integer(),
          fc.boolean(),
          fc.boolean(),
          async (shapes, conjuncts, emitLocator, targetBytes, inner, encoded, ticking) => {
            reach.beginHistory();
            const random = mulberry32(inner);
            const space = new MemoryExtentSpace();
            const segments: Segment[] = [];
            const source: Array<{ row: Row; late: boolean; locator: number; deleted: boolean }> =
              [];
            for (const [index, shape] of shapes.entries()) {
              const rows = rowsOf(random, shape.rows, shape.longText);
              const deleted = new Set<number>();
              for (let p = 0; p < rows.length; p++)
                if (random() < shape.deleteShare) deleted.add(p);
              if (deleted.size > 0) reach.reach("deleted rows");
              const late = shape.kind === "group" && shape.predatesLate;
              if (late) reach.reach("group predates a column");
              if (shape.kind === "memory") reach.reach("memory segment");
              if (shape.kind === "group" && rows.some((r) => (r.t?.length ?? 0) > 512))
                reach.reach("out-of-line value");
              segments.push(
                shape.kind === "group"
                  ? await groupSegment(index, rows, deleted, late, space)
                  : memorySegment(index, rows, deleted),
              );
              rows.forEach((row, p) =>
                source.push({ row, late, locator: index * 65_536 + p, deleted: deleted.has(p) }),
              );
            }
            const table: TableSnapshot = {
              tableId: 9,
              schemaVersion: 1,
              columns: DEFS,
              liveRowCount: source.filter((s) => !s.deleted).length,
              segments,
            };
            const filter = and(conjuncts);
            // The reference, unfiltered, is the source's live rows.
            const everything = await collect(
              scanOf(table, space, null, { pushdown: false, emitLocator: true }),
            );
            expect(everything).toEqual(
              source
                .filter((s) => !s.deleted)
                .map((s) => [...expectedRow(s.row, s.late), s.locator]),
            );
            const budget = counting();
            // A clock advancing 1 ms per reading: 4 ms slices end inside long filtered runs.
            let tick = 0;
            const clock = ticking ? { now: () => tick++ } : {};
            // Pushed conjuncts on encoded pages (the page format) or, forced, on decoded values.
            const scan = scanOf(
              table,
              space,
              filter,
              { emitLocator, targetBytes, budget, ...clock },
              encoded,
            );
            // Batches the scan decodes stay within the byte target (fixed-width parts included),
            // but a single row may exceed it.
            const seen = { fixedBound: false };
            const ours = await collect(scan, (batch, lent) => {
              if (lent || batch.length <= 1) return;
              const { bytes, varlen } = batchBytes(batch);
              expect(bytes).toBeLessThanOrEqual(targetBytes);
              // Cut short with most of its bytes fixed-width: sizing by varlen bytes alone would
              // have let it grow past the target.
              if (batch.length < 2048 && bytes > targetBytes / 2 && 2 * varlen < bytes)
                seen.fixedBound = true;
            });
            const stats: ScanStats = scan.stats;
            // The end of the scan gives everything back by itself; `close` is then a no-op.
            expect(budget.held).toBe(0);
            scan.close();
            expect(budget.held).toBe(0);
            if (seen.fixedBound) reach.reach("batch bounded by fixed-width bytes");
            if (stats.yields > 0) reach.reach("yield without output");
            const theirs = await collect(
              scanOf(table, space, filter, { pushdown: false, emitLocator }),
            );
            expect(ours).toEqual(theirs);
            if (stats.gatherBatches > 0) reach.reach("gather batch");
            if (stats.denseBatches > 0) reach.reach("dense batch");
            if (stats.residualBatches > 0) reach.reach("residual");
            if (stats.splits > 0) reach.reach("byte-bounded split");
            if (stats.groupsPruned > 0) reach.reach("group pruned");
            if (stats.encodedPredicates > 0) reach.reach("encoded-page pushdown");
            if (stats.decodedPredicates > 0) reach.reach("decoded fallback");
          },
        ),
        { seed, numRuns: 40 },
      );
      reach.assertReached();
    },
    300_000,
  );

  it("borrows a progress loan for one row the grant cannot hold, and repays it (R18.3)", async () => {
    const space = new MemoryExtentSpace();
    const rows = rowsOf(mulberry32(3), 40, false).map((r, k) =>
      k === 20 ? { ...r, t: `huge-${"y".repeat(200_000)}` } : r,
    );
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: rows.length,
      segments: [await groupSegment(0, rows, new Set(), false, space)],
    };
    let loaned = 0;
    let repaid = 0;
    let headroom = 0;
    // A grant with little room: the huge row's buffer fits only under a loan.
    const budget = {
      held: 0,
      allocate(bytes: number) {
        if (bytes > 150_000 + headroom) return false;
        budget.held += bytes;
        return true;
      },
      free(bytes: number) {
        budget.held -= bytes;
      },
    };
    const loans = {
      borrow(bytes: number): Promise<Loan> {
        loaned += 1;
        headroom += bytes;
        return Promise.resolve({
          bytes,
          repay: () => {
            repaid += 1;
            headroom -= bytes;
          },
        });
      },
    };
    const scan = scanOf(table, space, null, { budget, loans, targetBytes: 64 * 1024 });
    const ours = await collect(scan);
    scan.close();
    expect(ours.map((r) => r[3])).toEqual(rows.map((r) => r.t));
    expect(loaned).toBe(1);
    expect(repaid).toBe(1);
    expect(budget.held).toBe(0);
  });

  it("pushes NUMERIC, text, int8 and float constants exactly (encoded seam vs decoded)", async () => {
    const space = new MemoryExtentSpace();
    const rows = rowsOf(mulberry32(11), 3000, false);
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: rows.length,
      segments: [await groupSegment(0, rows, new Set(), false, space)],
    };
    const filters: ScalarExpr[] = [
      cmp("<=", C(4), lit(decimals(N12, [[12_345n, 2]], "safe", 2))),
      cmp(">", lit(decimals(N12, [[-500n, 2]], "safe", 2)), C(4)),
      cmp("=", C(3), lit(texts(["v3"]))),
      cmp("<", C(1), lit(bigints(I64, [2n ** 60n]))),
      cmp(">=", C(2), lit(numbers(F64, [1.5]))),
    ];
    for (const filter of filters) {
      const ours = await collect(scanOf(table, space, filter, {}, true));
      const theirs = await collect(scanOf(table, space, filter, { pushdown: false }));
      expect(ours.length).toBeGreaterThan(0);
      expect(ours).toEqual(theirs);
    }
  });
});

/**
 * Review B4-p2c-2: each finding's probe as a regression test. Law: memory/governor.ts (rule 3 batch
 * targets, rule 5 and `Loan`: repay after freeing, one loan per grant), exec/pipeline.ts and ruling
 * R23 (task yields within long synchronous work, cancellation checked when work resumes, scratch
 * charged to the instance), the scan's own contract (table-scan.ts header).
 */
describe("review B4-p2c-2: the scan's lifecycle, budget and pacing", () => {
  /** 40 rows, row 20 holding a 200,005-byte text value (stored out of line). */
  async function hugeRowTable(space: MemoryExtentSpace): Promise<{
    table: TableSnapshot;
    rows: Row[];
  }> {
    const rows = rowsOf(mulberry32(3), 40, false).map((r, k) =>
      k === 20 ? { ...r, t: `huge-${"y".repeat(200_000)}` } : r,
    );
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: rows.length,
      segments: [await groupSegment(0, rows, new Set(), false, space)],
    };
    return { table, rows };
  }

  /**
   * A grant capped at 150,000 bytes in total (what is held plus what is asked), and its loans:
   * a loan raises the cap by its bytes until repaid; `delay` holds a borrow until released.
   */
  function cappedGrant(options: { headroom?: boolean; delay?: boolean } = {}) {
    let onBorrow = (): void => undefined;
    const state = {
      loaned: 0,
      repaid: 0,
      headroom: 0,
      release: (): void => undefined,
      /** Settles when the first borrow is asked for. */
      borrowed: new Promise<void>((resolve) => {
        onBorrow = resolve;
      }),
    };
    const budget = {
      held: 0,
      allocate(bytes: number) {
        if (budget.held + bytes > 150_000 + state.headroom) return false;
        budget.held += bytes;
        return true;
      },
      free(bytes: number) {
        budget.held -= bytes;
      },
    };
    const loans = {
      borrow(bytes: number): Promise<Loan> {
        state.loaned += 1;
        onBorrow();
        if (options.headroom !== false) state.headroom += bytes;
        const loan: Loan = {
          bytes,
          repay: () => {
            state.repaid += 1;
            if (options.headroom !== false) state.headroom -= bytes;
          },
        };
        if (options.delay !== true) return Promise.resolve(loan);
        return new Promise<Loan>((resolve) => {
          state.release = () => {
            resolve(loan);
          };
        });
      },
    };
    return { state, budget, loans };
  }

  it("1. a loan is repaid once when the scan is closed right after the loan-backed batch", async () => {
    const space = new MemoryExtentSpace();
    const { table } = await hugeRowTable(space);
    const { state, budget, loans } = cappedGrant();
    const scan = scanOf(table, space, null, { budget, loans, targetBytes: 64 * 1024 });
    for (let k = 0; k < 100 && scan.stats.loans === 0; k++) await scan.next();
    scan.close();
    scan.close();
    expect(state.loaned).toBe(1);
    expect(state.repaid).toBe(1);
    expect(budget.held).toBe(0);
  });

  it("1. a loan served after the scan was aborted is repaid at once", async () => {
    const space = new MemoryExtentSpace();
    const { table } = await hugeRowTable(space);
    const { state, budget, loans } = cappedGrant({ delay: true });
    const scan = scanOf(table, space, null, { budget, loans, targetBytes: 64 * 1024 });
    // Up to the huge row: its batch waits on the loan.
    let pending: Promise<unknown> | null = null;
    for (let k = 0; k < 100 && pending === null; k++) {
      const item = Promise.resolve(scan.next());
      const first = await Promise.race([
        item.then(() => "batch"),
        state.borrowed.then(() => "loan"),
      ]);
      if (first === "loan") pending = item;
    }
    expect(pending).not.toBeNull();
    scan.abort();
    state.release();
    expect(await pending).toBeNull();
    expect(state.repaid).toBe(1);
    expect(budget.held).toBe(0);
  });

  it("1. a failure after borrowing leaves the loan to abort, which repays it", async () => {
    const space = new MemoryExtentSpace();
    const { table } = await hugeRowTable(space);
    // The loan adds no room: the row is refused even after it.
    const { state, budget, loans } = cappedGrant({ headroom: false });
    const scan = scanOf(table, space, null, { budget, loans, targetBytes: 64 * 1024 });
    await expect(collect(scan)).rejects.toThrow(/refused even after a loan/);
    expect(state.repaid).toBe(0);
    // The driver aborts its source on failure.
    scan.abort();
    expect(state.loaned).toBe(1);
    expect(state.repaid).toBe(1);
    expect(budget.held).toBe(0);
  });

  it("2. one loan covers the row's whole working set: its value lease and decode buffers", async () => {
    const space = new MemoryExtentSpace();
    const { table, rows } = await hugeRowTable(space);
    const { state, budget, loans } = cappedGrant();
    const scan = scanOf(table, space, null, { budget, loans, targetBytes: 64 * 1024 });
    const ours = await collect(scan);
    scan.close();
    expect(ours.map((r) => r[3])).toEqual(rows.map((r) => r.t));
    expect(state.loaned).toBe(1);
    expect(state.repaid).toBe(1);
    expect(budget.held).toBe(0);
  });

  it("3. fixed-width output counts toward the byte target", async () => {
    const space = new MemoryExtentSpace();
    const rows = rowsOf(mulberry32(3), 2048, false);
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: rows.length,
      segments: [await groupSegment(0, rows, new Set(), false, space)],
    };
    const peak = async (columns: number[], targetBytes: number): Promise<number> => {
      const budget = counting();
      const scan = scanOf(table, space, null, { columns, targetBytes, budget });
      let total = 0;
      let most = 0;
      const out = await collect(scan, (batch) => {
        total += batch.length;
        most = Math.max(most, budget.held);
        if (batch.length > 1) expect(batchBytes(batch).bytes).toBeLessThanOrEqual(targetBytes);
      });
      expect(total).toBe(2048);
      expect(out.map((r) => r[0])).toEqual(rows.map((r) => r.i));
      scan.close();
      expect(budget.held).toBe(0);
      return most;
    };
    await peak([1], 4096);
    // The buffers follow the batches: a small target never makes 2,048-row buffers (4 + 8 + 8 +
    // 8 bytes a row for these four columns: 57 KiB at 2,048 rows, about 4 KiB at the target).
    const small = await peak([1, 2, 3, 5], 4096);
    const large = await peak([1, 2, 3, 5], 1 << 20);
    expect(small + 40_000).toBeLessThan(large);
  });

  it("4. a scan rejecting every row yields to a task within one long next()", async () => {
    const space = new MemoryExtentSpace();
    const v = numbers(
      I32,
      Array.from({ length: 2048 }, () => 1),
    );
    const segment = memorySegment(0, [], new Set());
    const memory: Segment[] = Array.from({ length: 1000 }, (_, index) => ({
      ...segment,
      index,
      rowCount: 2048,
      liveRowCount: 2048,
      chunk: () => ({ vector: v, firstRow: 0, rowCount: 2048, complete: true, reason: null }),
    }));
    // Group segments whose ranges the pushed conjunct rejects on encoded pages (1s and 3s:
    // statistics cannot prune `i = 2`).
    const groups: Segment[] = [];
    for (let g = 0; g < 20; g++) {
      const rows = rowsOf(mulberry32(g), 4096, false).map((r, k) => ({ ...r, i: 1 + 2 * (k % 2) }));
      groups.push(await groupSegment(1000 + g, rows, new Set(), false, space));
    }
    for (const segments of [memory, groups]) {
      const table: TableSnapshot = {
        tableId: 9,
        schemaVersion: 1,
        columns: DEFS,
        liveRowCount: 0,
        segments,
      };
      // Pushed on group segments (B2 selects nothing); the whole filter on memory segments.
      const scan = scanOf(table, space, cmp("=", col(0, I32), lit(numbers(I32, [2]))), {
        columns: [1],
        // 1 ms per reading: every few rejected ranges end a 4 ms slice.
        now: (() => {
          let tick = 0;
          return () => tick++;
        })(),
      });
      // Another task (an owner message) queued before the call.
      let timer = false;
      const handle = setImmediate(() => {
        timer = true;
      });
      const pending = scan.next();
      expect(pending).toBeInstanceOf(Promise);
      expect(timer).toBe(false);
      let timerBeforeEnd = false;
      await Promise.resolve(pending).then((item) => {
        timerBeforeEnd = timer;
        expect(item).toBeNull();
      });
      clearImmediate(handle);
      expect(timerBeforeEnd).toBe(true);
      expect(scan.stats.yields).toBeGreaterThan(0);
      scan.close();
    }
  });

  it("5. abort during a memory chunk's asynchronous read: no batch, no later read", async () => {
    const space = new MemoryExtentSpace();
    const rows = rowsOf(mulberry32(3), 10, false);
    const segment = memorySegment(0, rows, new Set());
    let resume = (): void => undefined;
    const reads: number[] = [];
    const delayed: MemorySegment = {
      ...segment,
      chunk: (...args: Parameters<MemorySegment["chunk"]>) => {
        reads.push(args[0]);
        if (args[0] !== 1) return segment.chunk(...args);
        return new Promise<ChunkResult>((resolve) => {
          resume = () => {
            resolve(segment.chunk(...args));
          };
        });
      },
    };
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: 10,
      segments: [delayed],
    };
    const budget = counting();
    const scan = scanOf(table, space, null, { budget });
    const pending = scan.next();
    expect(pending).toBeInstanceOf(Promise);
    scan.abort();
    resume();
    expect(await pending).toBeNull();
    // Column 1 was in flight; columns 2 … 6 were never asked for.
    expect(reads).toEqual([1]);
    expect(budget.held).toBe(0);
  });

  it("5. abort at every suspension of a group scan: nothing more read or emitted, all closed", async () => {
    const space = new MemoryExtentSpace();
    const rows = rowsOf(mulberry32(5), 3000, true);
    const segment = await groupSegment(0, rows, new Set([3, 70, 2500]), false, space);
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: rows.length - 3,
      segments: [segment],
    };
    const filter = cmp("<", C(0), lit(numbers(I32, [60])));
    // The n-th asynchronous access (an open, a page or value load) aborts the scan just before it
    // settles; readers are wrapped to see every later call and whether each was closed.
    for (let n = 1; ; n++) {
      let calls = 0;
      let afterAbort = 0;
      let scan: TableScan | null = null;
      const readers: Array<{ closed: boolean }> = [];
      const hook = <T>(value: T): T => {
        if (!(value instanceof Promise)) return value;
        calls += 1;
        if (calls !== n) return value;
        return value.then((v: unknown) => {
          scan?.abort();
          return v;
        }) as T;
      };
      const wrap = (pages: ChunkPages): ChunkPages => {
        const state = { closed: false };
        readers.push(state);
        return new Proxy(pages, {
          get(target, key) {
            const member: unknown = Reflect.get(target, key, target);
            if (typeof member !== "function") return member;
            return (...args: unknown[]) => {
              if (key === "close") state.closed = true;
              else if (scan !== null && calls >= n) afterAbort += 1;
              const result: unknown = Reflect.apply(member, target, args);
              return hook(result);
            };
          },
        });
      };
      const reads: ChunkReads = {
        open: (_pages, chunk, options) =>
          hook(
            andThen(ChunkReader.open(space.view({ cache: false }), chunk, options), (opened) =>
              wrap(opened),
            ),
          ),
      };
      const budget = counting();
      scan = scanOf(table, space, filter, { budget, reads, targetBytes: 16 * 1024 });
      let item = await scan.next();
      while (item !== null && calls < n) item = await scan.next();
      if (calls < n) {
        // The scan ended before an n-th suspension: every point was covered (14 of them: opens,
        // page loads, value loads across both ranges).
        expect(n - 1).toBeGreaterThanOrEqual(10);
        break;
      }
      expect(item).toBeNull();
      expect(await scan.next()).toBeNull();
      // After the abort, only closes (or the access whose promise was settling).
      expect(afterAbort).toBe(0);
      expect(readers.every((r) => r.closed)).toBe(true);
      expect(budget.held).toBe(0);
    }
  });

  it("6. the scan charges its scratch before building it, and gives it back", async () => {
    const space = new MemoryExtentSpace();
    const empty: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: 0,
      segments: [],
    };
    for (const emitLocator of [false, true]) {
      const budget = counting();
      const scan = scanOf(empty, space, null, { budget, emitLocator });
      // Five Uint16Array(2048) position and selection lists, and Float64Array(2048) locators.
      expect(budget.held).toBe(5 * 2048 * 2 + (emitLocator ? 2048 * 8 : 0));
      scan.close();
      expect(budget.held).toBe(0);
    }
    // A refused charge throws and holds nothing.
    const refusing: ByteBudget = { allocate: () => false, free: () => undefined };
    expect(() => scanOf(empty, space, null, { budget: refusing })).toThrow(/refused/);
    // A memory segment's staging (re-point outputs and selection) is charged when first met.
    const rows = rowsOf(mulberry32(3), 10, false);
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: 10,
      segments: [memorySegment(0, rows, new Set())],
    };
    const budget = counting();
    const scan = scanOf(table, space, null, { budget });
    const before = budget.held;
    await scan.next();
    expect(budget.held).toBeGreaterThan(before + 6 * 2048 * 4);
    scan.close();
    expect(budget.held).toBe(0);
  });
});

/**
 * Review B4-p2c-3: loan lifecycles under the real governor (memory/governor.ts rule 5 and `Loan`:
 * a loan covers one bounded step, its bytes are freed before it is repaid, and a grant repays
 * before borrowing again), and the scan's abort contract (table-scan.ts "Pacing and ending").
 */
describe("review B4-p2c-3: loans under the real governor", () => {
  /** A 150,000-byte grant (no growth beyond it) and its loans, counted. */
  async function grantOf(): Promise<{
    governor: Governor;
    grant: Awaited<ReturnType<Governor["admit"]>>;
    loans: { borrow(bytes: number): Promise<Loan> };
    counts: { borrowed: number; repaid: number };
  }> {
    const governor = new Governor(16 * 1024 * 1024);
    const grant = await governor.admit({ label: "scan", minimumBytes: 150_000, spillable: false });
    const counts = { borrowed: 0, repaid: 0 };
    const loans = {
      async borrow(bytes: number): Promise<Loan> {
        counts.borrowed += 1;
        const loan = await grant.borrow(bytes);
        return {
          bytes,
          repay() {
            loan.repay();
            counts.repaid += 1;
          },
        };
      },
    };
    return { governor, grant, loans, counts };
  }

  /** Rows whose text is a distinct ~200 KB value (out of line), and its table. */
  async function hugeRows(
    space: MemoryExtentSpace,
    count: number,
  ): Promise<{ rows: Row[]; table: TableSnapshot }> {
    const rows = rowsOf(mulberry32(3), count, false).map((r, k) => ({
      ...r,
      i: k,
      t: `${String(k)}${"y".repeat(200_000)}`,
    }));
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: count,
      segments: [await groupSegment(0, rows, new Set(), false, space)],
    };
    return { rows, table };
  }

  it("1. a rejected oversized row gives its working set back and repays before the next", async () => {
    const space = new MemoryExtentSpace();
    const { table } = await hugeRows(space, 3);
    const { governor, grant, loans, counts } = await grantOf();
    // OR stays residual with default pushdown; it reads the huge text column and rejects all.
    const filter: ScalarExpr = {
      op: "or",
      args: [cmp("=", C(3), lit(texts(["no match"]))), cmp("=", C(3), lit(texts(["also not"])))],
      type: { kind: "bool" },
    };
    const scan = scanOf(table, space, filter, { budget: grant, loans, targetBytes: 65_536 });
    expect(await collect(scan)).toEqual([]);
    scan.close();
    expect(counts.borrowed).toBeGreaterThan(1);
    expect(counts.repaid).toBe(counts.borrowed);
    expect([grant.allocated, grant.loaned]).toEqual([0, 0]);
    grant.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("2. abort during an in-flight charged value read repays once the read settles", async () => {
    const space = new MemoryExtentSpace({ cache: false });
    const rows = rowsOf(mulberry32(3), 1, false).map((r) => ({ ...r, t: "x".repeat(200_000) }));
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: 1,
      segments: [await groupSegment(0, rows, new Set(), false, space)],
    };
    const { governor, grant, loans, counts } = await grantOf();
    // Holds back only the large value's read, after the space charged its lease.
    let resume = (): void => undefined;
    let reached = (): void => undefined;
    const paused = new Promise<void>((resolve) => {
      reached = resolve;
    });
    const view = space.view({ cache: false });
    const delayed = new Proxy(view, {
      get(target, key) {
        const member: unknown = Reflect.get(target, key, target);
        if (typeof member !== "function") return member;
        return (...args: unknown[]) => {
          const result: unknown = Reflect.apply(member, target, args);
          const refs = args[0] as ReadonlyArray<{ length: number }>;
          if (key !== "read" || !refs.some((ref) => ref.length >= 200_000)) return result;
          return Promise.resolve(result).then(
            (lease) =>
              new Promise((settle) => {
                resume = () => {
                  settle(lease);
                };
                reached();
              }),
          );
        };
      },
    });
    const reads: ChunkReads = {
      open: (_pages, chunk, options) => ChunkReader.open(delayed, chunk, options),
    };
    const scan = scanOf(table, space, null, { budget: grant, loans, reads });
    const pending = Promise.resolve(scan.next());
    await paused;
    // Abort while the scan's own read holds loan-backed bytes: no throw, repayment pending.
    expect(() => {
      scan.abort();
    }).not.toThrow();
    resume();
    expect(await pending).toBeNull();
    expect({ repaid: counts.repaid, allocated: grant.allocated, loaned: grant.loaned }).toEqual({
      repaid: 1,
      allocated: 0,
      loaned: 0,
    });
    scan.close();
    grant.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("abort after a decode failure under a real loan returns every byte and the loan", async () => {
    const space = new MemoryExtentSpace();
    const rows = rowsOf(mulberry32(3), 1, false).map((r) => ({ ...r, t: "x".repeat(200_000) }));
    const table: TableSnapshot = {
      tableId: 9,
      schemaVersion: 1,
      columns: DEFS,
      liveRowCount: 1,
      segments: [await groupSegment(0, rows, new Set(), false, space)],
    };
    const { governor, grant, loans } = await grantOf();
    const reads: ChunkReads = {
      open: async (_pages, chunk, options) => {
        const reader = await ChunkReader.open(space.view({ cache: true }), chunk, options);
        return new Proxy(reader, {
          get(target, key) {
            const member: unknown = Reflect.get(target, key, target);
            if (typeof member !== "function") return member;
            return (...args: unknown[]) => {
              const result: unknown = Reflect.apply(member, target, args);
              if ((key === "decodeRange" || key === "gather") && chunk.type.kind === "text")
                throw new Error("injected after decode");
              return result;
            };
          },
        });
      },
    };
    const scan = scanOf(table, space, null, { budget: grant, loans, reads, targetBytes: 65_536 });
    await expect(collect(scan)).rejects.toThrow("injected after decode");
    expect(grant.loaned).toBeGreaterThan(0);
    scan.abort();
    scan.close();
    expect([grant.allocated, grant.loaned]).toEqual([0, 0]);
    grant.close();
    expect(governor.usedBytes).toBe(0);
  });

  it.each(SEEDS)(
    "oversized rows under a real grant: residuals keep or reject them, loans balance (seed %i)",
    async (seed) => {
      const reach = createReach("next-table-scan-loans", [
        "rejected oversized row",
        "kept oversized row",
        "consecutive rejected oversized rows",
      ] as const);
      await fc.assert(
        fc.asyncProperty(
          fc.array(fc.boolean(), { minLength: 1, maxLength: 7 }),
          fc.array(fc.integer({ min: 0, max: 6 }), { minLength: 0, maxLength: 3 }),
          fc.boolean(),
          fc.integer(),
          async (huge, kept, dense, inner) => {
            reach.beginHistory();
            const space = new MemoryExtentSpace();
            const rows = rowsOf(mulberry32(inner), huge.length, false).map((r, k) => ({
              ...r,
              i: k,
              t: huge[k] === true ? `${String(k)}${"z".repeat(200_000)}` : `small${String(k)}`,
            }));
            const table: TableSnapshot = {
              tableId: 9,
              schemaVersion: 1,
              columns: DEFS,
              liveRowCount: rows.length,
              segments: [await groupSegment(0, rows, new Set(), false, space)],
            };
            // A residual over the text column (OR is never pushed): rows `kept` match.
            const wanted = new Set(kept.filter((k) => k < rows.length).map((k) => at(rows, k).t));
            const filter: ScalarExpr = {
              op: "or",
              args: [
                cmp("=", C(3), lit(texts(["never"]))),
                ...[...wanted].map((t) => cmp("=", C(3), lit(texts([t])))),
              ],
              type: { kind: "bool" },
            };
            const { governor, grant, loans, counts } = await grantOf();
            const scan = scanOf(table, space, filter, {
              budget: grant,
              loans,
              targetBytes: dense ? 1 << 20 : 65_536,
            });
            const ours = await collect(scan);
            scan.close();
            const expected = rows.filter((r) => wanted.has(r.t));
            expect(ours.map((r) => r[3])).toEqual(expected.map((r) => r.t));
            expect(counts.repaid).toBe(counts.borrowed);
            expect([grant.allocated, grant.loaned]).toEqual([0, 0]);
            grant.close();
            expect(governor.usedBytes).toBe(0);
            let run = 0;
            rows.forEach((r, k) => {
              if (huge[k] !== true) return;
              const keep = wanted.has(r.t);
              reach.reach(keep ? "kept oversized row" : "rejected oversized row");
              run = keep ? 0 : run + 1;
              if (run >= 2) reach.reach("consecutive rejected oversized rows");
            });
          },
        ),
        { seed, numRuns: 20 },
      );
      reach.assertReached();
    },
    300_000,
  );
});
