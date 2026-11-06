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
import type { Loan } from "../memory/governor.js";
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
import type { ChunkReads, ColumnChunkRef } from "./scan.js";
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

/** Every emitted row, as values (locators included), in output order. */
async function collect(scan: TableScan): Promise<unknown[][]> {
  const out: unknown[][] = [];
  for (;;) {
    const item = await scan.next();
    if (item === null) break;
    const batch: Batch = item.batch;
    const rows =
      batch.selection === null
        ? Array.from({ length: batch.length }, (_, k) => k)
        : Array.from(batch.selection.rows.subarray(0, batch.selection.count));
    for (const r of rows) out.push(batch.columns.map((column) => normalize(valueAt(column, r))));
  }
  return out;
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
          fc.constantFrom(1 << 20, 16 * 1024),
          fc.integer(),
          fc.boolean(),
          async (shapes, conjuncts, emitLocator, targetBytes, inner, encoded) => {
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
            // Pushed conjuncts on encoded pages (the page format) or, forced, on decoded values.
            const scan = scanOf(
              table,
              space,
              filter,
              { emitLocator, targetBytes, budget },
              encoded,
            );
            const ours = await collect(scan);
            const stats: ScanStats = scan.stats;
            scan.close();
            expect(budget.held).toBe(0);
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
