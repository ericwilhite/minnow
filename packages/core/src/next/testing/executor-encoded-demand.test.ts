/** Raw reducers must preserve every decoder demand of the ordinary source step,
 * including columns used only by a pushed predicate. Parsed bytes are an independent
 * codec-level fault fixture; this is not a storage-checksum proof.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  ENCODED_SOURCE_DEMAND_REACH,
  ENCODED_SOURCE_LARGE_DEMAND_REACH,
} from "../../testing/reach.js";
import { ChunkReader, DecodeFact } from "../page/reader.js";
import { decodeRange, gatherRows, parsePage } from "../page/decode.js";
import {
  PageEncoding,
  type EncodedPageCheck,
  type EncodedPageKernel,
  type PageEntry,
  type PageRows,
} from "../page/consume.js";
import { align8, PageCorruptionError, PageFlag, ValueTag } from "../page/format.js";
import { writeGroup } from "../page/test-support.js";
import type { ColumnDef } from "../access/catalog.js";
import type { GroupSegment, TableSnapshot } from "../access/snapshot.js";
import type { ByteBudget } from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import type { AggregateCall, Rel } from "../ir/ir.js";
import { AggregateOperator } from "../exec/agg/aggregate.js";
import { PageProjectOperator } from "../exec/filter.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { col, I32, I64, TEXT, valueAt } from "../exec/kernels/test-support.js";
import { drive, type Operator } from "../exec/pipeline.js";
import { TableScan, EncodedTableScan } from "../exec/table-scan.js";

class Budget implements ByteBudget {
  used = 0;
  allocate(bytes: number): boolean {
    this.used += bytes;
    return true;
  }
  free(bytes: number): void {
    this.used -= bytes;
    expect(this.used).toBeGreaterThanOrEqual(0);
  }
}
type Family = "int64" | "decimal" | "interval";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});
async function sourceDemand(
  kind: Family | "large",
  count: number,
  dense: boolean,
  argument: boolean,
  projected: boolean,
  accelerated: boolean,
  safePromise = true,
  split = false,
) {
  const type: PhysicalType =
      kind === "large"
        ? TEXT
        : kind === "decimal"
          ? { kind: "decimal", precision: 20, scale: 0 }
          : { kind },
    nullAt = (row: number) => (!split || row >= 64) && (dense ? row % 4 !== 0 : row % 8 === 1),
    values = Array.from({ length: count }, (_, row) => {
      const value = split ? BigInt(row + 1) << 40n : 1n;
      return nullAt(row)
        ? null
        : kind === "large"
          ? row === 0
            ? new Uint8Array(600 * 1024).fill(97)
            : new TextEncoder().encode(`value-${String(row)}`)
          : kind === "decimal"
            ? { coefficient: value, scale: 0, special: 0 }
            : kind === "interval"
              ? { months: 0, days: 0, micros: value }
              : value;
    }),
    fixture = await writeGroup(
      [
        { type, values },
        { type: I32, values: Array.from({ length: count }, (_, row) => row) },
      ],
      { gzip: false, pageBytes: split ? 192 : 32768, rawPageBytes: split ? 256 : 65536 },
    );
  let denseReads = 0,
    gathers = 0,
    firstPageRows = count,
    valueDemands = 0;
  const makeView = (original: PageEntry) => {
    const first = original.firstRow,
      rows = original.rowCount,
      nullCount = Array.from({ length: rows }, (_, i) => nullAt(first + i)).filter(Boolean).length,
      allNull = nullCount === rows,
      constant = first > 0 || allNull,
      maskBytes = !allNull && nullCount > 0 ? align8(Math.ceil(rows / 32) * 4) : 0,
      lane = align8(rows * 4),
      start = 16 + maskBytes,
      payload = start + (!constant && kind === "interval" ? 2 * lane : 0),
      width = kind === "interval" ? 16 : 8,
      bytes = new Uint8Array(allNull ? 16 : constant ? start + width : payload + rows * 8),
      header = new DataView(bytes.buffer),
      mask = maskBytes ? new Uint32Array(bytes.buffer, 16, Math.ceil(rows / 32)) : null;
    bytes[0] = constant ? PageEncoding.constant : PageEncoding.plain;
    bytes[1] =
      kind === "decimal" ? ValueTag.dec64 : kind === "interval" ? ValueTag.interval : ValueTag.i64;
    bytes[2] = allNull
      ? PageFlag.allNull
      : (safePromise ? PageFlag.safe : 0) | (mask ? PageFlag.validity : 0);
    bytes[3] = allNull ? 0 : width;
    header.setUint32(4, rows, true);
    for (let row = 0; row < rows; row++) {
      if (mask && !nullAt(first + row))
        mask[row >>> 5] = (mask[row >>> 5] ?? 0) | (1 << (row & 31));
      if (!allNull && !constant)
        header.setBigInt64(payload + row * 8, first + row === 0 ? (1n << 53n) + 3n : 1n, true);
    }
    if (!allNull && constant) header.setBigInt64(start + (kind === "interval" ? 8 : 0), 1n, true);
    const entry: PageEntry = {
      offset: 0,
      length: bytes.length,
      firstRow: first,
      rowCount: rows,
      encoding: bytes[0],
      nullCount,
      min: null,
      max: null,
      checksum: 0,
    };
    return { entry, view: parsePage(bytes, entry, bytes[1]) };
  };
  const defs: ColumnDef[] = [type, I32].map((t, k) => ({
      id: k + 1,
      name: `c${String(k)}`,
      type: t,
      declared: { name: "x" } as never,
      nullable: true,
      defaultSql: null,
      generatedSql: null,
      identitySequence: null,
      missing: null,
    })),
    segment: GroupSegment = {
      kind: "group",
      index: 0,
      rowCount: count,
      liveRowCount: count,
      staged: false,
      group: {} as GroupSegment["group"],
      root: () => ({}) as Awaited<ReturnType<GroupSegment["root"]>>,
      chunk: (id) => fixture.group.columns[id - 1] ?? null,
      deletions: () => null,
      statistics: () => null,
      pages: () => [],
    },
    table: TableSnapshot = {
      tableId: 1,
      schemaVersion: 1,
      columns: defs,
      liveRowCount: count,
      segments: [segment],
    },
    budget = new Budget(),
    arena = new OutputArena(budget, 2048),
    answers: unknown[][] = [],
    sink: Operator = {
      push(batch) {
        answers.push(batch.columns.map((v) => valueAt(v, 0)));
        return "ready";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    },
    call: AggregateCall = {
      fn: argument ? "count" : "count_star",
      type: I64,
      args: argument ? [col(projected ? 0 : 1, I32)] : [],
      distinct: false,
      filter: null,
      orderBy: [],
    },
    rel: Extract<Rel, { op: "aggregate" }> = {
      id: 1,
      op: "aggregate",
      input: { id: 0, op: "values", output: [], rows: [] },
      output: [{ name: "n", type: I64, nullable: false }],
      groupBy: [],
      groupingSets: null,
      aggregates: [call],
    },
    context = { arena, input: [type, I32], params: { slots: [] } },
    aggregate = new AggregateOperator(
      rel,
      { ...context, input: projected ? (argument ? [I32] : []) : context.input },
      budget,
      sink,
    ),
    consumer = projected
      ? new PageProjectOperator(argument ? [col(1, I32)] : [], context, aggregate)
      : aggregate,
    scan = new (accelerated ? EncodedTableScan : TableScan)({
      table,
      pages: fixture.space.view({ cache: true }),
      reads: {
        async open(_pages, chunk, options) {
          const reader = await ChunkReader.open(fixture.space, chunk, options);
          if (chunk.columnId !== fixture.group.columns[0]?.columnId) return reader;
          if (kind === "large") {
            expect(reader.directory.pages.some((entry) => entry.largeRegion !== undefined)).toBe(
              true,
            );
            firstPageRows = reader.directory.pages[0]?.rowCount ?? count;
            if (split) expect(firstPageRows).toBeLessThanOrEqual(64);
            return new Proxy(reader, {
              get(target, prop) {
                if (prop === "loadValues")
                  return (from: number, to: number) => {
                    if (from === 0 && to > 0) {
                      valueDemands++;
                      throw new PageCorruptionError("Unavailable source value");
                    }
                    return target.loadValues(from, to);
                  };
                if (prop === "loadValuesOf")
                  return (rows: PageRows, start = 0, end = rows.length) => {
                    for (let k = start; k < end; k++)
                      if (rows[k] === 0) {
                        valueDemands++;
                        throw new PageCorruptionError("Unavailable source value");
                      }
                    return target.loadValuesOf(rows, start, end);
                  };
                const value: unknown = Reflect.get(target, prop, target);
                return typeof value === "function" ? (value.bind(target) as unknown) : value;
              },
            });
          }
          const parsed = reader.directory.pages.map(makeView);
          firstPageRows = parsed[0]?.entry.rowCount ?? count;
          if (split) expect(firstPageRows).toBeLessThanOrEqual(64);
          return new Proxy(reader, {
            get(target, prop) {
              if (prop === "isSafe") return () => safePromise;
              // Keep target-shape proofs coherent with the independently parsed fault views.
              if (prop === "decodeFacts") return () => (safePromise ? DecodeFact.safeInt64 : 0);
              if (prop === "pageEntry") return (page: number) => parsed[page]?.entry;
              if (prop === "checkPage")
                return (page: number, check: EncodedPageCheck) => {
                  const item = parsed[page];
                  if (!item) throw new RangeError("Missing fault page");
                  return check(item.view, item.entry, type);
                };
              if (prop === "consumePage")
                return (
                  page: number,
                  rows: PageRows | null,
                  from: number,
                  to: number,
                  state: unknown,
                  kernel: EncodedPageKernel<unknown>,
                ) => {
                  const item = parsed[page];
                  if (!item) throw new RangeError("Missing fault page");
                  return kernel(state, item.view, item.entry, type, null, rows, from, to);
                };
              if (prop === "decodeRange")
                return (from: number, to: number, targetVector: FlatVector, at = 0) => {
                  denseReads++;
                  for (const { entry, view } of parsed) {
                    const start = Math.max(from, entry.firstRow),
                      stop = Math.min(to, entry.firstRow + entry.rowCount);
                    if (stop > start)
                      decodeRange(
                        view,
                        start - entry.firstRow,
                        stop - entry.firstRow,
                        targetVector,
                        at + start - from,
                      );
                  }
                };
              if (prop === "gather")
                return (
                  rows: PageRows,
                  targetVector: FlatVector,
                  at = 0,
                  start = 0,
                  end = rows.length,
                ) => {
                  gathers++;
                  let k = start;
                  for (const { entry, view } of parsed) {
                    const begin = k;
                    while (k < end && (rows[k] ?? 0) < entry.firstRow + entry.rowCount) k++;
                    if (k > begin)
                      gatherRows(
                        view,
                        rows,
                        begin,
                        k,
                        entry.firstRow,
                        targetVector,
                        at + begin - start,
                      );
                  }
                };
              const value: unknown = Reflect.get(target, prop, target);
              return typeof value === "function" ? (value.bind(target) as unknown) : value;
            },
          });
        },
      },
      budget,
      columns: [1, 2],
      filter: { op: "isNull", negated: false, arg: col(0, type), type: { kind: "bool" } },
      emitLocator: false,
      context,
      targetBytes: kind === "large" ? 1024 * 1024 : 65536,
      ...(accelerated ? { pageConsumer: consumer } : {}),
    });
  const consumed = vi.spyOn(aggregate, "consumePages");
  let error: unknown, pageCalls: number;
  try {
    await drive(scan, consumer);
  } catch (cause) {
    error = cause;
  } finally {
    pageCalls = consumed.mock.calls.length;
    consumed.mockRestore();
    scan.close();
    consumer.abort(undefined);
    arena.close();
  }
  expect(budget.used).toBe(0);
  return {
    error,
    answers,
    denseReads,
    gathers,
    pageCalls,
    firstPageRows,
    valueDemands,
    nullCount: values.filter((value) => value === null).length,
  };
}
it("preserves a predicate-only safe-number demand in a dense COUNT(*) step", async () => {
  const ordinary = await sourceDemand("int64", 257, true, false, false, false);
  expect(ordinary.error).toBeInstanceOf(PageCorruptionError);
  const raw = await sourceDemand("int64", 257, true, false, false, true);
  expect(raw.error).toBeInstanceOf(PageCorruptionError);
}, 120_000);

const families = ["int64", "decimal", "interval"] as const;
it.each(seedsFor("next-encoded-source-demand", [0xb4f1, 0xb4f2]))(
  "preserves source-only decoder demands without inspecting undemanded pages (seed %i)",
  async (seed) => {
    const reach = createReach("encoded source demand", ENCODED_SOURCE_DEMAND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...families),
        fc.integer({ min: 257, max: 1024 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (kind, count, dense, argument, projected, safePromise, split) => {
          reach.beginHistory();
          const ordinary = await sourceDemand(
              kind,
              count,
              dense,
              argument,
              projected,
              false,
              safePromise,
              split,
            ),
            raw = await sourceDemand(
              kind,
              count,
              dense,
              argument,
              projected,
              true,
              safePromise,
              split,
            );
          if (safePromise && dense) {
            expect(ordinary.error).toBeInstanceOf(PageCorruptionError);
            expect(ordinary.denseReads).toBeGreaterThan(0);
            expect(raw.error).toBeInstanceOf(PageCorruptionError);
            expect(raw.answers).toEqual([]);
            expect(raw.pageCalls).toBe(0);
            reach.reach("dense unselected scalar stays demanded");
            if (split) {
              expect(ordinary.firstPageRows).toBeLessThanOrEqual(64);
              reach.reach("entire unselected page stays demanded");
            }
          } else {
            expect(ordinary.error).toBeUndefined();
            expect(raw.error).toBeUndefined();
            const condition = `${split ? "i >= 64 AND " : ""}${dense ? "i % 4 <> 0" : "i % 8 = 1"}`,
              expected = await pg.query<{ n: string }>(
                `SELECT count(*)::text AS n FROM generate_series(0,${String(count - 1)}) AS t(i) WHERE ${condition}`,
              );
            expect(ordinary.answers).toEqual([[BigInt(expected.rows[0]?.n ?? "")]]);
            expect(raw.answers).toEqual(ordinary.answers);
            if (!dense) {
              expect(ordinary.gathers).toBeGreaterThan(0);
              reach.reach("sparse unsafe scalar stays undemanded");
            }
            if (!safePromise) {
              expect(raw.pageCalls).toBeGreaterThan(0);
              reach.reach("wide source stays direct");
            }
            if (safePromise && !dense && split && kind !== "interval") {
              expect(raw.pageCalls).toBeGreaterThan(0);
              reach.reach("undemanded page stays direct");
            }
          }
          reach.reach(kind);
          reach.reach(argument ? "COUNT other column" : "COUNT star");
          if (projected) reach.reach("pure projection hides source column");
          reach.reach("close returns all credit");
        },
      ),
      {
        seed,
        numRuns: 136,
        examples: families.flatMap((kind) =>
          [false, true].flatMap((dense) =>
            [false, true].flatMap((argument) =>
              [false, true].flatMap((projected) =>
                [false, true].flatMap((safePromise) =>
                  [false, true].map(
                    (split) =>
                      [kind, 257, dense, argument, projected, safePromise, split] as [
                        Family,
                        number,
                        boolean,
                        boolean,
                        boolean,
                        boolean,
                        boolean,
                      ],
                  ),
                ),
              ),
            ),
          ),
        ),
      },
    );
    reach.assertReached();
  },
  120_000,
);

it.each(seedsFor("next-encoded-source-large-demand", [0xb4f3, 0xb4f4]))(
  "preserves source-only out-of-line demand and skips undemanded pages (seed %i)",
  async (seed) => {
    const reach = createReach("encoded source large demand", ENCODED_SOURCE_LARGE_DEMAND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 257, max: 513 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (count, dense, argument, projected, split) => {
          reach.beginHistory();
          const ordinary = await sourceDemand(
              "large",
              count,
              dense,
              argument,
              projected,
              false,
              false,
              split,
            ),
            raw = await sourceDemand(
              "large",
              count,
              dense,
              argument,
              projected,
              true,
              false,
              split,
            );
          if (dense) {
            expect(ordinary.error).toBeInstanceOf(PageCorruptionError);
            expect(ordinary.valueDemands).toBe(1);
            expect(raw.error).toBeInstanceOf(PageCorruptionError);
            expect(raw.valueDemands).toBe(1);
            expect(raw.pageCalls).toBe(0);
            expect(raw.answers).toEqual([]);
            reach.reach("dense source value remains demanded");
            if (split) reach.reach("wholly unselected value page remains demanded");
          } else {
            expect(ordinary.error).toBeUndefined();
            expect(raw.error).toBeUndefined();
            expect(ordinary.valueDemands).toBe(0);
            expect(raw.valueDemands).toBe(0);
            const expected = await pg.query<{ n: string }>(
              `SELECT count(*)::text AS n FROM generate_series(0,${String(count - 1)}) AS t(i) WHERE ${split ? "i >= 64 AND " : ""}i % 8 = 1`,
            );
            expect(ordinary.answers).toEqual([[BigInt(expected.rows[0]?.n ?? "")]]);
            expect(raw.answers).toEqual(ordinary.answers);
            reach.reach("sparse value stays undemanded");
            if (split) {
              expect(raw.pageCalls).toBeGreaterThan(0);
              reach.reach("separate undemanded page stays direct");
            }
          }
          reach.reach(argument ? "COUNT other column" : "COUNT star");
          if (projected) reach.reach("pure projection hides source column");
          reach.reach("close returns all credit");
        },
      ),
      {
        seed,
        numRuns: 56,
        examples: [false, true].flatMap((dense) =>
          [false, true].flatMap((argument) =>
            [false, true].flatMap((projected) =>
              [false, true].map(
                (split) =>
                  [257, dense, argument, projected, split] as [
                    number,
                    boolean,
                    boolean,
                    boolean,
                    boolean,
                  ],
              ),
            ),
          ),
        ),
      },
    );
    reach.assertReached();
  },
  120_000,
);
