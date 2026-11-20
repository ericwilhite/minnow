/** S1 laws: encoded reduction preserves PostgreSQL values and the scan's selected/live rows.
 * A supported COUNT pipeline must consume verified codes/validity without materializing value
 * vectors (the page-consumer contract). The ordinary path remains the semantic fallback.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, ENCODED_SCAN_REACH } from "../../../testing/reach.js";
import type { ColumnDef } from "../../access/catalog.js";
import type { DeletionVector, GroupSegment, TableSnapshot } from "../../access/snapshot.js";
import type { ByteBudget } from "../../access/space.js";
import type { PhysicalType } from "../../types.js";
import { PageEncoding } from "../../page/consume.js";
import { PageProjectOperator } from "../filter.js";
import type { AggregateCall, Rel } from "../../ir/ir.js";
import { ChunkReader } from "../../page/reader.js";
import { defined, writeGroup } from "../../page/test-support.js";
import { ColumnTarget } from "../column-target.js";
import { F64, I32, I64, TEXT, col, cmp, lit, numbers, valueAt } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { PAGE_CONSUMER_METADATA_BYTES } from "../page-consumer.js";
import { drive, type Operator } from "../pipeline.js";
import { EncodedTableScan } from "../table-scan.js";
import { AggregateOperator } from "./aggregate.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

class Budget implements ByteBudget {
  used = 0;
  peak = 0;
  metadataDenials = 0;
  metadataRequest = false;
  constructor(readonly refuseMetadata = false) {}
  allocate(bytes: number): boolean {
    const metadata = this.metadataRequest;
    this.metadataRequest = false;
    if (metadata) {
      expect(bytes, "metadata admission must precede later query allocations").toBe(
        PAGE_CONSUMER_METADATA_BYTES,
      );
      if (this.refuseMetadata) {
        this.metadataDenials++;
        return false;
      }
    }
    this.used += bytes;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(bytes: number): void {
    this.used -= bytes;
    expect(this.used).toBeGreaterThanOrEqual(0);
  }
}

function liveRows(deleted: ReadonlySet<number>): DeletionVector | null {
  if (deleted.size === 0) return null;
  return {
    deletedCount: deleted.size,
    isDeleted: (row) => deleted.has(row),
    liveRows(from, length, input, out) {
      let count = 0;
      for (let k = 0; k < (input?.count ?? length); k++) {
        const row = input?.rows[k] ?? k;
        if (!deleted.has(from + row)) out.rows[count++] = row;
      }
      out.count = count;
    },
  };
}
const defs: ColumnDef[] = [I32, TEXT].map((type, k) => ({
  id: k + 1,
  name: `c${String(k)}`,
  type,
  declared: { name: "x" } as never,
  nullable: true,
  defaultSql: null,
  generatedSql: null,
  identitySequence: null,
  missing: null,
}));
const countCall = (column: number | null, type: PhysicalType = I32): AggregateCall => ({
  fn: column === null ? "count_star" : "count",
  type: I64,
  args: column === null ? [] : [col(column, type)],
  distinct: false,
  filter: null,
  orderBy: [],
});

it.each(seedsFor("next-encoded-scan-count", [0xb4e1, 0xb4e2]))(
  "consumes selected/live dictionary codes with admitted fallback (seed %i)",
  async (seed) => {
    const reach = createReach("encoded scan count", ENCODED_SCAN_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 129, max: 2200 }),
        fc.integer(),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom(0, 1, 50, 100),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (
          count,
          salt,
          nullable,
          deletedRows,
          percent,
          refuseMetadata,
          projected,
          aggregateValues,
          missing,
          nested,
        ) => {
          reach.beginHistory();
          const observed = new Set<"dictionary">();
          const integers = Array.from({ length: count }, (_, row) =>
              nullable && row % 13 === 0 ? null : (Math.imul(row ^ salt, 0x9e37_79b1) >>> 0) % 100,
            ),
            labels = integers.map((_, row) =>
              missing || (nullable && row % 7 === 0) ? null : `key-${String(row % 9)}`,
            ),
            deleted = new Set(
              Array.from({ length: count }, (_, row) => row).filter(
                (row) => deletedRows && row % 11 === 0,
              ),
            ),
            fixture = await writeGroup(
              [
                { type: I32, values: integers },
                {
                  type: TEXT,
                  values: labels.map((label) =>
                    label === null ? null : new TextEncoder().encode(label),
                  ),
                },
              ],
              { gzip: false, pageBytes: 1024, rawPageBytes: 8192 },
            ),
            deletion = liveRows(deleted),
            segment: GroupSegment = {
              kind: "group",
              index: 0,
              rowCount: count,
              liveRowCount: count - deleted.size,
              staged: false,
              group: {} as GroupSegment["group"],
              root: () => ({}) as Awaited<ReturnType<GroupSegment["root"]>>,
              chunk: (id) => (missing && id === 2 ? null : (fixture.group.columns[id - 1] ?? null)),
              deletions: () => deletion,
              statistics: () => null,
              pages: () => [],
            },
            table: TableSnapshot = {
              tableId: 1,
              schemaVersion: 1,
              columns: defs,
              liveRowCount: segment.liveRowCount,
              segments: [segment],
            },
            budget = new Budget(refuseMetadata),
            arena = new OutputArena(budget, 2048),
            answers: unknown[][] = [],
            sink: Operator = {
              push(batch) {
                answers.push(batch.columns.map((column) => valueAt(column, 0)));
                return "ready";
              },
              ready: () => Promise.resolve(),
              finish: () => undefined,
              abort: () => undefined,
            },
            calls: AggregateCall[] = [
              countCall(null),
              countCall(projected ? 1 : 0, I32),
              countCall(projected ? 0 : 1, TEXT),
              ...(aggregateValues
                ? [
                    { ...countCall(projected ? 1 : 0, I32), fn: "sum" as const, type: I64 },
                    { ...countCall(projected ? 1 : 0, I32), fn: "avg" as const, type: F64 },
                  ]
                : []),
            ],
            rel: Extract<Rel, { op: "aggregate" }> = {
              id: 1,
              op: "aggregate",
              input: { id: 0, op: "values", output: [], rows: [] },
              output: calls.map((call, k) => ({
                name: `n${String(k)}`,
                type: call.type,
                nullable: true,
              })),
              groupBy: [],
              groupingSets: null,
              aggregates: calls,
            },
            context = { arena, input: [I32, TEXT], params: { slots: [] } },
            aggregate = new AggregateOperator(
              rel,
              { ...context, input: projected ? [TEXT, I32] : [I32, TEXT] },
              budget,
              sink,
            ),
            consumer = nested
              ? new PageProjectOperator(
                  [col(1, TEXT), col(0, I32), col(1, TEXT)],
                  context,
                  new PageProjectOperator(
                    projected ? [col(2, TEXT), col(1, I32)] : [col(1, I32), col(0, TEXT)],
                    { ...context, input: [TEXT, I32, TEXT] },
                    aggregate,
                  ),
                )
              : projected
                ? new PageProjectOperator([col(1, TEXT), col(0, I32)], context, aggregate)
                : aggregate,
            decode = vi.spyOn(ColumnTarget.prototype, "decode"),
            scan = new EncodedTableScan({
              table,
              pages: fixture.space.view({ cache: true }),
              reads: {
                async open(_pages, chunk, options) {
                  const reader = await ChunkReader.open(fixture.space, chunk, options);
                  if (
                    chunk.type.kind === "text" &&
                    reader.directory.pages.some(
                      (entry) => (entry.encoding & ~PageEncoding.gzip) === PageEncoding.dictionary,
                    )
                  )
                    observed.add("dictionary");
                  return reader;
                },
              },
              budget,
              columns: [1, 2],
              filter: cmp("<", col(0, I32), lit(numbers(I32, [percent]))),
              emitLocator: false,
              context,
              targetBytes: 8192,
              get pageConsumer() {
                budget.metadataRequest = true;
                return consumer;
              },
            });
          try {
            await drive(scan, consumer);
            const rows = integers
                .map(
                  (value, row) =>
                    `(${String(row)},${value === null ? "NULL" : String(value)},${labels[row] === null ? "NULL" : `'${labels[row] ?? ""}'`},${deleted.has(row) ? "true" : "false"})`,
                )
                .join(","),
              expected = await pg.query<{ a: number; b: number; c: number; s: string | null }>(
                `SELECT count(*)::int a,count(i)::int b,count(t)::int c,sum(i)::text s FROM (VALUES ${rows}) v(id,i,t,gone) WHERE NOT gone AND i < ${String(percent)}`,
              );
            const answer = defined(expected.rows[0]);
            // Integer AVG keeps its existing float64 result contract. Exact SUM/count come
            // independently from PG; this adds no tolerance or NUMERIC output normalization.
            expect(answers).toEqual([
              [
                BigInt(answer.a),
                BigInt(answer.b),
                BigInt(answer.c),
                ...(aggregateValues
                  ? [
                      answer.s === null ? null : BigInt(answer.s),
                      answer.b === 0 ? null : Number(answer.s) / answer.b,
                    ]
                  : []),
              ],
            ]);
            if (answer.a === 0) {
              expect(decode.mock.calls.length).toBe(0);
              reach.reach("empty selection");
            } else if (refuseMetadata) {
              expect(budget.metadataDenials).toBeGreaterThan(0);
              expect(decode.mock.calls.length).toBeGreaterThan(0);
              reach.reach("metadata refusal uses ordinary batches");
            } else if (missing) {
              expect(decode.mock.calls.length).toBeGreaterThan(0);
              reach.reach("missing column uses complete ordinary step");
            } else {
              expect(decode.mock.calls.length).toBe(0);
              reach.reach("encoded count without value vectors");
            }
            if (aggregateValues && answer.a > 0) reach.reach("shared sum and average");
            if (projected) reach.reach("pure column remap");
            if (nested && answer.a > 0 && !missing && !refuseMetadata)
              reach.reach("nested duplicate-column bindings");
            if (nullable) reach.reach("nulls");
            if (deletedRows) reach.reach("visibility");
            if (percent === 1) reach.reach("sparse selection");
            if (percent === 100) reach.reach("dense selection");
            if (count > 2048) reach.reach("multiple logical steps");
            if (observed.has("dictionary") && answer.a > 0 && !missing && !refuseMetadata) {
              expect(decode.mock.calls.length).toBe(0);
              reach.reach("dictionary validity without expansion");
            }
          } finally {
            decode.mockRestore();
            scan.close();
            consumer.abort(undefined);
            arena.close();
          }
          expect(budget.used).toBe(0);
          reach.reach("close returns all credit");
        },
      ),
      {
        seed,
        numRuns: 80,
        examples: [
          [2200, 1, true, true, 1, false, true, true, false, false],
          [2200, 2, true, true, 100, true, false, false, false, false],
          [129, 3, false, false, 0, false, false, true, false, false],
          [2200, 3, true, true, 100, false, false, true, false, true],
          [2200, 4, true, true, 100, false, true, true, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120_000,
);
