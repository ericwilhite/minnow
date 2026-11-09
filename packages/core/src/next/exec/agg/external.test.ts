/** Real-governor pressure keeps groups exact across bounded external sort passes. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel } from "../../ir/ir.js";
import { Governor } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, type Row } from "../join-fixtures.js";
import { F64, I32, I64, col, sqlLiteral } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { drive } from "../pipeline.js";
import {
  ExternalAggregateOperator,
  externalAggregateMinimum,
  externalAggregateLoans,
} from "./external.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const input = [I32, F64, I32];
const shape = (scalar: boolean): Extract<Rel, { op: "aggregate" }> => ({
  op: "aggregate",
  id: 2,
  output: [],
  input: { op: "values", id: 1, output: [], rows: [] },
  groupBy: scalar ? [] : [0],
  groupingSets: null,
  aggregates: [
    { fn: "sum", args: [col(1, F64)], type: F64, filter: null, distinct: false, orderBy: [] },
    { fn: "avg", args: [col(1, F64)], type: F64, filter: null, distinct: false, orderBy: [] },
    { fn: "count", args: [col(2, I32)], type: I64, filter: null, distinct: true, orderBy: [] },
  ],
});

function hasCause(error: unknown, expected: Error): boolean {
  return (
    error === expected ||
    (error instanceof AggregateError &&
      error.errors.some((cause: unknown) => hasCause(cause, expected))) ||
    (error instanceof Error && error.cause !== undefined && hasCause(error.cause, expected))
  );
}

it.each(seedsFor("aggregate-external-pressure", [0xb499, 0xb49a]))(
  "spills without a partial hash transition and drains cancellation (seed %i)",
  async (seed) => {
    const reach = createReach("external aggregation pressure", [
      "scalar",
      "grouped",
      "sparse selection",
      "spill",
      "cancelled append",
      "close failure",
      "PG",
      "returned all credit",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 18000, max: 20000 }),
        fc.integer({ min: 5, max: 50 }),
        async (count, groups) => {
          reach.beginHistory();
          for (const mode of ["normal", "cancel", "close failure"] as const)
            for (const scalar of [false, true]) {
              const cancel = mode === "cancel";
              const rel = shape(scalar),
                minimum = externalAggregateMinimum(rel, input),
                slots = externalAggregateLoans(rel);
              const governor = new Governor(32 << 20);
              const parts = await governor.admitGroup({
                label: "aggregate",
                grants: [
                  { label: "minimum", minimumBytes: minimum, spillable: true },
                  ...Array.from({ length: slots }, (_, slot) => ({
                    label: `loan${String(slot)}`,
                    minimumBytes: 0,
                    spillable: true,
                  })),
                ],
              });
              const grant = parts[0];
              if (!grant) throw new Error("Missing fixed minimum");
              const filler = await governor.admit({
                label: "other pinned execution",
                minimumBytes: governor.capBytes - governor.reserveBytes - minimum,
                spillable: false,
              });
              const controller = new AbortController(),
                reason = new Error(
                  mode === "close failure"
                    ? "external spill close failed"
                    : "cancel during external aggregate spill",
                ),
                spill = new MemorySpillService();
              const arena = new OutputArena(grant, 2048),
                sink = new CollectSink(1);
              const operator = new ExternalAggregateOperator(
                rel,
                { arena, input, params: { slots: [] } },
                { grant, loans: parts.slice(1), spill, queryId: 3, signal: controller.signal },
                sink,
              );
              let files = 0;
              const create = spill.create.bind(spill);
              spill.create = async (q) => {
                files++;
                const file = await create(q),
                  append = file.append.bind(file);
                file.append = async (page) => {
                  if (cancel && !controller.signal.aborted) controller.abort(reason);
                  return append(page);
                };
                if (mode === "close failure") {
                  const close = file.close.bind(file);
                  file.close = async () => {
                    await close();
                    throw reason;
                  };
                }
                return file;
              };
              const rows: Row[] = Array.from({ length: count }, (_, i) => [
                i % groups,
                i % 3 === 0 ? 2 ** 40 : i % 3 === 1 ? -(2 ** 40) : (i % 17) / 10,
                i % 7001,
              ]);
              try {
                if (cancel) {
                  await expect(
                    drive(
                      new ArraySource(batchesOf(input, rows, { batchRows: 512, skip: 3 })),
                      operator,
                      controller.signal,
                    ),
                  ).rejects.toBe(reason);
                  expect(controller.signal.aborted).toBe(true);
                  reach.reach("cancelled append");
                } else if (mode === "close failure") {
                  const failure = await Promise.resolve(
                    drive(
                      new ArraySource(batchesOf(input, rows, { batchRows: 512, skip: 3 })),
                      operator,
                    ),
                  ).catch((e: unknown) => e);
                  expect(hasCause(failure, reason)).toBe(true);
                  reach.reach("close failure");
                } else {
                  await drive(
                    new ArraySource(batchesOf(input, rows, { batchRows: 512, skip: 3 })),
                    operator,
                  );
                  const keys = scalar ? "" : "k,",
                    by = scalar ? "" : " GROUP BY k";
                  const values = rows
                    .map(
                      (r) =>
                        `(${String(r[0])},${sqlLiteral("float8", Number(r[1]))},${String(r[2])})`,
                    )
                    .join(",");
                  // DISTINCT can change PG's physical input order for other aggregates. Judge these
                  // independently, so the float law is input-order transitions, not a presort artifact.
                  const floating = await pg.query<{ k?: number; s: string; a: string }>(
                    `SELECT ${keys}sum(v)::text s,avg(v)::text a FROM (VALUES ${values}) t(k,v,d)${by}`,
                  );
                  const distinct = await pg.query<{ k?: number; n: string }>(
                    `SELECT ${keys}count(DISTINCT d)::text n FROM (VALUES ${values}) t(k,v,d)${by}`,
                  );
                  const expected = new Map(
                    floating.rows.map((r) => [
                      r.k ?? 0,
                      [Number(r.s), Number(r.a), Number(distinct.rows.find((d) => d.k === r.k)?.n)],
                    ]),
                  );
                  const found = new Map(
                    sink.rows.map((r) => [
                      scalar ? 0 : Number(r[0]),
                      r.slice(scalar ? 0 : 1).map(Number),
                    ]),
                  );
                  expect(found).toEqual(expected);
                  reach.reach("PG");
                  reach.reach("sparse selection");
                }
                expect(files).toBeGreaterThan(0);
                reach.reach("spill");
                reach.reach(scalar ? "scalar" : "grouped");
              } finally {
                operator.abort(new Error("test cleanup"));
                await operator.closed.catch((error: unknown) => {
                  if (mode !== "close failure" || !hasCause(error, reason)) throw error;
                });
                arena.close();
                expect(parts.reduce((n, part) => n + part.allocated, 0)).toBe(0);
                for (const part of parts) part.close();
                filler.close();
              }
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
              reach.reach("returned all credit");
            }
        },
      ),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  },
  60000,
);
