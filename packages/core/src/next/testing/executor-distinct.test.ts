/** DISTINCT preserves equality classes and cannot invent a row absent from its input. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { Snapshot } from "../access/snapshot.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import { createExecutor } from "../exec/create.js";
import { numbers, I32 } from "../exec/kernels/test-support.js";
import type { ChunkReads } from "../exec/scan.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { Batch } from "../vector/vector.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-executor-distinct-columns", [0xc701, 0xc702]))(
  "preserves DISTINCT classes across column count and input emptiness (seed %i)",
  async (seed) => {
    const reach = createReach("distinct columns and cardinality", [
      "zero-empty",
      "zero-nonempty",
      "nonzero-empty",
      "nonzero-nonempty",
      "duplicates",
      "nulls",
    ]);
    const pg = new PGlite();
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.integer({ min: 0, max: 12 }),
          fc.integer({ min: 1, max: 5 }),
          fc.boolean(),
          async (columns, count, mod, nulls) => {
            reach.beginHistory();
            const output = Array.from({ length: columns }, (_, i) => ({
              name: `c${String(i)}`,
              type: I32,
              nullable: true,
            }));
            const rows = Array.from({ length: count }, (_, r) =>
              Array.from({ length: columns }, (_, c) =>
                nulls && (r + c) % 5 === 0 ? null : (r + c) % mod,
              ),
            );
            const input: Rel = {
              id: 1,
              op: "values",
              output,
              rows: rows.map((row) =>
                row.map((value) => ({
                  op: "literal" as const,
                  type: I32,
                  value: numbers(I32, [value]),
                })),
              ),
            };
            const root: Rel = { id: 2, op: "distinct", input, output };
            const plan: PhysicalPlan = {
              root,
              columns: output,
              parameters: [],
              placeholderCount: 0,
              clockSlot: null,
              initPlans: [],
              catalogVersion: 0,
              volatile: false,
            };
            // Map every empty tuple to the same constant, preserving exactly its equality classes.
            // PostgreSQL does not accept SELECT DISTINCT with an empty target list.
            const sql =
              columns === 0
                ? "SELECT DISTINCT 1 AS discard FROM generate_series(1,$1::integer)"
                : `SELECT DISTINCT ${output.map((column, c) => `${nulls ? `CASE WHEN (r+${String(c)})%5=0 THEN NULL ELSE (r+${String(c)})%${String(mod)} END` : `(r+${String(c)})%${String(mod)}`} AS ${column.name}`).join(",")} FROM generate_series(0,$1::integer-1) AS t(r)`;
            const oracle = (await pg.query<Record<string, unknown>>(sql, [count])).rows;
            const expected = columns === 0 ? oracle.map(() => ({})) : oracle;
            const governor = new Governor(8 * 1024 * 1024),
              spill = new MemorySpillService(),
              prepared = createExecutor().prepare(plan);
            const execution = prepared.execute({
              snapshot: { catalog: { version: 0 }, observedLsn: 0 } as unknown as Snapshot,
              statement: null,
              parameters: prepared.bind([]),
              statementTime: 0n,
              reads: {} as ChunkReads,
              governor,
              spill,
              queryId: 1,
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
              resultWindowBytes: 1024,
            });
            const batches: Batch[] = [];
            try {
              for (;;) {
                const frame = await execution.results.next();
                if (frame === null) break;
                try {
                  const payload = structuredClone(frame.payload);
                  batches.push({
                    length: payload.rowCount,
                    selection: null,
                    columns: payload.columns.map((v, i) => {
                      if (v.form !== "flat") throw new Error("expected flat result");
                      const type = output[i]?.type;
                      if (type === undefined) throw new Error("missing type");
                      return { ...v, type };
                    }),
                  });
                } finally {
                  frame.release();
                }
              }
              await execution.executed;
              await execution.delivered;
              const actual = oracleRows({ columns: output, batches });
              const canonical = (values: ReadonlyArray<Record<string, unknown>>) =>
                values.map((row) => JSON.stringify(row)).sort();
              expect(canonical(actual)).toEqual(canonical(expected));
            } finally {
              execution.cancel();
              await Promise.allSettled([execution.executed, execution.delivered]);
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
            }
            reach.reach(
              columns === 0
                ? count === 0
                  ? "zero-empty"
                  : "zero-nonempty"
                : count === 0
                  ? "nonzero-empty"
                  : "nonzero-nonempty",
            );
            const classes = new Set(rows.map((row) => JSON.stringify(row)));
            if (count > classes.size) reach.reach("duplicates");
            if (rows.some((row) => row.includes(null))) reach.reach("nulls");
          },
        ),
        {
          seed,
          numRuns: 40,
          examples: [
            [0, 0, 1, false],
            [0, 3, 1, false],
            [3, 0, 1, false],
            [2, 4, 1, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120000,
);

it("still consumes later input rows after the empty tuple's first representative", async () => {
  const literal = (n: number) => ({ op: "literal" as const, type: I32, value: numbers(I32, [n]) });
  const valueOutput = [{ name: "v", type: I32, nullable: false }];
  const input: Rel = {
    id: 1,
    op: "values",
    output: valueOutput,
    rows: [[literal(1)], [{ op: "call", fn: "div", type: I32, args: [literal(1), literal(0)] }]],
  };
  const projected: Rel = { id: 2, op: "project", input, exprs: [], output: [] };
  const root: Rel = { id: 3, op: "distinct", input: projected, output: [] };
  const plan: PhysicalPlan = {
    root,
    columns: [],
    parameters: [],
    placeholderCount: 0,
    clockSlot: null,
    initPlans: [],
    catalogVersion: 0,
    volatile: false,
  };
  const prepared = createExecutor().prepare(plan),
    governor = new Governor(8 * 1024 * 1024),
    spill = new MemorySpillService();
  const execution = prepared.execute({
    snapshot: { catalog: { version: 0 }, observedLsn: 0 } as unknown as Snapshot,
    statement: null,
    parameters: prepared.bind([]),
    statementTime: 0n,
    reads: {} as ChunkReads,
    governor,
    spill,
    queryId: 1,
    sequences: null,
    sequenceSession: null,
    returning: "atExecuted",
    resultWindowBytes: 1024,
  });
  const drain = async () => {
    for (;;) {
      const frame = await execution.results.next();
      if (frame === null) break;
      frame.release();
    }
    await execution.executed;
  };
  try {
    await expect(drain()).rejects.toMatchObject({ code: "MINNOW_DIVISION_BY_ZERO" });
  } finally {
    execution.cancel();
    await Promise.allSettled([execution.executed, execution.delivered]);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }
});
