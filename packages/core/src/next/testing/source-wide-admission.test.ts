/** PostgreSQL rows and native byte ownership are independent of a future flush's maximum format. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SOURCE_WIDE_ADMISSION_REACH } from "../../testing/reach.js";
import { beginTask, taskClock } from "../task.js";
import { Governor } from "../memory/governor.js";
import type { FlatVector } from "../vector/vector.js";
import type { PhysicalType } from "../types.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function strings(type: PhysicalType, cells: readonly string[]): FlatVector {
  const encoder = new TextEncoder(),
    encoded = cells.map((cell) => encoder.encode(cell));
  const offsets = new Int32Array(cells.length + 1);
  let size = 0;
  for (const [i, cell] of encoded.entries()) {
    size += cell.length;
    offsets[i + 1] = size;
  }
  const bytes = new Uint8Array(size);
  for (const [i, cell] of encoded.entries()) bytes.set(cell, offsets[i] ?? 0);
  return {
    form: "flat",
    type,
    length: cells.length,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
}
describe.each(seedsFor("next-source-wide-admission", [0xb742, 0xb743]))(
  "wide source seed %i",
  (seed) => {
    for (const capMiB of [64, 16])
      it(`admits actual wide-column steps at ${String(capMiB)}MiB`, async () => {
        const reach = createReach("wide source admission", SOURCE_WIDE_ADMISSION_REACH);
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 1, max: 8 }),
            fc.integer({ min: 512, max: 43690 }),
            fc.integer({ min: 1, max: 8 }),
            fc.boolean(),
            async (columns, width, count, utf8) => {
              reach.beginHistory();
              const core = await new StorageCore().open(),
                governor = new Governor(capMiB << 20);
              try {
                const names = Array.from({ length: columns }, (_, i) => "k" + String(i));
                await core.createTable({
                  name: "wide_source",
                  columns: [
                    { name: "n", type: "number" },
                    { name: "p", type: "string" },
                    ...names.map((name) => ({
                      name,
                      type: "string" as const,
                      sqlDomain: { kind: "jsonb" as const },
                    })),
                  ],
                });
                const table = core.harness.path.catalog.tableByName("wide_source");
                if (!table) throw new Error("Missing source table");
                const data = Array.from({ length: count }, (_, n) =>
                  names.map(
                    (_, i) =>
                      `{"n": 1, "s": ${JSON.stringify(String(i) + (utf8 ? "汉" : "x").repeat(Math.floor(width / (utf8 ? 3 : 1))) + String(n))}}`,
                  ),
                );
                const vectors: FlatVector[] = [
                  {
                    form: "flat",
                    type: { kind: "float64" },
                    length: count,
                    validity: null,
                    data: {
                      layout: "f64",
                      values: Float64Array.from({ length: count }, (_, i) => i + 1),
                    },
                  },
                  strings(
                    { kind: "text" },
                    Array.from({ length: count }, (_, i) => String(i + 1)),
                  ),
                  ...names.map((_, i) =>
                    strings(
                      { kind: "json", binary: true },
                      data.map((row) => row[i] ?? "null"),
                    ),
                  ),
                ];
                await core.insertVectors(table, {
                  length: count,
                  selection: null,
                  columns: vectors,
                });
                const parameters: unknown[] = [];
                const values = data.map(
                  (row, i) =>
                    "(" +
                    [i + 1, String(i + 1), ...row]
                      .map((v, j) => {
                        parameters.push(v);
                        return (
                          "$" +
                          String(parameters.length) +
                          (j === 0 ? "::float8" : j === 1 ? "::text" : "::jsonb")
                        );
                      })
                      .join(",") +
                    ")",
                );
                const expected = (
                  await pg.query(
                    "SELECT n,p," +
                      names.map((name) => name + "::text AS " + name).join(",") +
                      " FROM(VALUES" +
                      values.join(",") +
                      ")t(n,p," +
                      names.join(",") +
                      ") ORDER BY n",
                    parameters,
                  )
                ).rows;
                let fixedMinimum = 0;
                const immediate = governor.tryAdmit.bind(governor),
                  admit = governor.admitGroup.bind(governor);
                governor.tryAdmit = (options) => {
                  if (options.label.endsWith("/fixed-buffers")) fixedMinimum = options.minimumBytes;
                  return immediate(options);
                };
                governor.admitGroup = (options) => {
                  if (options.label.endsWith("/fixed-buffers"))
                    fixedMinimum = options.grants.reduce((sum, g) => sum + g.minimumBytes, 0);
                  return admit(options);
                };
                const original = core.executor.prepare.bind(core.executor);
                core.executor.prepare = (plan) => {
                  const prepared = original(plan);
                  return {
                    ...prepared,
                    execute: (request) => prepared.execute({ ...request, governor }),
                  };
                };
                // A/A main runs vary by one result slot when a CPU slice ends at a different
                // row. Compare native allocation under the SAME task schedule, without slack.
                // Real-clock publication, FIFO pressure and cancellation live in the recipe suite.
                const now = taskClock.now;
                try {
                  taskClock.now = () => 0;
                  beginTask();
                  const queried = await core.execute(
                    "SELECT n,p," + names.join(",") + " FROM wide_source ORDER BY n",
                  );
                  if (!("batches" in queried)) throw new Error("Expected query rows");
                  expect(oracleRows(queried)).toEqual(expected);
                } finally {
                  taskClock.now = now;
                  beginTask();
                }

                expect(fixedMinimum).toBeGreaterThan(0);
                reach.reach("scheduler-normalized allocation peak");
                if (process.env.MINNOW_TRACE_SOURCE_PEAKS)
                  console.info(
                    JSON.stringify({
                      seed,
                      capMiB,
                      columns,
                      width,
                      count,
                      utf8,
                      peak: governor.peakBytes,
                      fixedMinimum,
                    }),
                  );
                reach.reach(columns >= 6 ? "six or more wide columns" : "few columns");
                reach.reach(count === 1 ? "single row" : "multiple rows");
                reach.reach(utf8 ? "UTF8" : "ASCII");
                reach.reach("exact PostgreSQL rows");
              } finally {
                await core.close();
              }
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero resources");
            },
          ),
          {
            seed,
            numRuns: 8,
            endOnFailure: true,
            examples: [
              [6, 43690, 2, false],
              [1, 512, 1, true],
              [8, 32768, 3, true],
              [2, 4096, 8, false],
            ],
          },
        );
        reach.assertReached();
      }, 120000);
  },
);
