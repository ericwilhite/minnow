/** Equality-class counts must not replace the first left value's scale or zero sign on spill. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXCEPT_REPRESENTATIVE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ExceptOperator, exceptMinimumBytes } from "../exec/except.js";
import { ANYNUM, BIG, DBL, TEXT, ArraySource, vectorOf } from "../exec/join-fixtures.js";
import { drive } from "../exec/pipeline.js";
import { oracleRows } from "./differential-adapter.js";
import type { Batch, FlatVector } from "../vector/vector.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite({ parsers: { 1186: (value) => value, 20: (value) => BigInt(value) } });
  await pg.waitReady;
  await pg.exec("SET work_mem='64MB'");
}, 60_000);
afterAll(async () => {
  await pg.close();
});
interface Item {
  index: number;
  pass: number;
}
const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows
    .map((row) =>
      JSON.stringify(row, (_, v: unknown) =>
        typeof v === "bigint" ? `int8:${String(v)}` : Object.is(v, -0) ? "negative zero" : v,
      ),
    )
    .sort();
it.each(seedsFor("next-except-representative", [0xb631, 0xb632]))(
  "keeps PG representatives across spill ordering (seed %i)",
  async (seed) => {
    const reach = createReach<string>("EXCEPT representative", EXCEPT_REPRESENTATIVE_REACH);
    const examples: Array<["float" | "numeric" | "interval" | "int8", boolean, number, boolean]> =
      [];
    for (const family of ["float", "numeric", "interval", "int8"] as const)
      for (const all of [false, true])
        for (const collision of [false, true]) examples.push([family, all, 40, collision]);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("float" as const, "numeric" as const, "interval" as const, "int8" as const),
        fc.boolean(),
        fc.integer({ min: 32, max: 48 }),
        fc.boolean(),
        async (family, all, groups, collision) => {
          const integer = family === "int8";
          const numeric = family === "numeric",
            interval = family === "interval";
          reach.beginHistory();
          const type = integer
              ? BIG
              : interval
                ? { kind: "interval" as const }
                : numeric
                  ? ANYNUM
                  : DBL,
            types = [type, TEXT];
          const base: Item[] = Array.from({ length: groups }, (_, index) => ({ index, pass: 0 }));
          const left = [...base, ...base.map((item) => ({ ...item, pass: 1 }))],
            right = base.slice(0, Math.floor(groups / 2)).map((item) => ({ ...item, pass: 2 }));
          const label = (item: Item) =>
            `${integer ? "wide" : String(item.index)}:${"p".repeat(40000)}`;
          const scale = (item: Item) => (item.index + item.pass) % 3;
          const text = (item: Item) =>
            integer
              ? String(9_007_199_254_740_992n + BigInt(item.index))
              : interval
                ? ["1 month", "30 days", "720 hours"][item.pass]
                : numeric
                  ? scale(item) === 0
                    ? "1"
                    : `1.${"0".repeat(scale(item))}`
                  : item.pass === 0
                    ? "-0"
                    : "0";
          await pg.exec(
            `DROP TABLE IF EXISTS l,r;CREATE TEMP TABLE l(n ${integer ? "int8" : interval ? "interval" : numeric ? "numeric" : "float8"},t text);CREATE TEMP TABLE r(LIKE l)`,
          );
          for (const [table, rows] of [
            ["l", left],
            ["r", right],
          ] as const)
            await pg.query<Record<string, unknown>>(
              `INSERT INTO ${table} VALUES ${rows.map((_, i) => `($${String(2 * i + 1)}::${integer ? "int8" : interval ? "interval" : numeric ? "numeric" : "float8"},$${String(2 * i + 2)})`).join(",")}`,
              rows.flatMap((item) => [text(item), label(item)]),
            );
          const expected = (
            await pg.query<Record<string, unknown>>(
              `SELECT n,t FROM l EXCEPT${all ? " ALL" : ""} SELECT n,t FROM r`,
            )
          ).rows;
          const batches = (items: readonly Item[]): Batch[] => {
            const out: Batch[] = [];
            for (let from = 0; from < items.length; from += 17) {
              const part = items.slice(from, from + 17);
              let values: FlatVector = interval
                ? {
                    form: "flat",
                    type,
                    length: part.length,
                    validity: null,
                    data: {
                      layout: "interval",
                      months: Int32Array.from(part, (item) => (item.pass === 0 ? 1 : 0)),
                      days: Int32Array.from(part, (item) => (item.pass === 1 ? 30 : 0)),
                      micros: {
                        repr: "safe",
                        values: Float64Array.from(part, (item) =>
                          item.pass === 2 ? 2_592_000_000_000 : 0,
                        ),
                      },
                    },
                  }
                : (vectorOf(
                    type,
                    part.map((item) =>
                      integer
                        ? 9_007_199_254_740_992n + BigInt(item.index)
                        : numeric
                          ? 10n ** BigInt(scale(item))
                          : item.pass === 0
                            ? -0
                            : 0,
                    ),
                    { int64: "int64" },
                  ) as FlatVector);
              if (numeric) {
                if (values.data.layout !== "decimal") throw new Error("Expected decimal");
                values = {
                  ...values,
                  data: { ...values.data, scales: Uint16Array.from(part, scale) },
                };
              }
              out.push({
                length: part.length,
                selection: null,
                columns: [values, vectorOf(TEXT, part.map(label))],
              });
            }
            return out;
          };
          const minimum = exceptMinimumBytes(types),
            cap = Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7));
          const governor = new Governor(cap),
            spill = new MemorySpillService();
          const grant = await governor.admit({
            label: "except representatives",
            minimumBytes: minimum,
            spillable: true,
          });
          const rows: Array<Record<string, unknown>> = [];
          const sink = {
            push: (batch: Batch) => {
              rows.push(
                ...oracleRows({
                  columns: [
                    { name: "n", type, nullable: false },
                    { name: "t", type: TEXT, nullable: false },
                  ],
                  batches: [batch],
                }),
              );
              return "ready" as const;
            },
            ready: () => Promise.resolve(),
            finish: () => undefined,
            abort: () => undefined,
          };
          const op = new ExceptOperator(
            types,
            all,
            { grant, spill, queryId: 1, hashMask: collision ? 0 : 0xffff_ffff },
            sink,
          );
          try {
            await drive(new ArraySource(batches(left)), op.left);
            await drive(new ArraySource(batches(right)), op.right);
            await op.closed;
            expect(canonical(rows)).toEqual(canonical(expected));
            expect(op.profile.spilledBytes).toBeGreaterThan(0);
            reach.reach(
              integer
                ? "wide integer"
                : interval
                  ? "interval fields"
                  : numeric
                    ? "decimal scale"
                    : "negative zero",
            );
            reach.reach(all ? "ALL" : "DISTINCT");
            reach.reach("spill");
            if (collision) reach.reach("collision");
          } finally {
            op.abort(new Error("done"));
            await op.closed;
            grant.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("cleanup");
        },
      ),
      { seed, numRuns: examples.length + 4, examples },
    );
    reach.assertReached();
  },
  120_000,
);
