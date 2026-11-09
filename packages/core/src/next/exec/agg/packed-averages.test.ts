/** PostgreSQL float8_accum is the authority for all three transition fields. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import {
  F64,
  UNCHARGED,
  numbers,
  dictionary,
  sqlLiteral,
  valueAt,
} from "../kernels/test-support.js";
import type { Batch, Vector } from "../../vector/vector.js";
import { IDENTITY } from "../kernels/vectors.js";
import { PackedAverages } from "./packed-averages.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function need<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new Error("Missing transition fixture entry");
  return value;
}
type Cell = number | null;
async function transition(values: readonly Cell[]): Promise<readonly number[] | string> {
  const sql = `WITH RECURSIVE raw(ord,x) AS (VALUES ${values
    .map((v, i) => `(${String(i)},${sqlLiteral("float8", v)})`)
    .join(",")}),
    inp AS (SELECT row_number() OVER (ORDER BY ord) AS rn,x FROM raw WHERE x IS NOT NULL),
    walk(rn,s) AS (SELECT 0::bigint,ARRAY[0,0,0]::float8[] UNION ALL
      SELECT inp.rn,float8_accum(walk.s,inp.x) FROM walk JOIN inp ON inp.rn=walk.rn+1)
    SELECT s::text AS s FROM walk ORDER BY rn DESC LIMIT 1`;
  try {
    const result = await pg.query<{ s: string }>(sql);
    return need(result.rows, 0).s.slice(1, -1).split(",").map(Number);
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error)) throw error;
    return String(error.code);
  }
}

it.each(seedsFor("packed-averages", [0xb461, 0xb462]))(
  "preserves PG transitions through mask changes and exceptional batches (seed %i)",
  async (seed) => {
    const reach = createReach("packed AVG transitions", [
      "dense",
      "nullable",
      "constant",
      "dictionary",
      "special",
      "overflow",
      "promotion",
      "grown",
      "sxx",
      "independent-pg",
      "resume",
      "magnitude-boundary",
      "interleaved",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: -100, max: 100 }), { minLength: 12, maxLength: 12 }),
        fc.boolean(),
        async (base, selected) => {
          reach.beginHistory();
          for (const mode of [
            "dense",
            "nullable",
            "constant",
            "dictionary",
            "special",
            "boundary",
            "overflow",
          ] as const) {
            const bank = new PackedAverages(F64, UNCHARGED);
            bank.grow(1);
            const prefix = Array.from({ length: 5 }, (_, c) =>
              numbers(
                F64,
                base.map((v) => v * (c + 1)),
              ),
            );
            const gids = new Int32Array(2048);
            bank.updateBatch(
              { columns: prefix, length: base.length, selection: null },
              [0, 1, 2, 3, 4],
              gids,
              IDENTITY,
              base.length,
            );
            expect(bank.fusedRows).toBe(base.length);
            reach.reach("dense");
            const other = new PackedAverages(F64, UNCHARGED);
            other.grow(1);
            other.updateBatch(
              {
                columns: Array.from({ length: 5 }, () => numbers(F64, [10, 10, 10, 10])),
                length: 4,
                selection: null,
              },
              [0, 1, 2, 3, 4],
              gids,
              IDENTITY,
              4,
            );
            const count = 9;
            let columns: Vector[];
            if (mode === "constant")
              columns = prefix.map((_, c) => ({
                form: "constant",
                type: F64,
                length: count,
                value: numbers(F64, [c === 2 ? null : c - 2]),
              }));
            else if (mode === "dictionary")
              columns = prefix.map((_, c) =>
                dictionary(
                  numbers(F64, [3 + c, -7 - c, 11 + c]),
                  Array.from({ length: count }, (_, i) => (i + c) % 3),
                ),
              );
            else
              columns = prefix.map((_, c) =>
                numbers(
                  F64,
                  Array.from({ length: count }, (_, i) => {
                    if (mode === "nullable" && (i + c) % 3 === 0) return null;
                    if (mode === "special" && i === 4)
                      return [NaN, Infinity, -Infinity, -0, 0][c] ?? 0;
                    if (mode === "overflow") return i % 2 === 0 ? 1e154 : -1e154;
                    if (mode === "boundary")
                      return (i % 2 ? -1 : 1) * 2 ** 400 * (c === 4 ? 1 + Number.EPSILON : 1);
                    return (base[i] ?? 0) * (c + 1);
                  }),
                ),
              );
            const rows = selected ? Uint16Array.from([0, 2, 4, 6, 8]) : IDENTITY;
            const n = selected ? 5 : count;
            const batch: Batch = {
              columns,
              length: count,
              selection: selected ? { rows, count: n } : null,
            };
            bank.grow(4);
            reach.reach("grown");
            let failed: string | undefined;
            try {
              bank.updateBatch(batch, [0, 1, 2, 3, 4], gids, rows, n);
            } catch (error) {
              if (!(error instanceof Error) || !("sqlState" in error)) throw error;
              failed = String(error.sqlState);
            }
            const tail = prefix.map((_, c) => numbers(F64, [c + 1, c - 2, 0, -0]));
            if (failed === undefined) {
              bank.updateBatch(
                { columns: tail, length: 4, selection: null },
                [0, 1, 2, 3, 4],
                gids,
                IDENTITY,
                4,
              );
              reach.reach("resume");
            }
            let pgFailed = false;
            for (let c = 0; c < 5; c++) {
              const col = need(columns, c);
              const input: Cell[] = base.map((v) => v * (c + 1));
              for (let k = 0; k < n; k++) input.push(valueAt(col, rows[k] ?? 0) as Cell);
              for (let i = 0; i < 4; i++) input.push(valueAt(need(tail, c), i) as Cell);
              // Each group/aggregate has its OWN execution. An earlier failing
              // aggregate cannot credit reach for a later state PG never visited.
              const expected = await transition(input);
              reach.reach("independent-pg");
              if (typeof expected === "string") {
                expect(expected).toBe("22003");
                pgFailed = true;
                reach.reach("overflow");
              } else if (failed === undefined) {
                expect(bank.transition(c, 0)).toEqual(expected);
                expect(bank.transition(c, 3)).toEqual([0, 0, 0]);
                const output = need(bank.members, c).emit(0, 1);
                expect(valueAt(output, 0)).toEqual(need(expected, 1) / need(expected, 0));
                if (need(expected, 2) > 0) reach.reach("sxx");
              }
            }
            expect(failed).toBe(pgFailed ? "22003" : undefined);
            if (mode === "boundary") reach.reach("magnitude-boundary");
            else if (mode !== "overflow") reach.reach(mode);
            if (mode === "nullable" || mode === "constant" || mode === "dictionary") {
              expect(bank.fusedRows).toBe(base.length);
              reach.reach("promotion");
            }
            for (let c = 0; c < 5; c++) expect(other.transition(c, 0)).toEqual([4, 40, 0]);
            reach.reach("interleaved");
            for (const member of other.members) member.close();
            for (const member of bank.members) member.close();
          }
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("packed-average-charge", [0xb463]))(
  "charges resize overlap and releases failures (seed %i)",
  (seed) => {
    const reach = createReach("packed AVG grants", [
      "before-allocation",
      "resize-overlap",
      "failure-release",
      "promotion-admission",
    ]);
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 128 }), (groups) => {
        reach.beginHistory();
        let bytes = 0;
        const cap = 11 * 8 * groups;
        const bank = new PackedAverages(F64, {
          allocate(n) {
            if (bytes + n > cap) return false;
            bytes += n;
            return true;
          },
          free(n) {
            bytes -= n;
            expect(bytes).toBeGreaterThanOrEqual(0);
          },
        });
        bank.grow(groups);
        expect(bytes).toBe(cap);
        reach.reach("before-allocation");
        expect(() => bank.grow(groups + 1)).toThrow("aggregation growth was refused");
        expect(bytes).toBe(cap);
        reach.reach("resize-overlap");
        const nulls = Array.from({ length: 5 }, () => numbers(F64, [null]));
        expect(() =>
          bank.updateBatch(
            { columns: nulls, length: 1, selection: null },
            [0, 1, 2, 3, 4],
            new Int32Array(1),
            IDENTITY,
            1,
          ),
        ).toThrow("aggregation growth was refused");
        expect(bytes).toBe(cap);
        reach.reach("promotion-admission");
        for (const member of bank.members) member.close();
        expect(bytes).toBe(0);
        reach.reach("failure-release");
      }),
      { seed, numRuns: 20 },
    );
    reach.assertReached();
  },
);
