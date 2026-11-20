/** Noneligible plans must perform no S1 capability or page-dispatch work. The
 * inherited getter witnesses both discovery and an absent option read per batch;
 * the aggregate getter also witnesses discovery on residual-only reductions;
 * no clock or host-load bound is involved. PostgreSQL owns result correctness.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ENCODED_COST_REACH } from "../../testing/reach.js";
import { AggregateOperator } from "../exec/agg/aggregate.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const CASES = [
  "plain",
  "selective",
  "grouped",
  "minmax",
  "expression",
  "distinct",
  "filter",
  "numeric",
  "mixed",
  "limited",
  "residual",
  "partial residual",
] as const;
type Case = (typeof CASES)[number];
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});
function sql(mode: Case, table: string, threshold: number): string {
  switch (mode) {
    case "plain":
      return `SELECT id,i,t FROM ${table} ORDER BY id`;
    case "selective":
      return `SELECT id,i,t FROM ${table} WHERE i < ${String(threshold)} ORDER BY id`;
    case "grouped":
      return `SELECT i,COUNT(*)::text n,SUM(id)::text s FROM ${table} GROUP BY i ORDER BY i NULLS LAST`;
    case "minmax":
      return `SELECT MIN(i) AS i,MAX(t) AS t FROM ${table}`;
    case "expression":
      return `SELECT COUNT(i+1)::text n FROM ${table}`;
    case "distinct":
      return `SELECT COUNT(DISTINCT i)::text n FROM ${table}`;
    case "filter":
      return `SELECT COUNT(*) FILTER (WHERE i < ${String(threshold)})::text n FROM ${table}`;
    case "numeric":
      return `SELECT SUM(n)::text s FROM ${table}`;
    case "mixed":
      return `SELECT COUNT(*)::text c,SUM(n)::text s FROM ${table}`;
    case "limited":
      return `SELECT COUNT(*)::text n FROM (SELECT id FROM ${table} LIMIT 7) q`;
    case "residual":
      return `SELECT COUNT(*)::text n FROM ${table} WHERE id % 3 = 0`;
    case "partial residual":
      return `SELECT COUNT(i)::text n FROM ${table} WHERE i < ${String(threshold)} AND id % 3 = 0`;
  }
}
it.each(seedsFor("next-encoded-no-cost", [0xb4fd, 0xb4fe]))(
  "keeps noneligible native plans on the ordinary path (seed %i)",
  async (seed) => {
    const reach = createReach("encoded plan cost", ENCODED_COST_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...CASES),
        fc.integer({ min: 64, max: 257 }),
        fc.integer(),
        fc.boolean(),
        fc.boolean(),
        async (mode, count, salt, nullable, withTail) => {
          reach.beginHistory();
          const core = await new StorageCore(4096).open(),
            rows = Array.from({ length: count + (withTail ? 17 : 0) }, (_, id) => {
              const value = (Math.imul(id ^ salt, 0x9e37_79b1) >>> 0) % 23;
              return {
                id,
                i: nullable && id % 7 === 0 ? null : value,
                t: nullable && id % 11 === 0 ? null : `k-${String(value)}`,
                n: `${String(id - count)}.25`,
              };
            }),
            threshold = 1 + ((salt >>> 0) % 22),
            source = rows
              .map(
                (row) =>
                  `(${String(row.id)},${row.i === null ? "NULL::int4" : String(row.i)},${row.t === null ? "NULL::text" : `'${row.t}'`},${row.n}::numeric(12,2))`,
              )
              .join(","),
            query = sql(mode, "cost_input", threshold);
          let installed = false,
            probes = 0;
          const previous = Object.getOwnPropertyDescriptor(Object.prototype, "pageConsumer");
          const consume = vi.spyOn(AggregateOperator.prototype, "consumePages"),
            capability = vi.spyOn(AggregateOperator.prototype, "pageConsumer", "get");
          try {
            await core.execute(
              "CREATE TABLE cost_input (id INTEGER PRIMARY KEY,i INTEGER,t TEXT,n NUMERIC(12,2))",
            );
            await core.writeBatch("insertBatch", "cost_input", rows.slice(0, count));
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            if (withTail) await core.writeBatch("insertBatch", "cost_input", rows.slice(count));
            const expected = await pg.query(
              `WITH cost_input(id,i,t,n) AS (VALUES ${source}) ${query}`,
            );
            Object.defineProperty(Object.prototype, "pageConsumer", {
              configurable: true,
              get() {
                probes++;
                return undefined;
              },
            });
            installed = true;
            capability.mockClear();
            const actual = await core.execute(query);
            expect(probes, "noneligible capability/absent-option reads").toBe(0);
            expect(capability, "noneligible aggregate capability discovery").not.toHaveBeenCalled();
            expect(consume).not.toHaveBeenCalled();
            if (!("batches" in actual)) throw new Error("Expected SELECT rows");
            expect(oracleRows(actual)).toEqual(expected.rows);
            reach.reach(mode);
            reach.reach("no capability probes");
            reach.reach("no aggregate capability discovery");
            reach.reach("no encoded transitions");
            if (nullable) reach.reach("nullable source");
            if (withTail) reach.reach("ordinary memory tail");
            // Every history also proves that the planner still selects the encoded
            // variant for a supported reduction over the same real stored groups.
            const positive = await core.execute("SELECT COUNT(*)::text n FROM cost_input");
            if (!("batches" in positive)) throw new Error("Expected COUNT rows");
            expect(oracleRows(positive)).toEqual([{ n: String(rows.length) }]);
            expect(consume).toHaveBeenCalled();
            reach.reach("eligible reduction remains encoded");
          } finally {
            if (installed) {
              if (previous) Object.defineProperty(Object.prototype, "pageConsumer", previous);
              else Reflect.deleteProperty(Object.prototype, "pageConsumer");
            }
            consume.mockRestore();
            capability.mockRestore();
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("complete cleanup");
        },
      ),
      {
        seed,
        numRuns: 42,
        examples: CASES.map((mode, index): [Case, number, number, boolean, boolean] => [
          mode,
          257,
          index,
          true,
          index % 2 === 0,
        ]),
      },
    );
    reach.assertReached();
  },
  180_000,
);
