/** Vector metadata is authoritative; C11 integer AVG is float64, with exact SUM/COUNT inputs. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach } from "../packages/core/src/testing/reach.js";
import { layoutOf, type PhysicalType } from "../packages/core/src/next/types.js";
import { AggregateOperator } from "../packages/core/src/next/exec/agg/aggregate.js";
import { drive, type Operator } from "../packages/core/src/next/exec/pipeline.js";
import { OutputArena } from "../packages/core/src/next/exec/kernels/vectors.js";
import * as lib from "../packages/core/src/next/exec/kernels/test-support.js";
import type { Batch } from "../packages/core/src/next/vector/vector.js";
import { wideDecodedFixture } from "./lib/wide-decoded-fixture.mts";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function typeName(type: PhysicalType): string {
  switch (type.kind) {
    case "text":
      return "text";
    case "int32":
      return "int4";
    case "float64":
      return "float8";
    case "timestamp":
      return "timestamp";
    default:
      throw new Error(`unsupported fixture type ${type.kind}`);
  }
}
function literal(type: PhysicalType, value: ReturnType<typeof lib.valueAt>): string {
  const name = typeName(type);
  if (type.kind === "timestamp") {
    if (typeof value !== "bigint" || value % 1000n !== 0n)
      throw new Error("fixture timestamps must be exact milliseconds");
    return lib.sqlLiteral(
      name,
      new Date(Number(value / 1000n)).toISOString().replace("T", " ").replace("Z", ""),
    );
  }
  if (
    value !== null &&
    typeof value !== "number" &&
    typeof value !== "string" &&
    typeof value !== "bigint" &&
    typeof value !== "boolean"
  )
    throw new Error("unexpected non-scalar fixture input");
  return lib.sqlLiteral(name, value);
}
function keys(rows: unknown[][]) {
  return rows.sort((a, b) => {
    for (let c = 0; c < 3; c++) {
      if (String(a[c]) < String(b[c])) return -1;
      if (String(a[c]) > String(b[c])) return 1;
    }
    return 0;
  });
}

it.each(seedsFor("wide-decoded-fixture", [0xb451, 0xb452]))(
  "matches PG with typed results, selections and page-local dictionaries (seed %i)",
  async (seed) => {
    const reach = createReach("wide decoded fixture", [
      "single-batch",
      "cross-batch",
      "local-dictionaries",
      "nullable-count",
      "integer-average",
      "physical-layout",
      "pg-rows",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 8 }),
        fc.boolean(),
        fc.boolean(),
        async (tail, reverse, selected) => {
          reach.beginHistory();
          for (const n of [32 + tail, 2048 + tail]) {
            const fixture = wideDecodedFixture(lib, n);
            const batches: Batch[] = fixture.batches.map((batch) => ({
              ...batch,
              selection: selected
                ? {
                    rows: Uint16Array.from(
                      { length: Math.ceil(batch.length / 2) },
                      (_, i) => i * 2,
                    ),
                    count: Math.ceil(batch.length / 2),
                  }
                : null,
            }));
            if (reverse) batches.reverse();
            reach.reach(n > 2048 ? "cross-batch" : "single-batch");
            if (n > 2048) {
              const first = batches[0]?.columns[0],
                second = batches[1]?.columns[0];
              expect(first?.form).toBe("dictionary");
              expect(second?.form).toBe("dictionary");
              if (first?.form !== "dictionary" || second?.form !== "dictionary")
                throw new Error("missing local dictionaries");
              expect(first.dictionary).not.toBe(second.dictionary);
              reach.reach("local-dictionaries");
            }
            const actual: unknown[][] = [];
            const sink: Operator = {
              push(batch) {
                for (const vector of batch.columns) {
                  expect(vector.form).toBe("flat");
                  if (vector.form !== "flat") throw new Error("aggregate results must be flat");
                  expect(vector.data.layout).toBe(layoutOf(vector.type));
                }
                for (let i = 0; i < batch.length; i++)
                  actual.push(batch.columns.map((v) => lib.valueAt(v, i)));
                reach.reach("physical-layout");
                return "ready";
              },
              ready: () => Promise.resolve(),
              finish: () => undefined,
              abort: () => undefined,
            };
            const arena = new OutputArena(lib.UNCHARGED, 2048);
            const aggregate = new AggregateOperator(
              fixture.rel,
              { arena, params: { slots: [] }, input: fixture.input },
              lib.UNCHARGED,
              sink,
            );
            let index = 0;
            try {
              await drive(
                {
                  next: () => {
                    const batch = batches[index++];
                    return batch ? { batch } : null;
                  },
                  abort: () => undefined,
                },
                aggregate,
              );
            } finally {
              aggregate.abort(undefined);
              arena.close();
            }
            const values: string[] = [];
            let observedNull = false;
            for (const batch of batches)
              for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
                const row = batch.selection?.rows[k] ?? k;
                values.push(
                  "(" +
                    String(values.length) +
                    "," +
                    batch.columns
                      .map((vector, c) => {
                        const type = fixture.input[c];
                        if (!type) throw new Error("fixture type missing");
                        const value = lib.valueAt(vector, row);
                        if (c === 20 && value === null) observedNull = true;
                        return literal(type, value);
                      })
                      .join(",") +
                    ")",
                );
              }
            const aggSql = fixture.rel.aggregates.map((call) => {
              const arg = call.args[0];
              const column = arg?.op === "column" ? `c${String(arg.index)}` : null;
              if (call.fn === "count_star") return "count(*)";
              if (!column) throw new Error("direct fixture argument required");
              const input = arg?.type;
              if (call.fn === "avg" && input?.kind === "int32") {
                reach.reach("integer-average");
                return `(sum(${column})::float8/count(${column})::float8)`;
              }
              if (call.fn === "max" && input?.kind === "timestamp")
                return `round(extract(epoch from max(${column}))*1000000)::bigint`;
              return `${call.fn}(${column}${call.fn === "sum" || call.fn === "avg" ? " ORDER BY ord" : ""})`;
            });
            const names = fixture.input.map((_, c) => `c${String(c)}`);
            const sql = `WITH inp(ord,${names.join(",")}) AS (VALUES ${values.join(",")}) SELECT c0::text AS k0,c1::text AS k1,c2::text AS k2,${aggSql.map((s, i) => `(${s})::text AS a${String(i)}`).join(",")} FROM inp GROUP BY c0,c1,c2 ORDER BY c0,c1,c2`;
            const expected = (await pg.query<Record<string, string | null>>(sql)).rows.map(
              (row) => [
                row.k0,
                row.k1,
                row.k2,
                ...fixture.rel.aggregates.map((call, i) => {
                  const value = row[`a${String(i)}`];
                  if (value === null || value === undefined) return null;
                  if (call.type.kind === "int64" || call.type.kind === "timestamp")
                    return BigInt(value);
                  if (call.type.kind === "text") return value;
                  return Number(value);
                }),
              ],
            );
            expect(keys(actual)).toEqual(expected);
            reach.reach("pg-rows");
            if (observedNull) reach.reach("nullable-count");
          }
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  },
  60000,
);
