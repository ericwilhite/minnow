/** Law: PostgreSQL leaves unordered float aggregate input order unspecified.
 * The lead-authorized tolerance is finite-only and never a determinism rule.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { createHash } from "node:crypto";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, B9_FLOAT_PROBE_REACH } from "../packages/core/src/testing/reach.js";
import { createLowerer } from "../packages/core/src/next/lower/create.js";
import { MemoryCatalog } from "../packages/core/src/next/lower/memory-catalog.js";
import { EXECUTOR_COSTS } from "../packages/core/src/next/exec/executor.js";
import {
  unorderedFloatColumns,
  compareFloatResults,
  samePlanBits,
  sameResultBits,
  rawRow,
  exactJSON,
  type RawResult,
} from "./b9-float-law.mts";
import type { Verification } from "./lib/independent-cpu.mts";
const ALL = [
  "sum",
  "avg",
  "variance",
  "var_pop",
  "var_samp",
  "stddev",
  "stddev_pop",
  "stddev_samp",
];
function setup() {
  const catalog = new MemoryCatalog(),
    lowerer = createLowerer(),
    context = () => ({
      catalog: catalog.view,
      statistics: { table: () => null },
      costs: EXECUTOR_COSTS,
    });
  const ddl = lowerer.lower(
    "CREATE TABLE t(id INTEGER,k DOUBLE PRECISION,v DOUBLE PRECISION,r REAL,n NUMERIC(20,4))",
    context(),
  );
  if (ddl.kind !== "ddl" || ddl.change === null) throw new Error("scope fixture DDL");
  catalog.apply(ddl.change);
  return (sql: string) => {
    const lowered = lowerer.lower(sql, context());
    if (lowered.kind !== "query") throw new Error("scope query");
    return unorderedFloatColumns(lowered.plan.root);
  };
}
export function verification(
  values: ReadonlyArray<readonly unknown[]>,
  kinds: readonly string[],
  eligible: readonly boolean[],
  planIdentity: string,
): Verification {
  const rows = values.map((row) => rawRow(row, kinds)),
    digest = createHash("sha256").update(rows.join("\n")).digest("hex"),
    raw: RawResult = {
      version: 1,
      schema: kinds.map((kind, i) => ({ name: `c${String(i)}`, kind, type: exactJSON({ kind }) })),
      eligible,
      rows,
      planIdentity,
    };
  return {
    fingerprint: digest,
    determinismFingerprint: digest,
    rows: rows.length,
    detail: { raw },
  };
}
it("certifies only explicit unordered floating aggregate output lineage", () => {
  const mask = setup();
  for (const column of ["v", "r"]) {
    expect(
      mask(`SELECT ${ALL.map((fn) => `${fn}(${column}) AS x_${fn}`).join(",")} FROM t`),
    ).toEqual([true, true, ...ALL.slice(2).map(() => false)]);
    // The current parser refuses ordered SUM/AVG and expands variance/stddev.
    // Refused SQL stays an error; arithmetic expansions remain conservative exact.
    expect(() => mask(`SELECT sum(${column} ORDER BY id) FROM t`)).toThrow();
  }
  expect(
    mask(
      "SELECT k,sum(v) AS s,count(*) AS n,max(v) AS m FROM t GROUP BY k ORDER BY s DESC,k LIMIT 3",
    ),
  ).toEqual([false, true, false, false]);
  expect(
    mask(
      "SELECT sum(n),sum(id),avg(n),variance(n),CAST(sum(n) AS DOUBLE PRECISION),CAST(count(*) AS DOUBLE PRECISION),sum(v)+0 FROM t",
    ),
  ).toEqual([false, false, false, false, false, false, false]);
  expect(mask("SELECT sum(v) OVER (ORDER BY id) AS s FROM t")).toEqual([false]);
  expect(
    mask("SELECT a.s FROM (SELECT sum(v) AS s FROM t) a JOIN (VALUES(1::float8)) b(k) ON a.s=b.k"),
  ).toEqual([false]);
  expect(mask("SELECT s,count(*) FROM (SELECT sum(v) AS s FROM t) a GROUP BY s")).toEqual([
    false,
    false,
  ]);
  expect(mask("SELECT DISTINCT sum(v) FROM t")).toEqual([false]);
});
it("whitelists every named family and refuses ordered, integer and unknown aggregate IR", () => {
  const catalog = new MemoryCatalog(),
    lowerer = createLowerer();
  const context = () => ({
    catalog: catalog.view,
    statistics: { table: () => null },
    costs: EXECUTOR_COSTS,
  });
  const ddl = lowerer.lower("CREATE TABLE t(v DOUBLE PRECISION)", context());
  if (ddl.kind !== "ddl" || !ddl.change) throw new Error("DDL");
  catalog.apply(ddl.change);
  const result = lowerer.lower("SELECT sum(v) FROM t", context());
  if (
    result.kind !== "query" ||
    result.plan.root.op !== "project" ||
    result.plan.root.input.op !== "aggregate"
  )
    throw new Error("aggregate shape");
  const aggregate = result.plan.root.input,
    call = aggregate.aggregates[0];
  const arg = call?.args[0];
  if (!call || !arg) throw new Error("call");
  for (const fn of ALL) {
    // Synthetic IR tests the protocol whitelist independently of parser desugaring.
    const root = { ...aggregate, aggregates: [{ ...call, fn: fn as typeof call.fn }] };
    expect(unorderedFloatColumns(root)).toEqual([true]);
    expect(
      unorderedFloatColumns({
        ...root,
        aggregates: [
          {
            ...call,
            fn: fn as typeof call.fn,
            orderBy: [
              {
                expr: arg,
                direction: "asc" as const,
                nulls: "last" as const,
                collation: null,
              },
            ],
          },
        ],
      }),
    ).toEqual([false]);
  }
});
it.each(["float32", "float64"] as const)(
  "compares %s specials, keys, counts, NUMERIC and row positions exactly",
  (kind) => {
    const one = (v: unknown, id: string) => verification([[v]], [kind], [true], id);
    for (const v of [NaN, Infinity, -Infinity, -0, 0, null])
      expect(compareFloatResults(one(v, "a"), one(v, "b"))).toBe(true);
    for (const [a, b] of [
      [NaN, 0],
      [NaN, Infinity],
      [Infinity, -Infinity],
      [null, 0],
      [-0, 0],
    ])
      expect(compareFloatResults(one(a, "a"), one(b, "b"))).toBe(false);
    const kinds = ["float64", "int64", "decimal", "float64"],
      flags = [false, false, false, true],
      base = [
        [17, 3, "1.00", 1000],
        [18, 4, "2.00", 2000],
      ];
    const before = verification(base, kinds, flags, "a");
    for (const column of [0, 1, 2]) {
      const values = base.map((row) => [...row]);
      const row = values[0];
      if (!row) throw new Error("missing test row");
      row[column] = column === 0 ? 17 + 1e-12 : column === 1 ? 4 : "1.0";
      expect(compareFloatResults(before, verification(values, kinds, flags, "b"))).toBe(false);
    }
    expect(compareFloatResults(before, verification([...base].reverse(), kinds, flags, "b"))).toBe(
      false,
    );
    const drift = verification(
      [
        [17, 3, "1.00", 1000 * (1 + 1e-12)],
        [18, 4, "2.00", 2000],
      ],
      kinds,
      flags,
      "b",
    );
    expect(compareFloatResults(before, drift)).toBe(true);
    expect(sameResultBits(before, verification(base, kinds, flags, "b"))).toBe(true);
    expect(sameResultBits(before, drift)).toBe(false);
    expect(samePlanBits(before, drift)).toBe(false);
    const sameIdentity = {
      ...drift,
      detail: { raw: { ...(drift.detail as { raw: RawResult }).raw, planIdentity: "a" } },
    };
    expect(compareFloatResults(before, sameIdentity)).toBe(false);
    const nan = one(NaN, "a"),
      otherNaN = one(NaN, "b");
    const payload = (otherNaN.detail as { raw: RawResult }).raw;
    const cell = JSON.parse(payload.rows[0] ?? "") as Array<{ bits: string }>;
    const first = cell[0];
    if (!first) throw new Error("missing NaN cell");
    first.bits = kind === "float32" ? "7fc00001" : "7ff8000000000001";
    const differentPayload = {
      ...otherNaN,
      detail: { raw: { ...payload, rows: [JSON.stringify(cell)] } },
    };
    expect(compareFloatResults(nan, differentPayload)).toBe(false);
    const wrong = verification(
      [
        [17, 3, "1.00", 1000 * (1 + 1e-6)],
        [18, 4, "2.00", 2000],
      ],
      kinds,
      flags,
      "b",
    );
    expect(compareFloatResults(before, wrong)).toBe(false);
    const missing = { ...before, detail: { raw: { version: 1, error: "22003" } } };
    expect(compareFloatResults(missing, missing)).toBe(false);
  },
);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("b9-float-probe-law", [0xb9b31, 0xb9b32]))(
  "accepts legal PG accumulation orders, rejects1e-6 SUM perturbation (seed %i)",
  async (seed) => {
    const reach = createReach("B9 float probe", B9_FLOAT_PROBE_REACH),
      eligible = [false, false, ...ALL.map(() => true), false],
      kinds = ["float64", "int64", ...ALL.map(() => "float64"), "float64"];
    expect(eligible).toEqual([false, false, ...ALL.map(() => true), false]);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.option(fc.integer({ min: 1, max: 1000000 }), { nil: null }), {
          minLength: 3,
          maxLength: 32,
        }),
        async (encoded) => {
          reach.beginHistory();
          const values = encoded.map((v) => (v === null ? null : v / 1000)),
            query = (direction: string) =>
              `WITH inp AS MATERIALIZED(SELECT v,ord FROM unnest($1::float8[]) WITH ORDINALITY t(v,ord) ORDER BY ord ${direction}) SELECT 17::float8 k,count(v)::text n,${ALL.map((fn) => `${fn}(v) AS x_${fn}`).join(",")},sum(v ORDER BY ord) ordered FROM inp`,
            run = async (direction: string) => {
              const row = (
                await pg.query<Record<string, number | string | null>>(query(direction), [values])
              ).rows[0];
              if (!row) throw new Error("PG row");
              return [row.k, Number(row.n), ...ALL.map((fn) => row[`x_${fn}`]), row.ordered];
            },
            a = await run("ASC"),
            b = await run("DESC"),
            again = await run("ASC"),
            left = verification([a], kinds, eligible, "pg-a"),
            right = verification([b], kinds, eligible, "pg-b");
          expect(compareFloatResults(left, right)).toBe(true);
          expect(samePlanBits(left, verification([again], kinds, eligible, "pg-a"))).toBe(true);
          for (const fn of ALL) reach.reach(fn as (typeof B9_FLOAT_PROBE_REACH)[number]);
          reach.reach("PG orders");
          reach.reach("same-plan bits");
          expect(
            sameResultBits(left, verification([a], kinds, eligible, "native-other-source")),
          ).toBe(true);
          const oneBit = [...a];
          if (typeof oneBit[2] === "number" && Number.isFinite(oneBit[2])) {
            oneBit[2] =
              oneBit[2] === 0 ? Number.MIN_VALUE : oneBit[2] + Math.abs(oneBit[2]) * Number.EPSILON;
            expect(
              sameResultBits(left, verification([oneBit], kinds, eligible, "native-drift")),
            ).toBe(false);
          }
          reach.reach("native cross-source exact");
          reach.reach("ordered aggregate exact");
          if (encoded.includes(null)) reach.reach("NULL");
          if (a[2] !== null) {
            const changed = [...a];
            changed[2] = Number(a[2]) * (1 + 1e-6);
            expect(
              compareFloatResults(left, verification([changed], kinds, eligible, "mutant")),
            ).toBe(false);
            reach.reach("1e-6 rejected");
          }
          const key = [...a];
          key[0] = 17 + 1e-12;
          expect(compareFloatResults(left, verification([key], kinds, eligible, "key"))).toBe(
            false,
          );
          reach.reach("keys exact");
          const count = [...a];
          count[1] = Number(count[1]) + 1;
          expect(compareFloatResults(left, verification([count], kinds, eligible, "count"))).toBe(
            false,
          );
          reach.reach("counts exact");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [[100, 200, 300]],
          [[null, 100, 200, null, 300]],
          [[100, 100, 100]],
          [[null, null, null]],
        ],
      },
    );
    reach.assertReached();
  },
  180000,
);
