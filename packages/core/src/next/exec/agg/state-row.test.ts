/** Eviction preserves transition fields, not finalized values or recombined partials. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, beforeAll, afterAll } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { Governor } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import { vectorOf, DBL, INT, BIG, ANYNUM, TEXT, BOOL, type Value } from "../join-fixtures.js";
import { valueAt, sqlLiteral, decimals } from "../kernels/test-support.js";
import { PageWriter, decodePage } from "../spill-pages.js";
import { accumulatorFor } from "./states.js";
import { StateRow, StateReader } from "./state-row.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("aggregate-state-raw-float-bits", [0xb4c1, 0xb4c2]))(
  "native checkpoints preserve every float bit, including NaN payloads (seed %i)",
  async (seed) => {
    const reach = createReach("native transition bits", [
      "NaN payload",
      "negative zero",
      "zero charges",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.bigInt({ min: 0n, max: (1n << 51n) - 1n }), async (payload) => {
        reach.beginHistory();
        const governor = new Governor(8 << 20),
          grant = await governor.admit({
            label: "raw float snapshot",
            minimumBytes: 1 << 20,
            spillable: false,
          }),
          writer = new PageWriter(grant),
          row = new StateRow(grant),
          values = new Float64Array(3),
          raw = new BigUint64Array(values.buffer);
        raw[0] = 1n << 63n;
        raw[1] = 0x7ff8_0000_0000_0000n | payload;
        raw[2] = 0xfff8_0000_0000_0000n | payload;
        try {
          for (let i = 0; i < values.length; i++) row.float64(values, i);
          const page = writer.encode(
            { queryId: 1, run: 0, partition: 0, sequence: 0 },
            row.columns,
            1,
          );
          if (!page) throw new Error("Unfunded raw float codec");
          const decoded = decodePage(
            page,
            row.columns.map((column) => column.type),
            grant,
          );
          if ("needed" in decoded) throw new Error("Unfunded raw float decode");
          try {
            const actual = new Float64Array(3),
              reader = new StateReader(decoded.columns, 0);
            for (let i = 0; i < actual.length; i++) reader.float64(actual, i);
            expect(new BigUint64Array(actual.buffer)).toEqual(raw);
          } finally {
            grant.free(decoded.charged);
          }
          reach.reach("negative zero");
          reach.reach("NaN payload");
        } finally {
          row.close();
          writer.close();
          expect(grant.allocated).toBe(0);
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero charges");
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
);
const bits = (n: number) => {
  const d = new DataView(new ArrayBuffer(8));
  d.setFloat64(0, n);
  return d.getBigUint64(0).toString(16);
};
const cases: ReadonlyArray<{
  fn: string;
  input: PhysicalType;
  output: PhysicalType;
  kind: string;
}> = [
  { fn: "count", input: DBL, output: BIG, kind: "count" },
  { fn: "sum", input: DBL, output: DBL, kind: "float sum" },
  { fn: "avg", input: DBL, output: DBL, kind: "float AVG Sxx" },
  { fn: "min", input: DBL, output: DBL, kind: "float extrema" },
  { fn: "sum", input: BIG, output: ANYNUM, kind: "integer carry" },
  { fn: "avg", input: INT, output: DBL, kind: "integer AVG" },
  { fn: "sum", input: ANYNUM, output: ANYNUM, kind: "wide NUMERIC" },
  { fn: "avg", input: ANYNUM, output: ANYNUM, kind: "NUMERIC AVG" },
  { fn: "min", input: ANYNUM, output: ANYNUM, kind: "numeric extrema" },
  { fn: "max", input: BIG, output: BIG, kind: "int64 extrema" },
  { fn: "min", input: TEXT, output: TEXT, kind: "text extrema bytes" },
  { fn: "bool_and", input: BOOL, output: BOOL, kind: "boolean" },
];
it.each(seedsFor("aggregate-state-eviction-bits", [0xb4b7, 0xb4b8]))(
  "restores before continuation and agrees bit-exactly with uninterrupted state (seed %i)",
  async (seed) => {
    const reach = createReach("aggregate state eviction", [
      ...cases.map((c) => c.kind),
      "signed zero",
      "PG exact",
      "charged codec",
      "all charges returned",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 12, max: 30 }), async (split) => {
        reach.beginHistory();
        for (const scenario of cases) {
          const gov = new Governor(16 << 20),
            grant = await gov.admit({
              label: "transition roundtrip",
              minimumBytes: 8 << 20,
              spillable: true,
            });
          const a = accumulatorFor(scenario.fn, scenario.output, scenario.input, grant),
            b = accumulatorFor(scenario.fn, scenario.output, scenario.input, grant),
            writer = new PageWriter(grant);
          const values: Value[] = Array.from({ length: 64 }, (_, i) =>
            i % 13 === 1
              ? null
              : scenario.input.kind === "text"
                ? `text-${"x".repeat((i * 31) % 71)}-${String(i)}`
                : scenario.input.kind === "bool"
                  ? i % 5 !== 0
                  : scenario.input.kind === "decimal"
                    ? i % 3 === 0
                      ? 10n ** 45n
                      : i % 3 === 1
                        ? -(10n ** 45n)
                        : BigInt(i)
                    : scenario.input.kind === "int64"
                      ? i % 2 === 0
                        ? (1n << 62n) + BigInt(i)
                        : -((1n << 62n) + BigInt(i))
                      : scenario.input.kind === "int32"
                        ? i * 123456
                        : i % 3 === 0
                          ? -0
                          : i % 4 === 0
                            ? 1e16
                            : i % 4 === 1
                              ? 1
                              : -1e16,
          );
          const vector =
              scenario.input.kind === "decimal"
                ? decimals(
                    scenario.input,
                    values.map((v, i) => (v === null ? null : ([BigInt(v), i % 4] as const))),
                    "wide",
                  )
                : vectorOf(scenario.input, values, { int64: "int64" }),
            gids = Int32Array.from(values, (_, i) => i % 3),
            prefix = Uint16Array.from({ length: split }, (_, i) => i),
            tail = Uint16Array.from({ length: 64 - split }, (_, i) => i + split);
          try {
            a.grow(4);
            b.grow(4);
            a.update(vector, gids, prefix, prefix.length, false);
            for (let group = 0; group < 4; group++) {
              const row = new StateRow(grant);
              try {
                if (!a.save || !b.restore) throw new Error("Missing state serializer");
                a.save(group, row);
                const page = writer.encode(
                  { queryId: 1, run: 0, partition: group, sequence: 0 },
                  row.columns,
                  1,
                );
                if (!page) throw new Error("Unfunded snapshot page");
                const decoded = decodePage(
                  page,
                  row.columns.map((c) => c.type),
                  grant,
                );
                if ("needed" in decoded) throw new Error("Unfunded snapshot decode");
                try {
                  const reader = new StateReader(decoded.columns, 0);
                  b.restore(group, reader);
                  expect(reader.consumed).toBe(row.columns.length);
                } finally {
                  grant.free(decoded.charged);
                }
              } finally {
                row.close();
              }
            }
            if (a.floatTransition && b.floatTransition)
              for (let g = 0; g < 4; g++)
                expect(b.floatTransition(g).map(bits)).toEqual(a.floatTransition(g).map(bits));
            a.update(vector, gids, tail, tail.length, false);
            b.update(vector, gids, tail, tail.length, false);
            const left = a.emit(0, 4),
              right = b.emit(0, 4);
            for (let g = 0; g < 4; g++) expect(valueAt(right, g)).toEqual(valueAt(left, g));
            if (scenario.fn === "sum" && scenario.input.kind === "float64") {
              expect(Object.is(valueAt(right, 0), -0)).toBe(true);
              reach.reach("signed zero");
            }
            if (
              scenario.input.kind === "float64" &&
              (scenario.fn === "sum" || scenario.fn === "avg")
            ) {
              for (let g = 0; g < 3; g++) {
                const selected = values.filter((_, i) => i % 3 === g);
                const query = await pg.query<{ v: string }>(
                  `SELECT ${scenario.fn}(v)::text v FROM (VALUES ${selected.map((v) => `(${v === null ? "NULL::float8" : sqlLiteral("float8", Number(v))})`).join(",")}) t(v)`,
                );
                expect(bits(Number(query.rows[0]?.v))).toBe(bits(Number(valueAt(right, g))));
              }
              reach.reach("PG exact");
            }
            reach.reach(scenario.kind);
            reach.reach("charged codec");
          } finally {
            a.close();
            b.close();
            writer.close();
            expect(grant.allocated).toBe(0);
            grant.close();
          }
          expect(gov.usedBytes).toBe(0);
          reach.reach("all charges returned");
        }
      }),
      { seed, numRuns: 5 },
    );
    reach.assertReached();
  },
  60000,
);
