/** Normalized unsigned words retain PostgreSQL's full numeric order through stable LSD. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, INT, BIG, DBL, batchesOf, type Row } from "./join-fixtures.js";
import { TaskClock, drive } from "./pipeline.js";
import { lsdSortIds } from "./sort-lsd.js";
import { RadixScratch } from "./sort-keys.js";
import { Slice, runToEnd } from "./slices.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";

const reach = createReach("next-sort-lsd", [
  "large direct",
  "float specials",
  "wide int64",
  "asc",
  "desc",
  "first",
  "last",
  "constant digits",
  "unsigned words",
  "stable ties",
]);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});

for (const seed of seedsFor("next-sort-lsd", [0x15d, 0x641])) {
  it(`large numeric LSD vs PGlite seed ${String(seed)}`, async () => {
    const random = mulberry32(seed);
    const shift = Math.floor(random() * 19);
    const n = 70003; // cross the real branch threshold, with a partial final store chunk
    for (const type of [DBL, BIG]) {
      const isFloat = type === DBL;
      const expression = isFloat
        ? `CASE (i+${String(shift)})%19 WHEN 0 THEN NULL WHEN 1 THEN 'NaN'::float8 WHEN 2 THEN 'Infinity'::float8 WHEN 3 THEN '-Infinity'::float8 WHEN 4 THEN '-0'::float8 ELSE ((i::bigint*7919)%1009-500)::float8/7 END`
        : `CASE (i+${String(shift)})%19 WHEN 0 THEN NULL WHEN 1 THEN 4611686018427387905::bigint WHEN 2 THEN -4611686018427387905::bigint ELSE 9007199254740992::bigint+i END`;
      const data: Row[] = Array.from({ length: n }, (_, i) => {
        const p = (i + shift) % 19;
        const value =
          p === 0
            ? null
            : isFloat
              ? p === 1
                ? NaN
                : p === 2
                  ? Infinity
                  : p === 3
                    ? -Infinity
                    : p === 4
                      ? -0
                      : (((i * 7919) % 1009) - 500) / 7
              : p === 1
                ? 4611686018427387905n
                : p === 2
                  ? -4611686018427387905n
                  : 9007199254740992n + BigInt(i);
        return [i, value];
      });
      for (const direction of ["asc", "desc"] as const)
        for (const nulls of ["first", "last"] as const) {
          const expected = (
            await pg.query<{ id: number }>(
              `SELECT i AS id FROM generate_series(0,${String(n - 1)}) g(i) ORDER BY ${expression} ${direction} NULLS ${nulls}, i DESC`,
            )
          ).rows.map((r) => r.id);
          const governor = new Governor(128 << 20);
          const grant = await governor.admit({
            label: "lsd",
            minimumBytes: sortMinimumBytes([INT, type]),
            spillable: true,
          });
          const spill = new MemorySpillService();
          const sink = new CollectSink();
          const op = new SortOperator(
            {
              types: [INT, type],
              keys: [
                { column: 1, direction, nulls, collation: null },
                { column: 0, direction: "desc", nulls: "last", collation: null },
              ],
              limit: null,
              offset: 0,
              withTies: false,
            },
            { grant, spill, queryId: 1 },
            sink,
          );
          await drive(new ArraySource(batchesOf([INT, type], data, { int64: "int64" })), op);
          await op.closed;
          const actual = sink.rows.map((r) => r[0]);
          expect(actual.length).toBe(expected.length);
          const mismatch = actual.findIndex((id, i) => id !== expected[i]);
          expect({ mismatch, actual: actual[mismatch], expected: expected[mismatch] }).toEqual({
            mismatch: -1,
            actual: undefined,
            expected: undefined,
          });
          expect(op.profile.runs).toBe(0);
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.beginHistory();
          for (const state of [
            "large direct",
            isFloat ? "float specials" : "wide int64",
            direction,
            nulls,
          ] as const)
            reach.reach(state);
        }
    }
  }, 120000);

  it(`stable unsigned radix words seed ${String(seed)}`, () => {
    const random = mulberry32(seed);
    for (const n of [0, 1, 1025, 65537])
      for (const width of [1, 2, 5]) {
        const columns = Array.from({ length: width }, (_, w) =>
          Uint32Array.from({ length: n }, (_, i) =>
            w % 2 !== 0 ? 0xffffffff : i % 7 === 0 ? 0 : Math.floor(random() * 0x100000000),
          ),
        );
        const ids = new Int32Array(n);
        runToEnd(
          lsdSortIds(columns, ids, new Int32Array(n), n, new Slice(Infinity), new RadixScratch()),
        );
        const expected = Array.from({ length: n }, (_, i) => i).sort((a, b) => {
          for (const values of columns) {
            const x = values[a] ?? 0;
            const y = values[b] ?? 0;
            if (x !== y) return x < y ? -1 : 1;
          }
          return a - b;
        });
        const mismatch = ids.findIndex((id, i) => id !== expected[i]);
        expect({ mismatch, actual: ids[mismatch], expected: expected[mismatch] }).toEqual({
          mismatch: -1,
          actual: undefined,
          expected: undefined,
        });
      }
    reach.beginHistory();
    reach.reach("constant digits");
    reach.reach("unsigned words");
    reach.reach("stable ties");
  });
}

it("LSD checks the existing task clock inside long word scans", () => {
  let reads = 0;
  const values = new Proxy(new Uint32Array(32768), {
    get(target, property) {
      if (typeof property === "string" && /^\d+$/.test(property)) reads++;
      return Reflect.get(target, property, target) as unknown;
    },
  });
  const clock = new TaskClock(() => reads * 0.001);
  clock.begin();
  const job = lsdSortIds(
    [values],
    new Int32Array(32768),
    new Int32Array(32768),
    32768,
    new Slice(4, clock),
    new RadixScratch(),
  );
  expect(job.next().done).toBe(false);
  expect(reads).toBeLessThanOrEqual(4096);
  job.return();
});
