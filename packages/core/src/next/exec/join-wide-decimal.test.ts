/** PostgreSQL NUMERIC and JOIN laws: mixed payload gathers preserve arbitrary coefficients. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { Batch, FlatVector } from "../vector/vector.js";
import {
  ANYNUM,
  BOOL,
  INT,
  canonical,
  rowsOf,
  vectorOf,
  type Row,
  type Value,
} from "./join-fixtures.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinKind } from "./join.js";
const kinds = ["inner", "left", "right", "full", "semi", "anti", "mark"] as const;
const reach = createReach("next-join-wide-decimal", [
  ...kinds,
  "wide first",
  "wide last",
  "129 bits",
  "200 bits",
  "511 bits",
  "NULL",
  "negative",
  "duplicates",
  "released",
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
function column(type: PhysicalType, values: readonly Value[]): FlatVector {
  if (type.kind !== "decimal") return vectorOf(type, values) as FlatVector;
  const validity = new Uint32Array(Math.ceil(values.length / 32)).fill(0xffffffff);
  values.forEach((v, i) => {
    if (v === null) validity[i >>> 5] = (validity[i >>> 5] ?? 0) & ~(1 << (i & 31));
  });
  return {
    form: "flat",
    type,
    length: values.length,
    validity,
    data: {
      layout: "decimal",
      scale: 0,
      scales: null,
      specials: null,
      coefficients: { repr: "wide", values: values.map((v) => BigInt(v ?? 0)) },
    },
  };
}
async function execute(
  kind: HashJoinKind,
  types: readonly PhysicalType[],
  build: readonly Row[],
  probe: readonly Row[],
  minimal = false,
) {
  const probeTypes = minimal ? [INT] : [INT, BOOL];
  const spec = {
    kind,
    probeTypes,
    buildTypes: types,
    probeKeys: [0],
    buildKeys: [0],
    nullSafe: [false],
    residual: null,
    publishes: [],
  };
  const governor = new Governor(8 << 20),
    spill = new MemorySpillService();
  const grant = await governor.admit({
    label: "mixed numeric",
    minimumBytes: hashJoinMinimumBytes(spec),
    spillable: true,
  });
  const join = new HashJoin(spec, { grant, spill, queryId: 1 });
  const rows: Row[] = [];
  const out = join.probe({
    push(batch) {
      rows.push(...rowsOf(batch));
      return "blocked";
    },
    ready: () => Promise.resolve(),
    finish: () => undefined,
    abort: () => undefined,
  });
  const batch = (ts: readonly PhysicalType[], rs: readonly Row[]): Batch => ({
    length: rs.length,
    selection: null,
    columns: ts.map((t, c) =>
      column(
        t,
        rs.map((r) => r[c] ?? null),
      ),
    ),
  });
  try {
    if (join.build.push(batch(types, build)) === "blocked") await join.build.ready();
    await join.build.finish();
    if (out.push(batch(probeTypes, probe)) === "blocked") await out.ready();
    await out.finish();
    await join.closed;
    return rows;
  } finally {
    join.abort(new Error("test complete"));
    await join.closed;
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    reach.reach("released");
  }
}
it("minimal LEFT JOIN copies the exact 2^200 coefficient", async () => {
  reach.beginHistory();
  const n = 1n << 200n;
  const expected = (
    await pg.query<{ a: number; b: number; v: string }>(
      `SELECT p.k a,b.k b,b.v::text v FROM (VALUES(1))p(k) LEFT JOIN (VALUES(1,${String(n)}::numeric))b(k,v) ON b.k=p.k`,
    )
  ).rows.map((r) => [r.a, r.b, BigInt(r.v)]);
  expect(canonical(await execute("left", [INT, ANYNUM], [[1, n]], [[1]], true))).toEqual(
    canonical(expected),
  );
});
for (const seed of seedsFor("next-join-wide-decimal", [0xd201, 0xd202]))
  for (const first of [false, true])
    for (const kind of kinds)
      it(`mixed wide ${kind}, ${first ? "first" : "last"}, seed ${String(seed)}`, async () => {
        reach.beginHistory();
        const random = mulberry32(seed);
        const values: Value[] = [
          1n << 200n,
          -(1n << 129n),
          1n << 511n,
          null,
          BigInt(Math.floor(random() * 1000)) + (1n << 200n),
        ];
        const keys: Value[] = [1, 1, 2, null, 3],
          flags: Value[] = [true, null, false, true, false];
        const types = first ? [INT, ANYNUM, BOOL] : [INT, BOOL, ANYNUM];
        const build = values.map((n, i): Row =>
          first ? [keys[i] ?? null, n, flags[i] ?? null] : [keys[i] ?? null, flags[i] ?? null, n],
        );
        const probe: Row[] = [
          [1, true],
          [2, null],
          [9, false],
          [null, true],
        ];
        const literal = (v: Value) => (v === null ? "NULL" : String(v));
        const bv = values
          .map(
            (n, i) =>
              `(${literal(keys[i] ?? null)}::int,${literal(n)}::numeric,${literal(flags[i] ?? null)}::boolean)`,
          )
          .join(",");
        const pv = probe
          .map((r) => `(${literal(r[0] ?? null)}::int,${literal(r[1] ?? null)}::boolean)`)
          .join(",");
        const selected = [
          "p.k",
          "p.z",
          ...(kind === "mark"
            ? ["p.k IN (SELECT k FROM b)"]
            : kind === "semi" || kind === "anti"
              ? []
              : first
                ? ["b.k", "b.v::text", "b.z"]
                : ["b.k", "b.z", "b.v::text"]),
        ];
        const tail =
          kind === "mark"
            ? ""
            : kind === "semi" || kind === "anti"
              ? ` WHERE ${kind === "anti" ? "NOT " : ""}EXISTS(SELECT 1 FROM b WHERE b.k=p.k)`
              : ` ${kind.toUpperCase()} JOIN b ON b.k=p.k`;
        const sql = `WITH b(k,v,z) AS (VALUES ${bv}),p(k,z) AS (VALUES ${pv}) SELECT ${selected.map((s, i) => `${s} c${String(i)}`).join(",")} FROM p${tail}`;
        const expected = (await pg.query<Record<string, Value>>(sql)).rows.map(
          (r) => Object.values(r).map((v) => (typeof v === "string" ? BigInt(v) : v)) as Row,
        );
        expect(canonical(await execute(kind, types, build, probe))).toEqual(canonical(expected));
        for (const state of [
          kind,
          first ? "wide first" : "wide last",
          "129 bits",
          "200 bits",
          "511 bits",
          "NULL",
          "negative",
          "duplicates",
        ] as const)
          reach.reach(state);
      });
