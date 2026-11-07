/** PostgreSQL rows and governor rules 3-5: bound two-sided near-reserve working sets. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { INT, TEXT, canonical, runJoin, type Row, type Value } from "./join-fixtures.js";
const widths = [65535, 65536, 65537, 600000, 716800, 870400, 1048575] as const;
const kinds = ["inner", "left", "right", "full", "semi", "anti"] as const;
const sides = ["build", "probe", "both"] as const;
const reach = createReach("next-join-reserve-boundary", [
  ...widths.map(String),
  ...kinds,
  ...sides,
  "two-row reproduction",
  "NULL keys",
  "duplicate keys",
  "unmatched",
  "backpressure",
  "spill",
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
it("two 870400-byte build rows and one equally wide probe", async () => {
  reach.beginHistory();
  const width = 870400;
  const spec = {
    kind: "left",
    probeTypes: [INT, TEXT],
    buildTypes: [INT, TEXT],
    probeKeys: [0],
    buildKeys: [0],
    nullSafe: [false],
    residual: null,
    publishes: [],
  } as const;
  const ours = await runJoin(
    spec,
    [
      [0, "b".repeat(width)],
      [0, "b".repeat(width)],
    ],
    [[0, "p".repeat(width)]],
    { cap: "tight", slack: 0, batchRows: 1, blockEvery: 1 },
  );
  const expected = (
    await pg.query<Record<string, Value>>(
      `SELECT p.k a,p.v pv,b.k b,b.v bv FROM (VALUES(0,repeat('p',${String(width)})))p(k,v) LEFT JOIN (VALUES(0,repeat('b',${String(width)})),(0,repeat('b',${String(width)})))b(k,v) ON b.k=p.k`,
    )
  ).rows.map((r) => Object.values(r));
  expect(canonical(ours.rows)).toEqual(canonical(expected));
  expect(ours.governor.usedBytes).toBe(0);
  expect(ours.spill.bytesInUse).toBe(0);
  reach.reach("two-row reproduction");
});
for (const seed of seedsFor("next-join-reserve-boundary", [0xee10]))
  for (const width of widths)
    it(`boundary ${String(width)}, seed ${String(seed)}`, async () => {
      for (const side of sides)
        for (const kind of kinds) {
          reach.beginHistory();
          const bt = side !== "probe" ? [INT, TEXT] : [INT, INT],
            pt = side !== "build" ? [INT, TEXT] : [INT];
          const build: Row[] = Array.from({ length: 10 }, (_, i) => [
            i === 9 ? null : i % 3,
            side !== "probe" ? "b".repeat(width) : 42,
          ]);
          const probe: Row[] = [0, 1, 9, null].map((k) =>
            pt.length === 2 ? [k, "p".repeat(width)] : [k],
          );
          const cols = [
            "p.k",
            ...(pt.length === 2 ? ["p.v"] : []),
            ...(kind === "semi" || kind === "anti"
              ? []
              : ["b.k", side !== "probe" ? "b.v" : "b.x"]),
          ];
          const tail =
            kind === "semi" || kind === "anti"
              ? `WHERE ${kind === "anti" ? "NOT " : ""}EXISTS(SELECT 1 FROM b WHERE b.k=p.k)`
              : `${kind.toUpperCase()} JOIN b ON b.k=p.k`;
          const sql = `WITH b AS (SELECT CASE WHEN i=9 THEN NULL ELSE i%3 END k,42 x,repeat('b',${String(width)}) v FROM generate_series(0,9)i),p AS (SELECT k,repeat('p',${String(width)}) v FROM (VALUES(0),(1),(9),(NULL::int))x(k)) SELECT ${cols.map((c, i) => `${c} c${String(i)}`).join(",")} FROM p ${tail}`;
          const expected = (await pg.query<Record<string, Value>>(sql)).rows.map((r) =>
            Object.values(r),
          );
          const ours = await runJoin(
            {
              kind,
              probeTypes: pt,
              buildTypes: bt,
              probeKeys: [0],
              buildKeys: [0],
              nullSafe: [false],
              residual: null,
              publishes: [],
            },
            build,
            probe,
            { cap: "tight", slack: 0, batchRows: 1, blockEvery: 1 },
          );
          expect(canonical(ours.rows), `${kind}/${side}/${String(width)}`).toEqual(
            canonical(expected),
          );
          expect(ours.governor.usedBytes).toBe(0);
          expect(ours.spill.bytesInUse).toBe(0);
          for (const state of [
            kind,
            side,
            "NULL keys",
            "duplicate keys",
            "unmatched",
            "backpressure",
            "released",
          ] as const)
            reach.reach(state);
          reach.reach(String(width));
          if (ours.join.profile.spilledBytes > 0) reach.reach("spill");
        }
    }, 120000);
