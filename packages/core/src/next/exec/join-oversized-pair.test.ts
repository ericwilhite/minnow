/** PostgreSQL JOIN semantics; governor rules 4-5: ordinary capacity retires a progress loan. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import {
  INT,
  TEXT,
  canonical,
  residualOf,
  runJoin,
  type Row,
  type Value,
} from "./join-fixtures.js";
import { Charge, Pool } from "./spill-pool.js";
import type { HashJoinSpec } from "./join.js";

const reach = createReach("next-join-oversized-pair", [
  "850 KiB",
  "generated widths",
  "inner",
  "left",
  "full",
  "anti",
  "residual",
  "aligned",
  "NULL payload",
  "duplicate keys",
  "chunk replay",
  "loans repaid",
  "retained fixed buffers",
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
for (const seed of seedsFor("next-join-oversized-pair", [0x850, 0x851])) {
  it(`new fixed buffers do not pin a loan seed ${String(seed)}`, async () => {
    const random = mulberry32(seed);
    const base = 256 * 1024 + Math.floor(random() * 65536);
    const governor = new Governor(base + (1 << 20));
    const grant = await governor.admit({
      label: "retained fixed",
      minimumBytes: base,
      spillable: true,
    });
    const pool = new Pool(new Charge(grant), base);
    const initial = Math.floor(base * 0.6),
      step = Math.floor(base * 0.5),
      freed = Math.floor(base * 0.2);
    try {
      expect(pool.allocate(initial)).toBe(true);
      await pool.need(step, grant);
      expect(grant.loaned).toBeGreaterThan(0);
      expect(pool.allocate(step)).toBe(true);
      pool.free(freed);
      expect(grant.loaned).toBe(0);
      pool.free(initial + step - freed);
      reach.beginHistory();
      reach.reach("retained fixed buffers");
    } finally {
      pool.close();
      grant.close();
      expect(governor.usedBytes).toBe(0);
    }
  });
  for (const kind of ["inner", "left", "full", "anti"] as const)
    it(`two-sided wide ${kind}, seed ${String(seed)}`, async () => {
      const random = mulberry32(seed);
      for (const filtered of [false, true]) {
        reach.beginHistory();
        const width =
          kind === "inner" && !filtered ? 850 * 1024 : (800 + Math.floor(random() * 80)) * 1024;
        const count = 8 + Math.floor(random() * 5),
          domain = 2 + Math.floor(random() * 2);
        const build: Row[] = Array.from({ length: count }, (_, i) => [
          i % domain,
          i % 5 === 4 ? null : "b".repeat(width),
        ]);
        const probe: Row[] = [
          [0, "p".repeat(width)],
          [1, "p".repeat(width)],
          [9, null],
          [null, "null key"],
        ];
        const spec: HashJoinSpec = {
          kind,
          probeTypes: [INT, TEXT],
          buildTypes: [INT, TEXT],
          probeKeys: [0],
          buildKeys: [0],
          nullSafe: [false],
          residual: filtered ? residualOf(2, (p) => (p[0] === null ? null : p[0] === 1)) : null,
          publishes: [],
        };
        const ours = await runJoin(spec, build, probe, {
          cap: "tight",
          slack: 0,
          batchRows: 1,
          blockEvery: 1,
        });
        const sql = `WITH b AS (SELECT i%${String(domain)} k, CASE WHEN i%5=4 THEN NULL ELSE repeat('b',${String(width)}) END pad FROM generate_series(0,${String(count - 1)}) g(i)),
          p(k,pad) AS (VALUES (0,repeat('p',${String(width)})),(1,repeat('p',${String(width)})),(9,NULL),(NULL,'null key'))
          ${kind === "anti" ? "SELECT p.* FROM p WHERE NOT EXISTS (SELECT 1 FROM b WHERE p.k=b.k" : `SELECT p.*,b.* FROM p ${kind === "inner" ? "INNER" : kind.toUpperCase()} JOIN b ON p.k=b.k`}
          ${filtered ? " AND p.k=1" : ""}${kind === "anti" ? ")" : ""}`;
        const expected = await pg.query<Record<string, Value>>(sql, [], {
          rowMode: "array",
        } as never);
        expect(canonical(ours.rows)).toEqual(canonical(expected.rows as unknown as Row[]));
        expect(ours.join.profile.chunkPasses).toBeGreaterThan(1);
        expect(ours.join.profile.loans).toBeGreaterThan(0);
        expect(ours.join.profile.largestLoan).toBeLessThanOrEqual(ours.governor.reserveBytes);
        expect(ours.governor.usedBytes).toBe(0);
        expect(ours.spill.bytesInUse).toBe(0);
        for (const state of [
          kind,
          filtered ? "residual" : "aligned",
          "NULL payload",
          "duplicate keys",
          "chunk replay",
          "loans repaid",
        ] as const)
          reach.reach(state);
        reach.reach(width === 850 * 1024 ? "850 KiB" : "generated widths");
      }
    }, 120000);
}
