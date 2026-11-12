/** Legacy native loans obey the same one-step rule with no shared ordinary headroom. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, NATIVE_COMPOUND_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { ExceptOperator, exceptMinimumBytes } from "../exec/except.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinSpec } from "../exec/join.js";
import { INT, TEXT, ArraySource, CollectSink, batchesOf, type Row } from "../exec/join-fixtures.js";
import { drive } from "../exec/pipeline.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const canonical = (rows: readonly Row[]) => rows.map((row) => JSON.stringify(row)).sort();
it.each(seedsFor("next-native-compound-progress", [0xb722, 0xb723]))(
  "keeps large rows exact through collisions, layouts and join output (seed %i)",
  async (seed) => {
    const reach = createReach("native compound steps", NATIVE_COMPOUND_REACH);
    const random = mulberry32(seed);
    const width = 700000 + Math.floor(random() * 160000);
    for (const all of [false, true])
      for (const scale of [1, 2, 3]) {
        reach.beginHistory();
        const text = "w".repeat(width * scale);
        const left: Row[] = [
            [1, text],
            [1, text],
            [2, text + "2"],
            [3, null],
            [null, text],
          ],
          right: Row[] = [
            [1, text],
            [3, null],
            [null, text],
            [4, "z"],
          ];
        const governor = new Governor(128 << 20),
          spill = new MemorySpillService();
        const grant = await governor.admit({
          label: "EXCEPT collision",
          minimumBytes: exceptMinimumBytes([INT, TEXT]),
          spillable: true,
        });
        const sink = new CollectSink(1),
          op = new ExceptOperator(
            [INT, TEXT],
            all,
            { grant, spill, queryId: 1, hashMask: 0 },
            sink,
          );
        const blocker = await governor.admit({
          label: "competing retained grant",
          minimumBytes: 0,
          spillable: true,
        });
        expect(
          blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
        ).toBe(true);
        try {
          await drive(
            new ArraySource(
              batchesOf([INT, TEXT], left, { batchRows: 2, dictionary: true, skip: 2 }),
            ),
            op.left,
          );
          await drive(new ArraySource(batchesOf([INT, TEXT], right, { batchRows: 1 })), op.right);
          await op.closed;
          const l =
            "SELECT i, CASE WHEN i=2 THEN $1::text||'2' WHEN i=3 THEN NULL::text ELSE $1::text END AS t FROM (VALUES (1),(1),(2),(3),(NULL::int)) l(i)";
          const r =
            "SELECT i, CASE WHEN i=3 THEN NULL::text WHEN i=4 THEN 'z'::text ELSE $1::text END AS t FROM (VALUES (1),(3),(NULL::int),(4)) r(i)";
          const expected = await pg.query(
            `SELECT * FROM (${l}) l EXCEPT${all ? " ALL" : ""} SELECT * FROM (${r}) r`,
            [text],
            { rowMode: "array" } as never,
          );
          expect(canonical(sink.rows)).toEqual(canonical(expected.rows as unknown as Row[]));
          expect(op.profile.collisionPasses).toBeGreaterThan(0);
          reach.reach(all ? "ALL" : "DISTINCT");
          reach.reach(
            scale === 1
              ? "ordinary collision page"
              : scale === 2
                ? "medium collision page"
                : "wide collision page",
          );
          reach.reach("collision fallback");
          reach.reach("NULL and duplicate keys");
          reach.reach("selected dictionary");
        } finally {
          op.abort(new Error("test complete"));
          await op.closed;
          grant.close();
          blocker.close();
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
        }
      }
    for (const mode of ["encoded", "raw", "payload"] as const)
      for (const kind of ["inner", "left", "right", "full", "semi", "anti", "mark"] as const) {
        reach.beginHistory();
        const raw = mode === "raw",
          payload = mode === "payload";
        const text = "j".repeat(width * (payload && kind === "right" ? 3 : 1));
        const build: Row[] = [
            [1, text],
            [1, text],
            [3, null],
            [4, text + "4"],
          ],
          probe: Row[] =
            payload && kind === "right"
              ? []
              : [
                  [1, text],
                  [2, text],
                  [3, null],
                ];
        const spec: HashJoinSpec = {
          kind,
          probeTypes: [INT, TEXT],
          buildTypes: [INT, TEXT],
          probeKeys: payload ? [0] : raw ? [1] : [0, 1],
          buildKeys: payload ? [0] : raw ? [1] : [0, 1],
          nullSafe: raw || payload ? [false] : [false, false],
          residual: null,
          publishes: [],
          ...(kind === "mark" ? { correlated: raw || payload ? 0 : 1 } : {}),
        };
        const governor = new Governor(128 << 20),
          spill = new MemorySpillService();
        const grant = await governor.admit({
          label: "join compound",
          minimumBytes: hashJoinMinimumBytes(spec),
          spillable: true,
        });
        const op = new HashJoin(spec, { grant, spill, queryId: 2 }),
          sink = new CollectSink(1);
        const blocker = await governor.admit({
          label: "competing retained grant",
          minimumBytes: 0,
          spillable: true,
        });
        expect(
          blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
        ).toBe(true);
        try {
          await drive(
            new ArraySource(
              batchesOf([INT, TEXT], build, { batchRows: 3, dictionary: true, skip: 2 }),
            ),
            op.build,
          );
          await drive(
            new ArraySource(batchesOf([INT, TEXT], probe, { batchRows: 2 })),
            op.probe(sink),
          );
          const l =
            "SELECT i, CASE WHEN i=3 THEN NULL::text ELSE $1::text END AS t FROM (VALUES (1),(2),(3)) l(i)" +
            (payload && kind === "right" ? " WHERE FALSE" : "");
          const r =
            "SELECT i, CASE WHEN i=3 THEN NULL::text WHEN i=4 THEN $1::text||'4' ELSE $1::text END AS t FROM (VALUES (1),(1),(3),(4)) r(i)";
          const condition = payload ? "l.i=r.i" : (raw ? "" : "l.i=r.i AND ") + "l.t=r.t";
          const sql =
            kind === "mark"
              ? payload
                ? `SELECT l.*,l.i IN (SELECT r.i FROM (${r}) r) FROM (${l}) l`
                : `SELECT l.*,l.t IN (SELECT r.t FROM (${r}) r${raw ? "" : " WHERE l.i=r.i"}) FROM (${l}) l`
              : kind === "semi" || kind === "anti"
                ? `SELECT l.* FROM (${l}) l WHERE ${kind === "anti" ? "NOT " : ""}EXISTS (SELECT 1 FROM (${r}) r WHERE ${condition})`
                : `SELECT l.*,r.* FROM (${l}) l ${kind.toUpperCase()} JOIN (${r}) r ON ${condition}`;
          const expected = await pg.query(sql, [text], { rowMode: "array" } as never);
          expect(canonical(sink.rows)).toEqual(canonical(expected.rows as unknown as Row[]));
          expect(op.profile.spilledBytes).toBeGreaterThan(0);
          expect(op.profile.loans).toBeGreaterThan(0);
          reach.reach(payload ? "word key payload" : raw ? "raw key" : "encoded key");
          reach.reach(kind);
          if (payload && kind === "right") reach.reach("empty probe");
          reach.reach("backpressure");
        } catch (error) {
          throw new Error(`${kind} ${mode}`, { cause: error });
        } finally {
          op.abort(new Error("test complete"));
          await op.closed;
          blocker.close();
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
        }
      }
    reach.assertReached();
  },
  120000,
);
