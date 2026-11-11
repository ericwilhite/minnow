/** PostgreSQL join multiplicity and NULL semantics survive native phase retirement. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, it, expect } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RETAINED_JOIN_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinSpec } from "./join.js";
import type { JoinCheckpoint } from "./join-checkpoint.js";
import { INT, TEXT, batchesOf, CollectSink, residualOf, type Row } from "./join-fixtures.js";
import type { Operator } from "./pipeline.js";
const reach = createReach("join checkpoints", RETAINED_JOIN_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
async function push(op: Operator, rows: readonly Row[]) {
  for (const b of batchesOf([INT, TEXT], rows, { batchRows: 7, dictionary: true, skip: 3 }))
    if (op.push(b) === "blocked") await op.ready();
}
for (const seed of seedsFor("next-join-checkpoints", [0xb781, 0xb782]))
  it(`retains exact state across retired grants (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          n: fc.integer({ min: 4, max: 18 }),
          cutBuild: fc.boolean(),
          spill: fc.boolean(),
          keyless: fc.boolean(),
          residual: fc.boolean(),
        }),
        async ({ n, cutBuild, spill, keyless, residual }) => {
          for (const kind of ["inner", "left", "right", "full", "semi", "anti", "mark"] as const) {
            reach.beginHistory();
            const governor = new Governor(64 * 1024 * 1024),
              service = new MemorySpillService(),
              sink = new CollectSink(2);
            const noKeys = keyless && kind !== "mark" && n < 100;
            const extra = residual ? " AND (length(p.v)+length(b.v))%2=0" : "";
            const condition =
              noKeys || residual
                ? residualOf(2, (p, b) => {
                    const parity =
                      !residual || (String(p[1]).length + String(b[1]).length) % 2 === 0;
                    if (!parity) return false;
                    if (!noKeys) return true;
                    return p[0] === null || b[0] === null ? null : p[0] === b[0];
                  })
                : null;
            const spec: HashJoinSpec = {
              kind,
              probeTypes: [INT, TEXT],
              buildTypes: [INT, TEXT],
              probeKeys: noKeys ? [] : [0],
              buildKeys: noKeys ? [] : [0],
              nullSafe: noKeys ? [] : [false],
              residual: condition,
              publishes: [],
            };
            const build: Row[] = Array.from({ length: n }, (_, i) => [
              i % 5 === 0 ? null : n > 100 ? i : i % 3,
              `b${String(i)}${"x".repeat(spill ? 2048 : 2)}`,
            ]);
            const probe: Row[] = Array.from({ length: n }, (_, i) => [
              i % 4 === 0 ? null : n > 100 ? i : i % 4,
              `p${String(i)}`,
            ]);
            await pg.exec(
              "DROP TABLE IF EXISTS b,p; CREATE TABLE b(k int,v text); CREATE TABLE p(k int,v text)",
            );
            for (const [table, rows] of [
              ["b", build],
              ["p", probe],
            ] as const) {
              const placeholders = rows
                .map((_, i) => `($${String(i * 2 + 1)},$${String(i * 2 + 2)})`)
                .join(",");
              await pg.query(`INSERT INTO ${table} VALUES ${placeholders}`, rows.flat());
            }
            const sql =
              kind === "semi" || kind === "anti"
                ? `SELECT p.k,p.v FROM p WHERE ${kind === "anti" ? "NOT " : ""}EXISTS(SELECT 1 FROM b WHERE b.k=p.k${extra})`
                : kind === "mark"
                  ? `SELECT p.k,p.v,p.k IN (SELECT b.k FROM b${residual ? " WHERE (length(p.v)+length(b.v))%2=0" : ""}) FROM p`
                  : `SELECT p.k,p.v,b.k,b.v FROM p ${kind.toUpperCase()} JOIN b ON p.k=b.k${extra}`;
            const expected = (await pg.query(sql, [], { rowMode: "array" })).rows;
            const busy = await governor.admit({ label: "busy", minimumBytes: 0, spillable: true });
            const pending: { saved: JoinCheckpoint | undefined } = { saved: undefined };
            let spilledPrefix = false;
            let cuts = 0;
            const make = async (checkpoint?: JoinCheckpoint) => {
              const grant = await governor.admit({
                label: "join",
                minimumBytes: hashJoinMinimumBytes(spec),
                spillable: true,
              });
              const join = new HashJoin(spec, { grant, spill: service, queryId: 1 }, checkpoint);
              expect(typeof join.checkpoint, "native join continuation capability").toBe(
                "function",
              );
              join.probe(sink);
              return join;
            };
            let join = await make();
            if (spill)
              expect(
                busy.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
            const rotate = async () => {
              if (cuts++ === 0 && join.profile.spilledBytes === 0) reach.reach("resident");
              spilledPrefix ||= join.profile.spilledBytes > 0;
              if (join.profile.spilledBytes > 0) reach.reach("spilled");
              pending.saved = await join.checkpoint();
              expect(pending.saved.buildComplete).toBe(!cutBuild);
              join.abort(new Error("retire"));
              await join.closed;
              join = await make(pending.saved);
              await join.restore(pending.saved);
              pending.saved = undefined;
            };
            try {
              if (cutBuild) {
                await push(join.build, build.slice(0, 2));
                await rotate();
                await push(join.build, build.slice(2, 3));
                await rotate();
                await push(join.build, build.slice(3));
                await join.build.finish();
                await push(join.probe(sink), probe);
                reach.reach("build cut");
              } else {
                await push(join.build, build);
                await join.build.finish();
                await push(join.probe(sink), probe.slice(0, 2));
                await rotate();
                await push(join.probe(sink), probe.slice(2, 3));
                await rotate();
                await push(join.probe(sink), probe.slice(3));
                reach.reach("probe cut");
              }
              await join.finishProbe();
              await join.closed;
              const canonical = (rows: readonly unknown[]) =>
                rows.map((r) => JSON.stringify(r)).sort();
              expect(canonical(sink.rows)).toEqual(canonical(expected));

              if (n > 100) expect(spilledPrefix).toBe(true);
              reach.reach("multiple cuts");
              reach.reach("NULL keys");
              reach.reach("duplicates");
              if (noKeys) reach.reach("keyless residual");
              if (residual) reach.reach("ON residual");
              if (kind === "right" || kind === "full") reach.reach("right/full match bits");
            } finally {
              join.abort(new Error("cleanup"));
              await join.closed;
              await pending.saved?.close();
              busy.close();
              expect(governor.usedBytes).toBe(0);
              expect(service.bytesInUse).toBe(0);
              reach.reach("zero resources");
            }
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [{ n: 9, cutBuild: false, spill: false, keyless: true, residual: true }],
          [{ n: 2000, cutBuild: false, spill: true, keyless: false, residual: false }],
          [{ n: 9, cutBuild: false, spill: false, keyless: false, residual: false }],
          [{ n: 9, cutBuild: true, spill: true, keyless: false, residual: false }],
        ],
      },
    );
  }, 120000);
