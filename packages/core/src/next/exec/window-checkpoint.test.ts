/** Window ancestors checkpoint before evaluation: peers stay peers and ROWS keeps input ties. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, it, expect } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RETAINED_WINDOW_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { WindowOperator, windowRetainingMinimumBytes, type WindowSpec } from "./window.js";
import { type WindowCheckpoint } from "./window-checkpoint.js";
import { INT, DBL, TEXT, batchesOf, CollectSink, type Row } from "./join-fixtures.js";
const reach = createReach("window checkpoint", RETAINED_WINDOW_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-window-checkpoints", [0xb791, 0xb792]))
  it(`restores stable window rows without restarting frames (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 20, max: 80 }),
        fc.boolean(),
        fc.boolean(),
        async (n, pressure, special) => {
          reach.beginHistory();
          const gov = new Governor(128 * 1024 * 1024),
            spill = new MemorySpillService(),
            sink = new CollectSink(2);
          const types = [INT, INT, DBL, TEXT];
          const rows: Row[] = Array.from({ length: n }, (_, i) => [
            i % 11 === 0 ? null : i % 2,
            i % 7 === 0 ? null : i % 3,
            special && i % 17 === 0
              ? NaN
              : special && i % 19 === 0
                ? Infinity
                : ([1e16, 1, -1e16, 2][i % 4] ?? 0),
            `${String(i)}${"x".repeat(pressure ? 500000 : 2)}`,
          ]);
          await pg.exec(
            "DROP TABLE IF EXISTS w;CREATE TABLE w(id int,p int,k int,v float8,t text)",
          );
          for (let i = 0; i < rows.length; i++)
            await pg.query("INSERT INTO w VALUES($1,$2,$3,$4,$5)", [i, ...(rows[i] ?? [])]);
          const expected = (
            await pg.query(
              "SELECT p,k,v,t,rank() OVER(PARTITION BY p ORDER BY k),sum(v) OVER(PARTITION BY p ORDER BY k,id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  ,sum(v) OVER(PARTITION BY p ORDER BY k),lag(k) OVER(PARTITION BY p ORDER BY k,id) FROM w ORDER BY p,k,id",
              [],
              { rowMode: "array" },
            )
          ).rows;
          const spec: WindowSpec = {
            checkpointable: true,
            types,
            partitionBy: [0],
            orderBy: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
            functions: [
              { fn: "rank", arg: null, frame: null, type: { kind: "int64" } },
              {
                fn: "sum",
                arg: 2,
                type: DBL,
                frame: {
                  unit: "rows",
                  start: { kind: "unbounded-preceding", offset: null },
                  end: { kind: "current-row", offset: null },
                  exclude: "no-others",
                },
              },
              { fn: "sum", arg: 2, type: DBL, frame: null },
              { fn: "lag", arg: 1, type: INT, frame: null },
            ],
          };
          const busy = await gov.admit({ label: "busy", minimumBytes: 0, spillable: true });
          const pending: { saved: WindowCheckpoint | undefined } = { saved: undefined };
          let spilledPrefix = false;
          const make = async (add = 0) => {
            const grant = await gov.admit({
              label: "window",
              minimumBytes: windowRetainingMinimumBytes(types, 4) + add,
              spillable: true,
            });
            return new WindowOperator(spec, { grant, spill, queryId: 1 }, sink);
          };
          let win = await make();
          if (pressure)
            expect(busy.allocate(gov.capBytes - gov.reserveBytes - gov.usedBytes)).toBe(true);
          const push = async (part: readonly Row[]) => {
            for (const b of batchesOf(types, part, { batchRows: 9, dictionary: true, skip: 3 }))
              if (win.push(b) === "blocked") await win.ready();
          };
          const rotate = async () => {
            spilledPrefix ||= win.profile.sort.runs > 0;
            if (win.profile.sort.runs > 0) reach.reach("spilled prefix");
            pending.saved = await win.checkpoint();
            win.abort(new Error("retire"));
            await win.closed;
            if (pressure) {
              const bytes = Math.min(busy.allocated, pending.saved.minimumBytes);
              busy.free(bytes);
              busy.release(bytes);
            }
            win = await make(pending.saved.minimumBytes);
            await win.restore(pending.saved);
            pending.saved = undefined;
          };
          try {
            await push(rows.slice(0, Math.floor(n / 2)));
            await rotate();
            await push(rows.slice(Math.floor(n / 2), Math.floor(n / 2) + 5));
            await rotate();
            await push(rows.slice(Math.floor(n / 2) + 5));
            await win.finish();
            await win.closed;
            expect(
              sink.rows.map((r) => r.map((v) => (typeof v === "bigint" ? Number(v) : v))),
            ).toEqual(expected);
            reach.reach("two cuts");
            reach.reach("peer groups");
            reach.reach("nonassociative floats");
            reach.reach("selected dictionary input");
            reach.reach("NULL partition/order keys");
            reach.reach("RANGE peers and lag");
            if (special) reach.reach("non-finite floats");

            if (pressure) expect(spilledPrefix).toBe(true);
          } finally {
            win.abort(new Error("cleanup"));
            await win.closed;
            await pending.saved?.close();
            busy.close();
            expect(gov.usedBytes).toBe(0);
            expect(spill.bytesInUse).toBe(0);
            reach.reach("zero resources");
          }
        },
      ),
      {
        seed,
        numRuns: 5,
        examples: [
          [30, true, false],
          [30, false, true],
        ],
      },
    );
  }, 120000);
