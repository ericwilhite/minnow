/** Encoded correlation groups retain their own key table while full-key probes run. */
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { createReach, MARK_GROUP_PROGRESS_REACH } from "../../testing/reach.js";
import * as keys from "../exec/join-keys.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinSpec } from "../exec/join.js";
import { INT, TEXT, ArraySource, CollectSink, batchesOf } from "../exec/join-fixtures.js";
import { drive } from "../exec/pipeline.js";
import { QueryRuntime } from "../exec/run.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { oracleRows } from "./differential-adapter.js";
import {
  PressureCampaign,
  reserveWorkingSet,
  extractKeys,
} from "./compound-progress-history.test.fixture.js";
type MarkRow = readonly [number, string, number | null];
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const canonical = (rows: readonly unknown[]) => rows.map((row) => JSON.stringify(row)).sort();
for (const seed of seedsFor("next-mark-group-compound", [0xb724, 0xb725]))
  it(`preflights both key tables (seed ${String(seed)})`, async () => {
    const campaign = new PressureCampaign();
    const history = await campaign.history(128 << 20);
    try {
      const reach = createReach("encoded MARK group progress", MARK_GROUP_PROGRESS_REACH);
      const random = mulberry32(seed),
        types = [INT, TEXT, INT];
      const spec: HashJoinSpec = {
        kind: "mark",
        probeTypes: types,
        buildTypes: types,
        probeKeys: [0, 1, 2],
        buildKeys: [0, 1, 2],
        nullSafe: [false, false, false],
        correlated: 2,
        residual: null,
        publishes: [],
      };
      for (const stored of [false, true]) {
        history.guard();
        reach.beginHistory();
        const text = "g".repeat((stored ? 5 << 20 : 800000) + Math.floor(random() * 8192));
        const build: MarkRow[] = [
          [1, text, 1],
          [1, text, 2],
          [1, text, null],
          [2, text, 1],
        ];
        const probe: MarkRow[] = [
          [1, text, 1],
          [1, text, 3],
          [1, text, null],
          [2, text, 3],
          [3, text, 1],
          [3, text, null],
        ];
        const sql =
          "SELECT p.g,p.pad,p.x,p.x IN (SELECT b.x FROM mb b WHERE b.g=p.g AND b.pad=p.pad) AS m FROM mp p";
        await pg.exec(
          "DROP TABLE IF EXISTS mb,mp; CREATE TEMP TABLE mb(g int,pad text,x int); CREATE TEMP TABLE mp(g int,pad text,x int)",
        );
        for (const [name, rows] of [
          ["mb", build],
          ["mp", probe],
        ] as const)
          for (const row of rows) {
            history.guard();
            await pg.query(`INSERT INTO ${name} VALUES ($1,$2,$3)`, [...row]);
          }
        history.guard();
        const governor = new Governor(stored ? 2 * 1024 * 1024 * 1024 : 128 << 20);
        const blocker = await governor.admit({
          label: "competing retained grant",
          minimumBytes: 0,
          spillable: true,
        });
        const closeBlocker = history.ownCleanup(() => blocker.close());
        history.guard();
        const checked = new WeakSet<keys.KeyScratch>();
        const closeObserve = history.observe(() =>
          vi.spyOn(keys, "extractKeysJob").mockImplementation(function* (...args) {
            const scratch = args[4];
            if (
              scratch.layout.types.length === 2 &&
              scratch.layout.family === "enc" &&
              !checked.has(scratch)
            ) {
              checked.add(scratch);
              const requested = governor.capBytes + 1;
              const accepted = scratch.encodedCharge.allocate(requested);
              if (accepted) scratch.encodedCharge.free(requested);
              expect(accepted).toBe(false);
              reach.reach("bounded encoder");
            }
            return yield* extractKeys(...args);
          }),
        );
        try {
          if (!stored) {
            const spill = new MemorySpillService(),
              grant = await governor.admit({
                label: "encoded MARK",
                minimumBytes: hashJoinMinimumBytes(spec),
                spillable: true,
              });
            const join = new HashJoin(spec, { grant, spill, queryId: 1, signal: history.signal }),
              sink = new CollectSink(1);
            const closeJoin = history.ownCleanup(async () => {
              join.abort(new Error("test complete"));
              await join.closed;
              await closeBlocker();
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
            });
            expect(
              blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
            ).toBe(true);
            try {
              await drive(
                new ArraySource(
                  batchesOf(types, build, { batchRows: 2, dictionary: true, skip: 2 }),
                ),
                join.build,
              );
              await drive(
                new ArraySource(batchesOf(types, probe, { batchRows: 2 })),
                join.probe(sink),
              );
              const expected = await pg.query(sql, [], { rowMode: "array" } as never);
              expect(canonical(sink.rows)).toEqual(canonical(expected.rows));
              expect(join.profile.loans).toBeGreaterThan(0);
              for (const row of sink.rows)
                reach.reach(row[3] === true ? "TRUE" : row[3] === false ? "FALSE" : "UNKNOWN");
              reach.reach("native");
            } finally {
              await closeJoin();
            }
          } else {
            const core = history.core;
            history.guard();
            let occupied = false;
            const closeHook = history.observe(() =>
              vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                this: QueryRuntime,
                ...args
              ) {
                await reserveWorkingSet.apply(this, args);
                if (this.request.governor === governor && !occupied) {
                  expect(
                    blocker.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                  occupied = true;
                }
              }),
            );
            const prepare = core.executor.prepare.bind(core.executor);
            const closeAdapter = history.observe(() =>
              vi
                .spyOn(core.executor, "prepare")
                .mockImplementation((plan) => history.wrap(prepare(plan), governor)),
            );
            try {
              for (const name of ["mb", "mp"])
                await core.execute(`CREATE TABLE ${name}(g INTEGER,pad TEXT,x INTEGER)`);
              for (const [name, rows] of [
                ["mb", build],
                ["mp", probe],
              ] as const)
                await core.writeBatch(
                  "insertBatch",
                  name,
                  rows.map((row) => ({ g: row[0], pad: row[1], x: row[2] })),
                );
              const result = await core.execute(sql);
              if (!("batches" in result)) throw new Error("Expected MARK rows");
              expect(canonical(oracleRows(result))).toEqual(canonical((await pg.query(sql)).rows));
              expect(occupied).toBe(true);
              reach.reach("stored");
            } finally {
              closeHook();
              closeAdapter();
              await closeBlocker();
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
            }
          }
          reach.reach("cleanup");
        } finally {
          closeObserve();
        }
      }
      reach.assertReached();
    } finally {
      await campaign.close();
    }
  }, 120000);
