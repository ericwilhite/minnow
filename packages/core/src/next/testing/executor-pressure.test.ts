/** Grant-or-spill must preserve a real stored query under several resident breakers. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import type { Batch } from "../vector/vector.js";

const MiB = 1024 * 1024;

it.each(seedsFor("next-executor-pressure", [0xc301, 0xc302]))(
  "reclaims resident breakers before refusing a stored scan (seed %i)",
  async (seed) => {
    const reach = createReach("next-executor-pressure", [
      "multiple holders",
      "asynchronous release",
      "stored rows",
      "reclaimed",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 4 }),
        fc.integer({ min: 100, max: 240 }),
        async (holders, width) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open();
          const governor = new Governor(16 * MiB);
          const grants: Grant[] = [];
          const random = mulberry32(0x1234);
          let reclaimed = 0;
          try {
            await core.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)");
            const rows = Array.from({ length: 16384 }, (_, id) => {
              const text = () =>
                Array.from({ length: width }, () =>
                  String.fromCharCode(33 + Math.floor(random() * 90)),
                ).join("");
              return { id, a: text(), b: text(), c: text() };
            });
            await core.writeBatch("insertBatch", "t", rows);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            reach.reach("stored rows");
            for (let i = 0; i < holders; i++) {
              const grant = await governor.admit({
                label: `resident${String(i)}`,
                minimumBytes: 0,
                spillable: true,
              });
              grants.push(grant);
              grant.onReclaimRequest(async () => {
                await Promise.resolve();
                const bytes = grant.allocated;
                if (bytes === 0) return;
                grant.free(bytes);
                grant.release(bytes);
                reclaimed++;
                reach.reach("asynchronous release");
              });
            }
            reach.reach("multiple holders");
            const prepared = core.prepare("SELECT * FROM t ORDER BY a, id LIMIT 10 OFFSET 15000");
            const snapshot = core.harness.path.pin();
            let pressured = false;
            const execution = prepared.execute({
              snapshot,
              statement: null,
              parameters: [],
              statementTime: 0n,
              governor,
              spill: core.spill,
              queryId: 1,
              reads: {
                open: (pages, chunk, options) => {
                  if (!pressured) {
                    pressured = true;
                    const available =
                      governor.capBytes - governor.reserveBytes - governor.usedBytes;
                    for (const grant of grants)
                      expect(grant.allocate(Math.floor(available / grants.length))).toBe(true);
                  }
                  return ChunkReader.open(pages, chunk, options);
                },
              },
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
            });
            const batches: Batch[] = [];
            try {
              for (;;) {
                const frame = await execution.results.next();
                if (frame === null) break;
                try {
                  const payload = structuredClone(frame.payload);
                  batches.push({
                    length: payload.rowCount,
                    selection: null,
                    columns: payload.columns.map((v, i) => {
                      if (v.form !== "flat") throw new Error("expected owned flat result");
                      const type = prepared.columns[i]?.type;
                      if (type === undefined) throw new Error("missing result type");
                      return { ...v, type };
                    }),
                  });
                } finally {
                  frame.release();
                }
              }
              await execution.executed;
              await execution.delivered;
              expect(oracleRows({ columns: prepared.columns, batches })).toEqual(
                [...rows]
                  .sort((x, y) => (x.a < y.a ? -1 : x.a > y.a ? 1 : x.id - y.id))
                  .slice(15000, 15010),
              );
              expect(reclaimed).toBeGreaterThan(0);
              reach.reach("reclaimed");
            } finally {
              execution.cancel(new Error("test cleanup"));
              await Promise.allSettled([execution.executed, execution.delivered]);
              snapshot.release();
            }
            expect(governor.usedBytes).toBe(grants.reduce((n, g) => n + g.held, 0));
          } finally {
            for (const grant of grants) grant.close();
            await core.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 4, examples: [[2, 100]] },
    );
    reach.assertReached();
  },
  120000,
);
