/** Wide single-row histories build identity mappings per physical schema, not per effect. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, REPLAY_MAPPING_WORK_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { batch, ints, texts } from "../write/test-support.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("replay-mapping-work", [47405, 47406]))
  it(`bounds mapping builds across wide single-row commits seed ${String(seed)}`, async () => {
    const reach = createReach("replay identity work", REPLAY_MAPPING_WORK_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          tables: fc.integer({ min: 1, max: 3 }),
          width: fc.integer({ min: 2, max: 64 }),
          commits: fc.integer({ min: 4, max: 12 }),
          alter: fc.boolean(),
          unrelated: fc.boolean(),
        }),
        async ({ tables, width, commits, alter, unrelated }) => {
          reach.beginHistory();
          reach.reach(tables === 1 ? "one table" : "multiple tables");
          reach.reach(width >= 32 ? "wide" : "narrow");
          reach.reach(alter ? "two physical versions" : "unchanged schema");
          reach.reach(unrelated ? "unrelated DDL" : "no unrelated DDL");
          let release!: () => void;
          const gate = new Promise<void>((resolve) => {
            release = resolve;
          });
          const core = new StorageCore(1024);
          Object.assign(core.harness, {
            tuning: {
              merges: { mode: "off" },
              beforeFlushPublish: async () => {
                const owner = core.harness.path;
                await gate;
                if (owner.closed) throw new Error("test cut cancels the parked fold");
              },
            },
          });
          await core.open();
          try {
            const fields = Array.from({ length: width }, (_, c) => `v${String(c)}`);
            const expected: Array<Array<Record<string, number | string>>> = [];
            for (let t = 0; t < tables; t++) {
              await core.execute(
                `CREATE TABLE t${String(t)}(id INT PRIMARY KEY,${fields.map((n) => `${n} TEXT`).join(",")})`,
              );
              expected.push([]);
            }
            for (let phase = 0; phase < 2; phase++) {
              if (phase === 1) {
                if (unrelated) await core.execute("CREATE TABLE unrelated(v INT)");
                if (alter)
                  for (let t = 0; t < tables; t++)
                    await core.execute(
                      `ALTER TABLE t${String(t)} ADD COLUMN extra TEXT DEFAULT 'frozen'`,
                    );
              }
              for (let r = 0; r < commits; r++)
                for (let t = 0; t < tables; t++) {
                  const id = phase * commits + r;
                  const values = fields.map((_, c) => `${String(t)}:${String(id)}:${String(c)}`);
                  const row = Object.fromEntries(fields.map((n, c) => [n, values[c] ?? ""]));
                  expected[t]?.push({
                    id,
                    ...row,
                    ...(alter ? { extra: phase ? "after" : "frozen" } : {}),
                  });
                  await core.write(async (s) => {
                    const table = s.catalog.tableByName(`t${String(t)}`);
                    if (!table) throw new Error("missing replay table");
                    await s
                      .table(table.id)
                      .insert(
                        batch(
                          ints([id]),
                          ...values.map((v) => texts([v])),
                          ...(phase === 1 && alter ? [texts(["after"])] : []),
                        ),
                        null,
                      );
                  });
                }
            }
            // Keep the WAL-only physical versions until close cancels the parked fold.
            const closing = core.harness.path.close();
            release();
            await closing;
            await core.reopen();
            const costs = core.harness.path.maintenance;
            const distinct = tables * (alter ? 2 : 1);
            expect(costs.replayMappingBuilds, "one mapping build per table/physical schema").toBe(
              distinct,
            );
            expect(costs.replayMappingHits).toBe(tables * commits * 2 - distinct);
            reach.reach("mapping reuse bound");
            expect(costs.replayMappingPeakBytes).toBeGreaterThan(0);
            expect(costs.replayMappingBytes).toBe(0);
            expect(costs.replayMappingEvictions).toBe(0);
            reach.reach("charged then released");
            for (let t = 0; t < tables; t++) {
              const rows = await core.execute(`SELECT * FROM t${String(t)} ORDER BY id`);
              if (!("batches" in rows)) throw new Error("missing replay rows");
              expect(oracleRows(rows)).toEqual(expected[t]);
            }
            reach.reach("exact replay values");
          } finally {
            release();
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
            reach.reach("zero retained");
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [{ tables: 2, width: 64, commits: 12, alter: false, unrelated: true }],
          [{ tables: 1, width: 128, commits: 12, alter: true, unrelated: false }],
          [{ tables: 3, width: 4, commits: 4, alter: false, unrelated: false }],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
