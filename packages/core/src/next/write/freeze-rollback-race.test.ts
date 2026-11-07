/** PostgreSQL rollback preserves committed rows; a flush owns its frozen inputs until settled. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_FREEZE_ROLLBACK_STATES } from "../../testing/reach.js";
import { UniqueViolationError } from "../access/modify.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "./test-support.js";

const reach = createReach("rollback during frozen input publication", WRITE_FREEZE_ROLLBACK_STATES);
for (const seed of seedsFor("next-write-freeze-rollback-race", [0xb3c5]))
  it(`preserves later inserts when rollback overlaps a frozen flush (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          cap: fc.constantFrom(4, 8, 16),
          fresh: fc.constantFrom(1, 2),
          failed: fc.boolean(),
          secondary: fc.boolean(),
          backend: fc.constantFrom("memory", "indexeddb"),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.backend);
          const trace: unknown[] = [];
          let entered!: () => void, release!: () => void;
          const ready = new Promise<void>((resolve) => {
              entered = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              release = resolve;
            });
          const h = new Harness("strict", "memory", s.cap, undefined, {
            beforeFlushPublish: async () => {
              trace.push({ event: "input encoded; publication held" });
              entered();
              await gate;
            },
          });
          if (s.backend === "indexeddb")
            Object.assign(h, {
              backend: createIndexedDbBackend({
                database: () => simIndexedDb(h.disk),
                name: "freeze-rollback",
                locks: new SimLocks(),
                memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
              }),
            });
          let p = await h.open(),
            table = 0,
            secondary = 0;
          const capture = (event: string) => {
            trace.push({
              event,
              memtables: p
                .state()
                .tables.get(table)
                ?.memtables.map((m) => ({
                  first: m.firstRowId,
                  state: m.state,
                  rows: m.rowCount,
                  encoded: m.published?.group.rowCount,
                })),
            });
          };
          const values = (ids: number[]) => batch(ints(ids), ints(ids.map(() => 7)));
          try {
            await autocommit(p, async (st) => {
              table =
                (
                  await st.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false), column("v", INT, false)]),
                  })
                ).id ?? 0;
            });
            if (s.secondary) {
              await autocommit(p, async (statement) => {
                secondary =
                  (
                    await statement.changeCatalog({
                      kind: "createIndex",
                      index: {
                        tableId: table,
                        name: "by_v",
                        columnIds: [p.catalog.table(table)?.columns[1]?.id ?? 0],
                        unique: false,
                        role: "secondary",
                      },
                    })
                  ).id ?? 0;
              });
              reach.reach("secondary index checked");
            } else reach.reach("primary only");
            const base = Array.from({ length: s.cap - 2 }, (_, k) => k);
            await autocommit(p, (st) => st.table(table).insert(values(base), null));
            const tx = await p.begin(),
              st = tx.statement({
                rules: () => ({ check: () => null, generate: () => undefined }),
              });
            try {
              // Three rows require a new memtable, leaving the old one partially full.
              await st
                .table(table)
                .insert(values(s.failed ? [100, 101, 0] : [100, 101, 102]), null);
              if (s.failed) {
                await expect(st.end()).rejects.toBeInstanceOf(UniqueViolationError);
                reach.reach("failed statement rollback");
              } else {
                await st.end();
                reach.reach("successful statement rollback");
              }
              await ready;
              capture("rollback while publication owns input");
              reach.reach("flush owns partially full input");
            } finally {
              tx.rollback();
            }
            capture("rollback complete");
            const fresh = Array.from({ length: s.fresh }, (_, k) => 200 + k);
            await autocommit(p, (statement) => statement.table(table).insert(values(fresh), null));
            capture("later rows committed");
            reach.reach(s.fresh === 1 ? "one later row" : "two later rows");
            const expected = [...base, ...fresh].map((id) => [id, 7]);
            const check = async () => {
              const view = p.pin();
              try {
                expect(
                  (await rowsOf(view, table)).sort((a, b) => Number(a[0]) - Number(b[0])),
                  JSON.stringify(trace),
                ).toEqual(expected);
                if (s.secondary) {
                  const keys = new KeyBatch({ allocate: () => true, free: () => undefined });
                  try {
                    encodeKeyBatch(
                      "equality",
                      [{ vector: ints([7]), direction: "asc", nulls: "first" }],
                      { length: 1, selection: null },
                      keys,
                    );
                    const out = {
                      probes: new Int32Array(64),
                      locators: new Float64Array(64),
                      count: 0,
                    };
                    const result = await view.index(table, secondary).probe(keys, out);
                    expect(result.done).toBe(true);
                    expect(out.count, JSON.stringify(trace)).toBe(expected.length);
                  } finally {
                    keys.close();
                  }
                }
              } finally {
                view.release();
              }
            };
            await check();
            release();
            await p.quiesce();
            capture("publication complete");
            await check();
            reach.reach("publication retains later rows");
            p = await h.reopen();
            capture("durable reopen");
            await check();
            reach.reach("durable reopen");
            await h.space.audit();
          } finally {
            release();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120_000);
it("reaches rollback/publication states", () => reach.assertReached());
