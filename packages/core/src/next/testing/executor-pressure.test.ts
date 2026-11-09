/** Grant-or-spill must preserve a real stored query under several resident breakers. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { Snapshot } from "../access/snapshot.js";
import type { Execution } from "../exec/executor.js";
import { Governor, type Grant } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import type { Batch, FlatVector } from "../vector/vector.js";

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
            // Whole-minimum admission must reclaim before opening the first reader.
            // Fill the competing breakers before admission, then refill unused growth
            // headroom when scanning starts; neither may consume the private read step.
            const initial = governor.capBytes - governor.reserveBytes - governor.usedBytes;
            for (const grant of grants)
              expect(grant.allocate(Math.floor(initial / grants.length))).toBe(true);
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
                    const available = Math.max(
                      0,
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    );
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

it.each(seedsFor("next-read-minimum-deleted-prefix", [0xc417, 0xc418]))(
  "sizes beyond a deleted prefix before opening a LIMIT input (seed %i)",
  async (seed) => {
    const reach = createReach("read minimum live prefix", [
      "deleted-group",
      "later-group",
      "large-value",
      "rows",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 6 }),
        fc.integer({ min: 65536, max: 196608 }),
        async (width, size) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open();
          const names = Array.from({ length: width }, (_, i) => `v${String(i)}`);
          try {
            await core.execute(
              `CREATE TABLE t(id INTEGER PRIMARY KEY,${names.map((name) => `${name} TEXT`).join(",")})`,
            );
            await core.writeBatch(
              "insertBatch",
              "t",
              Array.from({ length: 8 }, (_, id) => ({
                id,
                ...Object.fromEntries(names.map((name) => [name, "small"])),
              })),
            );
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const expected = {
              id: 8,
              ...Object.fromEntries(
                names.map((name, i) => [name, String.fromCharCode(65 + i).repeat(size)]),
              ),
            };
            await core.writeBatch("insertBatch", "t", [expected]);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const def = core.harness.path.catalog.tableByName("t");
            if (!def) throw new Error("fixture table");
            await core.write(async (statement) => {
              const vector: FlatVector = {
                form: "flat",
                type: { kind: "locator", tableId: def.id },
                length: 8,
                validity: null,
                data: { layout: "f64", values: Float64Array.from({ length: 8 }, (_, i) => i) },
              };
              expect(
                await statement
                  .table(def.id)
                  .delete({ length: 8, columns: [vector], selection: null }, null),
              ).toBe(8);
            });
            const pin = core.harness.path.pin(),
              grant = await core.harness.governor.admit({
                label: "prefix fixture",
                minimumBytes: 4096,
                spillable: true,
              });
            try {
              const table = pin.table(def.id, { budget: grant });
              expect(table.segments[0]?.kind).toBe("group");
              expect(table.segments[0]?.liveRowCount).toBe(0);
              expect(table.segments.slice(1).some((segment) => segment.liveRowCount > 0)).toBe(
                true,
              );
              reach.reach("deleted-group");
              reach.reach("later-group");
            } finally {
              pin.release();
              grant.close();
            }
            const result = await core.execute("SELECT * FROM t LIMIT 1");
            if (!("batches" in result)) throw new Error("SELECT result");
            expect(oracleRows(result)).toEqual([expected]);
            reach.reach("large-value");
            reach.reach("rows");
          } finally {
            await core.close();
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [2, 65536],
          [6, 196608],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-admitted-reader-concurrency", [0xc419, 0xc41a]))(
  "several real executions keep reading when pinned holders fill the remaining cap (seed %i)",
  async (seed) => {
    const reach = createReach("admitted reader concurrency", [
      "two",
      "three",
      "pinned-input",
      "full-cap",
      "next-step",
      "rows",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 3 }),
        fc.integer({ min: 128, max: 512 }),
        async (count, width) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open(),
            governor = new Governor(64 * MiB);
          let go!: () => void,
            entered!: () => void,
            arrivals = 0,
            continuing = 0;
          const gate = new Promise<void>((resolve) => {
              go = resolve;
            }),
            ready = new Promise<void>((resolve) => {
              entered = resolve;
            });
          const executions: Execution[] = [],
            snapshots: Snapshot[] = [];
          let holder: Grant | null = null;
          const input = Array.from({ length: 4096 }, (_, id) => ({
            id,
            v: `${String(id)}:`.padEnd(width, "x"),
          }));
          try {
            await core.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)");
            await core.writeBatch("insertBatch", "t", input);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const prepared = core.prepare("SELECT id,v FROM t WHERE id >= 0");
            for (let k = 0; k < count; k++) {
              const snapshot = core.harness.path.pin();
              snapshots.push(snapshot);
              let paused = false;
              executions.push(
                prepared.execute({
                  snapshot,
                  statement: null,
                  parameters: [],
                  statementTime: 0n,
                  governor,
                  spill: core.spill,
                  queryId: k + 1,
                  sequences: null,
                  sequenceSession: null,
                  returning: "atExecuted",
                  reads: {
                    open: (pages, chunk, options) => {
                      const wrap = (reader: ChunkReader) =>
                        new Proxy(reader, {
                          get(target, name) {
                            if (name === "load")
                              return async (ids: readonly number[]) => {
                                await target.load(ids);
                                if (!paused) {
                                  paused = true;
                                  arrivals++;
                                  if (arrivals === count) entered();
                                  await gate;
                                  continuing++;
                                }
                              };
                            const value: unknown = Reflect.get(target, name, target);
                            return typeof value === "function"
                              ? (value.bind(target) as unknown)
                              : value;
                          },
                        });
                      const reader = ChunkReader.open(pages, chunk, options);
                      return reader instanceof Promise ? reader.then(wrap) : wrap(reader);
                    },
                  },
                }),
              );
            }
            const consume = async (execution: Execution) => {
              const rows: Array<Record<string, unknown>> = [];
              for (;;) {
                const frame = await execution.results.next();
                if (frame === null) break;
                try {
                  const payload = structuredClone(frame.payload);
                  rows.push(
                    ...oracleRows({
                      columns: prepared.columns,
                      batches: [
                        {
                          length: payload.rowCount,
                          selection: null,
                          columns: payload.columns.map((vector, i) => {
                            if (vector.form !== "flat") throw new Error("owned flat result");
                            const type = prepared.columns[i]?.type;
                            if (!type) throw new Error("result type");
                            return { ...vector, type };
                          }),
                        },
                      ],
                    }),
                  );
                } finally {
                  frame.release();
                }
              }
              await execution.executed;
              await execution.delivered;
              return rows;
            };
            const results = Promise.all(executions.map(consume));
            void results.catch(() => undefined);
            await ready;
            expect(continuing).toBe(0);
            reach.reach("pinned-input");
            const remaining = governor.capBytes - governor.usedBytes;
            expect(remaining).toBeGreaterThan(0);
            const held = await governor.admitGroup({
              label: "temporarily pinned competing execution",
              includesProgress: true,
              grants: [
                {
                  label: "temporarily pinned competing execution",
                  minimumBytes: remaining,
                  spillable: false,
                },
              ],
            });
            holder = held[0] ?? null;
            if (!holder) throw new Error("missing competing holder");
            expect(holder.allocate(remaining)).toBe(true);
            expect(governor.usedBytes).toBe(governor.capBytes);
            reach.reach("full-cap");
            go();
            for (const rows of await results)
              expect(rows.sort((left, right) => Number(left.id) - Number(right.id))).toEqual(input);
            expect(continuing).toBe(count);
            reach.reach("next-step");
            reach.reach("rows");
            reach.reach(count === 2 ? "two" : "three");
            expect(governor.usedBytes).toBe(holder.held);
          } finally {
            go();
            for (const execution of executions) execution.cancel(new Error("test cleanup"));
            await Promise.allSettled(
              executions.flatMap((execution) => [execution.executed, execution.delivered]),
            );
            holder?.close();
            for (const snapshot of snapshots) snapshot.release();
            await core.close();
            expect(governor.usedBytes).toBe(0);
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [2, 128],
          [3, 512],
          [2, 469],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
