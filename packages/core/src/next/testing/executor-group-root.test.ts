/** Stored scan metadata belongs to its step; the executor does not install B3 snapshot holds. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import type { Snapshot, TableSnapshot } from "../access/snapshot.js";
import { ChunkReader } from "../page/reader.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

it.each(seedsFor("next-scoped-group-metadata", [0xc41f, 0xc420]))(
  "scans frozen segment arrays without retaining provider roots after execution (seed %i)",
  async (seed) => {
    const reach = createReach("scoped group metadata", [
      "groups",
      "frozen",
      "private-root",
      "rows",
      "released-before-unpin",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 6 }),
        fc.integer({ min: 16, max: 64 }),
        async (groups, count) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open(),
            gov = new Governor(32 << 20);
          let pin: Snapshot | null = null,
            rawRoots = 0;
          try {
            await core.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,payload TEXT)");
            for (let group = 0; group < groups; group++) {
              await core.writeBatch(
                "insertBatch",
                "t",
                Array.from({ length: count }, (_, row) => ({
                  id: group * count + row,
                  payload: "unused",
                })),
              );
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
            }
            reach.reach("groups");
            const prepared = core.prepare("SELECT id FROM t ORDER BY id");
            pin = core.harness.path.pin();
            const snapshot: Snapshot = new Proxy<Snapshot>(pin, {
              get(target, name) {
                if (name === "table")
                  return async (
                    id: number,
                    options: Parameters<Snapshot["table"]>[1],
                  ): Promise<TableSnapshot> => {
                    const table = await target.table(id, options);
                    const segments = Object.freeze(
                      table.segments.map((segment) =>
                        segment.kind !== "group"
                          ? segment
                          : new Proxy(segment, {
                              get(source, key) {
                                if (key === "root")
                                  return () => {
                                    rawRoots++;
                                    throw new Error("executor installed a provider root hold");
                                  };
                                const value: unknown = Reflect.get(source, key, source);
                                return typeof value === "function"
                                  ? (value.bind(source) as unknown)
                                  : value;
                              },
                            }),
                      ),
                    );
                    expect(Object.isFrozen(segments)).toBe(true);
                    reach.reach("frozen");
                    return {
                      tableId: table.tableId,
                      schemaVersion: table.schemaVersion,
                      columns: table.columns,
                      get liveRowCount() {
                        return table.liveRowCount;
                      },
                      segments,
                    };
                  };
                const value: unknown = Reflect.get(target, name, target);
                return typeof value === "function" ? (value.bind(target) as unknown) : value;
              },
            });
            const execution = prepared.execute({
              snapshot,
              statement: null,
              parameters: [],
              statementTime: 0n,
              governor: gov,
              spill: core.spill,
              queryId: 1,
              reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
            });
            const rows: Array<Record<string, unknown>> = [];
            try {
              for (;;) {
                const frame = await execution.results.next();
                if (frame === null) break;
                try {
                  rows.push(
                    ...oracleRows({
                      columns: prepared.columns,
                      batches: [
                        {
                          length: frame.payload.rowCount,
                          selection: null,
                          columns: frame.payload.columns.map((vector, i) => {
                            if (vector.form !== "flat") throw new Error("owned flat result");
                            const type = prepared.columns[i]?.type;
                            if (!type) throw new Error("result type");
                            return { ...structuredClone(vector), type };
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
              expect(rows).toEqual(Array.from({ length: groups * count }, (_, id) => ({ id })));
              reach.reach("rows");
              expect(rawRoots).toBe(0);
              reach.reach("private-root");
              expect(gov.usedBytes).toBe(0);
              reach.reach("released-before-unpin");
            } finally {
              execution.cancel();
              await Promise.allSettled([execution.executed, execution.delivered]);
            }
          } finally {
            pin?.release();
            await core.close();
            expect(gov.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [2, 16],
          [6, 64],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
