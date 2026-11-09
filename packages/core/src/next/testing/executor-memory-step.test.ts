/** Production executor memory reads must use B3's admitted, caller-owned prefix API. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { MemorySegment, Snapshot, TableSnapshot } from "../access/snapshot.js";
import { Governor, MemoryAdmissionError } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { yieldTask } from "../task.js";
import type { Batch } from "../vector/vector.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

function executionError(error: unknown): Error {
  return error instanceof Error ? error : new Error("Executor failed", { cause: error });
}

it.each(seedsFor("next-executor-memory-step", [0xc433, 0xc434]))(
  "keeps real memory reads inside each execution's admitted prefix (seed %i)",
  async (seed) => {
    const reach = createReach("executor memory step", [
      "memory-view",
      "publication",
      "multiblob",
      "oversized-prefix",
      "concurrent",
      "bounded-call",
      "caller-output",
      "cancelled",
      "rows",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 2 }),
        async (publish, wide, cancel, copies) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open(),
            gov = new Governor(256 << 20);
          try {
            await core.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,p TEXT)");
            const rows = Array.from({ length: 8 }, (_, id) => ({
              id,
              p: id === 0 && wide ? "x".repeat((1 << 20) + 7) : `row ${String(id)}`,
            }));
            await core.writeBatch("insertBatch", "t", rows);
            const prepared = core.prepare("SELECT id,p FROM t LIMIT $1 OFFSET $2");
            let published = false,
              activeExecutions = 0,
              peakExecutions = 0;
            const execute = async (queryId: number) => {
              const pin = core.harness.path.pin();
              let calls = 0;
              const snapshot = new Proxy<Snapshot>(pin, {
                get(target, key) {
                  if (key === "table")
                    return async (
                      id: number,
                      options: Parameters<Snapshot["table"]>[1],
                    ): Promise<TableSnapshot> => {
                      const table = await target.table(id, options);
                      return {
                        ...table,
                        segments: table.segments.map((segment) => {
                          if (segment.kind !== "memory") return segment;
                          return new Proxy<MemorySegment>(segment, {
                            get(source, name) {
                              if (name === "readHint")
                                return async (...args: Parameters<MemorySegment["readHint"]>) => {
                                  const hint = await source.readHint(...args);
                                  if (hint.oversized) reach.reach("oversized-prefix");
                                  if (publish && !published) {
                                    published = true;
                                    await core.harness.path.recycleLog();
                                    await core.harness.path.quiesce();
                                    reach.reach("publication");
                                  }
                                  return hint;
                                };
                              if (name === "chunk")
                                return async (...args: Parameters<MemorySegment["chunk"]>) => {
                                  const [column, chunk, out, start = 0, step] = args;
                                  expect(
                                    step,
                                    "executor omitted its admitted memory step",
                                  ).toBeDefined();
                                  if (step === undefined)
                                    throw new Error("missing admitted memory step");
                                  reach.reach("bounded-call");
                                  const result = await source.chunk(
                                    column,
                                    chunk,
                                    out,
                                    start,
                                    step,
                                  );
                                  expect(result.vector).toBe(out.flat);
                                  expect(result.firstRow).toBe(start);
                                  expect(result.rowCount).toBe(step.hint.rowCount);
                                  expect(result.reason).not.toBe("need");
                                  expect(result.reason).not.toBe("denied");
                                  reach.reach("caller-output");
                                  reach.reach("memory-view");
                                  const data = out.flat.data;
                                  if (
                                    data.layout === "varlen" &&
                                    (data.offsets[result.rowCount] ?? 0) > 1 << 20
                                  )
                                    reach.reach("multiblob");
                                  calls++;
                                  if (cancel && calls === 1) {
                                    execution.cancel(
                                      new Error("cancel between bounded decode and delivery"),
                                    );
                                    await yieldTask();
                                  }
                                  return result;
                                };
                              const value: unknown = Reflect.get(source, name, source);
                              return typeof value === "function"
                                ? (value.bind(source) as unknown)
                                : value;
                            },
                          });
                        }),
                      };
                    };
                  const value: unknown = Reflect.get(target, key, target);
                  return typeof value === "function" ? (value.bind(target) as unknown) : value;
                },
              });
              const execution = prepared.execute({
                snapshot,
                statement: null,
                parameters: [2, 1],
                statementTime: 0n,
                governor: gov,
                spill: core.spill,
                queryId,
                reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
                sequences: null,
                sequenceSession: null,
                returning: "atExecuted",
              });
              activeExecutions++;
              peakExecutions = Math.max(peakExecutions, activeExecutions);
              const completed = Promise.allSettled([execution.executed, execution.delivered]);
              const batches: Batch[] = [];
              try {
                let error: unknown;
                try {
                  for (;;) {
                    const frame = await execution.results.next();
                    if (frame === null) break;
                    try {
                      batches.push({
                        length: frame.payload.rowCount,
                        selection: null,
                        columns: frame.payload.columns.map((vector, i) => {
                          if (vector.form !== "flat") throw new Error("owned memory output");
                          const type = prepared.columns[i]?.type;
                          if (type === undefined) throw new Error("result type");
                          return { ...structuredClone(vector), type };
                        }),
                      });
                    } finally {
                      frame.release();
                    }
                  }
                } catch (caught) {
                  error = caught;
                }
                const states = await completed;
                if (!cancel && error !== undefined) throw executionError(error);
                expect(calls).toBeGreaterThan(0);
                if (cancel) {
                  expect(error).toMatchObject({ name: "QueryCancelledError" });
                  expect(states.every((state) => state.status === "rejected")).toBe(true);
                  reach.reach("cancelled");
                } else {
                  if (error !== undefined) throw executionError(error);
                  expect(states.every((state) => state.status === "fulfilled")).toBe(true);
                  expect(oracleRows({ columns: prepared.columns, batches })).toEqual(
                    rows.slice(1, 3),
                  );
                  reach.reach("rows");
                }
              } finally {
                execution.cancel();
                await completed;
                pin.release();
                activeExecutions--;
              }
            };
            await Promise.all(Array.from({ length: copies }, (_, i) => execute(i + 1)));
            if (peakExecutions > 1) reach.reach("concurrent");
          } finally {
            await core.close();
            expect(gov.usedBytes).toBe(0);
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [false, false, false, 1],
          [true, true, false, 2],
          [false, true, true, 2],
          [true, false, true, 1],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-memory-step-admission", [0xc435, 0xc436]))(
  "refuses only an over-cap memory minimum before decoding input (seed %i)",
  async (seed) => {
    const reach = createReach("memory step admission", [
      "empty",
      "admitted",
      "over-cap",
      "before-chunk",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.constantFrom(4, 8, 16, 64), async (empty, cap) => {
        reach.beginHistory();
        const core = await new StorageCore(8192).open(),
          gov = new Governor(cap << 20);
        let pin: Snapshot | undefined;
        try {
          await core.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,p TEXT)");
          if (!empty)
            await core.writeBatch("insertBatch", "t", [
              { id: 1, p: "one" },
              { id: 2, p: "two" },
            ]);
          const prepared = core.prepare("SELECT id,p FROM t LIMIT 2");
          pin = core.harness.path.pin();
          let chunks = 0;
          const snapshot = new Proxy<Snapshot>(pin, {
            get(target, key) {
              if (key === "table")
                return async (
                  id: number,
                  options: Parameters<Snapshot["table"]>[1],
                ): Promise<TableSnapshot> => {
                  const table = await target.table(id, options);
                  return {
                    ...table,
                    segments: table.segments.map((segment) =>
                      segment.kind !== "memory"
                        ? segment
                        : new Proxy(segment, {
                            get(source, name) {
                              if (name === "chunk")
                                return (...args: Parameters<MemorySegment["chunk"]>) => {
                                  chunks++;
                                  return source.chunk(...args);
                                };
                              const value: unknown = Reflect.get(source, name, source);
                              return typeof value === "function"
                                ? (value.bind(source) as unknown)
                                : value;
                            },
                          }),
                    ),
                  };
                };
              const value: unknown = Reflect.get(target, key, target);
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
          const completed = Promise.allSettled([execution.executed, execution.delivered]);
          try {
            let count = 0,
              error: unknown;
            try {
              for (;;) {
                const frame = await execution.results.next();
                if (frame === null) break;
                try {
                  count += frame.payload.rowCount;
                } finally {
                  frame.release();
                }
              }
            } catch (caught) {
              error = caught;
            }
            const states = await completed;
            if (empty || cap === 64) {
              if (error !== undefined) throw executionError(error);
              expect(states.every((state) => state.status === "fulfilled")).toBe(true);
              expect(count).toBe(empty ? 0 : 2);
              if (empty) {
                expect(chunks).toBe(0);
                reach.reach("empty");
              } else {
                expect(chunks).toBeGreaterThan(0);
                reach.reach("admitted");
              }
            } else {
              expect(error).toBeInstanceOf(MemoryAdmissionError);
              if (!(error instanceof MemoryAdmissionError)) throw executionError(error);
              expect(error.bytes).toBeGreaterThan(error.capacity);
              expect(states.every((state) => state.status === "rejected")).toBe(true);
              expect(chunks).toBe(0);
              reach.reach("over-cap");
              reach.reach("before-chunk");
            }
          } finally {
            execution.cancel();
            await completed;
          }
          expect(gov.usedBytes).toBe(0);
        } finally {
          pin?.release();
          await core.close();
          expect(gov.usedBytes).toBe(0);
          reach.reach("released");
        }
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [true, 4],
          [false, 16],
          [false, 64],
        ],
      },
    );
    reach.assertReached();
  },
);
