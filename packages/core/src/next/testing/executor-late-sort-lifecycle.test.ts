/** Late fetch keeps the ordinary query's admission, cancellation and pinned-view laws. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import type { ExecuteRequest, Execution, PreparedPlan } from "../exec/executor.js";
import type { Snapshot } from "../access/snapshot.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { createExecutor } from "../exec/create.js";

const MiB = 1024 * 1024;
function request(core: StorageCore, snapshot: Snapshot, governor: Governor): ExecuteRequest {
  return {
    snapshot,
    statement: null,
    parameters: [],
    statementTime: 0n,
    governor,
    spill: core.spill,
    queryId: 700,
    sequences: null,
    sequenceSession: null,
    returning: "atExecuted",
    reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
  };
}
async function rows(execution: Execution, prepared: PreparedPlan) {
  const result: Array<Record<string, unknown>> = [];
  for (;;) {
    const frame = await execution.results.next();
    if (frame === null) break;
    try {
      result.push(
        ...oracleRows({
          columns: prepared.columns,
          batches: [
            {
              length: frame.payload.rowCount,
              selection: null,
              columns: frame.payload.columns.map((v, i) => {
                if (v.form !== "flat") throw new Error("owned flat frame");
                const type = prepared.columns[i]?.type;
                if (!type) throw new Error("missing result type");
                return { ...v, type };
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
  return result;
}

it.each(seedsFor("next-executor-late-sort-small-cap", [0xd105, 0xd106]))(
  "keeps the generic sort when helper minimums exceed the cap (seed %i)",
  async (seed) => {
    const reach = createReach("late sort small cap", [
      "wide",
      "memory",
      "stored",
      "control-admissible",
      "fallback",
      "rows",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 180, max: 200 }),
        fc.integer({ min: 32, max: 128 }),
        fc.boolean(),
        async (width, count, memory) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open(),
            governor = new Governor((memory ? 21 : 16) * MiB);
          const names = Array.from({ length: width }, (_, i) => `c${String(i)}`);
          const input: Array<Record<string, number>> = Array.from({ length: count }, (_, i) =>
            Object.fromEntries(
              names.map((name, j) => [name, j === 0 ? (i * 7919) % count : i * 1000 + j]),
            ),
          );
          let execution: Execution | null = null;
          const pin = () => core.harness.path.pin();
          let snapshot: Snapshot | null = null;
          try {
            await core.execute(`CREATE TABLE t(${names.map((n) => `${n} INTEGER`).join(",")})`);
            await core.writeBatch("insertBatch", "t", input);
            if (!memory) {
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
            }
            const prepared = core.prepare("SELECT * FROM t ORDER BY c0 DESC,c1 LIMIT 7 OFFSET 5");
            const expected = [...input]
              .sort((a, b) => (b.c0 ?? 0) - (a.c0 ?? 0) || (a.c1 ?? 0) - (b.c1 ?? 0))
              .slice(5, 12);
            const control = createExecutor({ lateSort: false }).prepare(prepared.plan);
            snapshot = pin();
            const fixture = await core.harness.governor.admit({
              label: "late-cap/state",
              minimumBytes: 4096,
              spillable: true,
            });
            try {
              const def = core.harness.path.catalog.tableByName("t");
              if (!def) throw new Error("fixture table");
              const table = await snapshot.table(def.id, { budget: fixture });
              expect(new Set(table.segments.map((segment) => segment.kind))).toEqual(
                new Set([memory ? "memory" : "group"]),
              );
            } finally {
              fixture.close();
            }
            execution = control.execute(request(core, snapshot, governor));
            expect(await rows(execution, control)).toEqual(expected);
            expect(governor.usedBytes).toBe(0);
            snapshot.release();
            reach.reach("control-admissible");
            snapshot = pin();
            execution = prepared.execute(request(core, snapshot, governor));
            expect(await rows(execution, prepared)).toEqual(expected);
            expect(
              execution.summary?.profile.operators.filter((op) => op.op === "sort"),
            ).toHaveLength(1);
            expect(execution.summary?.profile.pipelines).toBe(1);
            reach.reach("wide");
            reach.reach(memory ? "memory" : "stored");
            reach.reach("fallback");
            reach.reach("rows");
          } finally {
            execution?.cancel();
            if (execution) await Promise.allSettled([execution.executed, execution.delivered]);
            snapshot?.release();
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
          [200, 128, false],
          [200, 128, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-executor-late-sort-cancel", [0xd107, 0xd108]))(
  "drains late-fetch reads and result credit on cancellation (seed %i)",
  async (seed) => {
    const reach = createReach("late sort cancellation", [
      "leased-read",
      "held-frame",
      "late",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 8, max: 48 }), async (io, limit) => {
        reach.beginHistory();
        const core = await new StorageCore(8192).open(),
          governor = new Governor(64 * MiB);
        const names = Array.from({ length: 24 }, (_, i) => `c${String(i)}`);
        let snapshot: Snapshot | null = null,
          execution: Execution | null = null;
        let wake: (() => void) | undefined;
        const entered = new Promise<void>((resolve) => {
          wake = resolve;
        });
        let keyOpens = 0,
          gated = false;
        let readSignal: AbortSignal | undefined;
        let stopRead: ((reason: unknown) => void) | undefined;
        try {
          await core.execute(`CREATE TABLE t(${names.map((n) => `${n} INTEGER`).join(",")})`);
          await core.writeBatch(
            "insertBatch",
            "t",
            Array.from({ length: 257 }, (_, i) =>
              Object.fromEntries(
                names.map((n, j) => [n, j === 0 ? (i * 7919) % 257 : i * 1000 + j]),
              ),
            ),
          );
          await core.harness.path.recycleLog();
          await core.harness.path.quiesce();
          const table = core.harness.path.catalog.tableByName("t");
          const payloadId = table?.columns[2]?.id;
          if (payloadId === undefined || !table) throw new Error("fixture columns");
          const keys = new Set(table.columns.slice(0, 2).map((c) => c.id));
          const prepared = core.prepare(
            `SELECT * FROM t ORDER BY c0 DESC,c1 LIMIT ${String(limit)}`,
          );
          snapshot = core.harness.path.pin();
          const base = request(core, snapshot, governor);
          execution = prepared.execute({
            ...base,
            resultWindowBytes: 256,
            resultFrameRows: 1,
            reads: {
              open: (pages, chunk, options) => {
                if (keys.has(chunk.columnId)) keyOpens++;
                const opened = ChunkReader.open(pages, chunk, options);
                if (!io || chunk.columnId !== payloadId) return opened;
                const wrap = (reader: Awaited<typeof opened>) =>
                  new Proxy(reader, {
                    get(target, name) {
                      if (name === "load")
                        return async (ids: readonly number[]) => {
                          await target.load(ids);
                          if (gated) return;
                          gated = true;
                          expect(keyOpens).toBeGreaterThanOrEqual(4);
                          reach.reach("late");
                          reach.reach("leased-read");
                          wake?.();
                          await new Promise<void>((_resolve, reject) => {
                            const signal = options.signal;
                            if (!signal) throw new Error("fetch read requires cancellation signal");
                            readSignal = signal;
                            stopRead = reject;
                            const onAbort = () => {
                              const reason: unknown = signal.reason;
                              reject(
                                reason instanceof Error
                                  ? reason
                                  : new Error("fetch read cancelled", { cause: reason }),
                              );
                            };
                            if (signal.aborted) {
                              onAbort();
                              return;
                            }
                            signal.addEventListener("abort", onAbort, {
                              once: true,
                            });
                          });
                        };
                      const value: unknown = Reflect.get(target, name, target);
                      return typeof value === "function" ? (value.bind(target) as unknown) : value;
                    },
                  });
                return opened instanceof Promise ? opened.then(wrap) : wrap(opened);
              },
            },
          });
          if (io) {
            await Promise.race([
              entered,
              execution.executed.then(() => {
                throw new Error("fetch read was not reached");
              }),
            ]);
          } else {
            const frame = await execution.results.next();
            expect(frame).not.toBeNull();
            expect(keyOpens).toBeGreaterThanOrEqual(4);
            reach.reach("late");
            reach.reach("held-frame");
            execution.cancel("held-frame cancellation");
            frame?.release();
          }
          execution.cancel("fetch cancellation");
          if (io)
            expect(readSignal?.aborted, "the leased payload read must be cancelled").toBe(true);
          const settled = await Promise.allSettled([execution.executed, execution.delivered]);
          expect(settled.every((item) => item.status === "rejected")).toBe(true);
          await expect(execution.results.next()).rejects.toMatchObject({
            code: "MINNOW_QUERY_CANCELLED",
          });
          expect(core.spill.bytesInUse).toBe(0);
        } finally {
          execution?.cancel("test cleanup");
          // Drain the test-owned I/O gate even when the cancellation assertion rejects a mutant.
          stopRead?.(new Error("test gate cleanup"));
          if (execution) await Promise.allSettled([execution.executed, execution.delivered]);
          snapshot?.release();
          await core.close();
          expect(governor.usedBytes).toBe(0);
          reach.reach("released");
        }
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [true, 8],
          [false, 8],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
