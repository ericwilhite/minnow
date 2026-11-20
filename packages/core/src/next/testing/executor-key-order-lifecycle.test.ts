/** Index I/O, payload I/O, output credit and publication retain the pin until settlement. */
import { createReach } from "../../testing/reach.js";
import { expect, it } from "vitest";
import { StorageCore } from "./storage-core.js";
import type { Snapshot } from "../access/snapshot.js";
import type { IndexReader, IndexCursor } from "../access/key-index.js";
import { Governor } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { oracleRows } from "./differential-adapter.js";
import { createExecutor } from "../exec/create.js";
import { WINDOW_SIZING_LABEL } from "../exec/key-order.js";

// Index and skip I/O are gated in each cursor role: a sized window opens a sizing cursor, then
// the execution cursor, and each must close at once on cancel and settle before release.
it.each([
  ["index", "sizing"],
  ["index", "execution"],
  ["skip", "sizing"],
  ["skip", "execution"],
  ["fetch", null],
  ["frame", null],
  ["publish", null],
] as const)(
  "settles ordered-index %s (gated cursor: %s) before releasing query resources",
  async (phase, gatedRole) => {
    const label = gatedRole === null ? phase : `${phase}:${gatedRole}`;
    const reach = createReach("key-order lifecycle", [label]);
    reach.beginHistory();
    const core = await new StorageCore(8192).open(),
      governor = new Governor(64 << 20);
    const executor = createExecutor();
    core.executor.prepare = executor.prepare.bind(executor);
    const names = ["id", ...Array.from({ length: 16 }, (_, i) => `c${String(i)}`)];
    let pin: Snapshot | null = null,
      release: (() => void) | undefined,
      wake: (() => void) | undefined,
      opens = 0,
      closes = 0,
      // A window of at most one cursor batch is sized first by its own cursor (lead design
      // decision under the governor law: size before admission, release, then admit).
      sizingOpens = 0,
      executionOpens = 0,
      gated: "sizing" | "execution" | null = null,
      published = false;
    const entered = new Promise<void>((resolve) => {
      wake = resolve;
    });
    const cursors: IndexCursor[] = [];
    let execution: ReturnType<ReturnType<StorageCore["prepare"]>["execute"]> | null = null;
    try {
      await core.execute(
        `CREATE TABLE t(${names.map((n) => `${n} INTEGER${n === "id" ? " PRIMARY KEY" : ""}`).join(",")})`,
      );
      const input = Array.from({ length: 257 }, (_, i) =>
        Object.fromEntries(names.map((n, j) => [n, j === 0 ? i : i * 100 + j])),
      );
      await core.writeBatch("insertBatch", "t", input);
      if (phase !== "publish") {
        await core.harness.path.recycleLog();
        await core.harness.path.quiesce();
      }
      const offset = phase === "skip" ? 11 : 0;
      const prepared = core.prepare(`SELECT * FROM t ORDER BY id LIMIT 8 OFFSET ${String(offset)}`);
      pin = core.harness.path.pin();
      const gate = () =>
        new Promise<void>((resolve) => {
          release = resolve;
          wake?.();
        });
      const snapshot: Snapshot = new Proxy(pin, {
        get(target, name) {
          // This fixture owns the publication-wide index-I/O path. Hide the optional
          // retained read capability so resident-input specialization cannot bypass
          // its injected cursor publication. The new resident route has its own
          // publication-after-admission generator and captured-input lifetime checks.
          if (name === "table" && phase === "publish")
            return (...args: Parameters<Snapshot["table"]>) =>
              new Proxy(target.table(...args), {
                get(table, key) {
                  if (key === "prepareRead") return undefined;
                  const value: unknown = Reflect.get(table, key, table);
                  return typeof value === "function" ? (value.bind(table) as unknown) : value;
                },
              });
          if (name === "index")
            return (...args: Parameters<Snapshot["index"]>): IndexReader => {
              const reader = target.index(...args);
              return {
                def: reader.def,
                probe: reader.probe.bind(reader),
                range: () => {
                  throw new Error("Default SQL must use admitted cursors");
                },
                planRange: (range, dir) => {
                  const plan = reader.planRange?.(range, dir);
                  if (!plan) throw new Error("Missing native admitted cursor");
                  return {
                    minimumBytes: plan.minimumBytes,
                    open(options) {
                      opens++;
                      const role =
                        options.budget.label === WINDOW_SIZING_LABEL ? "sizing" : "execution";
                      if (role === "sizing") sizingOpens++;
                      else executionOpens++;
                      const cursor = plan.open(options);
                      cursors.push(cursor);
                      return {
                        skip: async (count) => {
                          const n = await cursor.skip(count);
                          if (phase === "skip" && role === gatedRole) {
                            gated ??= role;
                            await gate();
                          }
                          return n;
                        },
                        next: async (out) => {
                          const n = await cursor.next(out);
                          if (phase === "index" && role === gatedRole) {
                            gated ??= role;
                            await gate();
                          }
                          if (phase === "publish" && !published) {
                            await core.harness.path.recycleLog();
                            await core.harness.path.quiesce();
                            published = true;
                          }
                          return n;
                        },
                        close() {
                          closes++;
                          cursor.close();
                        },
                      };
                    },
                  };
                },
              };
            };
          const value: unknown = Reflect.get(target, name, target);
          return typeof value === "function" ? (value.bind(target) as unknown) : value;
        },
      });
      execution = prepared.execute({
        snapshot,
        statement: null,
        parameters: [],
        statementTime: 0n,
        governor,
        spill: core.spill,
        queryId: 901,
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
        resultWindowBytes: 256,
        resultFrameRows: 1,
        reads: {
          open: (pages, chunk, options) => {
            expect(executionOpens, "the key cursor must open before payload reads").toBe(1);
            expect(sizingOpens, "the window is sized by its own cursor first").toBe(1);
            const reader = ChunkReader.open(pages, chunk, options);
            if (phase !== "fetch") return reader;
            const wrap = (source: Awaited<typeof reader>) =>
              new Proxy(source, {
                get(target, name) {
                  if (name === "load")
                    return async (ids: readonly number[]) => {
                      await target.load(ids);
                      if (release === undefined) await gate();
                    };
                  const value: unknown = Reflect.get(target, name, target);
                  return typeof value === "function" ? (value.bind(target) as unknown) : value;
                },
              });
            return reader instanceof Promise ? reader.then(wrap) : wrap(reader);
          },
        },
      });
      if (phase === "publish") {
        const result: unknown[] = [];
        for (;;) {
          const frame = await execution.results.next();
          if (!frame) break;
          try {
            result.push(
              ...oracleRows({
                columns: prepared.columns,
                batches: [
                  {
                    length: frame.payload.rowCount,
                    selection: null,
                    columns: frame.payload.columns.map((v, i) => {
                      if (v.form !== "flat") throw new Error("flat frame");
                      const type = prepared.columns[i]?.type;
                      if (!type) throw new Error("output type");
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
        expect(result).toEqual(input.slice(offset, offset + 8));
        expect(published).toBe(true);
        expect([sizingOpens, executionOpens, closes]).toEqual([1, 1, 2]);
      } else {
        if (phase === "frame") {
          const frame = await execution.results.next();
          expect(frame).not.toBeNull();
          execution.cancel();
          frame?.release();
        } else {
          await Promise.race([
            entered,
            execution.executed.then(() => {
              throw new Error("I/O gate was not reached");
            }),
          ]);
          execution.cancel();
          // Every opened cursor closes at once on cancel: the sizing cursor too, while its
          // pending index call is still in flight.
          expect(closes, "every opened cursor closes on cancel").toBe(opens);
          let settled = false;
          void execution.executed.catch(() => {
            settled = true;
          });
          await Promise.resolve();
          expect(settled, "in-flight I/O must settle before execution cleanup").toBe(false);
          release?.();
        }
        const result = await Promise.allSettled([execution.executed, execution.delivered]);
        expect(result.every((r) => r.status === "rejected")).toBe(true);
        if (gatedRole === "sizing") {
          // Cancelled inside the sizing pass: its cursor closed on abort, its memory was held
          // until the gated index call settled, and no execution cursor ever opened.
          expect(gated).toBe("sizing");
          expect([sizingOpens, executionOpens, closes]).toEqual([1, 0, 1]);
        } else {
          // Sizing finished and closed; the execution cursor was cancelled (with a pending index
          // call when it is the gated role) and closed at once.
          if (gatedRole === "execution") expect(gated).toBe("execution");
          expect([sizingOpens, executionOpens, closes]).toEqual([1, 1, 2]);
        }
      }
      reach.reach(label);
      reach.assertReached();
      expect(core.spill.bytesInUse).toBe(0);
      expect(governor.usedBytes).toBe(0);
    } finally {
      execution?.cancel();
      release?.();
      if (execution) await Promise.allSettled([execution.executed, execution.delivered]);
      for (const c of cursors) c.close();
      pin?.release();
      await core.close();
      expect(governor.usedBytes).toBe(0);
    }
  },
  120000,
);
