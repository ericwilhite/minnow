/** Pressure-only hash checkpointing retires producers before atomic restore admission.
 * Prefix transitions and membership precede suffix input; the snapshot pin stays owned
 * by the execution and mutable parameters come from its admitted copy. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { RestoredAggregateOperator } from "../exec/agg/restored.js";
import { QueryRuntime } from "../exec/run.js";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { SCRATCH_CONTROL_BYTES } from "../spill/scratch-ranges.js";
import { FileScratch } from "../storage/file-store.js";
import { MemoryFile } from "../storage/memory-store.js";
import { Governor } from "../memory/governor.js";
import type { Execution, PreparedPlan } from "../exec/executor.js";
import { ChunkReader } from "../page/reader.js";
import type { Batch } from "../vector/vector.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { byteSliceEquals } from "./byte-equality.test.fixture.js";

/** Liveness guard: productive FIFO phases may take more than 30 seconds in total. */
async function deadline<T>(work: Promise<T>, activity: Set<() => void>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const reset = () => {
    clearTimeout(timer);
    timer = setTimeout(
      () => rejectStall(new Error("Concurrent SQL phase progress stalled")),
      30000,
    );
  };
  let rejectStall: (error: Error) => void = () => undefined;
  try {
    const stalled = new Promise<never>((_, reject) => {
      rejectStall = reject;
    });
    activity.add(reset);
    reset();
    return await Promise.race([work, stalled]);
  } finally {
    activity.delete(reset);
    clearTimeout(timer);
  }
}

it("keeps productive phase waits alive and still rejects 30 seconds without progress", async () => {
  vi.useFakeTimers();
  const activity = new Set<() => void>();
  let complete: () => void = () => undefined;
  try {
    const work = new Promise<void>((resolve) => {
      complete = resolve;
    });
    let outcome = "pending";
    const waiting = deadline(work, activity).then(
      () => {
        outcome = "complete";
      },
      () => {
        outcome = "stalled";
      },
    );
    await vi.advanceTimersByTimeAsync(29000);
    for (const advance of activity) advance();
    await vi.advanceTimersByTimeAsync(29000);
    expect(outcome).toBe("pending");
    complete();
    await waiting;
    expect(outcome).toBe("complete");
    expect(activity.size).toBe(0);

    let failure: unknown;
    const stalled = deadline(new Promise<never>(() => undefined), activity).catch(
      (error: unknown) => {
        failure = error;
      },
    );
    await vi.advanceTimersByTimeAsync(29999);
    expect(failure).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(failure).toEqual(new Error("Concurrent SQL phase progress stalled"));
    await stalled;
    expect(activity.size).toBe(0);
  } finally {
    complete();
    vi.useRealTimers();
  }
});

const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows
    .map((r) => ({
      g: Number(r.g ?? 0),
      n: Number(r.n),
      s: r.s === null ? null : Number(r.s),
      a: r.a === null ? null : Number(r.a),
    }))
    .sort((a, b) => a.g - b.g);
async function consume(
  prepared: PreparedPlan,
  execution: Execution,
  bytes: Uint8Array,
  advance: () => void = () => undefined,
) {
  const batches: Batch[] = [];
  for (;;) {
    const frame = await execution.results.next();
    if (!frame) break;
    try {
      const payload = frame.payload,
        marker = payload.columns.at(-1);
      if (marker?.form !== "flat" || marker.data.layout !== "varlen")
        throw new Error("Expected owned byte parameter above aggregate");
      for (let row = 0; row < payload.rowCount; row++) {
        const from = marker.data.offsets[row] ?? 0,
          to = marker.data.offsets[row + 1] ?? from;
        if (to - from !== bytes.length)
          throw new Error(
            `Owned parameter length differs at row ${String(row)}: ${String(to - from)} != ${String(bytes.length)}`,
          );
        // Compare every byte without a JS callback per byte. Large parameters repeated
        // across many output groups otherwise dominate this liveness test's CPU budget.
        if (!byteSliceEquals(marker.data.bytes, from, bytes))
          throw new Error(`Owned parameter bytes differ at row ${String(row)}`);
      }
      // Verify the parameter under the frame lease, then copy only result columns retained
      // by the PG oracle. Cloning a large repeated marker copies bytes we immediately discard.
      const columns = structuredClone(payload.columns.slice(0, -1));
      batches.push({
        length: payload.rowCount,
        selection: null,
        columns: columns.map((column, i) => {
          const type = prepared.columns[i]?.type;
          if (column.form !== "flat" || !type) throw new Error("Expected owned aggregate result");
          return { ...column, type };
        }),
      });
    } finally {
      frame.release();
      advance();
    }
  }
  await execution.executed;
  await execution.delivered;
  return canonical(oracleRows({ columns: prepared.columns.slice(0, -1), batches }));
}

it.each(seedsFor("next-aggregation-sql-handoff", [0xb4cb, 0xb4cc]))(
  "completes concurrent scalar/grouped restore phases and cancels with no retained credit (seed %i)",
  async (seed) => {
    const reach = createReach("SQL aggregate phase handoff", [
      "several executions",
      "scalar",
      "grouped",
      "skewed dominant group",
      "many small groups",
      "filtered transitions",
      "pressure checkpoint",
      "restore admission",
      "temporary pinned holder",
      "holder releases",
      "cancel pending phase",
      "cancel restore pass",
      "snapshot retained",
      "owned parameter",
      "empty owned parameter",
      "resident follower",
      "empty filtered resident state",
      "PG exact",
      "scratch service",
      "zero",
    ]);
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    await pg.waitReady;
    try {
      const ddl = "CREATE TABLE t(g INTEGER,h INTEGER,v TEXT,x DOUBLE PRECISION)";
      await core.execute(ddl);
      await pg.exec(ddl);
      const tinyDdl = ddl.replace("TABLE t(", "TABLE tiny(");
      await core.execute(tinyDdl);
      await pg.exec(tinyDdl);
      await core.writeBatch("insertBatch", "tiny", [
        { g: 0, h: 0, v: "resident-a", x: 0 },
        { g: 0, h: 1, v: "resident-b", x: 1e16 },
      ]);
      await pg.exec("INSERT INTO tiny VALUES (0,0,'resident-a',0),(0,1,'resident-b',1e16)");
      // The full DISTINCT payload exceeds the admitted initial hash workspace even
      // when a short read batch avoids a conservative batch-growth overestimate.
      const rowCount = 32768;
      const rows = Array.from({ length: rowCount }, (_, i) => ({
        g: i < (rowCount * 7) / 8 ? 0 : 1 + (i % 11),
        h: i % 1024,
        v: `v${String(i)}${"x".repeat(120)}`,
        x: i % 13 === 0 ? 0 : ([1e16, 1, -1e16, 0.125][Math.floor(i / 1024) % 4] ?? 0),
      }));
      await core.writeBatch("insertBatch", "t", rows);
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      for (let from = 0; from < rows.length; from += 256) {
        const chunk = rows.slice(from, from + 256);
        await pg.query(
          `INSERT INTO t VALUES ${chunk.map((_, i) => `($${String(i * 4 + 1)},$${String(i * 4 + 2)},$${String(i * 4 + 3)},$${String(i * 4 + 4)})`).join(",")}`,
          chunk.flatMap((r) => [r.g, r.h, r.v, r.x]),
        );
      }
      type Shape = "scalar" | "skew" | "many";
      const sql = (shape: Shape, marker: boolean, filtered = false, resident = false) => {
        const key = shape === "many" ? "h" : "g";
        const source = resident ? "tiny" : "t";
        return `SELECT ${shape !== "scalar" ? `${key} AS g,` : ""}count(DISTINCT v) n,sum(x)${filtered ? " FILTER (WHERE x <> 0)" : ""} s,avg(x)${filtered ? " FILTER (WHERE x <> 0)" : ""} a${marker ? ",$1::text AS marker" : ""} FROM ${source}${shape !== "scalar" ? ` GROUP BY ${key} ORDER BY ${key}` : ""}`;
      };
      const expected = new Map<string, ReturnType<typeof canonical>>();
      for (const shape of ["scalar", "skew", "many"] as const) {
        for (const filtered of [false, true]) {
          for (const resident of [false, true]) {
            // A PostgreSQL GROUP BY or DISTINCT can reorder equal-key input. Explicit
            // physical insertion ordinals witness the transition sequence required by
            // this executor's scan; do not compare two unspecified float orders.
            const oracle = sql(shape, false, filtered, resident)
              .replace("sum(x)", "sum(x ORDER BY ctid)")
              .replace("avg(x)", "avg(x ORDER BY ctid)");
            const rows = canonical((await pg.query<Record<string, unknown>>(oracle)).rows);
            expected.set(`${shape}/${String(filtered)}/${String(resident)}`, rows);
            // Independently establish the never-evicted result on the same scan order.
            const prepared = core.prepare(sql(shape, true, filtered, resident));
            const governor = new Governor(128 << 20);
            const snapshot = core.harness.path.pin();
            const execution = prepared.execute({
              snapshot,
              statement: null,
              parameters: [""],
              statementTime: 0n,
              governor,
              spill: core.spill,
              queryId: 100,
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
              reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
            });
            try {
              expect(await consume(prepared, execution, new Uint8Array(0))).toEqual(rows);
              expect(
                (await execution.executed).profile.operators.every((op) => op.spilledBytes === 0),
              ).toBe(true);
            } finally {
              execution.cancel();
              await Promise.allSettled([execution.executed, execution.delivered]);
              snapshot.release();
            }
            expect(governor.usedBytes).toBe(0);
          }
        }
      }
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 3 }),
          fc.integer({ min: 0, max: 65536 }),
          fc.constantFrom<Shape>("scalar", "skew", "many"),
          fc.constantFrom("none", "wait", "restore"),
          fc.boolean(),
          fc.boolean(),
          async (count, size, shape, cancellation, filtered, residentFollower) => {
            const cancel = cancellation === "wait";
            const grouped = shape !== "scalar";
            reach.beginHistory();
            const prepared = core.prepare(sql(shape, true, filtered)),
              resident = core.prepare(sql(shape, true, filtered, true)),
              // Preserve the original operator headroom after the shared owner's prepay
              // and the governor's one-eighth progress reserve (at most one byte rounding).
              sharedMinimum = grouped ? SCRATCH_CONTROL_BYTES : 0,
              governor = new Governor(Math.ceil(((((24 << 20) * 7) / 8 + sharedMinimum) * 8) / 7)),
              pinned = await governor.admit({
                label: "temporarily pinned competitor",
                minimumBytes: 0,
                spillable: true,
              }),
              snapshot = core.harness.path.pin(),
              executions: Execution[] = [],
              jobs: Array<Promise<unknown>> = [];
            const activity = new Set<() => void>(),
              advance = () => {
                for (const notify of activity) notify();
              };
            const spillControl = grouped
              ? await governor.admit({
                  label: "shared scratch metadata",
                  minimumBytes: SCRATCH_CONTROL_BYTES,
                  spillable: false,
                })
              : null;
            const scratch = spillControl
              ? new ScratchSpillService(
                  new FileScratch(
                    new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
                    // Synchronous scratch I/O during cancellation is real cleanup progress.
                    advance,
                    new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
                  ),
                  { budget: spillControl },
                )
              : null;
            const spill = scratch ?? core.spill;
            const marker = Uint8Array.from({ length: size }, (_, i) => 97 + (i % 26)),
              callers = Array.from({ length: count }, () => [new TextDecoder().decode(marker)]);
            const observed = { pinned: false };
            let pressured = false,
              phaseRequests = 0,
              cancelled = -1,
              cancelPrepared = false;
            let followersReady: () => void = () => undefined;
            const followers = new Promise<void>((resolve) => {
              followersReady = resolve;
            });
            // eslint-disable-next-line @typescript-eslint/unbound-method -- The original method is invoked with .call(this) inside the observer.
            const suspend = QueryRuntime.prototype.suspendProducer;
            const retirement = vi
              .spyOn(QueryRuntime.prototype, "suspendProducer")
              .mockImplementation(async function (this: QueryRuntime, sink, pool) {
                await suspend.call(this, sink, pool);
                if (
                  this.request.governor === governor &&
                  cancel &&
                  this.request.queryId === 1 &&
                  !cancelPrepared
                ) {
                  expect(this.params.slots).toHaveLength(0);
                  const room = Math.max(
                    0,
                    governor.capBytes - governor.reserveBytes - governor.usedBytes,
                  );
                  expect(pinned.allocate(room)).toBe(true);
                  cancelPrepared = true;
                }
              });
            // Start cancellation only after the sealed restore graph has actually opened.
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with .call(this).
            const restore = RestoredAggregateOperator.prototype.finish;
            const restoration = vi
              .spyOn(RestoredAggregateOperator.prototype, "finish")
              .mockImplementation(function (this: RestoredAggregateOperator) {
                const work = restore.call(this);
                if (
                  this.shared.spill === spill &&
                  this.shared.queryId === 1 &&
                  cancellation === "restore" &&
                  cancelled < 0
                ) {
                  cancelled = 0;
                  queueMicrotask(() => {
                    executions[0]?.cancel(new Error("cancel running restore pass"));
                    reach.reach("cancel restore pass");
                  });
                }
                return work;
              });
            const phaseQueries = new Set<number>(),
              native = governor.admitGroup.bind(governor);
            let releaseScheduled = false;
            const releaseHolder = () => {
              if (cancel || releaseScheduled || phaseQueries.size === 0) return;
              releaseScheduled = true;
              // The competing holder's completion must be independent of later
              // execution admissions. A fresh sizing request may be behind this
              // restore minimum in FIFO order; waiting for it would fake a cycle.
              queueMicrotask(() => {
                pinned.close();
                reach.reach("holder releases");
              });
            };
            governor.admitGroup = (options) => {
              const wanted = options.grants.reduce((n, part) => n + part.minimumBytes, 0);
              const available =
                governor.capBytes - governor.usedBytes + (options.idleCredit?.held ?? 0);
              const before = governor.usedBytes;
              const result = native(options);
              if (options.idleCredit && options.grants.some((part) => part.minimumBytes > 0)) {
                phaseRequests++;
                reach.reach("pressure checkpoint");
                const id = Number(/^query#(\d+)/.exec(options.label)?.[1]);
                phaseQueries.add(id);
                expect(snapshot.released).toBe(false);
                reach.reach("snapshot retained");
                if (cancel && id === 1 && cancelPrepared && cancelled < 0) {
                  // The whole restore cannot fit, even after adopting the idle result
                  // pool. No reclaimable producer remains, so this is a real FIFO wait.
                  expect(wanted).toBeGreaterThan(available);
                  expect(governor.usedBytes).toBe(before);
                  for (let i = 1; i < count; i++) start(i);
                  queueMicrotask(() => {
                    cancelled = 0;
                    executions[0]?.cancel(new Error("cancel pending aggregate phase"));
                    reach.reach("cancel pending phase");
                    pinned.close();
                    reach.reach("holder releases");
                    followersReady();
                  });
                } else releaseHolder();
                return result.then((parts) => {
                  expect(snapshot.released).toBe(false);
                  advance();
                  reach.reach("restore admission");
                  return parts;
                });
              }
              return result;
            };
            const start = (i: number) => {
              const caller = callers[i];
              if (!caller) throw new Error("Missing caller bytes");
              const chosen = residentFollower && i === count - 1 ? resident : prepared;
              const execution = chosen.execute({
                snapshot,
                statement: null,
                parameters: caller,
                statementTime: 0n,
                governor,
                spill,
                queryId: i + 1,
                sequences: null,
                sequenceSession: null,
                returning: "atExecuted",
                reads: {
                  open: (pages, chunk, options) => {
                    if (!pressured && chosen === prepared) {
                      pressured = true;
                      const room = Math.max(
                        0,
                        governor.capBytes - governor.reserveBytes - governor.usedBytes,
                      );
                      if (room > 0 && pinned.allocate(room)) {
                        observed.pinned = true;
                        reach.reach("temporary pinned holder");
                      }
                    }
                    // Opening the admitted scan is after parameter copying, before any handoff.
                    caller[0] = "caller changed after binding";
                    return ChunkReader.open(pages, chunk, options);
                  },
                },
              });
              executions[i] = execution;
              jobs[i] = consume(chosen, execution, marker, advance).catch((error: unknown) => {
                // An oracle/consumer failure must not leave the producer waiting on
                // frame credit, or a pinned test holder masking the original error.
                execution.cancel(error);
                const plannedCancellation =
                  i === cancelled &&
                  error !== null &&
                  typeof error === "object" &&
                  "code" in error &&
                  error.code === "MINNOW_QUERY_CANCELLED";
                if (!plannedCancellation) pinned.close();
                return error;
              });
            };
            try {
              if (cancel) {
                start(0);
                const first = jobs[0];
                if (!first) throw new Error("Missing first phase execution");
                await deadline(
                  Promise.race([
                    followers,
                    first.then((error) => {
                      throw error instanceof Error
                        ? error
                        : new Error("Query finished without reaching its queued phase");
                    }),
                  ]),
                  activity,
                );
              } else for (let i = 0; i < count; i++) start(i);
              const results = await deadline(Promise.all(jobs), activity);
              for (let i = 0; i < results.length; i++) {
                if (i === cancelled)
                  expect(results[i]).toMatchObject({
                    code: "MINNOW_QUERY_CANCELLED",
                    sqlState: "57014",
                  });
                else {
                  if (results[i] instanceof Error) throw results[i];
                  // A restore-order mutant must first fail on float transition order,
                  // before a simpler missing-count mismatch can mask it.
                  const actual = results[i] as ReturnType<typeof canonical>;
                  const inMemory = residentFollower && i === count - 1;
                  if (inMemory) {
                    expect(phaseQueries.has(i + 1)).toBe(false);
                    reach.reach("resident follower");
                    if (filtered && actual.some((row) => row.a === null))
                      reach.reach("empty filtered resident state");
                  }
                  expect(actual.map((row) => [row.g, row.s, row.a])).toEqual(
                    expected
                      .get(`${shape}/${String(filtered)}/${String(inMemory)}`)
                      ?.map((row) => [row.g, row.s, row.a]),
                  );
                  expect(actual).toEqual(
                    expected.get(`${shape}/${String(filtered)}/${String(inMemory)}`),
                  );
                }
              }
              expect(phaseRequests).toBeGreaterThan(0);
              if (cancel) expect(cancelled).toBeGreaterThanOrEqual(0);
              if (observed.pinned) expect(pinned.held).toBe(0);
              reach.reach("several executions");
              reach.reach(grouped ? "grouped" : "scalar");
              if (shape === "skew") reach.reach("skewed dominant group");
              if (shape === "many") reach.reach("many small groups");
              if (filtered) reach.reach("filtered transitions");
              reach.reach("owned parameter");
              if (size === 0) reach.reach("empty owned parameter");
              reach.reach("PG exact");
            } finally {
              retirement.mockRestore();
              restoration.mockRestore();
              pinned.close();
              for (const e of executions) e.cancel(new Error("pressure cleanup"));
              await Promise.allSettled(jobs);
              await Promise.allSettled(executions.flatMap((e) => [e.executed, e.delivered]));
              snapshot.release();
              try {
                expect(spill.bytesInUse).toBe(0);
                expect(governor.usedBytes).toBe(spillControl?.held ?? 0);
              } finally {
                await scratch?.close();
                spillControl?.close();
              }
            }
            expect(governor.usedBytes).toBe(0);
            if (scratch) reach.reach("scratch service");
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 14,
          examples: [
            [2, 0, "scalar", "none", false, false],
            [2, 4096, "scalar", "none", false, false],
            [2, 16384, "skew", "none", true, false],
            [2, 4096, "many", "none", false, false],
            [3, 256, "scalar", "wait", true, false],
            [2, 8192, "skew", "restore", false, false],
            [2, 0, "scalar", "none", false, true],
            [3, 4096, "many", "none", true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await core.close();
    }
  },
  360000,
);
