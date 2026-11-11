/** Native descriptors remain owned across every controller await, for both producers. */
import { AsyncLocalStorage } from "node:async_hooks";
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RETAINING_LIFECYCLE_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { Rel } from "../ir/ir.js";
import type { Execution } from "../exec/executor.js";
import { HybridAggregateOperator } from "../exec/agg/hybrid.js";
import { JsonPhaseOperator } from "../exec/json-phase.js";
import { HashJoin } from "../exec/join.js";
import { WindowOperator } from "../exec/window.js";
import { JoinCheckpoint } from "../exec/join-checkpoint.js";
import { WindowCheckpoint } from "../exec/window-checkpoint.js";
import { QueryRuntime } from "../exec/run.js";
import { MemorySpillFile } from "../spill/memory-spill.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const ddl = [
  "CREATE TABLE t(k INTEGER,doc TEXT,x DOUBLE PRECISION)",
  "CREATE TABLE u(k INTEGER,ord INTEGER)",
  "CREATE TABLE agg(k INTEGER,v TEXT)",
];
const jsonRows = Array.from({ length: 6 }, (_, i) => ({
  k: i === 5 ? null : i % 3,
  doc: `1e${String(6000 + i * 2000)}`,
  x: i === 0 ? 1e16 : i === 1 ? 1 : i === 2 ? -1e16 : i,
}));
const otherRows = [0, 0, 1, 4, null].map((k, ord) => ({ k, ord }));
const aggregateRows = Array.from({ length: 8192 }, (_, i) => ({
  k: i % 4,
  v: String(i) + "x".repeat(240),
}));
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  for (const sql of ddl) await pg.exec(sql);
  for (const r of jsonRows) await pg.query("INSERT INTO t VALUES ($1,$2,$3)", [r.k, r.doc, r.x]);
  for (const r of otherRows) await pg.query("INSERT INTO u VALUES ($1,$2)", [r.k, r.ord]);
  for (let from = 0; from < aggregateRows.length; from += 256) {
    const batch = aggregateRows.slice(from, from + 256);
    await pg.query(
      `INSERT INTO agg VALUES ${batch.map((_, i) => `($${String(i * 2 + 1)},$${String(i * 2 + 2)})`).join(",")}`,
      batch.flatMap((r) => [r.k, r.v]),
    );
  }
}, 60000);
afterAll(async () => {
  await pg.close();
});

type Producer = "json" | "aggregate" | "mixed";
type Edge = "checkpoint" | "suspend" | "admission" | "restore" | "close";
type Outcome = "resume" | "cancel" | "fault";
const producers: Producer[] = ["json", "aggregate", "mixed"];
const edges: Edge[] = ["checkpoint", "suspend", "admission", "restore", "close"];
const outcomes: Outcome[] = ["resume", "cancel", "fault"];
const examples: Array<[Producer, Edge, Outcome, boolean, boolean, number]> = producers.flatMap(
  (p, pi) =>
    edges.flatMap((edge, ei) =>
      outcomes.map((outcome, oi): [Producer, Edge, Outcome, boolean, boolean, number] => [
        p,
        edge,
        outcome,
        (pi + ei + oi) % 2 === 0,
        (ei + oi) % 2 === 0,
        1 + (ei % 3),
      ]),
    ),
);

it.each(seedsFor("next-retaining-lifecycle", [0xb6a4, 0xb6a5]))(
  "preserves owners, snapshots and bindings through pressured SQL continuations (seed %i)",
  async (seed) => {
    const reach = createReach<string>("retaining controller lifecycle", RETAINING_LIFECYCLE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...producers),
        fc.constantFrom(...edges),
        fc.constantFrom(...outcomes),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 3 }),
        async (producer, edge, outcome, concurrent, large, limit) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          const governor = new Governor(128 << 20),
            scope = new AsyncLocalStorage<number>();
          for (const sql of ddl) await core.execute(sql);
          await core.writeBatch("insertBatch", "t", jsonRows);
          await core.writeBatch("insertBatch", "u", otherRows);
          if (producer !== "json") await core.writeBatch("insertBatch", "agg", aggregateRows);
          await core.harness.path.recycleLog();
          await core.harness.path.quiesce();
          const aggregate = "(SELECT k,count(DISTINCT v) n FROM agg GROUP BY k)";
          const source =
            producer === "json"
              ? "(SELECT k,doc::jsonb j,x FROM t)"
              : producer === "aggregate"
                ? aggregate
                : `(SELECT k,n,'1e12000'::jsonb j FROM ${aggregate} z)`;
          const json = producer !== "aggregate";
          const float = producer === "json";
          const sql = `SELECT a.k,${json ? "a.j::text j," : ""}${producer !== "json" ? "a.n," : ""}$1::text marker,b.ord AS o,rank() OVER(ORDER BY a.k NULLS FIRST) r${float ? ",sum(a.x) OVER(ORDER BY a.k NULLS FIRST,a.x NULLS FIRST,b.ord NULLS FIRST ROWS UNBOUNDED PRECEDING) s" : ""} FROM ${source} a FULL JOIN u b ON a.k=b.k ORDER BY a.k NULLS FIRST,${float ? "a.x NULLS FIRST," : ""}b.ord NULLS FIRST LIMIT ${String(limit)}`;
          const marker = large ? "owned-" + "汉".repeat(24000) : "owned-marker";
          const expected = (await pg.query(sql, [marker])).rows;
          const callers: [string[], string[]] = [[marker], [marker]];
          const executions = new Map<number, Execution>();
          const openFiles = new Set<object>();
          const create = core.spill.create.bind(core.spill);
          const files = vi.spyOn(core.spill, "create").mockImplementation(async (id) => {
            const file = await create(id);
            openFiles.add(file);
            const close = file.close.bind(file);
            file.close = async () => {
              try {
                await close();
              } finally {
                openFiles.delete(file);
              }
            };
            return file;
          });
          const baseWindows = new Map<number, number>();
          const savedWindows = new Set<WindowCheckpoint>();
          const savedJoins = new Set<JoinCheckpoint>(),
            buildIds = new Set<number>();
          const savedOwners = new WeakMap<object, number>();
          let target: number | undefined,
            attempts = 0,
            jsonCuts = 0,
            aggregateCuts = 0,
            checkpointDepth = 0,
            restoreDepth = 0;
          const tripState = { done: false };
          let wrote = false,
            pressured = false,
            installing = false,
            held: Grant | undefined;
          const pinned = await governor.admit({
            label: "competing producer",
            minimumBytes: 0,
            spillable: true,
          });
          const accounts = new Set<Grant>();
          const injected = new Error("retaining controller injected " + edge);
          const nativePrepare = core.executor.prepare.bind(core.executor);
          const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
            const prepared = nativePrepare(plan);
            return {
              ...prepared,
              execute: (request) => {
                if (target === undefined) {
                  target = request.queryId;
                  let node = prepared.plan.root;
                  while ("input" in node) node = node.input;
                  if (node.op !== "join")
                    throw new Error("Expected the fixture's retaining FULL join");
                  const visit = (rel: Rel): void => {
                    buildIds.add(rel.id);
                    if ("input" in rel) visit(rel.input);
                    if ("left" in rel) {
                      visit(rel.left);
                      visit(rel.right);
                    }
                    if ("inputs" in rel) for (const child of rel.inputs) visit(child);
                  };
                  visit(node.right);
                }
                const execution = scope.run(request.queryId, () =>
                  prepared.execute({ ...request, governor }),
                );
                executions.set(request.queryId, execution);
                return execution;
              },
            };
          });
          const isTarget = (id: number | undefined) => target !== undefined && id === target;
          const trip = async (owner: number | undefined) => {
            if (tripState.done || !isTarget(owner)) return;
            tripState.done = true;
            reach.reach("edge " + edge);
            reach.reach(outcome);
            callers[0][0] = "caller-mutated-after-bind";
            if (outcome === "resume") return;
            pinned.close();
            held?.close();
            await Promise.resolve();
            if (outcome === "cancel") {
              const execution = target === undefined ? undefined : executions.get(target);
              expect(execution).toBeDefined();
              execution?.cancel(injected);
            }
            throw injected;
          };
          const observers: Array<() => void> = [];
          let followerReady: () => void = () => undefined;
          const followerReadyPromise = new Promise<void>((resolve) => {
            followerReady = resolve;
          });
          const follower: { started: boolean; start: () => void } = {
            started: false,
            start: () => {
              throw new Error("Follower launch not installed");
            },
          };
          const observed = new WeakSet();
          // eslint-disable-next-line @typescript-eslint/unbound-method -- called with its actual receiver
          const hybridPush = HybridAggregateOperator.prototype.push;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual JSON producer
          const jsonPush = JsonPhaseOperator.prototype.push;
          const aggregateObserver = vi
            .spyOn(HybridAggregateOperator.prototype, "push")
            .mockImplementation(function (this: HybridAggregateOperator, batch) {
              if (this.shared.handoff && !observed.has(this.shared)) {
                observed.add(this.shared);
                const original = this.shared.handoff;
                const spy = vi.spyOn(this.shared, "handoff").mockImplementation(async (phase) => {
                  if (isTarget(this.shared.queryId)) {
                    attempts++;
                    aggregateCuts++;
                    follower.start();
                    if (concurrent) await followerReadyPromise;
                  }
                  return scope.run(this.shared.queryId, () => original(phase));
                });
                observers.push(() => spy.mockRestore());
              }
              if (
                isTarget(this.shared.queryId) &&
                !pressured &&
                this.rel.aggregates.some((c) => c.fn === "count" && c.distinct)
              ) {
                const free = Math.max(
                  0,
                  governor.capBytes - governor.reserveBytes - governor.usedBytes,
                );
                expect(pinned.allocate(free)).toBe(true);
                pressured = true;
              }
              return hybridPush.call(this, batch);
            });
          const jsonObserver = vi
            .spyOn(JsonPhaseOperator.prototype, "push")
            .mockImplementation(function (this: JsonPhaseOperator, ...args) {
              if (!observed.has(this.shared)) {
                observed.add(this.shared);
                const original = this.shared.handoff;
                const spy = vi.spyOn(this.shared, "handoff").mockImplementation(async (phase) => {
                  if (isTarget(this.shared.queryId)) {
                    attempts++;
                    jsonCuts++;
                    follower.start();
                    if (concurrent) await followerReadyPromise;
                  }
                  return scope.run(this.shared.queryId, () => original(phase));
                });
                observers.push(() => spy.mockRestore());
              }
              return jsonPush.apply(this, args);
            });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- all native calls retain their receiver
          const joinCheckpoint = HashJoin.prototype.checkpoint;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual window
          const windowCheckpoint = WindowOperator.prototype.checkpoint;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual join
          const joinRestore = HashJoin.prototype.restore;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual window
          const windowRestore = WindowOperator.prototype.restore;
          const jc = vi.spyOn(HashJoin.prototype, "checkpoint").mockImplementation(async function (
            this: HashJoin,
          ) {
            const owner = scope.getStore();
            if (isTarget(owner)) checkpointDepth++;
            try {
              const saved = await joinCheckpoint.call(this);
              if (owner !== undefined) savedOwners.set(saved, owner);
              if (isTarget(owner)) savedJoins.add(saved);
              return saved;
            } finally {
              if (isTarget(owner)) checkpointDepth--;
            }
          });
          const wc = vi
            .spyOn(WindowOperator.prototype, "checkpoint")
            .mockImplementation(async function (this: WindowOperator) {
              const owner = scope.getStore();
              if (isTarget(owner)) checkpointDepth++;
              try {
                const saved = await windowCheckpoint.call(this);
                if (owner !== undefined) savedOwners.set(saved, owner);
                if (isTarget(owner)) savedWindows.add(saved);
                return saved;
              } finally {
                if (isTarget(owner)) checkpointDepth--;
              }
            });
          const jr = vi.spyOn(HashJoin.prototype, "restore").mockImplementation(async function (
            this: HashJoin,
            saved,
          ) {
            const owner = savedOwners.get(saved);
            if (isTarget(owner)) restoreDepth++;
            try {
              const result = await joinRestore.call(this, saved);
              if (isTarget(owner)) savedJoins.delete(saved);
              return result;
            } finally {
              if (isTarget(owner)) restoreDepth--;
            }
          });
          const wr = vi
            .spyOn(WindowOperator.prototype, "restore")
            .mockImplementation(async function (this: WindowOperator, saved) {
              const owner = savedOwners.get(saved);
              if (isTarget(owner)) restoreDepth++;
              try {
                const result = await windowRestore.call(this, saved);
                if (isTarget(owner)) savedWindows.delete(saved);
                return result;
              } finally {
                if (isTarget(owner)) restoreDepth--;
              }
            });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- all native calls retain their receiver
          const append = MemorySpillFile.prototype.append;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual file
          const read = MemorySpillFile.prototype.read;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual descriptor
          const joinClose = JoinCheckpoint.prototype.close;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual descriptor
          const windowClose = WindowCheckpoint.prototype.close;
          const appends = vi
            .spyOn(MemorySpillFile.prototype, "append")
            .mockImplementation(async function (this: MemorySpillFile, page) {
              const ref = await append.call(this, page);
              if (edge === "checkpoint" && checkpointDepth > 0) await trip(this.queryId);
              return ref;
            });
          const reads = vi
            .spyOn(MemorySpillFile.prototype, "read")
            .mockImplementation(async function (this: MemorySpillFile, ref) {
              const bytes = await read.call(this, ref);
              if (edge === "restore" && restoreDepth > 0) await trip(this.queryId);
              return bytes;
            });
          const jclose = vi
            .spyOn(JoinCheckpoint.prototype, "close")
            .mockImplementation(async function (this: JoinCheckpoint) {
              await joinClose.call(this);
              if (edge === "close") await trip(savedOwners.get(this));
            });
          const wclose = vi
            .spyOn(WindowCheckpoint.prototype, "close")
            .mockImplementation(async function (this: WindowCheckpoint) {
              await windowClose.call(this);
              if (edge === "close") await trip(savedOwners.get(this));
            });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- called with its original runtime
          const suspend = QueryRuntime.prototype.suspendProducer;
          const retirement = vi
            .spyOn(QueryRuntime.prototype, "suspendProducer")
            .mockImplementation(async function (this: QueryRuntime, ...args) {
              await suspend.apply(this, args);
              if (isTarget(this.request.queryId) && !wrote && outcome === "resume") {
                wrote = true;
                // This publication changes the first ordered row of an already consumed join build. The query keeps its OLD
                // pin; replay against the current view would multiply results and alter rank/sums.
                await core.writeBatch("insertBatch", "u", [{ k: null, ord: -1 }]);
                reach.reach("snapshot publication");
                callers[0][0] = "caller-mutated-after-bind";
              }
              if (edge === "suspend") await trip(this.request.queryId);
            });
          // Admission is observable before constructors/reads. Additional native restore
          // workspace is summed ONCE over immutable first-phase entries, not guessed from
          // whatever global growth happens to remain after old producers have retired.
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked on its runtime
          const reserve = QueryRuntime.prototype.reserve;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual runtime
          const restorePhase = QueryRuntime.prototype.restorePhase;
          const initial = vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(function (
            this: QueryRuntime,
            ...args
          ) {
            if (!isTarget(this.request.queryId) && follower.started) followerReady();
            if (isTarget(this.request.queryId))
              for (const entry of args[0])
                if (entry.rel.op === "window") baseWindows.set(entry.rel.id, entry.minimum);
            return reserve.apply(this, args);
          });
          const admissions = vi
            .spyOn(QueryRuntime.prototype, "restorePhase")
            .mockImplementation(function (this: QueryRuntime, ...args) {
              if (isTarget(this.request.queryId)) {
                if ([...savedJoins].some((saved) => saved.buildComplete)) {
                  expect(
                    args[0].some((entry) =>
                      buildIds.has(
                        "owner" in entry && typeof entry.owner === "number"
                          ? entry.owner
                          : entry.rel.id,
                      ),
                    ),
                    "consumed build subtree omitted from complete restore admission",
                  ).toBe(false);
                  reach.reach("consumed build excluded");
                }
                const actual = args[0]
                  .filter((entry) => entry.rel.op === "window")
                  .reduce(
                    (n, entry) => n + entry.minimum - (baseWindows.get(entry.rel.id) ?? 0),
                    0,
                  );
                const expected = [...savedWindows].reduce((n, saved) => n + saved.minimumBytes, 0);
                expect(
                  actual,
                  "ordinary window restore bytes admitted exactly once before construction",
                ).toBe(expected);
                reach.reach("restore bytes before construction");
              }
              return restorePhase.apply(this, args);
            });
          const nativeGroup = governor.admitGroup.bind(governor);
          governor.admitGroup = async (options) => {
            if (options.idleCredit) {
              accounts.add(options.idleCredit);
              expect(accounts.size, "one shared native idle account, never a per-waiter pool").toBe(
                1,
              );
            }
            const owner = /^query#(\d+)/.exec(options.label)?.[1];
            if (
              isTarget(owner === undefined ? undefined : Number(owner)) &&
              attempts > 0 &&
              options.idleCredit &&
              !installing
            ) {
              installing = true;
              pinned.close();
              [held] = await nativeGroup({
                label: "temporary whole-cap competitor",
                idleCredit: options.idleCredit,
                includesProgress: true,
                grants: [{ label: "held cap", minimumBytes: governor.capBytes, spillable: false }],
              });
              expect(governor.usedBytes).toBe(governor.capBytes);
              reach.reach("queued pressure");
              const waiting = nativeGroup(options);
              try {
                if (edge === "admission")
                  await trip(owner === undefined ? undefined : Number(owner));
                held?.close();
                return await waiting;
              } catch (error) {
                held?.close();
                const late = await waiting.catch(() => null);
                for (const grant of late ?? []) grant.close();
                throw error;
              }
            }
            return nativeGroup(options);
          };
          const restores = [
            () => prepare.mockRestore(),
            () => aggregateObserver.mockRestore(),
            () => jsonObserver.mockRestore(),
            () => jc.mockRestore(),
            () => wc.mockRestore(),
            () => jr.mockRestore(),
            () => wr.mockRestore(),
            () => appends.mockRestore(),
            () => reads.mockRestore(),
            () => jclose.mockRestore(),
            () => wclose.mockRestore(),
            () => retirement.mockRestore(),
            () => initial.mockRestore(),
            () => admissions.mockRestore(),
            () => files.mockRestore(),
          ];
          const work: Array<Promise<unknown>> = [];
          follower.start = () => {
            if (!concurrent || follower.started) return;
            follower.started = true;
            // The producer has ALREADY elected a cut. Its competitor may now retire;
            // allow follower planning to acquire the SAME pool before injecting an edge.
            pinned.close();
            const next = core.execute(sql, callers[1]);
            void next.then(followerReady, followerReady);
            work.push(next);
          };
          try {
            const firstWork = core.execute(sql, callers[0]);
            work.push(firstWork);
            // The target must actually reach its pressure cut before a resident follower
            // can finish and free the very headroom this witness is meant to exhaust.
            await Promise.allSettled([firstWork]);
            if (concurrent) expect(follower.started).toBe(true);
            const result = await Promise.allSettled(work);
            if (!tripState.done) {
              const failure = result.find((item) => item.status === "rejected");
              if (failure) throw failure.reason;
            }
            expect(tripState.done).toBe(true);
            expect(attempts).toBeGreaterThan(0);
            if (outcome === "resume") {
              const first = result[0];
              if (first?.status === "rejected") throw first.reason;
              expect(first?.status).toBe("fulfilled");
              if (first?.status !== "fulfilled") throw new Error("Missing resumed result");
              const value = first.value as Awaited<ReturnType<StorageCore["execute"]>>;
              if (!("batches" in value)) throw new Error("Expected rows");
              expect(oracleRows(value)).toEqual(expected);
              expect(wrote).toBe(true);
              reach.reach("owned parameters");
              reach.reach("demand");
              if (producer === "mixed") {
                expect(aggregateCuts).toBeGreaterThan(0);
                expect(jsonCuts).toBeGreaterThan(0);
                reach.reach("mixed transition");
              }
            } else {
              const first = result[0];
              expect(first?.status).toBe("rejected");
              if (first?.status === "rejected") {
                if (outcome === "cancel")
                  expect(first.reason).toMatchObject({
                    sqlState: "57014",
                    code: "MINNOW_QUERY_CANCELLED",
                  });
                else expect(first.reason).toBe(injected);
              }
            }
            if (concurrent) {
              const second = result[1];
              if (second?.status === "rejected")
                throw new Error("Concurrent survivor failed", { cause: second.reason });
              expect(second?.status).toBe("fulfilled");
              if (second?.status !== "fulfilled") throw new Error("Missing survivor");
              const value = second.value as Awaited<ReturnType<StorageCore["execute"]>>;
              if (!("batches" in value)) throw new Error("Expected rows");
              expect(oracleRows(value)).toEqual(expected);
              reach.reach("concurrent survivor");
            }
            expect(accounts.size).toBe(1);
            reach.reach("one idle account");
            reach.reach("producer " + producer);
            reach.reach(large ? "large binding" : "small binding");
            reach.reach("limit " + String(limit));
          } finally {
            pinned.close();
            held?.close();
            for (const execution of executions.values()) execution.cancel();
            await Promise.allSettled(work);
            for (const restore of restores) restore();
            for (const restore of observers) restore();
            scope.disable();
            try {
              expect(core.spill.bytesInUse).toBe(0);
              expect(openFiles.size).toBe(0);
              expect(governor.usedBytes).toBe(0);
              reach.reach("zero files/grants");
            } finally {
              await core.spill.sweep();
              await core.close();
            }
          }
        },
      ),
      { seed, numRuns: 60, examples },
    );
    reach.assertReached();
  },
  120000,
);
