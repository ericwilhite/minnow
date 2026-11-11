import { JoinCheckpoint } from "./join-checkpoint.js";
import { RecordCheckpoint } from "./record-checkpoint.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { SpillFiles, PageWriter } from "./spill-pages.js";
import { vectorOf } from "./join-fixtures.js";
/** Cancellation cannot retire native credit before checkpoint IO settles. */
import { expect, it, afterAll } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RETAINED_PARENT_IO_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinSpec } from "./join.js";
import { WindowOperator, windowRetainingMinimumBytes, type WindowSpec } from "./window.js";
import { INT, TEXT, batchesOf, CollectSink } from "./join-fixtures.js";
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const reach = createReach("parent checkpoint IO", RETAINED_PARENT_IO_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-parent-checkpoint-io", [0xb7a1, 0xb7a2]))
  for (const kind of ["join", "window"] as const)
    for (const mode of ["append", "read"] as const)
      for (const { fault, cancel } of [
        { fault: false, cancel: true },
        { fault: true, cancel: true },
        { fault: true, cancel: false },
      ])
        it(`${kind} ${mode} cancellation drains (${String(seed)}, fault=${String(fault)},cancel=${String(cancel)})`, async () => {
          reach.beginHistory();
          const governor = new Governor(64 * 1024 * 1024),
            spill = new MemorySpillService();
          const entered = deferred(),
            release = deferred();
          let armed = false,
            borrowed = false;
          const create = spill.create.bind(spill);
          spill.create = async (...args) => {
            const file = await create(...args);
            const append = file.append.bind(file),
              read = file.read.bind(file);
            file.append = async (page) => {
              if (armed && mode === "append" && !borrowed) {
                borrowed = true;
                entered.resolve();
                await release.promise;
                if (fault) throw new Error("checkpoint append fault");
              }
              return append(page);
            };
            file.read = async (ref) => {
              if (armed && mode === "read" && !borrowed) {
                borrowed = true;
                entered.resolve();
                await release.promise;
                if (fault) throw new Error("checkpoint read fault");
              }
              return read(ref);
            };
            return file;
          };
          const types = [INT, TEXT],
            sink = new CollectSink();
          const js: HashJoinSpec = {
            kind: "full",
            probeTypes: types,
            buildTypes: types,
            probeKeys: [0],
            buildKeys: [0],
            nullSafe: [false],
            residual: null,
            publishes: [],
          };
          const ws: WindowSpec = {
            checkpointable: true,
            types,
            partitionBy: [],
            orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
            functions: [{ fn: "rank", arg: null, frame: null, type: { kind: "int64" } }],
          };
          const grant = await governor.admit({
            label: kind,
            minimumBytes:
              kind === "join" ? hashJoinMinimumBytes(js) : windowRetainingMinimumBytes(types),
            spillable: true,
          });
          const old =
            kind === "join"
              ? new HashJoin(js, { grant, spill, queryId: 1 })
              : new WindowOperator(ws, { grant, spill, queryId: 1 }, sink);
          const input = old instanceof HashJoin ? old.build : old;
          for (const batch of batchesOf(types, [
            [1, "a".repeat(1024)],
            [2, "b"],
          ]))
            if (input.push(batch) === "blocked") await input.ready();
          let work: Promise<unknown>,
            actor: HashJoin | WindowOperator = old;
          let saved: Awaited<ReturnType<typeof old.checkpoint>> | undefined;
          if (mode === "read") {
            saved = await old.checkpoint();
            old.abort(new Error("retire"));
            await old.closed;
            const restoredGrant = await governor.admit({
              label: "restored",
              minimumBytes:
                (kind === "join" ? hashJoinMinimumBytes(js) : windowRetainingMinimumBytes(types)) +
                saved.minimumBytes,
              spillable: true,
            });
            if (old instanceof HashJoin) {
              actor = new HashJoin(
                js,
                { grant: restoredGrant, spill, queryId: 1 },
                saved as Awaited<ReturnType<HashJoin["checkpoint"]>>,
              );
              armed = true;
              work = actor.restore(saved as Awaited<ReturnType<HashJoin["checkpoint"]>>);
            } else {
              actor = new WindowOperator(ws, { grant: restoredGrant, spill, queryId: 1 }, sink);
              armed = true;
              work = actor.restore(saved as Awaited<ReturnType<WindowOperator["checkpoint"]>>);
            }
          } else {
            armed = true;
            work = old.checkpoint();
          }
          void work.catch(() => undefined);
          try {
            await entered.promise;
            if (cancel) actor.abort(new Error("cancel checkpoint"));
            if (cancel) {
              let closed = false;
              void actor.closed.then(
                () => {
                  closed = true;
                },
                () => {
                  closed = true;
                },
              );
              await Promise.resolve();
              expect(closed).toBe(false);
              expect(governor.usedBytes).toBeGreaterThan(0);
              reach.reach("cancel fence");
            }
            release.resolve();
            const failure: unknown = await work.catch((error: unknown) => error);
            expect(failure).toBeInstanceOf(Error);
            if (fault && !cancel)
              expect(failure).toMatchObject({ message: `checkpoint ${mode} fault` });
            actor.abort(new Error("cleanup after work"));
            await actor.closed;
            if (fault) reach.reach("fault cleanup");
            reach.reach(kind);
            reach.reach(mode);
          } finally {
            release.resolve();
            await work.catch(() => undefined);
            actor.abort(new Error("cleanup"));
            await actor.closed;
            await saved?.close();
            expect(governor.usedBytes).toBe(0);
            expect(spill.bytesInUse).toBe(0);
          }
        }, 30000);

for (const seed of seedsFor("next-parent-checkpoint-close", [0xb7b1, 0xb7b2]))
  it(`saved join close drains every owner before surfacing a fault (${String(seed)})`, async () => {
    reach.beginHistory();
    const governor = new Governor(16 * 1024 * 1024),
      service = new MemorySpillService();
    const grant = await governor.admit({
      label: "checkpoint IO",
      minimumBytes: 1024 * 1024,
      spillable: true,
    });
    const charge = new Charge(grant),
      pool = new Pool(charge, 256 * 1024),
      pages = new PageWriter(pool, 16 * 1024),
      files = new SpillFiles(service, 1, charge);
    const io = new SpillIo(pool, files, pages, { grant, queryId: 1 }, () => undefined);
    const entered = deferred(),
      release = deferred();
    let armed = false,
      index = 0;
    const create = service.create.bind(service);
    const closes = new Map<number, number>();
    service.create = async (...args) => {
      const file = await create(...args),
        id = index++,
        close = file.close.bind(file);
      file.close = async () => {
        closes.set(id, (closes.get(id) ?? 0) + 1);
        if (armed && id === 2 + (seed % 2)) {
          entered.resolve();
          await release.promise;
        }
        await close();
        if (armed && id === seed % 2) throw new Error("saved close fault");
      };
      return file;
    };
    const build = new RecordCheckpoint(io, service, [INT]),
      probe = new RecordCheckpoint(io, service, [INT]);
    await build.write([vectorOf(INT, [seed])], 0);
    await build.seal();
    await probe.write([vectorOf(INT, [seed + 1])], 0);
    await probe.seal();
    const saved = new JoinCheckpoint(build, probe, true, false, {
      rowsOut: 0,
      batchesOut: 0,
      buildRows: 1,
      probeRows: 1,
      spilledBytes: 0,
      spillPasses: 0,
      chunkPasses: 0,
      loans: 0,
      largestLoan: 0,
      forwardedPages: 0,
      peakHeldBytes: 0,
      table: null,
    });
    armed = true;
    const closing = saved.close();
    let settled = false;
    void closing.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    try {
      await entered.promise;
      await new Promise((done) => setTimeout(done, 0));
      expect(settled).toBe(false);
      expect(saved.close()).toBe(closing);
      reach.reach("close drains both owners");
      release.resolve();
      await expect(closing).rejects.toBeInstanceOf(AggregateError);
      expect([...closes.values()].every((n) => n === 1)).toBe(true);
      reach.reach("close once after failure");
    } finally {
      release.resolve();
      await closing.catch(() => undefined);
      pages.close();
      pool.close();
      await files.closeAll();
      grant.close();
      expect(service.bytesInUse).toBe(0);
      expect(governor.usedBytes).toBe(0);
    }
  });

for (const cancel of [false, true])
  it(`window checkpoint spends the shared task deadline across resolved appends (cancel=${String(cancel)})`, async () => {
    reach.beginHistory();
    const { vi } = await import("vitest");
    const { taskClock, beginTask } = await import("./pipeline.js");
    const governor = new Governor(64 * 1024 * 1024),
      spill = new MemorySpillService(),
      types = [INT, TEXT];
    const grant = await governor.admit({
      label: "window clock",
      minimumBytes: windowRetainingMinimumBytes(types),
      spillable: true,
    });
    const actor = new WindowOperator(
      {
        checkpointable: true,
        types,
        partitionBy: [],
        orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
        functions: [{ fn: "rank", arg: null, frame: null, type: { kind: "int64" } }],
      },
      { grant, spill, queryId: 2 },
      new CollectSink(),
    );
    for (const batch of batchesOf(
      types,
      Array.from({ length: 1000 }, (_, i) => [i, "clock"]),
    ))
      if (actor.push(batch) === "blocked") await actor.ready();
    let elapsed = 0,
      serviced = false;
    const create = spill.create.bind(spill);
    spill.create = async (...args) => {
      const file = await create(...args),
        append = file.append.bind(file);
      file.append = async (page) => {
        elapsed += 5;
        return append(page);
      };
      return file;
    };
    const spy = vi.spyOn(taskClock, "now").mockImplementation(() => elapsed);
    beginTask();
    const reason = new Error("cancel at task boundary");
    const task = setImmediate(() => {
      serviced = true;
      if (cancel) actor.abort(reason);
    });
    let saved: Awaited<ReturnType<WindowOperator["checkpoint"]>> | undefined;
    try {
      const work = actor.checkpoint();
      if (cancel) {
        await expect(work).rejects.toBe(reason);
        reach.reach("abort task resumption");
      } else saved = await work;
      expect(serviced, "checkpoint must yield after its task slice expires").toBe(true);
      reach.reach("task deadline");
    } finally {
      clearImmediate(task);
      actor.abort(new Error("cleanup"));
      await actor.closed;
      await saved?.close();
      spy.mockRestore();
      beginTask();
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    }
  });

for (const seed of seedsFor("next-parent-checkpoint-reclaim", [0xb7c1, 0xb7c2]))
  it(`window reclamation waits for its checkpoint borrow (${String(seed)})`, async () => {
    reach.beginHistory();
    const { TaskClock } = await import("./pipeline.js");
    const governor = new Governor(64 * 1024 * 1024),
      spill = new MemorySpillService(),
      types = [INT, TEXT];
    const nativeGrant = await governor.admit({
      label: "resident checkpoint reclamation",
      minimumBytes: windowRetainingMinimumBytes(types) + 8 * 1024 * 1024,
      spillable: true,
    });
    let reclaim: (() => unknown) | undefined;
    const grant = new Proxy(nativeGrant, {
      get(target, property) {
        if (property === "onReclaimRequest")
          return (callback: () => unknown) => {
            reclaim = callback;
            target.onReclaimRequest(callback);
          };
        const value: unknown = Reflect.get(target, property);
        return typeof value === "function" ? (value.bind(target) as unknown) : value;
      },
    });
    const actor = new WindowOperator(
      {
        checkpointable: true,
        types,
        partitionBy: [],
        orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
        functions: [{ fn: "rank", arg: null, frame: null, type: { kind: "int64" } }],
      },
      { grant, spill, queryId: seed, clock: new TaskClock(() => 0) },
      new CollectSink(),
    );
    const rows = 8192 + (seed % 64);
    for (const batch of batchesOf(
      types,
      Array.from({ length: rows }, (_, i) => [rows - i, "payload".repeat(7)]),
    ))
      if (actor.push(batch) === "blocked") await actor.ready();
    expect(actor.profile.sort.runs, "multiple resident chunks, no original spill").toBe(0);
    const entered = deferred(),
      release = deferred();
    let gated = false;
    const create = spill.create.bind(spill);
    spill.create = async (...args) => {
      const file = await create(...args),
        append = file.append.bind(file);
      file.append = async (page) => {
        if (!gated) {
          gated = true;
          entered.resolve();
          await release.promise;
        }
        return append(page);
      };
      return file;
    };
    const work = actor.checkpoint();
    void work.catch(() => undefined);
    let saved: Awaited<ReturnType<WindowOperator["checkpoint"]>> | undefined,
      reclaiming: Promise<unknown> | undefined;
    try {
      await entered.promise;
      expect(reclaim).toBeTypeOf("function");
      let reclaimed = false;
      reclaiming = Promise.resolve(reclaim?.()).then(
        () => {
          reclaimed = true;
        },
        (error: unknown) => {
          reclaimed = true;
          throw error;
        },
      );
      void reclaiming.catch(() => undefined);
      // All ungated IO resolves in microtasks and the private clock never expires.
      // A task turn therefore distinguishes waiting on the borrowed checkpoint from
      // a reclaimer that sorts/truncates the resident chunks behind its back.
      await new Promise<void>((done) => setImmediate(done));
      expect(reclaimed, "reclamation must not mutate a checkpoint's borrowed chunks").toBe(false);
      reach.reach("reclaim waits for checkpoint");
      release.resolve();
      saved = await work;
      await reclaiming;
      expect(saved.records.tape.rows).toBe(rows);
    } finally {
      release.resolve();
      saved ??= await work.catch(() => undefined);
      await reclaiming?.catch(() => undefined);
      actor.abort(new Error("cleanup"));
      await actor.closed;
      await saved?.close();
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    }
  }, 30000);
