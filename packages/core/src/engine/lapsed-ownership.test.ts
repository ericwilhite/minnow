/**
 * Work whose own transaction lease lapses while it runs.
 *
 * A transaction owner renews its 30-second lease on a timer and before each store write. Neither
 * runs while the thread is frozen: a laptop asleep, a background tab the browser suspended, a
 * process stopped under a debugger. Code resumes after the freeze in the middle of whatever it
 * was doing, and the wall clock says the lease ran out long ago, so the store refuses to renew
 * it. The soaks met this when the host Mac went into maintenance sleep mid-run: `compactTable`
 * rejected with `TransactionClosedError` on waking, and the vitest timer, firing late, reported
 * a timeout as long as the sleep.
 *
 * A lapsed owner never published, so nothing is lost and the work can begin again — the same
 * rule the engine already applies to a dead tab's fold. These tests move a test clock past the
 * lease before a store call of an operation, the deterministic image of a freeze, at every call
 * on the memory store and at every kind of call on the durable ones, and require the operation
 * to finish as if nothing had happened. One test freezes the real thread; the compaction soak
 * (compaction-soak.test.ts) runs a long seeded history with leases lapsing at random.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { TransactionClosedError } from "../transactions/index.js";
import { MinnowDatabase } from "./database.js";
import {
  heavyTestTimeout,
  instrumentStoreCalls,
  type StoreCallProbe,
} from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const LEASE_MS = 30_000;

/** A clock the test moves. */
function testClock(): { now: () => Date; advance: (ms: number) => void } {
  let current = Date.parse("2026-01-01T00:00:00Z");
  return {
    now: () => new Date(current),
    advance: (ms) => {
      current += ms;
    },
  };
}

const STORES: Array<{ name: string; create: () => Promise<BlockStore>; everyCall: boolean }> = [
  { name: "memory", create: async () => new MemoryBlockStore(), everyCall: true },
  {
    name: "indexeddb",
    create: async () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
    everyCall: false,
  },
  {
    name: "opfs",
    create: async () =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
    everyCall: false,
  },
];

type Store = (typeof STORES)[number];

interface Fixture {
  store: BlockStore;
  database: MinnowDatabase;
  clock: ReturnType<typeof testClock>;
  probe: StoreCallProbe;
}

/**
 * A small keyed table with enough deltas that one compactTable call writes several blocks, or
 * with `bulkRows`, one bulk-loaded table large enough that rewriting it overflows one storage
 * batch, so the rewrite stages through the durable journal its owner lease guards.
 */
async function fixture(kind: Store, bulkRows?: number): Promise<Fixture> {
  const clock = testClock();
  const store = await kind.create();
  const probe = instrumentStoreCalls(store);
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 4,
    autoCompact: false,
    autoCollect: false,
    now: clock.now,
  });
  await database.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)");
  if (bulkRows !== undefined) {
    await database.insertBatch(
      "t",
      Array.from({ length: bulkRows }, (_, id) => ({ id, a: id })),
    );
    return { store, database, clock, probe };
  }
  for (let id = 0; id < 12; id += 1) {
    await database.insert("t", { id, a: id });
    if (id % 3 === 0) await database.execute("UPDATE t SET a = a + 100 WHERE id = ?", [id]);
  }
  return { store, database, clock, probe };
}

async function closeFixture({ database, store }: Fixture): Promise<void> {
  await database.close();
  store.close();
}

const EXPECTED = Array.from({ length: 12 }, (_, id) => ({ id, a: id % 3 === 0 ? id + 100 : id }));

type Operation = (database: MinnowDatabase) => Promise<unknown>;

/**
 * The store calls to lapse the lease before: every one on the memory store; on a durable store,
 * whose fixtures cost more, the first and the last call of each method the operation makes,
 * which reaches every kind of step, the opening and closing ones above all.
 */
async function lapsePoints(kind: Store, operation: Operation, bulkRows?: number) {
  const context = await fixture(kind, bulkRows);
  context.probe.armed = true;
  await operation(context.database);
  context.probe.armed = false;
  await closeFixture(context);
  const { calls } = context.probe;
  if (kind.everyCall) return calls.map((_, index) => index + 1);
  const points = new Set<number>();
  for (const method of new Set(calls)) {
    points.add(calls.indexOf(method) + 1);
    points.add(calls.lastIndexOf(method) + 1);
  }
  return [...points].sort((left, right) => left - right);
}

/**
 * Lapses the lease before the `at`-th store call of the operation and checks that it finished
 * and left no transaction active. The caller checks the data and closes the fixture.
 */
async function lapseAt(
  kind: Store,
  at: number,
  operation: Operation,
  bulkRows?: number,
): Promise<Fixture> {
  const context = await fixture(kind, bulkRows);
  const { store, database, clock, probe } = context;
  probe.onCall = (call) => {
    if (call === at) clock.advance(LEASE_MS + 1);
  };
  probe.armed = true;
  try {
    await operation(database);
  } catch (error) {
    throw new Error(`${kind.name}: the lease lapsed before store call ${String(at)}`, {
      cause: error,
    });
  } finally {
    probe.armed = false;
  }
  const active = (await store.listTransactionPage(null, 1_000)).records.filter(
    (record) => record.status === "active",
  );
  expect(active, `${kind.name}, store call ${String(at)}: owners left active`).toEqual([]);
  return context;
}

describe.each(STORES)("$name: an operation whose own lease lapses mid-flight", (kind) => {
  it("compactTable takes its fold over again", async () => {
    const compact: Operation = async (database) => {
      expect((await database.compactTable("t")).compacted).toBe(true);
    };
    const points = await lapsePoints(kind, compact);
    expect(points.length).toBeGreaterThan(10);
    for (const at of points) {
      const context = await lapseAt(kind, at, compact);
      const { store, database } = context;
      expect(
        (await database.query("SELECT id, a FROM t ORDER BY id")).rows,
        `store call ${String(at)}`,
      ).toEqual(EXPECTED);
      const published = (await store.listCompactionJobs()).filter(
        (job) => job.state === "published",
      );
      expect(published, `store call ${String(at)}`).toHaveLength(1);
      await closeFixture(context);
    }
  });

  it("a one-statement batch write starts again with a fresh transaction", async () => {
    const writes: Record<string, Operation> = {
      insert: (database) => database.insertBatch("t", [{ id: 100, a: 1 }]),
      upsert: (database) =>
        database.upsertBatch("t", [
          { id: 1, a: 1001 },
          { id: 101, a: 2 },
        ]),
      update: (database) => database.updateBatch("t", { keys: [2], changes: { a: [1002] } }),
      delete: (database) => database.deleteBatch("t", { keys: [4] }),
    };
    const expected: Record<string, Array<{ id: number; a: number }>> = {
      insert: [...EXPECTED, { id: 100, a: 1 }],
      upsert: [
        ...EXPECTED.map((row) => (row.id === 1 ? { id: 1, a: 1001 } : row)),
        { id: 101, a: 2 },
      ],
      update: EXPECTED.map((row) => (row.id === 2 ? { id: 2, a: 1002 } : row)),
      delete: EXPECTED.filter((row) => row.id !== 4),
    };
    for (const [name, write] of Object.entries(writes)) {
      for (const at of await lapsePoints(kind, write)) {
        const context = await lapseAt(kind, at, write);
        expect(
          (await context.database.query("SELECT id, a FROM t ORDER BY id")).rows,
          `${name}, store call ${String(at)}`,
        ).toEqual(expected[name]);
        await closeFixture(context);
      }
    }
  });

  it("a SQL statement staged through the durable journal runs again", async () => {
    const rows = 200;
    const update: Operation = async (database) => {
      const result = await database.execute("UPDATE t SET a = a + 1");
      expect(result).toMatchObject({ kind: "update", rowCount: rows });
    };
    for (const at of await lapsePoints(kind, update, rows)) {
      const context = await lapseAt(kind, at, update, rows);
      expect(
        (await context.database.query("SELECT COUNT(*) AS n, SUM(a) AS total FROM t")).rows,
        `store call ${String(at)}`,
      ).toEqual([{ n: rows, total: (rows * (rows - 1)) / 2 + rows }]);
      await closeFixture(context);
    }
  });

  it("a write() scope fails whole instead, since its callback is never replayed", async () => {
    const rows = 200;
    const scope: Operation = (database) =>
      database.write(async (session) => {
        await session.execute("UPDATE t SET a = a + 1");
        await session.execute("UPDATE t SET a = a + 1 WHERE id < 100");
      });
    const before = (rows * (rows - 1)) / 2;
    let failed = 0;
    for (const at of await lapsePoints(kind, scope, rows)) {
      const context = await fixture(kind, rows);
      const { store, database, clock, probe } = context;
      probe.onCall = (call) => {
        if (call === at) clock.advance(LEASE_MS + 1);
      };
      probe.armed = true;
      const outcome = await scope(database).then(
        () => undefined,
        (error: unknown) => error,
      );
      probe.armed = false;
      const [{ total }] = (await database.query("SELECT SUM(a) AS total FROM t")).rows as [
        { total: number },
      ];
      if (outcome === undefined) {
        expect(total, `store call ${String(at)}`).toBe(before + rows + 100);
      } else {
        failed += 1;
        expect(outcome, `store call ${String(at)}`).toBeInstanceOf(TransactionClosedError);
        expect(total, `store call ${String(at)}: nothing of a failed scope publishes`).toBe(before);
      }
      const active = (await store.listTransactionPage(null, 1_000)).records.filter(
        (record) => record.status === "active",
      );
      expect(active, `store call ${String(at)}: owners left active`).toEqual([]);
      await closeFixture(context);
    }
    expect(failed).toBeGreaterThan(0);
  });
});

describe("a frozen thread", () => {
  it("does not fail a fold in flight", async () => {
    // The real thing rather than a test clock: the thread blocks past a short lease, so no
    // timer fires and the wall clock moves, exactly as it does across a laptop's sleep.
    const lease = 150;
    const frozen = new Int32Array(new SharedArrayBuffer(4));
    const open = async () => {
      const store = new MemoryBlockStore();
      const probe = instrumentStoreCalls(store);
      const database = new MinnowDatabase(store, {
        rowsPerBlock: 4,
        autoCompact: false,
        autoCollect: false,
        transactionOwnerLeaseMs: lease,
      });
      await database.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)");
      for (let id = 0; id < 12; id += 1) await database.insert("t", { id, a: id });
      await database.execute("UPDATE t SET a = a + 1 WHERE id < 6");
      return { probe, database };
    };
    const dry = await open();
    dry.probe.armed = true;
    await dry.database.compactTable("t");
    const calls = dry.probe.calls.length;
    await dry.database.close();
    // Late in the fold, once its transaction holds staged output: the staging and the
    // publication steps, where the owner's lease is what the next store write checks.
    for (const at of [Math.floor(calls * 0.85), Math.floor(calls * 0.92), calls - 2]) {
      const { probe, database } = await open();
      let froze = false;
      probe.onCall = (call) => {
        if (call === at) {
          froze = true;
          Atomics.wait(frozen, 0, 0, lease * 2);
        }
      };
      probe.armed = true;
      const result = await database.compactTable("t");
      probe.armed = false;
      expect(froze).toBe(true);
      expect(result.compacted, `frozen before store call ${String(at)} of ${String(calls)}`).toBe(
        true,
      );
      expect((await database.query("SELECT SUM(a) AS total FROM t")).rows).toEqual([
        { total: 66 + 6 },
      ]);
      await database.close();
    }
  });
});

describe("a fold stepped inside a writer's turn", () => {
  /**
   * Refuses the first renewal of one transaction after the clock has moved past its lease: the
   * owner looked live when the step read it and lapsed by the time the step resumed it.
   */
  class LapseOnResumeStore extends MemoryBlockStore {
    lapse: { transactionId: string; clock: ReturnType<typeof testClock> } | undefined;

    override async renewTransaction(
      input: Parameters<MemoryBlockStore["renewTransaction"]>[0],
    ): ReturnType<MemoryBlockStore["renewTransaction"]> {
      const lapse = this.lapse;
      if (lapse?.transactionId === input.transactionId) {
        this.lapse = undefined;
        lapse.clock.advance(LEASE_MS + 1);
        return false;
      }
      return super.renewTransaction(input);
    }
  }

  it("re-takes a lapsed owner inside the turn instead of waiting for the turn it holds", async () => {
    const clock = testClock();
    const store = new LapseOnResumeStore();
    const loader = new MinnowDatabase(store, {
      autoCompact: false,
      autoCollect: false,
      now: clock.now,
    });
    await loader.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER)");
    // Past twice one fold's prefix of level-zero segments, where every write drives a fold
    // step on its own turn before it commits.
    for (let id = 0; id < 520; id += 1) await loader.insert("t", { id, a: id });
    const partial = await loader.compactTableStep("t", { maxBlocks: 1 });
    expect(partial.result).toBeNull();
    if (partial.jobId === null) throw new Error("Expected a persisted compaction job");
    const owner = (await store.getCompactionJob(partial.jobId))?.transactionId;
    if (owner === null || owner === undefined) throw new Error("Expected a linked owner");

    const writer = new MinnowDatabase(store, { autoCollect: false, now: clock.now });
    store.lapse = { transactionId: owner, clock };
    const write = writer.insert("t", { id: 10_000, a: 0 });
    const outcome = await Promise.race([
      write.then(() => "written"),
      new Promise((resolve) => setTimeout(() => resolve("still waiting"), 10_000)),
    ]);
    expect(outcome).toBe("written");
    expect(store.lapse).toBeUndefined();
    expect((await store.getTransaction(owner))?.status).toBe("aborted");
    expect((await writer.query("SELECT COUNT(*) AS n FROM t")).rows).toEqual([{ n: 521 }]);
    await Promise.all([writer.close(), loader.close()]);
  });
});
