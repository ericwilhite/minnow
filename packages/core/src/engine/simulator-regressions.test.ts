/**
 * Defects the interaction-plan simulator found (packages/core/src/testing/interaction-simulator.ts),
 * each reduced to the smallest deterministic statement sequence that reproduces it. The seeded
 * suites keep exploring; these pin what they already caught.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import {
  CompactionJobConflictError,
  PostingBuildConflictError,
  type CompactionJobRecord,
  GarbageCollectionJobConflictError,
} from "../storage/types.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { CompactionJobCancelledError, UnknownTableError } from "./errors.js";
import { allTransactionRecords } from "./storage-test-helpers.js";

const stores: ReadonlyArray<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: async () => new MemoryBlockStore() },
  {
    name: "indexeddb",
    open: () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    open: () => OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

/** Ids that sort backwards, so the second segment of a commit sorts before the first. */
function descendingIds(): () => string {
  let next = 1_000_000;
  return () => `id-${String(next--)}`;
}

describe.each(stores)("simulator regressions over $name", ({ open }) => {
  it("retries background collection when another collector finishes and removes its job", async () => {
    // Chromium crash campaign seed 2654435807: a conflict was raised while the job still
    // existed, but the winning collector removed it before the losing collector reread it.
    const store = await open();
    const errors: unknown[] = [];
    let now = Date.parse("2026-09-24T00:00:00Z");
    const runStep = store.runGarbageCollectionStep.bind(store);
    const getJob = store.getGarbageCollectionJob.bind(store);
    let raced = false;
    let retired: string | undefined;
    const step = vi.spyOn(store, "runGarbageCollectionStep").mockImplementation(async (input) => {
      if (raced) return runStep(input);
      raced = true;
      // Complete the real persisted job on behalf of the other collector. Only completed
      // records may be removed, so the test exercises the adapter's actual lifecycle.
      let result = await runStep(input);
      while (result.job.state !== "completed") {
        result = await runStep({ ...input, expectedRevision: result.job.revision, maxItems: 1024 });
      }
      retired = input.jobId;
      throw new GarbageCollectionJobConflictError(
        input.jobId,
        input.expectedRevision,
        result.job.revision,
      );
    });
    const read = vi.spyOn(store, "getGarbageCollectionJob").mockImplementation(async (id) => {
      if (id === retired) {
        retired = undefined;
        await store.removeGarbageCollectionJob(id);
      }
      return getJob(id);
    });
    const database = new MinnowDatabase(store, {
      autoCompact: false,
      now: () => new Date(now),
      onBackgroundError: (error) => errors.push(error),
    });
    try {
      await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      await database.execute("INSERT INTO t VALUES (1)");
      now += 120_000;
      for (let id = 2; id <= 65; id += 1) await database.insert("t", { id });
      await vi.waitFor(
        () => {
          expect(raced).toBe(true);
          expect(database.maintenanceStatus()).toMatchObject({
            collectionRunning: false,
            collectionRequested: false,
            consecutiveFailures: 0,
            lastError: null,
          });
        },
        { timeout: 10_000 },
      );
      expect(errors).toEqual([]);
      await database.execute("INSERT INTO t VALUES (66)");
      expect((await database.query("SELECT * FROM t ORDER BY id")).rows).toEqual(
        Array.from({ length: 66 }, (_, index) => ({ id: index + 1 })),
      );
    } finally {
      await database.close();
      step.mockRestore();
      read.mockRestore();
      store.close();
    }
  });

  it("folds a transaction that wrote two segments to one table", async () => {
    // A SQL transaction holding a predicate DELETE and an INSERT on the same table commits two
    // segments with one logical order and one committed version. The merge planner ordered
    // its sources by segment id as the tie-break while readers order them by position within
    // the commit; whenever the ids disagreed with that position the fold refused with
    // "Mutation compaction sources are not in canonical logical order", and the table could
    // never be compacted again.
    const store = await open();
    const database = new MinnowDatabase(store, {
      rowsPerBlock: 4,
      autoCompact: false,
      autoCollect: false,
      createId: descendingIds(),
    });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    await database.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    await database.execute("BEGIN");
    await database.execute("DELETE FROM t WHERE v >= 20");
    await database.execute("INSERT INTO t VALUES (2, 21), (4, 40)");
    await database.execute("COMMIT");
    const before = await database.query("SELECT * FROM t ORDER BY id", { memoize: false });
    expect(before.rows).toEqual([
      { id: 1, v: 10 },
      { id: 2, v: 21 },
      { id: 4, v: 40 },
    ]);
    const compaction = await database.compactTable("t");
    expect(compaction.compacted).toBe(true);
    const after = await database.query("SELECT * FROM t ORDER BY id", { memoize: false });
    expect(after.rows).toEqual(before.rows);
    await database.close();
    store.close();
  });
});

describe.each(stores)("composite index over nullable columns on $name", ({ open }) => {
  it("does not prune rows whose unconstrained trailing column is NULL", async () => {
    // A tuple-v1 composite index only held rows whose every indexed value was non-null, so an
    // index on (c1, c0) had no entry for a row with c0 NULL. A prefix lookup on c1 alone that
    // pruned through it dropped that row: a SELECT missed it and an UPDATE reported one row too
    // few. tuple-v2 names such a row under a NULL component marker, so the lookup both prunes and
    // returns it; row 5 is the one with c0 NULL.
    const store = await open();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4, autoCompact: false });
    await database.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, c0 DOUBLE PRECISION, c1 INTEGER NOT NULL)",
    );
    await database.execute(
      "INSERT INTO t VALUES (1, -9.75, -4), (5, NULL, 50), (6, 13.5, 1), (9, -4, -13), (11, 2.25, 47), (14, 9.5, -1), (17, 15.75, -32), (22, 2.25, -40), (32, 2.25, -31), (39, 2.25, 36)",
    );
    await database.execute("CREATE INDEX t_c1_c0 ON t (c1, c0)");
    expect(await database.explain("SELECT id FROM t WHERE c1 > 18 ORDER BY id")).toContain(
      "a ready secondary index prunes",
    );
    const selected = await database.query("SELECT id FROM t WHERE c1 > 18 ORDER BY id", {
      memoize: false,
    });
    expect(selected.rows.map((row) => row.id)).toEqual([5, 11, 39]);
    const updated = await database.execute("UPDATE t SET c1 = c1 + 2 WHERE c1 > 18");
    expect(updated.kind === "update" && updated.rowCount).toBe(3);
    const after = await database.query("SELECT id, c1 FROM t WHERE c1 > 18 ORDER BY id", {
      memoize: false,
    });
    expect(after.rows).toEqual([
      { id: 5, c1: 52 },
      { id: 11, c1: 49 },
      { id: 39, c1: 38 },
    ]);
    // The same prefix through a constrained leading column still prunes when the trailing
    // column is constrained too (every candidate row is non-null there by the predicate).
    const both = await database.query("SELECT id FROM t WHERE c1 = 49 AND c0 = 2.25", {
      memoize: false,
    });
    expect(both.rows.map((row) => row.id)).toEqual([11]);
    await database.close();
    store.close();
  });
});

describe.each(stores)("block-level index pruning keeps every delta on $name", ({ open }) => {
  it("never serves a stale intermediate value from a set-operation member", async () => {
    // Two updates to one row: the first moved it into the predicate's range along with its
    // neighbours, the second moved it back out. The index no longer names the row, so pruning
    // dropped the second update's segment while the whole block holding the row was kept for
    // its neighbours -- and the first update was replayed onto it. The UNION ALL member (a
    // materialized input) then returned the row at the value the second update had replaced.
    const store = await open();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4, autoCompact: false });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 DOUBLE PRECISION)");
    await database.execute(
      "INSERT INTO t VALUES (1, 5), (2, 5), (3, 5), (4, 5), (5, 5), (6, 5), (7, 5), (8, 5), (9, 5), (10, 5)",
    );
    await database.execute("UPDATE t SET c1 = -17.75 WHERE id <> 10");
    await database.execute("UPDATE t SET c1 = -18.5 WHERE id = 3");
    // Built after both updates, as a rebuilt base is: it names row 3 under -18.5 only.
    await database.execute("CREATE INDEX t_c1 ON t (c1)");
    const expected = [1, 2, 4, 5, 6, 7, 8, 9];
    const member = await database.query(
      "SELECT id FROM t WHERE c1 = -17.75 UNION ALL SELECT id FROM t WHERE FALSE",
      { memoize: false },
    );
    expect(member.rows.map((row) => row.id).sort((a, b) => Number(a) - Number(b))).toEqual(
      expected,
    );
    const range = await database.query(
      "SELECT id FROM t WHERE c1 >= -18 UNION ALL SELECT id FROM t WHERE FALSE",
      { memoize: false },
    );
    expect(range.rows.map((row) => row.id).sort((a, b) => Number(a) - Number(b))).toEqual([
      ...expected,
      10,
    ]);
    const plain = await database.query("SELECT id FROM t WHERE c1 = -17.75 ORDER BY id", {
      memoize: false,
    });
    expect(plain.rows.map((row) => row.id)).toEqual(expected);
    await database.close();
    store.close();
  });
});

describe.each(stores)("zone-map pruning keeps every delete on $name", ({ open }) => {
  it("replays a re-inserted key after a pruned range query", async () => {
    // Delete a key, insert it again in a block that also spans the probed range, then update
    // rows so the replay has keys to map. Zone-map pruning dropped the delete because its key
    // block could not match the predicate, so the replay saw the key in two kept blocks with
    // nothing unmapping it in between and refused every keyed range read, UPDATE, and DELETE
    // on the table as "Stored table contains a duplicate unique key".
    const store = await open();
    const database = new MinnowDatabase(store, { rowsPerBlock: 8, autoCompact: false });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    await database.execute("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)");
    await database.execute("DELETE FROM t WHERE id = 3");
    await database.execute("INSERT INTO t VALUES (3, 33), (7, 7)");
    await database.execute("UPDATE t SET v = v + 1 WHERE id IN (3, 5)");
    const inList = await database.query("SELECT id, v FROM t WHERE id IN (5)", { memoize: false });
    expect(inList.rows).toEqual([{ id: 5, v: 6 }]);
    const range = await database.query("SELECT id, v FROM t WHERE id > 4 ORDER BY id", {
      memoize: false,
    });
    expect(range.rows).toEqual([
      { id: 5, v: 6 },
      { id: 6, v: 6 },
      { id: 7, v: 7 },
    ]);
    const updated = await database.execute("UPDATE t SET v = 55 WHERE id IN (5)");
    expect(updated.kind === "update" && updated.rowCount).toBe(1);
    const deleted = await database.execute("DELETE FROM t WHERE id = 5");
    expect(deleted.kind === "delete" && deleted.rowCount).toBe(1);
    const all = await database.query("SELECT id, v FROM t ORDER BY id", { memoize: false });
    expect(all.rows).toEqual([
      { id: 1, v: 1 },
      { id: 2, v: 2 },
      { id: 3, v: 34 },
      { id: 4, v: 4 },
      { id: 6, v: 6 },
      { id: 7, v: 7 },
    ]);
    await database.close();
    store.close();
  });
});

describe("IndexedDB unique-key cache across adapter instances", () => {
  it("does not carry a stale key set past another instance's commit", async () => {
    // Two tabs over one IndexedDB database. Tab B cached t3's keys, tab A inserted key 9 into
    // t3, then tab B committed to another table. That commit moved B's cached key set to the
    // new manifest version as if nothing had changed in between, so B's next insert of key 9
    // checked a set without it and was accepted: two rows with one primary key.
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const storeA = await IndexedDbBlockStore.open({ name, indexedDB });
    const storeB = await IndexedDbBlockStore.open({ name, indexedDB });
    const a = new MinnowDatabase(storeA, { autoCompact: false, autoCollect: false });
    const b = new MinnowDatabase(storeB, { autoCompact: false, autoCollect: false });
    await a.execute("CREATE TABLE t3 (id INTEGER PRIMARY KEY, c0 BOOLEAN)");
    await a.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, c0 INTEGER)");
    await a.execute("INSERT INTO t2 VALUES (1, 1)");
    await b.execute("INSERT INTO t3 VALUES (7, TRUE)");
    await a.execute("INSERT INTO t3 VALUES (9, FALSE), (4, TRUE)");
    await b.execute("UPDATE t2 SET c0 = 2 WHERE id = 1");
    await expect(b.execute("INSERT INTO t3 VALUES (14, TRUE), (9, TRUE)")).rejects.toThrow(
      /duplicate/iu,
    );
    for (const database of [a, b]) {
      const rows = await database.query("SELECT id FROM t3 ORDER BY id", { memoize: false });
      expect(rows.rows.map((row) => row.id)).toEqual([4, 7, 9]);
    }
    await a.close();
    await b.close();
    storeA.close();
    storeB.close();
  });
});

describe.each(stores)("index pruning keeps every delete on $name", ({ open }) => {
  it("replays a range-deleted, reinserted key under an exact index selection", async () => {
    // The index names the live rows exactly, so the delete segment covering the old row had no
    // candidate and was dropped from the pruned scan. The replay still maps every touched key
    // to the base rows holding it, and with the delete gone the reinserted key was mapped twice
    // and the whole table refused as holding a duplicate unique key.
    const store = await open();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4, autoCompact: false });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c0 INTEGER, c1 BOOLEAN)");
    await database.execute(
      "INSERT INTO t VALUES (1, 0, TRUE), (2, 0, FALSE), (3, 0, FALSE), (4, 0, FALSE), (5, 0, FALSE), (6, 0, FALSE), (7, 0, FALSE), (8, 0, FALSE)",
    );
    await database.execute("CREATE INDEX t_c1 ON t (c1)");
    // Two blocks of deleted keys, neither holding a row the predicate will name.
    await database.execute("DELETE FROM t WHERE id >= 3");
    await database.execute("INSERT INTO t VALUES (3, 0, FALSE), (9, 0, TRUE)");
    await database.execute("UPDATE t SET c0 = c0 + 1 WHERE id IN (1, 3, 9)");
    const count = await database.query("SELECT COUNT(*) AS n FROM t WHERE c1 = TRUE", {
      memoize: false,
    });
    expect(count.rows[0]?.n).toBe(2);
    const rows = await database.query("SELECT id, c0 FROM t WHERE c1 = TRUE ORDER BY id", {
      memoize: false,
    });
    expect(rows.rows).toEqual([
      { id: 1, c0: 1 },
      { id: 9, c0: 1 },
    ]);
    await database.close();
    store.close();
  });
});

describe.each(stores.map(({ name, open }) => ({ name, create: open })))(
  "compaction planning races on $name",
  ({ create }) => {
    it.each([
      ["published", false],
      ["published", true],
      ["cancelled", true],
    ] as const)(
      "reconciles a %s job without hiding unrelated I/O (fault=%s)",
      async (outcome, fault) => {
        const store = await create();
        const owner = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
        const contender = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
        const originalCreate = store.createCompactionJob.bind(store);
        const originalGet = store.getCompactionJob.bind(store);
        let armed: string | undefined;
        let published: number | null | undefined;
        const failure = new Error("unrelated source metadata I/O failure");
        let restoreFailedRead: (() => void) | undefined;
        const creating = vi
          .spyOn(store, "createCompactionJob")
          .mockImplementation(async (record) => {
            await originalCreate(record);
            armed = record.id;
          });
        const reading = vi.spyOn(store, "getCompactionJob").mockImplementation(async (id) => {
          const stale = await originalGet(id);
          if (id === armed) {
            armed = undefined;
            if (outcome === "published") {
              const winner = await owner.resumeCompactionJob(id, { maxBlocks: 64 });
              expect(winner.state).toBe("published");
              published = winner.result?.version;
            } else await owner.cancelCompactionJob(id);
            if (fault) {
              const failedRead = vi.spyOn(store, "getSegment").mockRejectedValueOnce(failure);
              restoreFailedRead = () => failedRead.mockRestore();
            }
          }
          return stale;
        });
        try {
          await owner.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, value INTEGER)");
          for (let id = 1; id <= 4; id += 1) await owner.insert("events", { id, value: id });
          const running = contender.compactTableStep("events", { maxBlocks: 1 });
          if (fault) await expect(running).rejects.toBe(failure);
          else {
            const result = await running;
            expect(result).toMatchObject({
              state: "published",
              result: { version: published, rowCount: 4 },
            });
          }
          expect((await contender.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
            { n: 10 },
          ]);
          expect((await store.listCompactionJobs()).every((job) => job.state === outcome)).toBe(
            true,
          );
          expect(
            (await allTransactionRecords(store)).some(
              (transaction) => transaction.status === "active",
            ),
          ).toBe(false);
        } finally {
          restoreFailedRead?.();
          reading.mockRestore();
          creating.mockRestore();
          await Promise.all([owner.close(), contender.close()]);
          store.close();
        }
      },
    );

    it("reports registration I/O even when the durable job was created and can be resumed", async () => {
      const store = await create();
      const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      const original = store.createCompactionJob.bind(store);
      const failure = new Error("registration I/O failed after persistence");
      const creating = vi.spyOn(store, "createCompactionJob").mockImplementation(async (record) => {
        await original(record);
        throw failure;
      });
      try {
        await database.execute("CREATE TABLE events(value INTEGER)");
        for (let value = 1; value <= 4; value += 1) await database.insert("events", { value });
        await expect(database.compactTableStep("events")).rejects.toBe(failure);
        const [job] = await store.listCompactionJobs();
        expect(job?.state).toBe("planned");
        if (job === undefined) throw new Error("Expected a persisted job");
        expect((await database.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
          { n: 10 },
        ]);
        creating.mockRestore();
        expect(await database.resumeCompactionJob(job.id, { maxBlocks: 64 })).toMatchObject({
          state: "published",
          result: { rowCount: 4 },
        });
      } finally {
        creating.mockRestore();
        await database.close();
        store.close();
      }
    });

    it("reports failure-recording I/O while preserving the original publication failure", async () => {
      const store = await create();
      const reports: Array<{ error: unknown; context: string }> = [];
      const database = new MinnowDatabase(store, {
        autoCompact: false,
        autoCollect: false,
        onBackgroundError: (error, context) => reports.push({ error, context }),
      });
      const primary = new Error("publication failed");
      const secondary = new Error("recording failed");
      const committing = vi.spyOn(store, "commitTransaction");
      const update = store.updateCompactionJob.bind(store);
      const updating = vi
        .spyOn(store, "updateCompactionJob")
        .mockImplementation(async (...args) => {
          if (args[2].error === primary.message) throw secondary;
          return update(...args);
        });
      try {
        await database.execute("CREATE TABLE events(value INTEGER)");
        for (let value = 1; value <= 4; value += 1) await database.insert("events", { value });
        committing.mockRejectedValueOnce(primary);
        await expect(database.compactTableStep("events", { maxBlocks: 64 })).rejects.toBe(primary);
        const [job] = await store.listCompactionJobs();
        if (job === undefined) throw new Error("Expected the durable job");
        expect(reports).toEqual([
          { error: secondary, context: `compaction failure recording for ${job.id}` },
        ]);
        expect((await database.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
          { n: 10 },
        ]);
      } finally {
        committing.mockRestore();
        updating.mockRestore();
        await database.close();
        store.close();
      }
    });

    it.each(["running", "published"] as const)(
      "reports I/O after persisting the %s transition and resumes safely",
      async (phase) => {
        const store = await create();
        const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
        const original = store.updateCompactionJob.bind(store);
        const failure = new Error(`I/O after ${phase} persisted`);
        let armed = true;
        const updating = vi
          .spyOn(store, "updateCompactionJob")
          .mockImplementation(async (...args) => {
            const job = await original(...args);
            if (armed && args[2].state === phase) {
              armed = false;
              throw failure;
            }
            return job;
          });
        try {
          await database.execute("CREATE TABLE events(value INTEGER)");
          for (let value = 1; value <= 4; value += 1) await database.insert("events", { value });
          await expect(database.compactTableStep("events", { maxBlocks: 64 })).rejects.toBe(
            failure,
          );
          expect(armed).toBe(false);
          const [job] = await store.listCompactionJobs();
          if (job === undefined) throw new Error("Expected a durable job");
          expect(
            (await allTransactionRecords(store)).some((record) => record.status === "active"),
          ).toBe(false);
          expect((await database.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
            { n: 10 },
          ]);
          expect(await database.resumeCompactionJob(job.id, { maxBlocks: 64 })).toMatchObject({
            state: "published",
            result: { rowCount: 4 },
          });
        } finally {
          updating.mockRestore();
          await database.close();
          store.close();
        }
      },
    );

    it("refuses a competing active job with the shared typed conflict and unchanged records", async () => {
      const store = await create();
      const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      const original = store.createCompactionJob.bind(store);
      let planned: CompactionJobRecord | undefined;
      const creating = vi.spyOn(store, "createCompactionJob").mockImplementation(async (record) => {
        planned = structuredClone(record);
        await original(record);
      });
      try {
        await database.execute("CREATE TABLE events(value INTEGER)");
        for (let value = 1; value <= 4; value += 1) await database.insert("events", { value });
        const partial = await database.compactTableStep("events", {
          maxBlocks: 1,
          targetBlockBytes: 9,
        });
        expect(partial.result).toBeNull();
        if (planned === undefined) throw new Error("Expected a planned job");
        const before = await store.listCompactionJobs();
        await expect(
          original({
            ...planned,
            id: `${planned.id}/other`,
            outputSegmentId: `${planned.id}/other/output`,
          }),
        ).rejects.toBeInstanceOf(CompactionJobConflictError);
        expect(await store.listCompactionJobs()).toEqual(before);
        expect((await database.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
          { n: 10 },
        ]);
      } finally {
        creating.mockRestore();
        await database.close();
        store.close();
      }
    });

    it("refuses posting ownership after a concurrent index drop with a typed conflict", async () => {
      const store = await create();
      const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      try {
        await database.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, value INTEGER)");
        await database.insert("events", { id: 1, value: 10 });
        await database.execute("CREATE INDEX by_value ON events(value)");
        const table = await store.getTableByName("events");
        const index = Object.values(table?.secondaryIndexes ?? {})[0];
        if (table === undefined || index === undefined) throw new Error("Expected the index");
        await database.execute("DROP INDEX by_value");
        const createdAt = new Date().toISOString();
        const input = {
          tableId: table.id,
          columnId: index.storageColumnId,
          buildId: "stale-build",
          ownerId: "stale-owner",
          createdAt,
          expiresAt: new Date(Date.now() + 60000).toISOString(),
        };
        await expect(store.beginFtsBaseBuild(input)).rejects.toMatchObject({
          name: "PostingBuildConflictError",
          buildId: input.buildId,
          ownerId: input.ownerId,
          reason: "index is no longer active",
        });
        await expect(store.beginFtsBaseBuild(input)).rejects.toBeInstanceOf(
          PostingBuildConflictError,
        );
        expect((await database.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
          { n: 10 },
        ]);
      } finally {
        await database.close();
        store.close();
      }
    });

    it.each(["drop", "missing", "I/O"] as const)(
      "rechecks a missing compaction owner without hiding %s failures",
      async (mode) => {
        const store = await create();
        const owner = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
        const contender = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
        const get = store.getTransactions.bind(store);
        let reads = 0;
        const failure = new Error("source owner I/O failure");
        const reading = vi.spyOn(store, "getTransactions").mockImplementation(async (...args) => {
          reads += 1;
          if (reads === 3 && mode === "I/O") throw failure;
          if (reads === 3 && mode === "missing") return args[0].map(() => undefined);
          const records = await get(...args);
          if (reads === 2 && mode === "drop") {
            await owner.execute("DROP TABLE events");
            await owner.collectGarbage({ retainRecentVersions: 0 });
          }
          return records;
        });
        try {
          await owner.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, value INTEGER)");
          for (let id = 1; id <= 4; id += 1) await owner.insert("events", { id, value: id });
          const run = contender.compactTableStep("events", { maxBlocks: 64 });
          if (mode === "drop") await expect(run).rejects.toBeInstanceOf(UnknownTableError);
          else if (mode === "I/O") await expect(run).rejects.toBe(failure);
          else
            await expect(run).rejects.toThrow("Compaction source segment has no committed owner");
          expect(reads).toBeGreaterThanOrEqual(3);
          expect(await store.listCompactionJobs()).toEqual([]);
          if (mode !== "drop")
            expect((await contender.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
              { n: 10 },
            ]);
        } finally {
          reading.mockRestore();
          await Promise.all([owner.close(), contender.close()]);
          store.close();
        }
      },
    );

    it("preserves cancellation when another tab drops the table before a suspended fold resumes", async () => {
      const store = await create();
      const owner = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      const contender = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      try {
        await owner.execute("CREATE TABLE events(value INTEGER)");
        for (let value = 1; value <= 4; value += 1) await owner.insert("events", { value });
        const partial = await contender.compactTableStep("events", {
          maxBlocks: 1,
          targetBlockBytes: 9,
        });
        if (partial.jobId === null) throw new Error("Expected a suspended fold");
        await owner.cancelCompactionJob(partial.jobId);
        await owner.execute("DROP TABLE events");
        await expect(contender.resumeCompactionJob(partial.jobId)).rejects.toBeInstanceOf(
          CompactionJobCancelledError,
        );
        expect(
          (await allTransactionRecords(store)).some((record) => record.status === "active"),
        ).toBe(false);
      } finally {
        await Promise.all([owner.close(), contender.close()]);
        store.close();
      }
    });

    it("refuses planning for a table dropped during source measurement", async () => {
      const store = await create();
      const owner = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      const contender = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      try {
        await owner.execute("CREATE TABLE events(value INTEGER)");
        for (let value = 1; value <= 4; value += 1) await owner.insert("events", { value });
        const original = store.getBlock.bind(store);
        let dropped = false;
        const reading = vi.spyOn(store, "getBlock").mockImplementation(async (id) => {
          const bytes = await original(id);
          if (!dropped) {
            dropped = true;
            await owner.execute("DROP TABLE events");
          }
          return bytes;
        });
        try {
          await expect(contender.compactTableStep("events")).rejects.toBeInstanceOf(
            UnknownTableError,
          );
          expect(dropped).toBe(true);
          expect(await store.listCompactionJobs()).toEqual([]);
        } finally {
          reading.mockRestore();
        }
      } finally {
        await Promise.all([owner.close(), contender.close()]);
        store.close();
      }
    });

    it("does not register a stale plan after another job replaces its sources", async () => {
      const store = await create();
      const owner = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      const contender = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      const get = store.getCompactionJob.bind(store);
      let armed: string | undefined;
      let prior: string | undefined;
      const reading = vi.spyOn(store, "getCompactionJob").mockImplementation(async (id) => {
        const job = await get(id);
        if (id === armed && job === undefined) {
          armed = undefined;
          if (prior === undefined) throw new Error("Expected the prior fold");
          expect(await owner.resumeCompactionJob(prior, { maxBlocks: 64 })).toMatchObject({
            state: "published",
          });
        }
        return job;
      });
      try {
        await owner.execute("CREATE TABLE events(value INTEGER)");
        for (let value = 1; value <= 4; value += 1) await owner.insert("events", { value });
        const partial = await owner.compactTableStep("events", {
          maxBlocks: 1,
          targetBlockBytes: 9,
        });
        if (partial.jobId === null) throw new Error("Expected the prior fold");
        prior = partial.jobId;
        await owner.insert("events", { value: 5 });
        const table = await store.getTableByName("events");
        if (table === undefined) throw new Error("Expected the table");
        armed = `compaction/${table.id}/manifest/${String(await store.getCurrentManifestVersion())}`;
        // Suppress the existing active job once so this models a planner whose job scan ran
        // before that job was registered, but whose snapshot includes the subsequent append.
        const page = store.listCompactionJobPage.bind(store);
        let scanned = false;
        const scanning = vi
          .spyOn(store, "listCompactionJobPage")
          .mockImplementation(async (...args) => {
            const result = await page(...args);
            if (!scanned) {
              scanned = true;
              return { ...result, records: [] };
            }
            return result;
          });
        try {
          expect(await contender.compactTable("events", { maxBlocksPerStep: 64 })).toMatchObject({
            rowCount: 5,
          });
          expect(armed).toBeUndefined();
          expect((await store.listCompactionJobs()).every((job) => job.state === "published")).toBe(
            true,
          );
          expect((await contender.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
            { n: 15 },
          ]);
        } finally {
          scanning.mockRestore();
        }
      } finally {
        reading.mockRestore();
        await Promise.all([owner.close(), contender.close()]);
        store.close();
      }
    });

    it("resumes a ready job without making it unready for another publisher", async () => {
      const store = await create();
      const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      const commit = store.commitTransaction.bind(store);
      const failure = new Error("publication I/O failure");
      const committing = vi.spyOn(store, "commitTransaction");
      const update = store.updateCompactionJob.bind(store);
      const updating = vi.spyOn(store, "updateCompactionJob");
      try {
        await database.execute("CREATE TABLE events(value INTEGER)");
        for (let value = 1; value <= 4; value += 1) await database.insert("events", { value });
        committing.mockRejectedValueOnce(failure);
        await expect(database.compactTableStep("events", { maxBlocks: 64 })).rejects.toBe(failure);
        const [job] = await store.listCompactionJobs();
        if (job === undefined) throw new Error("Expected a durable job");
        expect(job.state).toBe("ready");
        committing.mockImplementation(commit);
        updating.mockImplementation(async (id, revision, patch) => {
          if (patch.state === "running") throw new Error("A ready job must not be downgraded");
          return update(id, revision, patch);
        });
        expect(await database.resumeCompactionJob(job.id, { maxBlocks: 64 })).toMatchObject({
          state: "published",
          result: { rowCount: 4 },
        });
        expect((await database.query("SELECT SUM(value) AS n FROM events")).rows).toEqual([
          { n: 10 },
        ]);
      } finally {
        committing.mockRestore();
        updating.mockRestore();
        await database.close();
        store.close();
      }
    });

    it("replans an automatic fold when a concurrent schema commit invalidates publication", async () => {
      const store = await create();
      const reports: unknown[] = [];
      const owner = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      const contender = new MinnowDatabase(store, {
        autoCollect: false,
        onBackgroundError: (error) => reports.push(error),
      });
      const update = store.updateCompactionJob.bind(store);
      let changed = false;
      const updating = vi
        .spyOn(store, "updateCompactionJob")
        .mockImplementation(async (...args) => {
          const job = await update(...args);
          if (args[2].state === "ready" && !changed) {
            changed = true;
            await owner.execute("CREATE TABLE unrelated (n INTEGER)");
          }
          return job;
        });
      try {
        await owner.execute("CREATE TABLE events(value INTEGER)");
        for (let value = 0; value < 47; value += 1) await owner.insert("events", { value });
        await contender.insert("events", { value: 47 });
        await vi.waitFor(
          async () => {
            expect(changed).toBe(true);
            expect(
              (await store.listCompactionJobs()).some((job) => job.state === "published"),
            ).toBe(true);
          },
          { timeout: 5000 },
        );
        expect(reports).toEqual([]);
        expect(
          (await contender.query("SELECT COUNT(*) AS n, SUM(value) AS s FROM events")).rows,
        ).toEqual([{ n: 48, s: 1128 }]);
      } finally {
        updating.mockRestore();
        await Promise.all([owner.close(), contender.close()]);
        store.close();
      }
    });
  },
);
