/**
 * Background maintenance under a writer that never pauses, and repair of what a stalled fold
 * leaves behind. A caller that awaits one statement after another is the ordinary shape of an
 * import loop, and it used to be exactly the shape that stopped maintenance: a fold that had
 * finished its rewrite lost the manifest race to the next write every time, or sat parked on a
 * yield the caller never gave it, while every write scanned one more level-zero segment, its
 * owner lease expired unrenewed, the collector queued behind it, and at 4,096 commits the writes
 * were refused — and stayed refused after `collectGarbage()`, because every write at the
 * level-zero ceiling resumed the dead owner. These pin the properties that close that loop.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { allVisibleSegments } from "./storage-test-helpers.js";

/** A clock the test moves, for the durable owner lease a compaction transaction carries. */
function testClock(): { now: () => Date; advance: (ms: number) => void } {
  let current = Date.parse("2026-01-01T00:00:00Z");
  return {
    now: () => new Date(current),
    advance: (ms) => {
      current += ms;
    },
  };
}

async function publishedFolds(database: MinnowDatabase, table: string): Promise<number> {
  return (await database.listCompactionJobs(table)).filter((job) => job.state === "published")
    .length;
}

describe("maintenance under a writer that never pauses", () => {
  it("keeps folding and collecting through 4,200 back-to-back inserts on the memory store", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t(pk INTEGER PRIMARY KEY, a INTEGER, b TEXT)");
    let mostSegments = 0;
    for (let index = 0; index < 4_200; index += 1) {
      await database.execute(`INSERT INTO t VALUES(${String(index)}, ${String(index * 7)}, 'x')`);
      if (index % 256 === 255) {
        mostSegments = Math.max(mostSegments, (await allVisibleSegments(database, "t")).length);
      }
    }
    // Past the old refusal point, with the table folded as it went: never within an order of
    // magnitude of the 4,096-segment ceiling, and the collector never behind or failing.
    expect(mostSegments).toBeLessThan(512);
    expect(await publishedFolds(database, "t")).toBeGreaterThan(0);
    expect(database.maintenanceStatus()).toMatchObject({ lastError: null });
    expect(database.maintenanceStatus().pendingCommitDebt).toBeLessThan(512);
    expect((await database.query("SELECT COUNT(*) AS n, SUM(a) AS total FROM t")).rows).toEqual([
      { n: 4_200, total: (7 * 4_199 * 4_200) / 2 },
    ]);
    await database.close();
  }, 120_000);

  it("publishes a finished fold through the write queue rather than losing the manifest race", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t(pk INTEGER PRIMARY KEY, a INTEGER)");
    let firstFoldAt: number | undefined;
    let mostSegments = 0;
    for (let index = 0; index < 2_000; index += 1) {
      await database.execute(`INSERT INTO t VALUES(${String(index)}, ${String(index)})`);
      if (index % 64 !== 63) continue;
      // A steady writer, not a starving one: it gives the event loop a turn now and then, which
      // is what let the old rebase-and-retry loop run — and lose — on every one of them.
      await new Promise((resolve) => setTimeout(resolve, 0));
      mostSegments = Math.max(mostSegments, (await allVisibleSegments(database, "t")).length);
      if (firstFoldAt === undefined && (await publishedFolds(database, "t")) > 0) {
        firstFoldAt = index + 1;
      }
    }
    expect(firstFoldAt).toBeDefined();
    expect(firstFoldAt).toBeLessThanOrEqual(512);
    expect(mostSegments).toBeLessThan(512);
    await database.close();
  }, 120_000);

  it("keeps a read-then-update loop under the level-zero ceiling", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t(pk INTEGER PRIMARY KEY, a INTEGER)");
    for (let index = 0; index < 100; index += 1) {
      await database.execute(`INSERT INTO t VALUES(${String(index)}, ${String(index)})`);
    }
    for (let index = 0; index < 4_200; index += 1) {
      const pk = index % 100;
      const row = (await database.query(`SELECT a FROM t WHERE pk = ${String(pk)}`)).rows[0] as {
        a: number;
      };
      await database.execute(`UPDATE t SET a = ${String(row.a + 1)} WHERE pk = ${String(pk)}`);
    }
    expect((await allVisibleSegments(database, "t")).length).toBeLessThan(512);
    expect(database.maintenanceStatus()).toMatchObject({ lastError: null });
    expect((await database.query("SELECT SUM(a) AS total FROM t")).rows).toEqual([
      { total: (99 * 100) / 2 + 4_200 },
    ]);
    await database.close();
  }, 120_000);
});

describe("repair of a fold whose owner lease expired", () => {
  async function stalledFold(clock: ReturnType<typeof testClock>) {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      autoCompact: false,
      autoCollect: false,
      now: clock.now,
    });
    await database.execute("CREATE TABLE t(pk INTEGER PRIMARY KEY, a INTEGER, b TEXT)");
    for (let index = 0; index < 60; index += 1) {
      await database.execute(`INSERT INTO t VALUES(${String(index)}, ${String(index)}, 'x')`);
    }
    // One output block of the fold, then nothing: the job is running and owns a transaction.
    const progress = await database.compactTableStep("t", { maxBlocks: 1 });
    expect(progress.result).toBeNull();
    const job = (await database.listCompactionJobs("t")).find(
      (record) => record.state === "running",
    );
    if (job?.transactionId === null || job === undefined)
      throw new Error("Expected a running fold");
    const ownerId = job.transactionId;
    // The lease is 30 seconds; the tab that held it is gone as far as the store can tell.
    clock.advance(31_000);
    return { store, database, jobId: job.id, ownerId };
  }

  it("resumes the fold with a fresh owner instead of failing on the dead one", async () => {
    const clock = testClock();
    const { store, database, jobId, ownerId } = await stalledFold(clock);
    let progress = await database.resumeCompactionJob(jobId, { maxBlocks: 1 });
    while (progress.result === null) {
      progress = await database.resumeCompactionJob(jobId, { maxBlocks: 1 });
    }
    expect(progress.result.compacted).toBe(true);
    const published = await store.getCompactionJob(jobId);
    expect(published?.state).toBe("published");
    expect(published?.transactionId).not.toBe(ownerId);
    expect((await store.getTransaction(ownerId))?.status).toBe("aborted");
    await database.execute("INSERT INTO t VALUES(1000, 1, 'y')");
    expect((await database.query("SELECT COUNT(*) AS n FROM t")).rows).toEqual([{ n: 61 }]);
    await database.close();
  });

  it("collectGarbage() clears the dead owner so the next write and fold succeed", async () => {
    const clock = testClock();
    const { store, database, jobId, ownerId } = await stalledFold(clock);
    await database.collectGarbage();
    // Reconciled: the expired owner is aborted and its job no longer waits on it.
    expect((await store.getTransaction(ownerId))?.status ?? "reclaimed").not.toBe("active");
    const reconciled = await store.getCompactionJob(jobId);
    expect(
      reconciled === undefined ||
        reconciled.state === "cancelled" ||
        reconciled.transactionId !== ownerId,
    ).toBe(true);
    await database.execute("INSERT INTO t VALUES(1000, 1, 'y')");
    expect((await database.compactTable("t")).compacted).toBe(true);
    expect((await database.query("SELECT COUNT(*) AS n FROM t")).rows).toEqual([{ n: 61 }]);
    await database.close();
  });
});

describe("backing off a failing fold", () => {
  class PlanFaultStore extends MemoryBlockStore {
    failing = false;
    planAttempts = 0;

    override async createCompactionJob(
      record: Parameters<MemoryBlockStore["createCompactionJob"]>[0],
    ): Promise<void> {
      this.planAttempts += 1;
      if (this.failing) throw new Error("injected planning failure");
      return super.createCompactionJob(record);
    }
  }

  it("retries on a doubling timer, not on every eighth commit", async () => {
    const store = new PlanFaultStore();
    const database = new MinnowDatabase(store);
    await database.execute("CREATE TABLE t(pk INTEGER PRIMARY KEY, a INTEGER)");
    store.failing = true;
    for (let index = 0; index < 400; index += 1) {
      await database.execute(`INSERT INTO t VALUES(${String(index)}, ${String(index)})`);
    }
    // Four hundred commits to a table due for a fold from the forty-eighth: a segment-only
    // backoff attempts again at 96, 192, and 256 visible segments and then on every check,
    // some twenty times; a time backoff that doubles from a quarter second attempts a handful.
    const duringBurst = store.planAttempts;
    expect(duringBurst).toBeGreaterThan(0);
    expect(duringBurst).toBeLessThanOrEqual(6);
    // The timer re-checks the table itself, so a repaired store folds without another commit.
    store.failing = false;
    const deadline = Date.now() + 20_000;
    while ((await publishedFolds(database, "t")) === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(await publishedFolds(database, "t")).toBeGreaterThan(0);
    expect(store.planAttempts).toBeGreaterThan(duringBurst);
    await database.close();
  }, 60_000);
});
