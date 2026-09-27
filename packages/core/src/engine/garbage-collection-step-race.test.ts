import { expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabase } from "./database.js";

/**
 * Another connection finishes the collector's job and drops its record, as a second tab does
 * when its pass completes the shared job and then prunes finished records or hands the discovery
 * cursor to a successor job. It lands either just before the collector's next step, which then
 * loses the revision race, or just after one, so the collector's next resumption finds nothing.
 */
class JobFinishedElsewhereStore extends MemoryBlockStore {
  armed = false;
  raced = false;

  constructor(readonly lands: "before step" | "between steps") {
    super();
  }

  override async runGarbageCollectionStep(
    input: Parameters<MemoryBlockStore["runGarbageCollectionStep"]>[0],
  ) {
    if (!this.armed || this.raced) return super.runGarbageCollectionStep(input);
    if (this.lands === "before step") {
      await this.#finishElsewhere(input.jobId, input.updatedAt);
      return super.runGarbageCollectionStep(input);
    }
    const step = await super.runGarbageCollectionStep(input);
    if (step.job.state !== "completed") await this.#finishElsewhere(input.jobId, input.updatedAt);
    return step;
  }

  async #finishElsewhere(jobId: string, updatedAt: string): Promise<void> {
    this.raced = true;
    let job = await this.getGarbageCollectionJob(jobId);
    while (job !== undefined && job.state !== "completed") {
      const step = await super.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: 1_024,
        updatedAt,
      });
      job = step.job;
    }
    await this.removeGarbageCollectionJob(jobId);
  }
}

/** Background collection keeps a version for a minute; the test moves the clock instead. */
function testClock(): { now: () => Date; advance: (ms: number) => void } {
  let current = Date.parse("2026-01-01T00:00:00Z");
  return {
    now: () => new Date(current),
    advance: (ms) => {
      current += ms;
    },
  };
}

it("collectGarbage plans again when another connection finished and removed its job", async () => {
  const store = new JobFinishedElsewhereStore("before step");
  const db = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  try {
    await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, amount INTEGER)");
    for (let id = 0; id < 8; id += 1) {
      await db.execute("INSERT INTO items VALUES (?, ?)", [id, id]);
    }
    store.armed = true;

    await db.collectGarbage({ maxItemsPerStep: 1 });

    expect(store.raced).toBe(true);
    expect(
      (await store.listGarbageCollectionJobs()).filter((job) => job.state !== "completed"),
    ).toEqual([]);
    expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 8 }]);
  } finally {
    await db.close();
    store.close();
  }
});

it.each(["before step", "between steps"] as const)(
  "background collection reports no error when another connection finishes its job %s",
  async (lands) => {
    // Seen in real browsers: two tabs stepping one collection job over OPFS. One tab's step lost
    // the revision race, and by the time it re-read the job the other tab had completed and
    // removed it; the engine reported "Garbage collection job not found" through
    // onBackgroundError instead of treating the job as done elsewhere.
    const store = new JobFinishedElsewhereStore(lands);
    const clock = testClock();
    const background: Array<{ error: unknown; context: string }> = [];
    const db = new MinnowDatabase(store, {
      autoCompact: false,
      now: clock.now,
      onBackgroundError: (error, context) => background.push({ error, context }),
    });
    const settled = async () => {
      await vi.waitFor(() => {
        const status = db.maintenanceStatus();
        expect(status.collectionRunning || status.collectionRequested).toBe(false);
      });
    };
    try {
      await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, amount INTEGER)");
      for (let id = 0; id < 256; id += 1) {
        await db.execute("INSERT INTO items VALUES (?, ?)", [id, id]);
      }
      await settled();
      // Background jobs plan at most one step's worth of work. A job another connection planned
      // with a larger budget takes several, which opens the window between two of them.
      const planner = new MinnowDatabase(store, {
        autoCollect: false,
        autoCompact: false,
        now: clock.now,
      });
      const planned = await planner.collectGarbageStep({ maxItems: 1, retainRecentVersions: 0 });
      expect(planned.result).toBeNull();
      await planner.close();
      store.armed = true;

      // After a quiet minute, the next commit starts a pass.
      clock.advance(61_000);
      await db.execute("UPDATE items SET amount = amount + 1 WHERE id = 0");
      await vi.waitFor(() => {
        expect(store.raced).toBe(true);
      });
      await settled();

      expect(background).toEqual([]);
      expect(db.maintenanceStatus()).toMatchObject({ consecutiveFailures: 0, lastError: null });
    } finally {
      await db.close();
      store.close();
    }
  },
);
