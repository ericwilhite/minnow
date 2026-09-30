import { expect, it, vi } from "vitest";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";

it.each(["hook", "console", "throwing hook"] as const)(
  "reports a failed lazy full-text index through %s while the scan remains correct",
  async (mode) => {
    const failure = new Error("injected index I/O failure");
    class FaultStore extends MemoryBlockStore {
      attempts = 0;
      override async beginFtsBaseBuild(...args: Parameters<MemoryBlockStore["beginFtsBaseBuild"]>) {
        void args;
        this.attempts += 1;
        throw failure;
      }
    }
    const store = new FaultStore();
    const reports: Array<{ error: unknown; context: string }> = [];
    const logged = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const hookError = new Error("broken diagnostic observer");
    const db = new MinnowDatabase(store, {
      autoCompact: false,
      autoCollect: false,
      ftsAutoIndexRows: 1,
      ...(mode === "console"
        ? {}
        : {
            onBackgroundError(error: unknown, context: string) {
              reports.push({ error, context });
              if (mode === "throwing hook") throw hookError;
            },
          }),
    });
    try {
      await db.execute("CREATE TABLE search (body TEXT)");
      await db.insertBatch("search", [{ body: "hello world" }, { body: "other" }]);
      expect(
        (await db.query("SELECT body FROM search WHERE MATCH(body) AGAINST 'hello'")).rows,
      ).toEqual([{ body: "hello world" }]);
      // close joins the background build, so the assertion cannot race the failure report.
      await db.close();
      expect(store.attempts).toBe(1);
      if (mode !== "console")
        expect(reports).toEqual([
          { error: failure, context: "full-text index build for search/body" },
        ]);
      if (mode === "hook") expect(logged).not.toHaveBeenCalled();
      else
        expect(logged).toHaveBeenCalledWith(
          "[minnowdb] background failure (full-text index build for search/body):",
          failure,
        );
      if (mode === "throwing hook")
        expect(logged).toHaveBeenCalledWith(
          "[minnowdb] onBackgroundError callback failed:",
          hookError,
        );
    } finally {
      await db.close();
      logged.mockRestore();
    }
  },
);

it("reports automatic compaction failures while preserving the committed table", async () => {
  const failure = new Error("injected compaction read failure");
  const reports: Array<{ error: unknown; context: string }> = [];
  class FaultStore extends MemoryBlockStore {
    armed = false;
    override async getBlocks(...args: Parameters<MemoryBlockStore["getBlocks"]>) {
      if (this.armed) throw failure;
      return super.getBlocks(...args);
    }
  }
  const store = new FaultStore();
  const db = new MinnowDatabase(store, {
    autoCollect: false,
    rowsPerBlock: 1,
    compression: "raw",
    onBackgroundError: (error, context) => reports.push({ error, context }),
  });
  try {
    await db.execute("CREATE TABLE compacting (n INTEGER)");
    // Forty-seven independent commits remain below the automatic scan-fold threshold.
    for (let n = 0; n < 47; n += 1) await db.insert("compacting", { n });
    store.armed = true;
    await db.insert("compacting", { n: 47 });
    await vi.waitFor(() =>
      expect(
        reports.some(
          (report) =>
            report.error === failure && report.context === "automatic compaction for compacting",
        ),
      ).toBe(true),
    );
    store.armed = false;
    expect(
      (await db.query("SELECT COUNT(*) AS n, SUM(n) AS s FROM compacting", { memoize: false }))
        .rows,
    ).toEqual([{ n: 48, s: 1128 }]);
  } finally {
    store.armed = false;
    await db.close();
  }
});

it("reports a failed secondary-index rebuild and serves the correct scan result", async () => {
  const failure = new Error("injected secondary-index I/O failure");
  const reports: Array<{ error: unknown; context: string }> = [];
  class FaultStore extends MemoryBlockStore {
    armed = false;
    override async beginFtsBaseBuild(...args: Parameters<MemoryBlockStore["beginFtsBaseBuild"]>) {
      if (this.armed) throw failure;
      return super.beginFtsBaseBuild(...args);
    }
  }
  const store = new FaultStore();
  const db = new MinnowDatabase(store, {
    autoCollect: false,
    autoCompact: false,
    rowsPerBlock: 1,
    onBackgroundError: (error, context) => reports.push({ error, context }),
  });
  try {
    await db.execute("CREATE TABLE indexed (id INTEGER PRIMARY KEY, s TEXT)");
    await db.execute("INSERT INTO indexed VALUES (1,'a'),(2,'b')");
    await db.execute("CREATE INDEX by_s ON indexed(s)");
    const table = await store.getTableByName("indexed");
    if (table?.secondaryIndexes === undefined) throw new Error("Secondary index fixture missing");
    const entry = Object.entries(table.secondaryIndexes)[0];
    if (entry === undefined) throw new Error("Secondary index fixture empty");
    const [indexId] = entry;
    // A missing base response triggers the lazy rebuild without illegally removing an active index.
    vi.spyOn(store, "readFtsCandidates").mockImplementation(async (...args) => ({
      rowIdsByTerm: args[2].map(() => []),
      overflow: false,
      deltaChunkCount: 0,
      totalTokens: 0,
      coversVersion: -1,
      hasBase: false,
    }));
    store.armed = true;
    expect(
      (await db.query("SELECT id FROM indexed WHERE s = 'b'", { memoize: false })).rows,
    ).toEqual([{ id: 2 }]);
    await db.close();
    expect(reports).toEqual([
      { error: failure, context: `secondary index build for indexed/${indexId}` },
    ]);
  } finally {
    store.armed = false;
    await db.close();
  }
});

it("reports collection I/O failure even when it arrives while close joins maintenance", async () => {
  const failure = new Error("collection I/O failed during close");
  let release: () => void = () => undefined;
  const parked = new Promise<void>((resolve) => {
    release = resolve;
  });
  const reports: unknown[] = [];
  class FaultStore extends MemoryBlockStore {
    entered = false;
    override async createGarbageCollectionJob(
      ...args: Parameters<MemoryBlockStore["createGarbageCollectionJob"]>
    ): Promise<never> {
      void args;
      this.entered = true;
      await parked;
      throw failure;
    }
  }
  const store = new FaultStore();
  const db = new MinnowDatabase(store, {
    autoCompact: false,
    onBackgroundError: (error) => reports.push(error),
  });
  try {
    await db.execute("CREATE TABLE collected (n INTEGER)");
    for (let n = 0; n < 64; n += 1) await db.insert("collected", { n });
    await vi.waitFor(() => expect(store.entered).toBe(true));
    const closing = db.close();
    release();
    await closing;
    expect(reports).toEqual([failure]);
    const reopened = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    try {
      expect(
        (await reopened.query("SELECT COUNT(*) AS n, SUM(n) AS total FROM collected")).rows,
      ).toEqual([{ n: 64, total: 2016 }]);
    } finally {
      await reopened.close();
    }
  } finally {
    release();
    await db.close();
  }
});

it("reports both index execution and cleanup I/O failures instead of masking either", async () => {
  const buildError = new Error("build I/O failed");
  const cleanupError = new Error("cleanup I/O failed");
  const reports: unknown[] = [];
  class FaultStore extends MemoryBlockStore {
    override async beginFtsBaseBuild(...args: Parameters<MemoryBlockStore["beginFtsBaseBuild"]>) {
      void args;
      throw buildError;
    }
    override async removeFtsColumn(...args: Parameters<MemoryBlockStore["removeFtsColumn"]>) {
      void args;
      throw cleanupError;
    }
  }
  const db = new MinnowDatabase(new FaultStore(), {
    autoCompact: false,
    autoCollect: false,
    ftsAutoIndexRows: 1,
    onBackgroundError: (error) => reports.push(error),
  });
  try {
    await db.execute("CREATE TABLE search (body TEXT)");
    await db.insert("search", { body: "hello world" });
    expect(
      (await db.query("SELECT body FROM search WHERE MATCH(body) AGAINST 'hello'")).rows,
    ).toEqual([{ body: "hello world" }]);
    await db.close();
    expect(reports).toEqual([cleanupError, buildError]);
  } finally {
    await db.close();
  }
});
