/**
 * Background maintenance shared by several connections over one database. Every connection
 * folds tables and rebuilds index tails on its own commits, so with several of them writing the
 * same table they reach the same job together. Losing that race to another connection is not a
 * failure -- the other connection is doing the work -- and must not reach `onBackgroundError`;
 * the work must still get done.
 */
import { expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabase } from "./database.js";
import { allVisibleSegments, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const CONNECTIONS = 6;
const ROUNDS = 48;
const KEYS = 40;

async function waitForIdleMaintenance(databases: readonly MinnowDatabase[]): Promise<void> {
  const [first] = databases;
  if (first === undefined) return;
  await vi.waitFor(
    async () => {
      const jobs = await first.listCompactionJobs("items");
      expect(
        jobs.filter(
          (job) =>
            job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
        ),
      ).toEqual([]);
    },
    { timeout: 60_000, interval: 50 },
  );
  // close joins every in-flight fold and index build, so nothing settles after the asserts.
  await Promise.all(databases.map((database) => database.close()));
}

it("several connections writing one indexed table report no lost races and still converge", async () => {
  const store = new MemoryBlockStore();
  const reports: Array<{ name: string; context: string; message: string }> = [];
  const open = () =>
    new MinnowDatabase(store, {
      rowsPerBlock: 8,
      onBackgroundError: (error, context) =>
        reports.push({
          name: error instanceof Error ? error.name : typeof error,
          context,
          message: error instanceof Error ? error.message : String(error),
        }),
    });
  const databases = Array.from({ length: CONNECTIONS }, open);
  const [owner] = databases;
  if (owner === undefined) throw new Error("No connection");
  await owner.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT NOT NULL, qty INTEGER NOT NULL)",
  );
  await owner.execute("CREATE INDEX items_by_category ON items(category)");
  const expected = new Map<number, { category: string; qty: number }>();
  // Each connection upserts its own slice of keys, one statement per commit, all of them at once:
  // every commit adds a delta to the index tail and a segment to the table, so the tail bound
  // and the fold threshold are crossed by several connections together, many times over.
  await Promise.all(
    databases.map(async (database, connection) => {
      for (let round = 0; round < ROUNDS; round += 1) {
        const id = connection * KEYS + ((round * 7) % KEYS);
        const category = `c${String((round + connection) % 5)}`;
        await database.execute(
          "INSERT INTO items VALUES (?, ?, ?) ON CONFLICT (id) DO UPDATE SET category = excluded.category, qty = excluded.qty",
          [id, category, round],
        );
        expected.set(id, { category, qty: round });
        if (round % 6 === 5) {
          await database.query("SELECT id FROM items WHERE category = ?", {
            params: [category],
            memoize: false,
          });
        }
      }
    }),
  );
  await waitForIdleMaintenance(databases);

  const reader = open();
  try {
    expect(reports).toEqual([]);
    // Compaction ran and left the table under its fold thresholds.
    const jobs = await reader.listCompactionJobs("items");
    expect(jobs.some((job) => job.state === "published")).toBe(true);
    const segments = await Promise.all(
      (await allVisibleSegments(reader, "items")).map((segment) => store.getSegment(segment.id)),
    );
    expect(segments.filter((segment) => segment?.level === 0).length).toBeLessThan(48);
    // The index tail was folded into its base, and the index is ready.
    const table = await store.getTableByName("items");
    const index = Object.values(table?.secondaryIndexes ?? {})[0];
    expect(index?.state).toBe("ready");
    const version = (await store.getCurrentManifestVersion()) ?? -1;
    const candidates = await store.readFtsCandidates(
      table?.id ?? "",
      index?.storageColumnId ?? "",
      [],
      version,
    );
    expect(candidates.hasBase).toBe(true);
    expect(candidates.deltaChunkCount).toBeLessThanOrEqual(64);
    // And every row is right, through the index and without it.
    for (let category = 0; category < 5; category += 1) {
      const name = `c${String(category)}`;
      const want = [...expected]
        .filter(([, row]) => row.category === name)
        .map(([id]) => id)
        .sort((left, right) => left - right);
      const rows = (
        await reader.query("SELECT id FROM items WHERE category = ? ORDER BY id", {
          params: [name],
          memoize: false,
        })
      ).rows;
      expect(rows.map((row) => row.id)).toEqual(want);
    }
  } finally {
    await reader.close();
  }
});

it("a build of an index another connection is building defers to it instead of undoing it", async () => {
  // The first build parks after claiming its session; a second connection's query over the
  // index, which is still building, schedules a build of its own while the first is live.
  let release: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  class ParkedBuildStore extends MemoryBlockStore {
    parked = false;
    entered = false;
    override async writeFtsBaseBuildChunk(
      ...args: Parameters<MemoryBlockStore["writeFtsBaseBuildChunk"]>
    ) {
      if (this.parked) {
        this.entered = true;
        await gate;
      }
      return super.writeFtsBaseBuildChunk(...args);
    }
  }
  const store = new ParkedBuildStore();
  const reports: unknown[] = [];
  const options = {
    autoCompact: false,
    autoCollect: false,
    rowsPerBlock: 4,
    onBackgroundError: (error: unknown) => reports.push(error),
  };
  const builder = new MinnowDatabase(store, options);
  const other = new MinnowDatabase(store, options);
  try {
    await builder.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT)");
    for (let id = 1; id <= 24; id += 1) {
      await builder.execute("INSERT INTO items VALUES (?, ?)", [id, `c${String(id % 3)}`]);
    }
    store.parked = true;
    const created = builder.execute("CREATE INDEX items_by_category ON items(category)");
    await vi.waitFor(() => expect(store.entered).toBe(true));
    const building = await store.getTableByName("items");
    const buildId = Object.values(building?.secondaryIndexes ?? {})[0]?.buildId;
    expect(buildId).toBeDefined();

    expect(
      (
        await other.query("SELECT id FROM items WHERE category = 'c1' ORDER BY id", {
          memoize: false,
        })
      ).rows.map((row) => row.id),
    ).toEqual([1, 4, 7, 10, 13, 16, 19, 22]);
    // close joins the other connection's background build attempt.
    await other.close();
    expect(reports).toEqual([]);
    const during = await store.getTableByName("items");
    expect(Object.values(during?.secondaryIndexes ?? {})[0]).toMatchObject({
      state: "building",
      buildId,
    });

    store.parked = false;
    release();
    await created;
    const after = await store.getTableByName("items");
    expect(Object.values(after?.secondaryIndexes ?? {})[0]?.state).toBe("ready");
    expect(reports).toEqual([]);
  } finally {
    release();
    await other.close();
    await builder.close();
  }
});
