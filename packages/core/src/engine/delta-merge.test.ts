/**
 * Delta merges: level-zero updates and deletes merged among themselves, the partitions left
 * alone. The resolver is checked against a per-row replay; the merge against a shadow model on
 * every store, across reopen, under an injected fault at every point the step reaches, between
 * two connections, against a tampered plan, and under a live query whose window spans it.
 */
import { describe, expect, it, vi } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import {
  CompactionBacklogError,
  IndexedDbBlockStore,
  MAX_LEVEL_ZERO_SEGMENTS,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
  type CompactionJobRecord,
  type SegmentRecord,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { FaultInjectingBlockStore, faultPoints, type FaultPoint } from "../testing/index.js";
import { MinnowDatabase, type BatchRow, type BatchValue } from "./database.js";
import { internalsOf } from "./internals.js";
import {
  encodeDeltaMergeOutputs,
  resolveDeltaMerge,
  type DeltaMergeColumn,
  type DeltaMergeSourceRows,
} from "./delta-merge.js";
import { liveMaintenanceTestHooks } from "./live-maintenance.js";
import { allSegmentRecords, allVisibleSegments, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const COLUMNS: DeltaMergeColumn[] = [
  { columnId: "k", type: "number" },
  { columnId: "a", type: "number" },
  { columnId: "b", type: "string" },
  { columnId: "c", type: "datetime" },
];

function source(
  kind: "update" | "delete",
  columns: Record<string, BatchValue[]>,
): DeltaMergeSourceRows {
  return {
    kind,
    rowCount: columns.k?.length ?? 0,
    columns: new Map(Object.entries(columns)),
  };
}

/** The naive replay a reader performs: each source in order, row by row. */
function replay(
  base: Map<number, Record<string, BatchValue>>,
  sources: readonly DeltaMergeSourceRows[],
): Map<number, Record<string, BatchValue>> {
  const rows = new Map([...base].map(([key, row]) => [key, { ...row }]));
  for (const delta of sources) {
    const keys = delta.columns.get("k") ?? [];
    for (const [index, key] of keys.entries()) {
      const row = rows.get(key as number);
      if (delta.kind === "delete") {
        rows.delete(key as number);
        continue;
      }
      if (row === undefined) continue;
      for (const [columnId, values] of delta.columns) row[columnId] = values[index] ?? null;
    }
  }
  return rows;
}

function mulberry32(seed: number): () => number {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe("delta merge resolution", () => {
  it("resolves each key's net change as a per-row replay does", () => {
    for (let seed = 1; seed <= 200; seed += 1) {
      const random = mulberry32(seed);
      const base = new Map<number, Record<string, BatchValue>>();
      for (let key = 0; key < 40; key += 1) {
        base.set(key, { k: key, a: key, b: `b${String(key)}`, c: new Date(key * 1000) });
      }
      const sources: DeltaMergeSourceRows[] = [];
      const live = new Set(base.keys());
      for (let index = 0; index < 1 + Math.floor(random() * 12); index += 1) {
        const keys = [...live].filter(() => random() < 0.2);
        if (keys.length === 0) continue;
        if (random() < 0.25) {
          sources.push(source("delete", { k: keys }));
          for (const key of keys) live.delete(key);
          continue;
        }
        const columns: Record<string, BatchValue[]> = { k: keys };
        if (random() < 0.6) columns.a = keys.map(() => Math.floor(random() * 100));
        if (random() < 0.5) {
          columns.b = keys.map(() => (random() < 0.2 ? null : `v${String(random())}`));
        }
        if (random() < 0.3) columns.c = keys.map(() => new Date(Math.floor(random() * 1e9)));
        sources.push(source("update", columns));
      }
      const merged = resolveDeltaMerge(COLUMNS, "k", sources);
      // Each key appears in at most one output, so the outputs replay in any order.
      const seen = new Set<unknown>();
      for (const output of merged) {
        expect(output.columns[0]?.columnId).toBe("k");
        const keys = output.values[0] ?? [];
        for (const key of keys) {
          expect(seen.has(key)).toBe(false);
          seen.add(key);
        }
        expect([...keys].sort((left, right) => (left as number) - (right as number))).toEqual(keys);
      }
      const replayedOutputs = merged.map((output) =>
        source(
          output.kind,
          Object.fromEntries(
            output.columns.map((column, index) => [
              column.columnId,
              [...(output.values[index] ?? [])],
            ]),
          ),
        ),
      );
      expect(replay(base, replayedOutputs)).toEqual(replay(base, sources));
    }
  });

  it("drops updates after a delete and updates left with only their key", () => {
    const merged = resolveDeltaMerge(COLUMNS, "k", [
      source("update", { k: [1, 2], a: [10, 20] }),
      source("delete", { k: [2] }),
      source("update", { k: [2, 3], b: ["late", "x"] }),
      source("update", { k: [4] }),
    ]);
    expect(merged.map((output) => [output.kind, output.columns.map((c) => c.columnId)])).toEqual([
      ["delete", ["k"]],
      ["update", ["k", "a"]],
      ["update", ["k", "b"]],
    ]);
    expect(merged.map((output) => output.values)).toEqual([[[2]], [[1], [10]], [[3], ["x"]]]);
  });

  it("orders string and datetime keys and checksums the exact blocks", () => {
    const strings: DeltaMergeColumn[] = [
      { columnId: "k", type: "string" },
      { columnId: "a", type: "number" },
    ];
    const merged = resolveDeltaMerge(strings, "k", [
      source("update", { k: ["b", "a", "é", "B"], a: [1, 2, 3, 4] }),
    ]);
    expect(merged[0]?.values[0]).toEqual(["B", "a", "b", "é"]);
    const dates: DeltaMergeColumn[] = [
      { columnId: "k", type: "datetime" },
      { columnId: "a", type: "number" },
    ];
    const resolved = resolveDeltaMerge(dates, "k", [
      source("update", { k: [new Date(5), new Date(1)], a: [1, 2] }),
      source("update", { k: [new Date(5)], a: [3] }),
    ]);
    expect(resolved[0]?.values).toEqual([
      [new Date(1), new Date(5)],
      [2, 3],
    ]);

    const once = encodeDeltaMergeOutputs(merged, [[4]]);
    const again = encodeDeltaMergeOutputs(
      resolveDeltaMerge(strings, "k", [
        source("update", { k: ["b", "a", "é", "B"], a: [1, 2, 3, 4] }),
      ]),
      [[4]],
    );
    expect(again.checksum).toBe(once.checksum);
    expect(encodeDeltaMergeOutputs(merged, [[2, 2]]).checksum).not.toBe(once.checksum);
    const changed = resolveDeltaMerge(strings, "k", [
      source("update", { k: ["b", "a", "é", "B"], a: [1, 2, 3, 5] }),
    ]);
    expect(encodeDeltaMergeOutputs(changed, [[4]]).checksum).not.toBe(once.checksum);
    expect(() => encodeDeltaMergeOutputs(merged, [[3]])).toThrow(TypeError);
  });
});

interface Fixture {
  store: BlockStore;
  reopen(): Promise<BlockStore>;
}

const stores: Record<string, () => Promise<Fixture>> = {
  memory: () => {
    const store = new MemoryBlockStore();
    return Promise.resolve({ store, reopen: () => Promise.resolve(store) });
  },
  indexeddb: async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    return {
      store,
      reopen: () => {
        store.close();
        return IndexedDbBlockStore.open({ name, indexedDB });
      },
    };
  },
  opfs: async () => {
    const root = new MemoryOpfs().root;
    const name = crypto.randomUUID();
    const store = await OpfsBlockStore.open({ name, root });
    return {
      store,
      reopen: () => {
        store.close();
        return OpfsBlockStore.open({ name, root });
      },
    };
  },
};

interface Item extends BatchRow {
  id: number;
  region: string | null;
  amount: number;
  label: string;
}

const OPTIONS = { autoCompact: false, autoCollect: false, rowsPerBlock: 256 } as const;
const ROWS = 1_200;

async function seed(database: MinnowDatabase): Promise<Map<number, Item>> {
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string", nullable: true },
      { name: "amount", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  const shadow = new Map<number, Item>();
  const rows: Item[] = [];
  for (let id = 1; id <= ROWS; id += 1) {
    const row = {
      id,
      region: id % 5 === 0 ? null : `r${String(id % 3)}`,
      amount: id,
      label: `l${String(id)}`,
    };
    rows.push(row);
    shadow.set(id, { ...row });
  }
  // Three inserts, folded into partitions: a single segment has nothing to fold with.
  for (let start = 0; start < rows.length; start += 400) {
    await database.insertBatch("items", rows.slice(start, start + 400));
  }
  expect((await database.compactTable("items", { partitionRows: 400 })).compacted).toBe(true);
  // A secondary index answers by key: a merge must leave its answers exact.
  await database.execute("CREATE INDEX items_label ON items (label)");
  return shadow;
}

/** Single-row commits of every mergeable kind, each mirrored in the shadow. */
async function history(
  database: MinnowDatabase,
  shadow: Map<number, Item>,
  commits: number,
  random: () => number,
): Promise<void> {
  for (let index = 0; index < commits; index += 1) {
    const live = [...shadow.keys()];
    const id = live[Math.floor(random() * live.length)] ?? 1;
    const row = shadow.get(id);
    if (row === undefined) continue;
    const choice = random();
    if (choice < 0.15) {
      await database.execute("DELETE FROM items WHERE id = ?", [id]);
      shadow.delete(id);
    } else if (choice < 0.45) {
      const label = `u${String(index)}`;
      await database.execute("UPDATE items SET label = ? WHERE id = ?", [label, id]);
      row.label = label;
    } else if (choice < 0.6) {
      await database.execute("UPDATE items SET region = NULL, amount = amount * 2 WHERE id = ?", [
        id,
      ]);
      row.region = null;
      row.amount *= 2;
    } else {
      await database.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [id]);
      row.amount += 1;
    }
  }
}

async function expectShadow(database: MinnowDatabase, shadow: Map<number, Item>): Promise<void> {
  const expected = [...shadow.values()].sort((left, right) => left.id - right.id);
  const scanned = await database.query("SELECT id, region, amount, label FROM items ORDER BY id", {
    memoize: false,
  });
  expect(scanned.rows).toEqual(expected);
  const filtered = await database.query(
    "SELECT id, amount FROM items WHERE region = 'r1' AND amount > 300 ORDER BY id",
    { memoize: false },
  );
  expect(filtered.rows).toEqual(
    expected
      .filter((row) => row.region === "r1" && row.amount > 300)
      .map(({ id, amount }) => ({ id, amount })),
  );
  const totals = await database.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM items", {
    memoize: false,
  });
  expect(totals.rows).toEqual([
    { n: expected.length, total: expected.reduce((sum, row) => sum + row.amount, 0) },
  ]);
  for (const label of ["l7", "l400", "u3", "u12", "u40"]) {
    const indexed = await database.query("SELECT id FROM items WHERE label = ? ORDER BY id", {
      params: [label],
      memoize: false,
    });
    expect(indexed.rows).toEqual(
      expected.filter((row) => row.label === label).map(({ id }) => ({ id })),
    );
  }
  for (const id of [1, 2, 3, 400, 401, 799, 800, 1_199, 1_200]) {
    const point = await database.query("SELECT id, region, amount, label FROM items WHERE id = ?", {
      params: [id],
      memoize: false,
    });
    expect(point.rows).toEqual(shadow.has(id) ? [shadow.get(id)] : []);
  }
}

/** The memory store has no integrity check; the persistent stores check every payload. */
async function expectIntegrity(database: MinnowDatabase, store: BlockStore): Promise<void> {
  if (store.checkIntegrity === undefined) return;
  expect((await database.checkIntegrity({ mode: "full" })).issues).toEqual([]);
}

async function visibleRecords(
  database: MinnowDatabase,
  store: BlockStore,
): Promise<SegmentRecord[]> {
  const table = await store.getTableByName("items");
  const visible = new Set((await allVisibleSegments(database, "items")).map(({ id }) => id));
  return (await allSegmentRecords(store, table?.id)).filter((segment) => visible.has(segment.id));
}

async function deltaJobs(database: MinnowDatabase): Promise<CompactionJobRecord[]> {
  return (await database.listCompactionJobs()).filter((job) => job.rewritePlan.kind === "delta-v1");
}

describe("delta merge", () => {
  for (const [kind, open] of Object.entries(stores)) {
    it(`merges deltas among themselves and leaves the partitions (${kind})`, async () => {
      const fixture = await open();
      const database = new MinnowDatabase(fixture.store, OPTIONS);
      const shadow = await seed(database);
      await history(database, shadow, 40, mulberry32(7));
      const before = await visibleRecords(database, fixture.store);
      const partitions = before.filter((segment) => segment.level > 0).map(({ id }) => id);
      expect(partitions.length).toBeGreaterThan(1);
      const deltas = before.filter((segment) => segment.level === 0);
      expect(deltas.length).toBeGreaterThanOrEqual(40);
      await expectShadow(database, shadow);

      const progress = await internalsOf(database).automaticCompactionStep("items");
      expect(progress.result?.compacted).toBe(true);
      const [job] = await deltaJobs(database);
      expect(job?.state).toBe("published");
      expect(job?.targetLevel).toBe(0);
      const after = await visibleRecords(database, fixture.store);
      expect(after.filter((segment) => segment.level > 0).map(({ id }) => id)).toEqual(partitions);
      const merged = after.filter((segment) => segment.level === 0);
      // One delete and an update per changed-column set: label; region with amount; amount.
      expect(merged.length).toBeLessThanOrEqual(4);
      expect(merged.every((segment) => segment.kind === "update" || segment.kind === "delete"));
      // The merged run keeps its place: its outputs carry the run's last order.
      expect(new Set(merged.map((segment) => segment.logicalOrder)).size).toBe(1);
      expect(merged[0]?.logicalOrder).toBe(Math.max(...deltas.map((s) => s.logicalOrder)));
      await expectShadow(database, shadow);
      await expectIntegrity(database, fixture.store);

      // The merged deltas merge again with the commits after them, and a fold takes them all.
      await history(database, shadow, 30, mulberry32(8));
      await internalsOf(database).automaticCompactionStep("items");
      await expectShadow(database, shadow);
      await database.compactTable("items");
      await expectShadow(database, shadow);
      await database.close();

      const reopened = new MinnowDatabase(await fixture.reopen(), OPTIONS);
      await expectShadow(reopened, shadow);
      await expectIntegrity(reopened, fixture.store);
      await reopened.close();
    });
  }

  for (const [kind, seeds] of [
    ["memory", 24],
    ["indexeddb", 4],
    ["opfs", 4],
  ] as const) {
    it(`answers as a shadow model across random histories, merges, folds, and reopens (${kind})`, async () => {
      let merges = 0;
      for (let run = 1; run <= seeds; run += 1) {
        const random = mulberry32(run * 7_919);
        const open = stores[kind];
        if (open === undefined) throw new Error(`No ${kind} store`);
        const fixture = await open();
        let database = new MinnowDatabase(fixture.store, OPTIONS);
        const shadow = await seed(database);
        let nextId = ROWS + 1;
        for (let step = 0; step < 6; step += 1) {
          // Mostly runs of single-row updates and deletes, now and then an insert or an upsert
          // that breaks a run, and multi-row updates that make a merged row set larger.
          const commits = 8 + Math.floor(random() * 40);
          for (let commit = 0; commit < commits; commit += 1) {
            const choice = random();
            if (choice < 0.06) {
              const row = { id: nextId, region: "r9", amount: nextId, label: `n${String(nextId)}` };
              nextId += 1;
              await database.insertBatch("items", [row]);
              shadow.set(row.id, { ...row });
            } else if (choice < 0.1) {
              const live = [...shadow.keys()];
              const id = live[Math.floor(random() * live.length)] ?? 1;
              const row = { id, region: null, amount: -id, label: "upserted" };
              await database.upsertBatch("items", [row]);
              shadow.set(id, { ...row });
            } else if (choice < 0.14) {
              const low = 1 + Math.floor(random() * ROWS);
              await database.execute(
                "UPDATE items SET amount = amount + 3 WHERE id BETWEEN ? AND ?",
                [low, low + 25],
              );
              for (const [id, row] of shadow) {
                if (id >= low && id <= low + 25) row.amount += 3;
              }
            } else {
              await history(database, shadow, 1, random);
            }
          }
          const progress = await internalsOf(database).automaticCompactionStep("items");
          const job = (await database.listCompactionJobs()).find(
            (candidate) => candidate.id === progress.result?.jobId,
          );
          if (job?.rewritePlan.kind === "delta-v1") merges += 1;
          await expectShadow(database, shadow);
          if (random() < 0.3) {
            await database.close();
            database = new MinnowDatabase(await fixture.reopen(), OPTIONS);
            await expectShadow(database, shadow);
          }
        }
        await expectIntegrity(database, fixture.store);
        await database.close();
      }
      // The histories must actually reach the merge, not only the fold.
      expect(merges).toBeGreaterThan(seeds);
    });
  }

  it("declines below eight mergeable commits and past the rows a fold is due at", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
    const shadow = await seed(database);
    await history(database, shadow, 7, mulberry32(3));
    // Seven deltas: the automatic step folds them instead (two suffice for a fold).
    await internalsOf(database).automaticCompactionStep("items");
    expect(await deltaJobs(database)).toEqual([]);
    await expectShadow(database, shadow);

    // A large delta over a small table is folded, not merged.
    const keys = [...shadow.keys()].slice(0, 1_100);
    await database.updateBatch("items", {
      keys,
      changes: { amount: keys.map((id) => (shadow.get(id)?.amount ?? 0) + 1) },
    });
    for (const id of keys) {
      const row = shadow.get(id);
      if (row !== undefined) row.amount += 1;
    }
    await history(database, shadow, 10, mulberry32(4));
    await internalsOf(database).automaticCompactionStep("items");
    expect(await deltaJobs(database)).toEqual([]);
    await expectShadow(database, shadow);
    await database.close();
  });

  for (const [kind, open] of Object.entries(stores)) {
    it(`recovers from a fault at every point of the merge step (${kind})`, async () => {
      // Count what the step reaches, then fail each point in turn on a fresh copy.
      const counts = new Map<FaultPoint, number>(faultPoints.map((point) => [point, 0]));
      {
        const fixture = await open();
        let armed = false;
        const database = new MinnowDatabase(
          new FaultInjectingBlockStore(fixture.store, (point) => {
            if (armed) counts.set(point, (counts.get(point) ?? 0) + 1);
          }),
          OPTIONS,
        );
        const shadow = await seed(database);
        await history(database, shadow, 12, mulberry32(11));
        armed = true;
        await internalsOf(database).automaticCompactionStep("items");
        armed = false;
        expect((await deltaJobs(database))[0]?.state).toBe("published");
        await database.close();
      }
      let injections = 0;
      for (const point of faultPoints) {
        for (let nth = 1; nth <= (counts.get(point) ?? 0); nth += 1) {
          const fixture = await open();
          const injected = new Error(`injected ${point} #${String(nth)}`);
          let seen = 0;
          let enabled = false;
          const database = new MinnowDatabase(
            new FaultInjectingBlockStore(fixture.store, (at) => {
              if (enabled && at === point && ++seen === nth) throw injected;
            }),
            OPTIONS,
          );
          const shadow = await seed(database);
          await history(database, shadow, 12, mulberry32(11));
          enabled = true;
          await internalsOf(database)
            .automaticCompactionStep("items")
            .catch((error: unknown) => {
              if (!(error instanceof Error) || !causedBy(error, injected)) throw error;
            });
          enabled = false;
          await database.close();
          injections += 1;
          // Another connection finds the data whole, finishes or replans the merge, and the
          // data is still whole afterwards.
          const reopened = new MinnowDatabase(await fixture.reopen(), OPTIONS);
          try {
            await expectShadow(reopened, shadow);
            for (let step = 0; step < 4; step += 1) {
              const active = (await deltaJobs(reopened)).some(
                (job) =>
                  job.state === "planned" || job.state === "running" || job.state === "ready",
              );
              if (!active && step > 0) break;
              await internalsOf(reopened).automaticCompactionStep("items");
            }
            const jobs = await deltaJobs(reopened);
            expect(jobs.some((job) => job.state === "published")).toBe(true);
            expect(jobs.filter((job) => job.state === "published")).toHaveLength(1);
            await expectShadow(reopened, shadow);
            await expectIntegrity(reopened, fixture.store);
          } catch (error) {
            throw new Error(`${injected.message}: recovery failed`, { cause: error });
          } finally {
            await reopened.close();
          }
        }
      }
      expect(injections).toBeGreaterThan(4);
    });
  }

  // IndexedDB enforces the same count (existing + added - retired) in its own commit; four
  // thousand commits through fake-indexeddb take minutes, so the record-core stores carry it.
  for (const kind of ["memory", "opfs"] as const) {
    it(`publishes at the level-zero ceiling it relieves (${kind})`, async () => {
      const open = stores[kind];
      if (open === undefined) throw new Error(`No ${kind} store`);
      const fixture = await open();
      // Collection runs: four thousand commits of garbage would otherwise refuse writes first.
      const database = new MinnowDatabase(fixture.store, { ...OPTIONS, autoCollect: true });
      const shadow = await seed(database);
      // Single-row updates of a hundred hot rows until the ceiling refuses one: every commit is
      // a mergeable delta, and they merge to a hundred rows.
      let refused = false;
      for (let index = 0; index < MAX_LEVEL_ZERO_SEGMENTS + 8 && !refused; index += 1) {
        const id = (index % 100) + 1;
        try {
          await database.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [id]);
          const row = shadow.get(id);
          if (row !== undefined) row.amount += 1;
        } catch (error) {
          expect(error).toBeInstanceOf(CompactionBacklogError);
          refused = true;
        }
      }
      expect(refused).toBe(true);
      // The merge adds level-zero segments to a table already at the ceiling; it retires more
      // than it adds, so the ceiling it relieves must not refuse it.
      const progress = await internalsOf(database).automaticCompactionStep("items");
      expect(progress.result?.compacted).toBe(true);
      expect((await deltaJobs(database))[0]?.state).toBe("published");
      await database.execute("UPDATE items SET amount = amount + 1 WHERE id = 1");
      const row = shadow.get(1);
      if (row !== undefined) row.amount += 1;
      await expectShadow(database, shadow);
      await database.close();
    });
  }

  it("lets one of two connections publish a merge both step", async () => {
    const store = new MemoryBlockStore();
    const first = new MinnowDatabase(store, OPTIONS);
    const shadow = await seed(first);
    await history(first, shadow, 20, mulberry32(21));
    const second = new MinnowDatabase(store, OPTIONS);
    const results = await Promise.allSettled([
      internalsOf(first).automaticCompactionStep("items"),
      internalsOf(second).automaticCompactionStep("items"),
    ]);
    expect(results.some((result) => result.status === "fulfilled")).toBe(true);
    for (const database of [first, second]) {
      for (let step = 0; step < 3; step += 1) {
        await internalsOf(database)
          .automaticCompactionStep("items")
          .catch(() => undefined);
      }
    }
    const published = (await deltaJobs(first)).filter((job) => job.state === "published");
    expect(published.length).toBeGreaterThanOrEqual(1);
    const sources = published.flatMap((job) => job.sourceSegmentIds);
    expect(new Set(sources).size).toBe(sources.length);
    await expectShadow(first, shadow);
    await expectShadow(second, shadow);
    await first.close();
    await second.close();
  });

  it("refuses to write a merge whose plan its sources do not reproduce", async () => {
    const inner = new MemoryBlockStore();
    const store = new Proxy(inner, {
      get(target, property, receiver) {
        if (property === "createCompactionJob") {
          return async (record: CompactionJobRecord) => {
            if (record.rewritePlan.kind !== "delta-v1") return target.createCompactionJob(record);
            const plan = record.rewritePlan;
            return target.createCompactionJob({
              ...record,
              rewritePlan: {
                ...plan,
                resolution: { ...plan.resolution, checksum: (plan.resolution.checksum ^ 1) >>> 0 },
              },
            });
          };
        }
        const value: unknown = Reflect.get(target, property, receiver);
        return typeof value === "function"
          ? ((value as (...args: unknown[]) => unknown).bind(target) as unknown)
          : value;
      },
    });
    const database = new MinnowDatabase(store, OPTIONS);
    const shadow = await seed(database);
    await history(database, shadow, 12, mulberry32(31));
    await expect(internalsOf(database).automaticCompactionStep("items")).rejects.toThrow(
      "differs from its plan",
    );
    expect((await deltaJobs(database)).some((job) => job.state === "published")).toBe(false);
    await expectShadow(database, shadow);
    await database.close();
  });

  it.each([0, 30])(
    "folds a quiet table's deltas into its partitions, but not a trickle (pause %i ms)",
    async (pauseMs) => {
      const store = new MemoryBlockStore();
      const database = new MinnowDatabase(store, { rowsPerBlock: 256 });
      const shadow = await seed(database);
      const levelZero = async (): Promise<number> =>
        (await visibleRecords(database, store)).filter((segment) => segment.level === 0).length;
      const quiet = async (expectedLevelZero: number): Promise<void> => {
        let settled = 0;
        for (let attempt = 0; attempt < 200; attempt += 1) {
          await new Promise((resolve) => setTimeout(resolve, 25));
          const active = (await database.listCompactionJobs()).some(
            (job) => job.state === "planned" || job.state === "running" || job.state === "ready",
          );
          // Planning precedes the durable job record. Absence of a job alone can look idle
          // while a loaded runner is still planning the fold; require its resulting layout too.
          settled = !active && (await levelZero()) === expectedLevelZero ? settled + 1 : 0;
          if (settled >= 10) return;
        }
        throw new Error("Maintenance did not go quiet");
      };
      // Keep this first burst below the 256-row idle-fold threshold even if a busy runner pauses
      // between commits. It must merge before the final atomic write makes the idle fold due.
      for (let id = 1; id <= 32; id += 1) {
        if (pauseMs > 0) await new Promise((resolve) => setTimeout(resolve, pauseMs));
        await database.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [id]);
        const row = shadow.get(id);
        if (row !== undefined) row.amount += 1;
      }
      await quiet(1);
      expect((await deltaJobs(database)).length).toBeGreaterThan(0);
      // Single-row commits after an early idle fold would leave a valid sub-threshold trickle.
      // Publish the remaining rows together so this test can require a completely folded tail.
      await database.execute("UPDATE items SET amount = amount + 1 WHERE id BETWEEN 33 AND 300");
      for (let id = 33; id <= 300; id += 1) {
        const row = shadow.get(id);
        if (row !== undefined) row.amount += 1;
      }
      await quiet(0);
      expect(await levelZero()).toBe(0);
      expect((await deltaJobs(database)).length).toBeGreaterThan(0);
      await expectShadow(database, shadow);
      // A few rows are not worth a fold: they wait for more writes.
      for (let id = 1; id <= 5; id += 1) {
        await database.execute("UPDATE items SET label = 'trickle' WHERE id = ?", [id]);
        const row = shadow.get(id);
        if (row !== undefined) row.label = "trickle";
      }
      await quiet(5);
      expect(await levelZero()).toBe(5);
      await expectShadow(database, shadow);
      await database.close();
    },
  );

  it("is not a change to a live query whose window spans it", async () => {
    const store = new MemoryBlockStore();
    const writer = new MinnowDatabase(store, OPTIONS);
    const shadow = await seed(writer);
    // Updates of the column the aggregate reads, left as deltas.
    for (let id = 1; id <= 12; id += 1) {
      await writer.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [id]);
      const row = shadow.get(id);
      if (row !== undefined) row.amount += 1;
    }
    // The live set sits on another connection, so it sees the writer's commits only when it
    // refreshes: one window then spans a commit and the merge after it.
    const observer = new MinnowDatabase(store, OPTIONS);
    const live = observer.liveQueries();
    const results: unknown[] = [];
    const sql = "SELECT COUNT(*) AS n, SUM(amount) AS total FROM items";
    await live.subscribe(sql, { onChange: (result) => results.push(result.rows) });
    // A commit that writes no column the aggregate reads, then a merge of every delta —
    // the amount updates among them — in the same window.
    await writer.execute("UPDATE items SET label = 'quiet' WHERE id = 500");
    const row = shadow.get(500);
    if (row !== undefined) row.label = "quiet";
    await internalsOf(writer).automaticCompactionStep("items");
    expect((await deltaJobs(writer))[0]?.state).toBe("published");
    const queries = liveMaintenanceTestHooks.deltaQueries;
    await live.refresh();
    // The merge's outputs hold amount updates, but no row changed: the window's one change
    // leaves every column the aggregate reads, so it is answered without a query.
    expect(liveMaintenanceTestHooks.deltaQueries).toBe(queries);
    expect(live.stats.reruns).toBe(0);
    expect(results).toHaveLength(1);
    // A real change in a window that also holds a merge is still seen.
    await history(writer, shadow, 10, mulberry32(41));
    await internalsOf(writer).automaticCompactionStep("items");
    await live.refresh();
    const total = [...shadow.values()].reduce((sum, item) => sum + item.amount, 0);
    expect(results.at(-1)).toEqual([{ n: shadow.size, total }]);
    await expectShadow(observer, shadow);
    live.close();
    await observer.close();
    await writer.close();
  });
});

function causedBy(error: Error, injected: Error): boolean {
  const seen = new Set<unknown>();
  let current: unknown = error;
  while (current instanceof Error && !seen.has(current)) {
    if (current === injected) return true;
    seen.add(current);
    current = current.cause;
  }
  return false;
}
