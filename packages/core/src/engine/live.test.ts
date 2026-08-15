import {
  MemoryBlockStore,
  type CommitTransactionInput,
  type Manifest,
  type StoragePage,
} from "../storage/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { LiveQuerySet, type LiveQueryHintChannel, type LiveQueryInput } from "./live.js";
import { type QueryResult } from "./query.js";

/** A deterministic in-process stand-in for BroadcastChannel. */
function createChannelPair(): [LiveQueryHintChannel, LiveQueryHintChannel] {
  const listenersA = new Set<() => void>();
  const listenersB = new Set<() => void>();
  const endpoint = (own: Set<() => void>, peer: Set<() => void>): LiveQueryHintChannel => ({
    postMessage: () => {
      for (const listener of [...peer]) listener();
    },
    addEventListener: (_type, listener) => own.add(listener),
    removeEventListener: (_type, listener) => own.delete(listener),
  });
  return [endpoint(listenersA, listenersB), endpoint(listenersB, listenersA)];
}

/**
 * A deterministic host for interleaving tests: `commit()` advances the version and the single
 * table's value together, and `blockNextExecute(query)` stalls that query's next execute so the
 * test controls exactly when its (snapshot-at-call-time) result lands.
 */
function createRaceHost(): {
  host: {
    currentVersion(): Promise<number | null>;
    manifestPage(
      afterVersion: number | null,
      limit: number,
    ): Promise<StoragePage<Manifest, number>>;
    dependencyTableIds(query: LiveQueryInput): Promise<Set<string>>;
    execute(query: LiveQueryInput): Promise<QueryResult>;
  };
  commit(): void;
  blockNextExecute(query: LiveQueryInput): { started: Promise<void>; release(): void };
} {
  const tableId = "table-1";
  let version = 1;
  let value = 1;
  const manifests: Manifest[] = [
    { version: 1, previousVersion: null, createdAt: "", blockIds: [], changedTableIds: [tableId] },
  ];
  const blocks = new Map<LiveQueryInput, { started: () => void; gate: Promise<void> }>();
  return {
    host: {
      currentVersion: () => Promise.resolve(version),
      manifestPage: (afterVersion, limit) =>
        Promise.resolve({
          records: manifests
            .filter((manifest) => manifest.version > (afterVersion ?? -1))
            .slice(0, limit),
          nextCursor: null,
        }),
      dependencyTableIds: () => Promise.resolve(new Set([tableId])),
      execute: async (query) => {
        const snapshot = value;
        const block = blocks.get(query);
        if (block !== undefined) {
          blocks.delete(query);
          block.started();
          await block.gate;
        }
        return { columns: ["v"], rows: [{ v: snapshot }] };
      },
    },
    commit: () => {
      version += 1;
      value += 1;
      manifests.push({
        version,
        previousVersion: version - 1,
        createdAt: "",
        blockIds: [],
        changedTableIds: [tableId],
      });
    },
    blockNextExecute: (query) => {
      let started!: () => void;
      const startedPromise = new Promise<void>((resolve) => {
        started = resolve;
      });
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      blocks.set(query, { started, gate });
      return { started: startedPromise, release };
    },
  };
}

async function seeded(store: MemoryBlockStore): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
  await database.createTable({
    name: "events",
    columns: [
      { name: "value", type: "number" },
      { name: "label", type: "string", nullable: true },
    ],
  });
  await database.createTable({
    name: "other",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insertBatch("events", {
    columns: { value: [1, 2, 3], label: ["a", "b", null] },
  });
  await database.insertBatch("other", { columns: { value: [1] } });
  return database;
}

describe("live queries", () => {
  it("re-executes on dependent commits and skips unrelated and content-preserving ones", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    const subscription = await live.subscribe(
      "SELECT COUNT(*) AS count, SUM(value) AS total FROM events WHERE value >= 2",
      { onChange: (result) => changes.push(result) },
    );
    expect(changes).toHaveLength(1);
    expect(changes[0]?.rows).toEqual([{ count: 2, total: 5 }]);

    await database.insertBatch("events", { columns: { value: [10], label: ["z"] } });
    await live.refresh();
    expect(changes).toHaveLength(2);
    expect(changes[1]?.rows).toEqual([{ count: 3, total: 15 }]);

    // An unrelated table's commit is skipped entirely.
    const skippedBefore = live.stats.rerunsAvoided;
    await database.insertBatch("other", { columns: { value: [2] } });
    await live.refresh();
    expect(changes).toHaveLength(2);
    expect(live.stats.rerunsAvoided).toBe(skippedBefore + 1);

    // A dependent commit whose new block cannot match the predicate never re-runs at all:
    // zone statistics prove value >= 2 rejects the [1] block.
    await database.insertBatch("events", { columns: { value: [1], label: [null] } });
    await live.refresh();
    expect(changes).toHaveLength(2);
    expect(live.stats.zoneSkips).toBe(1);
    // A commit that passes the zone gate but leaves the result unchanged re-runs and
    // suppresses the notification through the digest.
    await database.insertBatch("events", { columns: { value: [2], label: [null] } });
    await live.refresh();
    expect(changes).toHaveLength(3);
    expect(changes[2]?.rows).toEqual([{ count: 4, total: 17 }]);

    // Compaction publishes an empty change set, so it never re-runs subscriptions.
    const rerunsBefore = live.stats.reruns;
    await database.compactTable("events");
    await live.refresh();
    expect(changes).toHaveLength(3);
    expect(live.stats.reruns).toBe(rerunsBefore);
    expect(live.stats.rerunsAvoided).toBeGreaterThan(skippedBefore + 1);

    subscription.close();
    live.close();
    store.close();
  });

  it("converges through refresh alone when every hint is missed", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    const writer = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    await live.subscribe("SELECT COUNT(*) AS count FROM events", {
      onChange: (result) => changes.push(result),
    });

    // The writer instance shares the store but no hint channel: nothing reaches the set.
    await writer.insertBatch("events", { columns: { value: [4], label: [null] } });
    await writer.insertBatch("events", { columns: { value: [5], label: [null] } });
    expect(changes).toHaveLength(1);

    await live.refresh();
    expect(changes).toHaveLength(2);
    expect(changes[1]?.rows).toEqual([{ count: 5 }]);
    live.close();
    store.close();
  });

  it("carries cross-tab hints through a channel without polling", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    const [channelA, channelB] = createChannelPair();
    const writer = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    const writerSet = writer.liveQueries({ channel: channelA });
    const live = database.liveQueries({ channel: channelB });
    const changes: QueryResult[] = [];
    await live.subscribe("SELECT COUNT(*) AS count FROM events", {
      onChange: (result) => changes.push(result),
    });

    await writer.insertBatch("events", { columns: { value: [4], label: [null] } });
    // The writer's local commit hint fans out through the channel to the reader's set.
    await live.refresh();
    expect(changes).toHaveLength(2);
    expect(changes[1]?.rows).toEqual([{ count: 4 }]);
    expect(live.stats.hints).toBeGreaterThanOrEqual(2);
    writerSet.close();
    live.close();
    store.close();
  });

  it("widens to every subscription when a commit carries no change set", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    const stripping = new Proxy(store, {
      get(target, property, receiver) {
        if (property === "commitTransaction") {
          return (input: CommitTransactionInput) => {
            const { changedTableIds, ...rest } = input;
            void changedTableIds;
            return target.commitTransaction(rest);
          };
        }
        const value = Reflect.get(target, property, receiver) as unknown;
        if (typeof value === "function") {
          return (value as (...callArguments: unknown[]) => unknown).bind(target);
        }
        return value;
      },
    });
    const legacyWriter = new MinnowDatabase(stripping, { rowsPerBlock: 8, compression: "raw" });
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    await live.subscribe("SELECT COUNT(*) AS count FROM events", {
      onChange: (result) => changes.push(result),
    });

    // The write touches the unrelated table, but its manifest lacks a change set, so the sweep
    // conservatively re-runs everything rather than risking staleness.
    await legacyWriter.insertBatch("other", { columns: { value: [3] } });
    await live.refresh();
    expect(live.stats.reruns).toBe(1);
    expect(changes).toHaveLength(1);
    expect(live.stats.lastSweepMs).toBeGreaterThanOrEqual(0);
    live.close();
    store.close();
  });

  it("skips re-runs when zone statistics prove new inserts cannot match", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    await live.subscribe("SELECT COUNT(*) AS n FROM events WHERE value >= 100", {
      onChange: (result) => changes.push(result),
    });
    expect(changes).toHaveLength(1);

    // New rows far below the predicate range: the commit is table-affecting, but every new
    // block's zone statistics reject value >= 100, so the sweep skips the re-run entirely.
    await database.insertBatch("events", {
      columns: { value: [10, 11, 12], label: [null, null, null] },
    });
    await live.refresh();
    expect(live.stats.zoneSkips).toBe(1);
    expect(live.stats.reruns).toBe(0);
    expect(changes).toHaveLength(1);

    // A matching insert re-runs and notifies.
    await database.insertBatch("events", { columns: { value: [150], label: ["hot"] } });
    await live.refresh();
    expect(live.stats.reruns).toBe(1);
    expect(changes).toHaveLength(2);
    expect(changes[1]?.rows).toEqual([{ n: 1 }]);
    live.close();
    store.close();
  });

  it("re-runs on keyed upserts even when new values reject the predicates", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    await database.createTable({
      name: "scores",
      uniqueKey: "name",
      columns: [
        { name: "name", type: "string" },
        { name: "score", type: "number" },
      ],
    });
    await database.insertBatch("scores", { columns: { name: ["a", "b"], score: [150, 20] } });
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    await live.subscribe("SELECT COUNT(*) AS n FROM scores WHERE score >= 100", {
      onChange: (result) => changes.push(result),
    });
    expect(changes[0]?.rows).toEqual([{ n: 1 }]);
    // The upsert's new value (5) rejects score >= 100, but it REPLACES a's 150 — the result
    // loses a row, so zone gating must not skip this.
    await database.upsertBatch("scores", { columns: { name: ["a"], score: [5] } });
    await live.refresh();
    expect(live.stats.zoneSkips).toBe(0);
    expect(changes).toHaveLength(2);
    expect(changes[1]?.rows).toEqual([{ n: 0 }]);
    live.close();
    store.close();
  });

  it("re-runs when a same-table subquery can shift even though zones reject the insert", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    await database.insertBatch("events", { columns: { value: [100, 200], label: [null, null] } });
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    // Seeded values are [1, 2, 3, 100, 200]: AVG is 61.2, so exactly one row (100) satisfies
    // value <= 100 AND value > AVG.
    await live.subscribe(
      "SELECT COUNT(*) AS n FROM events WHERE value <= 100 AND value > (SELECT AVG(value) FROM events)",
      { onChange: (result) => changes.push(result) },
    );
    expect(changes[0]?.rows).toEqual([{ n: 1 }]);
    // The new row zone-rejects value <= 100, but it drags AVG(value) above 100, which drops
    // the existing match — base-scan zone proofs are unsound for subquery plans.
    await database.insertBatch("events", { columns: { value: [1000], label: [null] } });
    await live.refresh();
    expect(live.stats.zoneSkips).toBe(0);
    expect(changes).toHaveLength(2);
    expect(changes[1]?.rows).toEqual([{ n: 0 }]);
    live.close();
    store.close();
  });

  it("re-runs when the changing segment was compacted away and garbage-collected", async () => {
    const store = new MemoryBlockStore();
    const writer = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    await writer.createTable({
      name: "scores",
      uniqueKey: "name",
      columns: [
        { name: "name", type: "string" },
        { name: "score", type: "number" },
      ],
    });
    await writer.insertBatch("scores", {
      columns: { name: ["a", "b", "c"], score: [150, 160, 170] },
    });
    // A prior upsert leaves a mutation segment so compaction rewrites the table.
    await writer.upsertBatch("scores", { columns: { name: ["a"], score: [155] } });
    // A separate reader instance so only explicit refresh() drives its sweeps.
    const reader = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    const live = reader.liveQueries();
    const changes: QueryResult[] = [];
    await live.subscribe("SELECT COUNT(*) AS n FROM scores WHERE score >= 100", {
      onChange: (result) => changes.push(result),
    });
    expect(changes[0]?.rows).toEqual([{ n: 3 }]);
    // The matching insert's segment is compacted into a "base" rewrite and then reclaimed:
    // by the time the reader sweeps, no surviving segment carries that commit. Absence of a
    // segment must read as "cannot prove neutral", not as proof.
    await writer.insertBatch("scores", { columns: { name: ["d"], score: [180] } });
    await writer.compactTable("scores");
    await writer.collectGarbage();
    await live.refresh();
    expect(changes).toHaveLength(2);
    expect(changes[1]?.rows).toEqual([{ n: 4 }]);
    live.close();
    store.close();
  });

  it("skips re-runs for data-neutral compaction commits", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    await database.insertBatch("events", {
      columns: { value: [4, 5, 6, 7, 8, 9, 10, 11, 12], label: Array(9).fill(null) },
    });
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    await live.subscribe("SELECT COUNT(*) AS n FROM events WHERE value >= 100", {
      onChange: (result) => changes.push(result),
    });
    const avoidedBefore = live.stats.rerunsAvoided;
    await database.compactTable("events");
    await live.refresh();
    // Compaction publishes an empty change set (logically unchanged), so the sweep avoids
    // the re-run before the zone gate is even consulted.
    expect(live.stats.rerunsAvoided).toBe(avoidedBefore + 1);
    expect(live.stats.reruns).toBe(0);
    expect(changes).toHaveLength(1);
    live.close();
    store.close();
  });

  it("converges a subscription whose initial execute raced a concurrent sweep", async () => {
    const race = createRaceHost();
    const live = new LiveQuerySet(race.host);
    const aChanges: QueryResult[] = [];
    await live.subscribe("A", { onChange: (result) => aChanges.push(result) });

    // B reads the current version, then its initial execute stalls holding version-1 data.
    const bBlock = race.blockNextExecute("B");
    const bChanges: QueryResult[] = [];
    const bPromise = live.subscribe("B", { onChange: (result) => bChanges.push(result) });
    await bBlock.started;

    // A commit and a full sweep land while B's initial execute is still in flight.
    race.commit();
    await live.refresh();
    expect(aChanges.at(-1)?.rows).toEqual([{ v: 2 }]);

    // B finishes subscribing with the stale snapshot it observed.
    bBlock.release();
    const bSubscription = await bPromise;
    expect(bChanges).toHaveLength(1);
    expect(bChanges[0]?.rows).toEqual([{ v: 1 }]);

    // A refresh with no new commit must still bring the lagging subscription up to date.
    await live.refresh();
    expect(bChanges.at(-1)?.rows).toEqual([{ v: 2 }]);
    bSubscription.close();
    live.close();
  });

  it("never delivers onChange after onComplete when closed during an in-flight re-run", async () => {
    const race = createRaceHost();
    const live = new LiveQuerySet(race.host);
    const events: string[] = [];
    const subscription = await live.subscribe("A", {
      onChange: () => events.push("change"),
      onComplete: () => events.push("complete"),
    });
    expect(events).toEqual(["change"]);

    // A commit triggers a sweep whose re-run stalls; the subscription closes mid-flight.
    race.commit();
    const block = race.blockNextExecute("A");
    const refreshPromise = live.refresh();
    await block.started;
    subscription.close();
    block.release();
    await refreshPromise;

    expect(events).toEqual(["change", "complete"]);
    live.close();
  });

  it("tracks dependencies through CTEs and subqueries", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    const subscription = await live.subscribe(
      "WITH big AS (SELECT value FROM events WHERE value > 1) SELECT COUNT(*) AS count FROM big WHERE value IN (SELECT value FROM other)",
      { onChange: (result) => changes.push(result) },
    );
    expect(subscription.dependencyTableIds).toHaveLength(2);
    await database.insertBatch("other", { columns: { value: [2] } });
    await live.refresh();
    expect(changes).toHaveLength(2);
    live.close();
    store.close();
  });
});
