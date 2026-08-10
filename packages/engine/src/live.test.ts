import { MemoryBlockStore, type CommitTransactionInput } from "@browserdatabase/storage-idb";
import { describe, expect, it } from "vitest";
import { BrowserDatabase } from "./database.js";
import { type LiveQueryHintChannel } from "./live.js";
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

async function seeded(store: MemoryBlockStore): Promise<BrowserDatabase> {
  const database = new BrowserDatabase(store, { rowsPerBlock: 8, compression: "raw" });
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

    // A dependent commit that does not change the result re-runs but suppresses notification.
    await database.insertBatch("events", { columns: { value: [1], label: [null] } });
    await live.refresh();
    expect(changes).toHaveLength(2);
    expect(live.stats.notificationsSuppressed).toBe(1);

    // Compaction publishes an empty change set, so it never re-runs subscriptions.
    const rerunsBefore = live.stats.reruns;
    await database.compactTable("events");
    await live.refresh();
    expect(changes).toHaveLength(2);
    expect(live.stats.reruns).toBe(rerunsBefore);
    expect(live.stats.rerunsAvoided).toBeGreaterThan(skippedBefore + 1);

    subscription.close();
    live.close();
    store.close();
  });

  it("converges through refresh alone when every hint is missed", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    const writer = new BrowserDatabase(store, { rowsPerBlock: 8, compression: "raw" });
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
    const writer = new BrowserDatabase(store, { rowsPerBlock: 8, compression: "raw" });
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
    const legacyWriter = new BrowserDatabase(stripping, { rowsPerBlock: 8, compression: "raw" });
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
