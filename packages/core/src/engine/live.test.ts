import {
  MemoryBlockStore,
  type CommitTransactionInput,
  type Manifest,
  type StoragePage,
  type WriteTransactionInput,
} from "../storage/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import {
  LiveQueryLimitError,
  LiveQuerySet,
  MAX_LIVE_QUERY_GROUPS,
  MAX_LIVE_QUERY_SETS_PER_DATABASE,
  type LiveQueryHintChannel,
  type LiveQueryInput,
} from "./live.js";
import { bindPlanParameters, compileQuery, type QueryResult } from "./query.js";

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
    currentProbe(): Promise<{
      manifestVersion: number | null;
      catalogEpoch: number;
      schemaEpoch: number;
    }>;
    manifestPage(
      afterVersion: number | null,
      limit: number,
    ): Promise<StoragePage<Manifest, number>>;
    dependencyTableIds(query: LiveQueryInput): Promise<Set<string>>;
    execute(query: LiveQueryInput): Promise<QueryResult>;
  };
  commit(): void;
  executions(query: LiveQueryInput): number;
  blockNextExecute(query: LiveQueryInput): { started: Promise<void>; release(): void };
} {
  const tableId = "table-1";
  let version = 1;
  let value = 1;
  const manifests: Manifest[] = [
    {
      version: 1,
      previousVersion: null,
      createdAt: "",
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: [tableId],
    },
  ];
  const blocks = new Map<LiveQueryInput, { started: () => void; gate: Promise<void> }>();
  const executions = new Map<LiveQueryInput, number>();
  return {
    host: {
      currentProbe: () =>
        Promise.resolve({ manifestVersion: version, catalogEpoch: version, schemaEpoch: 0 }),
      manifestPage: (afterVersion, limit) =>
        Promise.resolve({
          records: manifests
            .filter((manifest) => manifest.version > (afterVersion ?? -1))
            .slice(0, limit),
          nextCursor: null,
        }),
      dependencyTableIds: () => Promise.resolve(new Set([tableId])),
      execute: async (query) => {
        executions.set(query, (executions.get(query) ?? 0) + 1);
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
        liveBlockCount: 0,
        liveBlockBytes: 0,
        changedTableIds: [tableId],
      });
    },
    executions: (query) => executions.get(query) ?? 0,
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
  it("rejects a polling interval that would spin or be truncated", () => {
    const race = createRaceHost();
    expect(() => new LiveQuerySet(race.host, { pollIntervalMs: 0 })).toThrow(/positive whole/);
    expect(() => new LiveQuerySet(race.host, { pollIntervalMs: 1.5 })).toThrow(/positive whole/);
    expect(() => new LiveQuerySet(race.host, { maxGroups: MAX_LIVE_QUERY_GROUPS + 1 })).toThrow(
      /group limit/,
    );
  });

  it("bounds opening groups and subscriptions and reuses capacity after close", async () => {
    const race = createRaceHost();
    const live = new LiveQuerySet(race.host, { maxGroups: 1, maxSubscriptions: 1 });
    const first = await live.subscribe("A", { onChange: () => undefined });
    await expect(live.subscribe("A", { onChange: () => undefined })).rejects.toBeInstanceOf(
      LiveQueryLimitError,
    );
    first.close();
    const block = race.blockNextExecute("B");
    const opening = live.subscribe("B", { onChange: () => undefined });
    await block.started;
    await expect(live.subscribe("C", { onChange: () => undefined })).rejects.toBeInstanceOf(
      LiveQueryLimitError,
    );
    block.release();
    const second = await opening;
    second.close();
    await expect(live.subscribe("C", { onChange: () => undefined })).resolves.toBeDefined();
    live.close();
  });

  it("bounds database-wide live sets, reuses closed capacity, and releases all on close", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    let closed = 0;
    const sets = Array.from({ length: MAX_LIVE_QUERY_SETS_PER_DATABASE }, () =>
      database.liveQueries({
        onClosed: () => {
          closed += 1;
        },
      }),
    );
    expect(() => database.liveQueries()).toThrow(LiveQueryLimitError);

    sets[0]?.close();
    expect(closed).toBe(1);
    const replacement = database.liveQueries({
      onClosed: () => {
        closed += 1;
      },
    });
    expect(replacement).toBeInstanceOf(LiveQuerySet);

    await database.close();
    expect(closed).toBe(MAX_LIVE_QUERY_SETS_PER_DATABASE + 1);
    expect(() => database.liveQueries()).toThrow("Database is closed");
  });

  it("does not register a subscription that finishes opening after the set closes", async () => {
    const race = createRaceHost();
    const live = new LiveQuerySet(race.host);
    const block = race.blockNextExecute("A");
    const changes: QueryResult[] = [];
    const subscribing = live.subscribe("A", { onChange: (result) => changes.push(result) });
    await block.started;
    live.close();
    block.release();
    await expect(subscribing).rejects.toThrow("Live query set is closed");
    expect(changes).toEqual([]);
  });

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

  it("derives an exact change set when a low-level caller omits the commit hint", async () => {
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
        if (property === "writeTransaction") {
          return (input: WriteTransactionInput) => {
            const { changedTableIds, ...rest } = input;
            void changedTableIds;
            return target.writeTransaction(rest);
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

    // changedTableIds is only a caller hint. The store derives the durable set from staged
    // segments, so omitting it cannot hide a changed table or force a database-wide re-run.
    await legacyWriter.insertBatch("other", { columns: { value: [3] } });
    const other = await store.getTableByName("other");
    const current = await store.getCurrentManifest();
    expect(current?.changedTableIds).toEqual([other?.id]);
    await live.refresh();
    expect(live.stats.reruns).toBe(0);
    expect(live.stats.rerunsAvoided).toBe(1);
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

  it("deduplicates equal subscriptions during initial delivery and every sweep", async () => {
    const race = createRaceHost();
    const live = new LiveQuerySet(race.host);
    const block = race.blockNextExecute("shared");
    const left: QueryResult[] = [];
    const right: QueryResult[] = [];
    const leftOpening = live.subscribe("shared", { onChange: (result) => left.push(result) });
    await block.started;
    const rightOpening = live.subscribe("shared", { onChange: (result) => right.push(result) });
    block.release();
    const [leftSubscription, rightSubscription] = await Promise.all([leftOpening, rightOpening]);

    expect(race.executions("shared")).toBe(1);
    expect(left).toEqual(right);
    race.commit();
    await live.refresh();
    expect(race.executions("shared")).toBe(2);
    expect(left).toHaveLength(2);
    expect(right).toHaveLength(2);
    expect(live.stats.sharedExecutions).toBeGreaterThan(0);

    leftSubscription.close();
    rightSubscription.close();
    live.close();
  });

  it("never deduplicates typed plans whose values only collide under JSON serialization", async () => {
    const date = new Date("2024-01-02T03:04:05.000Z");
    const text = date.toISOString();
    const dateQuery: LiveQueryInput = {
      kind: "typed-query",
      plan: bindPlanParameters(compileQuery("SELECT ? AS value"), [date]),
    };
    const textQuery: LiveQueryInput = {
      kind: "typed-query",
      plan: bindPlanParameters(compileQuery("SELECT ? AS value"), [text]),
    };
    // JSON.stringify turns Date into this exact string, which used to merge these two groups.
    expect(JSON.stringify(dateQuery.plan)).toBe(JSON.stringify(textQuery.plan));
    let executions = 0;
    const live = new LiveQuerySet({
      currentProbe: () =>
        Promise.resolve({ manifestVersion: null, catalogEpoch: 0, schemaEpoch: 0 }),
      manifestPage: () => Promise.resolve({ records: [], nextCursor: null }),
      dependencyTableIds: () => Promise.resolve(new Set()),
      execute: (query) => {
        executions += 1;
        return Promise.resolve({
          columns: ["kind"],
          rows: [{ kind: query === dateQuery ? "date" : "text" }],
        });
      },
    });
    const dateResults: QueryResult[] = [];
    const textResults: QueryResult[] = [];
    await Promise.all([
      live.subscribe(dateQuery, { onChange: (result) => dateResults.push(result) }),
      live.subscribe(textQuery, { onChange: (result) => textResults.push(result) }),
    ]);
    expect(executions).toBe(2);
    expect(dateResults[0]?.rows).toEqual([{ kind: "date" }]);
    expect(textResults[0]?.rows).toEqual([{ kind: "text" }]);
    live.close();
  });

  it("uses exact equality after a 32-bit digest collision", async () => {
    // These two values collide under this module's complete FNV digest (column and row markers
    // included). The digest may reject inequality quickly, but can never prove equality.
    const values = ["srborjp0s6132n", "i7khstlux91r3m"];
    let version = 1;
    const manifests: Manifest[] = [
      {
        version,
        previousVersion: null,
        createdAt: "",
        liveBlockCount: 0,
        liveBlockBytes: 0,
        changedTableIds: ["table"],
      },
    ];
    const live = new LiveQuerySet({
      currentProbe: () =>
        Promise.resolve({ manifestVersion: version, catalogEpoch: version, schemaEpoch: 0 }),
      manifestPage: (after, limit) =>
        Promise.resolve({
          records: manifests.filter((manifest) => manifest.version > (after ?? -1)).slice(0, limit),
          nextCursor: null,
        }),
      dependencyTableIds: () => Promise.resolve(new Set(["table"])),
      execute: () =>
        Promise.resolve({ columns: ["v"], rows: [{ v: values[version - 1] ?? null }] }),
    });
    const changes: QueryResult[] = [];
    await live.subscribe("collision", { onChange: (result) => changes.push(result) });
    version = 2;
    manifests.push({
      version,
      previousVersion: 1,
      createdAt: "",
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: ["table"],
    });
    await live.refresh();
    expect(changes.map((result) => result.rows[0]?.v)).toEqual(values);
    live.close();
  });

  it("keeps a failed group dirty so refresh retries without another commit", async () => {
    const race = createRaceHost();
    let fail = false;
    const host = {
      ...race.host,
      execute: async (query: LiveQueryInput): Promise<QueryResult> => {
        if (fail) {
          fail = false;
          throw new Error("transient read");
        }
        return race.host.execute(query);
      },
    };
    const live = new LiveQuerySet(host);
    const changes: QueryResult[] = [];
    const errors: unknown[] = [];
    await live.subscribe("retry", {
      onChange: (result) => changes.push(result),
      onError: (error) => errors.push(error),
    });
    race.commit();
    fail = true;
    await live.refresh();
    expect(errors).toHaveLength(1);
    expect(changes).toHaveLength(1);
    await live.refresh();
    expect(changes.at(-1)?.rows).toEqual([{ v: 2 }]);
    live.close();
  });

  it("observes selectively without executing inside the live set", async () => {
    const race = createRaceHost();
    const live = new LiveQuerySet(race.host);
    const invalidations: Array<{ manifestVersion: number | null; initial: boolean }> = [];
    await live.observe("observed", {
      onInvalidate: ({ manifestVersion, initial }) =>
        invalidations.push({ manifestVersion, initial }),
    });
    expect(race.executions("observed")).toBe(0);
    expect(invalidations).toEqual([{ manifestVersion: 1, initial: true }]);
    race.commit();
    await live.refresh();
    expect(race.executions("observed")).toBe(0);
    expect(invalidations).toEqual([
      { manifestVersion: 1, initial: true },
      { manifestVersion: 2, initial: false },
    ]);
    live.close();
  });

  it("re-resolves view dependencies after a catalog-only replacement", async () => {
    const store = new MemoryBlockStore();
    const database = await seeded(store);
    await database.execute("CREATE VIEW watched AS SELECT value FROM events");
    const live = database.liveQueries();
    const changes: QueryResult[] = [];
    await live.subscribe("SELECT SUM(value) AS total FROM watched", {
      onChange: (result) => changes.push(result),
    });
    expect(changes[0]?.rows).toEqual([{ total: 6 }]);

    await database.execute("CREATE OR REPLACE VIEW watched AS SELECT value FROM other");
    await live.refresh();
    expect(changes.at(-1)?.rows).toEqual([{ total: 1 }]);
    await database.insertBatch("other", { columns: { value: [4] } });
    await live.refresh();
    expect(changes.at(-1)?.rows).toEqual([{ total: 5 }]);
    live.close();
    store.close();
  });

  it("refuses to index dependencies from a continuously changing catalog", async () => {
    let epoch = 0;
    let dependencyReads = 0;
    let executions = 0;
    const live = new LiveQuerySet({
      currentProbe: () => {
        epoch += 1;
        return Promise.resolve({ manifestVersion: null, catalogEpoch: epoch, schemaEpoch: epoch });
      },
      manifestPage: () => Promise.resolve({ records: [], nextCursor: null }),
      dependencyTableIds: () => {
        dependencyReads += 1;
        return Promise.resolve(new Set([`table-${String(dependencyReads)}`]));
      },
      execute: () => {
        executions += 1;
        return Promise.resolve({ columns: ["id"], rows: [{ id: 1 }] });
      },
    });
    await expect(
      live.subscribe("SELECT id FROM changing", { onChange: () => undefined }),
    ).rejects.toThrow("Catalog kept changing");
    expect(dependencyReads).toBe(8);
    expect(executions).toBe(0);
    live.close();
  });

  it("does not re-resolve dependencies or execute for physical catalog churn", async () => {
    let catalogEpoch = 0;
    let dependencyReads = 0;
    let executions = 0;
    const changes: QueryResult[] = [];
    const live = new LiveQuerySet({
      currentProbe: () => Promise.resolve({ manifestVersion: null, catalogEpoch, schemaEpoch: 0 }),
      manifestPage: () => Promise.resolve({ records: [], nextCursor: null }),
      dependencyTableIds: () => {
        dependencyReads += 1;
        return Promise.resolve(new Set(["table-1"]));
      },
      execute: () => {
        executions += 1;
        return Promise.resolve({ columns: ["id"], rows: [{ id: 1 }] });
      },
    });
    await live.subscribe("SELECT id FROM stable", {
      onChange: (result) => changes.push(result),
    });
    expect({ dependencyReads, executions }).toEqual({ dependencyReads: 1, executions: 1 });
    const sweeps = live.stats.sweeps;

    // Index build-state and other physical catalog records move catalogEpoch, but not the
    // structural epoch. They cannot change dependency identity or a query result.
    catalogEpoch += 1;
    await live.refresh();
    expect({ dependencyReads, executions }).toEqual({ dependencyReads: 1, executions: 1 });
    expect(live.stats.sweeps).toBe(sweeps);
    expect(changes).toHaveLength(1);
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
