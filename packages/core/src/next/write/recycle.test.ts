/**
 * Log recycling (engine.ts "Log recycling", log.ts "Recycling"): under sustained writes the log
 * stays within its bound; every table, including a cold one and one whose memtable holds only
 * tombstones, is checkpointed at the cut before the log goes; sequence frames appended while the
 * incarnation switches land in the new log; and every acknowledged commit and delivered sequence
 * value survives reopen and crash.
 */
import { describe, expect, it } from "vitest";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { Snapshot } from "../access/snapshot.js";
import type { Statement } from "../access/modify.js";
import { createReach, CATALOG_RECYCLE_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  BIG,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

async function present(view: Snapshot, table: number, ids: readonly number[]): Promise<number[]> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints([...ids]), direction: "asc", nulls: "first" }],
    { length: ids.length, selection: null },
    keys,
  );
  const out = { probes: new Int32Array(4096), locators: new Float64Array(4096), count: 0 };
  const index = view.index(table, view.catalog.table(table)?.primaryKey ?? 0);
  await index.probe(keys, out);
  keys.close();
  const found = ids.map(() => 0);
  for (let m = 0; m < out.count; m++) found[out.probes[m] ?? 0] = 1;
  return found;
}

async function locate(statement: Statement, table: number, id: number): Promise<number> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints([id]), direction: "asc", nulls: "first" }],
    { length: 1, selection: null },
    keys,
  );
  const out = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
  await statement.view
    .index(table, statement.view.catalog.table(table)?.primaryKey ?? 0)
    .probe(keys, out);
  keys.close();
  return out.locators[0] ?? 0;
}

const RECYCLE_REACH = [
  "recycled",
  "deferred-frame",
  "cold-table-flushed",
  "quiet-table-moved",
  "tombstone-only-flush",
  "reopen-after-recycle",
  "crash-after-recycle",
] as const;
const reach = createReach("next-write-log-recycle", RECYCLE_REACH);
const overlapReach = createReach("catalog-size-independent recycle overlap", CATALOG_RECYCLE_REACH);

async function create(
  path: WritePathImpl,
  name: string,
  columns: Parameters<typeof tableInput>[1],
) {
  let id = 0;
  await autocommit(path, async (s) => {
    id = (await s.changeCatalog({ kind: "createTable", table: tableInput(name, columns) })).id ?? 0;
  });
  return id;
}

describe("log recycling (generated)", () => {
  const SEEDS = seedsFor("next-write-log-recycle", [301, 302, 303, 304, 305, 306]);
  for (const seed of SEEDS)
    it(`keeps the log bounded and every acknowledged change (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      const bound = 800 + int(1600);
      let recyclingPrepared = false;
      let deferredHook: (() => Promise<void>) | null = null;
      const harness = new Harness("strict", "sim", 8 + int(16), undefined, {
        logRecycleBytes: bound,
        merges: { fanIn: 2 + int(3) },
        afterDeltaPrepared: (kind) => {
          recyclingPrepared = kind === "recycle";
        },
      });
      const open = harness.backend.open.bind(harness.backend);
      harness.backend.open = async (lease) => {
        const session = await open(lease),
          publish = session.publish.bind(session);
        session.publish = async (bytes) => {
          const atRecycle = recyclingPrepared;
          recyclingPrepared = false;
          if (atRecycle && deferredHook !== null) {
            const run = deferredHook;
            deferredHook = null;
            await run(); // append only: durability waits outside the held publication
          }
          return publish(bytes);
        };
        return session;
      };
      reach.beginHistory();
      overlapReach.beginHistory();
      let path = await harness.open();
      const hot = await create(path, "hot", [column("id", INT, false), column("v", INT)]);
      const cold = await create(path, "cold", [column("id", INT, false)]);
      const seq = await create(path, "seq", [
        { ...column("id", BIG, false), identity: { start: 1n, increment: 1n } },
      ]);
      const sequenceId = path.catalog.table(seq)?.columns[0]?.identitySequence ?? -1;
      const hotRows = new Set<number>();
      const coldRows = new Set<number>();
      const delivered = new Set<number>();
      let largest = 0;
      let recycledEver = false;
      const counters = () => path.maintenance;
      const seen = (name: (typeof RECYCLE_REACH)[number], value: number) => {
        if (value > 0) reach.reach(name);
      };
      // Catalog compression changes byte thresholds. Exercise overlap explicitly rather than
      // depending on a particular catalog spelling to make two async operations collide.
      await path.recycleLog(); // drain an automatically queued incarnation switch first
      const prefixCount = 129 + (seed % 7);
      const prefix = { repr: "safe" as const, values: new Float64Array(prefixCount) };
      let prefixLsn = -1;
      deferredHook = async () => {
        prefixLsn = await path.sequences.next(sequenceId, prefixCount, prefix, 0);
        overlapReach.reach("publish-held sequence reservation");
      };
      await path.recycleLog();
      expect(deferredHook).toBeNull();
      expect(path.maintenance.deferredFrames).toBeGreaterThan(0);
      seen("deferred-frame", path.maintenance.deferredFrames);
      overlapReach.reach("deferral observed");
      await path.durable(prefixLsn);
      for (const value of prefix.values) delivered.add(value);
      overlapReach.reach("reserved values durable");
      for (let step = 0; step < 120; step++) {
        const roll = int(100);
        if (roll < 45) {
          const ids = [...new Set(Array.from({ length: 1 + int(10) }, () => int(400)))].filter(
            (id) => !hotRows.has(id),
          );
          if (ids.length > 0) {
            await autocommit(path, (s) => s.table(hot).insert(batch(ints(ids), ints(ids)), null));
            for (const id of ids) hotRows.add(id);
          }
        } else if (roll < 62 && hotRows.size > 0) {
          // Deletes (often of flushed rows: tombstone-only memtables).
          const all = [...hotRows];
          const doomed = [
            ...new Set(Array.from({ length: 1 + int(4) }, () => all[int(all.length)] ?? 0)),
          ];
          await autocommit(path, async (s) => {
            const at: number[] = [];
            for (const id of doomed) at.push(await locate(s, hot, id));
            await s.table(hot).delete(batch(locators(at, hot)), null);
          });
          for (const id of doomed) hotRows.delete(id);
        } else if (roll < 66) {
          const id = int(1000);
          if (!coldRows.has(id)) {
            await autocommit(path, (s) => s.table(cold).insert(batch(ints([id])), null));
            coldRows.add(id);
          }
        } else if (roll < 84) {
          // Sequence values, drawn without waiting (a recycle may be switching), delivered once
          // durable: none may ever be handed out again.
          const out = { repr: "safe" as const, values: new Float64Array(4) };
          const count = 1 + int(3);
          const lsn = await path.sequences.next(sequenceId, count, out, 0);
          const values = Array.from(out.values.subarray(0, count));
          await path.durable(lsn);
          for (const value of values) {
            expect(
              delivered.has(value),
              `seed ${String(seed)}: sequence value ${String(value)} again`,
            ).toBe(false);
            delivered.add(value);
          }
        } else if (roll < 89) {
          await path.quiesce();
        } else if (roll < 94) {
          // A recycle now; sometimes then a delete alone and another recycle, which must flush a
          // memtable holding only that delete's tombstones.
          await path.recycleLog();
          if (random() < 0.6 && hotRows.size > 0) {
            const id = [...hotRows][int(hotRows.size)] ?? 0;
            await autocommit(path, async (s) =>
              s.table(hot).delete(batch(locators([await locate(s, hot, id)], hot)), null),
            );
            hotRows.delete(id);
            await path.recycleLog();
          }
        } else {
          const crash = random() < 0.5;
          recycledEver ||= counters().recycles > 0;
          path = crash ? await harness.crash() : await harness.reopen();
          if (recycledEver) reach.reach(crash ? "crash-after-recycle" : "reopen-after-recycle");
        }
        largest = Math.max(largest, path.stats().logBytes);
        recycledEver ||= counters().recycles > 0;
        const m = counters();
        seen("recycled", m.recycles);
        seen("deferred-frame", m.deferredFrames);
        seen("quiet-table-moved", m.recycleQuietTables);
        seen("tombstone-only-flush", m.rowlessRuns);
        if (m.recycleFlushedTables > 0 && coldRows.size > 0) reach.reach("cold-table-flushed");
        const view = path.pin();
        try {
          const ids = Array.from({ length: 400 }, (_, k) => k);
          expect(
            await present(view, hot, ids),
            `seed ${String(seed)} step ${String(step)}: hot`,
          ).toEqual(ids.map((id) => (hotRows.has(id) ? 1 : 0)));
          const coldIds = [...coldRows, 1000];
          expect(await present(view, cold, coldIds)).toEqual(
            coldIds.map((id) => (coldRows.has(id) ? 1 : 0)),
          );
        } finally {
          view.release();
        }
      }
      await path.quiesce();
      // The log stayed within its bound plus what one waiting recycle lets through.
      expect(largest, `seed ${String(seed)}: log bytes`).toBeLessThanOrEqual(bound + 4096);
      expect(recycledEver).toBe(true);
      await harness.space.audit();
      await harness.close();
    }, 300_000);

  it("reached every declared state", () => {
    reach.assertReached();
    overlapReach.assertReached();
  });
});

describe("log recycling", () => {
  it("hands the writer slot to a queued recycle in the step that frees it", async () => {
    const harness = new Harness("strict", "memory", 1000, undefined, { logRecycleBytes: 1 << 30 });
    const path = await harness.open();
    const t = await create(path, "t", [column("id", INT, false)]);
    const tx = await path.begin();
    const statement = tx.statement({
      rules: () => ({ check: () => null, generate: () => undefined }),
    });
    await statement.table(t).insert(batch(ints([1])), null);
    await statement.end();
    const recycling = path.recycleLog(); // waits for the slot behind tx
    // commit() frees the slot synchronously and hands it to the recycle: a writer arriving in this
    // very step waits for it (two holders would deadlock: the recycle defers the writer's frames).
    const committing = tx.commit(null);
    let recyclesWhenBegun = -1;
    const next = path.begin().then((writer) => {
      recyclesWhenBegun = path.maintenance.recycles;
      return writer;
    });
    const timeout = new Promise<"timeout">((resolve) =>
      setTimeout(() => resolve("timeout"), 10_000),
    );
    const receipt = await committing;
    expect(await Promise.race([Promise.all([recycling, next, receipt.durable]), timeout])).not.toBe(
      "timeout",
    );
    // The writer got the slot only once the recycle had finished with it.
    expect(recyclesWhenBegun).toBe(1);
    (await next).rollback();
    await harness.close();
  });

  it("gives up while an index build waits for its runs, and recycles once it is published", async () => {
    const harness = new Harness("strict", "memory", 1000, undefined, {
      logRecycleBytes: 1 << 30,
    });
    const path = await harness.open();
    const t = await create(path, "t", [column("id", INT, false), column("v", INT)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2, 3]), ints([4, 5, 6])), null));
    await path.quiesce();
    const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
    await autocommit(path, (s) =>
      s.changeCatalog({
        kind: "createIndex",
        index: { tableId: t, name: "t_v", columnIds: [v], unique: false, role: "secondary" },
      }),
    );
    await path.recycleLog();
    // The recycle flushed the table (its build entries with it), so it could switch.
    expect(path.maintenance.recycles + path.maintenance.recyclesAbandoned).toBe(1);
    const firstLsn = path.state().root.log.firstLsn;
    expect(firstLsn).toBeGreaterThan(1);
    expect(path.stats().logBytes).toBe(0);
    const reopened = await harness.reopen();
    const view = reopened.pin();
    expect(await present(view, t, [1, 2, 3, 4])).toEqual([1, 1, 1, 0]);
    view.release();
    await harness.close();
  });
});

const SEQUENCE_REACH = [
  "sequence-only-recycle",
  "empty-commit",
  "crash-after-sequence-recycle",
] as const;
const sequenceReach = createReach("next-write-log-recycle-sequences", SEQUENCE_REACH);

describe("log recycling under sequence-only traffic (review B3-p2b-1 #5)", () => {
  it("standalone sequence frames trigger recycling (the reviewer's probe)", async () => {
    const bound = 4096;
    const harness = new Harness("strict", "memory", undefined, undefined, {
      logRecycleBytes: bound,
    });
    const path = await harness.open();
    try {
      const t = await create(path, "seq", [
        { ...column("id", BIG, false), identity: { start: 1n, increment: 1n } },
      ]);
      const id = path.catalog.table(t)?.columns[0]?.identitySequence ?? -1;
      for (let k = 1; k <= 300; k++) {
        const lsn = await path.sequences.set(id, BigInt(k), true);
        await path.durable(lsn);
      }
      await path.quiesce();
      expect(path.stats().logBytes).toBeLessThanOrEqual(bound + 100);
      expect(path.maintenance.recycles).toBeGreaterThan(0);
    } finally {
      await harness.close();
    }
  });

  const SEEDS = seedsFor("next-write-log-recycle-sequences", [601, 602, 603, 604]);
  for (const seed of SEEDS)
    it(`sequence-only epochs and empty commits keep the log bounded (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      const bound = 1024 + int(3072);
      const harness = new Harness("strict", "sim", undefined, undefined, {
        logRecycleBytes: bound,
      });
      sequenceReach.beginHistory();
      let path = await harness.open();
      const t = await create(path, "seq", [
        { ...column("id", BIG, false), identity: { start: 1n, increment: 1n } },
      ]);
      const id = path.catalog.table(t)?.columns[0]?.identitySequence ?? -1;
      const delivered = new Set<number>();
      let recycledEver = false;
      for (let step = 0; step < 150; step++) {
        const roll = int(100);
        if (roll < 55) {
          const count = 1 + int(40);
          const out = { repr: "safe" as const, values: new Float64Array(count) };
          const lsn = await path.sequences.next(id, count, out, 0);
          await path.durable(lsn);
          for (const value of out.values) {
            expect(delivered.has(value), `seed ${String(seed)}: ${String(value)} again`).toBe(
              false,
            );
            delivered.add(value);
          }
        } else if (roll < 75) {
          // setval past everything delivered (values below it are never handed out again).
          const next = Math.max(0, ...delivered) + 1 + int(20);
          await path.durable(await path.sequences.set(id, BigInt(next), true));
          for (let value = 1; value <= next; value++) delivered.add(value);
        } else if (roll < 92) {
          // An empty commit, with or without an empty statement.
          const tx = await path.begin();
          if (random() < 0.5) {
            const s = tx.statement({
              rules: () => ({ check: () => null, generate: () => undefined }),
            });
            await s.end();
          }
          const receipt = await tx.commit(null);
          expect(receipt.wrote).toBe(false);
          sequenceReach.reach("empty-commit");
        } else {
          recycledEver ||= path.maintenance.recycles > 0;
          const crash = random() < 0.6;
          path = crash ? await harness.crash() : await harness.reopen();
          if (crash && recycledEver) sequenceReach.reach("crash-after-sequence-recycle");
        }
        // Wait for a recycle the traffic scheduled (quiesce starts none of its own).
        await path.quiesce();
        expect(
          path.stats().logBytes,
          `seed ${String(seed)} step ${String(step)}`,
        ).toBeLessThanOrEqual(bound + 1024);
        if (path.maintenance.recycles > 0) {
          recycledEver = true;
          sequenceReach.reach("sequence-only-recycle");
        }
      }
      expect(recycledEver).toBe(true);
      await path.quiesce();
      await harness.space.audit();
      await harness.close();
    }, 300_000);

  it("reached every declared state", () => {
    sequenceReach.assertReached();
  });
});
