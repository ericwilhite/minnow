/**
 * Ruling R25: every publication's extent delta (access/write-path.ts `PublishDelta`) is exact.
 * Replaying the deltas from an empty set gives, after each publication, exactly the extents a full
 * reachability walk of the published root finds - across flushes, folded deletions, merges,
 * index and table DDL, log recycles, idle publishes and reopens. Each delta adds only extents the
 * set lacks and releases only extents it holds.
 */
import { describe, expect, it } from "vitest";
import type { PublishDelta } from "../access/write-path.js";
import { reachableExtents } from "../format/reachable.js";
import type { ExtentSpace } from "../storage/extents.js";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import { ExtentSinkError, type WritePathImpl } from "./engine.js";
import { yieldTask } from "./merge.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  tableInput,
} from "./test-support.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { Statement } from "../access/modify.js";

const FREE = { allocate: () => true, free: () => undefined };

/** Every extent the space's current root reaches, by offset → length. */
async function walk(space: ExtentSpace): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  const root = need(space.root);
  for await (const reached of reachableExtents()(root, space, FREE)) {
    const extent = "extent" in reached ? reached.extent : reached;
    if (extent.length === 0) continue; // an empty directory names no extent
    out.set(extent.offset, extent.length);
  }
  return out;
}

const REACH = [
  "added",
  "released",
  "merge",
  "recycle",
  "idle-publish",
  "drop-table",
  "drop-index",
  "abandoned",
  "admission-refused",
  "flush-of-dropped-table",
  "flush-held-by-build",
  "build-settled",
  "reopen",
] as const;
const reach = createReach("next-write-extent-deltas", REACH);

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

describe("extent deltas (ruling R25)", () => {
  const SEEDS = seedsFor("next-write-extent-deltas", [701, 702, 703, 704]);
  for (const seed of SEEDS)
    it(`replaying the deltas gives each root's reachable extents (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      // Some merge attempts fail after their prepare, at the encode step (a hook) or at metadata
      // admission (the next allocation refused): the attempt aborts, the merge retries, the path
      // goes on (review B3-p2b-3 C2).
      let refuseNextAllocation = false;
      const harness = new Harness("strict", "sim", 6, undefined, {
        merges: { fanIn: 2 },
        logRecycleBytes: 1500 + int(2000),
        idleGarbageBytes: 1,
        // Some flushes wait a few tasks before publishing, so a drop can commit while they write.
        beforeFlushPublish: async () => {
          for (let turns = random() < 0.5 ? int(12) : 0; turns > 0; turns--) await yieldTask();
        },
        afterDeltaPrepared: (kind) => {
          if (kind !== "merge") return;
          const roll = random();
          if (roll < 0.2) throw new Error("injected failure at encode");
          if (roll < 0.4) refuseNextAllocation = true;
        },
      });
      const admit = harness.governor.admit.bind(harness.governor);
      harness.governor.admit = async (options) => {
        const grant = await admit(options);
        if (options.label === "write path metadata") {
          const allocate = grant.allocate.bind(grant);
          grant.allocate = (bytes) => {
            // The merge's own root charge: a flush can allocate between the hook and that charge.
            if (refuseNextAllocation && (new Error().stack ?? "").includes("#merge")) {
              refuseNextAllocation = false;
              reach.reach("admission-refused");
              return false;
            }
            return allocate(bytes);
          };
        }
        return grant;
      };
      reach.beginHistory();
      const set = new Map<number, number>();
      let sequence = 0;
      let lastAttempt = 0;
      const deltas: PublishDelta[] = [];
      // The committed list changes only at commit(); a prepared delta is provisional until then.
      let pending: { attempt: number; delta: PublishDelta } | null = null;
      harness.extentDeltas = {
        prepare: (attempt, delta) => {
          try {
            checkPrepare(attempt, delta);
          } catch (error) {
            sinkError ??= error;
            throw error;
          }
          pending = { attempt, delta };
          return null;
        },
        commit: (attempt, committed) => {
          const { attempt: prepared, delta } = need(pending);
          expect(attempt).toBe(prepared);
          expect(committed, "committed sequences are consecutive").toBe(sequence + 1);
          pending = null;
          sequence = committed;
          for (const extent of delta.released) set.delete(extent.offset);
          for (const extent of delta.added) set.set(extent.offset, extent.length);
          deltas.push(delta);
          if (delta.added.length > 0) reach.reach("added");
          if (delta.released.length > 0) reach.reach("released");
        },
        abort: (attempt) => {
          if (sinkError !== null) return; // the sink's own check failed: reported below
          expect(attempt).toBe(need(pending).attempt);
          pending = null;
          reach.reach("abandoned");
        },
      };
      let sinkError: unknown = null;
      const checkPrepare = (attempt: number, delta: PublishDelta) => {
        expect(pending, "one attempt at a time").toBeNull();
        expect(attempt, "attempts increase").toBeGreaterThan(lastAttempt);
        lastAttempt = attempt;
        // Built from the last committed list, whatever was aborted before.
        for (const extent of delta.released)
          expect(set.has(extent.offset), `released ${String(extent.offset)} is reached`).toBe(true);
        for (const extent of delta.added)
          expect(set.has(extent.offset), `added ${String(extent.offset)} is new`).toBe(false);
      };
      // Attempt numbers are per open write path (an open may publish before it returns).
      const open = harness.open.bind(harness);
      harness.open = () => {
        lastAttempt = 0;
        return open();
      };
      let path: WritePathImpl = await harness.open();
      const check = async (label: string) => {
        await path.quiesce();
        // Let an armed idle publish land, then compare with a full walk of the current root.
        await new Promise((resolve) => setTimeout(resolve, 150));
        await path.quiesce();
        expect(need(harness.space.root).length).toBeGreaterThan(0);
        const walked = await walk(harness.space);
        const extra = [...set].filter(([offset]) => !walked.has(offset));
        const missing = [...walked].filter(([offset]) => !set.has(offset));
        expect({ extra, missing }, `seed ${String(seed)} ${label}`).toEqual({
          extra: [],
          missing: [],
        });
        expect(set).toEqual(walked);
      };
      const tables: number[] = [];
      let named = 0;
      const rows = new Map<number, Set<number>>();
      const create = async () => {
        let t = 0;
        await autocommit(path, async (s) => {
          t =
            (
              await s.changeCatalog({
                kind: "createTable",
                table: tableInput(`t${String(++named)}`, [
                  column("id", INT, false),
                  column("v", INT),
                ]),
              })
            ).id ?? 0;
        });
        tables.push(t);
        rows.set(t, new Set());
        // Sometimes indexed at once: a build on a table with no rows (no entries to flush).
        if (random() < 0.3) {
          const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
          await autocommit(path, (s) =>
            s.changeCatalog({
              kind: "createIndex",
              index: {
                tableId: t,
                name: `t${String(t)}_v`,
                columnIds: [v],
                unique: false,
                role: "secondary",
              },
            }),
          );
        }
      };
      await create();
      for (let step = 0; step < 60; step++) {
        const t = need(tables[int(tables.length)]);
        const live = need(rows.get(t));
        const roll = int(100);
        if (roll < 50) {
          const ids = Array.from({ length: 1 + int(8) }, () => int(500)).filter(
            (id) => !live.has(id),
          );
          const unique = [...new Set(ids)];
          if (unique.length > 0) {
            await autocommit(path, (s) =>
              s.table(t).insert(batch(ints(unique), ints(unique)), null),
            );
            for (const id of unique) live.add(id);
          }
        } else if (roll < 65 && live.size > 0) {
          const id = need([...live][int(live.size)]);
          await autocommit(path, async (s) =>
            s.table(t).delete(batch(locators([await locate(s, t, id)], t)), null),
          );
          live.delete(id);
        } else if (roll < 72) {
          const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
          const existing = path.catalog
            .table(t)
            ?.indexes.find((index) => index.name.endsWith("_v"));
          await autocommit(path, (s) =>
            existing === undefined
              ? s.changeCatalog({
                  kind: "createIndex",
                  index: {
                    tableId: t,
                    name: `t${String(t)}_v`,
                    columnIds: [v],
                    unique: false,
                    role: "secondary",
                  },
                })
              : s.changeCatalog({ kind: "dropIndex", indexId: existing.id }),
          );
          if (existing !== undefined) reach.reach("drop-index");
        } else if (roll < 78) {
          await create();
        } else if (roll < 82 && tables.length > 1) {
          tables.splice(tables.indexOf(t), 1);
          rows.delete(t);
          // Sometimes a full memtable first, so the drop commits while its flush is writing.
          if (random() < 0.5) {
            const ids = Array.from({ length: 6 }, (_, k) => 1000 + k);
            await autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
          }
          await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
          reach.reach("drop-table");
        } else if (roll < 90) {
          await check(`step ${String(step)}`);
        } else {
          await path.quiesce();
          path = await harness.reopen();
          reach.reach("reopen");
          // A reopen publishes nothing; the space's root is the last delta's.
          await check(`reopen at step ${String(step)}`);
        }
        if (path.maintenance.merges > 0) reach.reach("merge");
        if (path.maintenance.recycles > 0) reach.reach("recycle");
        if (path.maintenance.idlePublishes > 0) reach.reach("idle-publish");
        if (path.maintenance.flushesAbandoned > 0) reach.reach("flush-of-dropped-table");
        if (path.maintenance.flushesHeld > 0) reach.reach("flush-held-by-build");
        if (path.maintenance.buildsSettled > 0) reach.reach("build-settled");
      }
      await check("the end");
      expect(deltas.length).toBeGreaterThan(5);
      await harness.close();
    }, 300_000);

  it("a rejected publish aborts its attempt; nothing after it commits", async () => {
    const harness = new Harness("strict", "memory", 2);
    const calls: string[] = [];
    harness.extentDeltas = {
      prepare: (attempt) => {
        calls.push(`prepare ${String(attempt)}`);
        return null;
      },
      commit: (attempt, sequence) => calls.push(`commit ${String(attempt)} as ${String(sequence)}`),
      abort: (attempt) => calls.push(`abort ${String(attempt)}`),
    };
    const path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    const failure = new Error("injected publish refusal");
    harness.space.publish = () => Promise.reject(failure);
    // Two commits: the first fills a memtable, the second freezes it, so its flush publishes.
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2])), null));
    await autocommit(path, (s) => s.table(t).insert(batch(ints([3])), null));
    await path.quiesce().catch(() => undefined);
    expect(path.failure).toBe(failure);
    // The initial root, then a prepared flush that failed: nothing after it was published.
    expect(calls).toEqual(["prepare 1", "commit 1 as 1", "prepare 2", "abort 2"]);
    await harness.close();
  });

  it("a sink that rejects a merge's prepare fails the write path (no retry around it)", async () => {
    const harness = new Harness("strict", "memory", 2, undefined, { merges: { fanIn: 2 } });
    const calls: string[] = [];
    let reject = false;
    let refused = 0;
    harness.extentDeltas = {
      prepare: (attempt) => {
        calls.push(`prepare ${String(attempt)}`);
        // A merge's prepare only: a retry around it would prepare (and be refused) again.
        if (reject && (new Error().stack ?? "").includes("#merge")) {
          refused += 1;
          throw new Error("sink refuses");
        }
        return null;
      },
      commit: (attempt) => calls.push(`commit ${String(attempt)}`),
      abort: (attempt) => calls.push(`abort ${String(attempt)}`),
    };
    const path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    // Flushes commit; then the sink refuses the merge two runs make: the path fails, no retry.
    reject = true;
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2])), null));
    await autocommit(path, (s) => s.table(t).insert(batch(ints([3, 4])), null));
    await autocommit(path, (s) => s.table(t).insert(batch(ints([5])), null)).catch(() => undefined);
    await path.quiesce().catch(() => undefined);
    expect(refused).toBe(1);
    expect(path.failure).toBeInstanceOf(ExtentSinkError);
    expect(path.maintenance.merges).toBe(0);
    const last = calls[calls.length - 1] ?? "";
    expect(last.startsWith("abort")).toBe(true);
    expect(calls.filter((call) => call.startsWith("prepare")).length).toBe(
      calls.filter((call) => !call.startsWith("prepare")).length,
    );
    await harness.close();
  });

  it("reached every declared state", () => {
    reach.assertReached();
  });
});
