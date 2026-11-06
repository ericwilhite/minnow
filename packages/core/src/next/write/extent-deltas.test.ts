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
  rules,
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
import { decodeManifestRoot } from "../format/manifest-codec.js";
import type { ExtentDeltaSink } from "../access/write-path.js";
import type { PublicationKind } from "./engine.js";
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
  "stage",
  "stage-rollback",
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
        await harness.space.audit();
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
        if (roll < 8) {
          // A large transaction: many calls stage full memtables before the commit; some roll
          // back to a savepoint, some roll back whole (their staging is retired).
          const tx = await path.begin();
          const s = tx.statement({ rules });
          const ids: number[] = [];
          for (let call = 0; call < 3 + int(5); call++) {
            const fresh = Array.from(
              { length: 4 + int(4) },
              (_, k) => 10_000 + step * 100 + call * 10 + k,
            ).filter((id) => !live.has(id));
            await s.table(t).insert(batch(ints(fresh), ints(fresh)), null);
            ids.push(...fresh);
          }
          await s.end();
          if (random() < 0.3) {
            tx.rollback();
            reach.reach("stage-rollback");
          } else {
            await (
              await tx.commit(null)
            ).durable;
            for (const id of ids) live.add(id);
          }
          if (path.maintenance.stages > 0) reach.reach("stage");
        } else if (roll < 50) {
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

/** The durable root's sequence, decoded from the space (what the store holds now). */
function durableSequence(harness: Harness): number {
  return decodeManifestRoot(need(harness.space.root), FREE).value.sequence;
}

async function oneTable(path: WritePathImpl): Promise<number> {
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
  return t;
}

describe("a sink whose commit or abort throws (review B3-p2b-4 D1)", () => {
  it("a throwing commit after a durable merge: installed, never retried, the path fails (the reviewer's probe)", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, {
      merges: { mode: "manual", fanIn: 2 },
    });
    let armed = false;
    const sequences: number[] = [];
    harness.extentDeltas = {
      prepare: () => null,
      commit: (_attempt, sequence) => {
        sequences.push(sequence);
        if (armed) {
          armed = false;
          throw new Error("commit callback failed");
        }
      },
      abort: () => undefined,
    };
    const path = await harness.open();
    const t = await oneTable(path);
    for (const ids of [[1, 2, 3, 4], [5, 6, 7, 8], [9]]) {
      await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
      await path.quiesce();
    }
    armed = true;
    await path.runMaintenance();
    expect(path.failure).toBeInstanceOf(ExtentSinkError);
    expect(path.maintenance.merges).toBe(1);
    expect(path.maintenance.mergeFailures).toBe(0);
    // The durable publication is installed: the write path's root is the store's.
    expect(path.state().root.sequence).toBe(durableSequence(harness));
    expect(sequences).toEqual(sequences.map((_, k) => need(sequences[0]) + k));
    await harness.close(); // the lock and the merge's grant were released
    harness.extentDeltas = null;
    const reopened = await harness.reopen().catch(async () => harness.open());
    expect(reopened.failure).toBeNull();
    await harness.space.audit();
    await harness.close();
  });

  const KINDS: readonly PublicationKind[] = ["initial", "flush", "merge", "recycle", "idle"];
  for (const kind of KINDS)
    for (const mode of ["commit", "abort"] as const)
      it(`a ${mode} that throws for a ${kind} publication fails the path and keeps cleanup`, async () => {
        let target = false;
        let pending: number | null = null;
        let armed: number | null = null;
        const calls: string[] = [];
        // A commit is heard only once its root is installed (D1): the path shows it already.
        let current: WritePathImpl | null = null;
        const uninstalled: number[] = [];
        const sink: ExtentDeltaSink = {
          prepare: (attempt) => {
            pending = attempt;
            calls.push(`prepare ${String(attempt)}`);
            return null;
          },
          commit: (attempt, sequence) => {
            calls.push(`commit ${String(attempt)}`);
            if (current !== null && current.state().root.sequence !== sequence)
              uninstalled.push(sequence);
            if (attempt === armed) throw new Error("commit callback failed");
          },
          abort: (attempt) => {
            calls.push(`abort ${String(attempt)}`);
            if (attempt === armed) throw new Error("abort callback failed");
          },
        };
        const harness = new Harness("strict", "memory", 4, undefined, {
          merges: { mode: "manual", fanIn: 2 },
          idleGarbageBytes: 1,
          afterDeltaPrepared: (published) => {
            if (!target || published !== kind || armed !== null) return;
            armed = pending;
            // An abort follows a failure after prepare: the encode step fails here.
            if (mode === "abort") throw new Error("injected failure at encode");
          },
        });
        harness.extentDeltas = sink;
        if (kind === "initial") {
          target = true;
          const opened = await harness.open().then(
            () => null,
            (error: unknown) => error,
          );
          expect(opened).toBeInstanceOf(ExtentSinkError);
          expect(calls[calls.length - 1]).toBe(`${mode} ${String(armed)}`);
          // The lock went with the refused open: the next open takes it.
          harness.extentDeltas = null;
          const path = await harness.open();
          expect(path.failure).toBeNull();
          await harness.close();
          return;
        }
        const path = await harness.open();
        current = path;
        const t = await oneTable(path);
        const insert = (ids: number[]) =>
          autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
        await insert([1, 2, 3, 4]);
        await insert([5]);
        await path.quiesce();
        await insert([6, 7, 8]);
        target = true;
        if (kind === "flush") {
          await insert([9]);
          await path.quiesce().catch(() => undefined);
        } else if (kind === "merge") {
          await insert([9]);
          target = false;
          await path.quiesce();
          target = true;
          await path.runMaintenance();
        } else if (kind === "recycle") await path.recycleLog().catch(() => undefined);
        else {
          await insert([9]);
          await path.quiesce();
          await new Promise((resolve) => setTimeout(resolve, 300));
          await path.quiesce().catch(() => undefined);
        }
        expect(armed, `a ${kind} publication was prepared`).not.toBeNull();
        const failure = path.failure;
        expect(failure).toBeInstanceOf(ExtentSinkError);
        const cause = failure?.cause;
        expect(cause).toBeInstanceOf(Error);
        expect((cause as Error).message).toBe(`${mode} callback failed`);
        // Settled once, as the protocol says: the armed attempt got exactly one of the two.
        const settled = calls.filter(
          (call) => call === `commit ${String(armed)}` || call === `abort ${String(armed)}`,
        );
        expect(settled).toEqual([`${mode} ${String(armed)}`]);
        expect(uninstalled, "commits heard before their root was installed").toEqual([]);
        if (mode === "commit") {
          expect(path.state().root.sequence).toBe(durableSequence(harness));
          if (kind === "merge") expect(path.maintenance.mergeFailures).toBe(0);
        }
        // Cleanup ran: the lock is free (close finishes) and nothing leaked (the audit).
        await harness.close();
        harness.extentDeltas = null;
        const reopened = await harness.open();
        expect(reopened.failure).toBeNull();
        await harness.space.audit();
        await harness.close();
      });
});

describe("the sink's retire list (B1's question)", () => {
  it("an extent the sink retires is held under R8 like a released one: not reused while a pin can reach it", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    let page: { offset: number; length: number } | null = null;
    let retireNow = false;
    const retiredBy = new Set<number>();
    const added: Array<{ offset: number; length: number }> = [];
    harness.extentDeltas = {
      // The sink writes a page into one publication and retires it in a later one (B1's list
      // pages, until the root has a field for them).
      prepare: async (_attempt, delta) => {
        for (const extent of delta.added) added.push(extent);
        if (page === null && delta.added.length > 0) {
          page = need((await delta.writer.write([new Uint8Array([1, 2, 3])])).blobs[0]).extent;
          return null;
        }
        if (retireNow && page !== null) {
          retireNow = false;
          return { list: [], pages: [], retire: [page] };
        }
        return null;
      },
      commit: () => undefined,
      abort: () => undefined,
    };
    const path = await harness.open();
    harness.space.publish = ((publish) => async (root, change) => {
      for (const extent of change.retired) retiredBy.add(extent.offset);
      return publish(root, change);
    })(harness.space.publish.bind(harness.space));
    const t = await oneTable(path);
    let next = 0;
    const flush = async () => {
      for (const ids of [[next++, next++, next++, next++], [next++]])
        await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
      await path.quiesce();
    };
    await flush();
    expect(page).not.toBeNull();
    const written = page as unknown as { offset: number; length: number };
    // A reader older than the retirement.
    const old = harness.space.pin("older reader");
    retireNow = true;
    retiredBy.clear();
    await flush();
    // Retired with the publication's other retirements, in the same list.
    expect([...retiredBy]).toContain(written.offset);
    added.length = 0;
    for (let k = 0; k < 6; k++) await flush();
    const overlaps = (extent: { offset: number; length: number }) =>
      extent.offset < written.offset + written.length &&
      written.offset < extent.offset + extent.length;
    expect(added.filter(overlaps), "reused while a pin reaches it").toEqual([]);
    old.release();
    for (let k = 0; k < 3; k++) await flush();
    await harness.space.audit();
    await harness.close();
  });

  it("with no pin, a retired page waits for the next publish like a released extent: the previous root stays intact (R8)", async () => {
    // R8 (storage/extents.ts): space retired by the publish of generation g is reused only once
    // g + 1 is durable; until then the other root slot holds g − 1, the root a reopen may choose,
    // so whatever it reaches must stay readable. Every writer of generation g allocates before
    // g + 1 is durable: none may get the page, nor a `released` extent of the same publication.
    const harness = new Harness("strict", "memory", 4, undefined, { merges: { mode: "off" } });
    let page: { offset: number; length: number } | null = null;
    let retireNow = false;
    // Set by the sink's callbacks: -1 while the retiring attempt is prepared, then its generation.
    const retire = { at: null as number | null };
    let releasedWith: Array<{ offset: number; length: number }> = [];
    const allocated: Array<{ offset: number; length: number; generation: number }> = [];
    harness.extentDeltas = {
      prepare: async (_attempt, delta) => {
        const generation = harness.space.generation;
        for (const extent of delta.added) allocated.push({ ...extent, generation });
        if (page === null && delta.added.length > 0) {
          page = need((await delta.writer.write([new Uint8Array([1, 2, 3])])).blobs[0]).extent;
          return null;
        }
        if (retireNow && page !== null && delta.released.length > 0) {
          retireNow = false;
          releasedWith = [...delta.released];
          retire.at = -1;
          return { list: [], pages: [], retire: [page] };
        }
        return null;
      },
      commit: () => {
        if (retire.at === -1) retire.at = harness.space.generation;
      },
      abort: () => undefined,
    };
    const path = await harness.open();
    const t = await oneTable(path);
    let next = 0;
    const flush = async () => {
      for (const ids of [[next++, next++, next++, next++], [next++]])
        await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
      await path.quiesce();
    };
    await flush();
    retireNow = true;
    // Deletes give the next flushes `released` extents (folded bitmaps), so the retire rides one.
    for (let k = 0; k < 8 && (retire.at === null || retire.at < 0); k++) {
      await autocommit(path, (s) => s.table(t).delete(batch(locators([k * 1], t)), null)).catch(
        () => undefined,
      );
      await flush();
    }
    expect(retire.at, "a publication retired the page with released extents").not.toBeNull();
    const g = retire.at ?? -1;
    for (let k = 0; k < 6; k++) await flush();
    const overlaps =
      (target: { offset: number; length: number }) =>
      (extent: { offset: number; length: number }) =>
        extent.offset < target.offset + target.length &&
        target.offset < extent.offset + extent.length;
    // Allocated while generation g was current (before g + 1 was durable).
    const early = allocated.filter((extent) => extent.generation === g);
    expect(early.length, "the next publication allocated extents").toBeGreaterThan(0);
    expect(early.filter(overlaps(page as unknown as { offset: number; length: number }))).toEqual(
      [],
    );
    for (const extent of releasedWith) expect(early.filter(overlaps(extent))).toEqual([]);
    await harness.space.audit();
    await harness.close();
  });
});

it("an observer cannot substitute its own extent list for the engine's keeper", async () => {
  const harness = new Harness("strict", "memory");
  const calls: string[] = [];
  harness.extentDeltas = {
    prepare: () => {
      calls.push("prepare");
      return {
        list: [
          {
            first: 0,
            count: 1,
            blob: { extent: { offset: 0, length: 1 }, offset: 0, length: 1, checksum: 0 },
          },
        ],
        pages: [],
        retire: [],
      };
    },
    commit: () => calls.push("commit"),
    abort: () => calls.push("abort"),
  };
  const outcome = await harness.open().then(
    () => ({ opened: true, error: null }),
    (error: unknown) => ({ opened: false, error }),
  );
  try {
    expect(outcome.error).toBeInstanceOf(ExtentSinkError);
    expect(calls).toEqual(["prepare", "abort"]);
    const lease = await harness.backend.acquire();
    try {
      const session = await harness.backend.open(lease);
      expect(await session.readRoot()).toBeNull();
      await session.close();
    } finally {
      await lease.release();
    }
  } finally {
    if (outcome.opened) await harness.close();
  }
});
