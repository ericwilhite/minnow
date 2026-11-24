import {
  decodeGroupIndex,
  decodeManifestRoot,
  GROUPS_PER_PAGE,
  isEmptyDirectory,
} from "../format/manifest-codec.js";
import { ftsDef } from "../access/fts-def.js";
/**
 * Generated histories of large transactions against a model (format/replay.ts "Large
 * transactions"; access/write-path.ts "Commit"; access/snapshot.ts visibility). Small memtables
 * make most transactions stage. Each transaction inserts in many calls, deletes and updates rows
 * (committed ones and its own, staged or not), takes savepoints and rolls back to them, fails
 * statements on duplicate keys, then commits, rolls back, or the process crashes. After each one
 * the visible rows and every key probe equal the model's committed rows; the space audit runs
 * often, and crashes and reopens replay the log. A non-indexed text column includes values over
 * 64 KiB; value equality and declared reach cover staged, recovered lazy and folded reads.
 * Updating only v must preserve body through the write path's published-row reader.
 *
 * Each seed also picks the groups per staged directory page: the default, or one to three, so a
 * staging of a few memtables writes the multi-page directory (its pages share one extent) that a
 * transaction of thousands of memtables does. Its next staging, its fold and a fold after replay
 * each release that directory once (ruling R25; storage/extent-list.ts refuses anything else).
 */
import { describe, expect, it } from "vitest";
import { UniqueViolationError } from "../access/modify.js";
import type { Snapshot } from "../access/snapshot.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import {
  createReach,
  FTS_ROOT_TRACKING_REACH,
  STAGED_DIRECTORY_PAGES_REACH,
} from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import { LazyGroupRoot, type Memtable } from "./memtable.js";
import { LARGE_VALUE_BYTES } from "../page/large-value.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  rules,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";

// Non-ASCII payload: 24,000 characters are 72,000 UTF-8 bytes, beyond the page inline limit.
const LARGE_BODY = "界".repeat(24_000);
const largeId = (id: number) => id % 17 === 0;
const bodyOf = (id: number) => `${String(id)}:${largeId(id) ? LARGE_BODY : "small"}`;

const FREE = { allocate: () => true, free: () => undefined };

const REACH = [
  ...FTS_ROOT_TRACKING_REACH,
  ...STAGED_DIRECTORY_PAGES_REACH,
  "staged",
  "folded",
  "truncated-tail",
  "dropped-staged-memtable",
  "rollback-drop",
  "crash-unclaimed",
  "replayed-commit",
  "residual-tombstones",
  "unique-violation",
  "savepoint-rollback",
  "large-value",
  "staged-large-value",
  "lazy-large-value",
  "folded-large-value",
] as const;
const reach = createReach("next-write-staged", REACH);

/** Locators of `ids` in `view` (each id present exactly once). */
async function locate(view: Snapshot, t: number, ids: readonly number[]): Promise<number[]> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(ids), direction: "asc", nulls: "first" }],
    { length: ids.length, selection: null },
    keys,
  );
  const out = {
    probes: new Int32Array(ids.length + 1),
    locators: new Float64Array(ids.length + 1),
    count: 0,
  };
  await view.index(t, need(view.catalog.table(t)?.primaryKey)).probe(keys, out);
  keys.close();
  expect(out.count).toBe(ids.length);
  return Array.from(out.locators.subarray(0, out.count));
}

/** id → v of every row `view` sees. */
async function contents(view: Snapshot, t: number): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  for (const row of await rowsOf(view, t)) {
    const id = Number(row[0]);
    expect(out.has(id), `row ${String(id)} is seen once`).toBe(false);
    expect(row[2], `body of row ${String(id)}`).toBe(bodyOf(id));
    if (largeId(id)) reach.reach("large-value");
    out.set(id, Number(row[1]));
  }
  return out;
}

async function check(path: WritePathImpl, t: number, model: Map<number, number>, label: string) {
  const view = path.pin();
  try {
    const seen = await contents(view, t);
    expect(
      [...seen].sort((a, b) => a[0] - b[0]),
      label,
    ).toEqual([...model].sort((a, b) => a[0] - b[0]));
    // Every key resolves through the index layers (staged runs included) to exactly its row.
    const present = [...model.keys()];
    if (present.length > 0) await locate(view, t, present);
    const large = present.filter(largeId);
    if (large.length > 0) {
      const table = view.table(t, { budget: FREE });
      for (const locator of await locate(view, t, large)) {
        const segment = need(table.segments[Math.floor(locator / 65_536)]);
        if (segment.kind === "group") reach.reach("folded-large-value");
        else {
          const memtable = (segment as unknown as { memtable: Memtable }).memtable;
          if (memtable.staged !== null) reach.reach("staged-large-value");
          if (memtable.staged?.groupRoot instanceof LazyGroupRoot) reach.reach("lazy-large-value");
        }
      }
    }
    const keys = new KeyBatch(FREE);
    const absent = Array.from({ length: 20 }, (_, k) => 5000 + k);
    encodeKeyBatch(
      "equality",
      [{ vector: ints(absent), direction: "asc", nulls: "first" }],
      { length: absent.length, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(32), locators: new Float64Array(32), count: 0 };
    await view.index(t, need(view.catalog.table(t)?.primaryKey)).probe(keys, out);
    keys.close();
    expect(out.count, label).toBe(0);
  } finally {
    view.release();
  }
}

describe("staged large transactions, generated", () => {
  const SEEDS = seedsFor("next-write-staged", [901, 902, 903, 904]);
  for (const { seed, withFts } of SEEDS.flatMap((seed) => [
    { seed, withFts: false },
    { seed, withFts: true },
  ]))
    it(`histories of large transactions equal the model (seed ${String(seed)}, FTS ${String(withFts)})`, async (context) => {
      expect(new TextEncoder().encode(LARGE_BODY).byteLength).toBeGreaterThan(LARGE_VALUE_BYTES);
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      // Flushes can be held before they publish: a crash right after a commit then finds its
      // staging unfolded, and the open replays its publishedRows.
      // The held generation's flushes never publish: a crashed process writes nothing more.
      let generation = 0;
      let held = -1;
      let retiresStaging = false;
      let ftsFlushGate: Promise<void> | null = null,
        releaseFtsFlush: (() => void) | undefined;
      context.onTestFinished(() => releaseFtsFlush?.());
      // From the seed alone, so a MINNOW_SEED replay stages the same directories.
      const pageGroups = [GROUPS_PER_PAGE, 1, 2, 3][seed % 4] ?? GROUPS_PER_PAGE;
      const harness = new Harness("strict", "sim", 6, undefined, {
        stagedDirectoryPageGroups: pageGroups,
        afterDeltaPrepared: (kind) => {
          if (kind === "fts" && retiresStaging)
            reach.reach("FTS publication retires native staging");
        },
        // Merges off: a crashed process must publish nothing more, and merges run on their own.
        merges: { mode: "off" },
        ...(withFts ? { idleGarbageBytes: Number.MAX_SAFE_INTEGER } : {}),
        beforeFlushPublish: async () => {
          if (ftsFlushGate !== null) await ftsFlushGate;
          if (held === generation) await new Promise<never>(() => undefined);
        },
      });
      if (withFts)
        harness.extentDeltas = {
          prepare: (_attempt, delta) => {
            if (harness.space.root === null) {
              retiresStaging = false;
              return null;
            }
            const root = decodeManifestRoot(harness.space.root, FREE).value;
            retiresStaging = root.tables.some((table) =>
              table.staged.some((staged) =>
                delta.released.some(
                  (extent) =>
                    extent.offset === staged.groups.extent.offset &&
                    extent.length === staged.groups.extent.length,
                ),
              ),
            );
            return null;
          },
          commit: () => {
            retiresStaging = false;
          },
          abort: () => {
            retiresStaging = false;
          },
        };
      reach.beginHistory();
      let path = await harness.open();
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [
                column("id", INT, false),
                column("v", INT),
                column("body", TEXT),
              ]),
            })
          ).id ?? 0;
      });
      if (withFts)
        await autocommit(path, async (statement) => {
          const { id: _id, ...fts } = ftsDef(1, t, need(path.catalog.table(t)?.columns[2]).id);
          void _id;
          await statement.changeCatalog({ kind: "createFts", fts });
        });
      const crash = () => {
        held = generation;
        generation += 1;
        return harness.crash();
      };
      const model = new Map<number, number>();
      let next = 1;
      // Staged directories of two or more pages the durable root names, by staging id: the
      // directory's index offset. `ending` is how the open transaction ended; a commit whose
      // staging the open replayed is `replayed` until its fold.
      let multiPage = new Map<number, number>();
      let ending: "open" | "commit" | "rollback" | "replayed" = "open";
      const observePages = async () => {
        const table = path.state().root.tables.find((x) => x.tableId === t);
        const now = new Map<number, number>();
        for (const entry of table?.staged ?? []) {
          if (isEmptyDirectory(entry.groups)) continue;
          const lease = await harness.space.read([entry.groups], { budget: FREE });
          try {
            const pages = decodeGroupIndex(need(lease.bytes[0]), FREE).value;
            const extents = new Set(pages.map((page) => page.ref.extent.offset));
            if (pages.length >= 2 && extents.size < pages.length) {
              reach.reach("multi-page staged directory");
              now.set(entry.transaction, entry.groups.extent.offset);
            }
          } finally {
            lease.release();
          }
        }
        for (const [id, offset] of multiPage) {
          const entry = table?.staged.find((staged) => staged.transaction === id);
          if (entry !== undefined && entry.groups.extent.offset !== offset)
            reach.reach("multi-page directory restaged");
          if (entry === undefined && ending === "commit")
            reach.reach("multi-page directory folded");
          if (entry === undefined && ending === "replayed")
            reach.reach("multi-page directory replayed");
        }
        multiPage = now;
      };
      const observe = () => {
        const m = path.maintenance;
        if (m.stages > 0) reach.reach("staged");
        if (m.stagedFolds > 0) reach.reach("folded");
        if (m.stagedTruncations > 0) reach.reach("truncated-tail");
        if (m.stagedMemtablesDropped > 0) reach.reach("dropped-staged-memtable");
        if (m.stagingRollbacks > 0) reach.reach("rollback-drop");
        if (m.stagingDropped > 0) reach.reach("crash-unclaimed");
        if (m.publishedReplays > 0) reach.reach("replayed-commit");
        if (m.stagedResidualEntries > 0) reach.reach("residual-tombstones");
      };
      for (let round = 0; round < 20; round++) {
        const tx = await path.begin();
        // The transaction's own view of the rows: the model plus its changes, per savepoint.
        let mine = new Map(model);
        const savepoints: Array<{
          point: ReturnType<typeof tx.savepoint>;
          rows: Map<number, number>;
        }> = [];
        const steps = 3 + int(10);
        for (let step = 0; step < steps; step++) {
          const roll = int(100);
          const s = tx.statement({ rules });
          try {
            if (roll < 45) {
              // Many calls of fresh rows: memtables fill and stage between calls.
              const calls = 1 + int(5);
              for (let call = 0; call < calls; call++) {
                const ids = Array.from({ length: 1 + int(7) }, () => next++);
                const values = ids.map(() => int(1000));
                await s
                  .table(t)
                  .insert(batch(ints(ids), ints(values), texts(ids.map(bodyOf))), null);
                ids.forEach((id, k) => mine.set(id, values[k] ?? 0));
              }
              await s.end();
            } else if (roll < 60 && mine.size > 0) {
              const ids = [
                ...new Set(
                  Array.from({ length: 1 + int(4) }, () => need([...mine.keys()][int(mine.size)])),
                ),
              ];
              await s.table(t).delete(batch(locators(await locate(s.view, t, ids), t)), null);
              await s.end();
              for (const id of ids) mine.delete(id);
            } else if (roll < 72 && mine.size > 0) {
              const ids = [
                ...new Set(
                  Array.from({ length: 1 + int(4) }, () => need([...mine.keys()][int(mine.size)])),
                ),
              ];
              const values = ids.map(() => 1000 + int(1000));
              const v = need(path.catalog.table(t)?.columns[1]).id;
              await s
                .table(t, { assigned: [v] })
                .update(batch(locators(await locate(s.view, t, ids), t), ints(values)), null);
              await s.end();
              ids.forEach((id, k) => mine.set(id, values[k] ?? 0));
            } else if (roll < 80 && mine.size > 0) {
              // A duplicate key fails the statement: all of it rolls back.
              const ids = [next++, next++];
              await s.table(t).insert(batch(ints(ids), ints([1, 2]), texts(ids.map(bodyOf))), null);
              const dup = need([...mine.keys()][int(mine.size)]);
              await s.table(t).insert(batch(ints([dup]), ints([3]), texts([bodyOf(dup)])), null);
              await expect(s.end()).rejects.toBeInstanceOf(UniqueViolationError);
              reach.reach("unique-violation");
              s.rollback();
              continue;
            } else if (roll < 90) {
              s.rollback();
              savepoints.push({ point: tx.savepoint(), rows: new Map(mine) });
              continue;
            } else {
              s.rollback();
              const saved = savepoints.pop();
              if (saved !== undefined) {
                tx.rollbackTo(saved.point);
                mine = saved.rows;
                reach.reach("savepoint-rollback");
              }
              continue;
            }
          } catch (error) {
            s.rollback();
            throw error;
          }
          observe();
          await observePages();
        }
        const end = int(100);
        if (end < 70) {
          const unfolded = int(100) < 25;
          if (unfolded) held = generation;
          await (
            await tx.commit(null)
          ).durable;
          ending = "commit";
          model.clear();
          for (const [id, v] of mine) model.set(id, v);
          if (unfolded) {
            observe();
            await observePages();
            path = await crash();
            if (multiPage.size > 0) ending = "replayed";
            observe();
            await check(path, t, model, `seed ${String(seed)} round ${String(round)} unfolded`);
            continue;
          }
        } else if (end < 88) {
          tx.rollback();
          ending = "rollback";
        } else {
          // The process dies mid-transaction: nothing of it may survive.
          observe();
          await observePages();
          ending = "rollback";
          path = await crash();
          observe();
          await check(path, t, model, `seed ${String(seed)} round ${String(round)} after a crash`);
          continue;
        }
        observe();
        await check(path, t, model, `seed ${String(seed)} round ${String(round)}`);
        const after = int(100);
        if (after < 15) {
          path = await crash();
          observe();
          await check(path, t, model, `seed ${String(seed)} round ${String(round)} crash`);
        } else if (after < 25) {
          await path.quiesce();
          observe();
          await harness.space.audit();
          path = await harness.reopen();
          observe();
          await check(path, t, model, `seed ${String(seed)} round ${String(round)} reopen`);
        } else if (after < 50) {
          await path.quiesce();
          observe();
          await harness.space.audit();
        }
      }
      await path.quiesce();
      if (withFts) {
        ftsFlushGate = new Promise<void>((resolve) => {
          releaseFtsFlush = resolve;
        });
        try {
          const discarded = await path.begin(),
            statement = discarded.statement({ rules });
          for (let call = 0; call < 4; call++) {
            const ids = Array.from({ length: 7 }, () => next++);
            await statement
              .table(t)
              .insert(batch(ints(ids), ints(ids), texts(ids.map(bodyOf))), null);
          }
          await statement.end();
          await path.admit(0);
          expect(
            path.state().root.tables.find((table) => table.tableId === t)?.staged.length,
          ).toBeGreaterThan(0);
          discarded.rollback();
          const id = next++;
          await autocommit(path, (statement) =>
            statement.table(t).insert(batch(ints([id]), ints([id]), texts([bodyOf(id)])), null),
          );
          model.set(id, id);
        } finally {
          releaseFtsFlush?.();
          ftsFlushGate = null;
        }
        await path.quiesce();
      }
      // An open drops staging no frame claimed at its first publication: make one.
      await path.recycleLog();
      observe();
      const final = path.state().root.tables.find((table) => table.tableId === t);
      expect(final?.staged ?? []).toEqual([]);
      await harness.space.audit();
      await check(path, t, model, `seed ${String(seed)} end`);
      await harness.close();
    }, 300_000);

  it("reached every declared state", () => {
    reach.assertReached();
  });
});
