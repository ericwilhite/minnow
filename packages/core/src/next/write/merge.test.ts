/**
 * Index run merges (merge.ts).
 * - The streaming merge against a model: generated input runs (puts, tombstones in the same or a
 *   newer run, tombstones whose put lies below the merge), merged with output splits; the output
 *   runs are disjoint and ascending, and hold exactly the surviving entries.
 * - The engine against a model: generated inserts, deletes, updates and rekeys on a table with a
 *   primary key, a UNIQUE column and a secondary index, with tiny memtables and fan-in, so merges
 *   run at every level, split, cancel pairs, drop tombstones at the bottom, interleave with flushes,
 *   publish under old snapshots, and survive reopen and crash. Every index answers like the model.
 * - Ruling R23: a merge yields to the event loop in slices, and stops at a slice when the write
 *   path closes.
 */
import { describe, expect, it } from "vitest";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { RunRef } from "../format/manifest.js";
import type { Snapshot } from "../access/snapshot.js";
import type { Statement } from "../access/modify.js";
import { compareBytes } from "../vector/varlen.js";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import { mergeRuns, planMerge, sortedRuns, yieldTask } from "./merge.js";
import { encodeRun, RunReader, runRef, type RunEntry } from "./runs.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

const compare = (a: Uint8Array, b: Uint8Array) => compareBytes(a, 0, a.length, b, 0, b.length);
const hex = (key: Uint8Array) =>
  Array.from(key, (byte) => byte.toString(16).padStart(2, "0")).join("");

describe("streaming merge", () => {
  const SEEDS = seedsFor("next-write-run-merge", [101, 102, 103, 104, 105, 106]);
  for (const seed of SEEDS)
    it(`merges generated runs like the model (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const harness = new Harness("strict", "memory");
      await harness.open();
      const space = harness.space;
      const inputs = 2 + Math.floor(random() * 6);
      const bottom = random() < 0.5;
      const keys = Array.from({ length: 40 + Math.floor(random() * 400) }, () => {
        const long = random() < 0.05;
        const length = long ? 300 + Math.floor(random() * 700) : 1 + Math.floor(random() * 12);
        return Uint8Array.from({ length }, () => 97 + Math.floor(random() * 3));
      });
      // Entries per input run (index 0 = newest). Each (key, row) has at most one put and one
      // tombstone, the tombstone in the same run or a newer one; some tombstones' puts lie below.
      interface Entry {
        key: Uint8Array;
        rowId: number;
        tombstone: boolean;
      }
      const runs = Array.from({ length: inputs }, () => new Map<string, Entry>());
      const expected = new Map<string, { key: Uint8Array; rowId: number; tombstone: boolean }>();
      let rowId = 1;
      for (let k = 0; k < 300 + Math.floor(random() * 3000); k++) {
        const key = need(keys[Math.floor(random() * keys.length)]);
        const id = rowId++;
        const entry = (tombstone: boolean) => ({ key, rowId: id, tombstone });
        const name = (tombstone: boolean) => `${hex(key)}:${String(id)}:${String(tombstone)}`;
        const roll = random();
        if (roll < 0.15) {
          // A tombstone whose put is older than every input.
          need(runs[Math.floor(random() * inputs)]).set(name(true), entry(true));
          if (!bottom) expected.set(name(true), entry(true));
          continue;
        }
        const put = Math.floor(random() * inputs);
        need(runs[put]).set(name(false), entry(false));
        if (roll < 0.5) need(runs[Math.floor(random() * (put + 1))]).set(name(true), entry(true));
        else expected.set(name(false), entry(false));
      }
      const writer = space.beginWrite("inputs");
      const refs: RunRef[] = [];
      for (const run of runs) {
        const sorted = [...run.values()].sort(
          (a, b) =>
            compare(a.key, b.key) || a.rowId - b.rowId || Number(a.tombstone) - Number(b.tombstone),
        );
        const grouped: Array<{ key: Uint8Array; entries: RunEntry[] }> = [];
        for (const entry of sorted) {
          const last = grouped[grouped.length - 1];
          if (last !== undefined && compare(last.key, entry.key) === 0)
            last.entries.push({ rowId: entry.rowId, tombstone: entry.tombstone });
          else
            grouped.push({
              key: entry.key,
              entries: [{ rowId: entry.rowId, tombstone: entry.tombstone }],
            });
        }
        const encoded = encodeRun(grouped);
        if (encoded === null) continue;
        refs.push(runRef(encoded, (await writer.write(encoded.parts)).blobs));
      }
      const readers: RunReader[] = [];
      for (const ref of refs) readers.push(await RunReader.open(space, ref, FREE, FREE));
      const grant = await harness.governor.admit({
        label: "merge test",
        minimumBytes: 8 * 1024 * 1024,
        spillable: false,
      });
      const splitBytes = random() < 0.5 ? 256 : 4096;
      const output = await mergeRuns({
        space,
        writer,
        grant,
        readers,
        level: 1,
        bottom,
        policy: { fanIn: 2, splitBytes, splitEntries: null, sliceEntries: 64 },
        stopped: () => false,
      });
      // Disjoint, ascending runs.
      for (let k = 1; k < output.runs.length; k++)
        expect(compare(need(output.runs[k - 1]).maxKey, need(output.runs[k]).minKey)).toBeLessThan(
          0,
        );
      expect(sortedRuns(output.runs, 0, output.runs.length)).toBe(output.runs.length > 0 ? 1 : 0);
      // Exactly the surviving entries, in order.
      const got: string[] = [];
      for (const ref of output.runs) {
        expect(ref.level).toBe(1);
        const reader = await RunReader.open(space, ref, FREE, FREE);
        await reader.cursor(space, FREE, (key, id, tombstone) => {
          got.push(`${hex(key)}:${String(id)}:${String(tombstone)}`);
          return false;
        });
        // Point lookups agree with the model for keys in this run's range.
        for (const key of keys) {
          if (compare(key, ref.minKey) < 0 || compare(key, ref.maxKey) > 0) continue;
          const found: RunEntry[] = [];
          await reader.lookup(space, key, found, FREE);
          const want = [...expected.values()]
            .filter((entry) => compare(entry.key, key) === 0)
            .sort((a, b) => a.rowId - b.rowId)
            .map((entry) => `${String(entry.rowId)}:${String(entry.tombstone)}`);
          expect(found.map((entry) => `${String(entry.rowId)}:${String(entry.tombstone)}`)).toEqual(
            want,
          );
        }
      }
      const want = [...expected.values()]
        .sort((a, b) => compare(a.key, b.key) || a.rowId - b.rowId)
        .map((entry) => `${hex(entry.key)}:${String(entry.rowId)}:${String(entry.tombstone)}`);
      expect(got).toEqual(want);
      expect(output.entriesOut).toBe(want.length);
      grant.close();
      await writer.abort();
      await harness.close();
    });

  it("a merge publishes after a flush that holds the publish lock, on top of its root", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, {
      merges: { mode: "manual", fanIn: 2 },
    });
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
    for (const ids of [[1, 2, 3, 4], [5, 6, 7, 8], [9]])
      await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
    await path.quiesce();
    const publish = harness.space.publish.bind(harness.space);
    let resume!: () => void;
    const gate = new Promise<void>((resolve) => {
      resume = resolve;
    });
    let held = 0;
    harness.space.publish = async (...args) => {
      held += 1;
      await gate;
      return publish(...args);
    };
    // A flush reaches its publish (and holds the lock); a merge is ready meanwhile.
    await autocommit(path, (s) => s.table(t).insert(batch(ints([10, 11, 12, 13, 14])), null));
    for (let turns = 0; turns < 20 && held === 0; turns++) await yieldTask();
    expect(held).toBe(1);
    const merging = path.runMaintenance();
    for (let turns = 0; turns < 50; turns++) await yieldTask();
    // The merge waits for the lock: nothing else reached the space.
    expect(held).toBe(1);
    resume();
    await merging;
    await path.quiesce();
    expect(path.maintenance.merges).toBeGreaterThan(0);
    const root = path.state().root.tables.find((table) => table.tableId === t);
    const view = path.state().tables.get(t);
    expect(root?.groupCount).toBe(view?.groups.count);
    expect(root?.keyIndex?.runs).toEqual(view?.runs.get(root?.keyIndex?.indexId ?? 0));
    await harness.space.audit();
    const reopened = await harness.reopen();
    const snapshot = reopened.pin();
    expect((await rowsOf(snapshot, t)).length).toBe(14);
    snapshot.release();
    await harness.close();
  });

  it("plans the lowest level in debt and counts sorted runs", () => {
    const key = (byte: number) => Uint8Array.of(byte);
    const run = (level: number, min: number, max: number): RunRef => ({
      level,
      entryCount: 1,
      entries: { extent: { offset: 0, length: 0 }, offset: 0, length: 0, checksum: 0 },
      fences: { extent: { offset: 0, length: 0 }, offset: 0, length: 0, checksum: 0 },
      bloom: { extent: { offset: 0, length: 0 }, offset: 0, length: 0, checksum: 0 },
      minKey: key(min),
      maxKey: key(max),
    });
    // Level 1: two split pieces of one merge (ascending, disjoint), then an older merge's run.
    const runs = [run(0, 5, 9), run(1, 1, 3), run(1, 4, 8), run(1, 2, 6), run(2, 0, 9)];
    expect(sortedRuns(runs, 1, 4)).toBe(2);
    expect(planMerge(runs, 2)).toEqual({ from: 1, to: 4, level: 1, bottom: false });
    expect(planMerge(runs, 3)).toBeNull();
    expect(planMerge([run(0, 1, 1), run(0, 1, 1)], 2)).toEqual({
      from: 0,
      to: 2,
      level: 0,
      bottom: true,
    });
  });
});

// ── The engine against a model ──────────────────────────────────────────────────────────────

const MERGE_REACH = [
  "merge-level-0",
  "merge-level-1+",
  "merge-bottom",
  "merge-cancelled",
  "merge-split",
  "flush-during-merge",
  "merge-during-flush",
  "snapshot-across-merge",
  "reopen-after-merge",
] as const;
const reach = createReach("next-write-merges", MERGE_REACH);

interface Row {
  u: number | null;
  v: number;
}

async function counts(
  view: Snapshot,
  table: number,
  index: number,
  values: readonly number[],
): Promise<number[]> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints([...values]), direction: "asc", nulls: "first" }],
    { length: values.length, selection: null },
    keys,
  );
  const out = { probes: new Int32Array(8192), locators: new Float64Array(8192), count: 0 };
  const result = await view.index(table, index).probe(keys, out);
  keys.close();
  expect(result.done).toBe(true);
  const found = values.map(() => 0);
  for (let m = 0; m < out.count; m++)
    found[out.probes[m] ?? 0] = (found[out.probes[m] ?? 0] ?? 0) + 1;
  return found;
}

async function locate(
  statement: Statement,
  table: number,
  index: number,
  id: number,
): Promise<number> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints([id]), direction: "asc", nulls: "first" }],
    { length: 1, selection: null },
    keys,
  );
  const out = { probes: new Int32Array(4), locators: new Float64Array(4), count: 0 };
  await statement.view.index(table, index).probe(keys, out);
  keys.close();
  expect(out.count).toBe(1);
  return out.locators[0] ?? 0;
}

const IDS = 160;
const US = 400;
const VS = 8;

/** Checks every index of `view` against `model`. */
async function check(
  view: Snapshot,
  ids: { t: number; pk: number; u: number; v: number },
  model: ReadonlyMap<number, Row>,
  label: string,
): Promise<void> {
  const all = Array.from({ length: IDS }, (_, k) => k);
  expect(await counts(view, ids.t, ids.pk, all), `${label}: primary key`).toEqual(
    all.map((id) => (model.has(id) ? 1 : 0)),
  );
  const us = Array.from({ length: US }, (_, k) => k);
  const used = new Map<number, number>();
  for (const row of model.values()) if (row.u !== null) used.set(row.u, (used.get(row.u) ?? 0) + 1);
  expect(await counts(view, ids.t, ids.u, us), `${label}: unique u`).toEqual(
    us.map((u) => used.get(u) ?? 0),
  );
  const vs = Array.from({ length: VS }, (_, k) => k);
  const byV = vs.map((v) => [...model.values()].filter((row) => row.v === v).length);
  expect(await counts(view, ids.t, ids.v, vs), `${label}: secondary v`).toEqual(byV);
}

describe("merges in the engine (generated)", () => {
  const SEEDS = seedsFor("next-write-merges", [201, 202, 203, 204, 205, 206]);
  for (const seed of SEEDS)
    it(`every index answers like the model across merges (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      const harness = new Harness("strict", "sim", 6, undefined, {
        merges: { fanIn: 2 + int(2), splitEntries: 4 + int(24), sliceEntries: 3 + int(6) },
        // Some flushes wait a few tasks before publishing, so merges publish while they write.
        beforeFlushPublish: async () => {
          for (let turns = random() < 0.5 ? int(12) : 0; turns > 0; turns--) await yieldTask();
        },
      });
      reach.beginHistory();
      let path: WritePathImpl = await harness.open();
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput(
                "t",
                [column("id", INT, false), column("u", INT), column("v", INT)],
                {
                  unique: [{ name: "t_u", columns: [1] }],
                },
              ),
            })
          ).id ?? 0;
      });
      const columnId = (k: number) => path.catalog.table(t)?.columns[k]?.id ?? 0;
      await autocommit(path, (s) =>
        s.changeCatalog({
          kind: "createIndex",
          index: {
            tableId: t,
            name: "t_v",
            columnIds: [columnId(2)],
            unique: false,
            role: "secondary",
          },
        }),
      );
      const indexes = () => {
        const def = need(path.catalog.table(t));
        return {
          t,
          pk: def.primaryKey ?? 0,
          u: need(def.indexes.find((index) => index.unique && index.id !== def.primaryKey)).id,
          v: need(def.indexes.find((index) => index.name === "t_v")).id,
        };
      };
      const model = new Map<number, Row>();
      const freeId = () => {
        for (let tries = 0; tries < 50; tries++) {
          const id = int(IDS);
          if (!model.has(id)) return id;
        }
        return null;
      };
      const usedU = () => new Set([...model.values()].map((row) => row.u));
      const freeU = () => {
        if (random() < 0.2) return null;
        const used = usedU();
        for (let tries = 0; tries < 50; tries++) {
          const u = int(US);
          if (!used.has(u)) return u;
        }
        return null;
      };
      const present = () => [...model.keys()];
      let old: { view: Snapshot; model: Map<number, Row>; merges: number; step: number } | null =
        null;
      const counters = () => path.maintenance;
      for (let step = 0; step < 110; step++) {
        const label = `seed ${String(seed)} step ${String(step)}`;
        const roll = int(100);
        const ids = indexes();
        if (roll < 40) {
          const rows: Array<[number, number | null, number]> = [];
          const takenU = usedU();
          for (let k = 1 + int(8); k > 0; k--) {
            const id = freeId();
            if (id === null || rows.some((row) => row[0] === id)) continue;
            let u = freeU();
            if (u !== null && (takenU.has(u) || rows.some((row) => row[1] === u))) u = null;
            rows.push([id, u, int(VS)]);
          }
          if (rows.length === 0) continue;
          await autocommit(path, (s) =>
            s
              .table(t)
              .insert(
                batch(
                  ints(rows.map((r) => r[0])),
                  ints(rows.map((r) => r[1])),
                  ints(rows.map((r) => r[2])),
                ),
                null,
              ),
          );
          for (const [id, u, v] of rows) model.set(id, { u, v });
        } else if (roll < 60 && model.size > 0) {
          const doomed = [
            ...new Set(Array.from({ length: 1 + int(3) }, () => need(present()[int(model.size)]))),
          ];
          await autocommit(path, async (s) => {
            const at: number[] = [];
            for (const id of doomed) at.push(await locate(s, t, ids.pk, id));
            await s.table(t).delete(batch(locators(at, t)), null);
          });
          for (const id of doomed) model.delete(id);
        } else if (roll < 72 && model.size > 0) {
          const id = need(present()[int(model.size)]);
          const v = int(VS);
          await autocommit(path, async (s) =>
            s
              .table(t, { assigned: [columnId(2)] })
              .update(batch(locators([await locate(s, t, ids.pk, id)], t), ints([v])), null),
          );
          need(model.get(id)).v = v;
        } else if (roll < 80 && model.size > 0) {
          const id = need(present()[int(model.size)]);
          const u = freeU();
          await autocommit(path, async (s) =>
            s
              .table(t, { assigned: [columnId(1)] })
              .update(batch(locators([await locate(s, t, ids.pk, id)], t), ints([u])), null),
          );
          need(model.get(id)).u = u;
        } else if (roll < 88 && model.size > 0) {
          const id = need(present()[int(model.size)]);
          const next = freeId();
          if (next === null) continue;
          await autocommit(path, async (s) =>
            s
              .table(t, { assigned: [columnId(0)] })
              .update(batch(locators([await locate(s, t, ids.pk, id)], t), ints([next])), null),
          );
          model.set(next, need(model.get(id)));
          model.delete(id);
        } else if (roll < 93) {
          // Pin a snapshot now (checked against its own model every step); release it once it
          // has lived ten steps.
          if (old === null)
            old = {
              view: path.pin(),
              model: new Map([...model].map(([k, r]) => [k, { ...r }])),
              merges: counters().merges,
              step,
            };
          else if (step - old.step >= 10) {
            if (counters().merges > old.merges) reach.reach("snapshot-across-merge");
            old.view.release();
            old = null;
          }
        } else if (roll < 97) {
          await path.quiesce();
        } else {
          old?.view.release();
          old = null;
          const mergedBeforeReopen = counters().merges > 0;
          path = random() < 0.5 ? await harness.reopen() : await harness.crash();
          await path.quiesce();
          if (mergedBeforeReopen) reach.reach("reopen-after-merge");
        }
        const view = path.pin();
        try {
          await check(view, ids, model, label);
        } finally {
          view.release();
        }
        if (old !== null)
          await check(old.view, ids, old.model, `${label} (snapshot of step ${String(old.step)})`);
        const m = counters();
        if ((m.mergesByLevel[0] ?? 0) > 0) reach.reach("merge-level-0");
        if (m.mergesByLevel.slice(1).some((count) => count > 0)) reach.reach("merge-level-1+");
        if (m.bottomMerges > 0) reach.reach("merge-bottom");
        if (m.mergeCancelled > 0) reach.reach("merge-cancelled");
        if (m.splitMerges > 0) reach.reach("merge-split");
        if (m.flushesDuringMerge > 0) reach.reach("flush-during-merge");
        if (m.mergesDuringFlush > 0) reach.reach("merge-during-flush");
        // Every root the space keeps reaches only allocated extents (retirement is exact). The
        // audit needs a quiet space: nothing publishing.
        if (step % 10 === 9) {
          await path.quiesce();
          await harness.space.audit();
        }
      }
      old?.view.release();
      await path.quiesce();
      // The rows themselves, and the run lists stay within their bound.
      const view = path.pin();
      const rows = await rowsOf(view, t);
      view.release();
      expect(
        new Map(
          rows.map((row) => [Number(row[0]), { u: row[1] as number | null, v: Number(row[2]) }]),
        ),
      ).toEqual(model);
      for (const runs of path.state().tables.get(t)?.runs.values() ?? [])
        expect(planMerge(runs, harness.tuning.merges?.fanIn ?? 8)).toBeNull();
      await harness.space.audit();
      await harness.close();
    }, 300_000);

  it("reached every declared state", () => {
    reach.assertReached();
  });
});

// ── Ruling R23 ──────────────────────────────────────────────────────────────────────────────

describe("merge slices (ruling R23)", () => {
  async function loaded(rows: number) {
    const harness = new Harness("relaxed", "memory", rows / 8, undefined, {
      merges: { mode: "manual" },
    });
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
    for (let from = 0; from < rows; from += 2048) {
      const ids = Array.from({ length: Math.min(2048, rows - from) }, (_, k) => from + k);
      await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
    }
    await path.quiesce();
    return { harness, path, t };
  }

  it("a timer scheduled before a large merge runs before the merge finishes", async () => {
    const { harness, path, t } = await loaded(160_000);
    const order: string[] = [];
    setTimeout(() => order.push("timer"), 0);
    await path.runMaintenance().then(() => order.push("merge"));
    expect(order).toEqual(["timer", "merge"]);
    expect(path.maintenance.merges).toBeGreaterThan(0);
    expect(path.maintenance.mergeSlices).toBeGreaterThan(0);
    const view = path.pin();
    expect(
      await counts(view, t, need(path.catalog.table(t)?.primaryKey), [0, 79_999, 159_999, 160_000]),
    ).toEqual([1, 1, 1, 0]);
    view.release();
    await harness.close();
  }, 300_000);

  it("closing stops a merge at its next slice; the store reopens intact", async () => {
    const { harness, path, t } = await loaded(160_000);
    const merging = path.runMaintenance();
    await new Promise((resolve) => setImmediate(resolve));
    await path.close();
    await merging;
    expect(path.maintenance.merges).toBe(0);
    await harness.reopen();
    await harness.space.audit();
    const view = harness.path.pin();
    expect(
      await counts(view, t, need(harness.path.catalog.table(t)?.primaryKey), [0, 159_999]),
    ).toEqual([1, 1]);
    view.release();
    await harness.close();
  }, 300_000);
});
