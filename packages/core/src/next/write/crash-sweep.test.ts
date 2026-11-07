/**
 * The exhaustive crash sweep (review B3-p2a-1): one scenario crossing commits, memtable flushes,
 * publishes (with retired extents), a CREATE INDEX build and a reopen, crashed at every physical
 * step of the simulated disk in turn (honest flush). After each crash the store reopens and must
 * hold every acknowledged change and nothing that was never attempted; the indexes must agree with
 * the rows; the extent space's audit must pass; and writing must continue across another reopen.
 *
 * `MINNOW_CRASH_STEP=n` starts the sweep at step n (to replay a failure first).
 */
import { describe, expect, it } from "vitest";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { SimDisk } from "../storage/sim-disk.js";
import type { WritePathImpl } from "./engine.js";
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
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };
const MEMTABLE_ROWS = 32;

type Expect = Map<number, number | "deleted">;

interface Outcome {
  readonly acked: Expect;
  /** Ids an interrupted operation touched: either state is allowed. */
  readonly uncertain: Map<number, Array<number | "deleted" | "absent">>;
  /** Ids one transaction inserted: after any crash, all of them or none (atomic commit). */
  readonly atomic?: number[][];
}

async function probe(
  path: WritePathImpl,
  t: number,
  indexId: number,
  keys: number[],
): Promise<number[]> {
  const view = path.pin();
  try {
    const encoded = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints(keys), direction: "asc", nulls: "first" }],
      { length: keys.length, selection: null },
      encoded,
    );
    const out = { probes: new Int32Array(4096), locators: new Float64Array(4096), count: 0 };
    await view.index(t, indexId).probe(encoded, out);
    encoded.close();
    return Array.from(out.locators.subarray(0, out.count));
  } finally {
    view.release();
  }
}

/** Runs the scenario until it finishes or the disk crashes; records what was acknowledged. */
async function scenario(harness: Harness, outcome: Outcome): Promise<void> {
  let path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", INT)]),
        })
      ).id ?? 0;
  });
  const step = async (
    ids: number[],
    states: Array<number | "deleted">,
    work: (path: WritePathImpl) => Promise<unknown>,
  ) => {
    ids.forEach((id, k) => {
      const before = outcome.acked.get(id);
      outcome.uncertain.set(id, [before ?? "absent", states[k] ?? "absent"]);
    });
    await work(path);
    ids.forEach((id, k) => {
      outcome.uncertain.delete(id);
      outcome.acked.set(id, states[k] ?? "deleted");
    });
  };
  const insert = (ids: number[]) =>
    step(
      ids,
      ids.map((id) => id * 10),
      (p) =>
        autocommit(p, (s) =>
          s.table(t).insert(batch(ints(ids), ints(ids.map((id) => id * 10))), null),
        ),
    );
  for (let k = 0; k < 4; k++) await insert(Array.from({ length: 12 }, (_, j) => k * 12 + j));
  const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
  await autocommit(path, (s) =>
    s.changeCatalog({
      kind: "createIndex",
      index: { tableId: t, name: "v_idx", columnIds: [v], unique: false, role: "secondary" },
    }),
  );
  const primary = path.catalog.table(t)?.primaryKey ?? 0;
  const doomed = [3, 20, 40];
  const located = await probe(path, t, primary, doomed);
  await step(doomed, ["deleted", "deleted", "deleted"], (p) =>
    autocommit(p, (s) => s.table(t).delete(batch(locators(located, t)), null)),
  );
  const [five] = await probe(path, t, primary, [5]);
  await step([5], [555], (p) =>
    autocommit(p, (s) =>
      s.table(t, { assigned: [v] }).update(batch(locators([five ?? 0], t), ints([555])), null),
    ),
  );
  for (let k = 4; k < 7; k++) await insert(Array.from({ length: 12 }, (_, j) => k * 12 + j));
  await path.quiesce();
  path = await harness.reopen();
  await insert([1000, 1001]);
  await path.quiesce();
  await harness.close();
}

/**
 * Large transactions (format/replay.ts "Large transactions"): committed rows, then a transaction
 * of many calls over small memtables that stages, deletes one of its staged rows, rolls back to a
 * savepoint and commits; then one that stages and rolls back; then a reopen and more rows. A crash
 * at any step keeps or loses each transaction whole.
 */
async function stagedScenario(harness: Harness, outcome: Outcome): Promise<void> {
  let path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", INT)]),
        })
      ).id ?? 0;
  });
  const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
  await autocommit(path, (s) =>
    s.changeCatalog({
      kind: "createIndex",
      index: { tableId: t, name: "v_idx", columnIds: [v], unique: false, role: "secondary" },
    }),
  );
  const commit = async (ids: number[]) => {
    for (const id of ids) outcome.uncertain.set(id, ["absent", id * 10]);
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(ids), ints(ids.map((id) => id * 10))), null),
    );
    for (const id of ids) {
      outcome.uncertain.delete(id);
      outcome.acked.set(id, id * 10);
    }
  };
  await commit(Array.from({ length: 12 }, (_, k) => k));
  // The large transaction: 6 calls of 8 rows stage; a savepoint drops the last 2 calls.
  const tx = await path.begin();
  const kept: number[] = [];
  const s = tx.statement({ rules });
  for (let call = 0; call < 6; call++) {
    const ids = Array.from({ length: 8 }, (_, k) => 100 + call * 8 + k);
    await s.table(t).insert(batch(ints(ids), ints(ids.map((id) => id * 10))), null);
    kept.push(...ids);
  }
  await s.end();
  const savepoint = tx.savepoint();
  const later = tx.statement({ rules });
  for (let call = 0; call < 2; call++) {
    const ids = Array.from({ length: 8 }, (_, k) => 200 + call * 8 + k);
    await later.table(t).insert(batch(ints(ids), ints(ids.map((id) => id * 10))), null);
  }
  await later.end();
  expect(path.maintenance.stages).toBeGreaterThan(0);
  tx.rollbackTo(savepoint);
  // A staged row deleted by a later statement.
  const d = tx.statement({ rules });
  const [located] = await probeIn(d, t, [100]);
  await d.table(t).delete(batch(locators([located ?? 0], t)), null);
  await d.end();
  const committed = kept.filter((id) => id !== 100);
  for (const id of committed) outcome.uncertain.set(id, ["absent", id * 10]);
  outcome.atomic?.push(committed);
  const receipt = await tx.commit(null);
  await receipt.durable;
  for (const id of committed) {
    outcome.uncertain.delete(id);
    outcome.acked.set(id, id * 10);
  }
  // One that stages and rolls back: none of it may ever appear.
  const gone = await path.begin();
  const g = gone.statement({ rules });
  for (let call = 0; call < 4; call++) {
    const ids = Array.from({ length: 8 }, (_, k) => 300 + call * 8 + k);
    await g.table(t).insert(batch(ints(ids), ints(ids)), null);
  }
  await g.end();
  gone.rollback();
  await path.quiesce();
  path = await harness.reopen();
  await commit(Array.from({ length: 20 }, (_, k) => 400 + k));
  await path.quiesce();
  await harness.close();
}

/** Locators of `ids` in a statement's view (primary key). */
async function probeIn(
  statement: { view: import("../access/snapshot.js").Snapshot },
  t: number,
  ids: number[],
): Promise<number[]> {
  const view = statement.view;
  const encoded = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(ids), direction: "asc", nulls: "first" }],
    { length: ids.length, selection: null },
    encoded,
  );
  const out = { probes: new Int32Array(64), locators: new Float64Array(64), count: 0 };
  await view.index(t, view.catalog.table(t)?.primaryKey ?? 0).probe(encoded, out);
  encoded.close();
  return Array.from(out.locators.subarray(0, out.count));
}

async function verify(harness: Harness, outcome: Outcome): Promise<void> {
  const path = await harness.open();
  const t = path.catalog.tableByName("t")?.id;
  if (t === undefined) {
    // Crashed before the table was durable: nothing may have been acknowledged.
    expect(outcome.acked.size).toBe(0);
    await harness.close();
    return;
  }
  const view = path.pin();
  const rows = new Map((await rowsOf(view, t)).map((row) => [Number(row[0]), Number(row[1])]));
  view.release();
  for (const [id, state] of outcome.acked) {
    if (outcome.uncertain.has(id)) continue;
    if (state === "deleted")
      expect(rows.has(id), `acknowledged delete of ${String(id)}`).toBe(false);
    else expect(rows.get(id), `acknowledged row ${String(id)}`).toBe(state);
  }
  for (const [id, value] of rows) {
    const allowed = outcome.uncertain.get(id) ?? [outcome.acked.get(id) ?? "absent"];
    expect(allowed, `row ${String(id)} = ${String(value)}`).toContain(value);
  }
  for (const group of outcome.atomic ?? []) {
    const present = group.filter((id) => rows.has(id)).length;
    expect(
      [0, group.length],
      `a transaction's ${String(group.length)} rows, all or none`,
    ).toContain(present);
  }
  // Indexes agree with the rows.
  const def = path.catalog.table(t);
  const ids = [...rows.keys()];
  if (ids.length > 0)
    expect((await probe(path, t, def?.primaryKey ?? 0, ids)).length).toBe(ids.length);
  const secondary = def?.indexes.find((index) => index.name === "v_idx");
  if (secondary !== undefined && ids.length > 0) {
    const values = [...new Set(rows.values())].slice(0, 60);
    const expected = [...rows.values()].filter((value) => values.includes(value)).length;
    expect((await probe(path, t, secondary.id, values)).length).toBe(expected);
  }
  await path.quiesce();
  await harness.space.audit();
  // Writing continues, and survives another reopen.
  await autocommit(path, (s) => s.table(t).insert(batch(ints([9999]), ints([1])), null));
  const reopened = await harness.reopen();
  expect((await probe(reopened, t, def?.primaryKey ?? 0, [9999])).length).toBe(1);
  await reopened.quiesce();
  await harness.space.audit();
  await harness.close();
}

describe("crash sweep", () => {
  it("a publish after replay records the first unflushed row id (found by the sweep)", async () => {
    const harness = new Harness("strict", "sim", MEMTABLE_ROWS);
    let path = await harness.open();
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", INT)]),
          })
        ).id ?? 0;
    });
    // Two commits fill most of one memtable; the third starts the next one.
    for (let k = 0; k < 3; k++) {
      const ids = Array.from({ length: 12 }, (_, j) => k * 12 + j);
      await autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
    }
    // Recovery replays all three; the flush publishes the first memtable only.
    path = await harness.crash();
    await path.quiesce();
    path = await harness.reopen();
    const view = path.pin();
    expect((await rowsOf(view, t)).length).toBe(36);
    view.release();
    await harness.close();
  });

  /** Crashes `make(disk)`'s scenario at every step of a clean run's step count. */
  async function sweep(
    make: (disk?: SimDisk) => Harness,
    run: (harness: Harness, outcome: Outcome) => Promise<void> = scenario,
  ): Promise<void> {
    const clean = make();
    await run(clean, { acked: new Map(), uncertain: new Map() });
    const steps = clean.disk.steps;
    // The scenario really crosses flushes: the clean store holds several published groups.
    const done = await clean.open();
    const t = done.catalog.tableByName("t")?.id ?? 0;
    const view = done.pin();
    const table = view.table(t, { budget: FREE });
    expect(
      table.segments.filter((segment) => segment.kind === "group").length,
    ).toBeGreaterThanOrEqual(3);
    view.release();
    await clean.close();
    let crashes = 0;
    for (let at = Number(process.env.MINNOW_CRASH_STEP ?? 0); at < steps; at++) {
      const disk = new SimDisk({ seed: at, crashAtStep: at });
      const outcome: Outcome = { acked: new Map(), uncertain: new Map(), atomic: [] };
      try {
        await run(make(disk), outcome);
      } catch (error) {
        // Any failure after the power loss is the dead world; one before it is a real bug.
        if (disk.steps <= at) throw error;
        crashes += 1;
      }
      // Real task-clock yields can give this replay fewer physical mutations than
      // the reference. Consume its unspent fault at the completed scenario boundary,
      // not during recovery/continued-write verification. Every case still loses power.
      if (disk.crashes === 0) disk.powerLoss();
      try {
        await verify(make(disk), outcome);
      } catch (error) {
        throw new Error(`Crash at step ${String(at)} of ${String(steps)}: ${String(error)}`, {
          cause: error,
        });
      }
    }
    expect(crashes).toBeGreaterThan(steps / 2);
    console.info(`crash sweep: ${String(steps)} steps, ${String(crashes)} crashed mid-scenario`);
  }

  it("keeps an unspent fault from a shorter replay out of recovery", async () => {
    let reference = true;
    let completedReplays = 0;
    const run = async (harness: Harness, outcome: Outcome) => {
      const path = await harness.open();
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false), column("v", INT)]),
            })
          ).id ?? 0;
      });
      for (const id of [1, 2, 3, 4]) {
        outcome.uncertain.set(id, ["absent", id * 10]);
        await autocommit(path, (s) => s.table(t).insert(batch(ints([id]), ints([id * 10])), null));
        outcome.uncertain.delete(id);
        outcome.acked.set(id, id * 10);
      }
      await path.quiesce();
      await harness.close();
      if (reference) {
        // Different task interleavings can publish different numbers of roots. Model
        // a longer reference deterministically, without depending on timer speed.
        const padding = harness.disk.open("reference-only-work", true);
        for (let step = 0; step < 8; step++) padding.truncate(0);
        padding.close();
        reference = false;
      } else completedReplays += 1;
    };
    await sweep((disk) => new Harness("strict", "sim", 1, disk, { merges: { mode: "off" } }), run);
    expect(completedReplays, "a replay ends before its planned fault").toBeGreaterThan(0);
  }, 1_800_000);

  it("survives a crash at every physical step of commit, flush and publish", async () => {
    await sweep(
      (disk) => new Harness("strict", "sim", MEMTABLE_ROWS, disk, { merges: { mode: "off" } }),
    );
  }, 1_800_000);

  it("survives a crash at every physical step of log recycling", async () => {
    // A small log bound: the scenario recycles several times, each with its flushes, the
    // switching root, the reset and the first frames of the new incarnation.
    const make = (disk?: SimDisk) =>
      new Harness("strict", "sim", MEMTABLE_ROWS, disk, {
        logRecycleBytes: 400,
        merges: { mode: "off" },
      });
    const probe = make();
    await scenario(probe, { acked: new Map(), uncertain: new Map() });
    const path = await probe.open();
    expect(path.state().root.log.firstLsn).toBeGreaterThan(1);
    await probe.close();
    await sweep(make);
  }, 1_800_000);

  it("survives a crash at every physical step of index merges", async () => {
    // Fan-in 2: every second flush merges, at every level; slices by entry count keep the
    // interleaving of merges with commits the same in every run.
    const make = (disk?: SimDisk) =>
      new Harness("strict", "sim", MEMTABLE_ROWS / 2, disk, {
        merges: { fanIn: 2, sliceEntries: 4 },
      });
    // The clean run merges at more than one level, and its publications retire merged runs.
    const probe = make();
    await scenario(probe, { acked: new Map(), uncertain: new Map() });
    const path = await probe.open();
    const t = path.catalog.tableByName("t")?.id ?? 0;
    const levels = [...(path.state().tables.get(t)?.runs.values() ?? [])].flatMap((runs) =>
      runs.map((run) => run.level),
    );
    expect(Math.max(...levels)).toBeGreaterThanOrEqual(2);
    await probe.close();
    await sweep(make);
  }, 1_800_000);

  it("survives a crash at every physical step of large transactions (staging, commit, fold)", async () => {
    const make = (disk?: SimDisk) =>
      new Harness("strict", "sim", 8, disk, { merges: { mode: "off" } });
    // The clean run stages, folds, and drops a rolled-back staging.
    const clean = make();
    await stagedScenario(clean, { acked: new Map(), uncertain: new Map(), atomic: [] });
    const path = await clean.open();
    expect(path.state().root.tables.every((table) => table.staged.length === 0)).toBe(true);
    await clean.close();
    await sweep(make, stagedScenario);
  }, 1_800_000);
});
