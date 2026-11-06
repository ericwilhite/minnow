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
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };
const MEMTABLE_ROWS = 32;

type Expect = Map<number, number | "deleted">;

interface Outcome {
  readonly acked: Expect;
  /** Ids an interrupted operation touched: either state is allowed. */
  readonly uncertain: Map<number, Array<number | "deleted" | "absent">>;
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
  async function sweep(make: (disk?: SimDisk) => Harness): Promise<void> {
    const clean = make();
    await scenario(clean, { acked: new Map(), uncertain: new Map() });
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
      const outcome: Outcome = { acked: new Map(), uncertain: new Map() };
      try {
        await scenario(make(disk), outcome);
      } catch (error) {
        // Any failure after the power loss is the dead world; one before it is a real bug.
        if (disk.steps <= at) throw error;
        crashes += 1;
      }
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

  it("survives a crash at every physical step of commit, flush and publish", async () => {
    await sweep(
      (disk) => new Harness("strict", "sim", MEMTABLE_ROWS, disk, { merges: { mode: "off" } }),
    );
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
});
