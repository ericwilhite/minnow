/** Shared sort/spill and task-clock coverage under tight grants, with independent PG answers. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, ORDERED_SET_MEMORY_REACH } from "../../../testing/reach.js";
import { Governor, type Grant } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import type { Rel, AggregateCall, AggregateFn } from "../../ir/ir.js";
import { OutputArena } from "../kernels/vectors.js";
import { ArraySource, CollectSink, DBL, INT, batchesOf, type Row } from "../join-fixtures.js";
import { lit, numbers } from "../kernels/test-support.js";
import { drive, taskClock } from "../pipeline.js";
import { OrderedSetOperator, orderedSetMinimumBytes } from "./ordered-set.js";
function present<T>(value: T | undefined): T {
  if (value === undefined) throw new TypeError("Missing generated function");
  return value;
}
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

async function run(
  fn: AggregateFn,
  n: number,
  groups: number,
  descending: boolean,
  cancel: boolean,
  logicalClock: boolean,
  multiple: boolean,
) {
  const types = [INT, DBL];
  const functions = multiple ? (["percentile_cont", "percentile_disc", "mode"] as const) : [fn];
  const output = [
    { name: "g", type: INT, nullable: false },
    ...functions.map((_, index) => ({ name: `p${String(index)}`, type: DBL, nullable: true })),
  ];
  const calls: AggregateCall[] = functions.map((fn) => ({
    fn,
    args: fn === "mode" ? [] : [lit(numbers(DBL, [0.5]))],
    distinct: false,
    filter: null,
    orderBy: [
      {
        expr: { op: "column", index: 1, type: DBL },
        direction: descending ? "desc" : "asc",
        nulls: "last",
        collation: null,
      },
    ],
    type: DBL,
  }));
  const rel: Extract<Rel, { op: "aggregate" }> = {
    op: "aggregate",
    id: 2,
    input: {
      op: "values",
      id: 1,
      rows: [],
      output: types.map((type, index) => ({ name: String(index), type, nullable: false })),
    },
    groupBy: [0],
    groupingSets: null,
    aggregates: calls,
    output,
  };
  const minimum = orderedSetMinimumBytes(rel, types);
  const governor = new Governor(Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7)));
  const loans: Grant[] = [];
  for (let slot = 0; slot < calls.length + 2; slot++)
    loans.push(
      await governor.admit({
        label: `ordered/loan${String(slot)}`,
        minimumBytes: 0,
        spillable: true,
      }),
    );
  const allocated = () => grant.allocated + loans.reduce((sum, owner) => sum + owner.allocated, 0);
  const grant = await governor.admit({
    label: "ordered-set",
    minimumBytes: minimum,
    spillable: true,
  });
  const arena = new OutputArena(grant, 2048);
  const spill = new MemorySpillService();
  let files = 0;
  let retained = false,
    closeWaited = false;
  const controller = new AbortController();
  const reason = new Error("cancel during spill append");
  const create = spill.create.bind(spill);
  spill.create = async (queryId: number) => {
    files++;
    const file = await create(queryId);
    const append = file.append.bind(file);
    file.append = async (page) => {
      if (cancel && !controller.signal.aborted) {
        const before = allocated();
        operator.abort(reason);
        controller.abort(reason);
        retained = allocated() === before;
        let drained = false;
        void operator.closed.then(() => {
          drained = true;
        });
        await Promise.resolve();
        closeWaited = !drained;
      }
      return append(page);
    };
    return file;
  };
  const sink = new CollectSink(1);
  const operator = new OrderedSetOperator(
    rel,
    { input: types, arena, params: { slots: [] } },
    { grant, loans, spill, queryId: 1, signal: controller.signal },
    sink,
  );
  const rows: Row[] = Array.from({ length: n }, (_, index) => [index % groups, n - index]);
  const now = taskClock.now;
  let time = 0;
  const beforeEpoch = taskClock.epoch;
  if (logicalClock) {
    taskClock.now = () => (time += 0.02);
    taskClock.begin();
  }
  try {
    const work = drive(
      new ArraySource(batchesOf(types, rows, { batchRows: 127 })),
      operator,
      controller.signal,
    );
    if (cancel) await expect(work).rejects.toBe(reason);
    else {
      await work;
      const aggregates = functions
        .map(
          (name, index) =>
            `${name}(${name === "mode" ? "" : "0.5"}) WITHIN GROUP(ORDER BY (${String(n)}-i)::double precision ${descending ? "DESC" : "ASC"}) AS p${String(index)}`,
        )
        .join(",");
      const sql = `SELECT i%${String(groups)} AS g,${aggregates} FROM generate_series(0,${String(n - 1)}) AS i GROUP BY g ORDER BY g`;
      expect(sink.rows).toEqual(
        (await pg.query<Record<string, unknown>>(sql)).rows.map(Object.values),
      );
      expect(grant.allocated).toBe(arena.bytes);
    }
    if (cancel) {
      expect(retained, "borrowed spill bytes remain charged until append settles").toBe(true);
      expect(closeWaited, "close waits for the borrowed append").toBe(true);
    }
    expect(files).toBeGreaterThan(0);
    await operator.closed;
    expect(spill.bytesInUse).toBe(0);
    expect(loans.every((owner) => owner.allocated === 0 && owner.held === 0)).toBe(true);
    expect(grant.allocated).toBe(arena.bytes);
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
    if (logicalClock) expect(taskClock.epoch).toBeGreaterThan(beforeEpoch);
  } finally {
    taskClock.now = now;
    taskClock.begin();
    operator.abort(new Error("test cleanup"));
    await operator.closed;
    arena.close();
    grant.close();
    for (const owner of loans) owner.close();
  }
  expect(governor.usedBytes).toBe(0);
}

it.each(seedsFor("next-ordered-set-memory", [0xb603, 0xb604]))(
  "spills and closes ordered sets under a tight cap (seed %i)",
  async (seed) => {
    const reach = createReach<string>("ordered-set spill", ORDERED_SET_MEMORY_REACH);
    const fns = ["percentile_cont", "percentile_disc", "mode"] as const;
    const examples: Array<[number, number, number, boolean, boolean, boolean, boolean]> = [];
    for (let fn = 0; fn < 3; fn++)
      for (const cancel of [false, true])
        examples.push([
          fn,
          20000,
          fn === 0 ? 1 : 1000,
          fn % 2 === 0,
          cancel,
          fn % 2 !== 0,
          fn === 2,
        ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 2 }),
        fc.integer({ min: 16000, max: 24000 }),
        fc.integer({ min: 1000, max: 2000 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (fn, n, groups, descending, cancel, clock, multiple) => {
          reach.beginHistory();
          await run(present(fns[fn]), n, groups, descending, cancel, clock, multiple);
          if (!cancel && groups === 1) reach.reach("single group");
          if (!cancel && groups >= 1000) reach.reach("many groups");
          if (multiple) reach.reach("multiple ordered calls");
          reach.reach(present(fns[fn]));
          reach.reach(cancel ? "cancelled append" : "backpressure");
          if (cancel) reach.reach("pending bytes retained");
          reach.reach(clock ? "task clock" : "real clock");
          reach.reach("spill");
          reach.reach("zero leak");
        },
      ),
      { seed, numRuns: 10, examples },
    );
    reach.assertReached();
  },
  300_000,
);
