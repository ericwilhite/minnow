/** Shared decode progress ends before a merge range lends its pages downstream. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import { batchesOf, CollectSink } from "./join-fixtures.js";
import type { Operator } from "./pipeline.js";
import { Charge, Pool } from "./spill-pool.js";
import { SemanticStep } from "./semantic-step.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";
it("merges four charged semantic runs and lends the shared pair to its consumer", async () => {
  const types: PhysicalType[] = Array.from({ length: 16 }, () => ({ kind: "json", binary: true }));
  const keys = types.map((_, column) => ({
    column,
    direction: "asc" as const,
    nulls: "last" as const,
    collation: null,
  }));
  const governor = new Governor(16 << 20),
    spill = new MemorySpillService(),
    signal = new AbortController();
  const grant = await governor.admit({
    label: "internal sort",
    minimumBytes: sortMinimumBytes(types),
    spillable: true,
  });
  const host = await governor.admit({
    label: "semantic host",
    minimumBytes: 1 << 20,
    spillable: true,
  });
  const pool = new Pool(new Charge(host), 1 << 20),
    step = new SemanticStep(pool, host, signal.signal);
  const blockerBytes = governor.capBytes - governor.reserveBytes - governor.usedBytes;
  const blocker = await governor.admit({
    label: "competing",
    minimumBytes: blockerBytes,
    spillable: true,
  });
  expect(blocker.allocate(blockerBytes)).toBe(true);
  const sink = new CollectSink(),
    owner = {};
  let pending = Promise.resolve();
  const consumer: Operator = {
    push(batch) {
      pending = step.prepare(owner, 128).then(() => {
        step.release(owner);
        sink.push(batch);
      });
      return "blocked";
    },
    ready: () => pending,
    finish: () => sink.finish(),
    abort: (reason) => sink.abort(reason),
  };
  const sort = new SortOperator(
    { types, keys, offset: 0, limit: null, withTies: false },
    { grant, spill, queryId: 517, signal: signal.signal, semanticStep: step },
    consumer,
  );
  const data = [3, 1, 2, 0].map((n) =>
    types.map((_, i) => (i === 15 ? String(n) : JSON.stringify("汉".repeat(1365)))),
  );
  const pg = new PGlite();
  try {
    await pg.waitReady;
    const names = types.map((_, i) => "k" + String(i));
    let param = 0;
    const values = data.map(
      (row) => "(" + row.map(() => "$" + String(++param) + "::jsonb").join(",") + ")",
    );
    const expected = (
      await pg.query<Record<string, string>>(
        "SELECT " +
          names.map((n) => n + "::text").join(",") +
          " FROM(VALUES" +
          values.join(",") +
          ")t(" +
          names.join(",") +
          ") ORDER BY " +
          names.join(","),
        data.flat(),
      )
    ).rows.map((row) => Object.values(row));
    for (const row of data) {
      for (const batch of batchesOf(types, [row]))
        if (sort.push(batch) === "blocked") await sort.ready();
      await sort.reclaim();
    }
    await sort.finish();
    await sort.closed;
    expect(sink.rows).toEqual(expected);
    expect(step.comparisons).toBeGreaterThan(0);
    expect(step.peakBytes).toBeLessThan(250000);
    expect(sort.profile.spilledBytes).toBeGreaterThan(0);
    expect(pool.used).toBe(0);
  } finally {
    sort.abort(new Error("test cleanup"));
    await sort.closed;
    await step.close();
    pool.close();
    host.close();
    grant.close();
    blocker.close();
    await pg.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
}, 60000);

it("retires resident semantic caches before delivery but preserves its admitted minimum", async () => {
  const types: PhysicalType[] = Array.from({ length: 3 }, () => ({ kind: "json", binary: true }));
  const keys = types.map((_, column) => ({
    column,
    direction: "asc" as const,
    nulls: "last" as const,
    collation: null,
  }));
  const governor = new Governor(32 << 20),
    spill = new MemorySpillService();
  const minimum = sortMinimumBytes(types) + (1 << 20);
  const grant = await governor.admit({
    label: "cached semantic sort",
    minimumBytes: minimum,
    spillable: true,
  });
  const host = await governor.admit({
    label: "cached semantic host",
    minimumBytes: 1 << 20,
    spillable: true,
  });
  const signal = new AbortController(),
    pool = new Pool(new Charge(host), 1 << 20);
  const step = new SemanticStep(pool, host, signal.signal),
    sink = new CollectSink();
  let heldDuringDelivery = 0;
  const consumer: Operator = {
    push(batch) {
      heldDuringDelivery = grant.held;
      return sink.push(batch);
    },
    ready: () => sink.ready(),
    finish: () => sink.finish(),
    abort: (reason) => sink.abort(reason),
  };
  const sort = new SortOperator(
    { types, keys, offset: 0, limit: null, withTies: false },
    { grant, spill, queryId: 518, signal: signal.signal, semanticStep: step },
    consumer,
  );
  try {
    for (const batch of batchesOf(
      types,
      [3, 1, 2, 0].map((n) => types.map(() => JSON.stringify({ x: "a".repeat(256), n }))),
    ))
      if (sort.push(batch) === "blocked") await sort.ready();
    await sort.finish();
    expect(sort.profile.operandBytes).toBeGreaterThan(0); // ordinary cached comparisons ran
    expect(heldDuringDelivery).toBeGreaterThanOrEqual(minimum);
  } finally {
    sort.abort(new Error("resident cache test cleanup"));
    await sort.closed;
    await step.close();
    pool.close();
    host.close();
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
});
