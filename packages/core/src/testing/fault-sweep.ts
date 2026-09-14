/** Shared fault campaign: exact whole-statement states, with acknowledged writes preserved. */
import { MinnowDatabase, type BatchRow } from "../engine/database.js";
import type { BlockStore } from "../storage/index.js";
import { FaultInjectingBlockStore, faultPoints, type FaultPoint } from "./index.js";

interface Row extends BatchRow {
  id: number;
  region: string;
  amount: number;
}
const regions = ["west", "east", "north", "south"];
const rows: Row[] = Array.from({ length: 12 }, (_, index) => ({
  id: index + 1,
  region: regions[index % regions.length] ?? "west",
  amount: (index + 1) * 10,
}));
const updated = rows.map((row) => (row.id === 1 ? { ...row, amount: 999 } : row));
// The full state after each statement, specified independently of the engine's answers.
const states: ReadonlyArray<readonly Row[]> = [
  [],
  rows.slice(0, 6),
  rows,
  rows,
  updated,
  updated,
  updated.filter((row) => row.id !== 4),
];
const steps: ReadonlyArray<(database: MinnowDatabase) => Promise<unknown>> = [
  (db) => db.insertBatch("items", rows.slice(0, 6)),
  (db) => db.insertBatch("items", rows.slice(6)),
  (db) => db.query("SELECT COUNT(*) AS n FROM items"),
  (db) => db.updateBatch("items", { keys: [1], changes: { amount: [999] } }),
  (db) => db.query("SELECT id FROM items WHERE amount > 50 ORDER BY id"),
  (db) => db.deleteBatch("items", { keys: [4] }),
];
const options = { rowsPerBlock: 2, autoCompact: false, autoCollect: false };

export interface FaultSweepStore {
  store: BlockStore;
  reopen(): Promise<BlockStore>;
  cleanup(): Promise<void>;
}

/** A failed statement may be absent or complete; every preceding success must survive. */
export function assertFaultSweepState(actual: unknown, completed: number, failed: boolean): void {
  const candidates = failed ? [states[completed], states[completed + 1]] : [states[completed]];
  const serialized = JSON.stringify(actual);
  if (!candidates.some((candidate) => JSON.stringify(candidate) === serialized)) {
    throw new Error(
      `Invalid durable state after ${String(completed)} acknowledged statements` +
        `${failed ? " and one interrupted statement" : ""}: ${serialized}; allowed ${JSON.stringify(candidates)}`,
    );
  }
}

async function schema(db: MinnowDatabase): Promise<void> {
  await db.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
}

function causedBy(error: unknown, injected: Error): boolean {
  const seen = new Set<unknown>();
  while (error instanceof Error && !seen.has(error)) {
    if (error === injected) return true;
    seen.add(error);
    error = error.cause;
  }
  return false;
}

export async function runFaultSweep(create: () => Promise<FaultSweepStore>): Promise<{
  injections: number;
  counts: Record<FaultPoint, number>;
  outcomes: number[];
}> {
  const counts: Record<FaultPoint, number> = {
    beforeBlockWrite: 0,
    afterBlockWrite: 0,
    beforeBlockRead: 0,
    afterBlockRead: 0,
    beforeTransactionCommit: 0,
    afterTransactionCommit: 0,
  };
  const baseline = await create();
  let armed = false;
  const counting = new MinnowDatabase(
    new FaultInjectingBlockStore(baseline.store, (point) => {
      if (armed) counts[point]++;
    }),
    options,
  );
  try {
    await schema(counting);
    armed = true;
    for (const step of steps) await step(counting);
    armed = false;
    await counting.close();
    const reopened = await baseline.reopen();
    const reader = new MinnowDatabase(reopened, options);
    try {
      assertFaultSweepState(
        (await reader.query("SELECT id, region, amount FROM items ORDER BY id")).rows,
        steps.length,
        false,
      );
    } finally {
      await reader.close();
      reopened.close();
    }
  } finally {
    armed = false;
    await counting.close();
    baseline.store.close();
    await baseline.cleanup();
  }

  let injections = 0;
  const outcomes = new Set<number>();
  for (const point of faultPoints) {
    if (counts[point] === 0) throw new Error(`Workload did not reach ${point}`);
    for (let nth = 1; nth <= counts[point]; nth++) {
      const fixture = await create();
      const injected = new Error(`injected ${point} #${String(nth)}`);
      let seen = 0;
      const injection = { fired: false };
      let enabled = false;
      const database = new MinnowDatabase(
        new FaultInjectingBlockStore(fixture.store, (at) => {
          if (enabled && at === point && ++seen === nth) {
            injection.fired = true;
            throw injected;
          }
        }),
        options,
      );
      try {
        await schema(database);
        enabled = true;
        let completed = 0;
        let failed = false;
        try {
          for (const step of steps) {
            await step(database);
            completed++;
          }
        } catch (error) {
          if (!causedBy(error, injected)) throw error;
          failed = true;
        }
        enabled = false;
        if (!injection.fired) throw new Error(`Fault never fired: ${injected.message}`);
        injections++;
        await database.close();
        const reopened = await fixture.reopen();
        const reader = new MinnowDatabase(reopened, options);
        try {
          const actual = (await reader.query("SELECT id, region, amount FROM items ORDER BY id"))
            .rows;
          assertFaultSweepState(actual, completed, failed);
          outcomes.add(actual.length);
        } catch (error) {
          throw new Error(`${injected.message}: recovery failed`, { cause: error });
        } finally {
          await reader.close();
          reopened.close();
        }
      } finally {
        enabled = false;
        await database.close();
        fixture.store.close();
        await fixture.cleanup();
      }
    }
  }
  return { injections, counts, outcomes: [...outcomes].sort((a, b) => a - b) };
}
