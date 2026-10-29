/**
 * Work bounds for maintained live queries: a one-row commit must cost the same whether the table
 * holds a thousand rows or fifty thousand, because maintenance touches what changed, not what is
 * held. The live oracles judge rows; this judges work, so a patch that quietly re-evaluates
 * everything fails here even while its results stay right. That happened to grouped aggregates:
 * each commit evaluated and sorted every output group, so a "maintained" GROUP BY over a hundred
 * thousand groups cost as much as running the statement again.
 *
 * Work is counted, never timed: delta queries the row-patching path ran, groups whose output an
 * aggregate evaluated or put back in order, and whether each delivery was patched or re-executed. Each shape runs the
 * same commits at both sizes and must report the same counts.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabase } from "./database.js";
import { liveAggregateTestHooks } from "./live-aggregate.js";
import { liveMaintenanceTestHooks } from "./live-maintenance.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const SIZES = [1_000, 50_000] as const;

/** Each shape over `t (id, g, x)`, where `g` has one value per two rows: groups grow with the table. */
const SHAPES = [
  { name: "keyed lookup", sql: "SELECT id, x FROM t WHERE id = 7" },
  { name: "ordered window", sql: "SELECT id, x FROM t ORDER BY x DESC, id LIMIT 20" },
  { name: "filtered list", sql: "SELECT id, x FROM t WHERE x < 3 ORDER BY id" },
  {
    name: "top groups by key",
    sql: "SELECT g, COUNT(*) AS n, SUM(x) AS s FROM t GROUP BY g ORDER BY g LIMIT 10",
  },
  {
    name: "top groups by aggregate, with HAVING",
    sql: "SELECT g, SUM(x) AS s FROM t GROUP BY g HAVING COUNT(*) > 1 ORDER BY s DESC, g LIMIT 10",
  },
  { name: "single aggregate", sql: "SELECT COUNT(*) AS n, SUM(x) AS s FROM t" },
] as const;

/** One insert, one update, one delete, each to a row near the start of the key range. */
const COMMITS = [
  "INSERT INTO t VALUES (-1, 3, 2)",
  "UPDATE t SET x = x + 50 WHERE id = 7",
  "UPDATE t SET x = 1 WHERE id = 9",
  "DELETE FROM t WHERE id = 4",
  "INSERT INTO t VALUES (-2, 0, 1)",
];

interface Work {
  deltaQueries: number;
  evaluatedGroups: number;
  placedGroups: number;
  maintained: number;
  reruns: number;
  deliveries: number;
}

async function measure(sql: string, rows: number): Promise<Work> {
  const database = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false });
  const live = database.liveQueries();
  try {
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g INTEGER, x INTEGER)");
    const batch = Array.from({ length: rows }, (_, id) => ({ id, g: id >> 1, x: id % 5 }));
    for (let start = 0; start < rows; start += 10_000) {
      await database.insertBatch("t", batch.slice(start, start + 10_000));
    }
    let deliveries = 0;
    await live.subscribe(sql, { onChange: () => (deliveries += 1) });
    const before = {
      deltaQueries: liveMaintenanceTestHooks.deltaQueries,
      evaluatedGroups: liveAggregateTestHooks.evaluatedGroups,
      placedGroups: liveAggregateTestHooks.placedGroups,
      maintained: live.stats.maintained,
      reruns: live.stats.reruns,
      deliveries,
    };
    for (const commit of COMMITS) {
      await database.execute(commit);
      await live.refresh();
    }
    return {
      deltaQueries: liveMaintenanceTestHooks.deltaQueries - before.deltaQueries,
      evaluatedGroups: liveAggregateTestHooks.evaluatedGroups - before.evaluatedGroups,
      placedGroups: liveAggregateTestHooks.placedGroups - before.placedGroups,
      maintained: live.stats.maintained - before.maintained,
      reruns: live.stats.reruns - before.reruns,
      deliveries: deliveries - before.deliveries,
    };
  } finally {
    live.close();
    await database.close();
  }
}

describe("maintained live queries do work bounded by the commit, not the table", () => {
  it.each(SHAPES)("$name", async ({ sql }) => {
    const [small, large] = [await measure(sql, SIZES[0]), await measure(sql, SIZES[1])];
    expect(small.reruns, "a maintained shape never re-executes").toBe(0);
    expect(large, `work at ${String(SIZES[1])} rows against ${String(SIZES[0])}`).toEqual(small);
  });

  it("evaluates and orders only the groups a commit touched", async () => {
    const work = await measure(SHAPES[3].sql, SIZES[1]);
    // Five commits, each touching one group (an update that moves a row between groups would
    // touch two); none may evaluate or re-sort the other twenty-five thousand.
    expect(work.evaluatedGroups).toBeGreaterThan(0);
    expect(work.evaluatedGroups).toBeLessThanOrEqual(COMMITS.length * 2);
    expect(work.placedGroups).toBeLessThanOrEqual(COMMITS.length * 2);
  });
});
