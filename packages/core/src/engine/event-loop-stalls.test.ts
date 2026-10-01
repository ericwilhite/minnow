/**
 * No background task holds the event loop. Each workload below once blocked every other query
 * for hundreds of milliseconds or minutes: a fold of upserts written in an unrelated order,
 * live aggregates patched after large commits, scans over tables carrying many upserts,
 * secondary-index builds and their first lookups, and full-text index builds. A watchdog timer records the longest interval it could not run; the
 * bound is loose enough for a loaded machine and tight enough that a task working through its
 * whole input without yielding fails here. `scripts/stall-survey.mts` measures the real numbers.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const STALL_BOUND_MS = process.env.CI === undefined ? 250 : 750;

/** Runs `work` while a 1 ms watchdog measures the longest stretch it was kept from running. */
async function longestStall(work: () => Promise<void>): Promise<number> {
  let longest = 0;
  let last = performance.now();
  const watchdog = setInterval(() => {
    const now = performance.now();
    longest = Math.max(longest, now - last);
    last = now;
  }, 1);
  try {
    await work();
  } finally {
    clearInterval(watchdog);
  }
  return Math.round(longest);
}

async function settled(database: MinnowDatabase, table: string): Promise<void> {
  let previous = "";
  let quiet = 0;
  while (quiet < 20) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    const active = (await database.listCompactionJobs(table)).some(
      (job) => job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
    );
    const layout = JSON.stringify(
      (await database.listVisibleSegmentPage(table, { limit: 64 })).records.map(
        (segment) => segment.id,
      ),
    );
    quiet = !active && layout === previous ? quiet + 1 : 0;
    previous = layout;
  }
}

function shuffled<T>(values: readonly T[], seed: number): T[] {
  const result = [...values];
  let state = seed;
  for (let index = result.length - 1; index > 0; index -= 1) {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31;
    const other = state % (index + 1);
    const value = result[index];
    const swap = result[other];
    if (value === undefined || swap === undefined) continue;
    result[index] = swap;
    result[other] = value;
  }
  return result;
}

function wideRows(count: number, version: number): Array<Record<string, unknown>> {
  return Array.from({ length: count }, (_, id) => {
    const row: Record<string, unknown> = { id };
    for (let index = 0; index < 13; index += 1) {
      row[`n${String(index)}`] = id * 13 + index + version;
      row[`d${String(index)}`] = new Date(1_700_000_000_000 + id * 1_000 + index);
      row[`s${String(index)}`] = `v${String((id + version) % 97)}-${String(index)}`;
    }
    return row;
  });
}

describe("background work never holds the event loop", () => {
  it("folds wide upserts written in an unrelated order in short slices", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const columns: Array<{ name: string; type: "number" | "datetime" | "string" }> = [
      { name: "id", type: "number" },
    ];
    for (let index = 0; index < 13; index += 1) {
      columns.push({ name: `n${String(index)}`, type: "number" });
      columns.push({ name: `d${String(index)}`, type: "datetime" });
      columns.push({ name: `s${String(index)}`, type: "string" });
    }
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: columns.map((column) => ({ ...column, nullable: column.name !== "id" })),
    });
    await database.insertBatch("t", wideRows(7_000, 0) as never);
    for (let refresh = 1; refresh <= 4; refresh += 1) {
      await database.upsertBatch("t", shuffled(wideRows(7_000, refresh), refresh) as never);
    }
    const stall = await longestStall(() => settled(database, "t"));
    expect(stall).toBeLessThan(STALL_BOUND_MS);
    expect((await database.listVisibleSegmentPage("t", { limit: 64 })).records.length).toBe(1);
    await database.close();
  });

  it("patches live aggregates and scans upsert-heavy tables in short slices", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "region", type: "string" },
        { name: "amount", type: "number" },
      ],
    });
    const row = (id: number, version: number) => ({
      id,
      region: `r${String(id % 7)}`,
      amount: id + version,
    });
    for (let start = 0; start < 200_000; start += 50_000) {
      await database.insertBatch(
        "t",
        Array.from({ length: 50_000 }, (_, index) => row(start + index, 0)),
      );
    }
    const live = database.liveQueries();
    await live.subscribe(
      "SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM t GROUP BY region",
      { onChange: () => undefined },
    );
    const stall = await longestStall(async () => {
      for (let batch = 0; batch < 10; batch += 1) {
        await database.upsertBatch(
          "t",
          Array.from({ length: 10_000 }, (_, index) => row(batch * 10_000 + index, batch + 1)),
        );
      }
      await database.query("SELECT region, SUM(amount) AS total FROM t GROUP BY region", {
        memoize: false,
      });
      await settled(database, "t");
    });
    expect(stall).toBeLessThan(STALL_BOUND_MS);
    live.close();
    await database.close();
  });

  it("builds secondary indexes and answers their first lookups in short slices", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
        { name: "label", type: "string" },
      ],
    });
    for (let start = 0; start < 200_000; start += 50_000) {
      await database.insertBatch(
        "t",
        Array.from({ length: 50_000 }, (_, index) => ({
          id: start + index,
          amount: start + index,
          label: `label-${String(start + index)}`,
        })),
      );
    }
    const stall = await longestStall(async () => {
      await database.execute("CREATE INDEX t_amount ON t (amount)");
      const byAmount = await database.query("SELECT id FROM t WHERE amount = 1234", {
        memoize: false,
      });
      expect(byAmount.rows).toEqual([{ id: 1234 }]);
      await database.execute("CREATE UNIQUE INDEX t_label ON t (label)");
      const byLabel = await database.query("SELECT id FROM t WHERE label = 'label-4321'", {
        memoize: false,
      });
      expect(byLabel.rows).toEqual([{ id: 4321 }]);
    });
    expect(stall).toBeLessThan(STALL_BOUND_MS);
    await database.close();
  });

  it("builds a full-text index in short slices", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "docs",
      columns: [
        { name: "id", type: "number" },
        { name: "body", type: "string" },
      ],
    });
    for (let start = 0; start < 200_000; start += 50_000) {
      await database.insertBatch(
        "docs",
        Array.from({ length: 50_000 }, (_, index) => ({
          id: start + index,
          body: `item ${String(start + index)} quick brown fox ${String((start + index) % 977)}`,
        })),
      );
    }
    const stall = await longestStall(async () => {
      const result = await database.query(
        "SELECT COUNT(*) AS n FROM docs WHERE MATCH(body) AGAINST 'quick fox'",
        { memoize: false },
      );
      expect(result.rows).toEqual([{ n: 200_000 }]);
      // The first full-text query starts the index build in the background; watch it finish.
      await new Promise((resolve) => setTimeout(resolve, 2_000));
    });
    expect(stall).toBeLessThan(STALL_BOUND_MS);
    await database.close();
  });
});
