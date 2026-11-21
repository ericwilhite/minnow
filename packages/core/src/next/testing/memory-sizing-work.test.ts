/**
 * A trigger body's keyed read sizes its source without visiting the memtable's rows (design A,
 * write/memtable.ts sizing summaries). Before, page/read-bounds.ts sized a one-row source with one
 * `readHint` per memtable row, so each lookup cost O(memtable rows) and a trigger statement
 * O(rows²). Law: the sizing work per lookup (hints and summaries read) is independent of the
 * memtable's size; rows are still exactly PostgreSQL's (each mirror row takes the new value).
 */
import { afterEach, expect, it, vi } from "vitest";
import { createReach, MEMORY_SIZING_WORK_REACH } from "../../testing/reach.js";
import { MemoryReads } from "../write/memory-read.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

afterEach(() => vi.restoreAllMocks());

const LOOKUPS = 50;

/** Hints and summaries read while `LOOKUPS` trigger-body lookups run against a memtable of `rows`. */
async function sizingWork(rows: number): Promise<{ hints: number; summaries: number }> {
  const owner = await publicOwner({ capBytes: 64 << 20 });
  const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  try {
    await db.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await db.execute("CREATE TABLE mirror(id INT PRIMARY KEY, v TEXT)");
    await db.execute(`INSERT INTO mirror SELECT g, 'm' FROM generate_series(1, ${String(rows)}) g`);
    await db.execute(`INSERT INTO t SELECT g, 'x' FROM generate_series(1, ${String(LOOKUPS)}) g`);
    await db.execute(
      "CREATE TRIGGER follow AFTER UPDATE ON t FOR EACH ROW BEGIN UPDATE mirror SET v=NEW.v WHERE id=NEW.id; END",
    );
    const hint = vi.spyOn(MemoryReads.prototype, "hint");
    // Counted where it exists (an engine without summaries sizes with hints alone).
    const summary =
      "summary" in MemoryReads.prototype ? vi.spyOn(MemoryReads.prototype, "summary") : null;
    await db.execute("UPDATE t SET v = $1 || id::text", ["new-"]);
    const work = { hints: hint.mock.calls.length, summaries: summary?.mock.calls.length ?? 0 };
    hint.mockRestore();
    summary?.mockRestore();
    expect(
      (await db.query("SELECT count(*) AS n FROM mirror WHERE v = 'new-' || id::text")).rows(),
    ).toEqual([{ n: LOOKUPS }]);
    expect((await db.query("SELECT count(*) AS n FROM mirror WHERE v = 'm'")).rows()).toEqual([
      { n: rows - LOOKUPS },
    ]);
    return work;
  } finally {
    await db.close();
    await owner.close();
  }
}

it("sizes trigger-body lookups with work independent of the memtable's size", async () => {
  const reach = createReach("memory sizing work", MEMORY_SIZING_WORK_REACH);
  reach.beginHistory();
  const work = new Map<number, { hints: number; summaries: number }>();
  for (const [rows, state] of [
    [100, "100-row memtable"],
    [1_000, "1,000-row memtable"],
    [20_000, "20,000-row memtable"],
  ] as const) {
    work.set(rows, await sizingWork(rows));
    reach.reach(state);
  }
  const small = work.get(100),
    large = work.get(20_000);
  if (small === undefined || large === undefined) throw new Error("missing measurement");
  if (large.summaries > 0) reach.reach("summaries read");
  for (const [rows, counts] of work) {
    // Per lookup: a handful of reads (the root's own scan of t adds a fixed few), never a
    // number that tracks the memtable's rows.
    const perLookup = (counts.hints + counts.summaries) / LOOKUPS;
    expect(perLookup, `sizing reads per lookup at ${String(rows)} memtable rows`).toBeLessThan(8);
    expect(counts, `sizing reads at ${String(rows)} rows equal those at 100`).toEqual(small);
  }
  reach.assertReached();
}, 600_000);
