import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { retailBatches, retailDefinition } from "../../lib/dataset/retail";
import { sampleQueries } from "./queries";

/**
 * Every query the playground offers, run against the dataset the playground builds. A chip that
 * errors is the worst thing this site can do — the reader clicked it to see the engine work — so
 * these run for real rather than being checked for syntax.
 */

/** The smallest scale that still fills every table: enough for the queries, quick to build. */
async function build(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  await database.migrate(retailDefinition);
  for (const batch of retailBatches({ scale: 0.05 })) {
    await database.insertBatch(batch.table, batch.rows);
  }
  return database;
}

/**
 * Generous next to the tens of milliseconds these actually take, because what it is guarding
 * against is not a slow machine. A market-basket self-join whose ON clause loses its hash key
 * runs for ten minutes rather than a tenth of a second, and the reader sees a panel that never
 * answers — the failure this budget catches is that one, in whatever form it comes back.
 */
const budgetMs = 5_000;

describe("playground sample queries", () => {
  it("every chip runs, answers with rows, and answers quickly", async () => {
    const database = await build();
    const failures: string[] = [];
    const empty: string[] = [];
    const slow: string[] = [];
    for (const query of sampleQueries) {
      const started = performance.now();
      try {
        const result = await database.query(query.sql, { memoize: false });
        if (result.rows.length === 0) empty.push(query.id);
      } catch (error) {
        failures.push(`${query.id}: ${error instanceof Error ? error.message : String(error)}`);
        continue;
      }
      const elapsed = performance.now() - started;
      if (elapsed > budgetMs) slow.push(`${query.id}: ${elapsed.toFixed(0)}ms`);
    }
    expect(failures).toEqual([]);
    // A query that compiles but answers nothing is a broken demonstration too.
    expect(empty).toEqual([]);
    expect(slow).toEqual([]);
  }, 120_000);
});
