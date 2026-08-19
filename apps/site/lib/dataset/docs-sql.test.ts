import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { retailBatches, retailDefinition } from "./retail";

/**
 * The SQL in the reading docs, executed. Every example is written against the playground's schema
 * precisely so a reader can paste it into the console, which makes an example the engine refuses
 * a broken promise rather than a typo. Only the SELECT guide is covered here: the DDL, DML, and
 * full-text pages build their own tables, and those pages are exercised by the engine's own
 * suites.
 */

const guide = fileURLToPath(new URL("../../content/docs/sql/select.mdx", import.meta.url));

function sqlBlocks(path: string): string[] {
  return [...readFileSync(path, "utf8").matchAll(/```sql\n([\s\S]*?)```/g)]
    .map((match) => (match[1] ?? "").trim())
    .filter((block) => block.length > 0);
}

describe("the SELECT guide's examples", () => {
  it("all run against the playground dataset", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.migrate(retailDefinition);
    for (const batch of retailBatches({ scale: 0.05 })) {
      await database.insertBatch(batch.table, batch.rows);
    }

    const blocks = sqlBlocks(guide);
    expect(blocks.length).toBeGreaterThan(5);
    const failures: string[] = [];
    for (const block of blocks) {
      try {
        await database.query(block, { memoize: false });
      } catch (error) {
        const first = block.split("\n")[0] ?? "";
        failures.push(`${first} … — ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    expect(failures.map(reason)).toEqual(knownGaps);
  }, 120_000);
});

/** The message an example failed with, without the example, so the list below stays readable. */
function reason(failure: string): string {
  return failure.slice(failure.indexOf("— ") + 2);
}

/**
 * Empty, and meant to stay that way. An example this page makes that the engine cannot keep is
 * listed here by its exact message rather than removed from the page, because each one is
 * ordinary SQL that belongs in a SELECT guide — the list is what has to shrink, not the
 * documentation. Adding a line is admitting a gap, so say why in a comment beside it.
 */
const knownGaps: string[] = [];
