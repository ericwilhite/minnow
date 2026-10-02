/**
 * Index bases fit the stores' MAX_FTS_BASE_CHUNKS whatever the table's size. Postings were
 * always written 128 to a chunk, so a secondary index over more than about 524,000 distinct
 * values, or a full-text index over a few hundred thousand documents, failed to build on every
 * store: a CREATE INDEX was refused, and full-text search fell back to scanning. Chunks now
 * grow only as a build approaches the limit. Both cases here failed before the change.
 *
 * A lookup reads every chunk whose term range covers its term, and a chunk's terms come from one
 * window of rows, so the third case pins windows to whole blocks: windows of 16,384 rows once
 * made each lookup through a 500,000-row index read four times the chunks, three times slower.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MAX_FTS_BASE_CHUNKS } from "../storage/types.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(180_000) });

/** Counts the chunks each posting build stages, keyed by build. */
function countChunks(store: MemoryBlockStore): Map<string, number> {
  const chunks = new Map<string, number>();
  const write = store.writeFtsBaseBuildChunk.bind(store);
  store.writeFtsBaseBuildChunk = async (input) => {
    chunks.set(input.buildId, Math.max(chunks.get(input.buildId) ?? 0, input.ordinal + 1));
    return write(input);
  };
  return chunks;
}

describe("index capacity", () => {
  it("lets at most one chunk per block of rows cover any one term", async () => {
    const store = new MemoryBlockStore();
    const ranges: Array<[string, string]> = [];
    const write = store.writeFtsBaseBuildChunk.bind(store);
    store.writeFtsBaseBuildChunk = async (input) => {
      ranges.push([input.chunk[0]?.term ?? "", input.chunk.at(-1)?.term ?? ""]);
      return write(input);
    };
    const db = new MinnowDatabase(store, { autoCompact: false });
    await db.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
      ],
    });
    const rows = 200_000;
    for (let start = 0; start < rows; start += 50_000) {
      await db.insertBatch(
        "t",
        Array.from({ length: 50_000 }, (_, index) => ({
          id: start + index,
          amount: ((start + index) * 7_919) % 1_000_003,
        })),
      );
    }
    await db.execute("CREATE INDEX t_amount ON t (amount)");
    const blocks = Math.ceil(rows / 65_536);
    const probes = ranges.filter((_, index) => index % 97 === 0).map(([first]) => first);
    for (const term of probes) {
      const covering = ranges.filter(([first, last]) => first <= term && term <= last).length;
      expect(covering).toBeLessThanOrEqual(blocks);
    }
    await db.close();
  });

  it("builds a secondary index over more distinct values than 128-posting chunks hold", async () => {
    const store = new MemoryBlockStore();
    const chunks = countChunks(store);
    const db = new MinnowDatabase(store, { autoCompact: false });
    await db.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
      ],
    });
    const rows = 600_000;
    for (let start = 0; start < rows; start += 50_000) {
      await db.insertBatch(
        "t",
        Array.from({ length: 50_000 }, (_, index) => ({
          id: start + index,
          amount: ((start + index) * 7_919) % 1_000_003,
        })),
      );
    }
    await db.execute("CREATE INDEX t_amount ON t (amount)");
    const table = await store.getTableByName("t");
    expect(Object.values(table?.secondaryIndexes ?? {})[0]?.state).toBe("ready");
    const [staged] = [...chunks.values()];
    expect(staged).toBeGreaterThan(rows / 128 / 2);
    expect(staged).toBeLessThanOrEqual(MAX_FTS_BASE_CHUNKS);
    const amount = (543_210 * 7_919) % 1_000_003;
    expect(
      (await db.query(`SELECT id FROM t WHERE amount = ${String(amount)}`, { memoize: false }))
        .rows,
    ).toEqual([{ id: 543_210 }]);
    await db.close();
  });

  it("builds a full-text index over a corpus 128-posting chunks cannot hold", async () => {
    const store = new MemoryBlockStore();
    const chunks = countChunks(store);
    const errors: unknown[] = [];
    const db = new MinnowDatabase(store, { onBackgroundError: (error) => errors.push(error) });
    await db.createTable({
      name: "docs",
      columns: [
        { name: "id", type: "number" },
        { name: "body", type: "string" },
      ],
    });
    const documents = 450_000;
    for (let start = 0; start < documents; start += 50_000) {
      await db.insertBatch(
        "docs",
        Array.from({ length: 50_000 }, (_, index) => {
          const id = start + index;
          return {
            id,
            body: `item ${String(id)} quick brown fox ${String(id % 977)} jumps over ${String(id % 31)}`,
          };
        }),
      );
    }
    await db.query("SELECT COUNT(*) AS n FROM docs WHERE MATCH(body) AGAINST 'quick fox'", {
      memoize: false,
    });
    // The first search starts the build in the background.
    for (let attempt = 0; attempt < 600; attempt += 1) {
      const table = await store.getTableByName("docs");
      const states = Object.values(table?.ftsColumns ?? {}).map((column) => column.state);
      if (states.includes("ready") || states.includes("invalid")) break;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    expect(errors).toEqual([]);
    const table = await store.getTableByName("docs");
    expect(Object.values(table?.ftsColumns ?? {}).map((column) => column.state)).toEqual(["ready"]);
    const [staged] = [...chunks.values()];
    expect(staged).toBeLessThanOrEqual(MAX_FTS_BASE_CHUNKS * 0.8);
    expect(
      (
        await db.query("SELECT id FROM docs WHERE MATCH(body) AGAINST 'item 123456'", {
          memoize: false,
        })
      ).rows,
    ).toEqual([{ id: 123_456 }]);
    await db.close();
  });
});
