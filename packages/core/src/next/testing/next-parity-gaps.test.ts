/**
 * Each reviewed parity gap matches the refusal the engine actually raises today, so an entry
 * cannot go stale silently: a reworded refusal, or a gap closed without removing its entry,
 * fails here.
 */
import { expect, it } from "vitest";
import { listedGap, NEXT_PARITY_GAPS } from "./next-parity-gaps.js";
import { StorageCore } from "./storage-core.js";

const REFUSED: Readonly<Record<string, readonly string[]>> = {
  "row-site-variable-expression": [
    "INSERT INTO t VALUES (1, 'a' || 'b')",
    "INSERT INTO t VALUES (1, 'q') ON CONFLICT (id) DO UPDATE SET v = t.v || '!'",
    "INSERT INTO t SELECT 2, 'q' RETURNING v || '!'",
    "INSERT INTO c VALUES (1, 'q')",
    "INSERT INTO g (id, v) VALUES (1, 'q')",
  ],
  "union-all-phase-arm": ["SELECT 'a' || 'b' AS c UNION ALL SELECT 'c'"],
};

it("every listed gap names a refusal the engine raises", async () => {
  const core = await new StorageCore(32).open();
  try {
    for (const ddl of [
      "CREATE TABLE t(id INT PRIMARY KEY, v TEXT)",
      "CREATE TABLE c(id INT PRIMARY KEY, v TEXT CHECK (v || 'x' <> 'zx'))",
      "CREATE TABLE g(id INT PRIMARY KEY, v TEXT, w TEXT GENERATED ALWAYS AS (v || '!') STORED)",
    ])
      await core.execute(ddl);
    for (const [id, statements] of Object.entries(REFUSED))
      for (const sql of statements) {
        const error: unknown = await core.execute(sql).then(
          () => undefined,
          (refusal: unknown) => refusal,
        );
        expect(listedGap(error)?.id, sql).toBe(id);
      }
    // Entries without a pinned statement here are covered by their own suites.
    const pinned = new Set([...Object.keys(REFUSED), "trigger-indexed-domain-preflight"]);
    expect(NEXT_PARITY_GAPS.filter((gap) => !pinned.has(gap.id))).toEqual([]);
  } finally {
    await core.close();
  }
});
