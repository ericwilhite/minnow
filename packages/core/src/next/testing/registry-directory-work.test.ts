/**
 * The root registry directory's spill work per row of a trigger statement stays flat and small.
 *
 * Law (owner/registry-directory.ts): the directory keeps `REGISTRY_DIRECTORY_LEAVES` record blocks
 * resident; a statement whose per-row working set fits them misses only the blocks it opens for the
 * first time. With 8 resident leaves, each updated row of a trigger UPDATE missed 9.6 leaves and
 * paid 368 directory page reads and 174 spill appends (C1's profile, 2026-11-22); with 16 it misses
 * 3.8 and pays about 111 reads and 60 appends. The bounds below sit just above the 16-leaf counts,
 * so a working set that outgrows the resident leaves again fails here, not only in a benchmark.
 */
import { expect, it } from "vitest";
import { REGISTRY_DIRECTORY_WORK } from "../owner/registry-directory.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

const ROWS = 400;
/** Per updated row: at most this much directory work. */
const BOUNDS = { leafMisses: 4.5, pageReads: 130, appends: 70 };

it("a trigger UPDATE's registry work per row stays within the resident leaves", async () => {
  const owner = await publicOwner({ capBytes: 64 << 20 });
  const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  try {
    await db.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await db.execute("CREATE TABLE mirror(id INT PRIMARY KEY, v TEXT)");
    await db.execute(`INSERT INTO mirror SELECT g, 'm' FROM generate_series(1, ${String(ROWS)}) g`);
    await db.execute(
      `INSERT INTO t SELECT g, 'x' || g::text FROM generate_series(1, ${String(ROWS)}) g`,
    );
    await db.execute(
      "CREATE TRIGGER follow AFTER UPDATE ON t FOR EACH ROW BEGIN UPDATE mirror SET v = NEW.v WHERE id = NEW.id; END",
    );
    const before = { ...REGISTRY_DIRECTORY_WORK };
    await db.execute("UPDATE t SET v = 'y' || (id * 7)::text WHERE id % 2 = 0");
    const updated = ROWS / 2;
    const perRow = Object.fromEntries(
      Object.entries(REGISTRY_DIRECTORY_WORK).map(([key, value]) => [
        key,
        (value - before[key as keyof typeof before]) / updated,
      ]),
    ) as Record<keyof typeof REGISTRY_DIRECTORY_WORK, number>;
    // The trigger really ran through the registry.
    expect(perRow.flushes, "the statement used the registry directory").toBeGreaterThan(0);
    expect((await db.query("SELECT count(*) AS n FROM mirror WHERE v LIKE 'y%'")).rows()).toEqual([
      { n: updated },
    ]);
    for (const [key, bound] of Object.entries(BOUNDS))
      expect(perRow[key as keyof typeof BOUNDS], `${key} per updated row`).toBeLessThanOrEqual(
        bound,
      );
  } finally {
    await db.close();
    await owner.close();
  }
}, 120_000);
