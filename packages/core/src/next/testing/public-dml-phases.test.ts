/** The reported shapes through the public client: DML whose input computes text with `||` runs in
 * producer phases, with the modification spool alive across the handoff. PostgreSQL is the
 * authority for the rows (dml-computed-differential.test.ts generates the wider space). */
import { expect, it } from "vitest";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

it.each([50, 5000])(
  "INSERT ... SELECT with a concatenation, %i rows",
  async (rows) => {
    const owner = await publicOwner({ capBytes: 64 << 20 });
    const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
    try {
      await db.execute("CREATE TABLE t(id INT PRIMARY KEY,v TEXT)");
      await db.execute(
        `INSERT INTO t SELECT g, 'x' || g::text FROM generate_series(1, ${String(rows)}) g`,
      );
      expect((await db.query("SELECT count(*) AS n, min(v) AS lo FROM t")).rows()).toEqual([
        { n: rows, lo: "x1" },
      ]);
      expect((await db.query("SELECT v FROM t WHERE id = 7")).rows()).toEqual([{ v: "x7" }]);
      await db.execute("UPDATE t SET v = v || '!' WHERE id <= 3");
      await db.execute("DELETE FROM t WHERE v || '?' = 'x2!?'");
      expect((await db.query("SELECT id, v FROM t WHERE id <= 3 ORDER BY id")).rows()).toEqual([
        { id: 1, v: "x1!" },
        { id: 3, v: "x3!" },
      ]);
    } finally {
      await db.close();
      await owner.close();
    }
  },
  120000,
);
