import { expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";

it("keeps empty-subquery membership independent of NULLs and query rewrites", async () => {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCompact: false,
    autoCollect: false,
  });
  try {
    await database.execute("CREATE TABLE probes (id INTEGER PRIMARY KEY, n INTEGER, s TEXT)");
    await database.execute("CREATE TABLE members (n INTEGER, s TEXT)");
    await database.execute("INSERT INTO probes VALUES (1,NULL,NULL),(2,2,'x'),(3,3,'y')");
    expect(
      (
        await database.query(`SELECT
      NULL IN (SELECT n FROM members) AS included,
      NULL NOT IN (SELECT n FROM members) AS excluded`)
      ).rows,
    ).toEqual([{ included: false, excluded: true }]);
    for (const column of ["n", "s"]) {
      for (const source of [
        `SELECT ${column} FROM members`,
        `SELECT m.${column} FROM members m WHERE m.${column} = p.${column}`,
        `SELECT ${column} FROM probes WHERE id < 0`,
      ]) {
        expect(
          (
            await database.query(`SELECT id, ${column} IN (${source}) AS included,
          ${column} NOT IN (${source}) AS excluded FROM probes p ORDER BY id`)
          ).rows,
        ).toEqual([1, 2, 3].map((id) => ({ id, included: false, excluded: true })));
        expect(
          (
            await database.query(
              `SELECT id FROM probes p WHERE ${column} NOT IN (${source}) ORDER BY id`,
            )
          ).rows,
        ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
        expect(
          (await database.query(`SELECT id FROM probes p WHERE ${column} IN (${source})`)).rows,
        ).toEqual([]);
      }
    }
    await database.execute("INSERT INTO members VALUES (NULL,NULL),(2,'x')");
    expect(
      (
        await database.query(`SELECT id, n IN (SELECT n FROM members) AS included,
      n NOT IN (SELECT n FROM members) AS excluded FROM probes ORDER BY id`)
      ).rows,
    ).toEqual([
      { id: 1, included: null, excluded: null },
      { id: 2, included: true, excluded: false },
      { id: 3, included: null, excluded: null },
    ]);
  } finally {
    await database.close();
  }
});
