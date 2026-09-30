import { describe, expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";
import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { OpfsBlockStore } from "../storage/opfs/store.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";

describe("exact JSON numbers", () => {
  it("keeps extraction, constructors, comparisons and JSON_TABLE exact", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
    });
    try {
      const result = await database.query(`SELECT
        CAST('9007199254740993' AS JSONB) = CAST('9007199254740992' AS JSONB) AS different,
        CAST('1.0000000000000001' AS JSONB) > CAST('1' AS JSONB) AS larger,
        CAST('1.0e2' AS JSONB) = CAST('100' AS JSONB) AS equivalent,
        CAST('{"n":9007199254740993}' AS JSONB) ->> 'n' AS arrow,
        JSON_VALUE('{"n":1.0000000000000001}', '$.n') AS scalar,
        JSON_QUERY('{"n":[9007199254740993]}', '$.n') AS nested,
        TO_JSON(9007199254740993::NUMERIC) AS constructed,
        JSON_ARRAY(9007199254740993::NUMERIC) AS members`);
      expect(result.rows).toEqual([
        {
          different: false,
          larger: true,
          equivalent: true,
          arrow: "9007199254740993",
          scalar: "1.0000000000000001",
          nested: "[9007199254740993]",
          constructed: "9007199254740993",
          members: "[9007199254740993]",
        },
      ]);
      expect(
        (
          await database.query(`SELECT n FROM JSON_TABLE(
            '[{"n":9007199254740993.0001},{"n":1.0000000000000001}]', '$[*]'
            COLUMNS (n NUMERIC(35,16) PATH '$.n')) AS j ORDER BY n`)
        ).rows,
      ).toEqual([{ n: "1.0000000000000001" }, { n: "9007199254740993.0001" }]);
    } finally {
      await database.close();
    }
  });

  for (const kind of ["indexeddb", "opfs"] as const) {
    it(`${kind}: preserves distinct values across reopen, grouping and ordered comparison`, async () => {
      const indexedDB = new IDBFactory();
      const opfs = new MemoryOpfs();
      const open = () =>
        kind === "indexeddb"
          ? IndexedDbBlockStore.open({ name: "json-precision", indexedDB, durability: "strict" })
          : OpfsBlockStore.open({ name: "json-precision", root: opfs.root, durability: "strict" });
      let store = await open();
      let database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      try {
        await database.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, payload JSONB)");
        await database.execute(`INSERT INTO docs VALUES
          (1, '{"n":9007199254740993}'),
          (2, '{"n":9007199254740992}'),
          (3, '{"n":9007199254740993.0}')`);
        await database.close();
        store.close();
        store = await open();
        database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
        expect(
          (await database.query("SELECT COUNT(DISTINCT payload) AS n FROM docs")).rows,
        ).toEqual([{ n: 2 }]);
        expect(
          (
            await database.query("SELECT id, payload ->> 'n' AS n FROM docs ORDER BY payload, id", {
              memoize: false,
            })
          ).rows,
        ).toEqual([
          { id: 2, n: "9007199254740992" },
          { id: 1, n: "9007199254740993" },
          { id: 3, n: "9007199254740993" },
        ]);
      } finally {
        await database.close();
        store.close();
      }
    });
  }
});
