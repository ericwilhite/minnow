import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { createReach, CATALOGUE_ORDER_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { assertCatalogueOrder, catalogueOrderColumns } from "./catalogue-order.js";

const reach = createReach("catalogue result ordering", CATALOGUE_ORDER_REACH, { minimum: 2 });
for (const seed of seedsFor("next-catalogue-order", [0xb701, 0xb702]))
  it(`checks generated PostgreSQL order and permits peers, seed ${String(seed)}`, async () => {
    const pg = await PGlite.create();
    const random = mulberry32(seed);
    try {
      await pg.exec("CREATE TABLE t(id INTEGER,k INTEGER,label TEXT)");
      for (let id = 0; id < 20; id++)
        await pg.query("INSERT INTO t VALUES($1,$2,$3)", [
          id,
          id % 5 === 0 ? null : Math.floor(random() * 4),
          `v${String(id % 3)}`,
        ]);
      const shapes = [
        ["ascending", "ORDER BY k ASC NULLS FIRST"],
        ["descending", "ORDER BY k DESC NULLS LAST"],
        ["multiple keys", "ORDER BY k DESC NULLS FIRST,label ASC"],
        ["ordinal", "ORDER BY 2 DESC,1"],
      ] as const;
      for (const [state, tail] of shapes) {
        reach.beginHistory();
        reach.reach(state);
        reach.reach("null keys");
        const sql = `SELECT id,k AS key,label FROM t ${tail.replaceAll(/\bk\b/gu, "key")}`;
        const expected = (await pg.query<Record<string, unknown>>(sql)).rows;
        const columns = ["id", "key", "label"];
        expect(() => assertCatalogueOrder(sql, columns, expected, expected)).not.toThrow();
        // A bag-only oracle (or independently sorting both key sequences) survives this mutant.
        expect(() => assertCatalogueOrder(sql, columns, [...expected].reverse(), expected)).toThrow(
          "ORDER BY key sequence",
        );
        const keys = catalogueOrderColumns(sql, columns);
        const peers: typeof expected = [];
        for (let start = 0; start < expected.length;) {
          let end = start + 1;
          while (
            end < expected.length &&
            keys.every((key) => expected[end]?.[key] === expected[start]?.[key])
          )
            end++;
          peers.push(...expected.slice(start, end).reverse());
          if (end - start > 1) reach.reach("ties");
          start = end;
        }
        expect(() => assertCatalogueOrder(sql, columns, peers, expected)).not.toThrow();
      }
      reach.beginHistory();
      reach.reach("window order only");
      const windowSQL = "SELECT id,ROW_NUMBER() OVER(ORDER BY k) AS rn FROM t";
      const unordered = (await pg.query<Record<string, unknown>>(windowSQL)).rows;
      expect(catalogueOrderColumns(windowSQL, ["id", "rn"])).toEqual([]);
      expect(() =>
        assertCatalogueOrder(windowSQL, ["id", "rn"], [...unordered].reverse(), unordered),
      ).not.toThrow();
      expect(() => catalogueOrderColumns("SELECT id FROM t ORDER BY k", ["id"])).toThrow(
        "unselected catalogue ORDER BY key",
      );
    } finally {
      await pg.close();
    }
  }, 60_000);
it("reaches the catalogue order dimensions", () => reach.assertReached());
