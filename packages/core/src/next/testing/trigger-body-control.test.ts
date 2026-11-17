/** Transaction control stays outside the inline DML-only trigger language. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

for (const control of ["SAVEPOINT body_point", "ROLLBACK"])
  it(`refuses in-body ${control} without executing or installing a trigger`, async () => {
    const native = await new StorageCore().open(),
      pg = new PGlite();
    try {
      for (const sql of [
        "CREATE TABLE src(n INT)",
        "CREATE TABLE audit(n INT)",
        "INSERT INTO src VALUES(7)",
      ])
        await native.execute(sql);
      const catalog = native.harness.path.catalog,
        version = catalog.version;
      expect(() =>
        native.lowerer.lower(
          `CREATE TRIGGER bad AFTER INSERT ON src BEGIN INSERT INTO audit VALUES(1); ${control}; END`,
          native.context(),
        ),
      ).toThrow(
        expect.objectContaining({
          sqlState: "42601",
          message: "Trigger bodies support INSERT, UPDATE, and DELETE statements",
        }),
      );
      expect(native.harness.path.catalog).toBe(catalog);
      expect(native.harness.path.catalog.version).toBe(version);
      expect(native.harness.path.catalog.triggers).toHaveLength(0);
      for (const [sql, expected] of [
        ["SELECT * FROM audit", []],
        ["SELECT * FROM src", [{ n: 7 }]],
      ] as const) {
        const result = await native.execute(sql);
        if (!("batches" in result)) throw new Error("Expected refusal observation query");
        expect(oracleRows(result)).toEqual(expected);
      }
      // PostgreSQL has a different function grammar: it can compile the body but refuses
      // transaction control when invoked by a trigger. No cross-grammar SQLSTATE claim.
      await pg.exec(
        `CREATE TABLE src(n INT); CREATE TABLE audit(n INT); CREATE FUNCTION body_control() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit VALUES(1); ${control}; RETURN NEW; END $$; CREATE TRIGGER bad AFTER INSERT ON src FOR EACH ROW EXECUTE FUNCTION body_control()`,
      );
      let failure: unknown;
      try {
        await pg.exec("INSERT INTO src VALUES(1)");
      } catch (error) {
        failure = error;
      }
      expect(failure).toMatchObject({ code: control === "ROLLBACK" ? "2D000" : "0A000" });
      expect((await pg.query("SELECT * FROM audit")).rows).toEqual([]);
      expect((await pg.query("SELECT * FROM src")).rows).toEqual([]);
    } finally {
      await pg.close();
      await native.close();
    }
  });
