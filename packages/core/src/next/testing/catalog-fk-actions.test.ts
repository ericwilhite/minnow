/** PG's declared actions survive MCAT; omitted ON UPDATE retains the documented follow contract. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_FK_ACTION_REACH } from "../../testing/reach.js";
import { currentInput } from "../lower/ddl.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { Catalog } from "../write/catalog.js";
import { decodeCatalogStream } from "../write/catalog-stream.js";
import { StorageCore } from "./storage-core.js";

const choices = ["omitted", "noAction", "restrict", "cascade", "setNull", "setDefault"] as const;
const spelling = {
  omitted: "",
  noAction: "NO ACTION",
  restrict: "RESTRICT",
  cascade: "CASCADE",
  setNull: "SET NULL",
  setDefault: "SET DEFAULT",
} as const;
const pgAction = {
  a: "noAction",
  r: "restrict",
  c: "cascade",
  n: "setNull",
  d: "setDefault",
} as const;
const display = {
  noAction: "no action",
  restrict: "restrict",
  cascade: "cascade",
  setNull: "set null",
  setDefault: "set default",
  follow: "follow",
} as const;
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
for (const seed of seedsFor("next-catalog-fk-actions", [0xb7ca, 0xb7cb]))
  it(`retains effective FK actions and their explicit/default distinction seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("catalog FK actions", CATALOG_FK_ACTION_REACH);
    try {
      await pg.exec("CREATE TABLE p(id INT PRIMARY KEY)");
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...choices),
          fc.constantFrom(...choices),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.constantFrom("pid", "Case", "a.b"),
          fc.boolean(),
          async (deletion, update, self, named, reverse, checkpoint, column, duplicateUpdate) => {
            reach.beginHistory();
            const native = await new StorageCore().open();
            let created = false;
            try {
              await native.execute("CREATE TABLE p(id INT PRIMARY KEY)");
              const clauses = [
                deletion === "omitted" ? "" : `ON DELETE ${spelling[deletion]}`,
                update === "omitted" ? "" : `ON UPDATE ${spelling[update]}`,
              ];
              if (reverse) clauses.reverse();
              const reference = `REFERENCES ${self ? "c" : "p"}(id) ${clauses.join(" ")}`,
                sql = `CREATE TABLE c(id INT PRIMARY KEY,${quote(column)} INT DEFAULT 0 ${named ? `,CONSTRAINT fk FOREIGN KEY(${quote(column)}) ` : ""}${reference})`;
              await pg.exec(sql);
              created = true;
              await native.execute(sql);
              const pgConstraint = (
                await pg.query<{
                  confdeltype: keyof typeof pgAction;
                  confupdtype: keyof typeof pgAction;
                }>(
                  "SELECT confdeltype,confupdtype FROM pg_constraint WHERE contype='f' AND conrelid='c'::regclass",
                )
              ).rows[0];
              if (!pgConstraint) throw new Error("missing PG foreign key");
              const expected = {
                  onDelete: pgAction[pgConstraint.confdeltype],
                  // Public product contract: omission follows row identity, unlike PG's NO ACTION.
                  onUpdate: update === "omitted" ? "follow" : pgAction[pgConstraint.confupdtype],
                } as const,
                table = native.harness.path.catalog.tableByName("c");
              if (!table) throw new Error("missing native table");
              expect(table.foreignKeys).toHaveLength(1);
              expect(table.foreignKeys[0]).toMatchObject(expected);
              expect(currentInput(table).input.foreignKeys[0]).toMatchObject(expected);
              expect(
                inspectCatalog(native.harness.path.catalog).tables.find((t) => t.name === "c")
                  ?.foreignKeys[0],
              ).toMatchObject({
                onDelete: display[expected.onDelete],
                onUpdate: display[expected.onUpdate],
              });
              reach.reach("introspection");
              if (update === "omitted") expect(pgConstraint.confupdtype).toBe("a");
              await pg.exec("ALTER TABLE c ADD COLUMN extra TEXT");
              await native.execute("ALTER TABLE c ADD COLUMN extra TEXT");
              if (checkpoint) await native.harness.path.recycleLog();
              await native.reopen();
              expect(native.harness.path.catalog.tableByName("c")?.foreignKeys[0]).toMatchObject(
                expected,
              );
              const data = decodeCatalogStream(native.harness.path.catalog.encode());
              for (const bad of [{ onUpdate: "unknown" }, { onDelete: "follow" }]) {
                const changed = structuredClone(data);
                const fk = changed.tables.find((t) => t.name === "c")?.foreignKeys[0];
                if (!fk) throw new Error("missing corruption target");
                Object.assign(fk, bad);
                expect(() => Catalog.fromStored(changed)).toThrow(
                  expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
                );
              }
              const duplicateEvent = duplicateUpdate ? "UPDATE" : "DELETE";
              const duplicateAction = spelling[duplicateUpdate ? update : deletion] || "NO ACTION";
              const invalid = `CREATE TABLE dup(id INT REFERENCES p(id) ON ${duplicateEvent} ${duplicateAction} ON ${duplicateEvent} CASCADE)`;
              let pgState: string | undefined;
              try {
                await pg.exec(invalid);
              } catch (error) {
                if (error instanceof Error && "code" in error) pgState = String(error.code);
                else throw error;
              }
              expect(pgState).toBe("42601");
              expect(() => native.lowerer.lower(invalid, native.context())).toThrow(
                expect.objectContaining({ sqlState: pgState }),
              );
              expect(native.harness.path.catalog.tableByName("dup")).toBeUndefined();
              reach.reach(
                duplicateUpdate ? "duplicate update refused" : "duplicate delete refused",
              );
              reach.reach(`delete ${deletion}`);
              reach.reach(`update ${update}`);
              reach.reach(self ? "self reference" : "external reference");
              reach.reach(named ? "named constraint" : "inline constraint");
              reach.reach(reverse ? "update clause first" : "delete clause first");
              reach.reach(checkpoint ? "checkpoint" : "WAL");
              reach.reach(column === "pid" ? "plain column" : "quoted column");
              reach.reach("reopen");
              reach.reach("invalid mode refused");
            } finally {
              if (created) await pg.exec("DROP TABLE c");
              await native.close();
            }
          },
        ),
        {
          seed,
          numRuns: 20,
          examples: [
            ["omitted", "omitted", false, false, false, false, "pid", false],
            ["noAction", "restrict", true, true, true, true, "a.b", true],
            ["restrict", "cascade", false, true, false, true, "Case", false],
            ["cascade", "setNull", true, false, true, false, "pid", false],
            ["setNull", "setDefault", false, false, false, true, "a.b", true],
            ["setDefault", "noAction", true, true, true, true, "Case", false],
            ["cascade", "cascade", false, false, true, false, "pid", false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
