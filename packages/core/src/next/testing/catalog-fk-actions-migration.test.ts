/** Managed-schema declarations cannot silently replace a native explicit action with follow. */
import { expect, it } from "vitest";
import { column, schema, table } from "../../engine/schema.js";
import { migrateCatalog } from "../owner/catalog-migration.js";
import { StorageCore } from "./storage-core.js";

for (const mode of [
  "omitted",
  "omitted-delete",
  "CASCADE",
  "NO ACTION",
  "RESTRICT",
  "SET NULL",
  "SET DEFAULT",
])
  it(`preserves native ${mode} during managed schema comparison`, async () => {
    const core = await new StorageCore().open();
    try {
      const implicit = mode === "omitted" || mode === "omitted-delete";
      for (const sql of [
        "CREATE TABLE p(id INT PRIMARY KEY)",
        `CREATE TABLE c(id INT PRIMARY KEY,pid INT DEFAULT 0 REFERENCES p(id) ${mode === "omitted-delete" ? "" : "ON DELETE RESTRICT"} ${implicit ? "" : `ON UPDATE ${mode}`})`,
      ]) {
        const lowered = core.lowerer.lower(sql, core.context());
        if (lowered.kind !== "ddl" || lowered.change?.kind !== "createTable")
          throw new Error("missing managed CREATE input");
        await core.change({ ...lowered.change, table: { ...lowered.change.table, managed: true } });
      }
      const before = core.harness.path.catalog.encode(),
        definition = schema([
          table("p", { id: column.integer().unique() }),
          table("c", {
            id: column.integer().unique(),
            pid: column.integer().nullable().default(0).references("p", "id"),
          }),
        ]),
        work = migrateCatalog(core.harness.path, core.lowerer, core.harness.governor, definition);
      if (implicit) await expect(work).resolves.toMatchObject({ steps: [] });
      else await expect(work).rejects.toThrow("FOREIGN KEY cannot change");
      expect(core.harness.path.catalog.encode()).toEqual(before);
    } finally {
      await core.close();
    }
  });
