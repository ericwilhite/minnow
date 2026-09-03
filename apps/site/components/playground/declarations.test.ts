import { describe, expect, it } from "vitest";
import { retailSchema } from "@/lib/dataset/retail";
import { PLAYGROUND_DATABASE } from "@/lib/dataset/load";
import { SETUP_SNIPPET, playgroundDeclarations } from "./declarations";

/**
 * The editor's promises are only worth something if they come from the schema the database was
 * built with. These check the printing; the inference itself is checked where it matters, by the
 * snippet suite typechecking real code against these declarations and the shipped `./schema`.
 */
describe("the playground's ambient declarations", () => {
  const printed = playgroundDeclarations(retailSchema);

  it("derives every table from the shipped schema module, keeping its description", () => {
    expect(printed).toContain('typeof import("./schema").retailDefinition');
    expect(printed).toContain('import("@minnowdb/kysely").InferKyselyDatabase<RetailSchema>');
    for (const table of retailSchema) {
      expect(printed).toContain(
        `  /** ${table.description} */\n  ${table.name}: RetailTables["${table.name}"];`,
      );
    }
  });

  it("declares the two names a snippet is handed, both typed by that schema", () => {
    expect(printed).toContain('declare const db: import("kysely").Kysely<DB>;');
    expect(printed).toContain(
      'declare const database: import("@minnowdb/core/client").MinnowDatabaseClient<RetailSchema>;',
    );
  });

  it("stays a script, so what it declares is global", () => {
    expect(printed).not.toMatch(/^\s*(import|export)\s/m);
  });

  it("shows the setup this page runs: the stable database name and the declared schema", () => {
    expect(SETUP_SNIPPET).toContain(`name: "${PLAYGROUND_DATABASE}"`);
    expect(SETUP_SNIPPET).toContain("schema: retailDefinition,");
    expect(SETUP_SNIPPET).toContain("await database.migrate();");
    expect(SETUP_SNIPPET).not.toContain("incompatibleSchema");
  });
});
