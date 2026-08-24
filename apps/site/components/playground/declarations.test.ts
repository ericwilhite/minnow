import { describe, expect, it } from "vitest";
import { retailSchema } from "@/lib/dataset/retail";
import { playgroundDeclarations } from "./declarations";

/**
 * The editor's promises are only worth something if they come from the schema the database was
 * built with. These check the printing, not the schema — the schema is checked by being the
 * thing `migrate()` runs.
 */
describe("the playground's ambient declarations", () => {
  const printed = playgroundDeclarations(retailSchema);

  it("names every table and every column", () => {
    for (const table of retailSchema) {
      expect(printed).toContain(`  ${table.name}: {`);
      for (const column of table.columns) expect(printed).toContain(`    ${column.name}: `);
    }
  });

  it("maps each column type onto the type a row actually holds", () => {
    expect(printed).toContain("placed_at: MinnowColumnType<Date, Date, Date, Date>;");
    expect(printed).toContain("total: MinnowColumnType<number, number, number, number>;");
    expect(printed).toContain("status: MinnowColumnType<string, string, string, string>;");
    expect(printed).toContain("active: MinnowColumnType<boolean, boolean, boolean, boolean>;");
  });

  it("widens a nullable column and leaves the others alone", () => {
    expect(printed).toContain(
      "employee_id: MinnowColumnType<number | null, number | null | undefined, number | null, number | null>;",
    );
    expect(printed).toContain("order_id: MinnowColumnType<number, number, never, number>;");
  });

  it("declares the two names a snippet is handed", () => {
    expect(printed).toContain('declare const db: import("kysely").Kysely<DB>;');
    expect(printed).toContain("declare const database:");
  });

  it("gives Kysely separate select, insert, update, and operand shapes", () => {
    expect(printed).toContain("  orders: {");
    expect(printed).toContain("order_id: MinnowColumnType<number, number, never, number>;");
  });

  it("stays a script, so what it declares is global", () => {
    expect(printed).not.toMatch(/^\s*(import|export)\s/m);
  });
});
