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
      expect(printed).toContain(`  ${table.name}: FromRow<{`);
      for (const column of table.columns) expect(printed).toContain(`    ${column.name}: `);
    }
  });

  it("maps each column type onto the type a row actually holds", () => {
    expect(printed).toContain("    placed_at: Date;");
    expect(printed).toContain("    total: number;");
    expect(printed).toContain("    status: string;");
    expect(printed).toContain("    active: boolean;");
  });

  it("widens a nullable column and leaves the others alone", () => {
    expect(printed).toContain("    employee_id: number | null;");
    expect(printed).toContain("    order_id: number;");
  });

  it("declares the two names a snippet is handed", () => {
    expect(printed).toContain('declare const db: import("@minnowdb/client").Minnow<DB>;');
    expect(printed).toContain("declare const database:");
  });

  it("wraps each row so the builders see an insert and update shape too", () => {
    expect(printed).toContain("  orders: FromRow<{");
  });

  it("stays a script, so what it declares is global", () => {
    expect(printed).not.toMatch(/^\s*(import|export)\s/m);
  });
});
