/**
 * The in-memory catalog's foreign keys (review B3-p2a-2 #2): `"primary"` and `{ unique }` name a
 * key of the parent the reference names, and a missing key is refused with `CatalogChangeError`,
 * as the write path's catalog does (write/catalog.ts).
 */
import { describe, expect, it } from "vitest";
import { CatalogChangeError, type TableInput } from "../access/catalog.js";
import { MemoryCatalog } from "./memory-catalog.js";

const id: TableInput["columns"][number] = {
  name: "id",
  type: { kind: "int32", bits: 32 },
  declared: { type: "number", integer: true, declaredTypeName: "INTEGER" },
  nullable: false,
  defaultSql: null,
  generatedSql: null,
  identitySequence: null,
  missing: null,
};

const table = (name: string, extra: Partial<TableInput> = {}): TableInput => ({
  name,
  columns: [id],
  primaryKey: [0],
  unique: [],
  foreignKeys: [],
  checks: [],
  ...extra,
});

const reference = (
  parentTableId: number | "self",
  parentIndexId: TableInput["foreignKeys"][number]["parentIndexId"],
): Partial<TableInput> => ({
  foreignKeys: [
    {
      name: "fk",
      columns: [0],
      parentTableId,
      parentIndexId,
      onDelete: "restrict",
      enforced: true,
    },
  ],
});

describe("memory catalog foreign keys", () => {
  it("resolves another table's primary key against that table", () => {
    const catalog = new MemoryCatalog();
    const parent = catalog.apply({ kind: "createTable", table: table("p") }).id ?? 0;
    const child =
      catalog.apply({ kind: "createTable", table: table("c", reference(parent, "primary")) }).id ??
      0;
    const fk = catalog.view.table(child)?.foreignKeys[0];
    expect(fk?.parentTableId).toBe(parent);
    expect(fk?.parentIndexId).toBe(catalog.view.table(parent)?.primaryKey);
  });

  it("resolves a self reference against the table being defined", () => {
    const catalog = new MemoryCatalog();
    const created =
      catalog.apply({ kind: "createTable", table: table("tree", reference("self", "primary")) })
        .id ?? 0;
    const def = catalog.view.table(created);
    expect(def?.foreignKeys[0]?.parentTableId).toBe(created);
    expect(def?.foreignKeys[0]?.parentIndexId).toBe(def?.primaryKey);
  });

  it("refuses a key the parent lacks, a self reference included", () => {
    const catalog = new MemoryCatalog();
    const parent = catalog.apply({ kind: "createTable", table: table("p") }).id ?? 0;
    expect(() =>
      catalog.apply({
        kind: "createTable",
        table: table("c", reference(parent, { unique: "none" })),
      }),
    ).toThrow(CatalogChangeError);
    expect(() =>
      catalog.apply({
        kind: "createTable",
        table: table("s", reference("self", { unique: "missing" })),
      }),
    ).toThrow(CatalogChangeError);
    expect(() =>
      catalog.apply({
        kind: "createTable",
        table: table("k", { primaryKey: null, ...reference("self", "primary") }),
      }),
    ).toThrow(CatalogChangeError);
  });
});
