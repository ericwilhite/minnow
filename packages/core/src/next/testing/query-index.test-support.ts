/** Native SQL access-plan controls for the indexed/scan differential campaigns. */
import type { CatalogView, TableDef } from "../access/catalog.js";
import type { Rel } from "../ir/ir.js";
export function operators(rel: Rel): string[] {
  return [
    rel.op,
    ...("input" in rel ? operators(rel.input) : []),
    ...("left" in rel ? operators(rel.left) : []),
    ...("right" in rel ? operators(rel.right) : []),
    ...("inputs" in rel ? rel.inputs.flatMap(operators) : []),
  ];
}
export function withoutIndexes(catalog: CatalogView): CatalogView {
  const strip = (t: TableDef | undefined) =>
    t ? { ...t, indexes: [], primaryKey: null } : undefined;
  return new Proxy(catalog, {
    get(target, key) {
      if (key === "tables") return target.tables.map(strip);
      if (key === "table") return (id: number) => strip(target.table(id));
      if (key === "tableByName") return (name: string) => strip(target.tableByName(name));
      const value = Reflect.get(target, key, target) as unknown;
      return typeof value === "function" ? (value.bind(target) as unknown) : value;
    },
  });
}
