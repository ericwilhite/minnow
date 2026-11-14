/** The one input-position → durable-column-identity boundary, shared with lowering's test catalog. */
import {
  CatalogChangeError,
  type ExpressionDependencies,
  type InputExpressionDependencies,
  type TableDef,
} from "./catalog.js";
import { CATALOG_NATIVE_GRAMMAR } from "../durable-format.js";
export function resolveDependencies(
  input: InputExpressionDependencies,
  ids: readonly number[],
): ExpressionDependencies {
  if (input.dialectVersion !== CATALOG_NATIVE_GRAMMAR.expressionDialect)
    throw new CatalogChangeError("Unsupported expression dialect");
  const columnIds = input.columns
    .map((position) => {
      const id = ids[position];
      if (!Number.isSafeInteger(position) || position < 0 || id === undefined)
        throw new CatalogChangeError("Unknown expression column position");
      return id;
    })
    .sort((a, b) => a - b);
  if (columnIds.some((id, i) => i > 0 && id === columnIds[i - 1]))
    throw new CatalogChangeError("Repeated expression dependency");
  const sequenceIds = [...input.sequenceIds].sort((a, b) => a - b);
  if (
    sequenceIds.some(
      (id, i) => !Number.isSafeInteger(id) || id <= 0 || (i > 0 && id === sequenceIds[i - 1]),
    )
  )
    throw new CatalogChangeError("Invalid sequence dependency");
  const typeIds = input.typeIds?.slice().sort((a, b) => a - b);
  if (
    typeIds?.some(
      (id, i) => !Number.isSafeInteger(id) || id <= 0 || (i > 0 && id === typeIds[i - 1]),
    )
  )
    throw new CatalogChangeError("Invalid type dependency");
  return {
    dialectVersion: input.dialectVersion,
    columnIds,
    sequenceIds,
    ...(typeIds?.length ? { typeIds } : {}),
  };
}

/** Object references in any stored rule count even if its current IR folds to a constant. */
export function tableUsesObject(
  table: TableDef,
  kind: "type" | "sequence",
  id: number,
  typeName?: string,
): boolean {
  const uses = (deps: ExpressionDependencies | undefined) =>
    (kind === "type" ? deps?.typeIds : deps?.sequenceIds)?.includes(id) ?? false;
  return (
    table.columns.some(
      (column) =>
        (kind === "type"
          ? column.typeId === id ||
            (typeName !== undefined && column.type.kind === "enum" && column.type.name === typeName)
          : column.identitySequence === id) ||
        uses(column.defaultDependencies) ||
        uses(column.generatedDependencies),
    ) || table.checks.some((check) => uses(check.dependencies))
  );
}
