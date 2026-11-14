/** Semantic admission for the relational catalog extensions, after wire/physical shape checks. */
import type { ExpressionDependencies, ViewDef } from "../access/catalog.js";
import type { CatalogStreamData } from "./catalog-stream.js";
import { CATALOG_NATIVE_GRAMMAR, DURABLE_CODECS, requireTag } from "../durable-format.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { UnsupportedCatalogError } from "./catalog-records.js";
import { validateCatalogDeclared, validateCatalogType } from "./catalog-values.js";
function check(ok: boolean, message: string): asserts ok {
  if (!ok) throw new StoreCorruptionError(message);
}
function ids(values: readonly number[]): void {
  check(
    values.every(
      (value, i) =>
        Number.isSafeInteger(value) && value > 0 && (i === 0 || value > (values[i - 1] ?? 0)),
    ),
    "Invalid ordered catalog dependencies",
  );
}
export function validateRelationalCatalog(data: CatalogStreamData): void {
  const tables = new Map(data.tables.map((table) => [table.id, table]));
  const types = new Map(data.types.map((type) => [type.id, type]));
  const views = new Map(data.views.map((view) => [view.id, view]));
  const sequences = new Map(data.sequences.map((sequence) => [sequence.id, sequence]));
  const columnMaps = new Map(
    data.tables.map((table) => [
      table.id,
      new Map(table.columns.map((column) => [column.id, column])),
    ]),
  );
  const expression = (
    deps: ExpressionDependencies,
    columns: Pick<ReadonlySet<number>, "has">,
  ): void => {
    if (deps.dialectVersion !== CATALOG_NATIVE_GRAMMAR.expressionDialect)
      throw new UnsupportedCatalogError(`expression dialect ${String(deps.dialectVersion)}`);
    ids(deps.columnIds);
    ids(deps.sequenceIds);
    ids(deps.typeIds ?? []);
    check(
      deps.columnIds.every((id) => columns.has(id)) &&
        deps.sequenceIds.every((id) => sequences.has(id)) &&
        (deps.typeIds ?? []).every((id) => types.has(id)),
      "Dangling expression dependency",
    );
  };
  for (const table of data.tables) {
    check(
      table.managed === undefined || typeof table.managed === "boolean",
      "Invalid table ownership flag",
    );
    const columns = columnMaps.get(table.id);
    check(columns !== undefined, "Missing table columns");
    for (const column of table.columns) {
      if (column.identitySequence !== null) {
        const sequence = sequences.get(column.identitySequence);
        check(
          sequence?.ownedBy?.tableId === table.id && sequence.ownedBy.columnId === column.id,
          "Identity sequence has another owner",
        );
      }
      if (column.autoIncrement !== undefined) {
        requireTag(
          column.autoIncrement,
          DURABLE_CODECS.presentFlag,
          "auto-increment flag",
          StoreCorruptionError,
        );
        check(
          column.identitySequence !== null &&
            (column.type.kind === "int32" ||
              column.type.kind === "int64" ||
              column.type.kind === "float64"),
          "Invalid auto-increment counter",
        );
      }
      if (column.typeId !== undefined) {
        const type = types.get(column.typeId),
          physical = column.type;
        check(
          type !== undefined &&
            physical.kind === "enum" &&
            physical.name === type.name &&
            physical.labels.length === type.labels.length &&
            physical.labels.every((label, i) => label === type.labels[i]),
          "Named column type disagrees with its declaration",
        );
      }
      if (column.allowedValues !== undefined) {
        check(
          column.type.kind === "text" &&
            column.allowedValues.length > 0 &&
            column.allowedValues.every((value) => typeof value === "string") &&
            new Set(column.allowedValues).size === column.allowedValues.length,
          "Invalid closed text set",
        );
      }
      if (column.declared.enumValues !== undefined)
        check(
          column.allowedValues?.length === column.declared.enumValues.length &&
            column.allowedValues.every((value, i) => value === column.declared.enumValues?.[i]),
          "Closed text declaration disagrees with its restriction",
        );
      if (column.defaultDependencies !== undefined) {
        check(
          column.defaultSql !== null || column.identitySequence !== null,
          "Dependencies without a default",
        );
        expression(column.defaultDependencies, columns);
        check(column.defaultDependencies.columnIds.length === 0, "A default reads a column");
        if (column.identitySequence !== null)
          check(
            column.defaultDependencies.sequenceIds.length === 1 &&
              column.defaultDependencies.sequenceIds[0] === column.identitySequence,
            "Identity default names another sequence",
          );
      }
      if (column.generatedDependencies !== undefined) {
        check(column.generatedSql !== null, "Dependencies without a generated expression");
        expression(column.generatedDependencies, columns);
        check(
          !column.generatedDependencies.columnIds.includes(column.id) &&
            column.generatedDependencies.sequenceIds.length === 0 &&
            column.generatedDependencies.columnIds.every(
              (id) => columns.get(id)?.generatedSql === null,
            ),
          "Invalid generated dependencies",
        );
      }
    }
    for (const condition of table.checks)
      if (condition.dependencies !== undefined) {
        expression(condition.dependencies, columns);
        check(
          condition.dependencies.columnIds.length === condition.columnIds.length &&
            condition.columnIds.every((id) => condition.dependencies?.columnIds.includes(id)),
          "CHECK dependencies disagree with its columns",
        );
      }
  }
  for (const sequence of data.sequences)
    if (sequence.ownedBy !== null) {
      const column = tables
        .get(sequence.ownedBy.tableId)
        ?.columns.find((column) => column.id === sequence.ownedBy?.columnId);
      check(column?.identitySequence === sequence.id, "Owned sequence disagrees with its column");
    }
  for (const view of data.views) {
    check(
      view.managed === undefined || typeof view.managed === "boolean",
      "Invalid view ownership flag",
    );
    if (view.outputs !== undefined) {
      check(
        new Set(view.outputs.map((output) => output.name)).size === view.outputs.length,
        "Repeated view output name",
      );
      for (const output of view.outputs) {
        validateCatalogType(output.type);
        validateCatalogDeclared(output.declared, output.type);
        check(typeof output.nullable === "boolean", "Invalid view output nullability");
      }
    }
    const schemas = new Set<string>();
    for (const source of view.sourceSchemas ?? []) {
      requireTag(source.kind, ["table", "view"], "view source kind", StoreCorruptionError);
      const key = `${source.kind}:${String(source.objectId)}`;
      check(!schemas.has(key), "Repeated view source schema");
      schemas.add(key);
      check(
        view.dependencies?.some(
          (dep) => dep.kind === source.kind && dep.objectId === source.objectId,
        ) === true,
        "View source without dependency",
      );
      check(
        Number.isSafeInteger(source.outputCount) && source.outputCount >= 0,
        "Invalid view output prefix",
      );
      if (source.kind === "table") {
        const table = tables.get(source.objectId);
        check(table !== undefined && source.outputCount === 0, "Invalid view table source");
        const unique = new Set(source.columnIds);
        check(
          unique.size === source.columnIds.length &&
            source.columnIds.every(
              (id) => Number.isSafeInteger(id) && id > 0 && id < data.counters.nextColumnId,
            ),
          "Invalid historical view column slots",
        );
        check(
          source.columnIds.every(
            (id) =>
              !data.tables.some(
                (t) => t.id !== source.objectId && t.columns.some((c) => c.id === id),
              ),
          ),
          "Foreign view column slot",
        );
        const dep = view.dependencies.find(
          (dep) => dep.kind === "table" && dep.objectId === source.objectId,
        );
        check(
          dep?.columnIds.every((id) => unique.has(id)) === true,
          "View source misses dependency",
        );
      } else {
        const output = views.get(source.objectId)?.outputs;
        check(
          output !== undefined &&
            source.columnIds.length === 0 &&
            source.outputCount <= output.length,
          "Invalid stored view output prefix",
        );
      }
    }
    if (view.sourceSchemas !== undefined)
      check(
        (view.dependencies ?? []).every(
          (dep) =>
            (dep.kind !== "table" && dep.kind !== "view") ||
            schemas.has(`${dep.kind}:${String(dep.objectId)}`),
        ),
        "View is missing a source schema",
      );
    const seen = new Set<string>();
    for (const dep of view.dependencies ?? []) {
      requireTag(
        dep.kind,
        ["table", "view", "type", "sequence"],
        "dependency kind",
        StoreCorruptionError,
      );
      check(!seen.has(`${dep.kind}:${String(dep.objectId)}`), "Repeated object dependency");
      seen.add(`${dep.kind}:${String(dep.objectId)}`);
      check(
        Number.isSafeInteger(dep.objectId) && dep.objectId > 0,
        "Invalid object dependency identity",
      );
      ids(dep.columnIds);
      if (dep.kind === "table") {
        const table = columnMaps.get(dep.objectId);
        check(
          table !== undefined && dep.columnIds.every((id) => table.has(id)),
          "Dangling view table dependency",
        );
      } else {
        check(dep.columnIds.length === 0, "Column dependencies on a non-table");
        check(
          dep.kind === "view"
            ? views.has(dep.objectId)
            : dep.kind === "sequence"
              ? sequences.has(dep.objectId)
              : types.has(dep.objectId),
          "Dangling view object dependency",
        );
      }
    }
  }
  // Iterative DFS: hostile stored chains cannot turn admission into an untyped stack overflow.
  const active = new Set<number>(),
    complete = new Set<number>();
  for (const view of data.views) {
    if (complete.has(view.id)) continue;
    const stack: Array<{ view: ViewDef; next: number }> = [{ view, next: 0 }];
    active.add(view.id);
    while (stack.length) {
      const frame = stack[stack.length - 1];
      if (!frame) throw new StoreCorruptionError("Missing view frame");
      const dep = frame.view.dependencies?.[frame.next++];
      if (dep === undefined) {
        active.delete(frame.view.id);
        complete.add(frame.view.id);
        stack.pop();
        continue;
      }
      if (dep.kind !== "view" || complete.has(dep.objectId)) continue;
      check(!active.has(dep.objectId), "Cyclic view dependency");
      const next = views.get(dep.objectId);
      check(next !== undefined, "Dangling view");
      stack.push({ view: next, next: 0 });
      active.add(next.id);
    }
  }
}
