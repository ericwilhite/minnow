/** Public schema introspection over stable native identities, shared by migration planning. */
import type { Catalog as SchemaCatalog, CatalogColumn } from "../../engine/catalog.js";
import type { ColumnDefault } from "../../storage/types.js";
import { compileDefaultExpression, compileWithDeclaredTypes } from "../../engine/query.js";
import type { CatalogView, ColumnDef } from "../access/catalog.js";
import { readDefaultLiteral } from "../access/catalog-default-source.js";
import { int64At } from "../vector/int64.js";
import { decodeUtf8 } from "../vector/varlen.js";
import { formatDate, PG_EPOCH_UNIX_MICROS } from "../temporal.js";
import { formatSqlInterval } from "../temporal-text.js";
import { int128At } from "../vector/int128.js";

function backfill(column: ColumnDef): boolean | number | string | Date | undefined {
  const value = column.missing;
  if (!value || (value.validity !== null && ((value.validity[0] ?? 0) & 1) === 0)) return undefined;
  const data = value.data;
  switch (data.layout) {
    case "bool":
      return data.values[0] === 1;
    case "i32":
      return column.type.kind === "date" ? formatDate(data.values[0] ?? 0) : data.values[0];
    case "f32":
    case "f64":
      return data.values[0];
    case "varlen":
      return decodeUtf8(data, 0);
    case "int64": {
      const n = int64At(data.values, 0);
      return column.type.kind === "timestamp"
        ? new Date(Number((n + PG_EPOCH_UNIX_MICROS) / 1000n))
        : Number.isSafeInteger(Number(n))
          ? Number(n)
          : n.toString();
    }
    case "decimal": {
      const n =
        data.coefficients.repr === "wide"
          ? (data.coefficients.values[0] ?? 0n)
          : data.coefficients.repr === "int128"
            ? int128At(data.coefficients.limbs, 0)
            : int64At(data.coefficients, 0);
      const scale = data.scales?.[0] ?? data.scale ?? 0,
        digits = (n < 0n ? -n : n).toString().padStart(scale + 1, "0"),
        end = digits.length - scale;
      return `${n < 0n ? "-" : ""}${scale === 0 ? digits : `${digits.slice(0, end)}.${digits.slice(end)}`}`;
    }
    case "interval":
      return formatSqlInterval(data.months[0] ?? 0, data.days[0] ?? 0, int64At(data.micros, 0));
    case "fixed16":
      return [...data.bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  }
}
function defaultOf(column: ColumnDef): ColumnDefault | undefined {
  if (column.identitySequence !== null) return { kind: "autoincrement" };
  if (column.defaultSql === null) return undefined;
  if (column.defaultSource === "expression") return { kind: "expression", sql: column.defaultSql };
  if (column.defaultSource !== undefined)
    return { kind: "literal", value: readDefaultLiteral(column.defaultSource, column.defaultSql) };
  const expression = compileWithDeclaredTypes(() =>
    compileDefaultExpression(column.defaultSql ?? "NULL"),
  );
  if (
    expression.kind === "literal" &&
    expression.value !== null &&
    (typeof expression.value === "string" ||
      typeof expression.value === "number" ||
      typeof expression.value === "boolean" ||
      expression.value instanceof Date)
  )
    return { kind: "literal", value: expression.value };
  return { kind: "expression", sql: column.defaultSql };
}
export function inspectCatalog(catalog: CatalogView): SchemaCatalog {
  const column = (def: ColumnDef, siblings: readonly ColumnDef[]): CatalogColumn => {
    const value = defaultOf(def),
      missing = backfill(def);
    return {
      id: String(def.id),
      name: def.name,
      ...def.declared,
      nullable: def.nullable,
      isAutoIncrementing: def.identitySequence !== null,
      ...(value === undefined ? {} : { defaultValue: value }),
      ...(missing === undefined ? {} : { backfill: missing }),
      ...(def.allowedValues === undefined ? {} : { enumValues: def.allowedValues }),
      ...(def.generatedSql === null
        ? {}
        : {
            generatedValue: {
              kind: "stored",
              sql: def.generatedSql,
              ...(def.generatedDependencies
                ? {
                    columns: def.generatedDependencies.columnIds.map(
                      (id) => siblings.find((column) => column.id === id)?.name ?? "",
                    ),
                  }
                : {}),
            },
          }),
    };
  };
  return {
    tables: catalog.tables.map((table) => {
      const primary = table.indexes.find((index) => index.id === table.primaryKey);
      return {
        name: table.name,
        managed: table.managed === true,
        columns: table.columns.map((def) => column(def, table.columns)),
        triggers: catalog.triggers
          .filter((trigger) => trigger.tableId === table.id)
          .map((trigger) => ({
            id: String(trigger.id),
            name: trigger.name,
            event: trigger.event,
            timing: trigger.timing,
          })),
        ...(primary === undefined
          ? {}
          : {
              primaryKeyColumnIds: primary.columnIds.map(String),
              ...(primary.columnIds.length === 1
                ? { uniqueKeyColumnId: String(primary.columnIds[0]) }
                : {}),
            }),
        checks: table.checks.map((check) => ({
          name: check.name,
          sql: check.conditionSql,
          columns: check.columnIds.map(
            (id) => table.columns.find((column) => column.id === id)?.name ?? "",
          ),
        })),
        foreignKeys: table.foreignKeys.map((key) => {
          const parent = catalog.table(key.parentTableId),
            index = parent?.indexes.find((index) => index.id === key.parentIndexId);
          return {
            name: key.name,
            columns: key.columnIds.map(
              (id) => table.columns.find((column) => column.id === id)?.name ?? "",
            ),
            parentTable: parent?.name ?? key.parentName ?? `dropped:${String(key.parentTableId)}`,
            parentColumns:
              index?.columnIds.map(
                (id) => parent?.columns.find((column) => column.id === id)?.name ?? "",
              ) ??
              key.parentColumnNames ??
              [],
            onDelete:
              key.onDelete === "setNull"
                ? ("set null" as const)
                : key.onDelete === "setDefault"
                  ? ("set default" as const)
                  : key.onDelete === "noAction"
                    ? ("no action" as const)
                    : key.onDelete,
            onUpdate:
              key.onUpdate === "setNull"
                ? ("set null" as const)
                : key.onUpdate === "setDefault"
                  ? ("set default" as const)
                  : key.onUpdate === "noAction"
                    ? ("no action" as const)
                    : key.onUpdate,
            enforced: key.enforced,
          };
        }),
        indexes: table.indexes
          .filter((index) => index.role === "secondary" || index.role === "unique")
          .map((index) => ({
            name: index.name,
            unique: index.unique,
            state: index.invalid ? ("invalid" as const) : ("ready" as const),
            columns: index.columnIds.map((id, i) => ({
              name: table.columns.find((column) => column.id === id)?.name ?? "",
              direction: index.directions?.[i] ?? ("asc" as const),
            })),
          })),
      };
    }),
    views: catalog.views.map((view) => ({
      name: view.name,
      sql: view.bodySql,
      managed: view.managed === true,
      columns: (view.outputs ?? []).map((output, i) => ({
        id: `view:${String(view.id)}:${String(i)}`,
        name: output.name,
        ...output.declared,
        nullable: output.nullable,
        isAutoIncrementing: false,
      })),
    })),
  };
}
