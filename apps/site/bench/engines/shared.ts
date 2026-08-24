/**
 * SQL/DDL helpers and value canonicalization shared by the engine drivers.
 * Descended from the old engine-comparison module; the monolithic four-engine comparison
 * run is gone, but the portable DDL, row conversion, and checksum machinery survives here.
 */
import type { LogicalType } from "@minnowdb/core/block-format";
import type { EntityDefinition, GeneratedBatchColumns } from "../benchmark";

export interface SqlQueryDefinition {
  id: string;
  name: string;
  complexity: "simple" | "moderate" | "complex";
  sql: string;
}

/**
 * Portable aggregation queries that run unchanged on every comparison engine. Kept for the
 * engine-comparison vitest and as ready-made examples for the query page.
 */
export const comparisonQueries: SqlQueryDefinition[] = [
  {
    id: "q1",
    name: "Order count",
    complexity: "simple",
    sql: "SELECT COUNT(*) AS row_count FROM orders",
  },
  {
    id: "q2",
    name: "Revenue by order status",
    complexity: "simple",
    sql: "SELECT status, COUNT(*) AS order_count, ROUND(SUM(total) * 100) / 100 AS revenue FROM orders GROUP BY status ORDER BY status",
  },
  {
    id: "q3",
    name: "Customer segment revenue",
    complexity: "moderate",
    sql: "SELECT c.segment, COUNT(*) AS order_count, ROUND(SUM(o.total) * 100) / 100 AS revenue FROM customers c JOIN orders o ON o.customer_id = c.customer_id GROUP BY c.segment ORDER BY c.segment",
  },
  {
    id: "q4",
    name: "Category line-item revenue",
    complexity: "complex",
    sql: "SELECT p.category, ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct)) * 100) / 100 AS revenue FROM order_items oi JOIN products p ON p.product_id = oi.product_id GROUP BY p.category ORDER BY p.category",
  },
  {
    id: "q5",
    name: "Tax by jurisdiction",
    complexity: "complex",
    sql: "SELECT tj.name, ROUND(SUM(ot.tax_amount) * 100) / 100 AS tax_total FROM order_taxes ot JOIN tax_rates tr ON tr.tax_rate_id = ot.tax_rate_id JOIN tax_jurisdictions tj ON tj.jurisdiction_id = tr.jurisdiction_id GROUP BY tj.name ORDER BY tj.name",
  },
  {
    id: "q6",
    name: "Payment funnel",
    complexity: "complex",
    sql: "SELECT p.status AS payment_status, pt.status AS transaction_status, COUNT(*) AS event_count, ROUND(SUM(pt.amount) * 100) / 100 AS amount FROM payments p JOIN payment_transactions pt ON pt.payment_id = p.payment_id GROUP BY p.status, pt.status ORDER BY p.status, pt.status",
  },
];

export function sqlType(type: LogicalType): string {
  switch (type) {
    case "boolean":
      return "BOOLEAN";
    case "number":
      return "DOUBLE PRECISION";
    case "string":
      return "TEXT";
    case "datetime":
      return "TIMESTAMP";
  }
}

/**
 * The part of a table definition the portable DDL and row pivot need. `EntityDefinition`
 * satisfies it, and so does the write suite's dedicated table, which has no generator.
 */
export interface TableShape {
  name: string;
  primaryKey?: string | undefined;
  columns: ReadonlyArray<{ name: string; type: LogicalType }>;
}

export function createTableSql(entity: TableShape): string {
  const columns = entity.columns.map((column) => {
    const primary = column.name === entity.primaryKey ? " PRIMARY KEY" : "";
    return `${quoteIdentifier(column.name)} ${sqlType(column.type)}${primary}`;
  });
  return `CREATE TABLE ${quoteIdentifier(entity.name)} (${columns.join(", ")})`;
}

/** `("a", "b")` — the column list shared by INSERT statements and read-back projections. */
export function columnList(entity: TableShape): string {
  return entity.columns.map((column) => quoteIdentifier(column.name)).join(", ");
}

export function secondaryIndexSql(entities: readonly EntityDefinition[]): string[] {
  return entities.flatMap((entity) =>
    (entity.secondaryIndexes ?? []).map(
      (column) =>
        `CREATE INDEX ${quoteIdentifier(`idx_${entity.name}_${column}`)} ON ${quoteIdentifier(entity.name)} (${quoteIdentifier(column)})`,
    ),
  );
}

/**
 * Builds the workload's secondary indexes through an engine's own DDL path and reports that
 * cost separately from row insertion. Keeping the loop here means every driver consumes the
 * exact same index list; a driver cannot quietly drift by copying only part of the workload DDL.
 */
export async function createSecondaryIndexes(
  entities: readonly EntityDefinition[],
  execute: (sql: string) => unknown,
): Promise<number> {
  const started = performance.now();
  for (const sql of secondaryIndexSql(entities)) await execute(sql);
  return performance.now() - started;
}

export function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

export function rowsFromColumns(
  entity: TableShape,
  columns: GeneratedBatchColumns,
): Array<Array<boolean | number | string | null>> {
  const rowCount = columns[entity.columns[0]?.name ?? ""]?.length ?? 0;
  return Array.from({ length: rowCount }, (_, rowIndex) =>
    entity.columns.map((column) => {
      const value = columns[column.name]?.[rowIndex] ?? null;
      return value instanceof Date ? value.toISOString() : value;
    }),
  );
}

export function estimateBatchBytes(columns: GeneratedBatchColumns): number {
  let total = 0;
  for (const values of Object.values(columns)) {
    for (const value of values) {
      if (value === null) total += 1;
      else if (typeof value === "boolean") total += 1;
      else if (typeof value === "number" || value instanceof Date) total += 8;
      else total += new TextEncoder().encode(value).byteLength;
    }
  }
  return total;
}

const datetimePattern = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z?$/;

/**
 * Puts one value into the shape used for cross-engine comparison and previews:
 * BigInt counts become numbers, Date objects and the engines' textual datetime spellings
 * all become `YYYY-MM-DDTHH:MM:SS.mmmZ`. The generator only ever writes UTC wall-clock
 * datetimes, so this normalization is lossless for generated data.
 */
export function canonicalizeValue(value: unknown): unknown {
  if (typeof value === "bigint") return Number(value);
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string" && datetimePattern.test(value)) {
    const spaced = value.replace(" ", "T").replace(/Z$/, "");
    const [datePart, fraction = ""] = spaced.split(".");
    const millis = fraction.padEnd(3, "0").slice(0, 3);
    return `${datePart ?? spaced}.${millis}Z`;
  }
  return value === undefined ? null : value;
}

export function canonicalizeRow(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, canonicalizeValue(value)]),
  );
}

/** Order-insensitive over object key order, tolerant of float noise in the last bits. */
export function resultChecksum(rows: readonly unknown[]): number {
  const normalized = JSON.stringify(rows, (_key, value: unknown) => {
    if (typeof value === "bigint") return Number(value);
    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.round(value * 1_000_000) / 1_000_000;
    }
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      return Object.fromEntries(
        Object.entries(value as Record<string, unknown>).sort(([left], [right]) =>
          left.localeCompare(right),
        ),
      );
    }
    return value;
  });
  let hash = 2_166_136_261;
  for (let index = 0; index < normalized.length; index += 1) {
    hash ^= normalized.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return hash >>> 0;
}

/** Unwraps engine row objects that expose toJSON (PGlite result rows do). */
export function normalizeRows(rows: unknown[]): Array<Record<string, unknown>> {
  return rows.map((row) => {
    if (row !== null && typeof row === "object" && "toJSON" in row) {
      const toJSON = (row as { toJSON?: () => unknown }).toJSON;
      if (typeof toJSON === "function") return toJSON.call(row) as Record<string, unknown>;
    }
    return row as Record<string, unknown>;
  });
}
