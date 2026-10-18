import {
  childExpressions,
  compileCheckExpression,
  expressionColumns,
  validateDefaultExpression,
  type Expression,
} from "./query.js";
import type { AnyTable, SchemaDefinition, TableCheck } from "./schema.js";

/**
 * The SQL a table declaration carries — CHECK conditions, stored generated expressions, and DEFAULT
 * expressions — compiled and checked by the engine. The schema DSL never parses SQL, so a page
 * that only declares tables does not download the compiler; these checks run when a declaration
 * reaches the engine (`migrate()`, `createTable`, `CREATE TABLE`, `ALTER TABLE`).
 */

const volatileGeneratedFunctions = new Set([
  "CURRENT_DATE",
  "CURRENT_TIMESTAMP",
  "LOCALTIME",
  "RANDOM",
  "GEN_RANDOM_UUID",
  "NEXTVAL",
  "CURRVAL",
]);

/** Splits `alias.column` into its qualifier (when present) and the column name. */
function splitReference(reference: string): { qualifier: string | undefined; column: string } {
  const pieces = reference.split(".");
  return {
    qualifier: pieces.length > 1 ? pieces.slice(0, -1).join(".") : undefined,
    column: pieces.at(-1) ?? reference,
  };
}

/** Compiles and validates one immutable, row-local generated-column expression. */
export function compileGeneratedColumnExpression(
  tableName: string,
  columnName: string,
  sql: string,
  columns: ReadonlyArray<{
    readonly name: string;
    readonly generatedValue?: { readonly kind: "stored"; readonly sql: string };
  }>,
): Expression {
  if (sql.length === 0 || sql.trim() !== sql) {
    throw new TypeError(
      `Generated SQL must be a trimmed non-empty expression: ${tableName}.${columnName}`,
    );
  }
  // Refuses aggregates, windows, and parameters; what is left to rule out is anything that is not
  // a pure function of the row.
  const expression = compileCheckExpression(sql, `generated ${tableName}.${columnName}`);
  const inspect = (node: Expression): void => {
    if (node.kind === "subquery" || node.kind === "exists") {
      throw new TypeError(
        `Generated column ${tableName}.${columnName} must be an immutable expression over sibling columns`,
      );
    }
    if (node.kind === "call" && volatileGeneratedFunctions.has(node.name)) {
      throw new TypeError(
        `Generated column ${tableName}.${columnName} cannot call volatile function ${node.name}`,
      );
    }
    childExpressions(node).forEach(inspect);
  };
  inspect(expression);
  for (const reference of expressionColumns(expression)) {
    const { qualifier, column: referencedName } = splitReference(reference);
    if (qualifier !== undefined && qualifier !== tableName) {
      throw new TypeError(
        `Generated column ${tableName}.${columnName} references another table: ${reference}`,
      );
    }
    const referenced = columns.find(({ name }) => name === referencedName);
    if (referenced === undefined) {
      throw new TypeError(
        `Generated column ${tableName}.${columnName} names an unknown column: ${referencedName}`,
      );
    }
    if (referencedName === columnName || referenced.generatedValue !== undefined) {
      throw new TypeError(
        `Generated column ${tableName}.${columnName} cannot reference a generated column: ${referencedName}`,
      );
    }
  }
  return expression;
}

/**
 * Compiles one CHECK and confirms it reads only this table's own columns. Returns the columns it
 * reads, unqualified and in first-use order.
 */
export function validateCheckColumns(
  tableName: string,
  check: TableCheck,
  hasColumn: (name: string) => boolean,
): string[] {
  const read: string[] = [];
  for (const reference of expressionColumns(compileCheckExpression(check.sql, check.name))) {
    const { qualifier, column } = splitReference(reference);
    if (qualifier !== undefined && qualifier !== tableName) {
      throw new TypeError(`CHECK ${check.name} references another table: ${reference}`);
    }
    if (!hasColumn(column)) {
      throw new TypeError(`CHECK ${check.name} names an unknown column: ${column}`);
    }
    if (!read.includes(column)) read.push(column);
  }
  return read;
}

/**
 * The columns of `tableName` that a stored row expression (a CHECK or a generated column) reads,
 * for the published catalog. Undefined when the text no longer compiles or names another table:
 * a planner then knows it cannot tell, rather than being told "none".
 */
export function storedExpressionColumns(
  tableName: string,
  sql: string,
  label: string,
): string[] | undefined {
  let references: string[];
  try {
    references = expressionColumns(compileCheckExpression(sql, label));
  } catch {
    return undefined;
  }
  const read: string[] = [];
  for (const reference of references) {
    const { qualifier, column } = splitReference(reference);
    if (qualifier !== undefined && qualifier !== tableName) return undefined;
    if (!read.includes(column)) read.push(column);
  }
  return read;
}

/**
 * Checks every SQL expression one declared table carries, exactly as `CREATE TABLE` would: each
 * generated column is immutable and reads only ordinary sibling columns, each DEFAULT expression
 * is variable-free and produces the column's type, and each CHECK compiles and reads only this
 * table's columns.
 */
export function validateTableExpressions(definition: AnyTable): void {
  const entries = Object.entries(definition.columns);
  const siblings = entries.map(([name, column]) => ({
    name,
    ...(column.generatedSpec === undefined ? {} : { generatedValue: column.generatedSpec }),
  }));
  for (const [columnName, column] of entries) {
    if (column.generatedSpec !== undefined) {
      compileGeneratedColumnExpression(
        definition.name,
        columnName,
        column.generatedSpec.sql,
        siblings,
      );
    }
    if (column.defaultSpec?.kind === "expression") {
      validateDefaultExpression(column.defaultSpec.sql, {
        name: `${definition.name}.${columnName}`,
        type: column.type,
        ...(column.sqlDomain === undefined ? {} : { sqlDomain: column.sqlDomain }),
      });
    }
  }
  for (const check of definition.checks) {
    validateCheckColumns(definition.name, check, (name) => Object.hasOwn(definition.columns, name));
  }
}

/** `validateTableExpressions` for every table a schema declares, before anything is applied. */
export function validateSchemaExpressions(definition: SchemaDefinition<readonly AnyTable[]>): void {
  for (const table of definition.tables) validateTableExpressions(table);
}
