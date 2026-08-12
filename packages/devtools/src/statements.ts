import { compileStatement } from "@minnowdb/core";

/**
 * What a statement will do, read off the compiled plan rather than the text. Matching on the
 * compiler's own discriminant is the only way to be sure: a statement starting with the letters
 * `select` can still be something else, and one mentioning `delete` in a string literal is not.
 */
export type StatementIntent =
  | { kind: "select" }
  | { kind: "insert"; table: string; columns: string[]; rowCount: number }
  | { kind: "update"; table: string; columns: string[]; filtered: boolean }
  | { kind: "delete"; table: string; filtered: boolean };

/** Throws the compiler's own `SqlCompileError` — position included — when the SQL is bad. */
export function classifyStatement(sql: string): StatementIntent {
  const statement = compileStatement(sql);
  switch (statement.kind) {
    case "insert":
      return {
        kind: "insert",
        table: statement.table,
        columns: [...statement.columns],
        rowCount: statement.rows.length,
      };
    case "update":
      return {
        kind: "update",
        table: statement.table,
        columns: statement.assignments.map(({ column }) => column),
        filtered: statement.predicates.length > 0,
      };
    case "delete":
      return { kind: "delete", table: statement.table, filtered: statement.predicates.length > 0 };
    default:
      return { kind: "select" };
  }
}

export function changesData(intent: StatementIntent): boolean {
  return intent.kind !== "select";
}

export interface StatementSummary {
  /** One line naming the operation and its target, for the confirmation heading. */
  title: string;
  /** Label/value pairs spelling out what is about to happen. */
  facts: Array<[string, string]>;
  /** Set when the statement is unbounded, so the confirmation can say so plainly. */
  warning?: string;
}

const plural = (count: number, noun: string): string =>
  `${String(count)} ${noun}${count === 1 ? "" : "s"}`;

/** The confirmation copy. Every change is described before it runs, never after. */
export function summarize(intent: StatementIntent): StatementSummary {
  switch (intent.kind) {
    case "insert":
      return {
        title: `Insert ${plural(intent.rowCount, "row")} into ${intent.table}`,
        facts: [
          ["table", intent.table],
          ["columns", intent.columns.join(", ")],
          ["rows", String(intent.rowCount)],
        ],
      };
    case "update":
      return {
        title: `Update ${intent.filtered ? "matching rows" : "every row"} in ${intent.table}`,
        facts: [
          ["table", intent.table],
          ["sets", intent.columns.join(", ")],
          ["filter", intent.filtered ? "the statement's WHERE clause" : "none"],
        ],
        ...(intent.filtered ? {} : { warning: "No WHERE clause — this updates every row." }),
      };
    case "delete":
      return {
        title: `Delete ${intent.filtered ? "matching rows" : "every row"} from ${intent.table}`,
        facts: [
          ["table", intent.table],
          ["filter", intent.filtered ? "the statement's WHERE clause" : "none"],
        ],
        ...(intent.filtered ? {} : { warning: "No WHERE clause — this deletes every row." }),
      };
    default:
      return { title: "Run query", facts: [] };
  }
}

/** The message shown when `permissions.write` is off and a statement would change data. */
export function writeBlockedMessage(intent: StatementIntent): string {
  return `Writes are turned off in these devtools, so this ${intent.kind.toUpperCase()} was not run.`;
}
