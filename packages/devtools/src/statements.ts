import { compileQuery, compileStatement, type CompiledStatement } from "@minnowdb/core/query";
import { sqlIdentifier } from "./sql/literal.js";

/**
 * What a statement will do, read off the compiled plan rather than the text. Matching on the
 * compiler's own discriminant is the only way to be sure: a statement starting with the letters
 * `select` can still be something else, and one mentioning `delete` in a string literal is not.
 */
export type StatementIntent =
  /** `limited` says whether the query bounds itself; the console caps one that does not. */
  | { kind: "select"; limited: boolean }
  /** `SET`, `RESET`, and `SHOW`: session settings, never data. Runs without review or the write gate. */
  | { kind: "session"; operation: "set" | "show" }
  | { kind: "insert"; table: string; columns: string[]; rowCount: number }
  | { kind: "update"; table: string; columns: string[]; filtered: boolean }
  | { kind: "delete"; table: string; filtered: boolean }
  | {
      kind: "execute";
      operation: Exclude<
        CompiledStatement["kind"],
        "select" | "insert" | "update" | "delete" | "set" | "show"
      >;
      summary: StatementSummary;
      /**
       * Whether the statement is described and confirmed before it runs. Off only for `BEGIN` and
       * `COMMIT`: neither changes anything by itself, and every write inside the transaction was
       * already confirmed one statement at a time. `ROLLBACK` discards those, so it stays on.
       */
      confirm: boolean;
    };

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
    case "select":
      return { kind: "select", limited: compileQuery(sql).limit !== undefined };
    case "create-table":
      return executeIntent(statement.kind, {
        title: `Create table ${statement.table}`,
        facts: [
          ["table", statement.table],
          ["columns", String(statement.columns.length)],
        ],
        confirmLabel: "Create table",
      });
    case "create-table-as":
      return executeIntent(statement.kind, {
        title: `Create table ${statement.table} from a query`,
        facts: [
          ["table", statement.table],
          ["source", "query result"],
        ],
        confirmLabel: "Create table",
      });
    case "create-enum":
      return executeIntent(statement.kind, {
        title: `Create enum type ${statement.name}`,
        facts: [
          ["type", statement.name],
          ["values", statement.values.join(", ")],
        ],
        confirmLabel: "Create type",
      });
    case "set":
    case "show":
      return { kind: "session", operation: statement.kind };
    case "create-sequence":
      return executeIntent(statement.kind, {
        title: `Create sequence ${statement.name}`,
        facts: [["sequence", statement.name]],
        confirmLabel: "Create sequence",
      });
    case "create-index":
      return executeIntent(statement.kind, {
        title: `Create${statement.unique === true ? " unique" : ""} index ${statement.index}`,
        facts: [
          ["index", statement.index],
          ["table", statement.table],
          [
            "columns",
            statement.columns
              .map((column) => `${column.name} ${column.direction.toUpperCase()}`)
              .join(", "),
          ],
        ],
        confirmLabel: "Create index",
      });
    case "drop-index":
      return executeIntent(statement.kind, {
        title: `Drop index ${statement.index}`,
        facts: [["index", statement.index]],
        warning: "The index will be removed. Queries keep working, but may scan more data.",
        confirmLabel: "Drop index",
        destructive: true,
      });
    case "create-trigger":
      return executeIntent(statement.kind, {
        title: `Create trigger ${statement.trigger.name}`,
        facts: [
          ["trigger", statement.trigger.name],
          ["table", statement.table],
          [
            "event",
            `${statement.trigger.timing.toUpperCase()} ${statement.trigger.event.toUpperCase()}`,
          ],
        ],
        confirmLabel: "Create trigger",
      });
    case "drop-trigger":
      return executeIntent(statement.kind, {
        title: `Drop trigger ${statement.name}`,
        facts: [["trigger", statement.name]],
        warning: "Future writes will no longer run this trigger.",
        confirmLabel: "Drop trigger",
        destructive: true,
      });
    case "drop-table":
      return executeIntent(statement.kind, {
        title: `Drop table ${statement.table}`,
        facts: [["table", statement.table]],
        warning: "This removes the table and every row in it.",
        confirmLabel: "Drop table",
        destructive: true,
      });
    case "create-view":
      return executeIntent(statement.kind, {
        title: `${statement.orReplace === true ? "Create or replace" : "Create"} view ${statement.view}`,
        facts: [["view", statement.view]],
        ...(statement.orReplace === true
          ? { warning: "An existing view with this name will be replaced.", destructive: true }
          : {}),
        confirmLabel: statement.orReplace === true ? "Replace view" : "Create view",
      });
    case "drop-view":
      return executeIntent(statement.kind, {
        title: `Drop view ${statement.view}`,
        facts: [["view", statement.view]],
        warning: "Queries that use this view will stop working.",
        confirmLabel: "Drop view",
        destructive: true,
      });
    case "transaction": {
      const label = `${statement.action[0]?.toUpperCase() ?? ""}${statement.action.slice(1)}`;
      const rollback = statement.action === "rollback";
      return executeIntent(
        statement.kind,
        {
          title: `${label} transaction`,
          facts: [["action", statement.action.toUpperCase()]],
          ...(rollback
            ? { warning: "Every uncommitted change in the transaction will be discarded." }
            : {}),
          confirmLabel: label,
          destructive: rollback,
        },
        rollback,
      );
    }
    case "merge":
      return executeIntent(statement.kind, {
        title: `Merge rows into ${statement.table}`,
        facts: [
          ["table", statement.table],
          ["source", statement.source.table ?? "query result"],
          ["branches", String(statement.branches.length)],
        ],
        ...(statement.branches.some((branch) => branch.action.kind === "delete")
          ? { warning: "At least one MERGE branch can delete matching rows.", destructive: true }
          : {}),
        confirmLabel: "Run merge",
      });
    case "add-column":
      return executeIntent(statement.kind, {
        title: `Add column ${statement.table}.${statement.column.name}`,
        facts: [
          ["table", statement.table],
          ["column", statement.column.name],
          ["type", statement.column.type],
        ],
        confirmLabel: "Add column",
      });
    case "drop-column":
      return executeIntent(statement.kind, {
        title: `Drop column ${statement.table}.${statement.column}`,
        facts: [
          ["table", statement.table],
          ["column", statement.column],
        ],
        warning: "This removes the column and its stored values.",
        confirmLabel: "Drop column",
        destructive: true,
      });
  }
}

function executeIntent(
  operation: Extract<StatementIntent, { kind: "execute" }>["operation"],
  summary: StatementSummary,
  confirm = true,
): StatementIntent {
  return { kind: "execute", operation, summary, confirm };
}

/**
 * Whether the statement can change the database, and so is refused when `permissions.write` is
 * off. Queries and session settings (`SET`, `RESET`, `SHOW`) never can.
 */
export function changesData(intent: StatementIntent): boolean {
  return intent.kind !== "select" && intent.kind !== "session";
}

/** Whether the statement is described and confirmed before it runs. A subset of {@link changesData}. */
export function needsConfirmation(intent: StatementIntent): boolean {
  return changesData(intent) && (intent.kind !== "execute" || intent.confirm);
}

const catalogOperations: ReadonlySet<string> = new Set([
  "create-table",
  "create-table-as",
  "create-enum",
  "create-sequence",
  "create-index",
  "drop-index",
  "add-column",
  "drop-column",
  "drop-table",
  "create-view",
  "drop-view",
  "create-trigger",
  "drop-trigger",
]);

/** Whether a successful run changes what the catalog says, so the rail and completion reread it. */
export function changesCatalog(intent: StatementIntent): boolean {
  return intent.kind === "execute" && catalogOperations.has(intent.operation);
}

/**
 * The query that counts the rows an UPDATE or DELETE will touch, so the confirmation can say
 * "12 rows" rather than "matching rows". The WHERE clause is lifted from the statement's own
 * text — the compiled predicates have no SQL form — by finding the top-level `WHERE` outside
 * strings, comments, and parentheses, and stopping at a top-level `RETURNING`. Undefined for
 * every other statement, and for a text this cannot read with confidence.
 */
export function previewQuery(sql: string, intent: StatementIntent): string | undefined {
  if (intent.kind !== "update" && intent.kind !== "delete") return undefined;
  const table = sqlIdentifier(intent.table);
  if (!intent.filtered) return `SELECT COUNT(*) AS row_count FROM ${table}`;
  const where = whereClauseOf(sql);
  return where === undefined
    ? undefined
    : `SELECT COUNT(*) AS row_count FROM ${table} WHERE ${where}`;
}

/** The text between a top-level WHERE and the end of the statement or its RETURNING clause. */
function whereClauseOf(sql: string): string | undefined {
  let depth = 0;
  let start: number | undefined;
  let index = 0;
  while (index < sql.length) {
    const char = sql[index] ?? "";
    const next = sql[index + 1] ?? "";
    if (char === "-" && next === "-") {
      const eol = sql.indexOf("\n", index);
      index = eol < 0 ? sql.length : eol + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const close = sql.indexOf("*/", index + 2);
      index = close < 0 ? sql.length : close + 2;
      continue;
    }
    if (char === "'" || char === '"') {
      index += 1;
      while (index < sql.length) {
        if (sql[index] === char) {
          if (sql[index + 1] === char) index += 2;
          else break;
        } else index += 1;
      }
      index += 1;
      continue;
    }
    if (char === "(") depth += 1;
    else if (char === ")") depth -= 1;
    else if (/[A-Za-z_]/.test(char)) {
      let end = index + 1;
      while (end < sql.length && /[A-Za-z0-9_$]/.test(sql[end] ?? "")) end += 1;
      const word = sql.slice(index, end).toUpperCase();
      if (depth === 0 && start === undefined && word === "WHERE") start = end;
      else if (depth === 0 && start !== undefined && word === "RETURNING") {
        return clause(sql.slice(start, index));
      }
      index = end;
      continue;
    }
    index += 1;
  }
  return start === undefined ? undefined : clause(sql.slice(start));
}

function clause(text: string): string | undefined {
  const trimmed = text.replace(/[\s;]+$/u, "").trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

interface StatementSummary {
  /** One line naming the operation and its target, for the confirmation heading. */
  title: string;
  /** Label/value pairs spelling out what is about to happen. */
  facts: Array<[string, string]>;
  /** Set when the statement is unbounded, so the confirmation can say so plainly. */
  warning?: string;
  confirmLabel: string;
  destructive?: boolean;
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
        confirmLabel: "Insert rows",
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
        confirmLabel: "Update rows",
        destructive: !intent.filtered,
      };
    case "delete":
      return {
        title: `Delete ${intent.filtered ? "matching rows" : "every row"} from ${intent.table}`,
        facts: [
          ["table", intent.table],
          ["filter", intent.filtered ? "the statement's WHERE clause" : "none"],
        ],
        ...(intent.filtered ? {} : { warning: "No WHERE clause — this deletes every row." }),
        confirmLabel: "Delete rows",
        destructive: true,
      };
    case "execute":
      return intent.summary;
    case "select":
      return { title: "Run query", facts: [], confirmLabel: "Run query" };
    case "session":
      return {
        title: intent.operation === "show" ? "Show setting" : "Change setting",
        facts: [],
        confirmLabel: intent.operation === "show" ? "Show" : "Set",
      };
  }
}

/** The message shown when `permissions.write` is off and a statement would change data. */
export function writeBlockedMessage(intent: StatementIntent): string {
  const operation = intent.kind === "execute" ? intent.operation : intent.kind;
  return `Writes are turned off in these devtools, so this ${operation.replaceAll("-", " ").toUpperCase()} was not run.`;
}
