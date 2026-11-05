/**
 * The class of a compiled statement: what the owner does with it (`kind`), the PostgreSQL command
 * tag it reports, and whether it changes the session's transaction state. Decided from the front
 * end's `CompiledStatement` alone, before any catalog lookup, so the owner can classify a request
 * (protocol "standalone" rules, owner/sequence.ts) without lowering it.
 */
import type { CompiledStatement } from "../../engine/query.js";

export type StatementKind = "query" | "modify" | "ddl" | "transactionControl" | "utility";

export interface StatementClass {
  readonly kind: StatementKind;
  /** PostgreSQL's command tag word(s): `SELECT`, `INSERT`, `CREATE TABLE`, `ROLLBACK`… */
  readonly command: string;
  /**
   * BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE and ROLLBACK TO: the statement changes the
   * session's transaction state, so it is never standalone and never answered by a fresh session.
   */
  readonly controlsTransaction: boolean;
}

const DDL_COMMANDS = {
  "create-table": "CREATE TABLE",
  // PostgreSQL reports CREATE TABLE AS with the tag of the rows it inserted.
  "create-table-as": "SELECT",
  "create-enum": "CREATE TYPE",
  "create-sequence": "CREATE SEQUENCE",
  "create-index": "CREATE INDEX",
  "drop-index": "DROP INDEX",
  "create-trigger": "CREATE TRIGGER",
  "drop-trigger": "DROP TRIGGER",
  "drop-table": "DROP TABLE",
  "create-view": "CREATE VIEW",
  "drop-view": "DROP VIEW",
  "add-column": "ALTER TABLE",
  "rename-column": "ALTER TABLE",
  "drop-column": "ALTER TABLE",
} as const satisfies Partial<Record<CompiledStatement["kind"], string>>;

const TRANSACTION_COMMANDS = {
  begin: "BEGIN",
  commit: "COMMIT",
  rollback: "ROLLBACK",
  savepoint: "SAVEPOINT",
  "rollback-to": "ROLLBACK",
  release: "RELEASE",
} as const;

/**
 * Classifies one compiled statement. TRUNCATE is `TRUNCATE` when compiled under
 * `compileWithDeclaredTypes` (lowering always is); compiled without it, the front end makes it an
 * unfiltered DELETE and keeps no trace of the spelling, so it reports `DELETE`.
 */
export function classifyStatement(statement: CompiledStatement): StatementClass {
  switch (statement.kind) {
    case "select":
      return { kind: "query", command: "SELECT", controlsTransaction: false };
    case "insert":
      return { kind: "modify", command: "INSERT", controlsTransaction: false };
    case "update":
      return { kind: "modify", command: "UPDATE", controlsTransaction: false };
    case "delete":
      return {
        kind: "modify",
        command: statement.truncate === true ? "TRUNCATE" : "DELETE",
        controlsTransaction: false,
      };
    case "merge":
      return { kind: "modify", command: "MERGE", controlsTransaction: false };
    case "transaction":
      return {
        kind: "transactionControl",
        command: TRANSACTION_COMMANDS[statement.action],
        controlsTransaction: true,
      };
    case "set":
      return {
        kind: "utility",
        command: statement.action === "set" ? "SET" : "RESET",
        controlsTransaction: false,
      };
    case "show":
      return { kind: "utility", command: "SHOW", controlsTransaction: false };
    default:
      return { kind: "ddl", command: DDL_COMMANDS[statement.kind], controlsTransaction: false };
  }
}
