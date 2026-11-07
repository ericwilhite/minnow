/**
 * The `Lowerer` (lower/lowerer.ts): one statement text to a `LoweredStatement`.
 */
import type { CatalogChange, CatalogView, TableDef } from "../access/catalog.js";
import type { CreateTableInput } from "../../engine/catalog-mutations.js";
import type { CompiledStatement, TransactionMode } from "../../engine/query.js";
import type { OutputColumn, PhysicalPlan, Rel } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import { ParameterTypes } from "./coerce.js";
import { descriptorOf, lowerDdl, tableInput } from "./ddl.js";
import { frontEndError, PlanningError } from "./errors.js";
import type { ExpressionState } from "./expressions.js";
import type { RowLocking, CompiledQuery } from "../../plan/model.js";
import { parseAny, parseQuery, prepareQuery } from "./frontend.js";
import type {
  LoweredModify,
  LoweredRowLocking,
  LoweredQuery,
  LoweredStatement,
  Lowerer,
  LoweringContext,
  TransactionModes,
  TransactionModeItem,
  WriteShape,
} from "./lowerer.js";
import {
  insertSelectText,
  lowerBatchWrite,
  lowerDelete,
  lowerInsert,
  lowerMerge,
  lowerUpdate,
} from "./modify.js";
import {
  installSubqueries,
  lowerBlock,
  outputColumns,
  PlanBuilder,
  type RelColumn,
} from "./query.js";
import { resultColumn } from "./result.js";
import { lowerTableRules } from "./rules.js";
import { classifyStatement } from "./statement.js";

/** PostgreSQL settings SHOW answers with the current engine's fixed values. */
const SETTINGS: Readonly<Record<string, string>> = {
  search_path: "public",
  server_version: "16.0",
  server_version_num: "160000",
  timezone: "UTC",
  time_zone: "UTC",
  transaction_isolation: "repeatable read",
  default_transaction_isolation: "repeatable read",
  client_encoding: "UTF8",
  server_encoding: "UTF8",
  standard_conforming_strings: "on",
  integer_datetimes: "on",
  datestyle: "ISO, MDY",
  intervalstyle: "postgres",
  application_name: "",
  is_superuser: "on",
  max_identifier_length: "63",
};

/**
 * The query of `CREATE TABLE [IF NOT EXISTS] name AS query`, as written: the compiled statement
 * keeps no text of it, and its follow-up INSERT must lower it again at the new catalog version.
 */
/** A trailing `WITH DATA` / `WITH NO DATA` of CREATE TABLE AS (group 1 is `NO`). */
const CTAS_DATA = /(?<=^\s*CREATE\s+TABLE\b[\s\S]*\bAS\b[\s\S]*)\s+WITH\s+(NO\s+)?DATA\s*;?\s*$/iu;

function ctasQueryText(text: string): string {
  const match =
    /^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"(?:[^"]|"")+"|[^\s(]+)\s+AS\s+/iu.exec(text);
  if (match === null) throw new PlanningError("Malformed CREATE TABLE AS", "42601");
  return text.slice(match[0].length).replace(/;\s*$/u, "");
}

interface Planning {
  readonly builder: PlanBuilder;
  readonly state: ExpressionState;
  readonly placeholders: number;
}

function planning(placeholders: number, context: LoweringContext): Planning {
  const parameters = new ParameterTypes(placeholders, context.parameterTypes ?? []);
  let builder: PlanBuilder | null = null;
  const state: ExpressionState = {
    parameters,
    catalog: context.catalog,
    clock: () => {
      if (builder === null) throw new PlanningError("clock before planning", "XX000");
      return builder.clock();
    },
    volatile: false,
    sequences: false,
  };
  builder = new PlanBuilder(context.catalog, state, context.statistics, context.costs);
  installSubqueries(builder);
  return { builder, state, placeholders };
}

function physicalPlan(
  root: Rel,
  columns: readonly OutputColumn[],
  plan: Planning,
  catalog: CatalogView,
): PhysicalPlan {
  return {
    root,
    columns,
    parameters: [...plan.state.parameters.settled(), ...plan.builder.slots],
    placeholderCount: plan.placeholders,
    clockSlot: plan.builder.clockSlot,
    initPlans: [...plan.builder.initPlans],
    catalogVersion: catalog.version,
    volatile: plan.state.volatile,
  };
}

const ROW_LOCK_INTENT: Readonly<Record<RowLocking, LoweredRowLocking>> = {
  "KEY SHARE": "keyShare",
  SHARE: "share",
  "NO KEY UPDATE": "noKeyUpdate",
  UPDATE: "update",
};
function rowLockIntent(lock: RowLocking | undefined): LoweredRowLocking {
  return lock === undefined ? "none" : ROW_LOCK_INTENT[lock];
}

function planned(plan: Planning, root: Rel, columns: readonly RelColumn[], catalog: CatalogView) {
  return {
    rowLocking: rowLockIntent(plan.builder.rowLocking),
    plan: physicalPlan(root, outputColumns(columns), plan, catalog),
    columns: columns.map((column) => resultColumn(column, column.big)),
    reads: [...plan.builder.reads].sort((a, b) => a - b),
    basis: [...plan.builder.rowCounts].map(([tableId, rowCount]) => ({ tableId, rowCount })),
    parameters: plan.state.parameters.settled(),
  };
}

function placeholderCount(statement: { readonly parameterCount?: number }): number {
  return statement.parameterCount ?? 0;
}

function lowerSelect(text: string, parsed: CompiledQuery, context: LoweringContext): LoweredQuery {
  const plan = planning(placeholderCount(parsed), context);
  const block = lowerBlock(prepareQuery(parsed, context.catalog), plan.builder);
  return {
    kind: "query",
    text,
    catalogVersion: context.catalog.version,
    command: "SELECT",
    controlsTransaction: false,
    ...planned(plan, block.rel, block.columns, context.catalog),
    pointRead: block.pointRead === true && !plan.state.volatile && !plan.state.sequences,
  };
}

function lowerModify(
  text: string,
  statement: Extract<CompiledStatement, { kind: "insert" | "update" | "delete" | "merge" }>,
  context: LoweringContext,
): LoweredModify {
  const plan = planning(placeholderCount(statement), context);
  const write =
    statement.kind === "insert"
      ? lowerInsert(statement, plan.builder)
      : statement.kind === "update"
        ? lowerUpdate(statement, plan.builder)
        : statement.kind === "merge"
          ? lowerMerge(statement, plan.builder)
          : lowerDelete(statement, plan.builder);
  return {
    kind: "modify",
    text,
    catalogVersion: context.catalog.version,
    command: classifyStatement(statement).command,
    controlsTransaction: false,
    ...planned(plan, write.rel, write.columns, context.catalog),
    writes: [write.tableId],
  };
}

/** Preserve every instruction; order-sensitive validation precedes the final-mode summary. */
function transactionModeItems(modes: readonly TransactionMode[]): TransactionModeItem[] {
  return modes.map((mode) =>
    mode.kind === "access"
      ? { kind: "access", value: mode.readOnly ? "readOnly" : "readWrite" }
      : mode.kind === "isolation"
        ? { kind: "isolation", value: mode.level }
        : { kind: "deferrable", value: mode.deferrable },
  );
}

/** Final values after ordered validation: the last item of each kind wins. */
function transactionModes(modes: readonly TransactionMode[]): TransactionModes {
  let readOnly: boolean | null = null;
  let isolation: TransactionModes["isolation"] = null;
  let deferrable: boolean | null = null;
  for (const mode of modes) {
    if (mode.kind === "access") readOnly = mode.readOnly;
    else if (mode.kind === "isolation") isolation = mode.level;
    else deferrable = mode.deferrable;
  }
  return { readOnly, isolation, deferrable };
}

const TRANSACTION_ACTIONS = {
  begin: "begin",
  commit: "commit",
  rollback: "rollback",
  savepoint: "savepoint",
  "rollback-to": "rollbackTo",
  release: "release",
} as const;

/** `Lowerer.lower` before its errors are translated (lower/errors.ts `frontEndError`). */
function lowerStatement(text: string, context: LoweringContext): LoweredStatement {
  // CREATE TABLE … AS … WITH [NO] DATA: the front end does not parse the clause (and keeps
  // refusing it); lowering strips it and decides whether the follow-up INSERT runs.
  const ctasData = CTAS_DATA.exec(text);
  const source = ctasData === null ? text : text.slice(0, ctasData.index);
  const noData = ctasData?.[1] !== undefined;
  const parsed = parseAny(source);
  if ("query" in parsed) return lowerSelect(text, parsed.query, context);
  const statement = parsed.statement;
  const statementClass = classifyStatement(statement);
  const base = {
    rowLocking: "none" as const,
    text,
    catalogVersion: context.catalog.version,
    command: statementClass.command,
    controlsTransaction: statementClass.controlsTransaction,
  };
  switch (statement.kind) {
    case "select":
      return lowerSelect(text, parseQuery(statement.sql), context);
    case "insert":
    case "update":
    case "delete":
      return lowerModify(text, statement, context);
    case "merge":
      return lowerModify(text, statement, context);
    case "transaction": {
      const modes = transactionModes(statement.modes ?? []);
      return {
        ...base,
        kind: "transactionControl",
        parameters: [],
        action: TRANSACTION_ACTIONS[statement.action],
        savepoint: statement.name ?? null,
        modes,
        modeItems: transactionModeItems(statement.modes ?? []),
        readOnly: statement.action === "begin" && modes.readOnly === true,
      };
    }
    case "set": {
      // SET TRANSACTION changes the transaction: a typed instruction, never a no-op utility.
      if (statement.transactionModes !== undefined) {
        const modes = transactionModes(statement.transactionModes);
        return {
          ...base,
          controlsTransaction: true,
          kind: "transactionControl",
          parameters: [],
          action: "setTransaction",
          savepoint: null,
          modes,
          modeItems: transactionModeItems(statement.transactionModes),
          readOnly: modes.readOnly === true,
        };
      }
      return { ...base, kind: "utility", parameters: [], show: null };
    }
    case "show": {
      const name = statement.name.toLowerCase();
      const value = SETTINGS[name];
      if (value === undefined)
        throw new PlanningError(
          `unrecognized configuration parameter "${statement.name}"`,
          "42704",
        );
      return {
        ...base,
        kind: "utility",
        parameters: [],
        show: {
          column: resultColumn({ name, type: { kind: "text" }, nullable: false }, false),
          value,
        },
      };
    }
    case "create-table-as": {
      if (context.catalog.tableByName(statement.table) !== undefined) {
        if (statement.ifNotExists === true)
          return { ...base, kind: "ddl", parameters: [], change: null, followUp: null };
        throw new PlanningError(`relation "${statement.table}" already exists`, "42P07");
      }
      const plan = planning(placeholderCount(statement.query), context);
      const block = lowerBlock(prepareQuery(statement.query, context.catalog), plan.builder);
      const columns = block.columns.map((column) => ({
        name: column.name,
        type: column.type,
        declared: descriptorOf(column.type),
        nullable: true,
        defaultSql: null,
        generatedSql: null,
        identitySequence: null,
        missing: null,
      }));
      if (new Set(columns.map((column) => column.name)).size !== columns.length)
        throw new PlanningError(
          `column specified more than once in table "${statement.table}"`,
          "42701",
        );
      const change: CatalogChange = {
        kind: "createTable",
        table: {
          name: statement.table,
          columns,
          primaryKey: null,
          unique: [],
          foreignKeys: [],
          checks: [],
        },
      };
      return {
        ...base,
        kind: "ddl",
        parameters: [],
        change,
        rowLocking: noData ? "none" : rowLockIntent(plan.builder.rowLocking),
        followUp: noData
          ? null
          : insertSelectText(
              statement.table,
              columns.map((column) => column.name),
              ctasQueryText(source),
            ),
      };
    }
    default: {
      const result = lowerDdl(statement, context.catalog);
      return {
        ...base,
        kind: "ddl",
        parameters: [],
        change: result.change,
        followUp: result.followUp,
      };
    }
  }
}

export function createLowerer(): Lowerer {
  return {
    lower(text: string, context: LoweringContext): LoweredStatement {
      // Every failure leaves as a PlanningError with a SQLSTATE (or UnsupportedPlanError).
      try {
        return lowerStatement(text, context);
      } catch (error) {
        throw frontEndError(error);
      }
    },

    lowerWrite(shape: WriteShape, context: LoweringContext): LoweredModify {
      const plan = planning(0, context);
      const write = lowerBatchWrite(shape, plan.builder);
      return {
        kind: "modify",
        text: "",
        catalogVersion: context.catalog.version,
        command: shape.op === "delete" ? "DELETE" : shape.op === "update" ? "UPDATE" : "INSERT",
        controlsTransaction: false,
        ...planned(plan, write.rel, write.columns, context.catalog),
        writes: [write.tableId],
      };
    },

    lowerTableRules(table: TableDef, catalog: CatalogView): TableRuleExpressions {
      return lowerTableRules(table, catalog);
    },

    lowerSchemaTable(input: CreateTableInput, catalog: CatalogView): CatalogChange {
      return { kind: "createTable", table: tableInput(input, catalog, "schema") };
    },
  };
}
