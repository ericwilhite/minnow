/**
 * The `Lowerer` (lower/lowerer.ts): one statement text to a `LoweredStatement`.
 */
import type { CatalogChange, CatalogView, TableDef } from "../access/catalog.js";
import type { CreateTableInput } from "../../engine/catalog-mutations.js";
import type { CompiledStatement } from "../../engine/query.js";
import type { OutputColumn, PhysicalPlan, Rel } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import type { PhysicalType } from "../types.js";
import { ParameterTypes } from "./coerce.js";
import { descriptorOf, lowerDdl, tableInput } from "./ddl.js";
import { PlanningError, unsupported } from "./errors.js";
import type { ExpressionState } from "./expressions.js";
import type { CompiledQuery } from "../../plan/model.js";
import { parseAny, parseQuery, prepareQuery } from "./frontend.js";
import type {
  LoweredModify,
  LoweredQuery,
  LoweredStatement,
  Lowerer,
  LoweringContext,
  WriteShape,
} from "./lowerer.js";
import {
  insertSelectText,
  lowerBatchWrite,
  lowerDelete,
  lowerInsert,
  lowerUpdate,
} from "./modify.js";
import { lowerBlock, outputColumns, PlanBuilder, type RelColumn } from "./query.js";
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

const TIMESTAMPTZ: PhysicalType = { kind: "timestamp", zoned: true };

/**
 * The query of `CREATE TABLE [IF NOT EXISTS] name AS query`, as written: the compiled statement
 * keeps no text of it, and its follow-up INSERT must lower it again at the new catalog version.
 */
function ctasQueryText(text: string): string {
  const match =
    /^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"(?:[^"]|"")+"|[^\s(]+)\s+AS\s+/iu.exec(text);
  if (match === null) throw new PlanningError("Malformed CREATE TABLE AS", "42601");
  return text.slice(match[0].length).replace(/;\s*$/u, "");
}

interface Planning {
  readonly builder: PlanBuilder;
  readonly state: ExpressionState & { clockUsed: boolean };
  readonly placeholders: number;
}

function planning(placeholders: number, context: LoweringContext): Planning {
  const parameters = new ParameterTypes(placeholders, context.parameterTypes ?? []);
  const state = {
    parameters,
    catalog: context.catalog,
    clockUsed: false,
    clock(): number {
      state.clockUsed = true;
      return placeholders;
    },
    volatile: false,
    sequences: false,
  };
  const builder = new PlanBuilder(
    context.catalog,
    state,
    (tableId) => context.statistics.table(tableId)?.rowCount ?? 0,
  );
  return { builder, state, placeholders };
}

function physicalPlan(
  root: Rel,
  columns: readonly OutputColumn[],
  plan: Planning,
  catalog: CatalogView,
): PhysicalPlan {
  const parameters = plan.state.parameters.settled();
  const clockSlot = plan.state.clockUsed ? plan.placeholders : null;
  return {
    root,
    columns,
    parameters: clockSlot === null ? parameters : [...parameters, TIMESTAMPTZ],
    placeholderCount: plan.placeholders,
    clockSlot,
    initPlans: [],
    catalogVersion: catalog.version,
    volatile: plan.state.volatile,
  };
}

function planned(plan: Planning, root: Rel, columns: readonly RelColumn[], catalog: CatalogView) {
  return {
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
  statement: Extract<CompiledStatement, { kind: "insert" | "update" | "delete" }>,
  context: LoweringContext,
): LoweredModify {
  const plan = planning(placeholderCount(statement), context);
  const write =
    statement.kind === "insert"
      ? lowerInsert(statement, plan.builder)
      : statement.kind === "update"
        ? lowerUpdate(statement, plan.builder)
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

const TRANSACTION_ACTIONS = {
  begin: "begin",
  commit: "commit",
  rollback: "rollback",
  savepoint: "savepoint",
  "rollback-to": "rollbackTo",
  release: "release",
} as const;

export function createLowerer(): Lowerer {
  return {
    lower(text: string, context: LoweringContext): LoweredStatement {
      const parsed = parseAny(text);
      if ("query" in parsed) return lowerSelect(text, parsed.query, context);
      const statement = parsed.statement;
      const statementClass = classifyStatement(statement);
      const base = {
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
          throw unsupported("merge", "MERGE (milestone M3)");
        case "transaction":
          return {
            ...base,
            kind: "transactionControl",
            parameters: [],
            action: TRANSACTION_ACTIONS[statement.action],
            savepoint: statement.name ?? null,
            readOnly: false,
          };
        case "set":
          return { ...base, kind: "utility", parameters: [], show: null };
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
            followUp: insertSelectText(
              statement.table,
              columns.map((column) => column.name),
              ctasQueryText(text),
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
