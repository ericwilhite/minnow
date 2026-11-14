/**
 * The `Lowerer` (lower/lowerer.ts): one statement text to a `LoweredStatement`.
 */
import type {
  CatalogChange,
  CatalogDependency,
  CatalogView,
  TableDef,
  ViewSourceSchema,
} from "../access/catalog.js";
import type { CreateTableInput } from "../../engine/catalog-mutations.js";
import type { CompiledStatement, TransactionMode } from "../../engine/query.js";
import type { OutputColumn, PhysicalPlan, Rel } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import { samePhysicalType } from "../types.js";
import { ParameterTypes } from "./coerce.js";
import { descriptorOf, lowerDdl, tableInput } from "./ddl.js";
import { frontEndError, PlanningError, unsupported } from "./errors.js";
import type { ExpressionState } from "./expressions.js";
import type { RowLocking, CompiledQuery } from "../../plan/model.js";
import { parseAny, parseQuery, prepareQuery } from "./frontend.js";
import {
  captureCatalogDeclarations,
  catalogDeclarations,
} from "../../engine/catalog-ddl-sources.js";
import type { TableSource } from "../../plan/model.js";
import type { OuterResolver } from "./from.js";
import { expressionObjects } from "./expression-objects.js";
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
import {
  liftedSlotsAreEquivalent,
  parameterize,
  type NotParameterizable,
  type ParameterizedPlan,
  type ParameterizedText,
} from "./parameterize.js";
import { resultColumn } from "./result.js";
import { lowerTableRules } from "./rules.js";
import { classifyStatement } from "./statement.js";
import { ctasDataClause } from "./ctas.js";
import { integerLiteralValue } from "./literals.js";
import { rowOutputName } from "../../engine/json-call-sources.js";

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
    ...(plan.builder.ftsQueries.length ? { ftsQueries: [...plan.builder.ftsQueries] } : {}),
    ...(plan.builder.ftsParameters.size
      ? {
          ftsParameters: [...plan.builder.ftsParameters].map(([source, slot]) => ({
            source,
            slot,
          })),
        }
      : {}),
    columns,
    parameters: [...plan.state.parameters.settled(), ...plan.builder.slots],
    placeholderCount: plan.placeholders,
    clockSlot: plan.builder.clockSlot,
    initPlans: [...plan.builder.initPlans],
    catalogVersion: catalog.version,
    volatile: plan.state.volatile,
    hasRowLocking: plan.builder.rowLocking !== undefined,
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
  if (columns.some((column) => column.record)) throw unsupported("function", "bare record output");
  return {
    rowLocking: rowLockIntent(plan.builder.rowLocking),
    plan: physicalPlan(root, outputColumns(columns), plan, catalog),
    columns: columns.map((column) => resultColumn(column, column.big)),
    reads: [...plan.builder.reads].sort((a, b) => a - b),
    // Empty when no shape decision read statistics: such a plan never needs re-lowering for them.
    basis: plan.builder.statisticsDependent
      ? [...plan.builder.rowCounts].map(([tableId, rowCount]) => ({ tableId, rowCount }))
      : [],
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
  const ctasData = ctasDataClause(text);
  const source = ctasData === null ? text : text.slice(0, ctasData.index);
  const noData = ctasData?.noData ?? false;
  const parsed = parseAny(source);
  // Candidate matching deliberately preserves the legacy header grammar. Only the
  // parsed statement kind distinguishes CTAS from AS inside ordinary DDL names/defaults.
  if (ctasData !== null && ("query" in parsed || parsed.statement.kind !== "create-table-as"))
    throw new PlanningError("WITH [NO] DATA requires CREATE TABLE AS", "42601");
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
    case "create-view": {
      const before = context.catalog.views.find((view) => view.name === statement.view);
      if (before !== undefined && statement.orReplace !== true)
        throw new PlanningError(`relation "${statement.view}" already exists`, "42P07");
      if (context.catalog.tableByName(statement.view) !== undefined)
        throw new PlanningError(`relation "${statement.view}" already exists`, "42P07");
      const parsed = captureCatalogDeclarations(() => parseQuery(statement.sql));
      const dependencies: CatalogDependency[] = [];
      const seen = new Set<object>();
      const visit = (value: unknown): void => {
        if (typeof value !== "object" || value === null || seen.has(value)) return;
        seen.add(value);
        const entry = value as Record<string, unknown>;
        if (
          typeof entry.table === "string" &&
          typeof entry.alias === "string" &&
          !entry.derived &&
          !entry.recursive &&
          !entry.union
        ) {
          const view = context.catalog.views.find((view) => view.name === entry.table);
          if (
            view !== undefined &&
            !dependencies.some((dep) => dep.kind === "view" && dep.objectId === view.id)
          )
            dependencies.push({ kind: "view", objectId: view.id, columnIds: [] });
        }
        for (const nested of Object.values(entry)) visit(nested);
      };
      visit(parsed);
      const plan = planning(placeholderCount(parsed), context);
      if (plan.placeholders > 0)
        throw new PlanningError("there is no parameter in a view definition", "42P02");
      const metadataPlans: Array<{
        block: ReturnType<typeof lowerBlock>;
        builder: PlanBuilder;
        parsed: CompiledQuery;
      }> = [];
      const visitedDeclarations = new Map<readonly object[], Set<OuterResolver | undefined>>();
      const bindDeclarations = (query: CompiledQuery, outer?: OuterResolver): void => {
        const sources = catalogDeclarations(query);
        if (!sources) return;
        let contexts = visitedDeclarations.get(sources);
        if (!contexts) visitedDeclarations.set(sources, (contexts = new Set()));
        if (contexts.has(outer)) return;
        contexts.add(outer);
        for (const source of sources) {
          const probe = parseQuery("SELECT * FROM __catalog_cte");
          probe.base = source as TableSource;
          const child = planning(0, context);
          child.builder.catalogDeclarations = bindDeclarations;
          const block = lowerBlock(prepareQuery(probe, context.catalog), child.builder, outer);
          visit(probe);
          metadataPlans.push({ block, builder: child.builder, parsed: probe });
        }
      };
      plan.builder.catalogDeclarations = bindDeclarations;
      const block = lowerBlock(prepareQuery(parsed, context.catalog), plan.builder);
      const addDependency = (kind: "sequence" | "type", objectId: number) => {
        if (!dependencies.some((dep) => dep.kind === kind && dep.objectId === objectId))
          dependencies.push({ kind, objectId, columnIds: [] });
      };
      for (const source of [parsed, ...metadataPlans.map((item) => item.parsed)]) {
        const sourceObjects = expressionObjects(source, context.catalog);
        for (const id of sourceObjects.typeIds) addDependency("type", id);
        for (const id of sourceObjects.sequenceIds) addDependency("sequence", id);
      }
      const semanticSeen = new Set<object>();
      const semantic = (value: unknown): void => {
        if (typeof value !== "object" || value === null || semanticSeen.has(value)) return;
        semanticSeen.add(value);
        const entry = value as Record<string, unknown>;
        if (entry.kind === "enum" && typeof entry.name === "string") {
          const named = context.catalog.typeByName(entry.name);
          if (named) addDependency("type", named.id);
        }
        if (
          entry.op === "call" &&
          ["nextval", "currval", "setval"].includes(String(entry.fn)) &&
          Array.isArray(entry.args)
        ) {
          const first = entry.args[0] as import("../ir/ir.js").ScalarExpr | undefined,
            id = first && integerLiteralValue(first);
          if (id !== null && id !== undefined) addDependency("sequence", Number(id));
        }
        for (const child of Object.values(entry)) semantic(child);
      };
      for (const item of [{ block, builder: plan.builder }, ...metadataPlans]) {
        semantic(item.block.rel);
        semantic(item.block.columns);
        for (const id of item.builder.reads) {
          let dep = dependencies.find((dep) => dep.kind === "table" && dep.objectId === id);
          if (!dep) {
            dep = { kind: "table", objectId: id, columnIds: [] };
            dependencies.push(dep);
          }
          const columns = new Set([
            ...dep.columnIds,
            ...(item.builder.columnDependencies.get(id) ?? []),
          ]);
          dependencies[dependencies.indexOf(dep)] = {
            ...dep,
            columnIds: [...columns].sort((a, b) => a - b),
          };
        }
      }
      if (before) {
        const pending = dependencies
            .filter((dep) => dep.kind === "view")
            .map((dep) => dep.objectId),
          visited = new Set<number>();
        while (pending.length) {
          const id = pending.pop();
          if (id === undefined || visited.has(id)) continue;
          if (id === before.id)
            throw new PlanningError(
              `infinite recursion detected in rules for relation "${before.name}"`,
              "42P17",
            );
          visited.add(id);
          for (const dep of context.catalog.views.find((view) => view.id === id)?.dependencies ??
            [])
            if (dep.kind === "view") pending.push(dep.objectId);
        }
      }
      const outputs = block.columns.map((column) => ({
        name: column.name,
        type: column.type,
        declared: descriptorOf(column.type),
        nullable: column.nullable,
      }));
      if (
        !parsed.select.some((item) => item.expression.kind === "wildcard") &&
        parsed.select.length === outputs.length
      )
        outputs.forEach((output, i) => {
          const item = parsed.select[i];
          if (item) output.name = rowOutputName(item) ?? item.alias;
        });
      if (new Set(outputs.map((column) => column.name)).size !== outputs.length)
        throw new PlanningError("column specified more than once in view", "42701");
      if (
        before?.outputs &&
        (outputs.length < before.outputs.length ||
          before.outputs.some(
            (column, i) =>
              column.name !== outputs.at(i)?.name ||
              !samePhysicalType(column.type, outputs.at(i)?.type ?? column.type),
          ))
      )
        throw new PlanningError("cannot change name or type of view column", "42P16");
      const sourceSchemas: ViewSourceSchema[] = dependencies.flatMap((dep): ViewSourceSchema[] => {
        if (dep.kind === "table") {
          const source = context.catalog.table(dep.objectId);
          if (!source) throw new PlanningError("Missing view table", "42P01");
          return [
            {
              kind: "table",
              objectId: source.id,
              columnIds: source.columns.map((c) => c.id),
              outputCount: 0,
            },
          ];
        }
        if (dep.kind === "view") {
          const source = context.catalog.views.find((v) => v.id === dep.objectId);
          if (!source?.outputs)
            throw new PlanningError("Cannot bind a view without an output descriptor", "0A000");
          return [
            {
              kind: "view",
              objectId: source.id,
              columnIds: [],
              outputCount: source.outputs.length,
            },
          ];
        }
        return [];
      });
      const view = {
        name: statement.view,
        bodySql: statement.sql,
        outputs,
        dependencies,
        sourceSchemas,
      };
      return {
        ...base,
        kind: "ddl",
        parameters: [],
        change:
          before === undefined
            ? { kind: "createView", view }
            : {
                kind: "replaceView",
                view: {
                  ...view,
                  id: before.id,
                  ...(before.managed === undefined ? {} : { managed: before.managed }),
                },
              },
        followUp: null,
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

    parameterize,

    lowerParameterized(
      parameterized: ParameterizedText,
      context: LoweringContext,
    ): ParameterizedPlan | NotParameterizable {
      const refused: NotParameterizable = { kind: "notParameterizable", key: parameterized.key };
      // Each slot takes exactly the type lowering gives its literal, by the same front end and
      // lowering rather than a parallel rule (`-2147483648` is int8, and past 2^53 whatever the
      // front end makes of it); where that type is not the class's, the family is not lifted.
      for (const [index, literal] of parameterized.literals.entries()) {
        let alone: LoweredStatement;
        try {
          alone = lowerStatement(`SELECT ${literal} AS v`, { ...context, parameterTypes: [] });
        } catch {
          return refused;
        }
        const type = alone.kind === "query" ? alone.plan.columns[0]?.type : undefined;
        const slot = parameterized.types[index];
        if (type === undefined || slot === undefined || !samePhysicalType(type, slot))
          return refused;
      }
      const own = context.parameterTypes ?? [];
      const parameterTypes = [
        ...Array.from({ length: parameterized.placeholders }, (_, index) => own[index] ?? null),
        ...parameterized.types,
      ];
      let lowered: LoweredStatement;
      try {
        lowered = lowerStatement(parameterized.text, { ...context, parameterTypes });
      } catch {
        // The original text lowers (or fails) on its own; the rewrite must not decide it.
        return refused;
      }
      if (lowered.kind !== "query" && lowered.kind !== "modify") return refused;
      const first = parameterized.placeholders;
      const lifted = new Set(parameterized.types.map((_, index) => first + index));
      if (!liftedSlotsAreEquivalent(lowered.plan, lifted)) return refused;
      // A lifted slot whose type lowering did not keep (a placeholder typed by its context).
      if (
        parameterized.types.some((type, index) => {
          const kept = lowered.plan.parameters[first + index];
          return kept === undefined || !samePhysicalType(kept, type);
        })
      )
        return refused;
      return {
        kind: "parameterized",
        key: parameterized.key,
        statement: {
          ...lowered,
          parameters: lowered.parameters.slice(0, first),
          liftedParameters: parameterized.types.length,
        },
      };
    },
  };
}
