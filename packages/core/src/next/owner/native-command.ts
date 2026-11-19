/** Real native command consumer. Owns funding/bindings through end and graph retirement;
 * caller retains the writer transaction and alone chooses commit/savepoint recovery.
 * Internal owner dispatch only; the public package/default engine is unchanged. */
import { layoutOf } from "../types.js";
import type { TableDef } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import type { WriteTransaction } from "../access/write-path.js";
import type { PhysicalPlan, ScalarExpr } from "../ir/ir.js";
import type { ParameterValue } from "../exec/executor.js";
import { UnsupportedPlanError } from "../exec/errors.js";
import { TriggerBodyRunner } from "../exec/trigger-body.js";
import { compileTableRules } from "../exec/rules.js";
import {
  bindTableRules,
  tableRuleMinimumBytes,
  bindColumnRules,
  columnRuleMinimumBytes,
  type BoundTableRules,
} from "../exec/row-rules.js";
import { tableRuleProgram } from "../exec/rule-program.js";
import {
  clockParameter,
  prepareParameters,
  needsParameterSizing,
  PARAMETER_SIZING_BYTES,
} from "../exec/parameters.js";
import { nativeParameterMinimumBytes } from "../exec/native-parameters.js";
import { StatementImpl } from "../write/transaction.js";
import { pauseIfDue } from "../task.js";
import { NativeCommandReach, nativeCommandReachBytes } from "./command-reach.js";
import {
  NativeCommandRoot,
  nativeCommandRootSetupBytes,
  type NativeRootPreparation,
  type NativeCommandRootScope,
} from "./command-root.js";
import { NativeRootCredits, NATIVE_ROOT_CREDIT_BYTES } from "./native-root-credits.js";
import { NATIVE_INPUT_CAPTURE_BYTES } from "./native-input.js";
import { PREPAID_QUERY_BYTES } from "./prepaid-query.js";
import { retainedBytes } from "./rule-cache.js";
import type { StatementPlanner, PlannerContext } from "./planner.js";

export interface NativeCommandRequest {
  readonly transaction: WriteTransaction;
  readonly rootId: number;
  readonly plan: PhysicalPlan;
  readonly parameters: readonly ParameterValue[];
  readonly planner: StatementPlanner;
  readonly context: PlannerContext;
  readonly governor: NativeCommandRootScope["governor"];
  readonly execution: NativeCommandRootScope["execution"];
  readonly signal: AbortSignal;
}
export interface NativeCommandResult {
  readonly rowCount: number;
  /** Conservative invalidation reach, never mutation counts derived from repeat tracking. */
  readonly tables: readonly number[];
}
interface Binding {
  readonly table: TableDef;
  readonly program: TableRules;
  readonly minimum: number;
  rules: BoundTableRules | null;
}
function refuse(feature: string): never {
  throw new UnsupportedPlanError(feature, { construct: "native modification" });
}
/** Fixed-width kernels are fully probed by tableRuleMinimumBytes. Borrowed varlen
 * leaves may be compared without decoding/copying; growing rule kernels need a
 * separate immutable output bound before an image is borrowed, not a retry under it. */
function fixedRule(expr: ScalarExpr): void {
  if (["array", "json", "decimal"].includes(expr.type.kind))
    refuse("structural or decimal native row-rule workspace");
  const fixed = !["text", "bytes", "json", "array", "decimal", "enum"].includes(expr.type.kind);
  if (expr.op === "column" || expr.op === "literal") return;
  if (!fixed) refuse("growing native row-rule expression");
  switch (expr.op) {
    case "call":
    case "and":
    case "or":
      for (const child of expr.args) fixedRule(child);
      break;
    case "compare":
    case "distinctFrom":
      fixedRule(expr.left);
      fixedRule(expr.right);
      break;
    case "cast":
      if (["text", "bytes", "enum"].includes(expr.input.type.kind))
        refuse("native row-rule variable cast workspace");
      fixedRule(expr.input);
      break;
    case "not":
    case "isNull":
      fixedRule(expr.arg);
      break;
    case "inList":
      fixedRule(expr.arg);
      for (const child of expr.list) fixedRule(child);
      break;
    case "case":
      for (const branch of expr.branches) {
        fixedRule(branch.when);
        fixedRule(branch.then);
      }
      if (expr.otherwise) fixedRule(expr.otherwise);
      break;
    case "like":
      // Pattern compilation may allocate decoded strings proportional to input.
      refuse("native row-rule LIKE workspace");
      break;
    case "param":
      refuse("parameterized native row rule");
  }
}
function qualify(program: TableRules): void {
  const p = tableRuleProgram(program);
  if (!p) return;
  for (const check of p.checks) fixedRule(check.condition);
  for (const generated of p.generated) {
    if (["text", "bytes", "json", "array", "decimal", "enum"].includes(generated.expr.type.kind))
      refuse("variable-width native generated result");
    fixedRule(generated.expr);
  }
}
function minimum(binding: Pick<Binding, "table" | "program">): number {
  return tableRuleProgram(binding.program)
    ? tableRuleMinimumBytes(binding.program, 1)
    : columnRuleMinimumBytes(binding.table, 1);
}
function checkPlan(plan: PhysicalPlan): void {
  const root = plan.root;
  if (
    root.op !== "modify" ||
    !["insert", "update", "delete"].includes(root.kind) ||
    root.onConflict ||
    root.returning ||
    plan.columns.length
  )
    refuse("native modification RETURNING or conflict action");
  if (plan.volatile || plan.ftsQueries?.length || plan.initPlans.length)
    refuse("volatile, FTS or init-plan native modification input");
}
/** Runs a qualified ordinary-owner modification. Returns the root count and
 * invalidation reach; no child-count approximation is made. */
export async function executeNativeCommand(
  request: NativeCommandRequest,
): Promise<NativeCommandResult> {
  const { transaction, signal, plan, governor } = request;
  signal.throwIfAborted();
  checkPlan(plan);
  const catalog = transaction.catalog;
  if (catalog !== request.context.catalog || catalog.version !== plan.catalogVersion)
    throw new TypeError("Stale native command catalog");
  const bindings: Binding[] = [];
  const statement = transaction.statement({
    signal,
    rules: (id) => {
      const binding = bindings.find((binding) => binding.table.id === id);
      if (!binding?.rules) throw new TypeError("Unbound native command rules");
      return binding.rules;
    },
  });
  if (!(statement instanceof StatementImpl)) {
    statement.rollback();
    throw new TypeError("Native commands require the native statement provider");
  }
  let credits: NativeRootCredits | null = null,
    reach: NativeCommandReach | null = null,
    root: NativeCommandRoot | null = null,
    retained = 0;
  let result: NativeCommandResult = { rowCount: 0, tables: [] },
    failure: { reason: unknown } | null = null;
  const cleanup: unknown[] = [];
  try {
    credits = await NativeRootCredits.open({ governor, statement, signal, includesProgress: true });
    const paid = credits;
    await paid.reserve(
      { metadata: NATIVE_ROOT_CREDIT_BYTES + nativeCommandReachBytes(catalog) },
      signal,
    );
    reach = await NativeCommandReach.prepare(
      catalog,
      plan.root as Extract<PhysicalPlan["root"], { op: "modify" }>,
      paid.metadata,
      { signal },
    );
    const triggers: Array<NativeRootPreparation["triggers"][number]> = [],
      defaults: Array<NativeRootPreparation["defaults"][number]> = [];
    let ruleBytes = 0,
      prelude =
        NATIVE_INPUT_CAPTURE_BYTES + PREPAID_QUERY_BYTES + nativeParameterMinimumBytes(plan);
    // Lower immutable programs only. No default/sequence/generated expression is evaluated
    // to size a workspace, and no source row or image is retained by this preparation.
    for (const id of reach.tables) {
      signal.throwIfAborted();
      const table = catalog.table(id);
      if (!table) throw new TypeError("Missing native command table");
      if (table.columns.some((c) => c.autoIncrement === true))
        refuse("native trigger auto-increment preflight");
      for (const index of table.indexes)
        if (
          index.columnIds.length > 16 ||
          index.columnIds.some((id) => {
            const column = table.columns.find((c) => c.id === id);
            return !column || ["varlen", "decimal"].includes(layoutOf(column.type));
          })
        )
          refuse("native trigger indexed-domain preflight");
      const expr = request.planner.lowerTableRules(table, catalog),
        program = compileTableRules(table, catalog.version, expr),
        entry: Binding = { table, program, minimum: 0, rules: null };
      qualify(program);
      const bytes = minimum(entry);
      bindings.push({ ...entry, minimum: bytes });
      ruleBytes += bytes;
      for (const column of table.columns)
        if (column.generatedSql === null)
          defaults.push({
            tableId: table.id,
            columnId: column.id,
            expr: expr.defaults?.find((d) => d.columnId === column.id)?.expr ?? null,
          });
    }
    for (const definition of reach.triggers) {
      const table = catalog.table(definition.tableId);
      if (!table) throw new TypeError("Missing native trigger table");
      const bodies: TriggerBodyRunner[] = [];
      for (const body of definition.statements) {
        signal.throwIfAborted();
        const lowered = request.planner.lower(body.sql, {
          ...request.context,
          parameterTypes: body.bindings.map((binding) => binding.type),
        });
        if (lowered.kind !== "modify") refuse("non-modifying native trigger body");
        checkPlan(lowered.plan);
        bodies.push(new TriggerBodyRunner(table, body, lowered.plan));
        prelude = Math.max(
          prelude,
          NATIVE_INPUT_CAPTURE_BYTES +
            PREPAID_QUERY_BYTES +
            nativeParameterMinimumBytes(lowered.plan),
        );
      }
      triggers.push({ definition, bodies });
    }
    // Reach already supplies the UTF8 name/ID order. Preparation/header arrays and rule IR
    // have their own retained charge in addition to the assembler's mutable objects.
    const preparation: NativeRootPreparation = {
      rootId: request.rootId,
      catalog,
      frames: reach.frames,
      rowEvents: reach.rowEvents,
      root: plan,
      triggers,
      tableIds: reach.tables,
      defaults,
      clock: clockParameter({ kind: "timestamp", zoned: true }, request.execution.statementTime),
    };
    const preparationBytes = retainedBytes({
      bindings: bindings.map((binding) => ({
        tableId: binding.table.id,
        minimum: binding.minimum,
      })),
      triggers: triggers.map((trigger) => ({
        id: trigger.definition.id,
        bodies: trigger.bodies.map((body) => body.plan),
      })),
      defaults,
      clock: preparation.clock,
    });
    const fixedMetadata =
      NATIVE_ROOT_CREDIT_BYTES +
      nativeCommandReachBytes(catalog) +
      preparationBytes +
      nativeCommandRootSetupBytes(preparation) +
      prelude;
    await paid.reserve(
      {
        metadata: fixedMetadata,
        rules: ruleBytes,
        parameters: needsParameterSizing(plan, request.parameters) ? PARAMETER_SIZING_BYTES : 0,
      },
      signal,
    );
    if (!paid.metadata.allocate(preparationBytes))
      throw new Error("Native preparation was not funded");
    retained = preparationBytes;
    const parameters = await prepareParameters(
      plan,
      request.parameters,
      signal,
      governor.capBytes,
      paid.parameters,
    );
    await paid.reserve(
      { metadata: fixedMetadata, rules: ruleBytes, parameters: parameters.bytes },
      signal,
    );
    const bound = await parameters.bind(paid.parameters, request.execution.statementTime);

    for (const binding of bindings)
      binding.rules = tableRuleProgram(binding.program)
        ? bindTableRules(binding.program, paid.rules, { capacity: 1, signal })
        : bindColumnRules(binding.table, paid.rules, { capacity: 1, signal });
    root = await NativeCommandRoot.create({
      ...preparation,
      statement,
      tables: bindings.map((binding) => {
        if (!binding.rules) throw new TypeError("Missing native rule binding");
        return { table: binding.table, rules: binding.rules };
      }),
      governor,
      signal,
      execution: request.execution,
      funding: {
        metadata: paid.metadata,
        source: paid.source,
        prepareMetadata: (n, s) => paid.prepareMetadata(n, s),
        prepareSource: (n, s) => paid.prepareSource(n, s),
        prepareRules: (_table, _image, _phase, s) => paid.protectRules(ruleBytes, s),
        releaseIdleStep: () => paid.releaseIdleStep(),
      },
      checkpoint: async () => {
        signal.throwIfAborted();
        const pause = pauseIfDue({ signal });
        if (pause) await pause;
        signal.throwIfAborted();
      },
    });
    const rowCount = await root.run({ slots: bound.slots.slice(0, plan.placeholderCount) });
    await root.end();
    result = { rowCount, tables: reach.tables.slice() };
  } catch (reason) {
    failure = { reason };
  }
  // Keep caller bindings and credit until every producer/queue/image/IO fence completes.
  try {
    await root?.close();
  } catch (error) {
    cleanup.push(error);
  }
  if (failure || cleanup.length) {
    try {
      statement.rollback();
    } catch (error) {
      cleanup.push(error);
    }
  }
  for (const binding of bindings) {
    try {
      binding.rules?.close();
    } catch (error) {
      cleanup.push(error);
    }
  }
  try {
    reach?.close();
  } catch (error) {
    cleanup.push(error);
  }
  if (credits) {
    try {
      if (credits.parameters.allocated) credits.parameters.free(credits.parameters.allocated);
      if (retained) credits.metadata.free(retained);
      await credits.close();
    } catch (error) {
      cleanup.push(error);
    }
  }
  if (cleanup.length)
    throw new AggregateError(
      failure ? [failure.reason, ...cleanup] : cleanup,
      "Native command retirement failed",
      { cause: failure?.reason ?? cleanup[0] },
    );
  if (failure) throw failure.reason;
  return result;
}
