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
import {
  bodyPreparationBound,
  closurePreparationBound,
  containerPreparationBound,
  rulePreparationBound,
} from "./native-preparation-bound.js";
import { withCopyMeter, type CopyMeter } from "../../engine/copy-meter.js";
import { MemoryAdmissionError } from "../memory/governor.js";
import type { TriggerBody } from "../access/trigger-def.js";
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
interface Want {
  readonly metadata: number;
  readonly rules: number;
  readonly parameters: number;
  /** The known need behind a doubled reservation, admitted instead if doubling overshoots. */
  readonly need?: number | undefined;
}
/** The initial reservation, as a multiple of the base shares over a static reach superset. */
export const PREPARATION_INITIAL_FACTOR = 2;
/** Raised by the preparation meter; the attempt that sees it is discarded and retried. */
const PREPARATION_SHORT = new Error("Native preparation reservation exceeded");

/** Lowers one stored trigger body with its base share and every copy metered (shared with the
 * calibration tests, so they check the same path). */
export function lowerBodyMetered(
  request: Pick<NativeCommandRequest, "planner" | "context">,
  catalog: NativeCommandRequest["context"]["catalog"],
  body: TriggerBody,
  meter: CopyMeter,
): ReturnType<StatementPlanner["lower"]> {
  meter.charge(bodyPreparationBound(body, catalog));
  const lowered = withCopyMeter(meter, () =>
    request.planner.lower(body.sql, {
      ...request.context,
      parameterTypes: body.bindings.map((binding) => binding.type),
    }),
  );
  return lowered;
}
interface Prepared {
  readonly preparation: NativeRootPreparation;
  readonly fixedMetadata: number;
  readonly ruleBytes: number;
}
/** The complete initial working set last needed by a cached root plan for one catalog. Lets
 * the next execution admit it at once instead of learning it by one released attempt. */
const learnedWants = new WeakMap<PhysicalPlan, Want>();
/** What a plan learns from a completed preparation: its measured need, never the padding of
 * the reservation that found it, and no parameter bytes (each execution sizes its own). */
export function learnedEntry(prepared: {
  readonly fixedMetadata: number;
  readonly ruleBytes: number;
}): Want {
  return {
    metadata: prepared.fixedMetadata,
    need: prepared.fixedMetadata,
    rules: prepared.ruleBytes,
    parameters: 0,
  };
}
/** Test view of a plan's learned preparation entry (its measured need, without padding). */
export function learnedNativePreparation(plan: PhysicalPlan): Readonly<Want> | undefined {
  return learnedWants.get(plan);
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
    // Metered preparation with a growing reservation. The meter charges each base share (linear
    // in stored text, native-preparation-bound.ts) and every copy the parser, optimizer and
    // lowering make of one source subtree (engine/copy-meter.ts) to the root's metadata before
    // it is allocated. When a charge does not fit, the attempt aborts: every attempt object and
    // charge is released, and the reservation is admitted again from nothing at
    // max(known need, min(2 x previous, cap)). Built-size workspaces (setup, rules, parameters)
    // are checked without waiting the same way. No wait holds preparation; attempts are at most
    // ceil(log2(cap / initial)) + 1; only a need above the cap is a typed 53200. The final
    // reservation is learned per cached root plan, so a later execution admits it at once.
    const reachBytes = nativeCommandReachBytes(catalog),
      base = NATIVE_ROOT_CREDIT_BYTES + reachBytes,
      closure = closurePreparationBound(catalog, (plan.root as { tableId: number }).tableId);
    // The initial size is an estimate; only control and reach are a certain need, so an
    // estimate that cannot be granted falls back to them rather than refusing.
    // Parameter sizing depends on this execution's values, never on a cached plan's last run.
    // A learned entry is the plan's measured need, without the doubling padding that found it:
    // reuse admits exactly that, so it neither waits for nor is refused over unused padding.
    // Size this execution's parameters first, in their own admission holding nothing, so every
    // attempt admits their exact bytes. Sizing retains nothing once it returns.
    if (needsParameterSizing(plan, request.parameters))
      await paid.reserve({ parameters: PARAMETER_SIZING_BYTES }, signal);
    const parameters = await prepareParameters(
      plan,
      request.parameters,
      signal,
      governor.capBytes,
      paid.parameters,
    );
    if (paid.parameters.allocated) throw new TypeError("Parameter sizing retained credit");
    paid.releaseAll();
    const learned = learnedWants.get(plan);
    let want: Want = learned
      ? { ...learned, parameters: parameters.bytes }
      : {
          metadata: Math.min(governor.capBytes, base + PREPARATION_INITIAL_FACTOR * closure.bytes),
          need: base,
          rules: 0,
          parameters: parameters.bytes,
        };
    let prepared: Prepared | null = null;
    while (!prepared) {
      signal.throwIfAborted();
      await admit(want, want.need ?? want.metadata);
      const outcome = await prepareWithin();
      if ("retry" in outcome) {
        paid.releaseAll();
        if (want.metadata >= governor.capBytes && outcome.retry.metadata > want.metadata)
          throw new MemoryAdmissionError(
            "native trigger preparation",
            outcome.retry.metadata,
            governor.capBytes,
          );
        want = {
          metadata: Math.max(
            outcome.retry.metadata,
            Math.min(2 * want.metadata, governor.capBytes),
          ),
          need: outcome.retry.metadata,
          rules: Math.max(want.rules, outcome.retry.rules),
          parameters: parameters.bytes,
        };
      } else prepared = outcome;
    }
    learnedWants.set(plan, learnedEntry(prepared));
    const { preparation } = prepared;
    if (
      !paid.cover({
        metadata: prepared.fixedMetadata,
        rules: prepared.ruleBytes,
        parameters: parameters.bytes,
      })
    )
      throw new TypeError("Native preparation lost its admitted credit");
    /** Admits `target`; when doubling overshot what the cap can ever grant beside the writer,
     * admits the known need instead, so a refusal means the need itself is above the cap. */
    async function admit(target: Want, need: number): Promise<void> {
      const { rules, parameters } = target;
      let { metadata } = target;
      for (;;) {
        try {
          await paid.reserve({ metadata, rules, parameters }, signal);
          want = { ...want, metadata };
          return;
        } catch (error) {
          // The refusal reports the joint minimum it could not fit under the cap; the largest
          // reservation that fits beside the writer is that much smaller. A known need above
          // it is a real refusal.
          if (!(error instanceof MemoryAdmissionError) || metadata <= need) throw error;
          const fit = metadata - Math.max(1, error.bytes - error.capacity);
          if (fit < need) throw error;
          paid.releaseAll();
          metadata = fit;
        }
      }
    }
    async function prepareWithin(): Promise<Prepared | { retry: Want }> {
      const triggers: Array<NativeRootPreparation["triggers"][number]> = [],
        defaults: Array<NativeRootPreparation["defaults"][number]> = [];
      let ruleBytes = 0,
        prelude =
          NATIVE_INPUT_CAPTURE_BYTES + PREPAID_QUERY_BYTES + nativeParameterMinimumBytes(plan),
        preparationBytes = 0,
        short: number | null = null;
      // Charges before allocating; a shortfall aborts the attempt wherever it is raised.
      const meter: CopyMeter = {
        charge(bytes) {
          if (short !== null) throw PREPARATION_SHORT;
          if (!Number.isSafeInteger(bytes) || bytes < 0) throw new RangeError("Invalid charge");
          if (!paid.metadata.allocate(bytes)) {
            short = preparationBytes + bytes;
            throw PREPARATION_SHORT;
          }
          retained += bytes;
          preparationBytes += bytes;
        },
      };
      const release = (): void => {
        bindings.length = 0;
        reach?.close();
        reach = null;
        if (retained) paid.metadata.free(retained);
        retained = 0;
        if (paid.parameters.allocated) paid.parameters.free(paid.parameters.allocated);
      };
      try {
        reach = await NativeCommandReach.prepare(
          catalog,
          plan.root as Extract<PhysicalPlan["root"], { op: "modify" }>,
          paid.metadata,
          { signal },
        );
        const prepared = reach;
        const tables = prepared.tables.map((id) => {
          const table = catalog.table(id);
          if (!table) throw new TypeError("Missing native command table");
          return table;
        });
        let bodyCount = 0;
        for (const definition of prepared.triggers) bodyCount += definition.statements.length;
        // Preparation object, its arrays, one binding/trigger entry each, and the clock.
        meter.charge(containerPreparationBound(tables.length, prepared.triggers.length, bodyCount));
        const clock = clockParameter(
          { kind: "timestamp", zoned: true },
          request.execution.statementTime,
        );
        for (const table of tables) {
          signal.throwIfAborted();
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
          meter.charge(rulePreparationBound(table));
          const expr = withCopyMeter(meter, () => request.planner.lowerTableRules(table, catalog)),
            program = compileTableRules(table, catalog.version, expr);
          qualify(program);
          const rows: Array<NativeRootPreparation["defaults"][number]> = [];
          for (const column of table.columns)
            if (column.generatedSql === null)
              rows.push({
                tableId: table.id,
                columnId: column.id,
                expr: expr.defaults?.find((d) => d.columnId === column.id)?.expr ?? null,
              });
          const bytes = minimum({ table, program });
          bindings.push({ table, program, minimum: bytes, rules: null });
          ruleBytes += bytes;
          for (const row of rows) defaults.push(row);
        }
        for (const definition of prepared.triggers) {
          const table = catalog.table(definition.tableId);
          if (!table) throw new TypeError("Missing native trigger table");
          const bodies: TriggerBodyRunner[] = [];
          for (const body of definition.statements) {
            signal.throwIfAborted();
            const lowered = lowerBodyMetered(request, catalog, body, meter);
            if (lowered.kind !== "modify") refuse("non-modifying native trigger body");
            checkPlan(lowered.plan);
            const runner = new TriggerBodyRunner(table, body, lowered.plan);
            bodies.push(runner);
            prelude = Math.max(
              prelude,
              NATIVE_INPUT_CAPTURE_BYTES +
                PREPAID_QUERY_BYTES +
                nativeParameterMinimumBytes(runner.plan),
            );
          }
          triggers.push({ definition, bodies });
        }
        // Reach already supplies the UTF8 name/ID order.
        const preparation: NativeRootPreparation = {
          rootId: request.rootId,
          catalog,
          frames: prepared.frames,
          rowEvents: prepared.rowEvents,
          root: plan,
          triggers,
          tableIds: prepared.tables,
          defaults,
          clock,
        };
        const fixedMetadata =
          base + preparationBytes + nativeCommandRootSetupBytes(preparation) + prelude;
        if (
          paid.metadata.held < fixedMetadata ||
          paid.rules.held < ruleBytes ||
          paid.parameters.held < parameters.bytes
        ) {
          release();
          return {
            retry: { metadata: fixedMetadata, rules: ruleBytes, parameters: parameters.bytes },
          };
        }
        return { preparation, fixedMetadata, ruleBytes };
      } catch (error) {
        release();
        // Set by the meter's closure; the aborted call may have wrapped its error, so the meter's
        // record is authoritative.
        const shortfall = short as number | null;
        if (shortfall === null) throw error;
        return { retry: { metadata: base + shortfall, rules: 0, parameters: 0 } };
      }
    }
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
        prepareRules: (_table, _image, _phase, s) => paid.protectRules(prepared.ruleBytes, s),
        releaseIdleStep: () => paid.releaseIdleStep(),
        prepareEnd: (s) => paid.prepareEnd(s),
        sourceRoom: () => paid.sourceRoom,
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
    result = { rowCount, tables: preparation.tableIds.slice() };
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
    // Assigned by the preparation attempt closure; flow analysis cannot see that.
    (reach as NativeCommandReach | null)?.close();
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
