/** One non-ending trigger-body command. Source materialization and native writes share the
 * admitted root; the host supplies execution, never an independent PreparedPlan.execute().
 */
import type { TableDef } from "../access/catalog.js";
import type { Statement } from "../access/modify.js";
import type { Snapshot } from "../access/snapshot.js";
import type { TriggerBody } from "../access/trigger-def.js";
import type {
  CommandToken,
  NativeCommandBoundary,
  NativeRowHooks,
  RowImageRef,
} from "../access/statement-command.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { TaskWork } from "../task.js";
import { samePhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { BoundParameters } from "./executor.js";
import { ExecutionError, PlanStaleError } from "./errors.js";
import { TriggerBindingProgram, type OwnedTriggerBindings } from "./trigger-bindings.js";

/** Opaque root-owned sealed input. Provider validates origin/liveness on apply AND retirement.
 * It contains no vectors, borrowed pages, operators, closures or grants. */
export interface TriggerBodyInput {
  readonly rootId: number;
  readonly id: number;
}
interface BodyRequest {
  readonly command: CommandToken;
  /** COMPLETE modification, including deferred candidate/rule and terminal-error metadata.
   * The host extracts its input without moving a later row's error before an earlier rule. */
  readonly plan: PhysicalPlan;
  readonly body: TriggerBody;
  readonly inputView: Snapshot;
  readonly signal: AbortSignal;
}
export interface TriggerBodyHost {
  /** Root-funded source factory. Parameters are already typed (leading placeholder slots);
   * reuse root clocks/sequences and fill internal slots without raw JS value rebinding.
   * Seal input, drop execution-local operators/ParamNode caches, drain all read/progress
   * borrows BEFORE resolve/reject. Native root owns any not-yet-delivered pending input.
   * No independent governor admission, statement end, delivery or commit. */
  spoolBodyInput(
    request: BodyRequest & { readonly parameters: BoundParameters },
  ): Promise<TriggerBodyInput>;
  /** Consume the sealed descriptor through the root's command/hooks-aware native row driver.
   * Preserve row-order BEFORE/rules/primary; inputView is now locator-only after first write.
   * No borrowed spool page/loan over callbacks or writer-growth waits. Returned promise fences
   * all input borrows, including failure/cancellation. Root separately retains accepted images
   * until queued uses retire; retiring this input cannot invalidate AFTER image references. */
  applyBodyInput(
    request: BodyRequest & { readonly input: TriggerBodyInput; readonly hooks: NativeRowHooks },
  ): Promise<void>;
  /** Origin-checked, idempotent and IO-fenced, including rejection. Does not retire images owned
   * by the AFTER registry. No callback/effect; root retains final cleanup ownership on failure. */
  releaseBodyInput(command: CommandToken, input: TriggerBodyInput): Promise<void>;
}
export interface TriggerBodyScope {
  readonly statement: Pick<Statement, "catalog" | "readOwn">;
  readonly native: Pick<NativeCommandBoundary, "readRowBindings">;
  readonly host: TriggerBodyHost;
  readonly hooks: NativeRowHooks;
  /** Prepaid root workspace, with max live binding metadata and exact unique copy progress. */
  readonly charge: ByteCharge;
  readonly work: TaskWork & { readonly signal: AbortSignal };
}
function invalid(message: string): never {
  throw new ExecutionError(`Invalid trigger body ${message}`, "MINNOW_TRIGGER_BODY_STATE", "XX000");
}
export class TriggerBodyRunner {
  readonly plan: PhysicalPlan;
  readonly #bindings: TriggerBindingProgram;
  get minimumBytes(): number {
    return this.#bindings.minimumBytes;
  }
  constructor(
    owner: TableDef,
    readonly body: TriggerBody,
    plan: PhysicalPlan,
  ) {
    const root = plan.root;
    if (
      root.op !== "modify" ||
      root.kind !== body.kind ||
      root.tableId !== body.targetTableId ||
      root.returning !== null ||
      root.onConflict !== null ||
      plan.columns.length !== 0
    )
      invalid("modification identity/shape");
    if (
      plan.placeholderCount !== body.bindings.length ||
      body.bindings.some((b, i) => {
        const parameter = plan.parameters[i];
        return parameter === undefined || !samePhysicalType(b.type, parameter);
      })
    )
      invalid("parameter types");
    // Product body cardinality contract, scoped to this command. Other body commands may
    // legitimately touch that row again. Preserve every other current/future modify field.
    this.plan = { ...plan, root: { ...root, onRepeat: "error" } };
    this.#bindings = new TriggerBindingProgram(owner, body.bindings);
  }
  async run(command: CommandToken, images: RowImageRef, scope: TriggerBodyScope): Promise<void> {
    const { statement, native, host, hooks, charge, work } = scope;
    work.signal.throwIfAborted();
    if (statement.catalog.version !== this.plan.catalogVersion)
      throw new PlanStaleError(this.plan.catalogVersion, statement.catalog.version);
    let pin: Snapshot | null = null,
      input: TriggerBodyInput | null = null,
      parameters: OwnedTriggerBindings | null = null;
    let failure: { reason: unknown } | null = null;
    const cleanup: unknown[] = [];
    try {
      pin = statement.readOwn();
      parameters = await this.#bindings.bind(images, native, charge, work);
      work.signal.throwIfAborted();
      input = await host.spoolBodyInput({
        command,
        plan: this.plan,
        body: this.body,
        parameters,
        inputView: pin,
        signal: work.signal,
      });
      work.signal.throwIfAborted();
      if (input.rootId !== command.rootId || !Number.isSafeInteger(input.id) || input.id < 1)
        invalid("input origin");
      // ParamNode caches must already be gone. No parent binding copy survives into a nested
      // BEFORE/AFTER body or a writer wait; one shared image/binding helper is sufficient.
      parameters.close();
      parameters = null;
      work.signal.throwIfAborted();
      await host.applyBodyInput({
        command,
        plan: this.plan,
        body: this.body,
        input,
        inputView: pin,
        hooks,
        signal: work.signal,
      });
      work.signal.throwIfAborted();
    } catch (reason) {
      failure = { reason };
    } finally {
      try {
        parameters?.close();
      } catch (error) {
        cleanup.push(error);
      }
      if (input)
        try {
          await host.releaseBodyInput(command, input);
        } catch (error) {
          cleanup.push(error);
        }
      try {
        pin?.release();
      } catch (error) {
        cleanup.push(error);
      }
    }
    if (cleanup.length)
      throw new AggregateError(
        failure ? [failure.reason, ...cleanup] : cleanup,
        "Trigger body cleanup failed",
        failure ? { cause: failure.reason } : undefined,
      );
    if (failure) throw failure.reason;
    // Retirement can suspend too. Cancellation during that fence must not turn a body
    // into a successful command merely because all primary writes already finished.
    work.signal.throwIfAborted();
    // Caller verifies command uniqueness, drains its nested AFTER frame, then verifies references.
    // It must not call root end/commit here; root alone owns rollback after all fences drain.
  }
}
