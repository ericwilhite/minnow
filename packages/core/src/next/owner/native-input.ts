/** Compiled, non-ending modification producer. It owns no statement or pin; its caller
 * owns the sealed input and applies row effects only after this producer has retired. */
import type { Snapshot } from "../access/snapshot.js";
import { SnapshotReleasedError } from "../access/snapshot.js";
import type { PhysicalPlan } from "../ir/ir.js";
import { bigintBytes, stringBytes } from "../memory/governor.js";
import type { ByteCharge } from "../vector/varlen.js";
import { compileModificationInput } from "../exec/plan.js";
import { NativeParameterBinding } from "../exec/native-parameters.js";
import { QueryRuntime, type Program, type ProgramSink } from "../exec/run.js";
import { ResultBuffers } from "../exec/result-buffer.js";
import type { ImageCaptureContext } from "./registry-images.js";
import { DeferredGrant } from "../exec/deferred-grant.js";
import {
  NumericOutOfRangeError,
  PlanStaleError,
  rowValueError,
  UnsupportedPlanError,
  type ExecutionError,
} from "../exec/errors.js";
import type { BoundParameters, ExecuteRequest } from "../exec/executor.js";
import { CardinalityViolationError } from "../access/modify.js";
import type { NativeInputTargets } from "../exec/native-target-guard.js";
import type { Operator } from "../exec/pipeline.js";
/** Maximum legal PG NUMERIC (131072 integral + 16383 fractional digits) needs less
 * than 256 KiB for the codec's four BigInt/limb overlap copies. Varlen payloads stream. */
export const NATIVE_INPUT_CAPTURE_BYTES = 256 * 1024;
/** A transient producer keeps only its immutable plan reference between invocations. */
export const NATIVE_INPUT_HANDLE_BYTES = 256;
/** Retained literal buffers are counted once, at their real size. Recipe maps,
 * optimizer nodes and key strings use separate per-object metadata; multiplying a large
 * immutable payload would manufacture a minimum above the cap instead of spilling. */
export function nativeInputProgramBytes(plan: PhysicalPlan): number {
  const seen = new Set<object>();
  const size = (value: unknown): number => {
    if (typeof value === "string") return stringBytes(value);
    if (typeof value === "bigint") return bigintBytes(value);
    if (typeof value === "number") return 8;
    if (value === null || typeof value !== "object" || seen.has(value)) return 0;
    seen.add(value);
    if (ArrayBuffer.isView(value)) {
      if (seen.has(value.buffer)) return 64;
      seen.add(value.buffer);
      return 64 + value.buffer.byteLength;
    }
    let bytes = 2048;
    for (const key in value) bytes += size((value as Record<string, unknown>)[key]);
    if (!Number.isSafeInteger(bytes)) throw new RangeError("Native input IR too large");
    return bytes;
  };
  const bytes = 16384 + size(plan);
  if (!Number.isSafeInteger(bytes)) throw new RangeError("Native input program too large");
  return bytes;
}
export interface NativeInputRequest {
  readonly inputView: Snapshot;
  readonly parameters: BoundParameters;
  readonly signal: AbortSignal;
  /** Root-owned query workspace/governor facade and clocks; statement is always null here. */
  readonly execution: Omit<ExecuteRequest, "snapshot" | "parameters" | "statement" | "signal">;
  readonly metadata: ByteCharge;
  readonly targets?: NativeInputTargets;
}
export class NativeInputProgram {
  #program: Program | null = null;
  #charge: ByteCharge | null = null;
  readonly #bytes: number;
  #running: Promise<unknown> | null = null;
  constructor(
    readonly plan: PhysicalPlan,
    charge: ByteCharge,
    readonly transient = false,
  ) {
    // Eager source materialization must not move volatile side effects before an earlier
    // BEFORE/CHECK. These kernels remain refused until there is an ordered provider.
    if (plan.volatile) throw new UnsupportedPlanError("volatile native modification input");
    if (plan.ftsQueries?.length)
      throw new UnsupportedPlanError("native body FTS prefix preparation");
    this.#bytes = transient ? NATIVE_INPUT_HANDLE_BYTES : nativeInputProgramBytes(plan);
    if (!charge.allocate(this.#bytes))
      throw new RangeError("Native input program must be admitted");
    try {
      if (!transient) this.#program = compileModificationInput(plan);
      this.#charge = charge;
    } catch (error) {
      charge.free(this.#bytes);
      throw error;
    }
  }
  /** The returned error follows the sealed prefix. Storage/admission/cancel failures throw.
   * No execution-local ParamNode, operator graph or runtime remains when this resolves. */
  run(
    request: NativeInputRequest,
    sink: Operator | ((capture: ImageCaptureContext) => Operator),
  ): Promise<ExecutionError | CardinalityViolationError | null> {
    if (!this.#charge || this.#running)
      throw new TypeError("Native input program is closed or active");
    request.signal.throwIfAborted();
    if (request.inputView.released) throw new SnapshotReleasedError();
    if (request.inputView.catalog.version !== this.plan.catalogVersion)
      throw new PlanStaleError(this.plan.catalogVersion, request.inputView.catalog.version);
    const charge = this.#charge;
    let program = this.#program,
      compiledBytes = 0;
    if (this.transient) {
      const bytes = nativeInputProgramBytes(this.plan);
      if (!charge.allocate(bytes)) throw new RangeError("Transient input program must be admitted");
      try {
        program = compileModificationInput(this.plan);
      } catch (error) {
        charge.free(bytes);
        throw error;
      }
      compiledBytes = bytes;
    }
    if (!program) throw new TypeError("Native input program is not compiled");
    const input = program;
    // Publish #running before a producer/visitor can synchronously reenter close.
    const work = Promise.resolve().then(async () => {
      let binding: NativeParameterBinding | null = null,
        runtime: QueryRuntime | null = null;
      const deferred = new DeferredGrant("native input unused result share");
      let captured = 0;
      if (!request.metadata.allocate(NATIVE_INPUT_CAPTURE_BYTES))
        throw new RangeError("Native capture workspace must be admitted");
      const capture: ImageCaptureContext = {
        charge: {
          allocate(bytes) {
            if (!Number.isSafeInteger(bytes) || bytes < 0)
              throw new RangeError("Invalid capture byte count");
            if (bytes + captured > NATIVE_INPUT_CAPTURE_BYTES) return false;
            captured += bytes;
            return true;
          },
          free(bytes) {
            if (bytes < 0 || bytes > captured)
              throw new RangeError("Native capture allocation underflow");
            captured -= bytes;
          },
        },
        prepare(bytes, signal) {
          signal.throwIfAborted();
          if (bytes > NATIVE_INPUT_CAPTURE_BYTES)
            throw new NumericOutOfRangeError("numeric value out of range");
        },
      };
      let terminal: ExecutionError | CardinalityViolationError | null = null;
      let downstream: Operator | null = null;
      let failure: { reason: unknown } | null = null;
      try {
        downstream = typeof sink === "function" ? sink(capture) : sink;
        const active = downstream;
        const target: ProgramSink = {
          push: (batch, transfer) => active.push(batch, transfer),
          ready: () => active.ready(),
          finish: () => active.finish(),
          abort: (reason) => active.abort(reason),
          suspendBudget: async () => undefined,
          resumeBudget: () => undefined,
        };
        binding = new NativeParameterBinding(
          this.plan,
          request.parameters,
          request.metadata,
          request.signal,
        );
        runtime = new QueryRuntime(
          this.plan,
          {
            ...request.execution,
            snapshot: request.inputView,
            statement: null,
            parameters: [],
            signal: request.signal,
          },
          request.inputView,
          request.signal,
          binding,
        );
        runtime.nativeInputTargets = request.targets ?? null;
        runtime.resultAllocator(async (bytes) => {
          const loan = await deferred.borrow(bytes, request.signal);
          try {
            return new ResultBuffers(deferred, bytes, () => loan.repay());
          } catch (error) {
            loan.repay();
            throw error;
          }
        });
        runtime.enablePhases();
        runtime.atomicResult(deferred, 0);
        await input(runtime, target);
        request.signal.throwIfAborted();
        if (request.inputView.released) throw new SnapshotReleasedError();
      } catch (error) {
        failure = { reason: error };
        runtime?.abort(error);
        if (
          !request.signal.aborted &&
          (rowValueError(error) || error instanceof CardinalityViolationError)
        )
          terminal = error;
      }
      // drive() can reject promptly while an accepted sink still borrows its batch.
      // Fence it before retiring arenas/parameters, including cancellation and SQL errors.
      let cleanup: { reason: unknown } | null = null;
      try {
        await downstream?.ready();
      } catch (reason) {
        if (!failure) cleanup = { reason };
      }
      for (const close of [
        () => runtime?.close(),
        () => deferred.close(),
        () => binding?.retire(),
        () => request.metadata.free(NATIVE_INPUT_CAPTURE_BYTES),
      ]) {
        try {
          await close();
        } catch (reason) {
          cleanup ??= { reason };
        }
      }
      if (captured !== 0) cleanup ??= { reason: new Error("Native capture retained workspace") };
      if (failure && terminal === null) throw failure.reason;
      if (cleanup) throw cleanup.reason;
      return terminal;
    });
    const pending = work.finally(() => {
      if (compiledBytes) charge.free(compiledBytes);
      if (this.#running === pending) this.#running = null;
    });
    this.#running = pending;
    return pending;
  }
  /** Caller fences active source work with its root cancellation before closing programs. */
  async close(): Promise<void> {
    if (this.#running) await this.#running.catch(() => undefined);
    this.#program = null;
    if (this.#charge) {
      this.#charge.free(this.#bytes);
      this.#charge = null;
    }
  }
}
