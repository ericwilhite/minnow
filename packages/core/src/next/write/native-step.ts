/** One writer's funding witness and pressure wakeup, never an execution or governor. */
import type {
  NativeWriteStepBoundary,
  NativeWriteStepOptions,
} from "../access/native-write-step.js";
import { NativeWriteStepChangedError } from "../access/native-write-step.js";
import { retryGuard } from "../memory/retry-guard.js";
import { TransactionStateError, WritePathClosedError } from "../access/write-path.js";
import { MemoryAdmissionError, type HolderChain, type MemoryGovernor } from "../memory/governor.js";
import type { ByteCharge } from "../vector/varlen.js";

/** Controller, one preparing request, one witness and one cancellable wakeup. */
export const NATIVE_WRITE_STEP_BYTES = 4096;

export interface NativeWriteStepScope {
  readonly charge: ByteCharge;
  readonly governor: MemoryGovernor;
  /** The writer's holder chain, handed to every boundary. */
  readonly holderChain: HolderChain;
  check(owner: object): void;
  protectedBytes(): number;
  stabilize(owner: object, rootBytes: number, capacity: number, signal: AbortSignal): Promise<void>;
}

class Boundary implements NativeWriteStepBoundary {
  readonly #stop = new AbortController();
  #parent: NativeWriteSteps | null;
  readonly invalidated = this.#stop.signal;
  constructor(
    parent: NativeWriteSteps,
    readonly protectedWriteBytes: number,
    readonly ordinaryCapacity: number,
    readonly capacityBytes: number,
    readonly holderChain: HolderChain,
  ) {
    this.#parent = parent;
  }
  invalidate(reason: unknown): void {
    if (this.#parent) this.#stop.abort(reason);
  }
  check(): void {
    if (!this.#parent) throw new TransactionStateError("Native write boundary was released");
    this.#parent.check(this);
    this.invalidated.throwIfAborted();
  }
  release(): void {
    const parent = this.#parent;
    if (!parent) return;
    this.#parent = null;
    parent.release(this);
  }
}

export class NativeWriteSteps {
  #scope: NativeWriteStepScope | null;
  #owner: object | null = null;
  #request: AbortController | null = null;
  #pending: Promise<NativeWriteStepBoundary> | null = null;
  #live: Boundary | null = null;
  #version = 0;
  #wake: (() => void) | null = null;
  #closed = false;
  #closing: Promise<void> | null = null;
  constructor(scope: NativeWriteStepScope) {
    if (!scope.charge.allocate(NATIVE_WRITE_STEP_BYTES))
      throw new RangeError("Native write-step control must be admitted");
    this.#scope = scope;
  }
  private get scope(): NativeWriteStepScope {
    if (this.#closed || !this.#scope) throw new WritePathClosedError();
    return this.#scope;
  }
  get version(): number {
    return this.#version;
  }
  get funding(): boolean {
    return this.#owner !== null;
  }
  changed(growth = false): void {
    if (!this.#pending && !this.#live) return;
    this.#version = (this.#version + 1) >>> 0;
    const wake = this.#wake;
    this.#wake = null;
    wake?.();
    const live = this.#live;
    if (live && (growth || this.scope.protectedBytes() !== live.protectedWriteBytes))
      live.invalidate(new NativeWriteStepChangedError());
  }
  wait(version: number, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    if (version !== this.#version) return Promise.resolve();
    if (this.#wake) throw new TransactionStateError("Concurrent native pressure waits");
    return new Promise<void>((resolve, reject) => {
      const clear = () => {
        if (this.#wake === ready) this.#wake = null;
        signal.removeEventListener("abort", abort);
      };
      const ready = () => {
        clear();
        resolve();
      };
      const abort = () => {
        clear();
        // Cancellation preserves arbitrary caller reasons, including primitive values.
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
        reject(signal.reason);
      };
      this.#wake = ready;
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
      else if (version !== this.#version) ready();
    });
  }
  prepare(
    owner: object,
    rootBytes: number,
    signal: AbortSignal,
    options: NativeWriteStepOptions = {},
  ): Promise<NativeWriteStepBoundary> {
    const scope = this.scope;
    scope.check(owner);
    signal.throwIfAborted();
    if (!Number.isSafeInteger(rootBytes) || rootBytes < 0)
      throw new RangeError("Native root memory must be a nonnegative safe integer");
    if (this.#owner) throw new TransactionStateError("Previous native write funding is live");
    const ordinary = scope.governor.capBytes - scope.governor.reserveBytes,
      capacity = options.includesProgress === true ? scope.governor.capBytes : ordinary;
    if (rootBytes > capacity) throw new MemoryAdmissionError("native root", rootBytes, capacity);
    const request = new AbortController(),
      stop = AbortSignal.any([signal, request.signal]);
    this.#owner = owner;
    this.#request = request;
    const pending = Promise.resolve().then(async () => {
      const guard = retryGuard("native write step boundary");
      for (;;) {
        const version = this.#version;
        await scope.stabilize(owner, rootBytes, capacity, stop);
        scope.check(owner);
        stop.throwIfAborted();
        // A native change can land after stabilize returns, before this continuation.
        // Retry preparation, not SQL, rather than publish unfunded/stale headroom or
        // classify a temporary change as an impossible compound minimum.
        if (version !== this.#version) {
          guard.retry();
          continue;
        }
        const bytes = scope.protectedBytes();
        if (bytes + rootBytes > capacity)
          throw new MemoryAdmissionError("native root and writer", bytes + rootBytes, capacity);
        const boundary = new Boundary(this, bytes, ordinary, capacity, scope.holderChain);
        this.#live = boundary;
        return boundary;
      }
    });
    this.#pending = pending;
    void pending.then(
      () => this.#finished(pending),
      () => this.#finished(pending),
    );
    return pending;
  }
  #finished(pending: Promise<NativeWriteStepBoundary>): void {
    if (this.#pending !== pending) return;
    this.#pending = null;
    this.#request = null;
    if (!this.#live) this.#owner = null;
  }
  check(boundary: Boundary): void {
    if (this.#live !== boundary || !this.#owner)
      throw new TransactionStateError("Foreign native write boundary");
    this.scope.check(this.#owner);
    if (this.scope.protectedBytes() !== boundary.protectedWriteBytes)
      boundary.invalidate(new NativeWriteStepChangedError());
  }
  release(boundary: Boundary): void {
    if (this.#live !== boundary) throw new TransactionStateError("Foreign native write boundary");
    this.#live = null;
    this.#owner = null;
  }
  checkReady(owner: object): void {
    if (this.#owner === owner)
      throw new TransactionStateError("Retire native funding before ending the statement");
  }
  async closeOwner(owner: object, reason: unknown): Promise<void> {
    if (this.#owner !== owner) return;
    this.#request?.abort(reason);
    this.#live?.invalidate(reason);
    this.#live?.release();
    await this.#pending?.then(
      () => undefined,
      () => undefined,
    );
  }
  cancel(reason: unknown): void {
    this.#request?.abort(reason);
    this.#live?.invalidate(reason);
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.cancel(new WritePathClosedError());
      this.#live?.release();
      await this.#pending?.then(
        () => undefined,
        () => undefined,
      );
      this.#closed = true;
      const scope = this.#scope;
      this.#scope = null;
      scope?.charge.free(NATIVE_WRITE_STEP_BYTES);
    })());
  }
}
