/** One native root's accounts on the SAME governor. Funding only; never runs SQL.
 * Callers retire source/image/producer leases before growth and close their graph,
 * bindings and parameter buffers before these accounts. The write owner is not re-admitted. */
import type {
  NativeWriteStepBoundary,
  NativeWriteStepOptions,
} from "../access/native-write-step.js";
import type { Grant, MemoryGovernor } from "../memory/governor.js";
import { releaseIdle } from "../write/budget.js";

export const NATIVE_ROOT_CREDIT_BYTES = 4096;
const ROLES = ["metadata", "source", "rules", "parameters"] as const;
type Role = (typeof ROLES)[number];
export interface NativeRootCreditScope {
  readonly governor: MemoryGovernor;
  readonly signal: AbortSignal;
  readonly statement: {
    prepareNativeStep(
      bytes: number,
      signal: AbortSignal,
      options?: NativeWriteStepOptions,
    ): Promise<NativeWriteStepBoundary>;
  };
  /** Only a proven complete step can select this mode; pass it unchanged to both ports. */
  readonly includesProgress?: boolean;
}
function checked(n: number): number {
  if (!Number.isSafeInteger(n) || n < 0) throw new RangeError("Invalid native root credit");
  return n;
}
function changed(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "MINNOW_NATIVE_WRITE_STEP_CHANGED"
  );
}
function closeAll(grants: readonly Grant[] | null): void {
  for (const grant of grants ?? []) grant.close();
}

export class NativeRootCredits {
  readonly #stop = new AbortController();
  readonly #base = { metadata: NATIVE_ROOT_CREDIT_BYTES, source: 0, rules: 0, parameters: 0 };
  readonly #wanted = { ...this.#base };
  readonly #grants: Record<Role, Grant>;
  #pending: Promise<void> | null = null;
  #closing: Promise<void> | null = null;
  #closed = false;
  private constructor(
    readonly scope: NativeRootCreditScope,
    grants: readonly Grant[],
  ) {
    const [metadata, source, rules, parameters] = grants;
    if (!metadata || !source || !rules || !parameters)
      throw new TypeError("Incomplete native root accounts");
    if (!metadata.allocate(NATIVE_ROOT_CREDIT_BYTES))
      throw new Error("Native root control was not admitted");
    this.#grants = { metadata, source, rules, parameters };
    for (const grant of grants) grant.onReclaimRequest(() => this.#reclaim());
  }
  static async open(scope: NativeRootCreditScope): Promise<NativeRootCredits> {
    for (;;) {
      scope.signal.throwIfAborted();
      const boundary = await scope.statement.prepareNativeStep(
        NATIVE_ROOT_CREDIT_BYTES,
        scope.signal,
        { includesProgress: scope.includesProgress === true },
      );
      let grants: readonly Grant[] | null = null;
      try {
        boundary.check();
        grants = await scope.governor.admitGroup({
          label: "native root open",
          signal: AbortSignal.any([scope.signal, boundary.invalidated]),
          includesProgress: scope.includesProgress === true,
          grants: ROLES.map((role) => ({
            label: `native root ${role}`,
            minimumBytes: role === "metadata" ? NATIVE_ROOT_CREDIT_BYTES : 0,
            spillable: true,
          })),
        });
        boundary.check();
        return new NativeRootCredits(scope, grants);
      } catch (error) {
        closeAll(grants);
        scope.signal.throwIfAborted();
        if (!changed(error)) throw error;
      } finally {
        boundary.release();
      }
    }
  }
  get metadata(): Grant {
    return this.#grants.metadata;
  }
  get source(): Grant {
    return this.#grants.source;
  }
  get rules(): Grant {
    return this.#grants.rules;
  }
  get parameters(): Grant {
    return this.#grants.parameters;
  }
  #check(): void {
    this.scope.signal.throwIfAborted();
    this.#stop.signal.throwIfAborted();
    if (this.#closed) throw new TypeError("Native root credit is closed");
    if (this.#pending !== null) throw new TypeError("Concurrent native root funding");
  }
  #reclaim(): void {
    for (const role of ROLES) releaseIdle(this.#grants[role], this.#wanted[role]);
  }
  /** Retained setup/prelude minima, not free bytes. Metadata includes this controller's4096B.
   * A rule/image phase may add a higher temporary minimum without losing these bindings. */
  reserve(
    minimum: Partial<Record<Exclude<Role, "source">, number>>,
    signal: AbortSignal,
  ): Promise<void> {
    this.#check();
    for (const role of ROLES) {
      if (role === "source") continue;
      const n = minimum[role];
      if (n === undefined) continue;
      this.#base[role] = Math.max(role === "metadata" ? NATIVE_ROOT_CREDIT_BYTES : 0, checked(n));
      this.#wanted[role] = this.#base[role];
    }
    return this.#fund(signal);
  }
  prepareMetadata(bytes: number, signal: AbortSignal): Promise<void> {
    this.#check();
    this.#wanted.metadata = Math.max(
      this.#base.metadata,
      checked(this.metadata.allocated + checked(bytes)),
    );
    return this.#fund(signal);
  }
  prepareSource(bytes: number, signal: AbortSignal): Promise<void> {
    this.#check();
    this.#wanted.source = checked(this.source.allocated + checked(bytes));
    return this.#fund(signal);
  }
  /** Total rule working set; retained fixed bindings and variable evaluator output overlap once. */
  protectRules(bytes: number, signal: AbortSignal): Promise<void> {
    this.#check();
    this.#wanted.rules = Math.max(this.#base.rules, checked(bytes));
    return this.#fund(signal);
  }
  /** Only an existing safe boundary calls this. Allocated buffers are always preserved. */
  releaseIdleStep(): void {
    this.#check();
    for (const role of ROLES) this.#wanted[role] = this.#base[role];
    this.#reclaim();
  }
  #fund(signal: AbortSignal): Promise<void> {
    const stop = AbortSignal.any([this.scope.signal, this.#stop.signal, signal]);
    const work = (async () => {
      for (;;) {
        stop.throwIfAborted();
        this.#reclaim();
        let total = 0,
          missing = 0;
        const targets: number[] = [];
        for (const role of ROLES) {
          const grant = this.#grants[role],
            target = Math.max(grant.allocated, this.#wanted[role]);
          targets.push(target);
          total = checked(total + target);
          missing += Math.max(0, target - grant.held);
        }
        // A prepaid producer may use already-held metadata without another native
        // boundary. A NEW wait under its source would be hold-and-wait. Between
        // producers, restabilize the writer even when the root needs no new credit.
        if (missing === 0 && this.source.allocated !== 0) return;
        for (const role of ROLES)
          if (this.#grants[role].loaned !== 0)
            throw new TypeError("Native root funding under a progress loan");
        if (this.source.allocated !== 0)
          throw new TypeError("Native root funding while a source is retained");
        const boundary = await this.scope.statement.prepareNativeStep(total, stop, {
          includesProgress: this.scope.includesProgress === true,
        });
        let provisional: readonly Grant[] | null = null;
        const moved = [0, 0, 0, 0];
        try {
          boundary.check();
          if (missing === 0) {
            boundary.check();
            return;
          }
          const delta = ROLES.map((role, i) => ({
            label: `native root ${role} growth`,
            minimumBytes: Math.max(0, (targets[i] ?? 0) - this.#grants[role].held),
            spillable: true,
          }));
          provisional = await this.scope.governor.admitGroup({
            label: "native root step",
            grants: delta,
            includesProgress: this.scope.includesProgress === true,
            signal: AbortSignal.any([stop, boundary.invalidated]),
          });
          boundary.check();
          for (const [i, role] of ROLES.entries()) {
            const from = provisional[i],
              amount = delta[i]?.minimumBytes ?? 0;
            if (!from || !this.scope.governor.transferCredit(from, this.#grants[role], amount))
              throw new Error("Native root credit transfer failed");
            moved[i] = amount;
          }
          boundary.check();
          stop.throwIfAborted();
          return;
        } catch (error) {
          // No caller buffer can allocate before this promise resolves. Unwind ONLY
          // provisional idle funding before retrying the native witness, never SQL.
          for (let i = ROLES.length - 1; i >= 0; i--) {
            const role = ROLES[i],
              to = provisional?.[i],
              amount = moved[i] ?? 0;
            if (
              role &&
              to &&
              amount &&
              !this.scope.governor.transferCredit(this.#grants[role], to, amount)
            )
              throw new Error("Native root provisional transfer rollback failed", { cause: error });
          }
          stop.throwIfAborted();
          if (!changed(error)) throw error;
        } finally {
          closeAll(provisional);
          boundary.release();
        }
      }
    })();
    this.#pending = work;
    void work.then(
      () => {
        if (this.#pending === work) this.#pending = null;
      },
      () => {
        if (this.#pending === work) this.#pending = null;
      },
    );
    return work;
  }
  /** Caller first fences graph IO, then retires rules/parameters/preparation. A premature
   * call fails without uncharging borrowed data; retry close after those users retire. */
  close(): Promise<void> {
    if (this.#closing !== null) return this.#closing;
    this.#stop.abort(new TypeError("Native root credit closed"));
    const closing = (async () => {
      await this.#pending?.catch(() => undefined);
      if (this.#closed) return;
      for (const role of ROLES) {
        const grant = this.#grants[role];
        if (
          grant.loaned !== 0 ||
          grant.allocated !== (role === "metadata" ? NATIVE_ROOT_CREDIT_BYTES : 0)
        )
          throw new TypeError("Native root credit still has live allocations");
      }
      this.metadata.free(NATIVE_ROOT_CREDIT_BYTES);
      for (const role of ROLES) this.#grants[role].close();
      this.#closed = true;
    })();
    this.#closing = closing;
    void closing.catch(() => {
      if (this.#closing === closing) this.#closing = null;
    });
    return closing;
  }
}
