/** Native fixed credit and independent progress lanes reused across aggregate groups. */
import type { Grant, Loan } from "../../memory/governor.js";
import { ReservedGrant } from "../admission.js";

function present<T>(value: T | undefined): T {
  if (value === undefined) throw new TypeError("Missing aggregate grant share");
  return value;
}

/** Protect every sort's normal minimum and give it an independent pre-admitted loan owner.
 * The recipe owns the fixed grant; a sort's close cannot surrender the next group's minimum.
 */
export class SortGrants {
  holdFrees = false;
  readonly #shares = new Map<
    Grant,
    { grant: ReservedGrant; extra: Grant; deferred: number; loans: Set<Loan>; capacity: number }
  >();
  readonly #fixed: Grant;
  constructor(
    readonly grant: Grant,
    readonly loans: readonly Grant[],
  ) {
    this.#fixed = new Proxy(grant, {
      get(target, property) {
        if (property === "release") return () => undefined;
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function"
          ? (value as (...args: unknown[]) => unknown).bind(target)
          : value;
      },
    });
    grant.onReclaimRequest(() => {
      // Unused fixed credit is still needed by later groups. Only growth may be returned.
      for (const share of this.#shares.values()) this.#releaseExtra(share.extra);
    });
  }
  #releaseExtra(extra: Grant): void {
    const free = extra.held - extra.allocated - extra.loaned;
    if (free > 0) extra.release(free);
  }
  fork(minimum: number, slot: number): Grant {
    const extra = present(this.loans[slot]);
    const reserved = new ReservedGrant(this.#fixed, extra, minimum);
    const share = {
      grant: reserved,
      extra,
      deferred: 0,
      loans: new Set<Loan>(),
      capacity: minimum + extra.held,
    };
    const proxy = new Proxy(reserved, {
      get: (target, property) => {
        if (property === "free")
          return (bytes: number) => {
            if (this.holdFrees) share.deferred += bytes;
            else target.free(bytes);
          };
        if (property === "close") return () => undefined;
        if (property === "release") return () => this.#releaseExtra(extra);
        if (property === "borrow")
          return async (bytes: number, signal?: AbortSignal): Promise<Loan> => {
            const loan = await target.borrow(bytes, signal);
            share.loans.add(loan);
            return {
              bytes: loan.bytes,
              repay: () => {
                // A spill service can still borrow buffers after synchronous abort cleanup.
                if (this.holdFrees) return;
                loan.repay();
                share.loans.delete(loan);
              },
            };
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function"
          ? (value as (...args: unknown[]) => unknown).bind(target)
          : value;
      },
    });
    this.#shares.set(proxy, share);
    return proxy;
  }
  /** Initial fixed/private credit only; later optional headroom never enlarges this bound. */
  stepCapacity(grant: Grant): number {
    return present(this.#shares.get(grant)).capacity;
  }
  reset(grant: Grant): void {
    if (this.holdFrees) return;
    const share = present(this.#shares.get(grant));
    if (share.grant.allocated !== 0)
      throw new RangeError("Ordered-set sort retained buffers after drain");
    for (const loan of share.loans) loan.repay();
    share.loans.clear();
    this.#releaseExtra(share.extra);
  }
  close(): void {
    const errors: unknown[] = [];
    for (const share of this.#shares.values()) {
      try {
        if (share.deferred > 0) share.grant.free(share.deferred);
        for (const loan of share.loans) loan.repay();
      } catch (error) {
        errors.push(error);
      } finally {
        share.grant.close();
      }
    }
    this.#shares.clear();
    if (errors.length) throw new AggregateError(errors, "Ordered-set grant cleanup failed");
  }
}
