/** Native fencing protocol. A revoked opener can steal only an obsolete owner identity.
 * Recovery advances the durable generation rather than stealing an opener's coordination lock.
 * Lock loss notification is an optimization, never the authority for a generation transition.
 */
import { OwnerUnresponsiveError, OwnershipLostError } from "./contract.js";
import {
  DEFAULT_ACQUIRE_TIMEOUT_MS,
  requestUntil,
  withDeadline,
  type HeldLock,
  type LockManagerLike,
  type PublicationGate,
} from "./fence.js";
import {
  identityGateName,
  identityOwnerName,
  nextIdentity,
  requireIdentity,
  type OwnerIdentity,
} from "./owner-identity.js";

export const PUBLICATION_GATE_TIMEOUT_MS = 1_000;
export class NativeAuthority implements HeldLock {
  readonly #lost = new AbortController();
  #current: HeldLock;
  readonly #held = new Set<HeldLock>();
  #identity: OwnerIdentity;
  #chain: Promise<void> = Promise.resolve();
  constructor(
    readonly locks: LockManagerLike,
    readonly name: string,
    identity: OwnerIdentity,
    lock: HeldLock,
  ) {
    this.#identity = identity;
    this.#current = lock;
    this.#held.add(lock);
    this.#watch(lock);
  }
  get lost(): AbortSignal {
    return this.#lost.signal;
  }
  get identity(): OwnerIdentity {
    return this.#identity;
  }
  #watch(lock: HeldLock): void {
    const lost = () => {
      if (this.#current === lock) this.#lost.abort(new OwnershipLostError(this.#identity.epoch));
    };
    if (lock.lost.aborted) lost();
    else lock.lost.addEventListener("abort", lost, { once: true });
  }
  guard(): void {
    if (this.lost.aborted) throw new OwnershipLostError(this.#identity.epoch);
  }
  release(): void {
    this.#lost.abort(new OwnershipLostError(this.#identity.epoch));
    for (const lock of this.#held) lock.release();
    this.#held.clear();
  }
  /** One chain per authority, not per session. The caller reserves its own write order first. */
  publication(
    advance: (expected: OwnerIdentity, next: OwnerIdentity) => Promise<void>,
    verify: (expected: OwnerIdentity) => Promise<void>,
  ): PublicationGate {
    return (publish) => {
      const work = this.#chain.then(async () => {
        this.guard();
        let gate: HeldLock;
        try {
          gate = await requestUntil(
            this.locks,
            identityGateName(this.name, this.identity),
            Date.now() + PUBLICATION_GATE_TIMEOUT_MS,
            PUBLICATION_GATE_TIMEOUT_MS,
            false,
            this.lost,
          );
        } catch (error) {
          this.guard();
          if (!(error instanceof OwnerUnresponsiveError)) throw error;
          // The old gate may be frozen forever. Neither it nor its delayed loss is consulted.
          const expected = this.identity,
            next = nextIdentity(expected, false);
          gate = await requestUntil(
            this.locks,
            identityGateName(this.name, next),
            Date.now() + PUBLICATION_GATE_TIMEOUT_MS,
            PUBLICATION_GATE_TIMEOUT_MS,
            false,
            this.lost,
          );
          let replacement: HeldLock | undefined;
          try {
            replacement = await requestUntil(
              this.locks,
              identityOwnerName(this.name, next),
              Date.now() + PUBLICATION_GATE_TIMEOUT_MS,
              PUBLICATION_GATE_TIMEOUT_MS,
              false,
              this.lost,
            );
            this.guard();
            const obsolete = this.#current;
            // Install BEFORE the CAS await. An obsolete-key steal cannot abort this authority.
            this.#held.add(replacement);
            this.#current = replacement;
            this.#watch(replacement);
            this.guard();
            await advance(expected, next);
            this.guard();
            this.#identity = next;
            obsolete.release();
            this.#held.delete(obsolete);
          } catch (failure) {
            replacement?.release();
            gate.release();
            this.release();
            throw failure;
          }
        }
        try {
          this.guard();
          await verify(this.identity);
          await publish();
        } finally {
          gate.release();
        }
      });
      this.#chain = work.catch(() => undefined);
      return work;
    };
  }
}

/** Inspect and compare full identities before any native mutation. A frozen attempt never
 * refreshes its target using an old compatibility proof; its only steal names its original key.
 */
export async function acquireNative<T>(
  locks: LockManagerLike,
  name: string,
  inspect: () => Promise<OwnerIdentity>,
  fence: (expected: OwnerIdentity, next: OwnerIdentity, deadline: number) => Promise<T>,
  close: (opened: T) => void,
  options: { steal?: boolean; timeoutMs?: number },
): Promise<{ authority: NativeAuthority; opened: T }> {
  const timeout = options.timeoutMs ?? DEFAULT_ACQUIRE_TIMEOUT_MS,
    deadline = Date.now() + timeout;
  const bounded = async <V>(work: Promise<V>, late?: (value: V) => void): Promise<V> => {
    const result = await withDeadline(work, deadline - Date.now());
    if (result.kind === "failed") throw result.error;
    if (result.kind === "deadline") {
      if (late !== undefined) void work.then(late, () => undefined);
      throw new OwnerUnresponsiveError(timeout);
    }
    if (Date.now() > deadline) {
      late?.(result.fence);
      throw new OwnerUnresponsiveError(timeout);
    }
    return result.fence;
  };
  const expected = await bounded(inspect()),
    next = nextIdentity(expected, true);
  let gate: HeldLock | undefined,
    nextGate: HeldLock | undefined,
    target: HeldLock | undefined,
    owner: HeldLock | undefined;
  try {
    if (options.steal === true) {
      gate = await requestUntil(locks, identityGateName(name, expected), deadline, timeout);
      requireIdentity(await bounded(inspect()), expected);
    }
    // Future identities are private to this attempt, but never wait beyond its deadline.
    nextGate = await requestUntil(locks, identityGateName(name, next), deadline, timeout);
    target = await requestUntil(
      locks,
      identityOwnerName(name, expected),
      deadline,
      timeout,
      options.steal === true,
    );
    const opened = await bounded(fence(expected, next, deadline), close);
    try {
      owner = await requestUntil(locks, identityOwnerName(name, next), deadline, timeout);
    } catch (error) {
      close(opened);
      throw error;
    }
    // The native compare-and-swap and held next gate protect the newly persisted identity.
    const authority = new NativeAuthority(locks, name, next, owner);
    owner = undefined;
    try {
      authority.guard();
    } catch (error) {
      close(opened);
      authority.release();
      throw error;
    }
    return { authority, opened };
  } finally {
    target?.release();
    owner?.release();
    gate?.release();
    nextGate?.release();
  }
}
