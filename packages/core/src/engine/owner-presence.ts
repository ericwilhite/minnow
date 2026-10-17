import type { BlockStore } from "../storage/types.js";
import { lockManager } from "./write-coordinator.js";

/**
 * Whether the connection behind an index build is still there.
 *
 * A build session -- the postings a `CREATE INDEX`, a fold, or a rebuild stages, and a UNIQUE
 * index's staged key set -- names the engine that began it, and its lease runs for an hour so a
 * large build never stops to renew. A tab that dies mid-build leaves that session behind, and
 * every other connection's build of the index would wait out the hour.
 *
 * So every engine holds the Web Lock `minnowdb-owner:<ownerId>` from construction until it closes.
 * The browser releases a lock when the context holding it goes away -- a tab closed, crashed, or
 * discarded, a worker terminated -- and in no other way. A session whose owner holds no such lock
 * belongs to a connection that can never touch it again: another connection ends it and builds
 * at once. A live owner always holds its lock, so its build is never taken from it.
 *
 * Ending a session wrongly would still be safe: the store refuses every later step of the build
 * that lost it, and the catalog names one build at a time. Liveness decides only whether waiting
 * can help, never what is correct -- which is why "unknown" (a store shared across contexts with
 * no Web Locks to ask) counts as alive, and the lease is the fallback.
 */

const OWNER_LOCK_PREFIX = "minnowdb-owner:";
const CREATION_LOCK_PREFIX = "minnowdb-index-create:";

/** Engines open in this context, by owner id. Several only when a custom `createId` repeats. */
const presentOwners = new Map<string, Set<OwnerPresence>>();

/** `CREATE INDEX` runs in this context that no Web Lock covers, by store and storage column. */
const creations = new Map<string, Promise<void>>();

function ownerLockName(ownerId: string): string {
  return `${OWNER_LOCK_PREFIX}${ownerId}`;
}

/** One engine's claim to be alive, held from its construction until it closes. */
export class OwnerPresence {
  readonly ownerId: string;
  /** Settles once the claim is visible to every context: build sessions begin only after it. */
  readonly ready: Promise<void>;
  /** Settles when the claim is let go: every lock this engine holds for liveness goes with it. */
  readonly gone: Promise<void>;
  #letGo: () => void = () => undefined;
  #markGone: () => void = () => undefined;
  #released = false;
  #lockHeld = false;

  constructor(ownerId: string) {
    this.ownerId = ownerId;
    this.gone = new Promise<void>((resolve) => {
      this.#markGone = resolve;
    });
    let present = presentOwners.get(ownerId);
    if (present === undefined) {
      present = new Set();
      presentOwners.set(ownerId, present);
    }
    present.add(this);
    const locks = lockManager();
    if (locks === undefined) {
      this.ready = Promise.resolve();
      return;
    }
    const held = new Promise<void>((resolve) => {
      this.#letGo = resolve;
    });
    this.ready = new Promise<void>((resolve) => {
      // `ifAvailable`: an owner id another context already holds (a custom `createId` that
      // repeats across tabs) is shown alive by that holder, and must not leave this engine
      // waiting for it.
      locks
        .request(ownerLockName(ownerId), { ifAvailable: true }, async (lock) => {
          this.#lockHeld = lock !== null;
          resolve();
          if (lock !== null) await held;
        })
        .then(
          () => undefined,
          () => resolve(),
        );
    });
  }

  /**
   * Whether no other engine anywhere can hold this owner id, so a build session under it that
   * this engine is not running was lost by this engine. Read after `ready`.
   */
  get sole(): boolean {
    return (
      !this.#released &&
      presentOwners.get(this.ownerId)?.size === 1 &&
      (this.#lockHeld || lockManager() === undefined)
    );
  }

  /** Lets go of the claim. Safe to call twice. */
  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.#letGo();
    this.#markGone();
    const present = presentOwners.get(this.ownerId);
    present?.delete(this);
    if (present?.size === 0) presentOwners.delete(this.ownerId);
  }
}

/**
 * Whether the engine that owns a build session is provably gone. True when it is not open in
 * this context and either no other context can reach the store (it has no cross-context
 * identity) or no context holds its owner lock. Without Web Locks a store shared across contexts
 * gives no proof, and the answer is false: the session lasts until its lease runs out.
 */
export async function ownerDeparted(store: BlockStore, ownerId: string): Promise<boolean> {
  if (presentOwners.has(ownerId)) return false;
  if (store.liveQueryChannelName === undefined) return true;
  const locks = lockManager();
  if (locks === undefined) return false;
  const name = ownerLockName(ownerId);
  try {
    const { held = [], pending = [] } = await locks.query();
    return !held.some((lock) => lock.name === name) && !pending.some((lock) => lock.name === name);
  } catch {
    return false;
  }
}

/**
 * Runs `step` as the one `CREATE INDEX` of the index stored under `columnId`, after any other
 * in any context has finished: the creator holds this for its whole statement, and a
 * `CREATE INDEX IF NOT EXISTS` that finds the index waits on it rather than reporting an index
 * whose build may still fail. A creator that dies lets go with its context.
 *
 * `exclusive` is true when that wait covers every context that can reach the store -- Web Locks
 * span them, or the store has no cross-context identity -- so no creator can still be running.
 * The lock is held until `step` settles or `presence` goes, as a context's locks go when it
 * dies. `signal` cancels the wait (a closing engine), never a step already running.
 */
export async function asIndexCreator<T>(
  store: BlockStore,
  columnId: string,
  presence: OwnerPresence,
  signal: AbortSignal,
  step: (exclusive: boolean) => Promise<T>,
): Promise<T> {
  signal.throwIfAborted();
  // The step's promise travels out wrapped: returned bare, the lock would be held until it
  // settled whatever happened to the owner.
  const hold = (exclusive: boolean): { result: Promise<T>; held: Promise<void> } => {
    const result = step(exclusive);
    const settled = result.then(
      () => undefined,
      () => undefined,
    );
    return { result, held: Promise.race([settled, presence.gone]) };
  };
  const identity = store.liveQueryChannelName;
  const locks = identity === undefined ? undefined : lockManager();
  if (locks !== undefined) {
    const { result } = await locks.request(
      `${CREATION_LOCK_PREFIX}${identity ?? ""}:${columnId}`,
      { signal },
      async () => {
        const running = hold(true);
        await running.held;
        return { result: running.result };
      },
    );
    return result;
  }
  const key = `${identity ?? ""}\u0000${columnId}`;
  const previous = creations.get(key) ?? Promise.resolve();
  let finished = (): void => undefined;
  const own = new Promise<void>((resolve) => {
    finished = resolve;
  });
  const tail = previous.then(() => own);
  creations.set(key, tail);
  const release = (): void => {
    finished();
    if (creations.get(key) === tail) creations.delete(key);
  };
  try {
    await new Promise<void>((resolve, reject) => {
      const abort = (): void => {
        reject(signal.reason instanceof Error ? signal.reason : new Error(String(signal.reason)));
      };
      signal.addEventListener("abort", abort, { once: true });
      void previous.then(() => {
        signal.removeEventListener("abort", abort);
        resolve();
      });
    });
  } catch (error) {
    release();
    throw error;
  }
  const running = hold(identity === undefined);
  void running.held.then(release);
  return running.result;
}

/**
 * Test seam: makes every engine in this context with this owner id look gone to other
 * connections, as if its tab had closed mid-build, while the engine object stays in memory.
 */
export function departOwnerForTests(ownerId: string): void {
  for (const presence of [...(presentOwners.get(ownerId) ?? [])]) presence.release();
}
