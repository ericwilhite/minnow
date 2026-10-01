import type { BlockStore } from "../storage/types.js";
import { admitWriter, type WriterAdmission, type WriterKind } from "./write-coordinator.js";
import { WriteAdmissionStalledError } from "./errors.js";
import { withLinkedSignal } from "./cancellation.js";
const CLOSE_MAINTENANCE_GRACE_MS = 2_000;
interface WriterAdmissionOptions {
  readonly store: BlockStore;
  readonly shutdown: AbortSignal;
  readonly closedError: () => Error;
  readonly report: (error: unknown, context: string) => void;
}
/** Per-connection ownership of writer waits, publication lending and maintenance shutdown.
 * The shared FIFO/Web Lock mechanism remains in write-coordinator. */
export class WriterAdmissions {
  readonly #writers = new Set<Promise<unknown>>();
  readonly #crossContextWaits = new AbortController();
  readonly #maintenanceQueue = new AbortController();
  readonly #pendingCompactionPublications = new Set<() => Promise<void>>();
  #pendingPublicationSignal: { promise: Promise<void>; resolve: () => void } | undefined;
  #grace: ReturnType<typeof setTimeout> | undefined;
  readonly #closedError: WriterAdmissionOptions["closedError"];
  readonly #shutdown: WriterAdmissionOptions["shutdown"];
  readonly #store: WriterAdmissionOptions["store"];
  readonly #report: WriterAdmissionOptions["report"];
  constructor(options: WriterAdmissionOptions) {
    this.#closedError = options.closedError;
    this.#shutdown = options.shutdown;
    this.#store = options.store;
    this.#report = options.report;
  }
  get maintenanceSignal(): AbortSignal {
    return this.#maintenanceQueue.signal;
  }
  cancelCrossContextWaits(reason: Error): void {
    this.#crossContextWaits.abort(reason);
  }
  beginDrain(): void {
    this.#grace = setTimeout(
      () => this.#maintenanceQueue.abort(this.#closedError()),
      CLOSE_MAINTENANCE_GRACE_MS,
    );
    (this.#grace as { unref?: () => void }).unref?.();
  }
  async drain(): Promise<void> {
    await Promise.allSettled([...this.#writers]);
  }
  finishDrain(): void {
    clearTimeout(this.#grace);
    this.#grace = undefined;
    this.#maintenanceQueue.abort(this.#closedError());
  }

  /**
   * Takes this database's turn as the one logical writer, then runs the callback with the
   * admission that proves it. Every path that publishes goes through here exactly once; paths
   * that already hold a turn receive its admission and must not take another (see
   * `WriterAdmission`). Waiting is cancelled by the scope's signal, which closing aborts, and a
   * holder that stops advancing is reported after `WRITE_ADMISSION_STALL_REPORT_MS` without
   * being bypassed.
   */
  admit<T>(
    kind: WriterKind,
    run: (admission: WriterAdmission) => Promise<T>,
    signal?: AbortSignal,
    cancelWait?: AbortSignal,
  ): Promise<T> {
    const maintenance = signal === this.#maintenanceQueue.signal;
    if (this.#shutdown.aborted && !maintenance) return Promise.reject(this.#closedError());
    const admit = (waitSignal: AbortSignal): Promise<T> =>
      admitWriter(
        this.#store,
        {
          kind,
          signal: waitSignal,
          ...(maintenance ? {} : { crossContextSignal: this.#crossContextWaits.signal }),
          onStalled: (stall) => {
            this.#report(
              new WriteAdmissionStalledError(stall.waitedMs, stall.holder, stall.holderKind),
              "write admission",
            );
          },
        },
        run,
      );
    const task =
      cancelWait !== undefined
        ? withLinkedSignal(cancelWait, admit, signal ?? this.#shutdown)
        : signal === undefined
          ? admit(this.#shutdown)
          : maintenance
            ? admit(signal)
            : withLinkedSignal(signal, admit, this.#shutdown);
    this.#writers.add(task);
    const forget = (): boolean => this.#writers.delete(task);
    void task.then(forget, forget);
    return task;
  }

  /**
   * Runs one publication attempt for a compaction as a writer of this database. The attempt
   * takes a turn of its own through admission, and any writer that gets its turn first runs
   * the attempt inside that turn, before its own statement, whichever comes first. Writers
   * that arrive while it runs wait for it, so only an uncoordinated writer can commit between
   * its rebase and its commit, and the attempt retries against those without yielding.
   *
   * Lending the turn is what keeps a fold moving under a caller that awaits one statement
   * after another: on an in-memory store such a caller never reaches the macrotask queue, and
   * a fold parked on a yield of its own sat with its lease expiring while every write scanned
   * one more level-zero segment.
   */
  publish<T>(attempt: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const unusedWait = new AbortController();
      // The claim stays pending until the attempt has finished, not until it has started: a
      // writer that arrives while it is in flight waits for it.
      let settled: Promise<void> | undefined;
      const claim = (): Promise<void> => {
        settled ??= (async () => {
          // This attempt owns a writer turn now, either its own or the caller's loan.
          // Cancel only its redundant wait; already-admitted work keeps its turn.
          unusedWait.abort(new Error("Compaction admitted"));
          try {
            resolve(await attempt());
          } catch (error) {
            reject(error instanceof Error ? error : new Error(String(error)));
          } finally {
            this.#pendingCompactionPublications.delete(claim);
          }
        })();
        return settled;
      };
      this.#pendingCompactionPublications.add(claim);
      const registered = this.#pendingPublicationSignal;
      this.#pendingPublicationSignal = undefined;
      registered?.resolve();
      void this.admit(
        "maintenance",
        () => claim(),
        this.maintenanceSignal,
        unusedWait.signal,
      ).catch((error: unknown) => {
        // Closing refuses the turn. A claim nobody lent a turn to never publishes; one a writer
        // already started runs to its known outcome inside that writer's turn.
        if (settled !== undefined) {
          if (error !== unusedWait.signal.reason) this.#report(error, "compaction admission");
          return;
        }
        this.#pendingCompactionPublications.delete(claim);
        reject(error instanceof Error ? error : new Error(String(error)));
      });
    });
  }

  /**
   * Runs, or waits for, one pending fold publication inside the caller's own turn; a
   * claim never throws to its runner. A writer calls this once admitted and before its
   * snapshot, so nothing of its own can conflict with the neutral manifest the fold publishes.
   */
  assist(): Promise<void> {
    const [claim] = this.#pendingCompactionPublications;
    return claim?.() ?? Promise.resolve();
  }

  /** Settles when the next fold publication registers for a turn. */
  #nextPendingPublication(): Promise<void> {
    if (this.#pendingPublicationSignal === undefined) {
      let resolve = (): void => undefined;
      const promise = new Promise<void>((settle) => {
        resolve = settle;
      });
      this.#pendingPublicationSignal = { promise, resolve };
    }
    return this.#pendingPublicationSignal.promise;
  }

  /**
   * Waits for a compaction step that may need this writer's turn to publish, lending the turn
   * to every publication that registers meanwhile. Waiting for the step outright would
   * deadlock: the step waits for a turn, and the turn is held by the waiter.
   */
  async lend(step: Promise<unknown>): Promise<void> {
    const state = { done: false };
    const settled = step.then(
      () => {
        state.done = true;
      },
      () => {
        state.done = true;
      },
    );
    // Read through a function: the flag flips inside the settlement callbacks, which narrowing
    // across the awaits below would otherwise read as never changing.
    const pending = (): boolean => !state.done;
    while (pending()) {
      // Arm the wake-up before draining: a publication that registers after the drain and
      // before the wait would otherwise be missed, and the step could never settle, since its
      // turn is the one this writer holds.
      const registered = this.#nextPendingPublication();
      await this.assist();
      if (!pending()) break;
      if (this.#pendingCompactionPublications.size > 0) continue;
      await Promise.race([settled, registered]);
    }
  }
}
