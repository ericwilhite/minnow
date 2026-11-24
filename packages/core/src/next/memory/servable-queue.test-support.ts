/**
 * Shared by the `servableNow` queue suites (servable-queue.test.ts, its generated twin): an owner
 * lending its holdings to a statement, with its own work queued as holder growth.
 */
import type { Governor, Grant, HolderChain } from "./governor.js";

export const take = (governor: Governor, label: string, bytes: number, spillable = false) =>
  governor.admit({ label, minimumBytes: bytes, spillable });

/** A refusal as a test reads it: its SQLSTATE when it has one, else its message. */
export const failure = (error: unknown): string =>
  (error as { sqlState?: string }).sqlState ??
  (error instanceof Error ? error.message : String(error));

/** Resolves "pending" after the turns a drain and its settlements take. */
export const turn = () =>
  new Promise<"pending">((resolve) =>
    setImmediate(() => {
      resolve("pending");
    }),
  );

/**
 * An owner lending its holdings to a statement (a writer whose turn the statement holds), with its
 * own work queued as holder growth. A job, once served, runs and finishes at once: it closes its
 * growth and the owner's grants it names that are still open (the first job to finish returns a
 * shared one). What it keeps for the statement (`retained`) is every open grant of its, except what
 * served, unfinished work will return (that work is finite).
 */
export class Owner {
  readonly grants: Grant[] = [];
  readonly #running = new Set<readonly Grant[]>();
  readonly chain: HolderChain;
  constructor(readonly governor: Governor) {
    this.chain = governor.openHolderChain("writer", "writer");
  }
  async account(label: string, bytes: number): Promise<Grant> {
    const grant = await take(this.governor, label, bytes);
    this.grants.push(grant);
    return grant;
  }
  get lent() {
    return {
      grants: this.grants,
      retained: () => {
        let kept = 0;
        for (const grant of this.grants)
          if (![...this.#running].some((returns) => returns.includes(grant))) kept += grant.held;
        return kept;
      },
    };
  }
  job(
    label: string,
    bytes: number,
    options: {
      readonly progress?: boolean;
      readonly returns?: readonly Grant[];
      readonly signal?: AbortSignal;
      readonly onLeave?: () => void;
    } = {},
  ): Promise<string> {
    const returns = options.returns ?? [];
    return this.governor
      .admitGroup({
        label,
        grants: [{ label, minimumBytes: bytes, spillable: false }],
        holdsCredit: this.chain,
        ...(options.progress === true ? { includesProgress: true } : {}),
        returns: () => returns,
        ...(options.signal === undefined ? {} : { signal: options.signal }),
        onLeave: () => {
          // Served (not withdrawn): its work runs from now on.
          if (options.signal?.aborted !== true) this.#running.add(returns);
          options.onLeave?.();
        },
      })
      .then(
        (grants) => {
          for (const grant of grants) grant.close();
          for (const grant of returns) grant.close();
          this.#running.delete(returns);
          return "served";
        },
        (error: unknown) => {
          // Served but cancelled before it ran (the governor returned its growth): its work never
          // runs, so what it names stays held. The owner's holdings changed: wake the queue, as
          // an owner does when its work ends.
          if (this.#running.delete(returns))
            this.grants.find((grant) => this.governor.isOpen(grant))?.release(0);
          return error instanceof Error ? error.message : String(error);
        },
      );
  }
  close(): void {
    for (const grant of this.grants) grant.close();
    this.chain.close();
  }
}
