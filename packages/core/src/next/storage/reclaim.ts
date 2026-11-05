/**
 * When retired extents may be reused: root generations, snapshot pins and deferred reclaim
 * (format/manifest.ts "Reclaim" and "Snapshots"). Memory only; pins never survive the process.
 *
 * - **Generations.** The root durable when the space opened is generation 0. Each successful
 *   publish makes the next generation current (`advance`).
 * - **Pins.** `pin()` holds the current generation's root for one query, read transaction or
 *   maintenance job. Pins are cheap and synchronous. `release` is idempotent.
 * - **Retirement.** A publish that makes generation g current names the extents generation g−1
 *   reached and g does not. They are still reachable from every pinned generation below g, so they
 *   become reusable only when no pin below g remains: at once if there is none, else when the last
 *   such pin is released. Releases happen in generation order.
 *
 * The ledger does not know what an item is; the extent space queues extent ranges and frees them
 * when `onReusable` hands them back.
 */
export interface RootPin {
  /** The root generation this pin holds (memory only, starts at 0 on each open). */
  readonly generation: number;
  readonly released: boolean;
  /** Idempotent. */
  release(): void;
}

export class ReclaimLedger<T> {
  readonly #onReusable: (items: readonly T[]) => void;
  /** Live pin count per generation. */
  readonly #pins = new Map<number, number>();
  /** Retired items waiting for pins, by the generation that retired them (ascending). */
  readonly #waiting: Array<{ readonly generation: number; readonly items: readonly T[] }> = [];
  #generation = 0;

  constructor(onReusable: (items: readonly T[]) => void) {
    this.#onReusable = onReusable;
  }

  /** The current (newest durable) generation. */
  get generation(): number {
    return this.#generation;
  }

  get pinCount(): number {
    let count = 0;
    for (const pins of this.#pins.values()) count += pins;
    return count;
  }

  /** Retired items still held by pins. */
  get waiting(): number {
    let count = 0;
    for (const entry of this.#waiting) count += entry.items.length;
    return count;
  }

  /** The oldest pinned generation, or null. */
  get oldestPinned(): number | null {
    let oldest: number | null = null;
    for (const generation of this.#pins.keys())
      if (oldest === null || generation < oldest) oldest = generation;
    return oldest;
  }

  pin(): RootPin {
    const generation = this.#generation;
    this.#pins.set(generation, (this.#pins.get(generation) ?? 0) + 1);
    let released = false;
    return {
      generation,
      get released() {
        return released;
      },
      release: () => {
        if (released) return;
        released = true;
        const left = (this.#pins.get(generation) ?? 1) - 1;
        if (left > 0) this.#pins.set(generation, left);
        else {
          this.#pins.delete(generation);
          this.#drain();
        }
      },
    };
  }

  /**
   * A publish is durable: the next generation is current, and `retired` (reached by the previous
   * generation, not by the new one) waits for every older pin. Returns the new generation.
   */
  advance(retired: readonly T[]): number {
    this.#generation += 1;
    if (retired.length > 0) this.#waiting.push({ generation: this.#generation, items: retired });
    this.#drain();
    return this.#generation;
  }

  #drain(): void {
    const oldest = this.oldestPinned;
    let reusable = 0;
    while (reusable < this.#waiting.length) {
      const entry = this.#waiting[reusable] as { readonly generation: number };
      if (oldest !== null && oldest < entry.generation) break;
      reusable += 1;
    }
    if (reusable === 0) return;
    const items = this.#waiting.splice(0, reusable).flatMap((entry) => entry.items);
    this.#onReusable(items);
  }
}
