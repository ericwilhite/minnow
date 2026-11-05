/**
 * When retired extents may be reused: root generations, snapshot pins and deferred reclaim
 * (format/manifest.ts "Reclaim" and "Snapshots"). Memory only; pins never survive the process.
 *
 * - **Generations.** The root durable when the space opened is generation 0. Each successful
 *   publish makes the next generation current (`advance`).
 * - **Pins.** `pin()` holds the current generation's root for one query, read transaction or
 *   maintenance job. Pins are cheap and synchronous. `release` is idempotent.
 * - **Two-generation reclaim (lead ruling R8).** A publish that makes generation g current names
 *   the extents generation g−1 reached and g does not. They become reusable only when both hold:
 *   1. **the next publish (g+1) is durable.** Until then the other root slot still holds g−1, the
 *      root a reopen falls back to if g's slot is lost (a lying flush, or a torn write of g+1 that
 *      destroys nothing else). So the current root and the one before it are always intact;
 *   2. **no pin below g remains**: every pinned generation below g still reaches them.
 *   Releases happen in generation order.
 * - **At open**, the extents only the previous root (the other valid slot) reaches are retired at
 *   generation 0 (`retireAtOpen`): they wait for the first publish, like any retirement.
 *
 * The ledger does not know what an item is; the extent space queues extent ranges and frees them
 * when `onReusable` hands them back.
 */
export interface RootPin {
  /** The root generation this pin holds (memory only, starts at 0 on each open). */
  readonly generation: number;
  /** Diagnostic label (backpressure reports the oldest pin by it). */
  readonly label: string;
  readonly released: boolean;
  /** Idempotent. */
  release(): void;
}

export class ReclaimLedger<T> {
  readonly #onReusable: (items: readonly T[]) => void;
  readonly #now: () => number;
  /** Live pins per generation, each with the time it was taken. */
  readonly #pins = new Map<number, Map<RootPin, number>>();
  /** Retired items not yet reusable, by the generation that retired them (ascending). */
  readonly #waiting: Array<{ readonly generation: number; readonly items: readonly T[] }> = [];
  #generation = 0;

  constructor(onReusable: (items: readonly T[]) => void, now: () => number = Date.now) {
    this.#onReusable = onReusable;
    this.#now = now;
  }

  /** The current (newest durable) generation. */
  get generation(): number {
    return this.#generation;
  }

  get pinCount(): number {
    let count = 0;
    for (const pins of this.#pins.values()) count += pins.size;
    return count;
  }

  /** Retired items not yet reusable (waiting for the next publish or for pins). */
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

  /** The longest-held live pin, or null. */
  get oldestPin(): { readonly label: string; readonly ageMs: number } | null {
    let oldest: { label: string; since: number } | null = null;
    for (const pins of this.#pins.values())
      for (const [pin, since] of pins)
        if (oldest === null || since < oldest.since) oldest = { label: pin.label, since };
    return oldest === null ? null : { label: oldest.label, ageMs: this.#now() - oldest.since };
  }

  pin(label = "pin"): RootPin {
    const generation = this.#generation;
    let pins = this.#pins.get(generation);
    if (pins === undefined) {
      pins = new Map();
      this.#pins.set(generation, pins);
    }
    const held = pins;
    let released = false;
    const pin: RootPin = {
      generation,
      label,
      get released() {
        return released;
      },
      release: () => {
        if (released) return;
        released = true;
        held.delete(pin);
        if (held.size > 0) return;
        this.#pins.delete(generation);
        this.#drain();
      },
    };
    held.set(pin, this.#now());
    return pin;
  }

  /** At open, before any publish: `items` only the previous root reaches wait for the first publish. */
  retireAtOpen(items: readonly T[]): void {
    if (this.#generation !== 0) throw new TypeError("retireAtOpen after a publish");
    if (items.length > 0) this.#waiting.push({ generation: 0, items });
  }

  /**
   * A publish is durable: the next generation is current, and `retired` (reached by the previous
   * generation, not by the new one) waits for the publish after this one and for every older pin.
   * Returns the new generation.
   */
  advance(retired: readonly T[]): number {
    this.#generation += 1;
    if (retired.length > 0) this.#waiting.push({ generation: this.#generation, items: retired });
    this.#drain();
    return this.#generation;
  }

  #drain(): void {
    const oldest = this.oldestPinned ?? Infinity;
    let reusable = 0;
    for (const entry of this.#waiting) {
      // Retired by g: needs generation ≥ g + 1 durable and no pin below g.
      if (this.#generation < entry.generation + 1 || oldest < entry.generation) break;
      reusable += 1;
    }
    if (reusable === 0) return;
    const items = this.#waiting.splice(0, reusable).flatMap((entry) => entry.items);
    this.#onReusable(items);
  }
}
