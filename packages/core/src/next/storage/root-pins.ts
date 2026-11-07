/** Snapshot-owned descriptors, in monotonic generation order. No per-generation Maps or
 * capacity growth. The descriptor and label are admitted before allocation and kept charged
 * through release, including a release after the space closes.
 */
import { ReadDeniedError, type ByteBudget, type CacheMemory } from "./extents.js";
import type { RootPin } from "./reclaim.js";
export const PIN_HEADER_BYTES = 320;
interface Credit {
  free(bytes: number): void;
}
class Pin implements RootPin {
  previous: Pin | null = null;
  next: Pin | null = null;
  #owner: RootPins | null;
  constructor(
    owner: RootPins,
    readonly generation: number,
    readonly label: string,
    readonly since: number,
    readonly bytes: number,
    private credit: Credit | null,
  ) {
    this.#owner = owner;
  }
  get released(): boolean {
    return this.#owner === null;
  }
  release(): void {
    const owner = this.#owner;
    if (owner === null) return;
    this.#owner = null;
    owner.remove(this);
    const credit = this.credit;
    this.credit = null;
    credit?.free(this.bytes);
    owner.changed();
  }
}
export class RootPins {
  #head: Pin | null = null;
  #tail: Pin | null = null;
  #count = 0;
  #generation = 0;
  #closed = false;
  #cacheBytes = 0;
  constructor(
    readonly memory: CacheMemory,
    readonly changed: () => void,
    readonly now: () => number = Date.now,
  ) {}
  get generation(): number {
    return this.#generation;
  }
  get pinCount(): number {
    return this.#count;
  }
  get oldestPinned(): number | null {
    return this.#head?.generation ?? null;
  }
  get oldestPin(): { label: string; ageMs: number } | null {
    const head = this.#head;
    return head === null ? null : { label: head.label, ageMs: this.now() - head.since };
  }
  /** Space-owned fallback only; caller-budget pins remain charged to that snapshot owner. */
  get bytes(): number {
    return this.#cacheBytes;
  }
  advance(): number {
    return ++this.#generation;
  }
  pin(label = "pin", budget?: ByteBudget): RootPin {
    if (this.#closed) throw new TypeError("Root pins closed");
    const bytes = PIN_HEADER_BYTES + 2 * label.length;
    let credit: Credit;
    if (budget === undefined) {
      if (!this.memory.charge(bytes)) throw new ReadDeniedError(bytes);
      this.#cacheBytes += bytes;
      credit = {
        free: (count) => {
          this.#cacheBytes -= count;
          this.memory.release(count);
          if (this.#closed && this.#cacheBytes === 0) this.memory.unregister?.();
        },
      };
    } else {
      if (!budget.allocate(bytes)) throw new ReadDeniedError(bytes);
      credit = budget;
    }
    const pin = new Pin(this, this.#generation, label, this.now(), bytes, credit);
    pin.previous = this.#tail;
    if (this.#tail === null) this.#head = pin;
    else this.#tail.next = pin;
    this.#tail = pin;
    this.#count++;
    return pin;
  }
  remove(pin: Pin): void {
    if (pin.previous === null) this.#head = pin.next;
    else pin.previous.next = pin.next;
    if (pin.next === null) this.#tail = pin.previous;
    else pin.next.previous = pin.previous;
    pin.previous = null;
    pin.next = null;
    this.#count--;
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#cacheBytes === 0) this.memory.unregister?.();
  }
}
