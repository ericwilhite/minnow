/**
 * Admission for the space's retained metadata. Capacity never shrinks before close:
 * deleting a Map entry or splicing an array need not return its backing allocation.
 * Per-slot estimates include references, objects and container overhead. Two complete
 * capacities cover old/new backing stores overlapping during growth, before mutation.
 */
import {
  ReadDeniedError,
  type ByteBudget,
  type CacheMemory,
  type OpenSpaceOptions,
} from "./extents.js";

export const RETAINED_BASE_BYTES = 1024;
export const FREE_RUN_SLOT_BYTES = 32;
export const RETIRED_SLOT_BYTES = 256;

export class RetainedMemory {
  readonly evictableBytes = 0;
  #budget: ByteBudget | null;
  #memory: CacheMemory | null = null;
  #bytes = RETAINED_BASE_BYTES;
  #closed = false;

  static create(budget: ByteBudget): RetainedMemory {
    if (!budget.allocate(RETAINED_BASE_BYTES)) throw new ReadDeniedError(RETAINED_BASE_BYTES);
    return new RetainedMemory(budget);
  }

  private constructor(budget: ByteBudget) {
    this.#budget = budget;
  }
  evict(): number {
    return 0;
  }

  /** Bytes currently reserved for the space's retained metadata (exact; for accounting checks). */
  get bytes(): number {
    return this.#bytes;
  }

  reserve(bytes: number): void {
    if (this.#closed) throw new TypeError("Retained space memory is closed");
    const accepted =
      this.#budget !== null ? this.#budget.allocate(bytes) : this.#memory?.charge(bytes);
    if (accepted !== true) throw new ReadDeniedError(bytes);
    this.#bytes += bytes;
  }

  transfer(register: OpenSpaceOptions["registerCache"]): void {
    const memory = register(this);
    if (!memory.charge(this.#bytes)) {
      memory.unregister?.();
      throw new ReadDeniedError(this.#bytes);
    }
    this.#budget?.free(this.#bytes);
    this.#budget = null;
    this.#memory = memory;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#budget?.free(this.#bytes);
    this.#memory?.release(this.#bytes);
    this.#memory?.unregister?.();
    this.#bytes = 0;
  }
}

/** Reserve before creating an entry, including a Map rehash or array growth overlap. */
export class RetainedCapacity {
  #capacity = 0;
  constructor(
    readonly memory: RetainedMemory,
    readonly slotBytes: number,
  ) {}
  ensure(count: number): void {
    if (count <= this.#capacity) return;
    let next = Math.max(32, this.#capacity);
    while (next < count) next *= 2;
    this.memory.reserve(2 * (next - this.#capacity) * this.slotBytes);
    this.#capacity = next;
  }
}
