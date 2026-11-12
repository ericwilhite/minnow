/** Result-grant allocations whose tight buffers can change cleanup owner without another charge. */
import type { Grant } from "../memory/governor.js";
import type { Transfer } from "./pipeline.js";
const owners = new WeakMap<Transfer, ResultBuffers>();
export type ResultAllocator = (bytes: number) => Promise<ResultBuffers>;
export class ResultBuffers {
  readonly #buffers = new Set<ArrayBuffer>();
  readonly #transfer: Transfer;
  #unallocated: number;
  #owned: number;
  #closed = false;
  constructor(
    readonly budget: Grant,
    bytes: number,
    readonly onClose?: () => void,
  ) {
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("Invalid result allocation");
    if (!budget.allocate(bytes))
      throw new Error("Result-owned buffers exceed their complete admission");
    this.#unallocated = bytes;
    this.#owned = bytes;
    this.#transfer = {
      detachableBudget: budget,
      detachableBuffers: this.#buffers,
      release: () => this.close(),
    };
    owners.set(this.#transfer, this);
  }
  get transfer(): Transfer {
    return this.#transfer;
  }
  /** Charge precedes all backing allocations; every array is dedicated and exact-sized. */
  bytes(bytes: number): Uint8Array<ArrayBuffer> {
    if (this.#closed || !Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.#unallocated)
      throw new Error("Result-owned allocation bound changed");
    const buffer = new ArrayBuffer(bytes);
    this.#buffers.add(buffer);
    this.#unallocated -= bytes;
    return new Uint8Array(buffer);
  }
  take(budget: Grant, buffers: ReadonlySet<ArrayBuffer>): number | null {
    if (this.#closed || budget !== this.budget) return null;
    // Validate the entire transfer before publishing any ownership change.
    if ([...buffers].some((buffer) => !this.#buffers.has(buffer))) return null;
    let bytes = 0;
    for (const buffer of buffers) {
      bytes += buffer.byteLength;
      this.#buffers.delete(buffer);
    }
    this.#owned -= bytes;
    return bytes;
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    try {
      this.budget.free(this.#owned);
    } finally {
      this.#owned = 0;
      this.#buffers.clear();
      this.onClose?.();
    }
  }
}
/** A foreign or unbranded transfer cannot bypass charging through metadata claims. */
export function takeResultBuffers(
  transfer: Transfer | null,
  budget: Grant,
  buffers: ReadonlySet<ArrayBuffer>,
): number | null {
  return transfer === null ? null : (owners.get(transfer)?.take(budget, buffers) ?? null);
}
