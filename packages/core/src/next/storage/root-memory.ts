/** Charged root copies. The prepaid bank covers five small frames (current, previous,
 * preparation, and two captured by the one serialized audit) plus owner/control headers.
 * Oversized roots need ordinary caller credit before allocating; no admission is awaited.
 * Native publication borrows the frame itself, never encoding/copying it a second time.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import { pauseIfDue, type TaskWork } from "../task.js";
import {
  ReadDeniedError,
  type ByteBudget,
  type CacheMemory,
  type OpenSpaceOptions,
} from "./extents.js";
import type { StoreSession } from "./contract.js";

export const ROOT_BANK_BYTES = 128 * 1024;
export const SMALL_ROOT_BYTES = 18 * 1024;
const FRAME_OVERHEAD = 512;
const ROOT_HEADER = 20;
interface PreparedRoot {
  readonly token: object;
  readonly frame: RootFrame;
  readonly slot: 0 | 1;
}
const prepared = new WeakMap<Uint8Array, PreparedRoot>();
/** Native adapters only accept a prepared frame made for this exact session. */
export function preparedRoot(session: StoreSession, bytes: Uint8Array): PreparedRoot | undefined {
  const value = prepared.get(bytes);
  return value?.token === (session.rootWriterToken ?? session) ? value : undefined;
}
export class RootMemory {
  readonly evictableBytes = 0;
  #small = 0;
  #extra = 0;
  #closed = false;
  private constructor(readonly memory: CacheMemory) {}
  static create(register: OpenSpaceOptions["registerCache"]): RootMemory {
    // Register before allocating the bank owner, as with the fixed cache slabs.
    const proxy = { evictableBytes: 0, evict: () => 0 },
      memory = register(proxy);
    if (!memory.charge(ROOT_BANK_BYTES)) {
      memory.unregister?.();
      throw new ReadDeniedError(ROOT_BANK_BYTES);
    }
    return new RootMemory(memory);
  }
  evict(): number {
    return 0;
  }
  get bytes(): number {
    return (this.#closed ? 0 : ROOT_BANK_BYTES) + this.#extra;
  }
  async copy(bytes: Uint8Array, budget: ByteBudget, work?: TaskWork): Promise<RootFrame> {
    if (this.#closed) throw new TypeError("Root memory closed");
    const charge = bytes.length + ROOT_HEADER + FRAME_OVERHEAD;
    const small =
      bytes.length <= SMALL_ROOT_BYTES && this.#small + charge <= ROOT_BANK_BYTES - 8192;
    if (small) this.#small += charge;
    else {
      if (!budget.allocate(charge)) throw new ReadDeniedError(charge);
      const moved =
        this.memory.takeFrom === undefined
          ? this.memory.charge(charge)
          : this.memory.takeFrom(budget.ordinarySource?.() ?? budget, charge);
      if (!moved) {
        budget.free(charge);
        throw new ReadDeniedError(charge);
      }
      if (this.memory.takeFrom === undefined) budget.free(charge);
      this.#extra += charge;
    }
    let frame: RootFrame | null = null;
    try {
      frame = new RootFrame(this, bytes.length, charge, small);
      for (let at = 0; at < bytes.length; at += 32 * 1024) {
        const pause = pauseIfDue(work);
        if (pause !== undefined) await pause;
        frame.bytes.set(bytes.subarray(at, at + 32 * 1024), at);
      }
      const pause = pauseIfDue(work);
      if (pause !== undefined) await pause;
      return frame;
    } catch (error) {
      if (frame === null) this.refund(charge, small);
      else frame.release();
      throw error;
    }
  }
  refund(bytes: number, small: boolean): void {
    if (small) this.#small -= bytes;
    else {
      this.#extra -= bytes;
      this.memory.release(bytes);
    }
    if (this.#closed && this.#small === 0 && this.#extra === 0) this.memory.unregister?.();
  }
  close(): void {
    if (this.#closed) return;
    // All frame owners (including captured audits) must release before closing the bank.
    if (this.#small !== 0 || this.#extra !== 0)
      throw new TypeError("Root frames still owned at close");
    this.#closed = true;
    this.memory.release(ROOT_BANK_BYTES);
    this.memory.unregister?.();
  }
}
export class RootFrame {
  #slot: Uint8Array;
  #bytes: Uint8Array;
  #refs = 1;
  constructor(
    readonly owner: RootMemory,
    length: number,
    readonly charge: number,
    readonly small: boolean,
  ) {
    this.#slot = new Uint8Array(length + ROOT_HEADER);
    this.#bytes = this.#slot.subarray(16, 16 + length);
  }
  get bytes(): Uint8Array {
    if (this.#refs === 0) throw new TypeError("Released root frame");
    return this.#bytes;
  }
  get slotBytes(): Uint8Array {
    if (this.#refs === 0) throw new TypeError("Released root frame");
    return this.#slot;
  }
  retain(): this {
    if (this.#refs === 0) throw new TypeError("Released root frame");
    this.#refs++;
    return this;
  }
  prepare(session: StoreSession, slot: 0 | 1, sequence: number): void {
    if (!Number.isSafeInteger(sequence) || sequence < 1) throw new RangeError("Bad root sequence");
    const view = new DataView(this.#slot.buffer);
    view.setUint32(0, 0x54524e4d, true);
    view.setUint32(4, sequence >>> 0, true);
    view.setUint32(8, Math.floor(sequence / 2 ** 32), true);
    view.setUint32(12, this.bytes.length, true);
    view.setUint32(this.#slot.length - 4, xxHash32(this.#slot, 0, 0, this.#slot.length - 4), true);
    prepared.set(this.bytes, { token: session.rootWriterToken ?? session, slot, frame: this });
  }
  release(): void {
    if (this.#refs <= 0) throw new TypeError("Root frame released twice");
    if (--this.#refs !== 0) return;
    prepared.delete(this.#bytes);
    this.#bytes = new Uint8Array(0);
    this.#slot = this.#bytes;
    this.owner.refund(this.charge, this.small);
  }
}
