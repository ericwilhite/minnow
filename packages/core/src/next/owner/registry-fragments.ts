/** One root scratch stream for image payloads. Reverse links avoid a resident page-ref list.
 * Complete scalar destinations are prepaid by the image helper; copying and I/O use two
 * fixed <=64KiB buffers, and every fragment passes through the root task clock.
 */
import type { MaybePromise } from "../access/key-index.js";
import type { SpillFile, SpillPageRef, SpillService } from "../spill/contract.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { ByteCharge } from "../vector/varlen.js";

const PAGE_BYTES = 65536,
  HEADER = 32,
  PAYLOAD = PAGE_BYTES - HEADER,
  MAGIC = 0x52465247;
export const REGISTRY_FRAGMENT_BYTES = 2 * PAGE_BYTES;
export interface RegistryPayload {
  readonly tail: SpillPageRef | null;
  readonly length: number;
}
export class RegistryFragments {
  readonly #stop = new AbortController();
  #signal: AbortSignal | null = null;
  #charge: ByteCharge | null = null;
  #checkpoint: (() => MaybePromise<void>) | null;
  #write: Uint8Array | null = null;
  #read: Uint8Array | null = null;
  #file: SpillFile | null = null;
  #opening: Promise<SpillFile> | null = null;
  #pending: Promise<unknown> | null = null;
  #closing: Promise<void> | null = null;
  #failure: { readonly reason: unknown } | null = null;
  #closed = false;
  constructor(
    charge: ByteCharge,
    readonly spill: SpillService,
    readonly queryId: number,
    signal: AbortSignal,
    checkpoint: () => MaybePromise<void>,
  ) {
    signal.throwIfAborted();
    if (!Number.isSafeInteger(queryId) || queryId < 0)
      throw new RangeError("Invalid registry query ID");
    if (!charge.allocate(REGISTRY_FRAGMENT_BYTES))
      throw new RangeError("Registry fragments must be admitted");
    this.#charge = charge;
    this.#checkpoint = checkpoint;
    try {
      this.#signal = AbortSignal.any([signal, this.#stop.signal]);
      this.#write = new Uint8Array(PAGE_BYTES);
      this.#read = new Uint8Array(PAGE_BYTES);
    } catch (error) {
      this.#stop.abort();
      this.#write = this.#read = null;
      this.#checkpoint = null;
      this.#signal = null;
      this.#charge = null;
      charge.free(REGISTRY_FRAGMENT_BYTES);
      throw error;
    }
  }
  #check(signal?: AbortSignal): void {
    signal?.throwIfAborted();
    this.#signal?.throwIfAborted();
    if (this.#closed) throw new TypeError("Registry fragments are closed");
    if (this.#failure) throw this.#failure.reason;
  }
  #work<T>(run: () => Promise<T>): Promise<T> {
    this.#check();
    if (this.#pending) throw new TypeError("Concurrent registry fragment operation");
    const work = run().catch((reason: unknown) => {
      this.#failure = { reason };
      throw reason;
    });
    const pending = work.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  async #pace(signal?: AbortSignal): Promise<void> {
    this.#check(signal);
    await this.#checkpoint?.();
    this.#check(signal);
  }
  async #open(): Promise<SpillFile> {
    this.#opening ??= this.spill.create(this.queryId, {
      allocate: () => false,
      free: () => undefined,
    });
    this.#file = await this.#opening;
    this.#check();
    return this.#file;
  }
  /** `fill` borrows the supplied payload window only until it resolves. It fills all bytes;
   * source buffers remain caller-owned, and the registry retains no source after completion. */
  append(
    length: number,
    fill: (into: Uint8Array, position: number) => MaybePromise<void>,
  ): Promise<RegistryPayload> {
    if (!Number.isSafeInteger(length) || length < 0)
      throw new RangeError("Invalid registry payload size");
    return this.#work(async () => {
      let tail: SpillPageRef | null = null;
      for (let position = 0; position < length;) {
        await this.#pace();
        const bytes = this.#write;
        if (!bytes) throw new TypeError("Registry fragments are closed");
        const count = Math.min(PAYLOAD, length - position),
          view = new DataView(bytes.buffer);
        view.setUint32(0, MAGIC, true);
        view.setUint32(4, 1, true);
        view.setFloat64(8, tail?.offset ?? -1, true);
        view.setUint32(16, tail?.length ?? 0, true);
        view.setUint32(20, tail?.checksum ?? 0, true);
        view.setUint32(24, count, true);
        view.setUint32(28, 0, true);
        await fill(bytes.subarray(HEADER, HEADER + count), position);
        this.#check();
        const file = await this.#open();
        tail = await file.append(bytes.subarray(0, HEADER + count));
        this.#check();
        position += count;
      }
      return { tail, length };
    });
  }
  readInto(payload: RegistryPayload, into: Uint8Array, signal?: AbortSignal): Promise<void> {
    this.#check(signal);
    if (
      !Number.isSafeInteger(payload.length) ||
      payload.length < 0 ||
      payload.length !== into.length
    )
      throw new RangeError("Registry payload destination has the wrong size");
    return this.#work(async () => {
      let cursor = payload.tail,
        remaining = payload.length,
        pages = Math.ceil(payload.length / PAYLOAD);
      while (cursor) {
        await this.#pace(signal);
        if (
          pages-- <= 0 ||
          !Number.isSafeInteger(cursor.offset) ||
          cursor.offset < 0 ||
          !Number.isInteger(cursor.length) ||
          cursor.length <= HEADER ||
          cursor.length > PAGE_BYTES ||
          !Number.isInteger(cursor.checksum) ||
          cursor.checksum < 0 ||
          cursor.checksum > 0xffffffff
        )
          throw new SpillCorruptionError("Invalid registry fragment reference");
        const bytes = this.#read,
          file = this.#file;
        if (!bytes || !file) throw new SpillCorruptionError("Missing registry payload file");
        const page = bytes.subarray(0, cursor.length);
        await file.readInto(cursor, page);
        this.#check(signal);
        const view = new DataView(page.buffer, page.byteOffset, page.byteLength),
          count = view.getUint32(24, true);
        if (
          view.getUint32(0, true) !== MAGIC ||
          view.getUint32(4, true) !== 1 ||
          view.getUint32(28, true) !== 0 ||
          count !== page.length - HEADER ||
          count > remaining
        )
          throw new SpillCorruptionError("Invalid registry fragment header");
        const offset = view.getFloat64(8, true),
          length = view.getUint32(16, true),
          checksum = view.getUint32(20, true);
        let previous: SpillPageRef | null = null;
        if (offset === -1) {
          if (length !== 0 || checksum !== 0)
            throw new SpillCorruptionError("Invalid registry fragment terminator");
        } else {
          if (
            !Number.isSafeInteger(offset) ||
            offset < 0 ||
            length <= HEADER ||
            length > PAGE_BYTES ||
            offset + length > cursor.offset
          )
            throw new SpillCorruptionError("Invalid registry fragment link");
          previous = { offset, length, checksum };
        }
        remaining -= count;
        into.set(page.subarray(HEADER), remaining);
        cursor = previous;
      }
      if (remaining !== 0 || pages !== 0)
        throw new SpillCorruptionError("Truncated registry payload");
    });
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new TypeError("Registry fragments are closed"));
      await this.#pending?.catch(() => undefined);
      try {
        const file = this.#file ?? (await this.#opening?.catch(() => null));
        await file?.close();
      } finally {
        this.#write = this.#read = null;
        this.#file = null;
        this.#opening = null;
        this.#pending = null;
        this.#checkpoint = null;
        this.#signal = null;
        this.#failure = null;
        const charge = this.#charge;
        this.#charge = null;
        charge?.free(REGISTRY_FRAGMENT_BYTES);
      }
    })());
  }
}
