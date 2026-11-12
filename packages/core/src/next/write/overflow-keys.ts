/** One prepaid full-key head. Every verified lease ends before the next payload read. */
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type { BlobRef, RunRef } from "../format/manifest.js";
import { ByteReader, valid } from "../format/bytes.js";
import { requireEncoding } from "../format/encoding.js";
import { parseCharge } from "../format/manifest-codec.js";
import {
  KEY_DIRECTORY_MAGIC,
  KEY_DIRECTORY_VERSION,
  readKeyPointer,
  type KeyPointer,
} from "../format/key-pointer.js";
import { readRunKey, RUN_KEY_CHUNK_BYTES } from "../format/run-key.js";
import { compareBytes } from "../vector/varlen.js";
import { need } from "./bytes.js";

/** Validate before native construction or trusting the directory's declared lengths. */
export function runKeyBytes(ref: RunRef): number {
  if ((ref.encoding ?? 1) === 1) return 2048;
  requireEncoding("run", ref.encoding ?? 1, [1, 2]);
  const bytes = ref.keyStorage?.maxKeyBytes;
  valid(
    bytes !== undefined && Number.isSafeInteger(bytes) && bytes >= 0,
    "Missing or invalid run key capacity",
  );
  valid(bytes <= ref.entries.offset, "Run key capacity exceeds its stored data");
  return bytes;
}
const directoryBound = (bytes: number): number => 80 + 16 * Math.ceil(bytes / RUN_KEY_CHUNK_BYTES);
/** Caller charges this fixed workspace before constructing OverflowKeys. No per-key growth. */
export function overflowCapacityBytes(bytes: number): number {
  valid(Number.isSafeInteger(bytes) && bytes >= 0, "Invalid key workspace capacity");
  return bytes + parseCharge(Math.max(2112, directoryBound(bytes))) + 512;
}
export function overflowKeyBytes(ref: RunRef): number {
  return overflowCapacityBytes(runKeyBytes(ref));
}
export function pointerFrom(bytes: Uint8Array, before: number): KeyPointer {
  const r = new ByteReader(bytes),
    pointer = readKeyPointer(r, before);
  r.end();
  return pointer;
}

export class OverflowKeys {
  #head: Uint8Array;
  #closed = false;
  #check(): void {
    this.signal.throwIfAborted();
    if (this.#closed) throw new Error("Run key reader is closed");
  }
  close(): void {
    this.#closed = true;
    this.#head = new Uint8Array(0);
  }
  constructor(
    readonly ref: RunRef,
    readonly space: SpaceReader,
    readonly budget: ByteBudget,
    readonly signal: AbortSignal,
  ) {
    this.#head = new Uint8Array(runKeyBytes(ref));
  }
  async #read<T>(ref: BlobRef, use: (bytes: Uint8Array) => T): Promise<T> {
    this.#check();
    const cached = this.space.peek(ref);
    if (cached !== null) return use(cached);
    const lease = await this.space.read([ref], {
      budget: this.budget,
      retain: false,
      signal: this.signal,
    });
    try {
      this.#check();
      return use(need(lease.bytes[0]));
    } finally {
      lease.release();
    }
  }
  /** The view is valid until this instance's next resolve. It never aliases an I/O lease. */
  async resolve(pointer: KeyPointer): Promise<Uint8Array> {
    this.#check();
    if (pointer.kind === "inline") {
      valid(pointer.bytes.length <= this.#head.length, "Inline key exceeds run capacity");
      this.#head.set(pointer.bytes);
      return this.#head.subarray(0, pointer.bytes.length);
    }
    valid(pointer.length <= this.#head.length, "Overflow key exceeds run capacity");
    const dir = pointer.directory;
    valid(dir.length <= directoryBound(pointer.length), "Oversized key chunk directory");
    const chunks = await this.#read({ extent: this.ref.entries.extent, ...dir }, (bytes) => {
      const r = new ByteReader(bytes);
      valid(r.u32() === KEY_DIRECTORY_MAGIC, "Invalid key directory magic");
      requireEncoding("key directory", r.u8(), [KEY_DIRECTORY_VERSION]);
      const key = readRunKey(r, dir.offset);
      r.end();
      valid(key.kind === "overflow", "Inline key in overflow directory");
      valid(
        key.length === pointer.length &&
          compareBytes(
            key.prefix,
            0,
            key.prefix.length,
            pointer.prefix,
            0,
            pointer.prefix.length,
          ) === 0,
        "Key directory differs from pointer",
      );
      return key.chunks;
    });
    let at = 0;
    for (const chunk of chunks) {
      await this.#read({ extent: this.ref.entries.extent, ...chunk }, (bytes) => {
        valid(bytes.length === chunk.length, "Truncated key payload");
        this.#head.set(bytes, at);
      });
      at += chunk.length;
    }
    valid(
      at === pointer.length &&
        compareBytes(
          this.#head,
          0,
          pointer.prefix.length,
          pointer.prefix,
          0,
          pointer.prefix.length,
        ) === 0,
      "Key payload differs from prefix",
    );
    return this.#head.subarray(0, at);
  }
}
