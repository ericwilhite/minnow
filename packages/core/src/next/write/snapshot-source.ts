import { SnapshotSourceChangedError } from "../snapshot-errors.js";
export { SnapshotSourceChangedError };
/** One load path: immutable exact-offset reads. One-shot producers spool at the owner edge. */
import {
  decodeSnapshotFrameHeader,
  snapshotFrameBytes,
  SnapshotFormatError,
  type SnapshotWork,
} from "../format/snapshot.js";
import type { MemoryGovernor } from "../memory/governor.js";
import { DATA_ALIGN, type ScratchSpace } from "../storage/contract.js";
import type { ByteBudget } from "../storage/extents.js";
import { snapshotInput } from "./snapshot-iterator.js";
import { snapshotTake, snapshotPace } from "./snapshot-stage.js";

/** Refusal of a replay that no longer names the original bytes, not a retryable IO error. */

/** Two streaming 32-bit mixing lanes, independent of read partitioning. This is an
 * accidental-change digest, not authentication. Do NOT use the archive CRC here: a valid
 * frame plus its own CRC has a constant CRC residue even when its contents change. */
class PrefixDigest {
  a = 0x811c9dc5;
  b = 0x9e3779b9;
  add(bytes: Uint8Array): void {
    for (const byte of bytes) {
      this.a = Math.imul(this.a ^ byte, 0x01000193) >>> 0;
      this.b = (Math.imul(this.b + byte, 0x85ebca6b) ^ (this.b >>> 13)) >>> 0;
    }
  }
}
/** Fixed bookkeeping survives admission retries, never source bytes. A replay verifies the
 * furthest prefix BEFORE parsing it, then checks that boundary again as the parser consumes
 * it. New bytes extend the checkpoint only after the old boundary has matched. */
export class SnapshotSourcePrefix {
  offset = 0;
  #a = 0x811c9dc5;
  #b = 0x9e3779b9;
  constructor(readonly byteLength: number) {
    if (!Number.isSafeInteger(byteLength) || byteLength < 0)
      throw new SnapshotFormatError("Invalid snapshot source length");
  }
  check(digest: PrefixDigest): void {
    if (digest.a !== this.#a || digest.b !== this.#b) throw new SnapshotSourceChangedError();
  }
  extend(offset: number, digest: PrefixDigest): void {
    if (offset <= this.offset) return;
    this.offset = offset;
    this.#a = digest.a;
    this.#b = digest.b;
  }
  async verify(
    source: ReplayableSnapshotInput,
    budget: ByteBudget,
    signal?: AbortSignal,
  ): Promise<void> {
    if (source.byteLength !== this.byteLength) throw new SnapshotSourceChangedError();
    if (this.offset === 0) return;
    const size = Math.min(65536, this.offset),
      charge = size * 2 + 256;
    snapshotTake(budget, charge);
    try {
      const scratch = new Uint8Array(size),
        digest = new PrefixDigest();
      const readSignal = signal ?? new AbortController().signal;
      for (let offset = 0; offset < this.offset;) {
        readSignal.throwIfAborted();
        const into = scratch.subarray(0, Math.min(size, this.offset - offset));
        try {
          await source.readInto(offset, into, readSignal);
        } catch {
          readSignal.throwIfAborted();
          throw new SnapshotSourceChangedError();
        }
        digest.add(into);
        offset += into.length;
        await snapshotPace({ signal: readSignal });
      }
      this.check(digest);
    } finally {
      budget.free(charge);
    }
  }
}
export interface ReplayableSnapshotInput {
  readonly byteLength: number;
  /** Exact, immutable read. Settle before releasing destination ownership, including on abort.
   * At most destination.length additional scratch; the caller funds both before calling. */
  readInto(offset: number, destination: Uint8Array, signal: AbortSignal): Promise<void>;
}
export function snapshotBlob(blob: Blob): ReplayableSnapshotInput {
  return {
    byteLength: blob.size,
    async readInto(offset, destination, signal) {
      signal.throwIfAborted();
      if (offset < 0 || offset + destination.length > blob.size)
        throw new SnapshotFormatError("Read outside snapshot source");
      const bytes = await blob.slice(offset, offset + destination.length).arrayBuffer();
      signal.throwIfAborted();
      if (bytes.byteLength !== destination.length)
        throw new SnapshotFormatError("Short snapshot read");
      destination.set(new Uint8Array(bytes));
    },
  };
}
/** Frame-aware bounded pulling preserves backpressure at the footer/publication boundary. */
export class SnapshotReplay implements AsyncIterableIterator<Uint8Array> {
  readonly controller = new AbortController();
  #offset = 0;
  readonly #digest = new PrefixDigest();
  #remaining = 64;
  #prefix = false;
  #held = 0;
  #pending: Promise<IteratorResult<Uint8Array>> | undefined;
  #closed = false;
  readonly #abort: () => void;
  constructor(
    readonly source: ReplayableSnapshotInput,
    readonly budget: ByteBudget,
    readonly signal?: AbortSignal,
    readonly prefix?: SnapshotSourcePrefix,
  ) {
    if (!Number.isSafeInteger(source.byteLength) || source.byteLength < 0)
      throw new SnapshotFormatError("Invalid snapshot source length");
    this.#abort = () => this.controller.abort(signal?.reason);
    signal?.addEventListener("abort", this.#abort, { once: true });
    if (signal?.aborted === true) this.#abort();
  }
  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    return this;
  }
  #free(): void {
    this.budget.free(this.#held);
    this.#held = 0;
  }
  next(): Promise<IteratorResult<Uint8Array>> {
    if (this.#pending !== undefined) throw new TypeError("Concurrent snapshot reads");
    const pending = this.#next();
    this.#pending = pending;
    return pending.finally(() => {
      this.#pending = undefined;
    });
  }
  async #next(): Promise<IteratorResult<Uint8Array>> {
    this.#free();
    if (this.#closed) return { done: true, value: undefined };
    this.controller.signal.throwIfAborted();
    if (this.#offset === this.source.byteLength) return { done: true, value: undefined };
    if (this.#remaining === 0) {
      this.#remaining = 40;
      this.#prefix = true;
    }
    const boundary =
      this.prefix !== undefined && this.#offset < this.prefix.offset
        ? this.prefix.offset - this.#offset
        : 65536;
    const count = Math.min(65536, boundary, this.#remaining, this.source.byteLength - this.#offset);
    // Destination and optional Blob/file adapter scratch coexist through read completion.
    snapshotTake(this.budget, count * 2 + 128);
    this.#held = count * 2 + 128;
    const bytes = new Uint8Array(count);
    await this.source.readInto(this.#offset, bytes, this.controller.signal);
    this.controller.signal.throwIfAborted();
    this.#digest.add(bytes);
    this.#offset += count;
    if (this.prefix !== undefined) {
      if (this.#offset === this.prefix.offset) this.prefix.check(this.#digest);
      this.prefix.extend(this.#offset, this.#digest);
    }
    this.#remaining -= count;
    if (this.#prefix && count === 40) {
      this.#remaining = snapshotFrameBytes(decodeSnapshotFrameHeader(bytes).payloadBytes) - 40;
      this.#prefix = false;
    }
    return { done: false, value: bytes };
  }
  async return(): Promise<IteratorResult<Uint8Array>> {
    this.#closed = true;
    this.controller.abort(new DOMException("Snapshot read closed", "AbortError"));
    // Started reads own the destination until they settle. The loader also awaits close().
    await this.#pending?.catch(() => undefined);
    this.#free();
    this.signal?.removeEventListener("abort", this.#abort);
    return { done: true, value: undefined };
  }
  async close(): Promise<void> {
    await this.return();
  }
}

export interface SnapshotSpoolRegion {
  readonly scratch: ScratchSpace;
  /** Exclusive owner reservation; no other user may write or reclaim these bytes. */
  readonly offset: number;
  readonly capacity: number;
}
/** Owner edge only. It never truncates shared scratch or adds another core loader path.
 * The owner bounds producer chunks; their borrowed capacity is included in the complete step.
 * close() relinquishes the source after all core attempts/reads have settled. */
export async function spoolSnapshotInput(
  input: AsyncIterable<Uint8Array>,
  region: SnapshotSpoolRegion,
  governor: MemoryGovernor,
  options: SnapshotWork & { readonly maxChunkBytes: number },
): Promise<ReplayableSnapshotInput & { close(): Promise<void> }> {
  const { scratch, offset, capacity } = region,
    { maxChunkBytes, signal } = options;
  if (
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    offset % DATA_ALIGN !== 0 ||
    !Number.isSafeInteger(capacity) ||
    capacity < 0 ||
    capacity % DATA_ALIGN !== 0 ||
    !Number.isSafeInteger(offset + capacity) ||
    !Number.isSafeInteger(maxChunkBytes) ||
    maxChunkBytes < 1
  )
    throw new TypeError("Invalid snapshot spool reservation");
  const bytes = maxChunkBytes + 65536 + 1024;
  const [grant, sourceGrant] = await governor.admitGroup({
    label: "snapshot spool",
    includesProgress: true,
    grants: [
      { label: "snapshot spool copy", minimumBytes: bytes, spillable: false },
      { label: "snapshot spool source", minimumBytes: 1024, spillable: false },
    ],
    ...(signal === undefined ? {} : { signal }),
  });
  if (grant === undefined || sourceGrant === undefined)
    throw new TypeError("Missing snapshot spool grant");
  if (!sourceGrant.allocate(1024)) throw new TypeError("Lost snapshot spool source admission");
  let length = 0,
    written = 0,
    closed = false;
  let closing: Promise<void> | undefined;
  // Repeated close waits for the same started reclamation. Its fixed control credit
  // belongs to this source until native cleanup settles (governor rule 8).
  const release = (): Promise<void> => {
    if (closing !== undefined) return closing;
    closed = true;
    closing = (async () => {
      try {
        if (written > 0)
          await scratch.release([{ offset, length: Math.ceil(written / DATA_ALIGN) * DATA_ALIGN }]);
      } finally {
        sourceGrant.close();
      }
    })();
    return closing;
  };
  try {
    if (!grant.allocate(bytes)) throw new TypeError("Lost snapshot spool admission");
    const buffer = new Uint8Array(65536);
    let used = 0;
    for await (const chunk of snapshotInput(input, signal)) {
      await snapshotPace(options);
      if (chunk.buffer.byteLength > maxChunkBytes)
        throw new SnapshotFormatError("Snapshot producer exceeds its declared chunk bound");
      if (!Number.isSafeInteger(length + chunk.length) || length + chunk.length > capacity)
        throw new DOMException("Snapshot exceeds owner scratch reservation", "QuotaExceededError");
      length += chunk.length;
      for (let at = 0; at < chunk.length;) {
        const take = Math.min(buffer.length - used, chunk.length - at);
        buffer.set(chunk.subarray(at, at + take), used);
        used += take;
        at += take;
        if (used === buffer.length) {
          signal?.throwIfAborted();
          // Include a failed/partial write in cleanup.
          const start = written;
          written += used;
          await scratch.write(offset + start, buffer);
          await snapshotPace(options);
          used = 0;
        }
      }
    }
    if (used > 0) {
      const start = written;
      written += used;
      await scratch.write(offset + start, buffer.subarray(0, used));
    }
    signal?.throwIfAborted();
    return {
      byteLength: length,
      async readInto(at, destination, readSignal) {
        readSignal.throwIfAborted();
        if (closed) throw new TypeError("Snapshot spool released");
        if (!Number.isSafeInteger(at) || at < 0 || at + destination.length > length)
          throw new SnapshotFormatError("Read outside snapshot spool");
        await scratch.readInto(offset + at, destination);
        readSignal.throwIfAborted();
      },
      close: release,
    };
  } catch (error) {
    await release().catch(() => undefined);
    throw error;
  } finally {
    grant.close();
  }
}
