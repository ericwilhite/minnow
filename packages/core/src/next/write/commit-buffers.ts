/** Owned commit buffers: capacities charged before allocation, including resize overlap. */
import type { Grant } from "../memory/governor.js";

/** Bounds the reusable fast path, independently of transaction size. */
export const SMALL_COMMIT_BYTES = 512 * 1024 - 16;
/** Two small writers, resize overlap, queued/in-flight frames and one streaming frame. */
export const COMMIT_MINIMUM_BYTES = 6 * 1024 * 1024;
/** Typed-array/buffer headers, ownership entry and a queue slot (governor object unit). */
export const COMMIT_BUFFER_OVERHEAD = 192;
export const LOG_QUEUE_BYTES = 1024 * 1024;

export interface BufferAllocator {
  create(bytes: number): Uint8Array;
  release(buffer: Uint8Array): void;
  /** Non-buffer transient codec memory (for example, request-count JSON spelling). */
  reserve?(bytes: number): () => void;
}

export class StreamCommit extends Error {}

export class CommitBuffers implements BufferAllocator {
  readonly #live = new Set<Uint8Array>();
  constructor(
    readonly grant: Grant,
    readonly observe?: (buffer: Uint8Array, live: boolean) => void,
  ) {}

  reserve(bytes: number): () => void {
    if (!this.grant.allocate(bytes)) throw new Error("Bounded commit-buffer minimum was exhausted");
    return () => this.grant.free(bytes);
  }

  create(bytes: number): Uint8Array {
    if (!this.grant.allocate(bytes + COMMIT_BUFFER_OVERHEAD))
      throw new Error("Bounded commit-buffer minimum was exhausted");
    let buffer: Uint8Array;
    try {
      buffer = new Uint8Array(bytes);
    } catch (error) {
      this.grant.free(bytes + COMMIT_BUFFER_OVERHEAD);
      throw error;
    }
    this.#live.add(buffer);
    this.observe?.(buffer, true);
    return buffer;
  }

  release(buffer: Uint8Array): void {
    if (!this.#live.delete(buffer)) throw new Error("Commit buffer released twice");
    this.observe?.(buffer, false);
    this.grant.free(buffer.byteLength + COMMIT_BUFFER_OVERHEAD);
  }

  close(): void {
    for (const buffer of this.#live) this.observe?.(buffer, false);
    this.#live.clear();
    this.grant.close();
  }
}
