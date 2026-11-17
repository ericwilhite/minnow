/** Client-side portable snapshot transport. The published 0.x entry point is unchanged.
 * One acknowledged chunk at a time; owner handles belong to one connection generation.
 */
import type { SnapshotExportOptions, SnapshotImportOptions } from "../../engine/database.js";
import type { WireValue } from "./protocol.js";

export type { SnapshotExportOptions, SnapshotImportOptions };
export const SNAPSHOT_TRANSFER_BYTES = 1 << 20;
export type SnapshotOp =
  | "snapshotExportOpen"
  | "snapshotExportRead"
  | "snapshotExportClose"
  | "snapshotImportOpen"
  | "snapshotImportWrite"
  | "snapshotImportFinish"
  | "snapshotImportAbort";
export type SnapshotRpc = (
  op: SnapshotOp,
  args: readonly WireValue[],
  signal?: AbortSignal,
) => Promise<unknown>;

export interface SnapshotApi {
  exportSnapshot(options?: SnapshotExportOptions): Promise<Uint8Array>;
  exportSnapshotStream(options?: SnapshotExportOptions): AsyncGenerator<Uint8Array, void, void>;
  /** Replaces the target. A failed or cancelled unpublished import preserves the old target. */
  importSnapshot(bytes: Uint8Array, options?: SnapshotImportOptions): Promise<void>;
  importSnapshotStream(
    source: AsyncIterable<Uint8Array>,
    options?: SnapshotImportOptions,
  ): Promise<void>;
}

const invalid = () => new TypeError("Invalid owner snapshot reply");
function handle(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) throw invalid();
  return value;
}

/** Races a stalled producer; iterator cleanup is requested but never awaited unboundedly. */
async function nextChunk(iterator: AsyncIterator<Uint8Array>, signal?: AbortSignal) {
  signal?.throwIfAborted();
  let abort: (() => void) | undefined;
  try {
    return await Promise.race([
      iterator.next(),
      new Promise<never>((_, reject) => {
        if (signal === undefined) return;
        // AbortSignal reasons may be arbitrary caller values, as with throwIfAborted().
        // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
        abort = () => reject(signal.reason);
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
      }),
    ]);
  } finally {
    if (abort !== undefined) signal?.removeEventListener("abort", abort);
  }
}

export class SnapshotClient implements SnapshotApi {
  constructor(
    readonly call: SnapshotRpc,
    readonly keepAlive: () => () => void = () => () => undefined,
  ) {}

  async exportSnapshot(options: SnapshotExportOptions = {}): Promise<Uint8Array> {
    const parts: Uint8Array[] = [];
    let length = 0;
    for await (const part of this.exportSnapshotStream(options)) {
      length += part.length;
      if (!Number.isSafeInteger(length) || length > 0xffffffff)
        throw new RangeError("Snapshot exceeds byte-array capacity; use exportSnapshotStream()");
      parts.push(part);
    }
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const part of parts) {
      bytes.set(part, offset);
      offset += part.length;
    }
    return bytes;
  }

  async *exportSnapshotStream(
    options: SnapshotExportOptions = {},
  ): AsyncGenerator<Uint8Array, void, void> {
    const { signal, onProgress } = options;
    signal?.throwIfAborted();
    onProgress?.({ phase: "reading", transferredBytes: 0, totalBytes: 0 });
    const id = handle(await this.call("snapshotExportOpen", [], signal));
    const relinquish = this.keepAlive();
    let released = false;
    const release = () => {
      if (!released) {
        released = true;
        relinquish();
      }
    };
    let transferredBytes = 0;
    let closing: Promise<unknown> | undefined;
    const close = () => (closing ??= this.call("snapshotExportClose", [id]).catch(() => undefined));
    // Cancellation must release the owner's pin even while the consumer holds a yielded chunk.
    const abort = () => {
      void close().finally(release);
    };
    signal?.addEventListener("abort", abort, { once: true });
    try {
      signal?.throwIfAborted();
      for (;;) {
        const value = await this.call("snapshotExportRead", [id], signal);
        if (value === null) {
          break;
        }
        if (
          !(value instanceof Uint8Array) ||
          value.length === 0 ||
          value.byteOffset !== 0 ||
          value.byteLength !== value.buffer.byteLength ||
          value.buffer.byteLength > SNAPSHOT_TRANSFER_BYTES
        )
          throw invalid();
        transferredBytes += value.length;
        if (!Number.isSafeInteger(transferredBytes)) throw invalid();
        onProgress?.({ phase: "transfer", transferredBytes, totalBytes: 0 });
        yield value;
      }
    } finally {
      signal?.removeEventListener("abort", abort);
      try {
        await close();
      } finally {
        release();
      }
    }
    onProgress?.({ phase: "done", transferredBytes, totalBytes: transferredBytes });
  }

  importSnapshot(bytes: Uint8Array, options: SnapshotImportOptions = {}): Promise<void> {
    if (!(bytes instanceof Uint8Array) || bytes.length === 0)
      return Promise.reject(new TypeError("Snapshot bytes must be a nonempty Uint8Array"));
    return this.importSnapshotStream(
      (async function* () {
        yield bytes;
      })(),
      options,
    );
  }

  async importSnapshotStream(
    source: AsyncIterable<Uint8Array>,
    options: SnapshotImportOptions = {},
  ): Promise<void> {
    const { signal, onProgress } = options;
    signal?.throwIfAborted();
    const id = handle(await this.call("snapshotImportOpen", [], signal));
    const release = this.keepAlive();
    let iterator: AsyncIterator<Uint8Array> | undefined;
    let writtenBytes = 0,
      complete = false;
    let aborting: Promise<unknown> | undefined;
    const cleanup = () =>
      (aborting ??= this.call("snapshotImportAbort", [id]).catch(() => undefined));
    const abort = () => {
      void cleanup();
    };
    signal?.addEventListener("abort", abort, { once: true });
    try {
      iterator = source[Symbol.asyncIterator]();
      onProgress?.({ phase: "blocks", writtenBytes, totalBytes: 0 });
      for (;;) {
        const next = await nextChunk(iterator, signal);
        if (next.done) break;
        const bytes = next.value;
        if (!(bytes instanceof Uint8Array))
          throw new TypeError("Snapshot chunks must be Uint8Array");
        for (let offset = 0; offset < bytes.length; offset += SNAPSHOT_TRANSFER_BYTES) {
          signal?.throwIfAborted();
          // A subview would structured-clone its entire backing store. Bound each message.
          const chunk = bytes.slice(offset, offset + SNAPSHOT_TRANSFER_BYTES);
          await this.call("snapshotImportWrite", [id, chunk], signal);
          writtenBytes += chunk.length;
          if (!Number.isSafeInteger(writtenBytes)) throw new RangeError("Snapshot length overflow");
          onProgress?.({ phase: "blocks", writtenBytes, totalBytes: 0 });
        }
      }
      onProgress?.({ phase: "catalog", writtenBytes, totalBytes: writtenBytes });
      await this.call("snapshotImportFinish", [id], signal);
      complete = true;
      onProgress?.({ phase: "done", writtenBytes, totalBytes: writtenBytes });
    } finally {
      signal?.removeEventListener("abort", abort);
      try {
        if (!complete) {
          try {
            void Promise.resolve(iterator?.return?.()).catch(() => undefined);
          } catch {
            /* cleanup cannot replace the source error */
          }
          await cleanup();
        }
      } finally {
        release();
      }
    }
  }
}
