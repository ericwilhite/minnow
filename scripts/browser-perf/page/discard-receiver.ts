/**
 * Discard-only benchmark consumer. After consume() the caller must retain no column views.
 * Explicit disposal avoids depending on receiver GC to reclaim transferred backing stores.
 * Dictionary deltas remain live through the stream, then close() disposes them too. This is
 * deliberately outside production: published client Result/column views keep their lifetime.
 */
import {
  frameTransferList,
  type FramePayload,
} from "../../../packages/core/src/next/exec/result.js";

function dispose(buffer: ArrayBuffer): void {
  // ES2024 in the measured Chromium/Node, while the library's TS target stays ES2022.
  const transferable = buffer as ArrayBuffer & { transfer(length: number): ArrayBuffer };
  if (typeof transferable.transfer !== "function")
    throw new Error("Discard receiver benchmark requires ArrayBuffer.transfer");
  transferable.transfer(0);
}

export class DiscardReceiver {
  readonly #dictionaries = new Set<ArrayBuffer>();
  #disposedBytes = 0;
  get disposedBytes(): number {
    return this.#disposedBytes;
  }
  consume(frame: FramePayload): void {
    for (const buffer of frameTransferList({ ...frame, columns: [] }))
      this.#dictionaries.add(buffer);
    for (const buffer of frameTransferList(frame)) {
      if (this.#dictionaries.has(buffer)) continue;
      this.#disposedBytes += buffer.byteLength;
      dispose(buffer);
    }
  }
  close(): void {
    for (const buffer of this.#dictionaries) {
      this.#disposedBytes += buffer.byteLength;
      dispose(buffer);
    }
    this.#dictionaries.clear();
  }
}
