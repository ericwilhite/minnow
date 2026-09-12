/**
 * Models what `MemoryOpfs` cannot: the loss of unflushed writes at power loss.
 *
 * The shim applies every `write`/`truncate` immediately, so a "crash" in the other suites is a
 * process death: whatever was written is kept, flushed or not. That models a tab dying, not a
 * device losing power. This helper rides the shim's write-fault hook (called with the path and
 * phase on every write, truncate, create, and flush) to remember each file's content as of its
 * last `flush()`. `powerLoss()` then rolls every touched file back to that content, optionally
 * keeping a prefix of the unflushed suffix (a partially written-back append).
 *
 * `close()` is deliberately not treated as a flush: a closed descriptor's dirty pages are still
 * only in the OS cache.
 */
import type { MemoryOpfs } from "../../testing/opfs-shim.js";

export class PowerLossModel {
  readonly #durable = new Map<string, Uint8Array>();
  readonly #touched = new Set<string>();
  readonly #shim: MemoryOpfs;

  constructor(shim: MemoryOpfs) {
    this.#shim = shim;
    shim.setWriteFault((path, phase) => {
      if (phase === "flush") {
        this.#durable.set(path, shim.readFileBytes(path) ?? new Uint8Array());
      } else {
        this.#touched.add(path);
      }
    });
  }

  /** Bytes that would survive power loss right now. */
  durableBytes(path: string): Uint8Array | undefined {
    return this.#durable.get(path);
  }

  /**
   * Reverts every file to its last flushed content. `keepPrefix(unflushed, path)` may return how
   * many of the unflushed appended bytes survive (a torn append). Returns the reverted paths.
   */
  powerLoss(keepPrefix?: (unflushedBytes: number, path: string) => number): string[] {
    const reverted: string[] = [];
    for (const path of this.#touched) {
      const current = this.#shim.readFileBytes(path);
      if (current === undefined) continue;
      const durable = this.#durable.get(path) ?? new Uint8Array();
      let kept = durable;
      if (
        keepPrefix !== undefined &&
        current.byteLength > durable.byteLength &&
        startsWith(current, durable)
      ) {
        const unflushed = current.byteLength - durable.byteLength;
        const extra = keepPrefix(unflushed, path);
        kept = current.slice(0, durable.byteLength + Math.max(0, Math.min(extra, unflushed)));
      }
      if (!bytesEqual(kept, current)) {
        this.#shim.writeFileBytes(path, kept);
        reverted.push(path);
      }
      this.#durable.set(path, kept.slice());
    }
    this.#touched.clear();
    return reverted;
  }
}

function startsWith(bytes: Uint8Array, prefix: Uint8Array): boolean {
  if (prefix.byteLength > bytes.byteLength) return false;
  for (let index = 0; index < prefix.byteLength; index += 1) {
    if (bytes[index] !== prefix[index]) return false;
  }
  return true;
}

export function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}
