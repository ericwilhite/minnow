/** Scripted native response and physical-store accounting for metadata integration tests. */
import { StoreCorruptionError, type MetadataStore } from "../storage/contract.js";
/** Physical storage is independent of managed frames. Simulate one native cloned response. */
export class ScriptedMetadataPages implements MetadataStore {
  readonly physical = new Map<number, Uint8Array>();
  native = 0;
  maxNative = 0;
  writes = 0;
  failAt = -1;
  afterRead: (() => void) | null = null;
  afterWrite: ((page: number, bytes: Uint8Array) => void) | null = null;
  async write(page: number, bytes: Uint8Array): Promise<void> {
    if (++this.writes === this.failAt) throw new Error("scripted metadata write failure");
    this.physical.set(page, bytes.slice());
    await Promise.resolve();
    this.afterWrite?.(page, bytes);
  }
  async read(page: number, target: Uint8Array): Promise<void> {
    const bytes = this.physical.get(page);
    if (bytes === undefined) throw new StoreCorruptionError("missing page");
    this.native += bytes.byteLength;
    this.maxNative = Math.max(this.maxNative, this.native);
    try {
      const response = bytes.slice();
      await Promise.resolve();
      target.set(response);
      this.afterRead?.();
    } finally {
      this.native -= bytes.byteLength;
    }
  }
  async reset(): Promise<void> {
    this.physical.clear();
  }
}
