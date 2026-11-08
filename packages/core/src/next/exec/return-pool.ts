/** Dedicated output buffers returned by a cooperative stream consumer. Never page/cache slabs.
 * A one-use capability names a sent frame slot, size and detached sender wrapper. Capabilities expire under bounded cache pressure, independently of credits.
 * Late/lost returns are ignored and never block execution.
 */
import {
  OBJECT_OVERHEAD_BYTES,
  type MemoryGovernor,
  type EvictableCache,
} from "../memory/governor.js";
const CAPABILITY_BYTES = OBJECT_OVERHEAD_BYTES;
// Native buffer wrapper plus its retained size-class entry/list storage.
const RETURNED_OVERHEAD_BYTES = 2 * OBJECT_OVERHEAD_BYTES;

/** Full-buffer classes: 8-byte alignment, quarter-power-of-two steps. Slack is output memory. */
export function outputCapacity(bytes: number): number {
  if (!Number.isSafeInteger(bytes) || bytes < 0) throw new RangeError("Invalid output size");
  if (bytes === 0) return 0;
  const step = Math.max(8, 2 ** (Math.floor(Math.log2(bytes)) - 2));
  const capacity = Math.ceil(bytes / step) * step;
  if (!Number.isSafeInteger(capacity)) throw new RangeError("Output size overflow");
  return capacity;
}

export interface ReturnedBuffer {
  readonly id: number;
  readonly buffer: ArrayBuffer;
}
export class ReturnPool implements EvictableCache {
  readonly #memory: ReturnType<MemoryGovernor["registerCache"]>;
  readonly #idle = new Map<number, ArrayBuffer[]>();
  readonly #issued = new Map<number, { frame: number; size: number; sender: ArrayBuffer }>();
  #next = 0;
  #bytes = 0;
  readonly #stored = new WeakSet<ArrayBuffer>();
  #closed = false;
  hits = 0;
  accepted = 0;
  rejected = 0;
  constructor(
    governor: MemoryGovernor,
    readonly capBytes: number,
  ) {
    if (!Number.isSafeInteger(capBytes) || capBytes < 0)
      throw new TypeError("Invalid return pool cap");
    this.#memory = governor.registerCache(this);
  }
  /** Called before output charging/allocation. take() still requires the exact physical size. */
  outputSize(bytes: number): number {
    return outputCapacity(bytes);
  }
  get evictableBytes(): number {
    return this.#bytes;
  }
  get oldestUnusedAt(): number | undefined {
    return this.#bytes > 0 ? 0 : undefined;
  }
  issue(frame: number, buffers: readonly ArrayBuffer[]): number[] {
    if (this.#closed) return [];
    return buffers.map((sender) => {
      this.#evictIdle(Math.max(0, this.#bytes + CAPABILITY_BYTES - this.capBytes));
      while (this.#issued.size >= 4096 || this.#bytes + CAPABILITY_BYTES > this.capBytes) {
        const first = this.#issued.keys().next().value;
        if (first === undefined) return -1;
        this.#issued.delete(first);
        this.#bytes -= CAPABILITY_BYTES;
        this.#memory.release(CAPABILITY_BYTES);
      }
      if (!this.#memory.charge(CAPABILITY_BYTES)) return -1;
      this.#bytes += CAPABILITY_BYTES;
      const id = ++this.#next;
      this.#issued.set(id, { frame, size: sender.byteLength, sender });
      return id;
    });
  }
  release(through: number, returns: readonly ReturnedBuffer[]): void {
    for (const item of returns) {
      const issued = this.#issued.get(item.id);
      if (issued) {
        this.#issued.delete(item.id);
        this.#bytes -= CAPABILITY_BYTES;
        this.#memory.release(CAPABILITY_BYTES);
      }
      const buffer = item.buffer;
      if (
        this.#closed ||
        !issued ||
        this.#stored.has(buffer) ||
        !Number.isSafeInteger(through) ||
        issued.frame > through ||
        issued.sender.byteLength !== 0 ||
        !(buffer instanceof ArrayBuffer) ||
        buffer.byteLength !== issued.size ||
        buffer.byteLength === 0 ||
        (buffer as ArrayBuffer & { resizable?: boolean }).resizable ||
        buffer.byteLength + RETURNED_OVERHEAD_BYTES > this.capBytes
      ) {
        this.rejected++;
        continue;
      }
      const cost = buffer.byteLength + RETURNED_OVERHEAD_BYTES;
      this.#evictIdle(Math.max(0, this.#bytes + cost - this.capBytes));
      if (this.#bytes + cost > this.capBytes || !this.#memory.charge(cost)) {
        this.rejected++;
        continue;
      }
      const list = this.#idle.get(buffer.byteLength) ?? [];
      this.#stored.add(buffer);
      list.push(buffer);
      this.#idle.set(buffer.byteLength, list);
      this.#bytes += cost;
      this.accepted++;
    }
  }
  take(bytes: number): ArrayBuffer | null {
    if (!Number.isSafeInteger(bytes) || bytes <= 0) return null;
    const list = this.#idle.get(bytes);
    const buffer = list?.pop();
    if (list?.length === 0) this.#idle.delete(bytes);
    if (!buffer) return null;
    this.#stored.delete(buffer);
    this.#bytes -= bytes + RETURNED_OVERHEAD_BYTES;
    this.#memory.release(bytes + RETURNED_OVERHEAD_BYTES);
    if (buffer.byteLength !== bytes) throw new Error("Detached or resized returned buffer");
    new Uint8Array(buffer).fill(0);
    this.hits++;
    return buffer;
  }
  #evictIdle(bytes: number): number {
    let freed = 0;
    for (const [size, list] of this.#idle) {
      while (list.length > 0 && freed < bytes) {
        const b = list.pop();
        if (!b) break;
        this.#stored.delete(b);
        freed += size + RETURNED_OVERHEAD_BYTES;
        this.#bytes -= size + RETURNED_OVERHEAD_BYTES;
        this.#memory.release(size + RETURNED_OVERHEAD_BYTES);
      }
      if (list.length === 0) this.#idle.delete(size);
      if (freed >= bytes) break;
    }
    return freed;
  }
  evict(bytes: number): number {
    let freed = this.#evictIdle(bytes);
    for (const id of this.#issued.keys()) {
      if (freed >= bytes) break;
      this.#issued.delete(id);
      freed += CAPABILITY_BYTES;
      this.#bytes -= CAPABILITY_BYTES;
      this.#memory.release(CAPABILITY_BYTES);
    }
    return freed;
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.evict(Infinity);
    this.#issued.clear();
    this.#memory.unregister?.();
  }
}
