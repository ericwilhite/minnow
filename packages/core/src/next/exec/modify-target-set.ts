/** Statement repeat tracking: eight charged bitmap leaves and a fixed-depth spilled radix directory.
 * Keys are (table ID, native row ID), so rekeys retain identity and tables never alias.
 * No resident directory or file is created per target, segment or parent event. */
import type { SpillFile, SpillPageRef, SpillService } from "../spill/contract.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { ByteCharge } from "../vector/varlen.js";
import { ExpressionMemoryError } from "./kernels/vectors.js";

const LEVELS = 25,
  NODE_BYTES = 264,
  LEAF_BYTES = 128,
  CACHE_LEAVES = 8;
export const MODIFY_TARGET_SET_BYTES = 16384;
const MAGIC = 0x4d544152;
interface Leaf {
  table: number;
  block: number;
  dirty: boolean;
  age: number;
  bits: Uint8Array;
}
export class ModifyTargetSet {
  #path: Uint8Array | null;
  readonly #leaves: Leaf[] = [];
  #root: SpillPageRef | null = null;
  #file: SpillFile | null = null;
  #opening: Promise<SpillFile> | null = null;
  #pending: Promise<boolean> | null = null;
  #closed = false;
  #closing: Promise<void> | null = null;
  #clock = 0;
  constructor(
    readonly charge: ByteCharge,
    readonly spill: SpillService,
    readonly queryId: number,
    readonly signal: AbortSignal,
  ) {
    if (!charge.allocate(MODIFY_TARGET_SET_BYTES))
      throw new ExpressionMemoryError(MODIFY_TARGET_SET_BYTES);
    this.#path = new Uint8Array(LEVELS * NODE_BYTES);
    for (let k = 0; k < CACHE_LEAVES; k++)
      this.#leaves.push({
        table: -1,
        block: -1,
        dirty: false,
        age: 0,
        bits: new Uint8Array(LEAF_BYTES),
      });
  }
  testAndSet(table: number, row: number): Promise<boolean> {
    if (this.#closed) return Promise.reject(new TypeError("Repeat tracker is closed"));
    if (this.#pending) throw new TypeError("Concurrent repeat-tracker operation");
    const work = this.#mark(table, row);
    this.#pending = work;
    void work
      .finally(() => {
        if (this.#pending === work) this.#pending = null;
      })
      .catch(() => undefined);
    return work;
  }
  #check(): void {
    this.signal.throwIfAborted();
    if (this.#closed) throw new TypeError("Repeat tracker is closed");
  }
  #digit(table: number, block: number, level: number): number {
    return level < 14
      ? Math.floor(table / 16 ** (13 - level)) % 16
      : Math.floor(block / 16 ** (24 - level)) % 16;
  }
  #ref(node: Uint8Array, digit: number): SpillPageRef | null {
    const view = new DataView(node.buffer, node.byteOffset, node.byteLength),
      at = 8 + 16 * digit;
    const length = view.getUint32(at + 8, true);
    return length === 0
      ? null
      : { offset: view.getFloat64(at, true), length, checksum: view.getUint32(at + 12, true) };
  }
  #setRef(node: Uint8Array, digit: number, ref: SpillPageRef): void {
    const view = new DataView(node.buffer, node.byteOffset, node.byteLength),
      at = 8 + 16 * digit;
    view.setUint32(0, MAGIC, true);
    view.setFloat64(at, ref.offset, true);
    view.setUint32(at + 8, ref.length, true);
    view.setUint32(at + 12, ref.checksum, true);
  }
  async #open(): Promise<SpillFile> {
    // Directory/bitmap buffers are prepaid; disable optional service write batching.
    this.#opening ??= this.spill.create(this.queryId, {
      allocate: () => false,
      free: () => undefined,
    });
    this.#file = await this.#opening;
    this.#check();
    return this.#file;
  }
  async #walk(table: number, block: number): Promise<SpillPageRef | null> {
    this.#check();
    const path = this.#path;
    if (!path) throw new TypeError("Repeat tracker is closed");
    let ref = this.#root;
    for (let level = 0; level < LEVELS; level++) {
      const node = path.subarray(level * NODE_BYTES, (level + 1) * NODE_BYTES);
      if (ref === null) node.fill(0);
      else {
        if (ref.length !== NODE_BYTES || !this.#file)
          throw new SpillCorruptionError("Invalid repeat directory page");
        await this.#file.readInto(ref, node);
        this.#check();
        if (new DataView(node.buffer, node.byteOffset).getUint32(0, true) !== MAGIC)
          throw new SpillCorruptionError("Invalid repeat directory magic");
      }
      ref = this.#ref(node, this.#digit(table, block, level));
    }
    return ref;
  }
  async #flush(leaf: Leaf): Promise<void> {
    if (!leaf.dirty) return;
    const file = await this.#open();
    await this.#walk(leaf.table, leaf.block);
    let ref = await file.append(leaf.bits);
    this.#check();
    const path = this.#path;
    if (!path) throw new TypeError("Repeat tracker is closed");
    for (let level = LEVELS - 1; level >= 0; level--) {
      const node = path.subarray(level * NODE_BYTES, (level + 1) * NODE_BYTES);
      this.#setRef(node, this.#digit(leaf.table, leaf.block, level), ref);
      ref = await file.append(node);
      this.#check();
    }
    this.#root = ref;
    leaf.dirty = false;
  }
  async #mark(table: number, row: number): Promise<boolean> {
    this.#check();
    if (!Number.isSafeInteger(table) || table < 0 || !Number.isSafeInteger(row) || row < 0)
      throw new RangeError("Repeat identity must be a nonnegative safe integer");
    const block = Math.floor(row / (LEAF_BYTES * 8));
    let leaf = this.#leaves.find((l) => l.table === table && l.block === block);
    if (!leaf) {
      leaf = this.#leaves.reduce((a, b) => (a.age <= b.age ? a : b));
      await this.#flush(leaf);
      const ref = await this.#walk(table, block);
      if (ref) {
        if (ref.length !== LEAF_BYTES || !this.#file)
          throw new SpillCorruptionError("Invalid repeat bitmap page");
        await this.#file.readInto(ref, leaf.bits);
        this.#check();
      } else leaf.bits.fill(0);
      leaf.table = table;
      leaf.block = block;
      leaf.dirty = false;
    }
    leaf.age = ++this.#clock;
    const bit = row % (LEAF_BYTES * 8),
      at = bit >>> 3,
      mask = 1 << (bit & 7),
      previous = leaf.bits[at] ?? 0;
    if ((previous & mask) !== 0) return true;
    leaf.bits[at] = previous | mask;
    leaf.dirty = true;
    return false;
  }
  close(): void | Promise<void> {
    if (this.#closing) return this.#closing;
    if (this.#closed) return;
    this.#closed = true;
    const release = () => {
      this.#root = null;
      this.#path = null;
      this.#leaves.length = 0;
      this.charge.free(MODIFY_TARGET_SET_BYTES);
    };
    if (!this.#pending && !this.#opening) {
      release();
      return;
    }
    return (this.#closing = (async () => {
      try {
        await this.#pending?.catch(() => undefined);
        const file = await this.#opening?.catch(() => null);
        await file?.close();
      } finally {
        this.#file = null;
        this.#opening = null;
        release();
      }
    })());
  }
}
