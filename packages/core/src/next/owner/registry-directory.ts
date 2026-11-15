/** Root-local scratch descriptors: fixed charged leaves and a spilled radix directory.
 * Every namespace shares one file. Record count changes disk use, never resident metadata.
 * Readers supply one prepaid record buffer; no decoded descriptors are retained here.
 */
import type { SpillFile, SpillPageRef, SpillService } from "../spill/contract.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { ByteCharge } from "../vector/varlen.js";

export const REGISTRY_RECORD_BYTES = 128;
export const REGISTRY_DIRECTORY_BYTES = 30720;
const RECORDS = 16,
  LEAF_BYTES = 32 + RECORDS * REGISTRY_RECORD_BYTES,
  LEAVES = 8,
  NODE_BYTES = 264,
  LEVELS = 29,
  NODE_MAGIC = 0x52444952,
  LEAF_MAGIC = 0x524c4546;
interface Leaf {
  kind: number;
  owner: number;
  block: number;
  age: number;
  dirty: boolean;
  readonly bytes: Uint8Array;
}
export class RegistryDirectory {
  readonly #stop = new AbortController();
  #signal: AbortSignal | null = null;
  #path: Uint8Array | null = null;
  #leaves: Leaf[] = [];
  #root: SpillPageRef | null = null;
  #file: SpillFile | null = null;
  #opening: Promise<SpillFile> | null = null;
  #pending: Promise<unknown> | null = null;
  #closing: Promise<void> | null = null;
  #failure: { readonly reason: unknown } | null = null;
  #closed = false;
  #clock = 0;
  #pathKind = -1;
  #pathOwner = -1;
  #pathBlock = -1;
  #charge: ByteCharge | null = null;
  constructor(
    charge: ByteCharge,
    readonly spill: SpillService,
    readonly queryId: number,
    signal: AbortSignal,
  ) {
    signal.throwIfAborted();
    if (!Number.isSafeInteger(queryId) || queryId < 0)
      throw new RangeError("Invalid registry query ID");
    if (!charge.allocate(REGISTRY_DIRECTORY_BYTES))
      throw new RangeError("Registry directory must be admitted before construction");
    this.#charge = charge;
    try {
      this.#signal = AbortSignal.any([signal, this.#stop.signal]);
      this.#path = new Uint8Array(LEVELS * NODE_BYTES);
      for (let k = 0; k < LEAVES; k++)
        this.#leaves.push({
          kind: -1,
          owner: -1,
          block: -1,
          age: 0,
          dirty: false,
          bytes: new Uint8Array(LEAF_BYTES),
        });
    } catch (error) {
      this.#stop.abort();
      this.#path = null;
      this.#leaves = [];
      this.#signal = null;
      this.#charge = null;
      charge.free(REGISTRY_DIRECTORY_BYTES);
      throw error;
    }
  }
  #check(): void {
    this.#signal?.throwIfAborted();
    if (this.#closed) throw new TypeError("Registry directory is closed");
    if (this.#failure) throw this.#failure.reason;
  }
  #work<T>(run: () => Promise<T>): Promise<T> {
    this.#check();
    if (this.#pending) throw new TypeError("Concurrent registry directory operation");
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
  #validate(kind: number, id: number, bytes: Uint8Array, owner: number): void {
    if (
      !Number.isInteger(kind) ||
      kind < 0 ||
      kind > 255 ||
      !Number.isSafeInteger(id) ||
      id < 0 ||
      !Number.isSafeInteger(owner) ||
      owner < 0 ||
      bytes.length !== REGISTRY_RECORD_BYTES
    )
      throw new RangeError("Invalid registry namespace, record ID or record buffer");
  }
  get(kind: number, id: number, into: Uint8Array, owner = 0): Promise<boolean> {
    this.#validate(kind, id, into, owner);
    return this.#work(async () => {
      const leaf = await this.#leaf(kind, owner, Math.floor(id / RECORDS));
      this.#check();
      const row = id % RECORDS;
      if ((new DataView(leaf.bytes.buffer).getUint16(24, true) & (1 << row)) === 0) {
        into.fill(0);
        return false;
      }
      into.set(
        leaf.bytes.subarray(
          32 + row * REGISTRY_RECORD_BYTES,
          32 + (row + 1) * REGISTRY_RECORD_BYTES,
        ),
      );
      return true;
    });
  }
  put(kind: number, id: number, bytes: Uint8Array, owner = 0): Promise<void> {
    this.#validate(kind, id, bytes, owner);
    return this.#work(async () => {
      const leaf = await this.#leaf(kind, owner, Math.floor(id / RECORDS));
      this.#check();
      const row = id % RECORDS,
        view = new DataView(leaf.bytes.buffer);
      leaf.bytes.set(bytes, 32 + row * REGISTRY_RECORD_BYTES);
      view.setUint16(24, view.getUint16(24, true) | (1 << row), true);
      leaf.dirty = true;
    });
  }
  #digit(kind: number, owner: number, block: number, level: number): number {
    return level < 2
      ? Math.floor(kind / 16 ** (1 - level)) % 16
      : level < 16
        ? Math.floor(owner / 16 ** (15 - level)) % 16
        : Math.floor(block / 16 ** (28 - level)) % 16;
  }
  #ref(node: Uint8Array, digit: number): SpillPageRef | null {
    const view = new DataView(node.buffer, node.byteOffset, node.byteLength),
      at = 8 + digit * 16;
    const length = view.getUint32(at + 8, true);
    const offset = view.getFloat64(at, true),
      checksum = view.getUint32(at + 12, true);
    if (length === 0) {
      if (offset !== 0 || checksum !== 0)
        throw new SpillCorruptionError("Invalid empty registry reference");
      return null;
    }
    if (!Number.isSafeInteger(offset) || offset < 0)
      throw new SpillCorruptionError("Invalid registry page offset");
    return { offset, length, checksum };
  }
  #setRef(node: Uint8Array, digit: number, ref: SpillPageRef): void {
    const view = new DataView(node.buffer, node.byteOffset, node.byteLength),
      at = 8 + digit * 16;
    view.setUint32(0, NODE_MAGIC, true);
    view.setFloat64(at, ref.offset, true);
    view.setUint32(at + 8, ref.length, true);
    view.setUint32(at + 12, ref.checksum, true);
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
  async #walk(kind: number, owner: number, block: number): Promise<SpillPageRef | null> {
    this.#check();
    const path = this.#path;
    if (!path) throw new TypeError("Registry directory is closed");
    // The path is already prepaid. Reuse shared ancestors, including nodes updated by
    // the last copy-on-write flush; consecutive record IDs usually need no page reads.
    let reused = 0;
    if (this.#pathKind >= 0)
      for (let level = 0; level < LEVELS; level++) {
        reused = level + 1;
        if (
          this.#digit(kind, owner, block, level) !==
          this.#digit(this.#pathKind, this.#pathOwner, this.#pathBlock, level)
        )
          break;
      }
    let ref = this.#root;
    for (let level = 0; level < LEVELS; level++) {
      const node = path.subarray(level * NODE_BYTES, (level + 1) * NODE_BYTES);
      if (level >= reused) {
        if (ref === null) node.fill(0);
        else {
          if (!this.#file || ref.length !== NODE_BYTES)
            throw new SpillCorruptionError("Invalid registry directory page");
          await this.#file.readInto(ref, node);
          this.#check();
          if (new DataView(node.buffer, node.byteOffset).getUint32(0, true) !== NODE_MAGIC)
            throw new SpillCorruptionError("Invalid registry directory magic");
        }
      }
      ref = this.#ref(node, this.#digit(kind, owner, block, level));
    }
    this.#pathKind = kind;
    this.#pathOwner = owner;
    this.#pathBlock = block;
    return ref;
  }
  async #flush(leaf: Leaf): Promise<void> {
    if (!leaf.dirty) return;
    const file = await this.#open();
    await this.#walk(leaf.kind, leaf.owner, leaf.block);
    let ref = await file.append(leaf.bytes);
    this.#check();
    const path = this.#path;
    if (!path) throw new TypeError("Registry directory is closed");
    for (let level = LEVELS - 1; level >= 0; level--) {
      const node = path.subarray(level * NODE_BYTES, (level + 1) * NODE_BYTES);
      this.#setRef(node, this.#digit(leaf.kind, leaf.owner, leaf.block, level), ref);
      ref = await file.append(node);
      this.#check();
    }
    this.#root = ref;
    leaf.dirty = false;
  }
  async #leaf(kind: number, owner: number, block: number): Promise<Leaf> {
    this.#check();
    let leaf = this.#leaves.find(
      (entry) => entry.kind === kind && entry.owner === owner && entry.block === block,
    );
    if (!leaf) {
      leaf = this.#leaves.reduce((a, b) => (a.age <= b.age ? a : b));
      await this.#flush(leaf);
      const ref = await this.#walk(kind, owner, block);
      if (ref) {
        if (!this.#file || ref.length !== LEAF_BYTES)
          throw new SpillCorruptionError("Invalid registry leaf size");
        await this.#file.readInto(ref, leaf.bytes);
        this.#check();
        const view = new DataView(leaf.bytes.buffer);
        if (
          view.getUint32(0, true) !== LEAF_MAGIC ||
          view.getUint32(4, true) !== kind ||
          view.getFloat64(8, true) !== owner ||
          view.getFloat64(16, true) !== block
        )
          throw new SpillCorruptionError("Invalid registry leaf identity");
      } else {
        leaf.bytes.fill(0);
        const view = new DataView(leaf.bytes.buffer);
        view.setUint32(0, LEAF_MAGIC, true);
        view.setUint32(4, kind, true);
        view.setFloat64(8, owner, true);
        view.setFloat64(16, block, true);
      }
      leaf.kind = kind;
      leaf.owner = owner;
      leaf.block = block;
      leaf.dirty = false;
    }
    leaf.age = ++this.#clock;
    return leaf;
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#closed = true;
      this.#stop.abort(new TypeError("Registry directory is closed"));
      await this.#pending?.catch(() => undefined);
      try {
        const file = this.#file ?? (await this.#opening?.catch(() => null));
        await file?.close();
      } finally {
        this.#path = null;
        this.#leaves = [];
        this.#root = null;
        this.#file = null;
        this.#opening = null;
        this.#pending = null;
        this.#signal = null;
        this.#failure = null;
        const charge = this.#charge;
        this.#charge = null;
        charge?.free(REGISTRY_DIRECTORY_BYTES);
      }
    })());
  }
}
