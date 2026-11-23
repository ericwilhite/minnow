/** Root-local scratch descriptors: fixed charged leaves and a spilled radix directory.
 * Every namespace shares one file. Record count changes disk use, never resident metadata.
 * Readers supply one prepaid record buffer; no decoded descriptors are retained here.
 *
 * The directory is a path-compressed radix-16 tree over the 29 digits of (kind, owner, block):
 * a node at depth `d` consumes its `skip` digits (stored in the node), then branches on digit
 * `d + skip`; a child at depth 29 is a leaf. Real keys are small (few kinds, owner 0, blocks
 * counting up), so a path is a handful of nodes instead of 29: a leaf miss reads and a flush
 * appends only those. The pages are this query's scratch (a spill file), not a stored format.
 */
import type { SpillFile, SpillPageRef, SpillService } from "../spill/contract.js";
import { SpillCorruptionError } from "../spill/contract.js";
import type { ByteCharge } from "../vector/varlen.js";
import {
  REGISTRY_DIRECTORY_BYTES,
  REGISTRY_DIRECTORY_LEAVES,
  REGISTRY_LEAF_BYTES,
  REGISTRY_LEAF_RECORDS,
  REGISTRY_LEVELS,
  REGISTRY_NODE_BYTES,
  REGISTRY_RECORD_BYTES,
} from "../exec/row-image-layout.js";

export { REGISTRY_DIRECTORY_BYTES, REGISTRY_RECORD_BYTES };
const RECORDS = REGISTRY_LEAF_RECORDS,
  LEAF_BYTES = REGISTRY_LEAF_BYTES,
  LEAVES = REGISTRY_DIRECTORY_LEAVES,
  NODE_BYTES = REGISTRY_NODE_BYTES,
  LEVELS = REGISTRY_LEVELS,
  NODE_HEADER = 40,
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
/**
 * The directory's work since load, summed over every directory: plain counters on its spill path
 * (never on a record copy), read by tests that bound the work per written row.
 */
export const REGISTRY_DIRECTORY_WORK = {
  /** A record block whose leaf was not resident. */
  leafMisses: 0,
  /** Dirty leaves written out (each appends the leaf and its root-to-leaf path). */
  flushes: 0,
  /** Directory node and leaf pages read back from the spill file. */
  pageReads: 0,
  /** Pages appended to the spill file. */
  appends: 0,
};
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
  /** The cached path: `#pathCount` nodes for the key in `#pathKey` (valid when it is >= 0). */
  #pathCount = -1;
  readonly #pathKey = new Uint8Array(LEVELS);
  readonly #key = new Uint8Array(LEVELS);
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
      // One node per key digit at most, plus a scratch node for a split's remainder.
      this.#path = new Uint8Array((LEVELS + 1) * NODE_BYTES);
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
  #keyOf(kind: number, owner: number, block: number, into: Uint8Array): void {
    for (let level = 0; level < LEVELS; level++)
      into[level] = this.#digit(kind, owner, block, level);
  }
  #slot(index: number): Uint8Array {
    const path = this.#path;
    if (!path) throw new TypeError("Registry directory is closed");
    return path.subarray(index * NODE_BYTES, (index + 1) * NODE_BYTES);
  }
  #ref(node: Uint8Array, digit: number): SpillPageRef | null {
    const view = new DataView(node.buffer, node.byteOffset, node.byteLength),
      at = NODE_HEADER + digit * 16;
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
      at = NODE_HEADER + digit * 16;
    view.setFloat64(at, ref.offset, true);
    view.setUint32(at + 8, ref.length, true);
    view.setUint32(at + 12, ref.checksum, true);
  }
  /** Writes an empty node at `depth` that skips `key[depth .. depth + skip - 1]`. */
  #initNode(
    node: Uint8Array,
    depth: number,
    skip: number,
    digits: ArrayLike<number>,
    from: number,
  ): void {
    node.fill(0);
    new DataView(node.buffer, node.byteOffset, node.byteLength).setUint32(0, NODE_MAGIC, true);
    node[4] = depth;
    node[5] = skip;
    for (let k = 0; k < skip; k++) node[8 + k] = digits[from + k] ?? 0;
  }
  /** A node read back at `depth`: checked before any of it is used. */
  #checkNode(node: Uint8Array, depth: number): void {
    const skip = node[5] ?? 0;
    if (
      new DataView(node.buffer, node.byteOffset, node.byteLength).getUint32(0, true) !==
        NODE_MAGIC ||
      node[4] !== depth ||
      depth + skip >= LEVELS
    )
      throw new SpillCorruptionError("Invalid registry directory node");
    for (let k = 0; k < skip; k++)
      if ((node[8 + k] ?? 16) > 15) throw new SpillCorruptionError("Invalid registry skip digit");
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
  async #append(page: Uint8Array): Promise<SpillPageRef> {
    const file = await this.#open();
    REGISTRY_DIRECTORY_WORK.appends++;
    const ref = await file.append(page);
    this.#check();
    return ref;
  }
  /**
   * Loads the path of `key` into the path slots and says where it ends: at the leaf's reference
   * (`found`), at a missing child of node `at` (`missing`), inside node `at`'s skip after `shared`
   * matching digits (`split`), or at an empty tree (`empty`). Nodes the cached path shares with
   * `key` are not read again.
   */
  async #walk(
    key: Uint8Array,
  ): Promise<
    | { kind: "empty" }
    | { kind: "found"; at: number; ref: SpillPageRef }
    | { kind: "missing"; at: number }
    | { kind: "split"; at: number; shared: number }
  > {
    this.#check();
    // Cached nodes stay valid up to the depth at which `key` leaves the cached key.
    let common = 0;
    if (this.#pathCount >= 0)
      while (common < LEVELS && key[common] === this.#pathKey[common]) common++;
    let ref = this.#root,
      depth = 0,
      at = 0;
    if (ref === null) {
      this.#pathCount = 0;
      this.#pathKey.set(key);
      return { kind: "empty" };
    }
    for (;;) {
      const node = this.#slot(at);
      const cached = this.#pathCount > at && depth <= common;
      if (!cached) {
        if (!this.#file || ref === null || ref.length !== NODE_BYTES)
          throw new SpillCorruptionError("Invalid registry directory page");
        REGISTRY_DIRECTORY_WORK.pageReads++;
        await this.#file.readInto(ref, node);
        this.#check();
        this.#checkNode(node, depth);
      }
      const skip = node[5] ?? 0;
      let shared = 0;
      while (shared < skip && node[8 + shared] === key[depth + shared]) shared++;
      this.#pathCount = at + 1;
      this.#pathKey.set(key);
      if (shared < skip) return { kind: "split", at, shared };
      const child = this.#ref(node, key[depth + skip] ?? 0);
      const next = depth + skip + 1;
      if (next === LEVELS)
        return child ? { kind: "found", at, ref: child } : { kind: "missing", at };
      if (child === null) return { kind: "missing", at };
      ref = child;
      depth = next;
      at++;
      if (at >= LEVELS) throw new SpillCorruptionError("Invalid registry directory depth");
    }
  }
  async #flush(leaf: Leaf): Promise<void> {
    if (!leaf.dirty) return;
    REGISTRY_DIRECTORY_WORK.flushes++;
    const key = this.#key;
    this.#keyOf(leaf.kind, leaf.owner, leaf.block, key);
    const end = await this.#walk(key);
    let ref = await this.#append(leaf.bytes);
    // The node that now leads to the leaf, at path slot `at`; its ancestors are slots below it.
    let at: number;
    if (end.kind === "empty") {
      // One node for the whole key: it skips digits 0..27 and branches on the last.
      at = 0;
      const tail = this.#slot(0);
      this.#initNode(tail, 0, LEVELS - 1, key, 0);
      this.#setRef(tail, key[LEVELS - 1] ?? 0, ref);
      ref = await this.#append(tail);
    } else if (end.kind === "found" || end.kind === "missing") {
      at = end.at;
      const node = this.#slot(at),
        depth = node[4] ?? 0,
        branch = depth + (node[5] ?? 0);
      if (branch + 1 < LEVELS) {
        // A new tail below: it skips the rest of the key and branches on its last digit.
        const tail = this.#slot(at + 1);
        this.#initNode(tail, branch + 1, LEVELS - branch - 2, key, branch + 1);
        this.#setRef(tail, key[LEVELS - 1] ?? 0, ref);
        ref = await this.#append(tail);
        this.#pathCount = at + 2;
      }
      this.#setRef(node, key[branch] ?? 0, ref);
      ref = await this.#append(node);
    } else {
      // `key` leaves node `at` inside its skip: split it into a branch over the shared digits,
      // the node's remainder below it, and a new tail for `key`.
      at = end.at;
      const old = this.#slot(at),
        depth = old[4] ?? 0,
        skip = old[5] ?? 0,
        shared = end.shared,
        oldDigit = old[8 + shared] ?? 0;
      const rest = this.#slot(LEVELS);
      rest.set(old);
      rest[4] = depth + shared + 1;
      rest[5] = skip - shared - 1;
      rest.fill(0, 8, 8 + 28);
      for (let k = 0; k < skip - shared - 1; k++) rest[8 + k] = old[8 + shared + 1 + k] ?? 0;
      const restRef = await this.#append(rest);
      const tail = this.#slot(at + 1),
        tailDepth = depth + shared + 1;
      this.#initNode(tail, tailDepth, LEVELS - tailDepth - 1, key, tailDepth);
      this.#setRef(tail, key[LEVELS - 1] ?? 0, ref);
      const tailRef = await this.#append(tail);
      const branch = this.#slot(at);
      this.#initNode(branch, depth, shared, key, depth);
      this.#setRef(branch, oldDigit, restRef);
      this.#setRef(branch, key[depth + shared] ?? 0, tailRef);
      ref = await this.#append(branch);
      this.#pathCount = at + 2;
    }
    // Copy-on-write up the path: each ancestor points at its new child.
    for (let index = at - 1; index >= 0; index--) {
      const node = this.#slot(index);
      this.#setRef(node, key[(node[4] ?? 0) + (node[5] ?? 0)] ?? 0, ref);
      ref = await this.#append(node);
    }
    this.#root = ref;
    this.#pathKey.set(key);
    leaf.dirty = false;
  }
  async #leaf(kind: number, owner: number, block: number): Promise<Leaf> {
    this.#check();
    let leaf = this.#leaves.find(
      (entry) => entry.kind === kind && entry.owner === owner && entry.block === block,
    );
    if (!leaf) {
      REGISTRY_DIRECTORY_WORK.leafMisses++;
      leaf = this.#leaves.reduce((a, b) => (a.age <= b.age ? a : b));
      await this.#flush(leaf);
      this.#keyOf(kind, owner, block, this.#key);
      const end = await this.#walk(this.#key);
      const ref = end.kind === "found" ? end.ref : null;
      if (ref) {
        if (!this.#file || ref.length !== LEAF_BYTES)
          throw new SpillCorruptionError("Invalid registry leaf size");
        REGISTRY_DIRECTORY_WORK.pageReads++;
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
