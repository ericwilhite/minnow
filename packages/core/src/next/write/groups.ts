/**
 * A table's resident group directory (lead ruling on review B3-p2a-3, findings 1, 2 and 6): the
 * durable `GroupRef`s in row-id order, held as fixed-size immutable **chunks** of up to
 * `CHUNK_GROUPS` groups, each a struct of typed arrays in one `ArrayBuffer`.
 *
 * - **Exact charge.** A chunk's charge is its buffer's `byteLength` plus `CHUNK_OBJECT_BYTES` for
 *   its three views and the object itself, taken from the budget before the buffer is allocated.
 *   No decoded `GroupRef` is retained: `get` decodes one on demand, and hot loops read the columns.
 * - **Sharing.** A directory is immutable. `with` makes the next generation's directory: it shares
 *   every unchanged chunk and copies only the chunks it changes (normally the tail), so an append
 *   copies at most one chunk plus the chunk list (`count / CHUNK_GROUPS` pointers).
 * - **Ownership.** Chunks, and each directory object with its chunk list (`directoryObjectBytes`),
 *   are refcounted by the generations (write states) that reference them. A new directory holds
 *   one creation reference on itself and on every chunk it lists (new or shared), owned by its
 *   maker (the open, or a pending publication), which drops it (`releaseCreation`) once the
 *   generation that installs the directory has retained it, or at once when the publication fails
 *   or its table is gone. The last release returns the charge.
 */
import type { BlobRef, GroupRef } from "../format/manifest.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import type { ByteBudget } from "../storage/extents.js";
import { need } from "./bytes.js";

/** Groups per chunk. */
export const CHUNK_GROUPS = 1024;

const F64 = 9; // groupId, firstRowId, lastRowId, root extent offset/length, root offset, deletion extent offset/length, deletion offset
const U32 = 6; // rowCount, liveRowCount, root length/checksum, deletion length/checksum

/**
 * Bytes charged per chunk beyond its buffer: the chunk object, its `ArrayBuffer` header and three
 * typed-array views. V8 (Node 26, `--expose-gc`, 20,000 retained chunks) measures 489 B for a
 * one-group chunk and 292 B for a full one; the charge rounds up.
 */
export const CHUNK_OBJECT_BYTES = 640;

/** A directory object and its chunk list: a fixed part plus one pointer per chunk. */
export const directoryObjectBytes = (chunks: number): number => 256 + 8 * chunks;

/** Bytes of a chunk's buffer for `count` groups (f64 columns, u32 columns, a flag byte each). */
export function chunkBufferBytes(count: number): number {
  return count * (F64 * 8 + U32 * 4) + Math.ceil(count / 8) * 8;
}

/** The charge of a chunk of `count` groups. */
export function chunkCharge(count: number): number {
  return chunkBufferBytes(count) + CHUNK_OBJECT_BYTES;
}

/** The charge of a directory of `groups` groups built at once (full chunks and a tail). */
export function directoryCharge(groups: number): number {
  const full = Math.floor(groups / CHUNK_GROUPS);
  const tail = groups % CHUNK_GROUPS;
  return full * chunkCharge(CHUNK_GROUPS) + (tail > 0 ? chunkCharge(tail) : 0);
}

export class GroupChunk {
  readonly f64: Float64Array;
  readonly u32: Uint32Array;
  /** Bit 0: the group has a deletion bitmap. */
  readonly flags: Uint8Array;
  readonly charge: number;
  #refs = 1;
  #released = false;

  /** Charges the budget first (`ParseBudgetError` on refusal), then allocates. */
  constructor(
    readonly count: number,
    readonly budget: ByteBudget,
  ) {
    this.charge = chunkCharge(count);
    if (!budget.allocate(this.charge)) throw new ParseBudgetError(this.charge);
    const buffer = new ArrayBuffer(chunkBufferBytes(count));
    this.f64 = new Float64Array(buffer, 0, count * F64);
    this.u32 = new Uint32Array(buffer, count * F64 * 8, count * U32);
    this.flags = new Uint8Array(buffer, count * (F64 * 8 + U32 * 4), count);
  }

  /** References held (diagnostics and tests). */
  get refs(): number {
    return this.#refs;
  }

  retain(): void {
    if (this.#released) throw new RangeError("A released group chunk was retained");
    this.#refs += 1;
  }

  release(): void {
    if (this.#released) return;
    this.#refs -= 1;
    if (this.#refs > 0) return;
    this.#released = true;
    this.budget.free(this.charge);
  }

  set(k: number, group: GroupRef): void {
    const f = k * F64;
    const u = k * U32;
    const a = this.f64;
    const b = this.u32;
    a[f] = group.groupId;
    a[f + 1] = group.firstRowId;
    a[f + 2] = group.lastRowId;
    a[f + 3] = group.root.extent.offset;
    a[f + 4] = group.root.extent.length;
    a[f + 5] = group.root.offset;
    b[u] = group.rowCount;
    b[u + 1] = group.liveRowCount;
    b[u + 2] = group.root.length;
    b[u + 3] = group.root.checksum >>> 0;
    const deletions = group.deletions;
    if (deletions === null) {
      this.flags[k] = 0;
      a[f + 6] = 0;
      a[f + 7] = 0;
      a[f + 8] = 0;
      b[u + 4] = 0;
      b[u + 5] = 0;
    } else {
      this.flags[k] = 1;
      a[f + 6] = deletions.extent.offset;
      a[f + 7] = deletions.extent.length;
      a[f + 8] = deletions.offset;
      b[u + 4] = deletions.length;
      b[u + 5] = deletions.checksum >>> 0;
    }
  }

  /** Copies group `from` of `source` to slot `to` (no object is made). */
  copy(to: number, source: GroupChunk, from: number): void {
    this.f64.set(source.f64.subarray(from * F64, from * F64 + F64), to * F64);
    this.u32.set(source.u32.subarray(from * U32, from * U32 + U32), to * U32);
    this.flags[to] = source.flags[from] ?? 0;
  }

  groupId(k: number): number {
    return this.f64[k * F64] ?? 0;
  }

  firstRowId(k: number): number {
    return this.f64[k * F64 + 1] ?? 0;
  }

  lastRowId(k: number): number {
    return this.f64[k * F64 + 2] ?? 0;
  }

  rowCount(k: number): number {
    return this.u32[k * U32] ?? 0;
  }

  liveRowCount(k: number): number {
    return this.u32[k * U32 + 1] ?? 0;
  }

  root(k: number): BlobRef {
    const f = k * F64;
    const u = k * U32;
    return {
      extent: { offset: this.f64[f + 3] ?? 0, length: this.f64[f + 4] ?? 0 },
      offset: this.f64[f + 5] ?? 0,
      length: this.u32[u + 2] ?? 0,
      checksum: this.u32[u + 3] ?? 0,
    };
  }

  deletions(k: number): BlobRef | null {
    if (((this.flags[k] ?? 0) & 1) === 0) return null;
    const f = k * F64;
    const u = k * U32;
    return {
      extent: { offset: this.f64[f + 6] ?? 0, length: this.f64[f + 7] ?? 0 },
      offset: this.f64[f + 8] ?? 0,
      length: this.u32[u + 4] ?? 0,
      checksum: this.u32[u + 5] ?? 0,
    };
  }

  get(k: number): GroupRef {
    return {
      groupId: this.groupId(k),
      firstRowId: this.firstRowId(k),
      lastRowId: this.lastRowId(k),
      rowCount: this.rowCount(k),
      liveRowCount: this.liveRowCount(k),
      root: this.root(k),
      deletions: this.deletions(k),
    };
  }
}

const NO_CHUNKS: readonly GroupChunk[] = [];

/** An immutable directory over shared chunks (see the module comment). */
export class GroupDirectory {
  static readonly EMPTY = new GroupDirectory(NO_CHUNKS, 0, null, NO_CHUNKS);

  readonly count: number;
  /** This object's own charge (0 for `EMPTY`), apart from its chunks'. */
  readonly charge: number;
  readonly #budget: ByteBudget | null;
  #refs = 1;
  /** Chunks the maker holds a creation reference on (every chunk listed), until released. */
  #created: readonly GroupChunk[] | null;

  /** Charges its own object first (`ParseBudgetError` on refusal) when `budget` is given. */
  private constructor(
    readonly chunks: readonly GroupChunk[],
    /** Σ liveRowCount over the groups. */
    readonly liveRowCount: number,
    budget: ByteBudget | null,
    created: readonly GroupChunk[],
  ) {
    let count = 0;
    for (const chunk of chunks) count += chunk.count;
    this.count = count;
    this.#budget = budget;
    this.#created = created;
    this.charge = budget === null ? 0 : directoryObjectBytes(chunks.length);
    if (budget !== null && !budget.allocate(this.charge)) throw new ParseBudgetError(this.charge);
  }

  /**
   * Builds a directory of exactly `groups.length` groups in fresh chunks: every chunk is charged
   * before it is allocated. The caller owns the creation references (`releaseCreation`).
   */
  static build(groups: readonly GroupRef[], budget: ByteBudget): GroupDirectory {
    const builder = new DirectoryBuilder(budget).expect(groups.length);
    try {
      for (const group of groups) builder.add(group);
      return builder.finish();
    } catch (error) {
      builder.abandon();
      throw error;
    }
  }

  /** A builder that fills chunks as groups arrive (open: one directory page at a time). */
  static builder(budget: ByteBudget): DirectoryBuilder {
    return new DirectoryBuilder(budget);
  }

  /** @internal The builder's constructor (its chunks are all created). */
  static made(
    chunks: readonly GroupChunk[],
    liveRowCount: number,
    budget: ByteBudget,
    created: readonly GroupChunk[],
  ): GroupDirectory {
    return new GroupDirectory(chunks, liveRowCount, budget, created);
  }

  /** Total charge a fresh build of `groups` groups takes (its chunks and its object). */
  static buildCharge(groups: number): number {
    return directoryCharge(groups) + directoryObjectBytes(Math.ceil(groups / CHUNK_GROUPS));
  }

  #locate(index: number): { chunk: GroupChunk; k: number } {
    // Chunks before the tail are full, so the chunk is index / CHUNK_GROUPS, except after a
    // chunk that `with` left short (never: only the tail is short), checked below.
    const c = Math.floor(index / CHUNK_GROUPS);
    const chunk = this.chunks[c];
    const k = index - c * CHUNK_GROUPS;
    if (chunk === undefined || k >= chunk.count || index < 0)
      throw new RangeError(`Group ${String(index)} is outside the directory`);
    return { chunk, k };
  }

  get(index: number): GroupRef {
    const { chunk, k } = this.#locate(index);
    return chunk.get(k);
  }

  groupId(index: number): number {
    const { chunk, k } = this.#locate(index);
    return chunk.groupId(k);
  }

  firstRowId(index: number): number {
    const { chunk, k } = this.#locate(index);
    return chunk.firstRowId(k);
  }

  rowCount(index: number): number {
    const { chunk, k } = this.#locate(index);
    return chunk.rowCount(k);
  }

  liveRows(index: number): number {
    const { chunk, k } = this.#locate(index);
    return chunk.liveRowCount(k);
  }

  root(index: number): BlobRef {
    const { chunk, k } = this.#locate(index);
    return chunk.root(k);
  }

  deletions(index: number): BlobRef | null {
    const { chunk, k } = this.#locate(index);
    return chunk.deletions(k);
  }

  /** Decoded groups `[from, to)` (transient objects: nothing is retained). */
  slice(from: number, to = this.count): GroupRef[] {
    const out: GroupRef[] = [];
    for (let index = from; index < to; index++) out.push(this.get(index));
    return out;
  }

  /** The last group's id + 1, or 1 when empty. */
  get nextGroupId(): number {
    return this.count === 0 ? 1 : this.groupId(this.count - 1) + 1;
  }

  /** The index of group `groupId` (ids ascend), or −1. */
  indexOfGroupId(groupId: number): number {
    return this.#search(
      (index) => this.groupId(index) < groupId,
      (index) => this.groupId(index) === groupId,
    );
  }

  /** The index of the group holding `rowId`, or −1. */
  indexOfRow(rowId: number): number {
    // Last group whose first row id ≤ rowId.
    let lo = 0;
    let hi = this.count;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.firstRowId(mid) <= rowId) lo = mid + 1;
      else hi = mid;
    }
    const index = lo - 1;
    if (index < 0) return -1;
    const { chunk, k } = this.#locate(index);
    return rowId <= chunk.lastRowId(k) ? index : -1;
  }

  #search(below: (index: number) => boolean, match: (index: number) => boolean): number {
    let lo = 0;
    let hi = this.count;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (below(mid)) lo = mid + 1;
      else hi = mid;
    }
    return lo < this.count && match(lo) ? lo : -1;
  }

  /** Takes one reference on itself and every chunk (a generation that references it). */
  retain(): void {
    if (this.#budget === null) return;
    if (this.#refs <= 0) throw new RangeError("A released group directory was retained");
    this.#refs += 1;
    for (const chunk of this.chunks) chunk.retain();
  }

  release(): void {
    if (this.#budget === null || this.#refs <= 0) return;
    for (const chunk of this.chunks) chunk.release();
    this.#refs -= 1;
    if (this.#refs === 0) this.#budget.free(this.charge);
  }

  /** Drops the maker's creation references (its own and the chunks it made). Idempotent. */
  releaseCreation(): void {
    const created = this.#created;
    if (created === null) return;
    this.#created = null;
    for (const chunk of created) chunk.release();
    if (this.#budget === null) return;
    this.#refs -= 1;
    if (this.#refs === 0) this.#budget.free(this.charge);
  }

  /** Bytes charged for this directory: its object and its chunks (shared chunks counted here too). */
  get chargedBytes(): number {
    let total = this.charge;
    for (const chunk of this.chunks) total += chunk.charge;
    return total;
  }

  /**
   * The next generation: groups at the indexes of `changes` replaced, then `appended` added at the
   * end. Unchanged chunks are shared; each changed chunk is copied once and the tail chunk is
   * copied (or a new one made) for appends. The caller owns the creation references of the new
   * directory and the chunks it made (`releaseCreation`, see "Ownership"). Charges before
   * allocating; on refusal nothing stays charged.
   */
  with(
    changes: ReadonlyMap<number, GroupRef>,
    appended: readonly GroupRef[],
    budget: ByteBudget,
  ): GroupDirectory {
    const chunks = this.chunks.slice();
    const created: GroupChunk[] = [];
    let live = this.liveRowCount;
    try {
      const touched = new Map<number, GroupRef[]>();
      for (const [index, group] of changes) {
        const { k } = this.#locate(index);
        const c = Math.floor(index / CHUNK_GROUPS);
        let list = touched.get(c);
        if (list === undefined) touched.set(c, (list = []));
        list[k] = group;
        live += group.liveRowCount - this.liveRows(index);
      }
      for (const [c, list] of touched) {
        const source = need(chunks[c]);
        const copy = new GroupChunk(source.count, budget);
        created.push(copy);
        for (let k = 0; k < source.count; k++) {
          const group = list[k];
          if (group === undefined) copy.copy(k, source, k);
          else copy.set(k, group);
        }
        chunks[c] = copy;
      }
      let rest = 0;
      while (rest < appended.length) {
        const last = chunks[chunks.length - 1];
        const room = last === undefined ? 0 : CHUNK_GROUPS - last.count;
        if (last !== undefined && room > 0) {
          const take = Math.min(room, appended.length - rest);
          const grown = new GroupChunk(last.count + take, budget);
          created.push(grown);
          for (let k = 0; k < last.count; k++) grown.copy(k, last, k);
          for (let k = 0; k < take; k++) grown.set(last.count + k, need(appended[rest + k]));
          // A chunk this call made and now replaces is dropped at once.
          const made = created.indexOf(last);
          if (made >= 0) {
            created.splice(made, 1);
            last.release();
          }
          chunks[chunks.length - 1] = grown;
          rest += take;
          continue;
        }
        const take = Math.min(CHUNK_GROUPS, appended.length - rest);
        const chunk = new GroupChunk(take, budget);
        created.push(chunk);
        for (let k = 0; k < take; k++) chunk.set(k, need(appended[rest + k]));
        chunks.push(chunk);
        rest += take;
      }
      for (const group of appended) live += group.liveRowCount;
      // The new directory's creation reference covers every chunk it lists: the shared ones too
      // (review B3-p2a-4 #3), so a pending publication keeps them charged while it awaits even
      // if every installed generation lets them go meanwhile.
      const shared = chunks.filter((chunk) => !created.includes(chunk));
      for (const chunk of shared) chunk.retain();
      try {
        return new GroupDirectory(chunks, live, budget, chunks.slice());
      } catch (error) {
        for (const chunk of shared) chunk.release();
        throw error;
      }
    } catch (error) {
      for (const chunk of created) chunk.release();
      throw error;
    }
  }
}

/** Fills full chunks in order; `finish` gives the directory (its chunks hold creation refs). */
export class DirectoryBuilder {
  readonly #chunks: GroupChunk[] = [];
  #current: GroupChunk | null = null;
  #filled = 0;
  #live = 0;
  #remaining: number;

  constructor(readonly budget: ByteBudget) {
    this.#remaining = 0;
  }

  /** Declares how many groups will be added in total (sizes the tail chunk exactly). */
  expect(total: number): this {
    this.#remaining = total;
    return this;
  }

  add(group: GroupRef): void {
    if (this.#current === null || this.#filled === this.#current.count) {
      if (this.#remaining <= 0) throw new RangeError("More groups than the directory declared");
      const count = Math.min(CHUNK_GROUPS, this.#remaining);
      this.#current = new GroupChunk(count, this.budget);
      this.#chunks.push(this.#current);
      this.#filled = 0;
      this.#remaining -= count;
    }
    this.#current.set(this.#filled++, group);
    this.#live += group.liveRowCount;
  }

  /** The directory; it and every chunk hold creation references (`releaseCreation`). */
  finish(): GroupDirectory {
    if (this.#remaining > 0 || (this.#current !== null && this.#filled !== this.#current.count))
      throw new RangeError("Fewer groups than the directory declared");
    return GroupDirectory.made(this.#chunks, this.#live, this.budget, this.#chunks.slice());
  }

  /** Releases every chunk made so far (a failed open). */
  abandon(): void {
    for (const chunk of this.#chunks) chunk.release();
    this.#chunks.length = 0;
  }
}
