/**
 * The window's grouped sort (track B7): what a window needs from a sort — rows grouped by
 * partition, ordered within it — without the general sort's key bytes, 24-byte tuples or
 * comparison passes over the whole input.
 *
 * - **Arrival.** Rows go into a column store (the window's one copy of them) with a partition
 *   id per row and, per ORDER BY key, its value normalized to two order-preserving 32-bit words
 *   (direction applied) and, once a NULL is seen, a header byte placing NULLs first or last. A
 *   dictionary partition column translates each dictionary entry once (per dictionary).
 * - **Sort.** Partition ids are ranked by their values (ascending, NULL last: the sort operator's
 *   order for the window's partition keys), rows are counted into partitions (a counting sort,
 *   no comparisons), and each partition is ordered by an LSD radix over the first key's words,
 *   skipping every byte the partition's rows share. Runs equal on the first key are refined by
 *   the other keys. Small partitions are insertion-sorted.
 * - **Peers** are adjacent rows whose normalized keys are all equal (normalization is exact:
 *   −0 = 0 and NaN = NaN, as the sort's keys and PostgreSQL's peers).
 *
 * Applies (`groupedSortable`) to at most one PARTITION BY column of type text or bytes (byte
 * order), the int32 family (int32, date, bool) or the int64 family (int64, time, timestamp), and
 * ORDER BY keys that are all fixed-width numbers or dates (bool, int32, date, int64, time,
 * timestamp, float32, float64). Everything is charged; when the grant refuses (`add` or
 * `sortJob` false/null) the window moves the rows to the sort operator instead (window.ts).
 */
import type { PhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import { newDictionaryId, type Batch, type FlatVector, type Vector } from "../vector/vector.js";
import { ColumnStore, wordsOf } from "./column-store.js";
import { SLICE_ROWS, type Slice, type SlicedJob } from "./slices.js";
import { compareRanges, isByteCollation, mergeSortJob, type SortColumn } from "./sort-keys.js";
import { ByteSet } from "./sort.js";

type PartKind = "bytes" | "i32" | "int64";

function partKind(type: PhysicalType): PartKind | null {
  switch (type.kind) {
    case "text":
    case "bytes":
      return "bytes";
    case "int32":
    case "date":
    case "bool":
      return "i32";
    case "int64":
    case "time":
    case "timestamp":
      return "int64";
    default:
      return null;
  }
}

function orderable(type: PhysicalType): boolean {
  switch (type.kind) {
    case "bool":
    case "int32":
    case "date":
    case "int64":
    case "time":
    case "timestamp":
    case "float32":
    case "float64":
      return true;
    default:
      return false;
  }
}

/** Whether a window over these keys can use the grouped sort (see the module comment). */
export function groupedSortable(
  types: readonly PhysicalType[],
  partitionBy: readonly number[],
  orderBy: readonly SortColumn[],
): boolean {
  if (partitionBy.length > 1) return false;
  const part = partitionBy[0];
  if (part !== undefined) {
    const type = types[part];
    if (type === undefined || partKind(type) === null) return false;
  }
  return orderBy.every((key) => {
    const type = types[key.column];
    return type !== undefined && orderable(type) && isByteCollation(key.collation);
  });
}

// Module scratch, sync-only (R23.1): written and read inside one synchronous normalization.
const f64 = new Float64Array(1);
const f64Words = new Uint32Array(f64.buffer);

/**
 * Sorted rows: `ids` in order, partition p is `ids[starts[p] … starts[p + 1])`; `breaks[i]` is 1
 * where sorted row i starts a peer group (the first row of a partition, or a key change).
 */
export interface GroupedOrder {
  readonly ids: Int32Array;
  readonly starts: Int32Array;
  readonly breaks: Uint8Array;
  /** The partition id of each partition, in order (`partitionValue`). */
  readonly groupOf: Int32Array;
}

/** One ORDER BY key's normalized words per row, and its NULL headers once it has a NULL. */
interface KeyWords {
  hi: Uint32Array;
  lo: Uint32Array;
  /** 0 (NULLS FIRST) or 2 (NULLS LAST) for a NULL, 1 for a value; null while none is NULL. */
  header: Uint8Array | null;
  readonly key: SortColumn;
  readonly type: PhysicalType;
}

/** Rows held for a window, with partition ids and normalized order keys (module comment). */
export class GroupedRows {
  readonly store: ColumnStore;
  rows = 0;
  #capacity = 0;
  #gids = new Int32Array(0);
  readonly #keys: KeyWords[];
  /** Bytes charged for the per-row arrays (gids, words, headers). */
  #arrayBytes = 0;
  readonly #part: number | null;
  readonly #partKind: PartKind | null;
  readonly #bytes: ByteSet | null;
  readonly #numbers: Map<number | bigint, number> | null;
  /** Number-kind partition values by id (ranked at the sort). */
  readonly #values: Array<number | bigint> = [];
  #groups = 0;
  #nullGroup = -1;
  /** The last dictionary's translation, code → partition id (−1 untranslated). */
  #dictionaryId = -1;
  #translation = new Int32Array(0);
  #translationBytes = 0;
  #sortBytes = 0;
  #closed = false;
  /** Byte-set id → partition id (the null group may sit between them). */
  readonly #byteGroup: number[] = [];
  /** Bytes charged for the number-kind partition map. */
  #mapBytes = 0;
  #fallbackOffsets: Int32Array | null = null;
  #fallbackSetOf = new Int32Array(0);

  /**
   * A text or bytes partition column nothing else reads is not stored (its rows hold a
   * placeholder bool): its values are the byte set's, and `batchColumns` rebuilds it.
   */
  readonly #skipped: boolean;
  readonly #placeholder: FlatVector = {
    form: "flat",
    type: { kind: "bool" },
    length: 1,
    validity: null,
    data: { layout: "bool", values: new Uint8Array(1) },
  };

  constructor(
    readonly types: readonly PhysicalType[],
    partitionBy: readonly number[],
    orderBy: readonly SortColumn[],
    readonly charge: ByteCharge,
    /** Whether something other than the partitioning reads the partition column. */
    partitionRead = true,
  ) {
    this.#part = partitionBy[0] ?? null;
    const partType = this.#part === null ? undefined : types[this.#part];
    this.#partKind = partType === undefined ? null : partKind(partType);
    this.#skipped = this.#partKind === "bytes" && !partitionRead;
    // Chunks of 2,048 rows (no byte target: a target stops appends at the first full chunk).
    this.store = new ColumnStore(
      this.#skipped ? types.map((type, c) => (c === this.#part ? { kind: "bool" } : type)) : types,
      charge,
    );
    this.#bytes = this.#partKind === "bytes" ? new ByteSet(charge) : null;
    this.#numbers = this.#partKind === "i32" || this.#partKind === "int64" ? new Map() : null;
    if (this.#part === null) this.#groups = 1;
    this.#keys = orderBy.map((key) => ({
      hi: new Uint32Array(0),
      lo: new Uint32Array(0),
      header: null,
      key,
      type: types[key.column] ?? { kind: "int32", bits: 32 },
    }));
  }

  get groups(): number {
    return this.#groups;
  }

  /** Bytes per row of the per-row arrays. */
  #rowBytes(): number {
    let bytes = 4;
    for (const key of this.#keys) bytes += 8 + (key.header === null ? 0 : 1);
    return bytes;
  }

  /** Grows the per-row arrays to `rows` (charged); false when refused. */
  #reserve(rows: number): boolean {
    if (rows <= this.#capacity) return true;
    const capacity = Math.max(rows, 2 * this.#capacity, 4096);
    const bytes = capacity * this.#rowBytes();
    if (!this.charge.allocate(bytes)) return false;
    this.charge.free(this.#arrayBytes);
    this.#arrayBytes = bytes;
    const grow = <T extends Int32Array | Uint32Array | Uint8Array>(
      old: T,
      make: (n: number) => T,
    ): T => {
      const next = make(capacity);
      next.set(old.subarray(0, this.rows));
      return next;
    };
    this.#gids = grow(this.#gids, (n) => new Int32Array(n));
    for (const key of this.#keys) {
      key.hi = grow(key.hi, (n) => new Uint32Array(n));
      key.lo = grow(key.lo, (n) => new Uint32Array(n));
      if (key.header !== null) key.header = grow(key.header, (n) => new Uint8Array(n).fill(1));
    }
    this.#capacity = capacity;
    return true;
  }

  /** Gives key k its header array (the first NULL), charged; false when refused. */
  #headers(key: KeyWords): boolean {
    if (!this.charge.allocate(this.#capacity)) return false;
    this.#arrayBytes += this.#capacity;
    key.header = new Uint8Array(this.#capacity).fill(1);
    return true;
  }

  /** Takes a batch's active rows; false (nothing taken) when the grant refuses. */
  add(batch: Batch): boolean {
    const count = batch.selection === null ? batch.length : batch.selection.count;
    if (count === 0) return true;
    const ids = batch.selection?.rows ?? null;
    const base = this.rows;
    if (!this.#reserve(base + count)) return false;
    if (!this.#partitionIds(batch.columns, ids, count, base)) return false;
    for (const key of this.#keys)
      if (!this.#normalize(key, columnOf(batch.columns, key.key.column), ids, count, base))
        return false;
    const columns = this.#skipped
      ? batch.columns.map((vector, c): Vector =>
          c === this.#part
            ? {
                form: "constant",
                type: { kind: "bool" },
                length: batch.length,
                value: this.#placeholder,
              }
            : vector,
        )
      : batch.columns;
    const taken = this.store.append(columns, ids, 0, count);
    if (taken < count) {
      // Partition ids handed out for this batch stay (an id without rows ranks like any other).
      this.store.truncate(base);
      return false;
    }
    this.rows = base + count;
    return true;
  }

  /** Partition ids of the batch's rows into `gids[base …]`; false when refused. */
  #partitionIds(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    count: number,
    base: number,
  ): boolean {
    const gids = this.#gids;
    if (this.#part === null) {
      gids.fill(0, base, base + count);
      return true;
    }
    const vector = columnOf(columns, this.#part);
    if (vector.form === "constant") {
      const id = this.#idOf(vector.value, 0);
      if (id < 0) return false;
      gids.fill(id, base, base + count);
      return true;
    }
    if (vector.form === "dictionary") {
      if (vector.dictionaryId !== this.#dictionaryId) {
        const size = vector.dictionary.length;
        if (size > this.#translation.length) {
          const bytes = 4 * Math.max(size, 64);
          if (!this.charge.allocate(bytes)) return false;
          this.charge.free(this.#translationBytes);
          this.#translationBytes = bytes;
          this.#translation = new Int32Array(bytes / 4);
        }
        this.#translation.fill(-1, 0, size);
        this.#dictionaryId = vector.dictionaryId;
      }
      const translation = this.#translation;
      const bits = vector.validity;
      for (let k = 0; k < count; k++) {
        const row = ids === null ? k : (ids[k] ?? 0);
        if (bits !== null && ((bits[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) {
          const id = this.#nullId();
          gids[base + k] = id;
          continue;
        }
        const code = vector.codes[row] ?? 0;
        let id = translation[code] ?? -1;
        if (id < 0) {
          id = this.#idOf(vector.dictionary, code);
          if (id < 0) {
            // The translation stays valid for the codes done; the batch is refused whole.
            translation.fill(-1);
            this.#dictionaryId = -1;
            return false;
          }
          translation[code] = id;
        }
        gids[base + k] = id;
      }
      return true;
    }
    for (let k = 0; k < count; k++) {
      const id = this.#idOf(vector, ids === null ? k : (ids[k] ?? 0));
      if (id < 0) return false;
      gids[base + k] = id;
    }
    return true;
  }

  #nullId(): number {
    if (this.#nullGroup < 0) {
      this.#nullGroup = this.#groups++;
      this.#values.push(0);
    }
    return this.#nullGroup;
  }

  /** The partition id of slot `slot` of a flat vector; −1 when refused. */
  #idOf(flat: FlatVector, slot: number): number {
    const bits = flat.validity;
    if (bits !== null && ((bits[slot >>> 5] ?? 0) & (1 << (slot & 31))) === 0)
      return this.#nullId();
    const data = flat.data;
    if (this.#bytes !== null && data.layout === "varlen") {
      const before = this.#bytes.size;
      const set = this.#bytes.add(data.bytes, data.offsets[slot] ?? 0, data.offsets[slot + 1] ?? 0);
      if (set < 0) return -1;
      if (this.#bytes.size > before) {
        this.#values[this.#groups] = set;
        return this.#newGroup(set);
      }
      return this.#byteGroup[set] ?? -1;
    }
    const numbers = this.#numbers;
    if (numbers === null) throw new TypeError("No partition key kind");
    let value: number | bigint;
    if (data.layout === "int64") {
      const values = data.values;
      if (values.repr === "safe") value = values.values[slot] ?? 0;
      else {
        const big = values.values[slot] ?? 0n;
        const n = Number(big);
        value = Number.isSafeInteger(n) ? n : big;
      }
    } else value = (data as { values: Int32Array | Uint8Array }).values[slot] ?? 0;
    const known = numbers.get(value);
    if (known !== undefined) return known;
    if (!this.charge.allocate(64)) return -1;
    this.#mapBytes += 64;
    const id = this.#groups++;
    numbers.set(value, id);
    this.#values.push(value);
    return id;
  }

  #newGroup(set: number): number {
    const id = this.#groups++;
    this.#byteGroup[set] = id;
    return id;
  }

  /** Normalized words (and headers) of key `key` for the batch's rows; false when refused. */
  #normalize(
    key: KeyWords,
    vector: Vector,
    ids: Uint16Array | null,
    count: number,
    base: number,
  ): boolean {
    const desc = key.key.direction === "desc";
    const nullHeader = key.key.nulls === "first" ? 0 : 2;
    const flat =
      vector.form === "flat"
        ? vector
        : vector.form === "constant"
          ? vector.value
          : vector.dictionary;
    const bits = flat.validity;
    const data = flat.data;
    const hi = key.hi;
    const lo = key.lo;
    for (let k = 0; k < count; k++) {
      const row = ids === null ? k : (ids[k] ?? 0);
      const slot =
        vector.form === "flat" ? row : vector.form === "constant" ? 0 : (vector.codes[row] ?? 0);
      const valid =
        (vector.form !== "dictionary" ||
          vector.validity === null ||
          ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0) &&
        (bits === null || ((bits[slot >>> 5] ?? 0) & (1 << (slot & 31))) !== 0);
      const at = base + k;
      if (!valid) {
        const headers = key.header ?? (this.#headers(key) ? key.header : null);
        if (headers === null) return false;
        headers[at] = nullHeader;
        hi[at] = 0;
        lo[at] = 0;
        continue;
      }
      if (key.header !== null) key.header[at] = 1;
      let h = 0;
      let l: number;
      switch (data.layout) {
        case "i32":
          l = ((data.values[slot] ?? 0) ^ 0x8000_0000) >>> 0;
          break;
        case "bool":
          l = data.values[slot] ?? 0;
          break;
        case "int64": {
          const values = data.values;
          if (values.repr === "safe") {
            const value = values.values[slot] ?? 0;
            const high = Math.floor(value / 4_294_967_296);
            h = ((high >>> 0) ^ 0x8000_0000) >>> 0;
            l = (value - high * 4_294_967_296) >>> 0;
          } else {
            const words = wordsOf(values.values);
            h = ((words[slot * 2 + 1] ?? 0) ^ 0x8000_0000) >>> 0;
            l = (words[slot * 2] ?? 0) >>> 0;
          }
          break;
        }
        case "f64":
        case "f32": {
          const value = data.values[slot] ?? 0;
          f64[0] = value !== value ? Number.NaN : value === 0 ? 0 : value;
          h = f64Words[1] ?? 0;
          l = f64Words[0] ?? 0;
          if (h >= 0x8000_0000) {
            h = ~h >>> 0;
            l = ~l >>> 0;
          } else h = (h | 0x8000_0000) >>> 0;
          break;
        }
        default:
          throw new TypeError(`No grouped sort key for ${data.layout}`);
      }
      if (desc) {
        h = ~h >>> 0;
        l = ~l >>> 0;
      }
      hi[at] = h;
      lo[at] = l;
    }
    return true;
  }

  /**
   * Partition `gid`'s value as a one-row vector of the partition column's type (a view: copy it
   * before the next `add`). Null for a window without PARTITION BY.
   */
  partitionValue(gid: number): FlatVector | null {
    const part = this.#part;
    if (part === null) return null;
    const type = this.types[part] ?? { kind: "int32", bits: 32 };
    const validity = gid === this.#nullGroup ? new Uint32Array(1) : null;
    const set = this.#bytes;
    if (set !== null) {
      const id = validity === null ? (this.#values[gid] as number) : -1;
      const start = id < 0 ? 0 : (set.starts[id] ?? 0);
      const end = id < 0 ? 0 : (set.ends[id] ?? 0);
      return {
        form: "flat",
        type,
        length: 1,
        validity,
        data: { layout: "varlen", bytes: set.arena, offsets: Int32Array.of(start, end) },
      };
    }
    const value = this.#values[gid] ?? 0;
    if (type.kind === "bool")
      return {
        form: "flat",
        type,
        length: 1,
        validity,
        data: { layout: "bool", values: Uint8Array.of(Number(value)) },
      };
    if (this.#partKind === "i32")
      return {
        form: "flat",
        type,
        length: 1,
        validity,
        data: { layout: "i32", values: Int32Array.of(Number(value)) },
      };
    return {
      form: "flat",
      type,
      length: 1,
      validity,
      data: {
        layout: "int64",
        values:
          typeof value === "bigint"
            ? { repr: "int64", values: BigInt64Array.of(value) }
            : { repr: "safe", values: Float64Array.of(value) },
      },
    };
  }

  /**
   * Chunk k's rows as a batch of the input's columns (the fallback to the sort operator): a
   * skipped partition column comes back as a dictionary vector over the byte set.
   */
  batchColumns(k: number): Vector[] {
    const store = this.store;
    const columns: Vector[] = this.types.map((_, c) => store.chunk(c, k));
    const part = this.#part;
    const set = this.#bytes;
    if (!this.#skipped || part === null || set === null) return columns;
    const length = columns[0]?.length ?? 0;
    if (this.#fallbackOffsets === null) {
      // Once (rows no longer arrive): the set's entries as a dictionary, and gid → entry.
      const offsets = new Int32Array(set.size + 1);
      for (let id = 0; id < set.size; id++) offsets[id + 1] = set.ends[id] ?? 0;
      const setOf = new Int32Array(this.#groups).fill(-1);
      this.#byteGroup.forEach((gid, id) => {
        setOf[gid] = id;
      });
      this.#fallbackOffsets = offsets;
      this.#fallbackSetOf = setOf;
    }
    const offsets = this.#fallbackOffsets;
    const setOf = this.#fallbackSetOf;
    const codes = new Int32Array(length);
    let validity: Uint32Array | null = null;
    const base = k * store.chunkRows;
    for (let i = 0; i < length; i++) {
      const id = setOf[this.#gids[base + i] ?? 0] ?? -1;
      if (id < 0) {
        validity ??= new Uint32Array(Math.ceil(length / 32)).fill(0xffff_ffff);
        validity[i >>> 5] = (validity[i >>> 5] ?? 0) & ~(1 << (i & 31));
      } else codes[i] = id;
    }
    columns[part] = {
      form: "dictionary",
      type: this.types[part] ?? { kind: "text" },
      length,
      codes,
      validity,
      dictionary: {
        form: "flat",
        type: this.types[part] ?? { kind: "text" },
        length: set.size,
        validity: null,
        data: { layout: "varlen", bytes: set.arena, offsets },
      },
      dictionaryId: newDictionaryId(),
    };
    return columns;
  }

  /** Order of rows `a`, `b` by keys `from …` (−1, 0, 1). */
  #compare(a: number, b: number, from: number): number {
    const keys = this.#keys;
    for (let k = from; k < keys.length; k++) {
      const key = keys[k];
      if (key === undefined) continue;
      if (key.header !== null) {
        const x = key.header[a] ?? 1;
        const y = key.header[b] ?? 1;
        if (x !== y) return x < y ? -1 : 1;
      }
      const ha = key.hi[a] ?? 0;
      const hb = key.hi[b] ?? 0;
      if (ha !== hb) return ha < hb ? -1 : 1;
      const la = key.lo[a] ?? 0;
      const lb = key.lo[b] ?? 0;
      if (la !== lb) return la < lb ? -1 : 1;
    }
    return 0;
  }

  /** Bytes the sort needs on top of the rows (ids, partition starts, radix buffers). */
  sortBytes(): number {
    // ids, breaks, starts, the radix buffers (28 bytes a row), counts, the MSD stack, ranks.
    return (
      4 * this.rows +
      this.rows +
      4 * (this.#groups + 1) +
      28 * this.rows +
      2 * (DIGIT_BUCKETS + 1) * 4 +
      4 * stackWords(this.rows) +
      8 * this.#groups
    );
  }

  /** The rows in window order (module comment); null when the grant refuses its arrays. */
  *sortJob(slice: Slice): SlicedJob<GroupedOrder | null> {
    const n = this.rows;
    const groups = this.#groups;
    const bytes = this.sortBytes();
    if (!this.charge.allocate(bytes)) return null;
    this.#sortBytes = bytes;
    // Partition ranks: ascending values, NULL last.
    const order = Array.from({ length: groups }, (_, g) => g);
    const values = this.#values;
    const set = this.#bytes;
    const nullGroup = this.#nullGroup;
    const rankOf = new Int32Array(groups);
    if (groups > 1) {
      order.sort((a, b) => {
        if (a === nullGroup || b === nullGroup) return a === nullGroup ? 1 : -1;
        if (set !== null) {
          const x = set.bytes(values[a] as number);
          const y = set.bytes(values[b] as number);
          return compareRanges(x, 0, x.length, y, 0, y.length);
        }
        const x = values[a] ?? 0;
        const y = values[b] ?? 0;
        return x < y ? -1 : x > y ? 1 : 0;
      });
    }
    order.forEach((g, rank) => {
      rankOf[g] = rank;
    });
    // Partition sizes, so partition r is ids[starts[r] … starts[r + 1]).
    const starts = new Int32Array(groups + 1);
    const gids = this.#gids;
    for (let i = 0; i < n; i++) {
      const r = (rankOf[gids[i] ?? 0] ?? 0) + 1;
      starts[r] = (starts[r] ?? 0) + 1;
      if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
    }
    for (let r = 0; r < groups; r++) starts[r + 1] = (starts[r + 1] ?? 0) + (starts[r] ?? 0);
    const ids = new Int32Array(n);
    const breaks = new Uint8Array(n);
    const radix = new RadixBuffers(n);
    yield* this.#byRank(ids, rankOf, groups, radix, slice);
    for (let r = 0; r < groups; r++) {
      yield* this.#sortRange(ids, breaks, starts[r] ?? 0, starts[r + 1] ?? 0, radix, slice);
      if (slice.due()) yield;
    }
    return { ids, starts, breaks, groupOf: Int32Array.from(order) };
  }

  /**
   * Rows into partition order: an LSD radix on the partition rank in digits of at most 7 bits
   * (one pass up to 128 partitions; each pass writes to at most 128 places per array, so a
   * thousand partitions do not scatter to three thousand), stable, so a partition keeps arrival
   * order. The first key's words travel with the ids, so a partition's sort reads them in place
   * (`radix.loA`, `radix.hiA`) instead of gathering them by row id.
   */
  *#byRank(
    ids: Int32Array,
    rankOf: Int32Array,
    groups: number,
    radix: RadixBuffers,
    slice: Slice,
  ): SlicedJob<void> {
    const n = this.rows;
    const gids = this.#gids;
    const first = this.#keys[0];
    const width = Math.max(1, Math.ceil(Math.log2(Math.max(2, groups))));
    const passes = Math.ceil(width / RANK_BITS);
    const bits = Math.ceil(width / passes);
    const mask = (1 << bits) - 1;
    const counts = radix.counts;
    // Sets: Y = (ids, loA, hiA, rankA), the result; X = (idsB, loB, hiB, rankB).
    const Y = { ids, lo: radix.loA, hi: radix.hiA, rank: radix.rankA };
    const X = { ids: radix.idsB, lo: radix.loB, hi: radix.hiB, rank: radix.rankB };
    let source: typeof Y | null = null;
    for (let pass = 0; pass < passes; pass++) {
      const shift = pass * bits;
      const dest = (passes - 1 - pass) % 2 === 0 ? Y : X;
      counts.fill(0, 0, mask + 2);
      for (let i = 0; i < n; i++) {
        const r = source === null ? (rankOf[gids[i] ?? 0] ?? 0) : (source.rank[i] ?? 0);
        const d = (r >>> shift) & mask;
        counts[d + 1] = (counts[d + 1] ?? 0) + 1;
      }
      for (let d = 0; d <= mask; d++) counts[d + 1] = (counts[d + 1] ?? 0) + (counts[d] ?? 0);
      for (let i = 0; i < n; i++) {
        let id: number;
        let r: number;
        let l = 0;
        let h = 0;
        if (source === null) {
          id = i;
          r = rankOf[gids[i] ?? 0] ?? 0;
          if (first !== undefined) {
            l = first.lo[i] ?? 0;
            h = first.hi[i] ?? 0;
          }
        } else {
          id = source.ids[i] ?? 0;
          r = source.rank[i] ?? 0;
          l = source.lo[i] ?? 0;
          h = source.hi[i] ?? 0;
        }
        const d = (r >>> shift) & mask;
        const at = counts[d] ?? 0;
        counts[d] = at + 1;
        dest.ids[at] = id;
        dest.rank[at] = r;
        dest.lo[at] = l;
        dest.hi[at] = h;
        if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
      }
      source = dest;
    }
  }

  /**
   * Orders `ids[s … e)` (one partition) by every key and marks its peer breaks: the first key
   * through `msdWords` (skipped when the rows already arrive in order), NULLs by a stable pass
   * on the header, then runs equal on the first key ordered by the others.
   */
  *#sortRange(
    ids: Int32Array,
    breaks: Uint8Array,
    s: number,
    e: number,
    radix: RadixBuffers,
    slice: Slice,
  ): SlicedJob<void> {
    const m = e - s;
    if (m <= 0) return;
    breaks[s] = 1;
    const keys = this.#keys;
    const key = keys[0];
    if (key === undefined) return;
    if (m === 1) return;
    const { idsA: ia, loA: la, hiA: ha } = radix;
    const header = key.header;
    // The first key's words are in place (sortJob); whether the rows already arrive in order.
    ia.set(ids.subarray(s, e), s);
    let sorted = true;
    for (let i = s + 1; i < e && sorted; i++) {
      const h = ha[i] ?? 0;
      const p = ha[i - 1] ?? 0;
      if (header !== null) {
        const x = header[ia[i] ?? 0] ?? 1;
        const y = header[ia[i - 1] ?? 0] ?? 1;
        if (x !== y) {
          sorted = x > y;
          continue;
        }
      }
      if (h < p || (h === p && (la[i] ?? 0) < (la[i - 1] ?? 0))) sorted = false;
    }
    if (!sorted) {
      let from = s;
      if (header !== null) {
        // NULLs first or last: a stable pass on the header, then each segment by its words.
        const counts = radix.counts;
        counts.fill(0, 0, 4);
        for (let i = s; i < e; i++) {
          const d = header[ia[i] ?? 0] ?? 1;
          counts[d + 1] = (counts[d + 1] ?? 0) + 1;
        }
        for (let d = 0; d < 3; d++) counts[d + 1] = (counts[d + 1] ?? 0) + (counts[d] ?? 0);
        const segments = [s + (counts[1] ?? 0), s + (counts[2] ?? 0), e];
        for (let i = s; i < e; i++) {
          const d = header[ia[i] ?? 0] ?? 1;
          const at = s + (counts[d] ?? 0);
          counts[d] = (counts[d] ?? 0) + 1;
          radix.idsB[at] = ia[i] ?? 0;
          radix.loB[at] = la[i] ?? 0;
          radix.hiB[at] = ha[i] ?? 0;
        }
        ia.set(radix.idsB.subarray(s, e), s);
        la.set(radix.loB.subarray(s, e), s);
        ha.set(radix.hiB.subarray(s, e), s);
        for (const end of segments) {
          yield* msdWords(radix, from, end, slice);
          from = end;
        }
      } else yield* msdWords(radix, s, e, slice);
    }
    ids.set(ia.subarray(s, e), s);
    // Peer breaks and ties: a run equal on the first key is ordered by the others.
    let start = s;
    for (let i = s + 1; i <= e; i++) {
      if (
        i < e &&
        la[i] === la[i - 1] &&
        ha[i] === ha[i - 1] &&
        (header === null || header[ids[i] ?? 0] === header[ids[i - 1] ?? 0])
      ) {
        breaks[i] = 0;
        continue;
      }
      if (i < e) breaks[i] = 1;
      if (keys.length > 1 && i - start > 1) {
        if (i - start > SMALL_RANGE)
          yield* mergeSortJob(
            ids.subarray(start, i),
            radix.idsB.subarray(start, i),
            (a, b) => this.#compare(a, b, 1),
            slice,
          );
        else insertionSort(ids, start, i, (a, b) => this.#compare(a, b, 1));
        for (let j = start + 1; j < i; j++)
          breaks[j] = this.#compare(ids[j - 1] ?? 0, ids[j] ?? 0, 1) === 0 ? 0 : 1;
      }
      start = i;
      if ((i & (SLICE_ROWS - 1)) === 0 && slice.tick()) yield;
    }
  }

  /** Frees the sort's arrays (after the window took the rows). */
  releaseSort(): void {
    if (this.#sortBytes > 0) this.charge.free(this.#sortBytes);
    this.#sortBytes = 0;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.releaseSort();
    this.store.close();
    this.#bytes?.close();
    this.charge.free(this.#arrayBytes + this.#translationBytes + this.#mapBytes);
    this.#arrayBytes = 0;
    this.#translationBytes = 0;
    this.#mapBytes = 0;
    this.#gids = new Int32Array(0);
    for (const key of this.#keys) {
      key.hi = new Uint32Array(0);
      key.lo = new Uint32Array(0);
      key.header = null;
    }
  }
}

/** Bits of a partition-rank digit (`#byRank`). */
const RANK_BITS = 7;

/** Ranges up to this many rows are insertion-sorted. */
const SMALL_RANGE = 32;
/** Digits of 11 bits for ranges of at least DIGIT_BUCKETS rows, else 8. */
const DIGIT_BITS = 11;
const DIGIT_BUCKETS = 1 << DIGIT_BITS;

/** Words of the MSD stack for n rows: pending ranges are disjoint, each > SMALL_RANGE. */
function stackWords(n: number): number {
  return 2 * (Math.floor(n / (SMALL_RANGE + 1)) + 2);
}

/**
 * MSD radix of `radix.{ids,lo,hi}A[s … e)` by (hi, lo) unsigned: each range's first varying bit
 * (or-and masks) picks its digit, 11 bits for large ranges and 8 for small ones, scattered
 * through the B buffers; ranges of at most SMALL_RANGE rows are insertion-sorted. Bits every
 * row of a range shares are skipped without a pass.
 */
function* msdWords(radix: RadixBuffers, s: number, e: number, slice: Slice): SlicedJob<void> {
  const { idsA: ia, loA: la, hiA: ha, idsB: ib, loB: lb, hiB: hb, counts, starts, stack } = radix;
  let top = 0;
  stack[top++] = s;
  stack[top++] = e;
  while (top > 0) {
    const hi = stack[--top] ?? 0;
    const lo = stack[--top] ?? 0;
    const n = hi - lo;
    if (n <= SMALL_RANGE) {
      insertionWords(ia, la, ha, lo, hi);
      continue;
    }
    let orHi = 0;
    let andHi = 0xffff_ffff;
    let orLo = 0;
    let andLo = 0xffff_ffff;
    for (let i = lo; i < hi; i++) {
      const h = ha[i] ?? 0;
      const l = la[i] ?? 0;
      orHi |= h;
      andHi &= h;
      orLo |= l;
      andLo &= l;
    }
    const varyHi = (orHi ^ andHi) >>> 0;
    const varyLo = (orLo ^ andLo) >>> 0;
    if (varyHi === 0 && varyLo === 0) continue;
    const high = varyHi !== 0;
    const top1 = 31 - Math.clz32(high ? varyHi : varyLo);
    const bits = n >= DIGIT_BUCKETS ? DIGIT_BITS : 8;
    const shift = Math.max(0, top1 + 1 - bits);
    const mask = (1 << bits) - 1;
    const buckets = mask + 1;
    const src = high ? ha : la;
    counts.fill(0, 0, buckets + 1);
    for (let i = lo; i < hi; i++) {
      const d = ((src[i] ?? 0) >>> shift) & mask;
      counts[d + 1] = (counts[d + 1] ?? 0) + 1;
    }
    for (let d = 0; d < buckets; d++) counts[d + 1] = (counts[d + 1] ?? 0) + (counts[d] ?? 0);
    starts.set(counts.subarray(0, buckets + 1));
    for (let i = lo; i < hi; i++) {
      const d = ((src[i] ?? 0) >>> shift) & mask;
      const at = lo + (counts[d] ?? 0);
      counts[d] = (counts[d] ?? 0) + 1;
      ib[at] = ia[i] ?? 0;
      lb[at] = la[i] ?? 0;
      hb[at] = ha[i] ?? 0;
    }
    ia.set(ib.subarray(lo, hi), lo);
    la.set(lb.subarray(lo, hi), lo);
    ha.set(hb.subarray(lo, hi), lo);
    for (let d = 0; d < buckets; d++) {
      const a = lo + (starts[d] ?? 0);
      const b = lo + (starts[d + 1] ?? 0);
      if (b - a > SMALL_RANGE) {
        if (top + 2 > stack.length) throw new RangeError("Window sort stack overflow");
        stack[top++] = a;
        stack[top++] = b;
      } else if (b - a > 1) insertionWords(ia, la, ha, a, b);
    }
    if (n > SLICE_ROWS && slice.due()) yield;
  }
}

/** Insertion sort of the parallel arrays `[s … e)` by (hi, lo) unsigned. */
function insertionWords(
  ids: Int32Array,
  lo: Uint32Array,
  hi: Uint32Array,
  s: number,
  e: number,
): void {
  for (let i = s + 1; i < e; i++) {
    const id = ids[i] ?? 0;
    const l = lo[i] ?? 0;
    const h = hi[i] ?? 0;
    let j = i - 1;
    for (; j >= s; j--) {
      const hj = hi[j] ?? 0;
      if (hj < h || (hj === h && (lo[j] ?? 0) <= l)) break;
      ids[j + 1] = ids[j] ?? 0;
      lo[j + 1] = lo[j] ?? 0;
      hi[j + 1] = hi[j] ?? 0;
    }
    ids[j + 1] = id;
    lo[j + 1] = l;
    hi[j + 1] = h;
  }
}

/** The LSD passes' double buffers (charged by `sortBytes`). */
class RadixBuffers {
  readonly idsA: Int32Array;
  readonly idsB: Int32Array;
  readonly loA: Uint32Array;
  readonly loB: Uint32Array;
  readonly hiA: Uint32Array;
  readonly hiB: Uint32Array;
  readonly rankA: Uint32Array;
  readonly rankB: Uint32Array;
  readonly counts = new Int32Array(DIGIT_BUCKETS + 1);
  readonly starts = new Int32Array(DIGIT_BUCKETS + 1);
  readonly stack: Int32Array;
  constructor(n: number) {
    this.stack = new Int32Array(stackWords(n));
    this.idsA = new Int32Array(n);
    this.idsB = new Int32Array(n);
    this.loA = new Uint32Array(n);
    this.loB = new Uint32Array(n);
    this.hiA = new Uint32Array(n);
    this.hiB = new Uint32Array(n);
    this.rankA = new Uint32Array(n);
    this.rankB = new Uint32Array(n);
  }
}

/** Stable insertion sort of `ids[s … e)` by `compare`. */
function insertionSort(
  ids: Int32Array,
  s: number,
  e: number,
  compare: (a: number, b: number) => number,
): void {
  for (let i = s + 1; i < e; i++) {
    const value = ids[i] ?? 0;
    let j = i - 1;
    for (; j >= s && compare(ids[j] ?? 0, value) > 0; j--) ids[j + 1] = ids[j] ?? 0;
    ids[j + 1] = value;
  }
}

function columnOf(columns: readonly Vector[], c: number): Vector {
  const column = columns[c];
  if (column === undefined) throw new RangeError(`No column ${String(c)}`);
  return column;
}
