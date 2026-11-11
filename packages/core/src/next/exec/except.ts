/** Hash-count EXCEPT, with radix spill and a bounded one-key collision fallback. */
import type { Grant } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import type { PhysicalType } from "../types.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  newDictionaryId,
  type Batch,
  type Vector,
} from "../vector/vector.js";
import { int64At } from "../vector/int64.js";
import { ColumnStore } from "./column-store.js";
import { ExecutionError, UnsupportedPlanError } from "./errors.js";
import { RecordCheckpoint } from "./record-checkpoint.js";
import { ExceptTape } from "./except-tape.js";
import { KeyLayout, KeyScratch, KeyWork, equalJob, extractKeysJob } from "./join-keys.js";
import { isValidSlot, slotOf } from "./kernels/vectors.js";
import {
  afterYield,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type Transfer,
} from "./pipeline.js";
import { Slice, runSliced } from "./slices.js";
import { PageWriter, SpillFiles } from "./spill-pages.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";

const PARTITIONS = 8,
  LAST_LEVEL = 11;
const COUNT: PhysicalType = { kind: "int64" };
const MAX_COUNT = (1n << 63n) - 1n;
interface Location {
  tape: ExceptTape;
  ref: { offset: number; length: number; checksum: number };
  row: number;
}
function ownLocation(location: Location): Location {
  return { tape: location.tape, row: location.row, ref: { ...location.ref } };
}
interface Entry {
  readonly key: Uint8Array;
  readonly hash: number;
  readonly second: number;
  row: number;
  rowBytes: number;
  ordinal: bigint;
  bytes: number;
  location?: Location;
  readonly next: Entry | null;
  left: bigint;
  right: bigint;
}
interface Children {
  tapes: ExceptTape[];
  stores: ColumnStore[];
}
export interface ExceptContext {
  readonly grant: Grant;
  readonly spill: SpillService;
  readonly queryId: number;
  readonly signal?: AbortSignal;
  /** Collision stress seam; production uses all hash bits. */
  readonly hashMask?: number;
}
export function exceptKeyLayout(types: readonly PhysicalType[]): KeyLayout {
  if (types.length === 0 || types.length > 31)
    throw new UnsupportedPlanError("EXCEPT needs 1 to 31 key columns", { construct: "setOp" });
  if (types.some((type) => type.kind === "text" && type.fixed === true))
    throw new UnsupportedPlanError("EXCEPT blank-padded CHAR equality", { construct: "setOp" });
  try {
    return new KeyLayout(
      types,
      types.map(() => true),
    );
  } catch (error) {
    if (error instanceof UnsupportedPlanError)
      throw new UnsupportedPlanError("EXCEPT key type", { construct: "setOp" });
    throw error;
  }
}
function reserve(types: readonly PhysicalType[]): number {
  return (2 << 20) + PARTITIONS * (types.length + 3) * 8192;
}
export function exceptMinimumBytes(types: readonly PhysicalType[]): number {
  const layout = exceptKeyLayout(types);
  return reserve(types) + (2 << 20) + KeyScratch.bytes(layout, 1);
}
function countAt(vector: Vector | undefined, row: number): bigint {
  if (vector === undefined) throw new Error("Missing EXCEPT count");
  const [flat, slot] = slotOf(vector, row);
  if (flat.data.layout !== "int64") throw new Error("Invalid EXCEPT count");
  return int64At(flat.data.values, slot);
}

/** Only immutable row/count records survive an execution phase boundary. */
export interface ExceptCheckpoint {
  readonly records: RecordCheckpoint;
  readonly leftDone: boolean;
  readonly leftOrdinal: bigint;
  readonly recordBytes: number;
  readonly profile: ExceptOperator["profile"];
}

export class ExceptOperator {
  readonly left: Operator;
  readonly right: Operator;
  readonly profile = { rowsIn: 0, rowsOut: 0, spilledBytes: 0, spillPasses: 0, collisionPasses: 0 };
  readonly #charge: Charge;
  readonly #pool: Pool;
  readonly #files: SpillFiles;
  readonly #pages: PageWriter;
  readonly #io: SpillIo;
  #scratch: KeyScratch;
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  readonly #work = new KeyWork();
  readonly #recordTypes: readonly PhysicalType[];
  readonly #table = new Map<number, Entry>();
  readonly #keep: number;
  #store: ColumnStore;
  #entryBytes = 0;
  #rowCredit = 0;
  #ioBytes = 0;
  #children: Children | null = null;
  #pending: Promise<void> = Promise.resolve();
  #disposed: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  readonly #codes: Int32Array;
  readonly #nulls: Uint32Array;
  readonly #counts: readonly [BigInt64Array, BigInt64Array, BigInt64Array];
  #leftOrdinal = 0n;
  #leftDone = false;
  #priorSpilled = 0;
  readonly #countVectors: readonly Vector[];
  readonly closed: Promise<void>;
  #resolveClosed!: () => void;
  #rejectClosed!: (reason: unknown) => void;
  constructor(
    readonly types: readonly PhysicalType[],
    readonly all: boolean,
    readonly context: ExceptContext,
    readonly downstream: Operator,
  ) {
    const layout = exceptKeyLayout(types);
    this.#keep = reserve(types);
    this.#charge = new Charge(context.grant);
    this.#pool = new Pool(this.#charge, exceptMinimumBytes(types));
    // Tag I/O suballocations so reserved I/O room is not counted twice once occupied.
    const ioPool = new Proxy(this.#pool, {
      get: (target, property) => {
        if (property === "allocate")
          return (bytes: number) => {
            if (!target.allocate(bytes)) return false;
            this.#ioBytes += bytes;
            return true;
          };
        if (property === "free")
          return (bytes: number) => {
            this.#ioBytes -= bytes;
            target.free(bytes);
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function"
          ? (value as (...args: unknown[]) => unknown).bind(target)
          : value;
      },
    });
    // File write batching is optional. Bound it across every open partition; a service
    // that cannot obtain a cache writes pages directly instead of pinning one per file.
    let cached = 0;
    this.#files = new SpillFiles(context.spill, context.queryId, {
      allocate: (bytes) => {
        if (cached + bytes > 256 * 1024 || !ioPool.allocate(bytes)) return false;
        cached += bytes;
        return true;
      },
      free: (bytes) => {
        cached -= bytes;
        ioPool.free(bytes);
      },
    });
    this.#pages = new PageWriter(ioPool, 16 * 1024);
    this.#io = new SpillIo(
      ioPool,
      this.#files,
      this.#pages,
      context,
      () => this.#check(),
      () => this.#pace(),
    );
    this.#scratch = new KeyScratch(layout, 1, ioPool);
    if (!this.#pool.allocate(4 * BATCH_ROWS + 4096))
      throw new RangeError("EXCEPT fixed scratch must be admitted");
    this.#codes = new Int32Array(BATCH_ROWS);
    this.#nulls = new Uint32Array((BATCH_ROWS + 31) >>> 5);
    this.#counts = [new BigInt64Array(1), new BigInt64Array(1), new BigInt64Array(1)];
    this.#recordTypes = [...types, COUNT, COUNT, COUNT];
    this.#countVectors = this.#counts.map((values): Vector => ({
      form: "constant",
      type: COUNT,
      length: 65536,
      value: {
        form: "flat",
        type: COUNT,
        length: 1,
        validity: null,
        data: { layout: "int64", values: { repr: "int64", values } },
      },
    }));
    this.#store = this.#newStore();
    this.closed = new Promise((resolve, reject) => {
      this.#resolveClosed = resolve;
      this.#rejectClosed = reject;
    });
    // The runtime observes cleanup; avoid an unhandled rejection between an abort and disposal.
    this.closed.catch(() => undefined);
    this.left = this.#input(false);
    this.right = this.#input(true);
  }
  #check(): void {
    if (this.context.signal?.aborted) throw this.context.signal.reason;
    if (this.#aborted) throw this.#reason;
  }
  #pace(): Promise<void> | null {
    this.#check();
    return this.#slice.due()
      ? yieldTask().then(() => {
          afterYield(this.#slice.clock);
          this.#check();
        })
      : null;
  }
  #headroom(): number {
    return Math.max(0, this.#keep - this.#ioBytes);
  }
  #newStore(): ColumnStore {
    return new ColumnStore(
      this.types,
      {
        allocate: (bytes) => {
          if (bytes <= this.#rowCredit) {
            this.#rowCredit -= bytes;
            return true;
          }
          return this.#pool.allocate(bytes, this.#headroom());
        },
        free: (bytes) => this.#pool.free(bytes),
      },
      { chunkRows: 256, varlenBytes: 256 },
    );
  }
  #input(right: boolean): Operator {
    return {
      push: (batch, transfer?: Transfer) => {
        try {
          this.#check();
        } catch (error) {
          transfer?.release();
          throw error;
        }
        this.#pending = this.#ingest(batch, right).finally(() => transfer?.release());
        return "blocked";
      },
      ready: () => this.#pending,
      finish: () => {
        if (right) return (this.#pending = this.#finish());
        this.#leftDone = true;
      },
      abort: (reason) => this.abort(reason),
    };
  }
  get spilledBytes(): number {
    return this.#priorSpilled + this.#files.written;
  }
  get leftComplete(): boolean {
    return this.#leftDone;
  }
  /** Pressure only: collapse resident entries to their exact counted records, copy
   * existing spill records, then detach every native writer before admission waits.
   * Counts and first-left ordinals restore directly; input rows are never replayed. */
  checkpoint(): Promise<ExceptCheckpoint> {
    const prior = this.#pending;
    const work = (async () => {
      await prior;
      this.#check();
      const records = new RecordCheckpoint(this.#io, this.context.spill, this.#recordTypes);
      try {
        await this.#flush(0);
        const children = this.#children;
        if (children) {
          for (let i = 0; i < children.tapes.length; i++) {
            const tape = children.tapes[i],
              store = children.stores[i];
            if (!tape || !store) throw new Error("Missing EXCEPT checkpoint partition");
            await tape.flush(store);
            await tape.each(this.#recordTypes, async (columns, rows) =>
              records.write(columns, 0, rows),
            );
          }
        }
        await records.seal();
        this.#check();
        return {
          records,
          leftDone: this.#leftDone,
          leftOrdinal: this.#leftOrdinal,
          recordBytes: records.files.written,
          profile: {
            ...this.profile,
            spilledBytes: this.#priorSpilled + this.#files.written + records.files.written,
          },
        };
      } catch (error) {
        await records.close().catch(() => undefined);
        throw error;
      }
    })();
    this.#pending = work.then(() => undefined);
    void this.#pending.catch(() => undefined);
    return work;
  }
  restore(checkpoint: ExceptCheckpoint): Promise<void> {
    const prior = this.#pending;
    const work = (async () => {
      await prior;
      this.#check();
      await checkpoint.records.read(this.#io, async (columns, rows) => {
        for (let row = 0; row < rows; row++) {
          await this.#pace();
          await this.#add(
            columns,
            row,
            countAt(columns[this.types.length], row),
            countAt(columns[this.types.length + 1], row),
            0,
            countAt(columns[this.types.length + 2], row),
          );
        }
      });
      this.#leftDone = checkpoint.leftDone;
      this.#leftOrdinal = checkpoint.leftOrdinal;
      Object.assign(this.profile, checkpoint.profile);
      this.#priorSpilled =
        checkpoint.profile.spilledBytes + checkpoint.records.files.written - checkpoint.recordBytes;
      await checkpoint.records.close();
    })();
    this.#pending = work;
    void work.catch(() => undefined);
    return work;
  }
  async #ingest(batch: Batch, right: boolean): Promise<void> {
    const count = batch.selection?.count ?? batch.length;
    for (let k = 0; k < count; k++) {
      await this.#pace();
      const row = batch.selection?.rows[k] ?? k;
      await this.#add(
        batch.columns,
        row,
        right ? 0n : 1n,
        right ? 1n : 0n,
        0,
        right ? MAX_COUNT : ++this.#leftOrdinal,
      );
      if (this.#pool.owes) {
        await this.#flush(0);
        this.#shrinkKeys();
      }
      this.profile.rowsIn++;
    }
  }
  #shrinkKeys(): void {
    const scratch = this.#scratch;
    scratch.encoded?.close();
    scratch.encodedParts?.close();
    this.#io.pool.free(
      scratch.words.byteLength +
        scratch.nulls.byteLength +
        scratch.hashes.byteLength +
        scratch.hashes2.byteLength +
        scratch.slots.byteLength,
    );
    this.#scratch = new KeyScratch(scratch.layout, 1, this.#io.pool);
    this.#pool.settle();
  }
  async #key(
    columns: readonly Vector[],
    row: number,
    level: number,
    mayFlush: boolean,
  ): Promise<Uint8Array> {
    const extract = () =>
      runSliced(
        extractKeysJob(columns, null, row, 1, this.#scratch, this.#work, this.#slice),
        this.#slice,
        () => this.#check(),
      );
    if ((await extract()) !== 1) {
      if (mayFlush) await this.#flush(level);
      if ((await extract()) !== 1) {
        await this.#io.need(this.#scratch.requiredBytes + this.#keep);
        if ((await extract()) !== 1) throw new RangeError("EXCEPT key could not be encoded");
      }
    }
    if (this.#scratch.layout.family === "words")
      return new Uint8Array(this.#scratch.words.buffer, 0, 4 * this.#scratch.layout.width);
    const encoded = this.#scratch.encoded;
    if (encoded === null) throw new Error("Missing EXCEPT equality key");
    return encoded.key(0);
  }
  async #find(key: Uint8Array, hash: number, second: number): Promise<Entry | null> {
    for (let entry = this.#table.get(hash) ?? null; entry !== null; entry = entry.next) {
      if (
        entry.second === second &&
        (await runSliced(
          equalJob(key, 0, key.length, entry.key, 0, entry.key.length, this.#work, this.#slice),
          this.#slice,
          () => this.#check(),
        ))
      )
        return entry;
    }
    return null;
  }
  async #add(
    columns: readonly Vector[],
    row: number,
    left: bigint,
    right: bigint,
    level: number,
    ordinal: bigint,
    peel?: (columns: readonly Vector[], row: number) => Promise<void>,
    location?: Location,
  ): Promise<void> {
    const key = await this.#key(
      columns.slice(0, this.types.length),
      row,
      level,
      peel === undefined,
    );
    const mask = this.context.hashMask ?? 0xffff_ffff;
    const hash = ((this.#scratch.hashes[0] ?? 0) & mask) >>> 0,
      second = ((this.#scratch.hashes2[0] ?? 0) & mask) >>> 0;
    let entry = await this.#find(key, hash, second);
    if (entry === null && peel !== undefined && this.#table.size > 0) {
      await peel(columns, row);
      return;
    }
    if (entry === null) {
      // Payload buffer capacity, key buffer header, entry, hash slot and bigint overlap.
      const bytes = key.length + 6 * 64 + 128 + (location === undefined ? 0 : 256);
      let rowNeed = location === undefined ? this.#store.appendNeed(columns, row) : 0;
      let need = bytes + rowNeed;
      if (!this.#pool.allocate(need, this.#headroom())) {
        await this.#flush(level);
        rowNeed = location === undefined ? this.#store.appendNeed(columns, row) : 0;
        need = bytes + rowNeed;
        if (!this.#pool.allocate(need, this.#headroom())) {
          await this.#io.need(need + this.#headroom());
          if (!this.#pool.allocate(need, this.#headroom()))
            throw new RangeError("EXCEPT row could not be held");
        }
      }
      this.#rowCredit = need - bytes;
      const id = location === undefined ? this.#store.rows : -1;
      if (
        location === undefined &&
        this.#store.append(columns.slice(0, this.types.length), null, row, 1) !== 1
      ) {
        this.#pool.free(bytes + this.#rowCredit);
        this.#rowCredit = 0;
        if (this.#table.size === 0)
          throw new RangeError("EXCEPT representative could not be copied");
        await this.#flush(level);
        return this.#add(columns, row, left, right, level, ordinal, peel, location);
      }
      this.#pool.free(this.#rowCredit);
      this.#rowCredit = 0;
      const copy = new Uint8Array(key.length);
      for (let offset = 0; offset < key.length; offset += 8192) {
        copy.set(key.subarray(offset, offset + 8192), offset);
        await this.#pace();
      }
      entry = {
        key: copy,
        hash,
        second,
        row: id,
        ordinal,
        ...(location === undefined ? {} : { location: ownLocation(location) }),
        rowBytes: Math.max(
          rowNeed,
          location?.ref.length ?? 0,
          key.length + 64 * this.types.length + 256,
        ),
        bytes,
        next: this.#table.get(hash) ?? null,
        left: 0n,
        right: 0n,
      };
      this.#entryBytes += bytes;
      this.#table.set(hash, entry);
    }
    // Equality does not imply identical presentation (-0, NUMERIC scale, 1 month/30 days).
    // Preserve the first LEFT representative, even when spill pages are read in reverse order.
    if (left > 0n && ordinal < entry.ordinal) {
      if (location !== undefined) {
        if (entry.location === undefined) {
          if (!this.#pool.allocate(256, this.#headroom())) {
            await this.#flush(level);
            return this.#add(columns, row, left, right, level, ordinal, peel, location);
          }
          entry.bytes += 256;
          this.#entryBytes += 256;
        }
        if (entry.location === undefined) entry.location = ownLocation(location);
        else {
          entry.location.tape = location.tape;
          entry.location.row = location.row;
          entry.location.ref.offset = location.ref.offset;
          entry.location.ref.length = location.ref.length;
          entry.location.ref.checksum = location.ref.checksum;
        }
        entry.row = -1;
        entry.rowBytes = Math.max(location.ref.length, key.length + 64 * this.types.length + 256);
      } else if (
        this.types.some((type) => ["decimal", "interval", "float32", "float64"].includes(type.kind))
      ) {
        if (peel !== undefined) {
          this.#store.close();
          this.#store = this.#newStore();
        }
        const need = this.#store.appendNeed(columns, row);
        if (!this.#pool.allocate(need, this.#headroom())) {
          await this.#flush(level);
          return this.#add(columns, row, left, right, level, ordinal, peel, location);
        }
        this.#rowCredit = need;
        const id = this.#store.rows;
        if (this.#store.append(columns.slice(0, this.types.length), null, row, 1) !== 1) {
          this.#pool.free(this.#rowCredit);
          this.#rowCredit = 0;
          await this.#flush(level);
          return this.#add(columns, row, left, right, level, ordinal, peel, location);
        }
        this.#pool.free(this.#rowCredit);
        this.#rowCredit = 0;
        entry.row = id;
        delete entry.location;
        entry.rowBytes = Math.max(need, key.length + 64 * this.types.length + 256);
      }
      entry.ordinal = ordinal;
    }
    entry.left += left;
    entry.right += right;
    if (entry.left > MAX_COUNT || entry.right > MAX_COUNT)
      throw new ExecutionError("EXCEPT count overflow", "MINNOW_EXCEPT_COUNT", "22003");
  }
  *#entries(): Generator<Entry> {
    for (const head of this.#table.values())
      for (let entry: Entry | null = head; entry !== null; entry = entry.next) yield entry;
  }
  #columns(row: number): { columns: Vector[]; row: number } {
    const chunk = row >>> this.#store.chunkShift;
    return {
      columns: this.types.map((_, c) => this.#store.chunk(c, chunk)),
      row: row & this.#store.chunkMask,
    };
  }
  async #value(
    entry: Entry,
    use: (columns: readonly Vector[], row: number) => Promise<void>,
  ): Promise<void> {
    const location = entry.location;
    if (location !== undefined) {
      await location.tape.withPage(location.ref, this.#recordTypes, (columns) =>
        use(columns.slice(0, this.types.length), location.row),
      );
    } else {
      const value = this.#columns(entry.row);
      await use(value.columns, value.row);
    }
  }
  #newChildren(): Children {
    return {
      tapes: Array.from({ length: PARTITIONS }, (_, p) => new ExceptTape(this.#io, p)),
      stores: Array.from(
        { length: PARTITIONS },
        () =>
          new ColumnStore(this.#recordTypes, this.#io.pool, {
            chunkRows: 64,
            targetBytes: 16 * 1024,
            varlenBytes: 256,
          }),
      ),
    };
  }
  async #flush(level: number): Promise<void> {
    if (this.#table.size === 0) return;
    const children = (this.#children ??= this.#newChildren());
    for (const entry of this.#entries()) {
      await this.#pace();
      const partition = (entry.hash >>> ((level % LAST_LEVEL) * 3)) & (PARTITIONS - 1);
      const tape = children.tapes[partition],
        store = children.stores[partition];
      if (tape === undefined || store === undefined) throw new Error("Missing EXCEPT partition");
      await this.#value(entry, async (columns, row) => {
        this.#counts[0][0] = entry.left;
        this.#counts[1][0] = entry.right;
        this.#counts[2][0] = entry.ordinal;
        await tape.stage(store, [...columns, ...this.#countVectors], row);
      });
    }
    this.#reset();
  }
  #reset(): void {
    this.#table.clear();
    this.#pool.free(this.#entryBytes);
    this.#entryBytes = 0;
    this.#store.close();
    this.#store = this.#newStore();
  }
  async #emit(): Promise<void> {
    for (const entry of this.#entries()) {
      let copies = this.all
        ? entry.left - entry.right
        : entry.left > 0n && entry.right === 0n
          ? 1n
          : 0n;
      if (copies <= 0n) continue;
      await this.#value(entry, async (columns, row) => {
        while (copies > 0n) {
          await this.#pace();
          const limit = Math.max(
            1,
            Math.min(BATCH_ROWS, Math.floor(BATCH_TARGET_BYTES / entry.rowBytes)),
          );
          const n = Number(copies > BigInt(limit) ? BigInt(limit) : copies);
          this.#codes.fill(row, 0, n);
          const output: Vector[] = columns.map((dictionary) => {
            if (dictionary.form !== "flat") throw new Error("EXCEPT representatives must be flat");
            return {
              form: "dictionary",
              type: dictionary.type,
              length: n,
              codes: this.#codes,
              validity: isValidSlot(dictionary, row) ? null : this.#nulls,
              dictionary,
              dictionaryId: newDictionaryId(),
            };
          });
          const batch: Batch = { columns: output, length: n, selection: null };
          if (this.downstream.push(batch) === "blocked") await this.downstream.ready();
          this.profile.rowsOut += n;
          copies -= BigInt(n);
        }
      });
    }
  }
  async #drain(level: number): Promise<void> {
    if (this.#children === null) {
      await this.#emit();
      this.#reset();
      return;
    }
    await this.#flush(level);
    const children = this.#children;
    this.#children = null;
    for (let p = 0; p < PARTITIONS; p++) {
      const tape = children.tapes[p],
        store = children.stores[p];
      if (tape === undefined || store === undefined) throw new Error("Missing EXCEPT partition");
      await tape.flush(store);
      store.close();
    }
    for (const tape of children.tapes) {
      if (tape.rows > 0) await this.#merge(tape, level + 1);
      await tape.close();
    }
  }
  async #merge(tape: ExceptTape, level: number): Promise<void> {
    this.profile.spillPasses++;
    if (level >= LAST_LEVEL) {
      await this.#peel(tape);
      return;
    }
    await tape.each(
      this.#recordTypes,
      async (columns, rows, ref) => {
        for (let row = 0; row < rows; row++) {
          await this.#pace();
          await this.#add(
            columns,
            row,
            countAt(columns[this.types.length], row),
            countAt(columns[this.types.length + 1], row),
            level,
            countAt(columns[this.types.length + 2], row),
            undefined,
            rows === 1 && ref.length > this.#keep / 4 ? { tape, ref, row } : undefined,
          );
        }
      },
      async (ref) => {
        if (ref.length > this.#keep / 4 && this.#table.size > 0) await this.#flush(level);
      },
    );
    await this.#drain(level);
  }
  async #peel(tape: ExceptTape): Promise<void> {
    let input = tape;
    for (;;) {
      this.profile.collisionPasses++;
      const rest = new ExceptTape(this.#io, LAST_LEVEL);
      const staging = new ColumnStore(this.#recordTypes, this.#io.pool, {
        chunkRows: 64,
        targetBytes: 16 * 1024,
        varlenBytes: 256,
      });
      await input.each(this.#recordTypes, async (columns, rows, ref) => {
        for (let row = 0; row < rows; row++) {
          await this.#pace();
          await this.#add(
            columns,
            row,
            countAt(columns[this.types.length], row),
            countAt(columns[this.types.length + 1], row),
            LAST_LEVEL,
            countAt(columns[this.types.length + 2], row),
            (values, at) => rest.stage(staging, values, at),
            { tape: input, ref, row },
          );
        }
      });
      await rest.flush(staging);
      staging.close();
      await this.#emit();
      this.#reset();
      if (input !== tape) await input.close();
      if (rest.rows === 0) {
        await rest.close();
        return;
      }
      input = rest;
    }
  }
  async #finish(): Promise<void> {
    try {
      this.#check();
      await this.#drain(0);
      this.profile.spilledBytes = this.#priorSpilled + this.#files.written;
      await this.downstream.finish();
    } finally {
      await this.#dispose();
    }
  }
  #dispose(): Promise<void> {
    return (this.#disposed ??= (async () => {
      try {
        await this.#files.closeAll();
      } finally {
        this.#table.clear();
        for (const store of this.#children?.stores ?? []) store.close();
        this.#children = null;
        this.#store.close();
        this.#scratch.encoded?.close();
        this.#scratch.encodedParts?.close();
        this.#pages.close();
        this.#pool.close();
      }
    })().then(
      () => this.#resolveClosed(),
      (error: unknown) => {
        this.#rejectClosed(error);
        throw error;
      },
    ));
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    try {
      this.downstream.abort(reason);
    } finally {
      this.#pending
        .catch(() => undefined)
        .then(() => this.#dispose())
        .catch(() => undefined);
    }
  }
}
