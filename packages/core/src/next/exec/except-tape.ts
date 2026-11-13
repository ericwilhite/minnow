/** Bounded reverse-page tape over shared spill I/O. EXCEPT counts do not depend on page order. */
import type { PhysicalType } from "../types.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import type { SpillPageRef } from "../spill/contract.js";
import { int64At } from "../vector/int64.js";
import { SpillIo } from "./spill-pool.js";
import { PageWriter, SpilledRun } from "./spill-pages.js";
import { ColumnStore } from "./column-store.js";
import { SORT_ROW_VIEW_BYTES, sortRowView } from "./sort-row.js";

const TAPE_FIXED_BYTES = 2048;
const INDEX_TYPES = [
  { kind: "int64" },
  { kind: "int32", bits: 32 },
  { kind: "int32", bits: 32 },
] as const satisfies readonly PhysicalType[];
/** One directory page per data page, linking backwards. No in-memory page-ref list. */
export class ExceptTape {
  readonly run = new SpilledRun();
  #tip: SpillPageRef | null = null;
  #sequence = 0;
  #maxPageBytes = 0;
  #closed = false;
  readonly #offset: Float64Array;
  readonly #length: Int32Array;
  readonly #checksum: Int32Array;
  readonly #index: readonly FlatVector[];
  constructor(
    readonly io: SpillIo,
    readonly partition: number,
  ) {
    // All fixed directory buffers/objects are included in the operator's admitted pool.
    if (!io.pool.allocate(TAPE_FIXED_BYTES))
      throw new RangeError("EXCEPT directory buffers must be admitted");
    this.#offset = new Float64Array(2);
    this.#length = new Int32Array(2);
    this.#checksum = new Int32Array(2);
    this.#index = [
      {
        form: "flat",
        type: INDEX_TYPES[0],
        length: 2,
        validity: null,
        data: { layout: "int64", values: { repr: "safe", values: this.#offset } },
      },
      {
        form: "flat",
        type: INDEX_TYPES[1],
        length: 2,
        validity: null,
        data: { layout: "i32", values: this.#length },
      },
      {
        form: "flat",
        type: INDEX_TYPES[2],
        length: 2,
        validity: null,
        data: { layout: "i32", values: this.#checksum },
      },
    ];
  }
  get rows(): number {
    return this.run.rows;
  }
  get maxPageBytes(): number {
    return this.#maxPageBytes;
  }
  async #append(
    columns: readonly FlatVector[],
    rows: number,
    counted: number,
  ): Promise<SpillPageRef> {
    this.io.checkAborted();
    const header = {
      queryId: this.io.context.queryId,
      run: this.partition,
      partition: this.partition,
      sequence: this.#sequence++,
    };
    let page = this.io.pages.encode(header, columns, rows);
    if (page === null) {
      await this.io.need(this.io.pages.growthFor(columns, rows));
      page = this.io.pages.encode(header, columns, rows);
      if (page === null) throw new RangeError("EXCEPT tape page could not be encoded");
    }
    await this.io.files.append(this.run, page, counted);
    const ref = this.run.pages.pop();
    if (ref === undefined) throw new Error("Missing EXCEPT tape page");
    if (counted > 0) this.#maxPageBytes = Math.max(this.#maxPageBytes, ref.length);
    this.io.checkAborted();
    return ref;
  }
  async flush(store: ColumnStore): Promise<void> {
    if (store.rows === 0) return;
    const columns = store.vectors();
    const large = PageWriter.encodedBytes(columns, store.rows) > store.targetBytes;
    const data = await this.#append(columns, store.rows, store.rows);
    await this.#link(data);
    this.io.pages.shrinkIf(large || this.io.pool.owes);
    this.io.reset(store);
  }
  async #link(data: SpillPageRef): Promise<void> {
    for (let k = 0; k < 2; k++) {
      const ref = k === 0 ? this.#tip : data;
      this.#offset[k] = ref?.offset ?? 0;
      this.#length[k] = ref?.length ?? 0;
      this.#checksum[k] = ref?.checksum ?? 0;
    }
    this.#tip = await this.#append(this.#index, 2, 0);
  }
  async stage(store: ColumnStore, columns: readonly Vector[], row: number): Promise<void> {
    // A wide input is already borrowed. Encode it directly instead of pinning a
    // power-of-two staging copy while asking for the page encoder's second loan.
    if (store.appendNeed(columns, row) > Math.max(store.targetBytes, 64 * 1024)) {
      await this.flush(store);
      const metadata = store.types.length * SORT_ROW_VIEW_BYTES;
      await this.io.need(metadata);
      if (!this.io.pool.allocate(metadata)) throw new RangeError("EXCEPT row views not admitted");
      try {
        const views = columns
          .slice(0, store.types.length)
          .map((column) => sortRowView(column, row));
        const data = await this.#append(views, 1, 1);
        await this.#link(data);
        this.io.pages.shrinkIf(true);
      } finally {
        this.io.pool.free(metadata);
      }
      return;
    }
    if (store.append(columns, null, row, 1) === 0) {
      await this.flush(store);
      if (store.append(columns, null, row, 1) === 0) {
        await this.io.need(2 * store.appendNeed(columns, row) + 64 * 1024);
        if (store.append(columns, null, row, 1) === 0)
          throw new RangeError("EXCEPT tape row could not be staged");
      }
    }
    if (
      this.io.pool.owes ||
      (store.rows === 1 && PageWriter.encodedBytes(store.vectors(), 1) > store.targetBytes)
    )
      await this.flush(store);
  }
  async #read(ref: SpillPageRef, types: readonly PhysicalType[]) {
    this.run.pages.push(ref);
    let page;
    try {
      page = await this.io.readPage(this.run, 0);
    } finally {
      this.run.pages.pop();
    }
    try {
      return { page, decoded: await this.io.decode(page, types) };
    } catch (error) {
      this.io.releasePage(page);
      throw error;
    }
  }
  async each(
    types: readonly PhysicalType[],
    visit: (columns: readonly Vector[], rows: number, ref: SpillPageRef) => Promise<void>,
    beforePage?: (ref: SpillPageRef) => Promise<void>,
  ): Promise<void> {
    let tip = this.#tip;
    while (tip !== null) {
      const index = await this.#read(tip, INDEX_TYPES);
      let data: SpillPageRef;
      try {
        const offset = index.decoded.columns[0]?.data;
        const length = index.decoded.columns[1]?.data;
        const checksum = index.decoded.columns[2]?.data;
        if (offset?.layout !== "int64" || length?.layout !== "i32" || checksum?.layout !== "i32")
          throw new Error("Invalid EXCEPT directory");
        const ref = (k: number): SpillPageRef => ({
          offset: Number(int64At(offset.values, k)),
          length: length.values[k] ?? 0,
          checksum: (checksum.values[k] ?? 0) >>> 0,
        });
        const previous = ref(0);
        tip = previous.length === 0 ? null : previous;
        data = ref(1);
      } finally {
        this.io.releasePage(index.page);
      }
      await beforePage?.(data);
      await this.withPage(data, types, (columns, rows) => visit(columns, rows, data));
    }
  }
  async withPage(
    ref: SpillPageRef,
    types: readonly PhysicalType[],
    visit: (columns: readonly Vector[], rows: number) => Promise<void>,
  ): Promise<void> {
    const held = await this.#read(ref, types);
    try {
      await visit(held.decoded.columns, held.decoded.rows);
    } finally {
      this.io.releasePage(held.page);
    }
  }
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.io.files.release(this.run);
    this.#tip = null;
    this.io.pool.free(TAPE_FIXED_BYTES);
  }
}
