/** FIFO spill pages with a bounded binary directory; no in-memory page-ref list. */
import type { PhysicalType } from "../types.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import type { SpillPageRef } from "../spill/contract.js";
import { SpillCorruptionError } from "../spill/contract.js";
import { int64At } from "../vector/int64.js";
import { SpillIo } from "./spill-pool.js";
import { SpilledRun, SpillClosedError } from "./spill-pages.js";
// Byte offsets are safe JS integers: fewer than 2^53 pages, hence at most 53 carry levels.
const LEVELS = 53,
  METADATA = 16384;
const INDEX_TYPES = [
  { kind: "int64" },
  { kind: "int32", bits: 32 },
  { kind: "int32", bits: 32 },
] as const satisfies readonly PhysicalType[];
interface Frame {
  readonly ref: SpillPageRef;
  readonly level: number;
}
export class RecursiveTape {
  readonly run: SpilledRun;
  readonly #roots: Array<SpillPageRef | null>;
  readonly #stack: Array<Frame | null>;
  readonly #offset: Float64Array;
  readonly #length: Int32Array;
  readonly #checksum: Int32Array;
  readonly #index: readonly FlatVector[];
  #sequence = 0;
  #closed = false;
  #closing: Promise<void> | null = null;
  #reading = false;
  constructor(
    readonly io: SpillIo,
    readonly id: number,
  ) {
    if (!io.pool.allocate(METADATA))
      throw new RangeError("Recursive tape metadata must be admitted");
    this.run = new SpilledRun();
    this.#roots = new Array<SpillPageRef | null>(LEVELS).fill(null);
    this.#stack = new Array<Frame | null>(LEVELS).fill(null);
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
  #check(): void {
    this.io.checkAborted();
    if (this.#closed) throw new SpillClosedError();
  }
  async #append(
    columns: readonly FlatVector[],
    rows: number,
    counted: number,
  ): Promise<SpillPageRef> {
    this.#check();
    const paced = this.io.pace?.();
    if (paced != null) await paced;
    this.#check();
    const header = {
      queryId: this.io.context.queryId,
      run: this.id,
      partition: this.id,
      sequence: this.#sequence++,
    };
    let page = this.io.pages.encode(header, columns, rows);
    if (page === null) {
      await this.io.need(this.io.pages.growthFor(columns, rows));
      this.#check();
      page = this.io.pages.encode(header, columns, rows);
      if (page === null) throw new RangeError("Recursive page must fit its prepaid pool");
    }
    await this.io.files.append(this.run, page, counted);
    const ref = this.run.pages.pop();
    if (ref === undefined) throw new Error("Missing recursive page reference");
    this.#check();
    return ref;
  }
  async append(columns: readonly FlatVector[], rows: number): Promise<void> {
    if (this.#reading) throw new TypeError("Append after recursive tape read");
    let ref = await this.#append(columns, rows, rows),
      level = 0;
    while (this.#roots[level] !== null) {
      const left = this.#roots[level];
      if (left == null) throw new RangeError("Recursive spill offset exceeds safe integers");
      for (let k = 0; k < 2; k++) {
        const child = k === 0 ? left : ref;
        this.#offset[k] = child.offset;
        this.#length[k] = child.length;
        this.#checksum[k] = child.checksum;
      }
      ref = await this.#append(this.#index, 2, 0);
      this.#roots[level] = null;
      level++;
    }
    this.#roots[level] = ref;
  }
  async #read(ref: SpillPageRef, types: readonly PhysicalType[]) {
    this.#check();
    const paced = this.io.pace?.();
    if (paced != null) await paced;
    this.#check();
    this.run.pages.push(ref);
    let page;
    try {
      page = await this.io.readPage(this.run, 0);
    } finally {
      this.run.pages.pop();
    }
    try {
      this.#check();
      const decoded = await this.io.decode(page, types);
      this.#check();
      return { page, decoded };
    } catch (error) {
      this.io.releasePage(page);
      throw error;
    }
  }
  async each(
    types: readonly PhysicalType[],
    visit: (columns: readonly Vector[], rows: number) => Promise<boolean>,
  ): Promise<void> {
    this.#reading = true;
    let top = 0;
    const push = (ref: SpillPageRef, level: number) => {
      if (top === LEVELS) throw new SpillCorruptionError("Recursive directory depth");
      this.#stack[top++] = { ref, level };
    };
    // Carry roots high-to-low are successive chunks in original append order.
    for (let level = LEVELS - 1; level >= 0; level--) {
      const root = this.#roots[level];
      if (root == null) continue;
      push(root, level);
      while (top > 0) {
        this.#check();
        const frame = this.#stack[--top];
        this.#stack[top] = null;
        if (frame == null) throw new SpillCorruptionError("Missing recursive directory frame");
        if (frame.level === 0) {
          const data = await this.#read(frame.ref, types);
          try {
            if (!(await visit(data.decoded.columns, data.decoded.rows))) return;
          } finally {
            this.io.releasePage(data.page);
          }
          continue;
        }
        const node = await this.#read(frame.ref, INDEX_TYPES);
        let left: SpillPageRef, right: SpillPageRef;
        try {
          const offsets = node.decoded.columns[0]?.data,
            lengths = node.decoded.columns[1]?.data,
            checksums = node.decoded.columns[2]?.data;
          if (
            node.decoded.rows !== 2 ||
            offsets?.layout !== "int64" ||
            lengths?.layout !== "i32" ||
            checksums?.layout !== "i32"
          )
            throw new SpillCorruptionError("Invalid recursive directory");
          const ref = (k: number): SpillPageRef => {
            const offset = Number(int64At(offsets.values, k)),
              length = lengths.values[k] ?? 0;
            if (!Number.isSafeInteger(offset) || offset < 0 || length <= 0)
              throw new SpillCorruptionError("Invalid recursive directory reference");
            return { offset, length, checksum: (checksums.values[k] ?? 0) >>> 0 };
          };
          left = ref(0);
          right = ref(1);
        } finally {
          this.io.releasePage(node.page);
        }
        push(right, frame.level - 1);
        push(left, frame.level - 1);
      }
    }
  }
  close(): Promise<void> {
    if (this.#closing !== null) return this.#closing;
    this.#closed = true;
    this.#closing = (async () => {
      try {
        await this.io.files.release(this.run);
      } finally {
        this.#roots.fill(null);
        this.#stack.fill(null);
        this.io.pool.free(METADATA);
      }
    })();
    return this.#closing;
  }
}
