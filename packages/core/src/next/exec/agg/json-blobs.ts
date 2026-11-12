/** Constant-memory native JSON blob chains. All page references live in spill, not a JS array. */
import type { SpillPageRef, SpillService } from "../../spill/contract.js";
import { SpillCorruptionError } from "../../spill/contract.js";
import type { PhysicalType } from "../../types.js";
import type { FlatVector } from "../../vector/vector.js";
import { SpilledRun, SpillFiles, SpillClosedError } from "../spill-pages.js";
import { SpillIo } from "../spill-pool.js";
import { TapeDirectory } from "./hash-tape.js";

const BYTES: PhysicalType = { kind: "bytes" };
const META = 2048;
export interface JsonBlob {
  readonly head: SpillPageRef | null;
  readonly pages: number;
  readonly maximum: number;
  readonly bytes: number;
}
export class JsonBlobs {
  readonly io: SpillIo;
  readonly files: SpillFiles;
  readonly #data: SpilledRun;
  readonly #index: SpilledRun;
  readonly #directory: TapeDirectory;
  readonly #offsets: Int32Array;
  readonly #column: FlatVector;
  readonly #wireData: { layout: "varlen"; bytes: Uint8Array; offsets: Int32Array };
  readonly #pending = new Set<Promise<unknown>>();
  #closing: Promise<void> | null = null;
  constructor(parent: SpillIo, service: SpillService) {
    if (!parent.pool.allocate(META)) throw new Error("JSON blob metadata was not admitted");
    this.files = new SpillFiles(service, parent.context.queryId, {
      allocate: () => false,
      free: () => {
        throw new Error("Unbuffered JSON blob cache");
      },
    });
    this.io = new SpillIo(
      parent.pool,
      this.files,
      parent.pages,
      parent.context,
      parent.checkAborted,
      parent.pace,
    );
    this.#data = new SpilledRun();
    this.#index = new SpilledRun();
    this.#directory = new TapeDirectory(this.io, parent.pool);
    this.#offsets = new Int32Array(2);
    this.#wireData = { layout: "varlen", bytes: new Uint8Array(0), offsets: this.#offsets };
    this.#column = { form: "flat", type: BYTES, length: 1, validity: null, data: this.#wireData };
  }
  empty(): JsonBlob {
    return { head: null, pages: 0, maximum: 0, bytes: 0 };
  }
  #track<T>(work: () => Promise<T>): Promise<T> {
    if (this.#closing) return Promise.reject(new SpillClosedError());
    const pending = work();
    this.#pending.add(pending);
    void pending.finally(() => this.#pending.delete(pending)).catch(() => undefined);
    return pending;
  }
  append(previous: JsonBlob, bytes: Uint8Array): Promise<JsonBlob> {
    return this.#track(() => this.#append(previous, bytes));
  }
  async #append(previous: JsonBlob, bytes: Uint8Array): Promise<JsonBlob> {
    this.io.checkAborted();
    const d = this.#wireData;
    d.bytes = bytes;
    this.#offsets[1] = bytes.length;
    try {
      let page = this.io.pages.encode(
        { queryId: this.io.context.queryId, run: 0, partition: 0, sequence: this.#data.rows },
        [this.#column],
        1,
      );
      if (!page) {
        await this.io.need(this.io.pages.growthFor([this.#column], 1));
        page = this.io.pages.encode(
          { queryId: this.io.context.queryId, run: 0, partition: 0, sequence: this.#data.rows },
          [this.#column],
          1,
        );
      }
      if (!page) throw new Error("JSON blob page was not admitted");
      const ref = await this.files.appendRef(this.#data, page, 1);
      const head = await this.files.appendRef(
        this.#index,
        this.#directory.encode(previous.head, ref),
        1,
      );
      return {
        head,
        pages: previous.pages + 1,
        maximum: Math.max(previous.maximum, page.length),
        bytes: previous.bytes + bytes.length,
      };
    } finally {
      d.bytes = new Uint8Array(0);
      this.#offsets[1] = 0;
    }
  }
  /** Reverse only directory nodes. The payload never moves while a value is sealed. */
  seal(blob: JsonBlob): Promise<JsonBlob> {
    return this.#track(() => this.#seal(blob));
  }
  async #seal(blob: JsonBlob): Promise<JsonBlob> {
    let cursor = blob.head,
      head: SpillPageRef | null = null,
      remaining = blob.pages;
    while (cursor) {
      if (remaining-- <= 0) throw new SpillCorruptionError("Cyclic JSON blob");
      const item = await this.#directory.read(this.#index, cursor, blob.maximum);
      head = await this.files.appendRef(this.#index, this.#directory.encode(head, item.data), 1);
      cursor = item.previous;
    }
    if (remaining !== 0) throw new SpillCorruptionError("Truncated JSON blob");
    return { ...blob, head };
  }
  read(blob: JsonBlob, visit: (bytes: Uint8Array) => Promise<void>): Promise<void> {
    return this.#track(() => this.#read(blob, visit));
  }
  async #read(blob: JsonBlob, visit: (bytes: Uint8Array) => Promise<void>): Promise<void> {
    let cursor = blob.head,
      remaining = blob.pages,
      total = 0;
    while (cursor) {
      this.io.checkAborted();
      const paced = this.io.pace?.();
      if (paced) await paced;
      if (remaining-- <= 0) throw new SpillCorruptionError("Cyclic JSON blob");
      const item = await this.#directory.read(this.#index, cursor, blob.maximum);
      const page = await this.io.readRef(this.#data, item.data);
      try {
        const decoded = await this.io.decode(page, [BYTES]),
          column = decoded.columns[0];
        if (decoded.rows !== 1 || column?.data.layout !== "varlen")
          throw new SpillCorruptionError("Invalid JSON blob page");
        const data = column.data,
          part = data.bytes.subarray(data.offsets[0] ?? 0, data.offsets[1] ?? 0);
        total += part.length;
        await visit(part);
      } finally {
        this.io.releasePage(page);
      }
      cursor = item.previous;
    }
    if (remaining !== 0 || total !== blob.bytes)
      throw new SpillCorruptionError("Invalid JSON blob length");
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      try {
        await Promise.allSettled([...this.#pending]);
        await this.files.closeAll();
      } finally {
        this.#directory.detach();
        this.io.pool.free(META);
      }
    })());
  }
}
