/** A scan-owned metadata hold; it never installs roots on the write path's snapshot. */
import type { BlobRef, ByteBudget, ExtentReader } from "../storage/extents.js";
import type { ColumnChunk, GroupRoot } from "../format/manifest.js";
import { MetadataReader, type MaybePromise } from "../format/metadata-reader.js";
import type { Held } from "../format/reachable.js";
import { ChunkReader } from "./reader.js";
import type { ColumnStatistics } from "../types.js";

export class GroupRead {
  #held: Held<GroupRoot> | null = null;
  #pending: Promise<GroupRoot> | null = null;
  #epoch = 0;
  readonly #readers: ChunkReader[] = [];
  #summaries = 0;
  constructor(
    readonly ref: BlobRef,
    readonly pages: ExtentReader,
    readonly budget: ByteBudget,
  ) {}
  root(): MaybePromise<GroupRoot> {
    if (this.#held !== null) return this.#held.value;
    if (this.#pending !== null) return this.#pending;
    const epoch = this.#epoch;
    const keep = (held: Held<GroupRoot>) => {
      if (epoch !== this.#epoch) {
        held.release();
        throw new Error("Group metadata read ended");
      }
      this.#held = held;
      return held.value;
    };
    const got = new MetadataReader(this.pages).groupRoot(this.ref, this.budget);
    if (!(got instanceof Promise)) return keep(got);
    const pending = got.then(keep).finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return pending;
  }
  chunk(id: number): ColumnChunk | null {
    if (this.#held === null) throw new TypeError("Call root before reading a group chunk");
    return this.#held.value.columns.find((column) => column.columnId === id) ?? null;
  }
  summaries(
    id: number,
  ): MaybePromise<ReadonlyArray<ColumnStatistics & { firstRow: number; rowCount: number }>> {
    const chunk = this.chunk(id);
    if (chunk === null) return [];
    const epoch = this.#epoch;
    const keep = (reader: ChunkReader) => {
      if (epoch !== this.#epoch) {
        reader.close();
        throw new Error("Group metadata read ended");
      }
      this.#readers.push(reader);
      const bytes = 64 * reader.directory.pages.length;
      if (!this.budget.allocate(bytes))
        throw new RangeError("Group page summaries exceed admission");
      this.#summaries += bytes;
      return reader.directory.pages.map((page) => ({
        firstRow: page.firstRow,
        rowCount: page.rowCount,
        nullCount: page.nullCount,
        min: page.min,
        max: page.max,
        dictionarySize: null,
      }));
    };
    const reader = ChunkReader.open(this.pages, chunk, { budget: this.budget });
    return reader instanceof Promise ? reader.then(keep) : keep(reader);
  }
  release(): void {
    this.#epoch++;
    this.#pending = null;
    for (const reader of this.#readers) reader.close();
    this.#readers.length = 0;
    if (this.#summaries > 0) this.budget.free(this.#summaries);
    this.#summaries = 0;
    this.#held?.release();
    this.#held = null;
  }
}
