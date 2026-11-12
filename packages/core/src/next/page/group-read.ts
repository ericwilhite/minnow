/** A scan-owned metadata hold; it never installs roots on the write path's snapshot. */
import type { BlobRef, ByteBudget, ExtentReader } from "../storage/extents.js";
import type { ColumnChunk, GroupRoot } from "../format/manifest.js";
import { MetadataReader, type MaybePromise } from "../format/metadata-reader.js";
import type { Held } from "../format/reachable.js";
import type { QueryRoots, ScanRootFacts } from "./query-roots.js";
import { ChunkReader } from "./reader.js";
import type { ColumnStatistics } from "../types.js";

export class GroupRead {
  #held: Held<GroupRoot> | null = null;
  #pending: Promise<GroupRoot> | null = null;
  #facts: Held<ScanRootFacts> | null = null;
  #preparing: Promise<void> | null = null;
  #epoch = 0;
  readonly #readers: ChunkReader[] = [];
  #summaries = 0;
  constructor(
    readonly ref: BlobRef,
    readonly pages: ExtentReader,
    readonly budget: ByteBudget,
    readonly roots?: QueryRoots,
    readonly tableId = 0,
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
  prepare(columns: readonly number[]): MaybePromise<void> {
    if (this.#held !== null) return;
    if (this.#preparing !== null) return this.#preparing.then(() => this.prepare(columns));
    if (this.#facts !== null) {
      if (this.roots?.covers(this.tableId, columns)) return;
      const root = this.root();
      return root instanceof Promise ? root.then(() => undefined) : undefined;
    }
    if (this.roots === undefined) {
      const root = this.root();
      return root instanceof Promise ? root.then(() => undefined) : undefined;
    }
    const epoch = this.#epoch;
    const keep = (facts: Held<ScanRootFacts>) => {
      if (epoch !== this.#epoch) {
        facts.release();
        throw new Error("Group metadata read ended");
      }
      this.#facts = facts;
    };
    const got = this.roots.read(this.tableId, this.ref, this.pages, this.budget, columns);
    if (!(got instanceof Promise)) return keep(got);
    const pending = got.then(keep).finally(() => {
      if (this.#preparing === pending) this.#preparing = null;
    });
    this.#preparing = pending;
    return pending;
  }
  chunk(id: number): ColumnChunk | null {
    const facts = this.#held ?? this.#facts;
    if (facts === null) throw new TypeError("Prepare root before reading a group chunk");
    return facts.value.columns.find((column) => column.columnId === id) ?? null;
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
    this.#preparing = null;
    this.#facts?.release();
    this.#facts = null;
    for (const reader of this.#readers) reader.close();
    this.#readers.length = 0;
    if (this.#summaries > 0) this.budget.free(this.#summaries);
    this.#summaries = 0;
    this.#held?.release();
    this.#held = null;
  }
}
