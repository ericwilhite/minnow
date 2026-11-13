/** Pinned logical export. No source writes, native addresses, whole-table or whole-value copy. */
import { snapshotInput } from "./snapshot-iterator.js";
import { DURABLE_CODECS } from "../durable-format.js";
import type { ColumnDef } from "../access/catalog.js";
import { MAX_LSN, type Segment, type Snapshot } from "../access/snapshot.js";
import {
  SnapshotBudgetError,
  SnapshotFormatError,
  SnapshotKind,
  SnapshotStreamWriter,
  SNAPSHOT_PAYLOAD_BYTES,
  snapshotCrc32,
  snapshotNumbers,
  type SnapshotSink,
  type SnapshotWork,
} from "../format/snapshot.js";
import { ChunkReader } from "../page/reader.js";
import type { ByteBudget } from "../storage/extents.js";
import { layoutOf, type PhysicalType } from "../types.js";
import { pauseIfDue, sortInSlices } from "../task.js";
import { allocateData, type FlatVector, type Vector, type VectorData } from "../vector/vector.js";
import { sourceSlot, MEMORY_CHUNK_ROWS } from "./columns.js";
import {
  encodeSnapshotColumnHeader,
  SNAPSHOT_BLOCK_ROWS,
  snapshotFixedValue,
  snapshotValid,
  snapshotValueWidth,
} from "./snapshot-column.js";

export class SnapshotScopeError extends Error {
  readonly code = "MINNOW_SNAPSHOT_SCOPE";
  constructor(message: string) {
    super(message);
    this.name = "SnapshotScopeError";
  }
}
function take(budget: ByteBudget, bytes: number): void {
  if (!budget.allocate(bytes)) throw new SnapshotBudgetError(bytes);
}
async function pace(work: SnapshotWork): Promise<void> {
  const pending = pauseIfDue(work);
  if (pending !== undefined) await pending;
}
/** Fixed128-byte scratch credit covers every strongest stored scalar layout and its wrappers. */
function scalar(type: PhysicalType): FlatVector {
  let data: VectorData;
  if (layoutOf(type) === "int64")
    data = { layout: "int64", values: { repr: "int64", values: new BigInt64Array(1) } };
  else if (type.kind === "interval")
    data = {
      layout: "interval",
      months: new Int32Array(1),
      days: new Int32Array(1),
      micros: { repr: "int64", values: new BigInt64Array(1) },
    };
  else if (type.kind === "decimal")
    data = {
      layout: "decimal",
      scale: type.scale === null ? null : Math.max(0, type.scale),
      scales: type.scale === null ? new Uint16Array(1) : null,
      specials: new Uint8Array(1),
      coefficients: { repr: "int128", limbs: new Uint32Array(4) },
    };
  else data = allocateData(type, 1);
  return { form: "flat", type, length: 1, validity: new Uint32Array(1), data };
}
class Source {
  #reader: ChunkReader | null = null;
  #vector: Vector | null = null;
  #base = 0;
  #page = -1;
  readonly #slot: { flat: FlatVector | null; index: number } = { flat: null, index: -1 };
  private constructor(readonly scratch: FlatVector) {}
  static async open(
    snapshot: Snapshot,
    segment: Segment,
    def: ColumnDef,
    first: number,
    budget: ByteBudget,
    work: SnapshotWork,
  ): Promise<Source> {
    const source = new Source(scalar(def.type));
    try {
      let stored;
      if (segment.kind === "group") {
        await segment.root();
        stored = segment.chunk(def.id);
        if (stored === null)
          source.#vector = {
            form: "constant",
            type: def.type,
            length: segment.rowCount,
            value: def.missing ?? source.scratch,
          };
      } else {
        if (segment.portableChunk === undefined)
          throw new SnapshotScopeError(
            "This memory reader does not support portable large-value streaming",
          );
        const input = await segment.portableChunk(def.id, Math.floor(first / MEMORY_CHUNK_ROWS));
        source.#base = input.firstRow;
        if (input.kind === "resident") source.#vector = input.vector;
        else stored = input.chunk;
      }
      if (stored !== undefined && stored !== null)
        source.#reader = await ChunkReader.open(snapshot.pages, stored, {
          budget,
          once: true,
          task: work,
          ...(work.signal === undefined ? {} : { signal: work.signal }),
        });
      return source;
    } catch (error) {
      source.close();
      throw error;
    }
  }
  async #load(row: number): Promise<void> {
    const reader = this.#reader;
    if (reader === null) return;
    const [page] = reader.pagesInRange(row, row + 1);
    if (page !== this.#page) {
      reader.release();
      await reader.load([page]);
      this.#page = page;
    }
  }
  async fixed(position: number): Promise<{ flat: FlatVector; index: number }> {
    const row = position - this.#base,
      vector = this.#vector;
    if (vector !== null) {
      sourceSlot(vector, row, this.#slot);
      return { flat: this.#slot.flat, index: this.#slot.index };
    }
    const reader = this.#reader;
    if (reader === null) throw new SnapshotFormatError("No portable column source");
    await this.#load(row);
    reader.decodeRange(row, row + 1, this.scratch);
    return { flat: this.scratch, index: snapshotValid(this.scratch, 0) ? 0 : -1 };
  }
  async length(position: number): Promise<number | null> {
    const row = position - this.#base;
    if (this.#vector !== null) {
      sourceSlot(this.#vector, row, this.#slot);
      if (this.#slot.index < 0) return null;
      const data = this.#slot.flat.data;
      if (data.layout !== "varlen")
        throw new SnapshotFormatError("Portable varlen source has wrong layout");
      return (data.offsets[this.#slot.index + 1] ?? 0) - (data.offsets[this.#slot.index] ?? 0);
    }
    await this.#load(row);
    if (this.#reader === null) throw new SnapshotFormatError("No portable value source");
    return this.#reader.valueLength(row);
  }
  async *parts(position: number): AsyncGenerator<Uint8Array> {
    const row = position - this.#base;
    if (this.#vector !== null) {
      sourceSlot(this.#vector, row, this.#slot);
      const { flat, index } = this.#slot;
      if (index < 0 || flat.data.layout !== "varlen")
        throw new SnapshotFormatError("Portable dictionary definition is null/not bytes");
      yield flat.data.bytes.subarray(
        flat.data.offsets[index] ?? 0,
        flat.data.offsets[index + 1] ?? 0,
      );
    } else {
      await this.#load(row);
      if (this.#reader === null) throw new SnapshotFormatError("No portable value stream");
      yield* this.#reader.valueParts(row);
    }
  }
  close(): void {
    this.#reader?.close();
    this.#reader = null;
    this.#vector = null;
    this.#slot.flat = null;
  }
}

/** Ownership of snapshot transfers to this call, including errors/cancel before the first byte.
 * catalog yields the matching portable catalog1 bytes, borrowed until advance; stage4 captures
 * its catalog/sequence scope at pin time. Output backpressure is sink.write settlement.
 */
export async function exportPinnedSnapshot(
  snapshot: Snapshot,
  id: Uint8Array,
  catalog: AsyncIterable<Uint8Array>,
  sink: SnapshotSink,
  budget: ByteBudget,
  work: SnapshotWork = {},
): Promise<void> {
  // One payload window, row-position/length vectors, scalar+record scratch and bounded wrappers.
  const credit = SNAPSHOT_PAYLOAD_BYTES + 8192;
  // Statement views belong to their statement and cannot transfer pin ownership here.
  if (snapshot.bound > MAX_LSN)
    throw new SnapshotScopeError("Portable export requires an owned committed snapshot pin");
  let funded = false,
    writer: SnapshotStreamWriter | undefined;
  try {
    if (snapshot.released)
      throw new SnapshotScopeError("Portable export requires an owned committed snapshot pin");
    take(budget, credit);
    funded = true;
    const window = new Uint8Array(SNAPSHOT_PAYLOAD_BYTES),
      rows = new Uint16Array(SNAPSHOT_BLOCK_ROWS),
      lengths = new Uint32Array(SNAPSHOT_BLOCK_ROWS),
      record = new Uint8Array(64);
    writer = await SnapshotStreamWriter.open(
      { id, sourceLsn: snapshot.observedLsn },
      sink,
      budget,
      work,
    );
    await writer.frame(SnapshotKind.catalogBegin, 1, 0, record.subarray(0, 0));
    let catalogBytes = 0,
      catalogCrc = 0,
      part = 1;
    for await (const chunk of snapshotInput(catalog, work.signal)) {
      await pace(work);
      for (let from = 0; from < chunk.length; from += SNAPSHOT_PAYLOAD_BYTES) {
        const bytes = chunk.subarray(from, from + SNAPSHOT_PAYLOAD_BYTES);
        await writer.frame(SnapshotKind.catalogPart, 1, part++, bytes);
        catalogBytes += bytes.length;
        catalogCrc = snapshotCrc32(bytes, catalogCrc);
      }
    }
    snapshotNumbers([catalogBytes, 0], record.subarray(0, 16));
    new DataView(record.buffer).setUint32(8, catalogCrc, true);
    await writer.frame(SnapshotKind.catalogEnd, 1, part, record.subarray(0, 16));
    const output = writer;
    let object = 1;
    for (const def of snapshot.catalog.tables) {
      await pace(work);
      const columnCredit = def.columns.length * 16 + 256;
      take(budget, columnCredit);
      try {
        const columns = [...def.columns];
        await sortInSlices(columns, (a, b) => a.id - b.id, work);
        const table = await snapshot.table(def.id, { budget }),
          tableObject = ++object;
        snapshotNumbers([def.id, def.schemaVersion, columns.length], record.subarray(0, 24));
        await output.frame(SnapshotKind.tableBegin, tableObject, 0, record.subarray(0, 24));
        let emitted = 0;
        if (table.portableSegments === undefined)
          throw new SnapshotScopeError(
            "This table reader does not support bounded portable traversal",
          );
        for await (const segment of table.portableSegments(work)) {
          const deleted = await segment.deletions();
          for (let first = 0; first < segment.rowCount; first += SNAPSHOT_BLOCK_ROWS) {
            await pace(work);
            let count = 0;
            for (
              let row = first;
              row < Math.min(segment.rowCount, first + SNAPSHOT_BLOCK_ROWS);
              row++
            )
              if (deleted?.isDeleted(row) !== true) rows[count++] = row;
            if (count === 0) continue;
            for (const column of columns) {
              const source = await Source.open(snapshot, segment, column, first, budget, work);
              try {
                const width = snapshotValueWidth(column.type),
                  varlen = width === 0;
                let encodedBytes = 16;
                for (let i = 0; i < count; i++) {
                  const position = rows[i] ?? 0;
                  if (varlen) {
                    const length = await source.length(position);
                    if (
                      length !== null &&
                      (!Number.isInteger(length) || length < 0 || length > 64 * 1024 * 1024)
                    )
                      throw new SnapshotFormatError("Portable value exceeds stored value limit");
                    lengths[i] = length ?? 0xffffffff;
                    encodedBytes += length === null ? 1 : length + 10;
                  } else {
                    const slot = await source.fixed(position);
                    lengths[i] = slot.index < 0 ? 0 : 1;
                    encodedBytes += 1 + (slot.index < 0 ? 0 : width);
                  }
                }
                const columnObject = ++object;
                snapshotNumbers(
                  [def.id, column.id, def.schemaVersion, emitted, count, encodedBytes],
                  record.subarray(0, 48),
                );
                await output.frame(
                  SnapshotKind.columnBegin,
                  columnObject,
                  0,
                  record.subarray(0, 48),
                );
                let used = 0,
                  partIndex = 1;
                const flush = async () => {
                  if (used !== 0) {
                    await output.frame(
                      SnapshotKind.columnPart,
                      columnObject,
                      partIndex++,
                      window.subarray(0, used),
                    );
                    used = 0;
                  }
                };
                const append = async (bytes: Uint8Array) => {
                  for (let at = 0; at < bytes.length;) {
                    const take = Math.min(window.length - used, bytes.length - at);
                    window.set(bytes.subarray(at, at + take), used);
                    used += take;
                    at += take;
                    if (used === window.length) await flush();
                  }
                };
                encodeSnapshotColumnHeader(column.type, count, record.subarray(0, 16));
                await append(record.subarray(0, 16));
                for (let i = 0; i < count; i++) {
                  await pace(work);
                  const position = rows[i] ?? 0,
                    length = lengths[i] ?? 0;
                  if (!varlen) {
                    if (length === 0) {
                      record[0] = 0;
                      await append(record.subarray(0, 1));
                    } else {
                      const slot = await source.fixed(position);
                      const size = snapshotFixedValue(slot.flat, slot.index, record);
                      await append(record.subarray(0, size));
                    }
                  } else if (length === 0xffffffff) {
                    record[0] = 0;
                    await append(record.subarray(0, 1));
                  } else {
                    record[0] = DURABLE_CODECS.snapshotValue.define;
                    new DataView(record.buffer).setUint32(1, length, true);
                    await append(record.subarray(0, 5));
                    let copied = 0;
                    for await (const bytes of source.parts(position)) {
                      await pace(work);
                      copied += bytes.length;
                      await append(bytes);
                    }
                    if (copied !== length)
                      throw new SnapshotFormatError("Pinned value changed its byte length");
                    record.fill(0, 0, 5);
                    record[0] = DURABLE_CODECS.snapshotValue.reference;
                    await append(record.subarray(0, 5));
                  }
                }
                await flush();
                snapshotNumbers([encodedBytes, count], record.subarray(0, 16));
                await output.frame(
                  SnapshotKind.columnEnd,
                  columnObject,
                  partIndex,
                  record.subarray(0, 16),
                );
              } finally {
                source.close();
              }
            }
            emitted += count;
          }
        }
        if (emitted !== table.liveRowCount)
          throw new SnapshotFormatError("Pinned table row count mismatch");
        snapshotNumbers([emitted], record.subarray(0, 8));
        await output.frame(SnapshotKind.tableEnd, tableObject, 1, record.subarray(0, 8));
      } finally {
        budget.free(columnCredit);
      }
    }
    await writer.finish();
  } finally {
    writer?.close();
    if (funded) budget.free(credit);
    snapshot.release();
  }
}
