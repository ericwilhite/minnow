/** Native page reconstruction from a bounded portable block; never materializes a large value. */
import type { ColumnChunk, GroupRef, PageEntry } from "../format/manifest.js";
import { encodeGroupRoot } from "../format/manifest-codec.js";
import { encodeDirectory } from "../page/directory.js";
import {
  ByteBounds,
  PageScratch,
  encodeBoolPage,
  encodeFixed16Page,
  encodeFloatPage,
  encodeInt128Page,
  encodeInt32Page,
  encodeInt64Page,
  encodeIntervalPage,
  encodeVarlenPage,
  type EncodedPage,
} from "../page/encode.js";
import { chunkTagOf, ValueTag } from "../page/format.js";
import { encodePointer, LARGE_VALUE_BYTES, VALUE_PART_BYTES } from "../page/large-value.js";
import { DistinctSketch } from "../page/sketch.js";
import type {
  ByteBudget,
  ExtentBuilder,
  ExtentReader,
  ExtentWriter,
  PendingBlob,
} from "../storage/extents.js";
import { layoutOf, type PhysicalType } from "../types.js";
import { allocateData, type FlatVector, type VectorData } from "../vector/vector.js";
import {
  SnapshotColumnBytes,
  SnapshotAccount,
  snapshotCheck,
  snapshotPace,
  type SnapshotStagedColumn,
} from "./snapshot-stage.js";

/** Caller reserves 32KiB before constructing a <=256-row scalar vector and its decode scratch. */
export function snapshotFlat(type: PhysicalType, rows: number, bytes = 0): FlatVector {
  let data: VectorData;
  if (layoutOf(type) === "int64")
    data = { layout: "int64", values: { repr: "int64", values: new BigInt64Array(rows) } };
  else if (type.kind === "interval")
    data = {
      layout: "interval",
      months: new Int32Array(rows),
      days: new Int32Array(rows),
      micros: { repr: "int64", values: new BigInt64Array(rows) },
    };
  else if (type.kind === "decimal")
    data = {
      layout: "decimal",
      coefficients: { repr: "int128", limbs: new Uint32Array(rows * 4) },
      scale: type.scale === null ? null : Math.max(0, type.scale),
      scales: type.scale === null ? new Uint16Array(rows) : null,
      specials: new Uint8Array(rows),
    };
  else if (layoutOf(type) === "varlen")
    data = { layout: "varlen", bytes: new Uint8Array(bytes), offsets: new Int32Array(rows + 1) };
  else data = allocateData(type, rows);
  return {
    form: "flat",
    type,
    length: rows,
    validity: new Uint32Array(Math.ceil(rows / 32)),
    data,
  };
}
/** Populate strongest lanes without a JS Number round trip for float or int64 words. */
export async function fillSnapshotFlat(
  column: SnapshotStagedColumn,
  source: SnapshotColumnBytes,
  from: number,
  to: number,
  flat: FlatVector,
  record: Uint8Array,
): Promise<void> {
  const data = flat.data,
    view = new DataView(record.buffer, record.byteOffset, record.byteLength);
  let cursor = 0;
  for (let row = from; row < to; row++) {
    await snapshotPace(column.work);
    const index = row - from,
      length = column.lengths[row] ?? 0xffffffff;
    if (length !== 0xffffffff) {
      if (flat.validity !== null)
        flat.validity[index >>> 5] = (flat.validity[index >>> 5] ?? 0) | (1 << (index & 31));
      const offset = column.offsets[row] ?? 0;
      if (data.layout === "varlen") {
        snapshotCheck(
          cursor + length <= data.bytes.length,
          "Portable key/value output exceeds its charged buffer",
        );
        await source.fill(offset, data.bytes.subarray(cursor, cursor + length));
        cursor += length;
      } else {
        await source.fill(offset, record.subarray(0, length));
        switch (data.layout) {
          case "bool":
            data.values[index] = record[0] ?? 0;
            break;
          case "i32":
            data.values[index] = view.getInt32(0, true);
            break;
          case "f32":
          case "f64":
            new Uint8Array(
              data.values.buffer,
              data.values.byteOffset + index * data.values.BYTES_PER_ELEMENT,
              length,
            ).set(record.subarray(0, length));
            break;
          case "int64":
            snapshotCheck(data.values.repr === "int64", "Expected exact int64 target");
            new Uint8Array(data.values.values.buffer, index * 8, 8).set(record.subarray(0, 8));
            break;
          case "fixed16":
            data.bytes.set(record.subarray(0, 16), index * 16);
            break;
          case "interval":
            data.months[index] = view.getInt32(0, true);
            data.days[index] = view.getInt32(4, true);
            snapshotCheck(data.micros.repr === "int64", "Expected exact interval target");
            new Uint8Array(data.micros.values.buffer, index * 8, 8).set(record.subarray(8, 16));
            break;
          case "decimal":
            snapshotCheck(data.coefficients.repr === "int128", "Expected exact decimal target");
            new Uint8Array(data.coefficients.limbs.buffer, index * 16, 16).set(
              record.subarray(0, 16),
            );
            if (data.scales !== null) data.scales[index] = view.getUint16(16, true);
            if (data.specials !== null) data.specials[index] = record[18] ?? 0;
            break;
        }
      }
    }
    if (data.layout === "varlen") data.offsets[index + 1] = cursor;
  }
}
function encodeFixed(flat: FlatVector, scratch: PageScratch, sketch: DistinctSketch): EncodedPage {
  const d = flat.data,
    valid = flat.validity,
    rows = flat.length;
  switch (d.layout) {
    case "bool":
      return encodeBoolPage(d.values, valid, rows, sketch, scratch);
    case "i32":
      return encodeInt32Page(d.values, valid, rows, sketch, scratch);
    case "f32":
    case "f64":
      return encodeFloatPage(d.values, valid, rows, sketch, scratch);
    case "int64":
      snapshotCheck(d.values.repr === "int64", "Expected exact int64 page");
      return encodeInt64Page(
        new Int32Array(d.values.values.buffer),
        valid,
        rows,
        ValueTag.i64,
        sketch,
        scratch,
      );
    case "fixed16":
      return encodeFixed16Page(d.bytes, valid, rows, sketch, new ByteBounds(), scratch);
    case "interval":
      snapshotCheck(d.micros.repr === "int64", "Expected exact interval page");
      return encodeIntervalPage(
        d.months,
        d.days,
        new Int32Array(d.micros.values.buffer),
        valid,
        rows,
        sketch,
        scratch,
      );
    case "decimal":
      snapshotCheck(d.coefficients.repr === "int128", "Expected exact decimal page");
      return encodeInt128Page(
        d.coefficients.limbs,
        valid,
        rows,
        { scales: d.scales, specials: d.specials },
        null,
        scratch,
        new Int32Array(rows * 2),
      );
    case "varlen":
      throw new TypeError("Varlen values use streamed native pages");
  }
}

/** <=16MiB per column, except one oversized row; boundaries identical across all columns. */
export function snapshotBlockEnd(
  columns: readonly SnapshotStagedColumn[],
  from: number,
  rows: number,
): number {
  let end = rows;
  for (const column of columns) {
    let size = 0,
      row = from;
    for (; row < end; row++) {
      const length = column.lengths[row] ?? 0xffffffff;
      const next = (length === 0xffffffff ? 0 : length) + 256;
      if (row > from && size + next > 16 * 1024 * 1024) break;
      size += next;
    }
    end = Math.min(end, row);
  }
  return end;
}
export class SnapshotNativeBuilder {
  readonly account: SnapshotAccount;
  readonly scratch: PageScratch;
  readonly record: Uint8Array;
  readonly inline: Uint8Array;
  readonly part: Uint8Array;
  readonly zeros: Uint8Array;
  constructor(
    readonly writer: ExtentWriter,
    readonly space: ExtentReader,
    readonly budget: ByteBudget,
    readonly retained: SnapshotAccount,
  ) {
    this.account = new SnapshotAccount(budget);
    // Raw part, <=64KiB inline value, strongest vector and bounded directory/codec coexistence.
    this.account.take((1 << 20) + (1 << 16) + (1 << 17));
    this.record = new Uint8Array(64);
    this.inline = new Uint8Array(LARGE_VALUE_BYTES);
    this.part = new Uint8Array(VALUE_PART_BYTES);
    this.zeros = new Uint8Array(8);
    this.scratch = new PageScratch((grown, old) => {
      this.account.take(grown);
      this.account.free(old);
    });
  }
  async #append(
    funding: SnapshotAccount,
    builder: ExtentBuilder,
    bytes: Uint8Array,
    aligned = true,
  ): Promise<PendingBlob> {
    funding.take(256);
    if (aligned && (builder.length & 7) !== 0)
      await this.#append(funding, builder, this.zeros.subarray(0, 8 - (builder.length & 7)), false);
    if (builder.length + bytes.length > builder.capacity)
      builder.extend(Math.max(bytes.length, 65536));
    const [blob] = await builder.append([bytes]);
    snapshotCheck(blob !== undefined, "Missing native append result");
    return blob;
  }
  async group(
    columns: readonly SnapshotStagedColumn[],
    from: number,
    to: number,
    firstRowId: number,
    groupId: number,
  ): Promise<{ group: GroupRef; chunks: readonly ColumnChunk[]; release(): void }> {
    const local = new SnapshotAccount(this.budget);
    local.take(65536 + columns.length * 2048);
    try {
      const estimate = columns.reduce((sum, col) => {
        for (let row = from; row < to; row++)
          sum += (col.lengths[row] === 0xffffffff ? 0 : (col.lengths[row] ?? 0)) + 256;
        return sum + 4096;
      }, 8);
      snapshotCheck(Number.isSafeInteger(estimate), "Native group estimate overflow");
      this.retained.take(1024); // two sealed extents + writer/reference bookkeeping through publish
      const builder = this.writer.open(estimate),
        chunks: Array<Omit<ColumnChunk, "data" | "directory">> = [],
        pending: Array<{ start: number; end: number; directory: PendingBlob }> = [];
      for (const column of columns) {
        await snapshotPace(column.work);
        if ((builder.length & 7) !== 0)
          await this.#append(
            local,
            builder,
            this.zeros.subarray(0, 8 - (builder.length & 7)),
            false,
          );
        const start = builder.length,
          source = new SnapshotColumnBytes(column, this.space),
          pages: PageEntry[] = [];
        let nulls = 0;
        try {
          if (layoutOf(column.column.type) !== "varlen") {
            const flat = snapshotFlat(column.column.type, to - from);
            await fillSnapshotFlat(column, source, from, to, flat, this.record);
            const encoded = encodeFixed(flat, this.scratch, new DistinctSketch());
            const blob = await this.#append(local, builder, encoded.bytes);
            nulls = encoded.nullCount;
            pages.push({
              offset: blob.offset - start,
              length: blob.length,
              checksum: blob.checksum,
              firstRow: 0,
              rowCount: to - from,
              nullCount: nulls,
              encoding: encoded.encoding,
              min: encoded.min,
              max: encoded.max,
            });
          } else {
            for (let row = from; row < to; row++) {
              await snapshotPace(column.work);
              const length = column.lengths[row] ?? 0xffffffff,
                offset = column.offsets[row] ?? 0;
              const valid = length === 0xffffffff ? new Uint32Array(1) : null;
              let bytes: Uint8Array, region: { offset: number; length: number } | undefined;
              if (length === 0xffffffff) {
                bytes = this.inline.subarray(0, 0);
                nulls++;
              } else if (length <= LARGE_VALUE_BYTES) {
                bytes = this.inline.subarray(0, length);
                await source.fill(offset, bytes);
              } else {
                const largeStart = builder.length,
                  checksums: number[] = [];
                for (let at = 0; at < length; at += VALUE_PART_BYTES) {
                  const part = this.part.subarray(0, Math.min(VALUE_PART_BYTES, length - at));
                  await source.fill(offset + at, part);
                  checksums.push((await this.#append(local, builder, part, false)).checksum);
                }
                region = { offset: largeStart - start, length };
                bytes = encodePointer({ offset: largeStart - start, length, checksums });
              }
              const offsets = new Int32Array([0, bytes.length]);
              let page = encodeVarlenPage(
                bytes,
                offsets,
                valid,
                1,
                null,
                new ByteBounds(),
                this.scratch,
                region === undefined
                  ? null
                  : { bitmap: new Uint32Array([1]), prefix: () => this.inline.subarray(0, 0) },
              );
              // Unknown bounds remain sound, including arrays/JSON and streamed large bytes.
              page = { ...page, min: null, max: null };
              const blob = await this.#append(local, builder, page.bytes);
              pages.push({
                offset: blob.offset - start,
                length: blob.length,
                checksum: blob.checksum,
                firstRow: row - from,
                rowCount: 1,
                nullCount: page.nullCount,
                encoding: page.encoding,
                min: null,
                max: null,
                ...(region === undefined ? {} : { largeRegion: region }),
              });
            }
          }
          const directory = encodeDirectory({
            tag: chunkTagOf(column.column.type),
            rowCount: to - from,
            scale:
              column.column.type.kind === "decimal"
                ? column.column.type.scale === null
                  ? null
                  : Math.max(0, column.column.type.scale)
                : 0,
            hasSpecials: column.column.type.kind === "decimal",
            pages,
            dictionary: null,
            sketch: null,
          });
          const blob = await this.#append(local, builder, directory);
          pending.push({ start, end: builder.length, directory: blob });
          chunks.push({
            columnId: column.column.id,
            type: column.column.type,
            statistics: { nullCount: nulls, min: null, max: null, dictionarySize: null },
            distinctSketch: null,
          });
        } finally {
          source.close();
        }
      }
      // Empty-column groups still need a reachable extent for their root only.
      const sealed = await builder.seal();
      const complete: ColumnChunk[] = chunks.map((chunk, i) => {
        const range = pending[i];
        snapshotCheck(range !== undefined && sealed !== null, "Missing native chunk extent");
        return {
          ...chunk,
          data: { extent: sealed.extent, offset: range.start, length: range.end - range.start },
          directory: { extent: sealed.extent, ...range.directory },
        };
      });
      const bytes = encodeGroupRoot({
        rowCount: to - from,
        rowIds: { kind: "dense" },
        columns: complete,
      });
      const written = await this.writer.write([bytes]),
        root = written.blobs[0];
      snapshotCheck(root !== undefined, "Missing native group root");
      return {
        group: {
          groupId,
          firstRowId,
          lastRowId: firstRowId + to - from - 1,
          rowCount: to - from,
          liveRowCount: to - from,
          root,
          deletions: null,
        },
        chunks: complete,
        release: () => local.close(),
      };
    } catch (error) {
      local.close();
      throw error;
    }
  }
  close(): void {
    this.account.close();
  }
}
