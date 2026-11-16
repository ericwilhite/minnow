/** Portable catalog1 = PCAT header + complete common MCAT + exact sequence horizons.
 * Lives inside multipart catalog frames. No native catalog tag, format or JSON reviver.
 */
import { DURABLE_FORMATS, requireHeader } from "../durable-format.js";
import { parseCharge, type Parsed } from "../format/manifest-codec.js";
import { SnapshotFormatError, snapshotNumbers, snapshotU53 } from "../format/snapshot.js";
import type { ManifestRoot } from "../format/manifest.js";
import type { ByteBudget } from "../storage/extents.js";
import { Catalog } from "./catalog.js";
import { CatalogBuffers } from "./catalog-storage.js";
import {
  CATALOG_RECORD_BYTES,
  catalogStream,
  catalogStreamByteLength,
  decodeCatalogStream,
  type CatalogStreamData,
} from "./catalog-stream.js";
import { snapshotCheck, snapshotTake } from "./snapshot-stage.js";
import { SnapshotScopeError } from "./snapshot-export.js";

export type SnapshotHorizon = ManifestRoot["sequences"][number];
const HEADER_BYTES = 24,
  SEQUENCE_BYTES = 16;
const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
/** Fund BigInt threshold construction BEFORE allocation. Normal SQL horizons stop at64bits;
 * wider native values get a bounded decimal workspace without an unconditional large minimum. */
function decimalBound(value: bigint, budget: ByteBudget): number {
  for (let bits = 64; bits <= 2 * CATALOG_RECORD_BYTES * 4; bits *= 2) {
    const charge = 256 + Math.ceil(bits / 8) * 4;
    snapshotTake(budget, charge);
    try {
      const threshold = 1n << BigInt(bits);
      if (value > -threshold && value < threshold)
        return Math.ceil(bits * Math.LOG10E * Math.LN2) + 2;
    } finally {
      budget.free(charge);
    }
  }
  throw new SnapshotScopeError("Sequence horizon exceeds portable catalog record capacity");
}
export function encodeSnapshotCatalogHeader(length: number, count: number, into: Uint8Array): void {
  snapshotCheck(into.length === HEADER_BYTES && length >= 16, "Invalid portable catalog header");
  into.fill(0);
  const view = new DataView(into.buffer, into.byteOffset, into.byteLength);
  view.setUint32(0, DURABLE_FORMATS.snapshotCatalog.magic, true);
  view.setUint16(4, 1, true);
  snapshotNumbers([length, count], into.subarray(8));
}
export function decodeSnapshotCatalogHeader(bytes: Uint8Array): { length: number; count: number } {
  snapshotCheck(bytes.length === HEADER_BYTES, "Invalid portable catalog header length");
  requireHeader("snapshotCatalog", bytes, SnapshotFormatError);
  const length = snapshotU53(bytes, 8),
    count = snapshotU53(bytes, 16);
  snapshotCheck(length >= 16, "Invalid MCAT stream length");
  return { length, count };
}
export function encodeSnapshotSequence(
  value: SnapshotHorizon,
  budget: ByteBudget,
): { bytes: Uint8Array; close(): void } {
  const bound = decimalBound(value.reservedThrough, budget),
    scratch = bound * 2 + 256;
  snapshotTake(budget, scratch);
  const buffers = new CatalogBuffers(budget);
  try {
    const decimal = value.reservedThrough.toString();
    if (decimal.length > CATALOG_RECORD_BYTES)
      throw new SnapshotScopeError("Sequence horizon exceeds portable catalog record capacity");
    const bytes = buffers.create(SEQUENCE_BYTES + decimal.length);
    snapshotNumbers([value.sequenceId], bytes.subarray(0, 8));
    const view = new DataView(bytes.buffer);
    view.setUint32(8, decimal.length, true);
    view.setUint32(12, 0, true);
    for (let i = 0; i < decimal.length; i++) bytes[SEQUENCE_BYTES + i] = decimal.charCodeAt(i);
    let closed = false;
    return {
      bytes,
      close() {
        if (closed) return;
        closed = true;
        buffers.close();
        budget.free(scratch);
      },
    };
  } catch (error) {
    buffers.close();
    budget.free(scratch);
    throw error;
  }
}
export function decodeSnapshotSequence(
  bytes: Uint8Array,
  budget: ByteBudget,
): Parsed<SnapshotHorizon> {
  requireHeader("snapshotSequence", bytes, SnapshotFormatError);
  snapshotCheck(bytes.length > SEQUENCE_BYTES, "Truncated portable sequence");
  const charge = parseCharge(bytes.length);
  snapshotTake(budget, charge);
  try {
    const sequenceId = snapshotU53(bytes, 0),
      length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(8, true);
    snapshotCheck(
      sequenceId > 0 && length <= CATALOG_RECORD_BYTES && length === bytes.length - SEQUENCE_BYTES,
      "Invalid portable sequence length/id",
    );
    for (const byte of bytes.subarray(SEQUENCE_BYTES))
      snapshotCheck(byte === 45 || (byte >= 48 && byte <= 57), "Invalid portable sequence ASCII");
    const decimal = text.decode(bytes.subarray(SEQUENCE_BYTES));
    snapshotCheck(/^(?:0|-?[1-9][0-9]*)$/u.test(decimal), "Noncanonical portable sequence horizon");
    return { value: { sequenceId, reservedThrough: BigInt(decimal) }, charge };
  } catch (error) {
    budget.free(charge);
    throw error;
  }
}
/** Borrowed parts remain charged until next()/return(). Caller closes the generator on error. */
export function* snapshotCatalogParts(
  catalog: CatalogStreamData,
  horizons: readonly SnapshotHorizon[],
  budget: ByteBudget,
): Generator<Uint8Array, void> {
  snapshotCheck(horizons.length === catalog.sequences.length, "Missing portable sequence horizons");
  const buffers = new CatalogBuffers(budget);
  try {
    const header = buffers.create(HEADER_BYTES);
    encodeSnapshotCatalogHeader(catalogStreamByteLength(catalog), horizons.length, header);
    yield header;
    buffers.release(header);
    yield* catalogStream(catalog, buffers);
    for (let i = 0; i < horizons.length; i++) {
      const horizon = horizons[i];
      snapshotCheck(
        horizon !== undefined && horizon.sequenceId === catalog.sequences[i]?.id,
        "Portable sequence order/identity mismatch",
      );
      const encoded = encodeSnapshotSequence(horizon, budget);
      try {
        yield encoded.bytes;
      } finally {
        encoded.close();
      }
    }
  } finally {
    buffers.close();
  }
}
/** Full logical graph is charged before decode; returned ownership survives encoded-byte release. */
export function decodeSnapshotCatalog(
  bytes: Uint8Array,
  budget: ByteBudget,
): Parsed<{ catalog: Catalog; horizons: readonly SnapshotHorizon[] }> {
  const header = decodeSnapshotCatalogHeader(bytes.subarray(0, HEADER_BYTES));
  snapshotCheck(
    header.length <= bytes.length - HEADER_BYTES &&
      header.count <=
        Math.floor((bytes.length - HEADER_BYTES - header.length) / (SEQUENCE_BYTES + 1)),
    "Truncated portable catalog scope",
  );
  const charge = parseCharge(bytes.length) * 4;
  snapshotTake(budget, charge);
  try {
    const catalog = Catalog.fromStored(
        decodeCatalogStream(bytes.subarray(HEADER_BYTES, HEADER_BYTES + header.length)),
        budget,
      ),
      horizons: SnapshotHorizon[] = [];
    snapshotCheck(
      header.count === catalog.sequences.length,
      "Missing/extra portable sequence horizons",
    );
    let at = HEADER_BYTES + header.length;
    for (let i = 0; i < header.count; i++) {
      snapshotCheck(at + SEQUENCE_BYTES <= bytes.length, "Truncated portable sequence header");
      const length = new DataView(bytes.buffer, bytes.byteOffset + at, SEQUENCE_BYTES).getUint32(
        8,
        true,
      );
      snapshotCheck(
        length <= bytes.length - at - SEQUENCE_BYTES,
        "Truncated portable sequence body",
      );
      const parsed = decodeSnapshotSequence(
        bytes.subarray(at, at + SEQUENCE_BYTES + length),
        budget,
      );
      try {
        snapshotCheck(
          parsed.value.sequenceId === catalog.sequences[i]?.id,
          "Duplicate/unresolved/out-of-order portable sequence",
        );
        horizons.push(parsed.value);
      } finally {
        budget.free(parsed.charge);
      }
      at += SEQUENCE_BYTES + length;
    }
    snapshotCheck(at === bytes.length, "Trailing portable catalog bytes");
    return { value: { catalog, horizons }, charge };
  } catch (error) {
    budget.free(charge);
    throw error;
  }
}
