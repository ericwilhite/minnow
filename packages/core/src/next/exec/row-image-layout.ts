/** Image-registry ABI budget. The native provider must fit this fragmented, scalar layout.
 * Admitted roots install the native provider; this module only describes image layout. */
import { OBJECT_OVERHEAD_BYTES } from "../memory-values.js";
import type { PhysicalType } from "../types.js";
import { dataBytes } from "../vector/vector.js";
import { SORT_ROW_VIEW_BYTES } from "./sort-row.js";
export const ROW_IMAGE_HELPER_SLOTS = 1;
export const ROW_IMAGE_FRAGMENT_BYTES = 65536;

/** The root registry directory's layout (owner/registry-directory.ts). */
export const REGISTRY_RECORD_BYTES = 128;
/** Records per leaf: a leaf holds one block of record IDs. */
export const REGISTRY_LEAF_RECORDS = 16;
export const REGISTRY_LEAF_BYTES = 32 + REGISTRY_LEAF_RECORDS * REGISTRY_RECORD_BYTES;
/**
 * Leaves kept resident. A native statement's per-row working set spans about a dozen record
 * blocks (descriptors, row images, fragments, following), so 8 leaves thrashed: each updated row
 * of a 2,000-row trigger UPDATE missed 9.6 leaves and paid 368 directory page reads and 174 spill
 * appends. 16 leave only the compulsory misses of new blocks (testing/registry-directory-work).
 */
export const REGISTRY_DIRECTORY_LEAVES = 16;
/**
 * A directory node: a 40-byte header (magic, its depth, its skip and up to 28 skipped digits) and
 * 16 child references of 16 bytes.
 */
export const REGISTRY_NODE_BYTES = 296;
/** Key digits (radix 16) from namespace to block: 2 kind, 14 owner, 13 block. */
export const REGISTRY_LEVELS = 29;
/** The directory's own state: its controller, signal, leaf table and file handle. */
const REGISTRY_CONTROL_BYTES = 4096;
/**
 * A directory's whole resident charge: its control, the root-to-leaf path buffer (at most one
 * node per key digit, plus one for a split's remainder), and each resident leaf (its bytes, its
 * record and its buffer object).
 */
export const REGISTRY_DIRECTORY_BYTES =
  REGISTRY_CONTROL_BYTES +
  (REGISTRY_LEVELS + 1) * REGISTRY_NODE_BYTES +
  OBJECT_OVERHEAD_BYTES +
  REGISTRY_DIRECTORY_LEAVES * (REGISTRY_LEAF_BYTES + 2 * OBJECT_OVERHEAD_BYTES);
/** The image registry's own control (owner/registry-images.ts `REGISTRY_IMAGE_BYTES`). */
const REGISTRY_IMAGE_CONTROL_BYTES = 2048;
/** One paged-directory cursor plus bounded encoded/decoded fragment buffers. */
export const ROW_IMAGE_FIXED_BYTES =
  REGISTRY_DIRECTORY_BYTES + REGISTRY_IMAGE_CONTROL_BYTES + 2 * ROW_IMAGE_FRAGMENT_BYTES;
export function rowImageMinimumBytes(types: readonly PhysicalType[]): number {
  return (
    ROW_IMAGE_FIXED_BYTES +
    2 * types.reduce((sum, type) => sum + SORT_ROW_VIEW_BYTES + dataBytes(type, 1) + 4, 0)
  );
}
/** Additional payload beyond the scalar fixed buffers. Exact allocation avoids resize doubles.
 * The provider prepays image payload plus binding-copy room BEFORE returning a lease. Default
 * copy room covers one unique copy of every OLD/NEW column; repeated parameter slots alias it.
 * Fixed directory/fragment I/O is already in minimumBytes. No row-count/depth multiplier. */
export function rowImageProgressBytes(
  imagePayloadBytes: number,
  bindingPayloadBytes = imagePayloadBytes,
): number {
  for (const bytes of [imagePayloadBytes, bindingPayloadBytes])
    if (!Number.isSafeInteger(bytes) || bytes < 0)
      throw new RangeError("Image payload size must be a nonnegative safe integer");
  const bytes = imagePayloadBytes + bindingPayloadBytes;
  if (!Number.isSafeInteger(bytes))
    throw new RangeError("Image progress size exceeds the safe integer range");
  return bytes;
}
