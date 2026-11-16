/** Image-registry ABI budget. The native provider must fit this fragmented, scalar layout;
 * SQL firing remains absent until the admitted root installs its native provider. */
import type { PhysicalType } from "../types.js";
import { dataBytes } from "../vector/vector.js";
import { SORT_ROW_VIEW_BYTES } from "./sort-row.js";
export const ROW_IMAGE_HELPER_SLOTS = 1;
export const ROW_IMAGE_FRAGMENT_BYTES = 65536;
/** One paged-directory cursor plus bounded encoded/decoded fragment buffers. */
export const ROW_IMAGE_FIXED_BYTES = 32768 + 2 * ROW_IMAGE_FRAGMENT_BYTES;
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
