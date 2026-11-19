/** Complete one-row merge minimum, independent of the chosen source batch size.
 * Encoded sort keys escape every NUL byte (key-encoding.ts). A wide merge keeps
 * two pages, its output arena, and optionally the WITH TIES row/key together.
 * Parsed semantic operands and unknown expanding expressions keep their existing
 * specialized admission; this bound covers native scalar layouts only.
 */
import type { PhysicalType } from "../types.js";
import { BATCH_TARGET_BYTES } from "../vector/vector.js";
import { simpleAggregateType } from "./agg/parameter-copies.js";
import { directWidth, PREFIX_WORDS, sortMode, type SortColumn } from "./sort-keys.js";
import { sortMergeWorkspaceBytes } from "./sort.js";
import { SORT_ROW_VIEW_BYTES } from "./sort-row.js";

export function sortRowProgressBytes(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  widths: ReadonlyArray<number | null>,
  held: boolean,
): number | null {
  if (
    types.some((type) => !simpleAggregateType(type)) ||
    widths.some((width) => width === null) ||
    sortMode(types, keys) !== "keys"
  )
    return null;
  let payload = 0,
    variable = 0,
    arena = 0;
  for (let column = 0; column < types.length; column++) {
    const width = widths[column];
    if (width === undefined || width === null) return null;
    payload += width;
    if (types[column]?.kind === "text" || types[column]?.kind === "bytes") variable += width;
    // ColumnStore.appendNeed includes the initial 2,048-row chunk, bitmap,
    // offsets and 16KiB arena before any growth. 32KiB covers that fixed share.
    arena +=
      32768 +
      (width > BATCH_TARGET_BYTES ? width : width > 0 ? 2 ** Math.ceil(Math.log2(width)) : 0);
  }
  // Sorter.#makeRoom already prepays the bounded small-row path. Preserve that
  // minimum; only its >64KiB variable-payload branch emits direct wide pages.
  if (variable <= 64 * 1024) return 0;
  let encoded = 0;
  if (directWidth(types, keys) === null)
    for (const key of keys) {
      const type = types[key.column],
        width = widths[key.column];
      if (type === undefined || width === undefined || width === null) return null;
      encoded += type.kind === "text" || type.kind === "bytes" ? 2 * width + 3 : 17;
    }
  const columns = types.length + PREFIX_WORDS + Number(encoded > 0);
  // PageWriter metadata, padded buffers and fixed prefix words beside payload.
  const page = payload + encoded + 4096 + 256 * columns;
  return sortMergeWorkspaceBytes(
    page,
    arena,
    columns * SORT_ROW_VIEW_BYTES,
    0,
    held ? encoded : undefined,
  );
}
