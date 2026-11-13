/** Encoded-key workspace follows projected positions, including repetitions.
 * Native pages include offsets/alignment; the caller also reserves bounded key
 * headers. NUMERIC's decimal digit-pair key can exceed its binary coefficient,
 * so add a second bound for that column alone, not other key/payload fields. */
import type { FlatVector } from "../vector/vector.js";
import { PageWriter } from "./spill-pages.js";
export function joinKeyPageBytes(columns: readonly FlatVector[], rows: number): number {
  let bytes = PageWriter.encodedBytes(columns, rows);
  for (const column of columns)
    if (column.type.kind === "decimal") bytes += PageWriter.encodedBytes([column], rows);
  return bytes;
}
