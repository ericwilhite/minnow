/** Prebuilt COUNT page kernel. No decoded values, dictionary expansion or allocation.
 * The page parser already checked validity and dictionary codes against their directory.
 */
import { countBits } from "../../page/bits.js";
import type { PageView } from "../../page/decode.js";
import type { PageRows } from "../../page/consume.js";

/** Non-null rows in one page range, or selected group positions; preserves duplicates. */
export function countPageRows(
  view: PageView,
  firstRow: number,
  rows: PageRows | null,
  from: number,
  to: number,
): number {
  if (view.allNull) return 0;
  const validity = view.validity;
  if (validity === null) return to - from;
  if (rows === null) return countBits(validity, to) - countBits(validity, from);
  let count = 0;
  for (let k = from; k < to; k++) {
    const row = (rows[k] ?? 0) - firstRow;
    count += ((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1;
  }
  return count;
}
