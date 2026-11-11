/** Publication-wide minimum: planning references only the snapshot's existing metadata. */
import type { IndexRangePlan, KeyRange } from "../access/key-index.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import { StreamingRangeCursor } from "./range-cursor.js";
import { rangeRunBytes } from "./range-input.js";
import type { ViewIndex } from "./view.js";

export interface PlannedRange {
  readonly sizes: { resident: number; minimum: number; keyBytes: number };
}

export function planIndexRange(
  index: ViewIndex,
  range: KeyRange,
  direction: "asc" | "desc",
): IndexRangePlan {
  const table = index.table,
    id = index.def.id,
    sign = direction === "asc" ? 1 : -1;
  let resident = 1024,
    keyBytes = 2048,
    io = false;
  // A flush writes at most one run per index per memtable. Staging writes one own-put
  // run plus a residual delta (#writeStaged); replay can already hold multiple staged runs.
  // Reserve both representations, even for empty/invisible inputs and out-of-range fences.
  // Neither later writes (invisible at this bound) nor publication can add a memtable to
  // this immutable TableView. Truncating a staged run cannot increase its run count.
  for (const memtable of table.tableView.memtables) {
    const delta = memtable.deltas.get(id);
    keyBytes = Math.max(keyBytes, delta?.maxKeyBytes ?? 0);
    const slots = 1 + Math.max(1, memtable.staged?.runs.get(id)?.length ?? 0);
    resident += slots * (512 + 4096 + 512);
    io = true;
  }
  for (const run of table.tableView.runs.get(id) ?? []) {
    resident += 512 + rangeRunBytes(run, sign);
    io = true;
  }
  resident += keyBytes * 2;
  const sizes = {
    resident,
    keyBytes,
    minimum: resident + (io ? MAX_BLOB_BYTES * 2 + 128 * 1024 : 0),
  };
  let opened = false;
  return {
    minimumBytes: sizes.minimum,
    open({ budget, signal }) {
      if (opened) throw new TypeError("Index range plan has already opened");
      opened = true;
      table.snapshot.check();
      signal.throwIfAborted();
      if (budget.held - budget.allocated < sizes.minimum)
        throw new RangeError("Index range grant is smaller than the planned minimum");
      return new StreamingRangeCursor(index, range, direction, { plan: { sizes }, budget, signal });
    },
  };
}
