/** Publication-wide minimum: planning references only the snapshot's existing metadata. */
import { indexMaximumKeyBytes } from "./index-limits.js";
import type { IndexRangePlan, KeyRange } from "../access/key-index.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import { StreamingRangeCursor } from "./range-cursor.js";
import { runKeyBytes, overflowCapacityBytes } from "./overflow-keys.js";
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
    const delta = memtable.deltas.get(id),
      published = memtable.published?.runs.get(id);
    const width = Math.max(
      delta?.maxKeyBytes ?? 0,
      memtable.state === "live" ? indexMaximumKeyBytes(index.def, table.def) : 0,
      published === undefined ? 0 : runKeyBytes(published),
    );
    keyBytes = Math.max(keyBytes, width);
    const staged = memtable.staged?.runs.get(id) ?? [];
    const slots = 1 + Math.max(1, staged.length);
    resident += slots * (512 + 4096 + 512);
    const possible = width > 2048 ? overflowCapacityBytes(width) : 0;
    resident += Math.max(
      possible,
      published === undefined ? 0 : rangeRunBytes(published, sign) - 4608,
    );
    for (const run of staged) {
      keyBytes = Math.max(keyBytes, runKeyBytes(run));
      resident += Math.max(possible, rangeRunBytes(run, sign) - 4608);
    }
    if (staged.length === 0) resident += possible;
    io = true;
  }
  for (const run of table.tableView.runs.get(id) ?? []) {
    resident += 512 + rangeRunBytes(run, sign);
    keyBytes = Math.max(keyBytes, runKeyBytes(run));
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
