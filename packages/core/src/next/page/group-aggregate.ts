/** Metadata-only A1 access. No Reader.open, value/dictionary read, or NUMERIC rescale.
 * This certifies immutable record identity; the executor must separately certify complete
 * exactly-once PINNED visibility, no overlays and proved physical/logical type provenance.
 */
import type { ColumnChunk, GroupRef } from "../format/manifest.js";
import { valid } from "../format/bytes.js";
import { parseCharge } from "../format/manifest-codec.js";
import {
  decodeGroupAggregateRecord,
  MAX_GROUP_AGGREGATE_BYTES,
  GROUP_AGGREGATE_HEADER_BYTES,
  type GroupAggregateRecord,
} from "../format/group-aggregate.js";
import type { ByteBudget, ExtentReader, ReadLease } from "../storage/extents.js";

export { groupAggregateCoefficient } from "../format/group-aggregate.js";
export type { GroupAggregateRecord } from "../format/group-aggregate.js";
export interface GroupAggregateLease {
  /** Borrowed coefficient bytes expire at release; never retain/transfer them. */
  readonly value: GroupAggregateRecord;
  release(): void;
}
export interface GroupAggregateReadOptions {
  readonly budget: ByteBudget;
  readonly signal?: AbortSignal;
  readonly once?: boolean;
}
/** Complete bounded input plus parse/conversion peak. Consumer state/arithmetic is additional.
 * Read private copies on once misses, so pooled/slab/gap capacity cannot exceed this minimum. */
export function groupAggregateMinimum(column: ColumnChunk): number {
  const ref = column.aggregateSummary;
  if (ref === null) return 0;
  valid(
    Number.isInteger(ref.length) &&
      ref.length >= GROUP_AGGREGATE_HEADER_BYTES &&
      ref.length <= MAX_GROUP_AGGREGATE_BYTES,
    "Invalid aggregate blob length",
  );
  return ref.length + parseCharge(ref.length);
}
export async function readGroupAggregate(
  pages: ExtentReader,
  group: GroupRef,
  column: ColumnChunk,
  options: GroupAggregateReadOptions,
): Promise<GroupAggregateLease | null> {
  options.signal?.throwIfAborted();
  const ref = column.aggregateSummary;
  if (ref === null || group.deletions !== null || group.liveRowCount !== group.rowCount)
    return null;
  groupAggregateMinimum(column);
  const lease = await pages.read([ref], {
    budget: options.budget,
    ...(options.signal === undefined ? {} : { signal: options.signal }),
    ...(options.once === undefined ? {} : { once: options.once }),
    ...(options.once === true ? { retain: false } : {}),
  });
  let release = true;
  try {
    options.signal?.throwIfAborted();
    const bytes = lease.bytes[0];
    valid(bytes?.length === ref.length, "Missing aggregate bytes");
    const parsed = decodeGroupAggregateRecord(bytes, group.rowCount, column, options.budget);
    let value: GroupAggregateRecord | null = parsed.value;
    let owned: ReadLease | null = lease;
    const charge = parsed.charge,
      budget = options.budget;
    const held: GroupAggregateLease = {
      get value() {
        if (value === null) throw new TypeError("Aggregate lease was released");
        return value;
      },
      release() {
        const givingBack = owned;
        if (givingBack === null) return;
        owned = null;
        value = null;
        try {
          budget.free(charge);
        } finally {
          givingBack.release();
        }
      },
    };
    release = false;
    return held;
  } finally {
    if (release) lease.release();
  }
}
