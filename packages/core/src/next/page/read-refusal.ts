/**
 * A read refused by the budget it was charged to, typed as the public memory refusal.
 *
 * The space refuses a lease it cannot charge (`ReadDeniedError`), a chunk reader its own charge
 * (`PageBudgetError`), a metadata parse its parse charge (`ParseBudgetError`). Readers that can
 * make room catch them and retry; where a read cannot (the write path's own reads: old rows,
 * index runs, deletion bitmaps, group roots, the scans of foreign-key and ALTER checks, an
 * upsert's conflicting row), the statement is refused with `MemoryAdmissionError` (SQLSTATE
 * 53200), naming the read, never with the reader's internal error.
 *
 * Interim (lead decision on #65): the write path does not fund these reads before they allocate,
 * and does not wait for them, so under memory pressure such a statement can be refused below the
 * cap. Funding write-path reads is a follow-up ("write-path read funding").
 */
import { MemoryAdmissionError } from "../memory-values.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import { ReadDeniedError } from "../storage/extents.js";
import { PageBudgetError } from "./reader.js";

/** `error` as the typed 53200 when it is a read's budget refusal; any other error as it is. */
export function readRefusal(error: unknown, reader = "write path read"): unknown {
  const read =
    error instanceof ReadDeniedError
      ? "a stored value or page lease"
      : error instanceof PageBudgetError
        ? "a chunk reader's pages"
        : error instanceof ParseBudgetError
          ? "a metadata parse"
          : null;
  if (read === null) return error;
  const bytes = (error as { readonly bytes: number }).bytes;
  return new MemoryAdmissionError(`${reader} (${read})`, bytes, 0);
}
