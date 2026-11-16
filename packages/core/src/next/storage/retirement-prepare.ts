/** Reuse native audit capture/sort/difference; no resident descriptor set or second scratch store. */
import { captureAudit, clearCapture, flattenCapture, type AuditCapture } from "./audit-capture.js";
import { disposeSnapshot } from "./extent-snapshot.js";
import { sortExtents, mergeExtents } from "./metadata-extents.js";
import { pairPhase, type MetadataWork } from "./metadata-work.js";
import type { MetadataJournal } from "./metadata-journal.js";
import type { MetadataMutex } from "./metadata-mutex.js";
import type { ExtentRef } from "./extents.js";
import { ReadDeniedError } from "./extents.js";
import { StoreCorruptionError } from "./contract.js";
import type {
  RetirementInput,
  RetirementInventory,
  RetirementOptions,
} from "./retirement-contract.js";
import {
  makeRetirementInventory,
  RETIREMENT_OWNER_BYTES,
  type InventoryContext,
} from "./retirement-inventory.js";
import { validateExtentPair } from "./metadata-extents.js";
/** Space preparation record/controller is distinct from the returned inventory owner. */
export const RETIREMENT_PREPARATION_BYTES = 4096;
export interface RetirementPreparationContext extends InventoryContext {
  readonly journal: MetadataJournal;
  readonly mutex: MetadataMutex;
  withSlab<T>(work: (io: MetadataWork) => Promise<T>): Promise<T>;
  check(): void;
}
/** Async generator serializes return behind a pending next, even for an input whose own
 * return() settles immediately. Source bytes remain owned through EVERY pending producer wait.
 */
async function* references(
  input: RetirementInput,
  options: RetirementOptions,
): AsyncGenerator<ExtentRef> {
  const iterator =
    Symbol.asyncIterator in input ? input[Symbol.asyncIterator]() : input[Symbol.iterator]();
  try {
    for (;;) {
      options.signal?.throwIfAborted();
      const next = await iterator.next();
      options.signal?.throwIfAborted();
      if (next.done === true) break;
      const value = next.value;
      if ("extent" in value && options.retained === undefined)
        throw new TypeError("Page retirements require the new root's retained references");
      const extent = extentOf(value);
      yield { offset: extent.offset, length: extent.length };
    }
  } finally {
    await iterator.return?.();
  }
}
/** Complete caller preflight includes this fixed owner descriptor plus its source walker.
 * The space's existing W/S workspaces fund capture/sort; consumer windows are charged separately.
 */
export async function prepareRetirementInventory(
  context: RetirementPreparationContext,
  input: RetirementInput,
  options: RetirementOptions,
): Promise<RetirementInventory> {
  options.signal?.throwIfAborted();
  context.check();
  if (!options.budget.allocate(RETIREMENT_OWNER_BYTES))
    throw new ReadDeniedError(RETIREMENT_OWNER_BYTES);
  let candidates: AuditCapture | null = null,
    kept: AuditCapture | null = null;
  let raw = 0,
    ordered = 0,
    keptRaw = 0,
    keptOrdered = 0,
    result = 0,
    count = 0;
  let failed = false,
    reason: Error | undefined;
  try {
    candidates = await captureAudit(
      context.journal,
      context.mutex,
      references(input, options),
      options,
    );
    if (options.retained !== undefined)
      // Retained page witnesses need no second witness of their own: their extents are kept whole.
      kept = await captureAudit(
        context.journal,
        context.mutex,
        retainedReferences(options.retained, options),
        options,
      );
    options.signal?.throwIfAborted();
    context.check();
    await context.mutex.run(() =>
      context.withSlab(async (io) => {
        options.signal?.throwIfAborted();
        context.check();
        if (candidates === null) throw new Error("Missing retirement capture");
        raw = await flattenCapture(context.journal, candidates, io, options);
        ordered = await pairPhase(context.journal, io, "merge", (slots) =>
          sortExtents(context.journal, raw, options.duplicates ?? "exact", options, slots),
        );
        if (kept !== null) {
          keptRaw = await flattenCapture(context.journal, kept, io, options);
          keptOrdered = await pairPhase(context.journal, io, "merge", (slots) =>
            sortExtents(context.journal, keptRaw, "exact", options, slots),
          );
          result = await pairPhase(context.journal, io, "merge", (slots) =>
            mergeExtents(
              context.journal,
              ordered,
              keptOrdered,
              "difference",
              options,
              "exact",
              slots,
            ),
          );
        } else {
          result = ordered;
          ordered = 0;
        }
        await context.journal.workspace.run("spool", async () => {
          const descriptor = await context.journal.describe(result);
          if (descriptor.length % 16 !== 0)
            throw new StoreCorruptionError("Truncated retirement inventory");
          count = descriptor.length / 16;
        });
        options.signal?.throwIfAborted();
        context.check();
      }),
    );
  } catch (error) {
    failed = true;
    reason ??= error instanceof Error ? error : new Error(String(error));
  } finally {
    // Every handle is an OWNED reference; equal handle numbers may legitimately own two refs.
    for (const handle of [raw, ordered, keptRaw, keptOrdered])
      if (handle !== 0)
        try {
          await context.mutex.run(() => disposeSnapshot(context.journal, handle));
        } catch (error) {
          failed = true;
          reason ??= error instanceof Error ? error : new Error(String(error));
        }
    for (const capture of [candidates, kept])
      if (capture !== null)
        try {
          await context.mutex.run(() => clearCapture(context.journal, capture));
        } catch (error) {
          failed = true;
          reason ??= error instanceof Error ? error : new Error(String(error));
        }
  }
  if (!failed) {
    try {
      options.signal?.throwIfAborted();
      context.check();
      return makeRetirementInventory(context, result, count, options.budget);
    } catch (error) {
      reason ??= error instanceof Error ? error : new Error(String(error));
    }
  }
  try {
    if (result !== 0) await context.mutex.run(() => disposeSnapshot(context.journal, result));
  } catch {
    /* preserve the original preparation/cancellation failure */
  } finally {
    options.budget.free(RETIREMENT_OWNER_BYTES);
  }
  throw reason ?? new Error("Retirement preparation failed");
}
async function* retainedReferences(
  input: RetirementInput,
  options: RetirementOptions,
): AsyncGenerator<ExtentRef> {
  const iterator =
    Symbol.asyncIterator in input ? input[Symbol.asyncIterator]() : input[Symbol.iterator]();
  try {
    for (;;) {
      options.signal?.throwIfAborted();
      const next = await iterator.next();
      options.signal?.throwIfAborted();
      if (next.done === true) break;
      const extent = extentOf(next.value);
      yield { offset: extent.offset, length: extent.length };
    }
  } finally {
    await iterator.return?.();
  }
}

function extentOf(value: ExtentRef | import("./extents.js").BlobRef): ExtentRef {
  if (
    "extent" in value &&
    (!Number.isSafeInteger(value.offset) ||
      value.offset < 0 ||
      !Number.isSafeInteger(value.length) ||
      value.length < 0 ||
      !Number.isSafeInteger(value.offset + value.length) ||
      value.offset + value.length > value.extent.length ||
      !Number.isInteger(value.checksum) ||
      value.checksum < 0 ||
      value.checksum > 0xffffffff)
  )
    throw new RangeError("Invalid retirement page reference");
  const extent = "extent" in value ? value.extent : value;
  validateExtentPair(extent.offset, extent.length);
  return extent;
}
