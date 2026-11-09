/** Ownership checks use immutable current/new snapshots and sealed supplied-writer records.
 * The lossy map's complement is never consulted. Caller marks those writers publishing before
 * yielding, holds the mutation barrier through native publish, and owns S and the workspace.
 * Preserve the pair APIs' synchronous fast paths; await only actual I/O/clock promises, not
 * each scalar of the immutable roots. Checks and bounded shared-clock checkpoints stay exact.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { DATA_ALIGN, StoreCorruptionError } from "./contract.js";
import { ExtentPairs, openPairs, ExtentPairWriter } from "./metadata-extents.js";
import { MetadataJournal, type ChainWriter } from "./metadata-journal.js";
import { pairPhase, type MetadataWork } from "./metadata-work.js";
import { disposeSnapshot } from "./extent-snapshot.js";

const sizeOf = (length: number) => Math.ceil(length / DATA_ALIGN) * DATA_ALIGN;
async function check(work?: TaskWork) {
  const pending = pauseIfDue(work);
  if (pending !== undefined) await pending;
}
async function closeAll(
  a: ExtentPairs | null,
  b: ExtentPairs | null,
  c: ExtentPairs | null,
  d: ExtentPairs | null,
  writer: ChainWriter | null,
  failed: boolean,
) {
  let first: Error | undefined;
  for (const close of [
    () => a?.close(),
    () => b?.close(),
    () => c?.close(),
    () => d?.close(),
    () => writer?.abort(),
  ]) {
    try {
      await close();
    } catch (error) {
      first ??= error instanceof Error ? error : new Error(String(error));
    }
  }
  if (!failed && first !== undefined) throw first;
}

/** null claims only counts A-B, used for the generation-zero ticket. All lengths shared by
 * roots must match exactly; supplied retirement lengths may round to the same allocation size.
 */
export function validateRetirements(
  journal: MetadataJournal,
  before: number,
  after: number,
  claims: number | null,
  io: MetadataWork,
  work?: TaskWork,
): Promise<number> {
  return pairPhase(journal, io, "merge", async (slots) => {
    let a: ExtentPairs | null = null,
      b: ExtentPairs | null = null,
      c: ExtentPairs | null = null,
      failed = false;
    try {
      a = await openPairs(journal, before, slots.left, true);
      b = await openPairs(journal, after, slots.right, true);
      if (claims !== null) c = await openPairs(journal, claims, slots.right + 1, true);
      let hasA = await a.next(work),
        hasB = await b.next(work),
        hasC = await c?.next(work),
        total = 0,
        steps = 0;
      while (hasA) {
        if (steps++ % 128 === 0) await check(work);
        if (hasB && b.offset < a.offset) {
          const nextB = b.next(work);
          hasB = typeof nextB === "boolean" ? nextB : await nextB;
          continue;
        }
        if (hasB && a.offset === b.offset) {
          if (a.length !== b.length)
            throw new StoreCorruptionError("Current/new roots disagree on extent length");
          const nextA = a.next(work);
          hasA = typeof nextA === "boolean" ? nextA : await nextA;
          const nextB = b.next(work);
          hasB = typeof nextB === "boolean" ? nextB : await nextB;
          continue;
        }
        const size = sizeOf(a.length);
        if (c !== null) {
          if (!hasC || c.offset !== a.offset || sizeOf(c.length) !== size)
            throw new RangeError(
              `publish: retirement does not match the current/new root difference at ${String(a.offset)}`,
            );
          const nextC = c.next(work);
          hasC = typeof nextC === "boolean" ? nextC : await nextC;
        }
        total += size;
        const nextA = a.next(work);
        hasA = typeof nextA === "boolean" ? nextA : await nextA;
      }
      if (hasC)
        throw new RangeError("publish: retirement is not in the current/new root difference");
      await check(work);
      return total;
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      await closeAll(a, b, c, null, null, failed);
    }
  });
}

/** Validate A-B retirements and incoming ownership in one merge of the immutable roots.
 * Four cursors borrow the prepaid S slab; only control/output own W frames. The returned disk
 * worklist is supplied output absent from B, never published and freeable after commit. No
 * ownership proof comes from the lossy map or from a mutable/unsupplied writer.
 */
export function validatePublication(
  journal: MetadataJournal,
  current: number,
  next: number,
  claims: number,
  owners: number,
  io: MetadataWork,
  work?: TaskWork,
): Promise<{ retiredBytes: number; unused: number }> {
  return pairPhase(journal, io, "merge", async (slots) => {
    let a: ExtentPairs | null = null,
      b: ExtentPairs | null = null,
      c: ExtentPairs | null = null,
      d: ExtentPairs | null = null,
      writer: ChainWriter | null = null,
      failed = false,
      produced = 0;
    try {
      a = await openPairs(journal, current, slots.left, true);
      b = await openPairs(journal, next, slots.right, true);
      c = await openPairs(journal, claims, slots.right + 1, true);
      d = await openPairs(journal, owners, slots.right + 2, true);
      writer = await journal.writer(slots.output);
      const unused = new ExtentPairWriter(writer, true);
      let hasA = await a.next(work),
        hasB = await b.next(work),
        hasC = await c.next(work),
        hasD = await d.next(work),
        lastCurrentEnd = 0,
        retiredBytes = 0,
        steps = 0;
      while (hasA || hasB) {
        if (steps++ % 128 === 0) await check(work);
        if (hasA && (!hasB || a.offset < b.offset)) {
          // A-only: the supplied claim must match the exact rounded allocation.
          const size = sizeOf(a.length);
          if (!hasC || c.offset !== a.offset || sizeOf(c.length) !== size)
            throw new RangeError(
              `publish: retirement does not match the current/new root difference at ${String(a.offset)}`,
            );
          retiredBytes += size;
          const nextC = c.next(work);
          hasC = typeof nextC === "boolean" ? nextC : await nextC;
          lastCurrentEnd = a.offset + size;
          const nextA = a.next(work);
          hasA = typeof nextA === "boolean" ? nextA : await nextA;
          continue;
        }
        while (hasD && d.offset < b.offset) {
          const pending = unused.append(d.offset, d.length, work);
          if (pending !== undefined) await pending;
          const nextD = d.next(work);
          hasD = typeof nextD === "boolean" ? nextD : await nextD;
        }
        if (hasA && a.offset === b.offset) {
          if (a.length !== b.length)
            throw new StoreCorruptionError("Current/new roots disagree on extent length");
          if (hasD && d.offset === b.offset)
            throw new RangeError("publish: a writer overlaps current-root ownership");
          lastCurrentEnd = a.offset + sizeOf(a.length);
          const nextA = a.next(work);
          hasA = typeof nextA === "boolean" ? nextA : await nextA;
        } else {
          if (b.offset < lastCurrentEnd || (hasA && b.offset + sizeOf(b.length) > a.offset))
            throw new RangeError("publish: an incoming extent overlaps current-root ownership");
          if (!hasD || d.offset !== b.offset || d.length !== b.length)
            throw new RangeError(
              `publish: incoming extent at ${String(b.offset)} is neither current-root nor supplied sealed-writer output`,
            );
          const nextD = d.next(work);
          hasD = typeof nextD === "boolean" ? nextD : await nextD;
        }
        const nextB = b.next(work);
        hasB = typeof nextB === "boolean" ? nextB : await nextB;
      }
      if (hasC)
        throw new RangeError("publish: retirement is not in the current/new root difference");
      while (hasD) {
        const pending = unused.append(d.offset, d.length, work);
        if (pending !== undefined) await pending;
        const nextD = d.next(work);
        hasD = typeof nextD === "boolean" ? nextD : await nextD;
      }
      produced = await writer.finish();
      writer = null;
      await check(work);
      return { retiredBytes, unused: produced };
    } catch (error) {
      failed = true;
      if (produced !== 0) {
        try {
          await journal.release(produced);
        } catch {
          /* preserve the primary failure */
        }
      }
      throw error;
    } finally {
      await closeAll(a, b, c, d, writer, failed);
    }
  });
}

/** No resident vector beyond256 ranges. Used only under the mutation barrier after all root
 * input cursors close; release callbacks must report failure rather than advance this worklist.
 */
export async function releaseWorklist(
  journal: MetadataJournal,
  handle: number,
  release: (
    ranges: ReadonlyArray<{ readonly offset: number; readonly length: number }>,
  ) => Promise<void>,
  work?: TaskWork,
): Promise<void> {
  await journal.workspace.run("release", async () => {
    const pairs = await openPairs(journal, handle, 1, true);
    try {
      let more = await pairs.next(work);
      while (more) {
        const ranges: Array<{ offset: number; length: number }> = [];
        while (more && ranges.length < 256) {
          if (ranges.length % 128 === 0) await check(work);
          ranges.push({ offset: pairs.offset, length: sizeOf(pairs.length) });
          more = await pairs.next(work);
        }
        await release(ranges);
      }
    } finally {
      await pairs.close();
    }
  });
}

/** Cleanup outside an active workspace phase, preserving a primary native failure. */
export async function discardPrepared(
  journal: MetadataJournal,
  handles: readonly number[],
  failed: boolean,
): Promise<void> {
  let first: Error | undefined;
  for (const handle of handles)
    if (handle !== 0) {
      try {
        await disposeSnapshot(journal, handle);
      } catch (error) {
        first ??= error instanceof Error ? error : new Error(String(error));
      }
    }
  if (!failed && first !== undefined) throw first;
}
