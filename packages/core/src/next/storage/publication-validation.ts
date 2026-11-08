/** Ownership checks use immutable current/new snapshots and sealed supplied-writer records.
 * The lossy map's complement is never consulted. Caller marks those writers publishing before
 * yielding, holds the mutation barrier through native publish, and owns S and the workspace.
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
  writer: ChainWriter | null,
  failed: boolean,
) {
  let first: Error | undefined;
  for (const close of [
    () => a?.close(),
    () => b?.close(),
    () => c?.close(),
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
          hasB = await b.next(work);
          continue;
        }
        if (hasB && a.offset === b.offset) {
          if (a.length !== b.length)
            throw new StoreCorruptionError("Current/new roots disagree on extent length");
          hasA = await a.next(work);
          hasB = await b.next(work);
          continue;
        }
        const size = sizeOf(a.length);
        if (c !== null) {
          if (!hasC || c.offset !== a.offset || sizeOf(c.length) !== size)
            throw new RangeError(
              `publish: retirement does not match the current/new root difference at ${String(a.offset)}`,
            );
          hasC = await c.next(work);
        }
        total += size;
        hasA = await a.next(work);
      }
      if (hasC)
        throw new RangeError("publish: retirement is not in the current/new root difference");
      await check(work);
      return total;
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      await closeAll(a, b, c, null, failed);
    }
  });
}

/** Prove every incoming extent comes from current or a supplied sealed writer. The returned
 * disk worklist is any supplied writer output the root does not reach: it was never published
 * and may be freed after commit, without a retirement ticket. All three cursors close first.
 */
export function validateIncoming(
  journal: MetadataJournal,
  current: number,
  next: number,
  owners: number,
  io: MetadataWork,
  work?: TaskWork,
): Promise<number> {
  return pairPhase(journal, io, "merge", async (slots) => {
    let a: ExtentPairs | null = null,
      b: ExtentPairs | null = null,
      c: ExtentPairs | null = null,
      writer: ChainWriter | null = null,
      failed = false,
      produced = 0;
    try {
      a = await openPairs(journal, current, slots.left, true);
      b = await openPairs(journal, next, slots.right, true);
      c = await openPairs(journal, owners, slots.right + 1, true);
      writer = await journal.writer(slots.output);
      const unused = new ExtentPairWriter(writer, true);
      let hasA = await a.next(work),
        hasB = await b.next(work),
        hasC = await c.next(work),
        lastCurrentEnd = 0,
        steps = 0;
      while (hasB) {
        if (steps++ % 128 === 0) await check(work);
        while (hasC && c.offset < b.offset) {
          await unused.append(c.offset, c.length, work);
          hasC = await c.next(work);
        }
        while (hasA && a.offset < b.offset) {
          lastCurrentEnd = a.offset + sizeOf(a.length);
          hasA = await a.next(work);
        }
        if (hasA && a.offset === b.offset) {
          if (a.length !== b.length)
            throw new RangeError("publish: current-root extent changed length");
          if (hasC && c.offset === b.offset)
            throw new RangeError("publish: a writer overlaps current-root ownership");
        } else {
          if (b.offset < lastCurrentEnd || (hasA && b.offset + sizeOf(b.length) > a.offset))
            throw new RangeError("publish: an incoming extent overlaps current-root ownership");
          if (!hasC || c.offset !== b.offset || c.length !== b.length)
            throw new RangeError(
              `publish: incoming extent at ${String(b.offset)} is neither current-root nor supplied sealed-writer output`,
            );
          hasC = await c.next(work);
        }
        hasB = await b.next(work);
      }
      while (hasC) {
        await unused.append(c.offset, c.length, work);
        hasC = await c.next(work);
      }
      produced = await writer.finish();
      writer = null;
      await check(work);
      return produced;
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
      await closeAll(a, b, c, writer, failed);
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
