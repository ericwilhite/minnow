/** Audit enumeration can pause outside the single workspace. Partial chains form a disk queue,
 * not a resident chunk vector; a contending publication causes a bounded prefix flush first.
 */
import type { TaskWork } from "../task.js";
import type { ExtentRef } from "./extents.js";
import { MetadataMutex, WORKSPACE_YIELD } from "./metadata-mutex.js";
import { MetadataJournal } from "./metadata-journal.js";
import { ExtentPairWriter, openPairs } from "./metadata-extents.js";
import { pairPhase, type MetadataWork } from "./metadata-work.js";

export interface AuditCapture {
  head: number;
  tail: number;
}
async function finishProducer(
  iterator: AsyncIterator<ExtentRef>,
  failed: boolean,
  cleanup: () => Promise<void>,
): Promise<void> {
  try {
    await iterator.return?.();
  } catch (error) {
    await cleanup().catch(() => undefined);
    if (!failed) throw error;
  }
}
export async function captureAudit(
  journal: MetadataJournal,
  mutex: MetadataMutex,
  input: AsyncIterable<ExtentRef>,
  work?: TaskWork,
): Promise<AuditCapture> {
  const capture: AuditCapture = { head: 0, tail: 0 },
    iterator = input[Symbol.asyncIterator]();
  const progress: { pending: Promise<IteratorResult<ExtentRef>> | null; finished: boolean } = {
    pending: null,
    finished: false,
  };
  let failed = false;
  try {
    while (!progress.finished) {
      const first = await (progress.pending ?? iterator.next());
      progress.pending = null;
      if (first.done === true) break;
      await mutex.run((turn) =>
        journal.workspace.run("spool", async () => {
          const writer = await journal.writer(1),
            output = new ExtentPairWriter(writer, false);
          let handle = 0;
          try {
            await output.append(first.value.offset, first.value.length, work);
            while (!turn.contended) {
              progress.pending = iterator.next();
              const next = await turn.race(progress.pending);
              if (next === WORKSPACE_YIELD) break;
              progress.pending = null;
              if (next.done === true) {
                progress.finished = true;
                break;
              }
              const writing = output.append(next.value.offset, next.value.length, work);
              if (writing !== undefined) await writing;
            }
            handle = await writer.finish();
            if (capture.tail !== 0) await journal.link(capture.tail, handle);
            else capture.head = handle;
            capture.tail = handle;
          } catch (error) {
            await writer.abort().catch(() => undefined);
            if (handle !== 0) await journal.release(handle).catch(() => undefined);
            throw error;
          }
        }),
      );
    }
    return capture;
  } catch (error) {
    failed = true;
    await mutex.run(() => clearCapture(journal, capture)).catch(() => undefined);
    throw error;
  } finally {
    await finishProducer(iterator, failed, () => mutex.run(() => clearCapture(journal, capture)));
  }
}
/** Workspace mutex held; no frame phase active. */
export function clearCapture(journal: MetadataJournal, capture: AuditCapture): Promise<void> {
  return journal.workspace.run("spool", async () => {
    while (capture.head !== 0) {
      const head = capture.head,
        next = (await journal.describe(head)).next;
      await journal.release(head);
      capture.head = next;
    }
    capture.tail = 0;
  });
}
/** Flatten prefixes through one slab cursor and one owned output; caller sorts this raw chain.
 * The partial chains remain referenced until the caller explicitly clears their capture.
 */
export function flattenCapture(
  journal: MetadataJournal,
  capture: AuditCapture,
  io: MetadataWork,
  work?: TaskWork,
): Promise<number> {
  return pairPhase(journal, io, "merge", async (slots) => {
    const writer = await journal.writer(slots.output),
      output = new ExtentPairWriter(writer, false);
    let head = capture.head;
    try {
      while (head !== 0) {
        const next = (await journal.describe(head)).next,
          pairs = await openPairs(journal, head, slots.left);
        try {
          for (;;) {
            const value = pairs.next(work);
            if (!(typeof value === "boolean" ? value : await value)) break;
            const writing = output.append(pairs.offset, pairs.length, work);
            if (writing !== undefined) await writing;
          }
        } finally {
          await pairs.close();
        }
        head = next;
      }
      return await writer.finish();
    } catch (error) {
      await writer.abort().catch(() => undefined);
      throw error;
    }
  });
}
