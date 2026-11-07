/** A caller blocked on a publication cannot keep the publication's workspace. */
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { captureAudit, clearCapture, flattenCapture } from "./audit-capture.js";
import { MetadataMutex } from "./metadata-mutex.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_WORKSPACE_BYTES,
} from "./metadata-journal.js";
import { openPairs } from "./metadata-extents.js";
import { disposeSnapshot } from "./extent-snapshot.js";
import { countingBudget } from "./space-test-support.js";
import { ScriptedMetadataPages } from "../testing/metadata-test-support.js";
import type { ExtentRef } from "./extents.js";
for (const seed of seedsFor("audit-capture", [1641, 1642, 1643, 1644]))
  it(`flushes a prefix before yielding to a contending publication (${String(seed)})`, async () => {
    const budget = countingBudget(METADATA_WORKSPACE_BYTES),
      workspace = MetadataWorkspace.create(budget),
      journal = new MetadataJournal(new ScriptedMetadataPages(), workspace),
      mutex = new MetadataMutex();
    let enter!: () => void, resume!: () => void;
    const entered = new Promise<void>((resolve) => {
        enter = resolve;
      }),
      held = new Promise<void>((resolve) => {
        resume = resolve;
      });
    const count = 300 + (seed % 71),
      pivot = 31 + (seed % 17);
    const capture = captureAudit(
      journal,
      mutex,
      (async function* () {
        for (let i = 0; i < count; i++) {
          if (i === pivot) {
            enter();
            await held;
          }
          yield { offset: 2 ** 40 + i * 8192, length: 4096 };
        }
      })(),
    );
    try {
      await entered;
      await mutex.run(() =>
        workspace.run("spool", async () => {
          const writer = await journal.writer(1),
            handle = await writer.finish();
          await journal.release(handle);
          resume();
        }),
      );
      const complete = await capture;
      const handle = await flattenCapture(journal, complete, { slab: new Uint8Array(1 << 20) });
      try {
        await workspace.run("spool", async () => {
          const pairs = await openPairs(journal, handle, 1);
          try {
            for (let i = 0; i < count; i++) {
              expect(await pairs.next()).toBe(true);
              expect(pairs.offset).toBe(2 ** 40 + i * 8192);
              expect(pairs.length).toBe(4096);
            }
            expect(await pairs.next()).toBe(false);
          } finally {
            await pairs.close();
          }
        });
      } finally {
        await disposeSnapshot(journal, handle);
        await clearCapture(journal, complete);
      }
    } finally {
      resume();
      await capture.catch(() => undefined);
      workspace.close();
    }
    expect(budget.used).toBe(0);
  });
it("preserves a producer failure when its iterator cleanup fails too", async () => {
  const budget = countingBudget(METADATA_WORKSPACE_BYTES),
    workspace = MetadataWorkspace.create(budget),
    journal = new MetadataJournal(new ScriptedMetadataPages(), workspace),
    mutex = new MetadataMutex(),
    primary = new Error("producer failed");
  const input: AsyncIterable<ExtentRef> = {
    [Symbol.asyncIterator]: () => ({
      next: () => Promise.reject(primary),
      return: () => Promise.reject(new Error("cleanup failed")),
    }),
  };
  try {
    await expect(captureAudit(journal, mutex, input)).rejects.toBe(primary);
  } finally {
    workspace.close();
  }
  expect(budget.used).toBe(0);
});
