import { writerViewSource } from "./writer-view.js";
/** Bounded immutable delta inputs; native histories spill instead of materializing arrays. */
import type { TaskWork } from "../task.js";
import { ReadDeniedError, type ByteBudget, type ExtentRef, type ExtentWriter } from "./extents.js";
import type { RetirementInput, RetirementInventory } from "./retirement-contract.js";
import { WriterInventoryRequiredError } from "./writer-inventory.js";
export interface CapturedWriterExtents {
  readonly added: readonly ExtentRef[];
  readonly addedInventory?: RetirementInventory;
  release(): Promise<void>;
}
export async function captureWriterExtents(
  writer: ExtentWriter,
  budget: ByteBudget,
  work: TaskWork = {},
  since?: number,
): Promise<CapturedWriterExtents> {
  work.signal?.throwIfAborted();
  let resident: readonly ExtentRef[] | undefined;
  if (since === undefined || writer.snapshotExtents === undefined) {
    try {
      const view = writer.extents;
      resident = writerViewSource(view)?.resident() ?? view;
    } catch (error) {
      if (!(error instanceof WriterInventoryRequiredError)) throw error;
    }
  }
  if (resident !== undefined) {
    const start = since ?? 0,
      count = Math.max(0, resident.length - start),
      bytes = 128 + count * 128;
    if (!budget.allocate(bytes)) throw new ReadDeniedError(bytes);
    let added: ExtentRef[] = [];
    try {
      for (let at = start; at < resident.length; at++) {
        const extent = resident[at];
        if (extent === undefined) throw new TypeError("Sparse writer ownership");
        added.push({ offset: extent.offset, length: extent.length });
      }
      let live = true;
      return {
        get added() {
          return added;
        },
        release: async () => {
          if (!live) return;
          live = false;
          added = [];
          budget.free(bytes);
        },
      };
    } catch (error) {
      added = [];
      budget.free(bytes);
      throw error;
    }
  }
  const native = writerViewSource(writer.extents)?.writer ?? writer;
  if (native.snapshotExtents === undefined) throw new TypeError("Writer has no bounded inventory");
  const inventory = await native.snapshotExtents({
    ...work,
    budget,
    ...(since === undefined ? {} : { since }),
  });
  return { added: [], addedInventory: inventory, release: () => inventory.release() };
}
/** Each yielded descriptor is copied before a source cursor advances. */
export async function* capturedWriterReferences(
  captured: CapturedWriterExtents,
  budget: ByteBudget,
  work: TaskWork = {},
): AsyncGenerator<ExtentRef> {
  const cursor = captured.addedInventory?.cursor({
    budget,
    ...(work.signal === undefined ? {} : { signal: work.signal }),
  });
  try {
    if (cursor !== undefined)
      while (await cursor.next(work)) yield { offset: cursor.offset, length: cursor.length };
    yield* captured.added;
  } finally {
    await cursor?.close();
  }
}
/** Explicit callback shape keeps source factories out of durable metadata. */
export type WriterExtentSource = () => RetirementInput;
