/** Internal portable export entry: catalog and sequence state captured with the committed pin. */
import type { WritePathImpl } from "./engine.js";
import type { ByteBudget } from "../storage/extents.js";
import type { SnapshotSink, SnapshotWork } from "../format/snapshot.js";
import { Catalog } from "./catalog.js";
import { snapshotCatalogParts } from "./snapshot-catalog.js";
import { exportPinnedSnapshot, SnapshotScopeError } from "./snapshot-export.js";

export async function exportSnapshot(
  path: Pick<WritePathImpl, "pin" | "sequences">,
  id: Uint8Array,
  sink: SnapshotSink,
  budget: ByteBudget,
  work: SnapshotWork = {},
): Promise<void> {
  const pin = path.pin();
  let captured: ReturnType<WritePathImpl["sequences"]["capture"]> | undefined;
  let parts: Generator<Uint8Array, void> | undefined;
  try {
    if (!(pin.catalog instanceof Catalog))
      throw new SnapshotScopeError("Portable export requires a native catalog");
    // Row durability does not bound nontransactional sequence state. Cover already-emitted
    // reservations too; no await here, so later setval/reservations cannot change the capture.
    captured = path.sequences.capture(pin.catalog.sequences, budget);
    parts = snapshotCatalogParts(pin.catalog, captured.value, budget);
    const producer = parts;
    await exportPinnedSnapshot(
      pin,
      id,
      {
        [Symbol.asyncIterator]() {
          return {
            next: () => Promise.resolve(producer.next()),
            return: () => Promise.resolve(producer.return(undefined)),
          };
        },
      },
      sink,
      budget,
      work,
    );
  } finally {
    parts?.return(undefined);
    captured?.release();
    pin.release();
  }
}
