/** Portable load shares the native export grammar. Storage leases remain charged through
 * parser close; quota/replay/cancellation errors keep the loader's original retry policy. */
import { SnapshotFormatError } from "../format/snapshot.js";
import type { ExtentReader } from "../storage/extents.js";
import {
  SnapshotAccount,
  SnapshotColumnBytes,
  snapshotCheck,
  snapshotPace,
  type SnapshotStagedColumn,
} from "./snapshot-stage.js";
import { StructuredValueCorruptionError, validateStructuredParts } from "./structured-value.js";

export async function validateSnapshotValue(
  column: SnapshotStagedColumn,
  space: ExtentReader,
  offset: number,
  length: number,
): Promise<void> {
  const type = column.column.type;
  if (type.kind === "bytes") return;
  snapshotCheck(
    ["array", "json", "enum", "text"].includes(type.kind),
    "Unsupported structured portable type",
  );
  const account = new SnapshotAccount(column.budget);
  try {
    // The helper prepays its original131072-byte parser workspace BEFORE entering
    // this producer. Its iterator close runs this finally BEFORE parser credit returns.
    const parts = async function* () {
      const source = new SnapshotColumnBytes(column, space);
      try {
        yield* source.parts(offset, length);
      } finally {
        source.close();
      }
    };
    await validateStructuredParts(
      type,
      parts,
      length,
      {
        take: (bytes) => account.take(bytes),
        free: (bytes) => account.free(bytes),
        pace: () => snapshotPace(column.work),
      },
      () => undefined,
    );
  } catch (error) {
    if (error instanceof StructuredValueCorruptionError)
      throw new SnapshotFormatError(error.message);
    throw error;
  } finally {
    account.close();
  }
}
