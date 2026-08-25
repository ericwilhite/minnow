import { OpfsBlockStore, deleteOpfsDatabase } from "@minnowdb/core/storage/opfs";
import type { EngineDriver } from "./session";
import { createMinnowDriver, datasetStorageName } from "./minnow";

/**
 * The same engine, the same workload, a different floor: a write-ahead log and packed extent
 * files in the Origin Private File System, held open by a leader so every commit and read is a
 * synchronous file operation. Sharing `createMinnowDriver` with the IndexedDB column is the
 * point — any measured difference between the two Minnow columns is storage, not workload
 * drift.
 */
export const minnowOpfsDriver: EngineDriver = createMinnowDriver({
  id: "minnow-opfs",
  persistence: "OPFS · immutable compressed column blocks",
  openStore: (record) =>
    OpfsBlockStore.open({ name: datasetStorageName(record), durability: record.durability }),
  deleteDataset: (materialization) => deleteOpfsDatabase({ name: materialization.storageName }),
});
