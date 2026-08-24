/**
 * The storage adapter toolkit: optional building blocks for implementing `BlockStore` against
 * a new substrate. Nothing here is part of the storage contract — the contract is the
 * `BlockStore` capability interfaces, their documented rules, and the conformance kit in
 * `@minnowdb/core/testing` — and the engine never references this module. An adapter is free
 * to ignore all of it and store records however its platform does best.
 *
 * What it offers, and which shipped adapter proves each piece:
 *
 * - `RecordCore` — the synchronous record-state machine behind `MemoryBlockStore` and
 *   `OpfsBlockStore`: every record family, every validation order, every typed conflict the
 *   contract requires, with `dump()`/`load()` for checkpointing. An adapter that owns a
 *   single-writer view of the records (a leader, a lone process) can wrap it and spend its
 *   own code purely on persistence.
 * - `WalWriter` / `replayWalFrames` — checksummed write-ahead-log frames over any
 *   `SyncFileHandle`.
 * - `ExtentPool` — packed append-only files for bulk bytes, addressed by `Placement`.
 * - `encodeRecordJson` and the envelope codecs — JSON with bigint support, checksummed and
 *   versioned, for frames, checkpoints, and immutable chunks.
 *
 * `SyncFileHandle` is the only substrate assumption the file-shaped pieces make; the browser's
 * `FileSystemSyncAccessHandle` satisfies it as-is, and so does anything with positioned
 * synchronous I/O. See `/docs/storage/custom` for the guide.
 */
export type { SyncFileHandle } from "./sync-file.js";
export { RecordCore, type PhysicalBlocks, type RecordCoreState } from "./record-core.js";
export { WalWriter, replayWalFrames } from "./wal.js";
export {
  ExtentPool,
  extentPath,
  type ExtentFiles,
  type ExtentMeta,
  type Placement,
} from "./extents.js";
export {
  LOG_FORMAT_VERSION,
  decodeChunk,
  decodePostingChunk,
  decodeRecordJson,
  decodeSyncCheckpoint,
  encodeChunk,
  encodePostingChunk,
  encodeRecordJson,
  encodeSyncCheckpoint,
} from "./wire.js";
