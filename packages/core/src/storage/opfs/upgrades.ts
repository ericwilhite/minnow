/** Supported native upgrades run under the WAL and checkpoint locks. Preparation touches only
 * a temporary tree; a checksummed ready record makes publication retryable after tab/power loss.
 * The original payloads remain in place. No operation is admitted until publication completes. */
import { crc32, crc32Continue } from "../../block-format/checksum.js";
import { readFully, writeFully, type SyncFileHandle } from "../toolkit/sync-file.js";
import { iterateWalFrames } from "../toolkit/wal.js";
import {
  WalAcknowledgements,
  WalAcknowledgementCorruptionError,
} from "../toolkit/wal-acknowledgement.js";
import { decodeSyncCheckpoint, encodeSyncCheckpoint } from "../toolkit/wire.js";
import { StorageCorruptionError, StorageFormatVersionError } from "../types.js";
import { OpfsTree, isDomError } from "./files.js";
import { MAX_OPFS_CHECKPOINT_BYTES, MAX_OPFS_WAL_BYTES, OpfsLeader } from "./leader.js";

export const FIRST_SUPPORTED_OPFS_LAYOUT = 6;
export const OPFS_UPGRADES = [{ from: 6, to: 7 }] as const;
const DIRECTORY = "upgrade-6-7";
const READY = "ready";
const COMPLETE = "upgrade-6-7-complete";
const CONTROL_NAMES = ["wal", "checkpoint-a", "checkpoint-b"] as const;
const MARKER = new TextEncoder().encode(JSON.stringify({ formatVersion: 7 }));

interface Fingerprint {
  size: number;
  checksum: number;
}
export interface UpgradeHandles {
  wal: SyncFileHandle;
  slotA: SyncFileHandle;
  slotB: SyncFileHandle;
}
interface ReadyRecord {
  from: 6;
  to: 7;
  source: Fingerprint[];
  checkpoint: Fingerprint;
  acknowledgement: Fingerprint;
  sequence: number;
}

function corrupt(message: string, cause?: unknown): StorageCorruptionError {
  const error = new StorageCorruptionError("opfs", DIRECTORY, message);
  if (cause !== undefined) Object.defineProperty(error, "cause", { value: cause });
  return error;
}

function checkpointSequence(bytes: Uint8Array): number {
  const decoded = decodeSyncCheckpoint(bytes) as { lastSeq?: unknown } | undefined;
  if (
    decoded === undefined ||
    !Number.isSafeInteger(decoded.lastSeq) ||
    typeof decoded.lastSeq !== "number" ||
    decoded.lastSeq < 0
  ) {
    throw corrupt("Invalid upgrade checkpoint sequence");
  }
  return decoded.lastSeq;
}

function fingerprint(handle: SyncFileHandle): Fingerprint {
  const size = handle.getSize();
  if (!Number.isSafeInteger(size) || size < 0) throw corrupt("Invalid upgrade file length");
  const buffer = new Uint8Array(Math.min(size, 64 * 1024));
  let checksum = 0;
  for (let offset = 0; offset < size; offset += buffer.length) {
    const chunk = buffer.subarray(0, Math.min(buffer.length, size - offset));
    readFully(handle, chunk, offset, "fingerprinting an upgrade source");
    checksum = crc32Continue(checksum, chunk);
  }
  return { size, checksum };
}

function same(left: Fingerprint, right: Fingerprint): boolean {
  return left.size === right.size && left.checksum === right.checksum;
}

function bytesFingerprint(bytes: Uint8Array): Fingerprint {
  return { size: bytes.length, checksum: crc32(bytes) };
}

function boundedBytes(handle: SyncFileHandle, maximum: number): Uint8Array {
  const size = handle.getSize();
  if (size > maximum) throw corrupt("Upgrade control file exceeds its recovery bound");
  const bytes = new Uint8Array(size);
  readFully(handle, bytes, 0, "reading upgrade control bytes");
  return bytes;
}

function convertCheckpoint(bytes: Uint8Array): Uint8Array {
  if (bytes.length === 0) return bytes;
  if (bytes.length < 20 || new TextDecoder().decode(bytes.subarray(0, 8)) !== "MNWCKPS1") {
    throw corrupt("Layout-6 checkpoint is incomplete or foreign");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(8, true) !== 6) {
    const version = view.getUint32(8, true);
    throw new StorageFormatVersionError(
      "opfs",
      "upgrade/checkpoint",
      version,
      6,
      version < 6 ? "older" : "newer",
    );
  }
  if (
    view.getUint32(12, true) !== bytes.length - 20 ||
    view.getUint32(16, true) !== crc32(bytes.subarray(20))
  ) {
    throw corrupt("Layout-6 checkpoint checksum or length is invalid");
  }
  const converted = bytes.slice();
  new DataView(converted.buffer).setUint32(8, 7, true);
  return converted;
}

async function copyFile(
  source: SyncFileHandle,
  destination: OpfsTree,
  path: string[],
): Promise<void> {
  const target = await destination.openHandle(path, { create: true });
  try {
    target.truncate(0);
    const size = source.getSize();
    const buffer = new Uint8Array(Math.min(size, 64 * 1024));
    for (let offset = 0; offset < size; offset += buffer.length) {
      const chunk = buffer.subarray(0, Math.min(buffer.length, size - offset));
      readFully(source, chunk, offset, "copying an upgrade source");
      writeFully(target, chunk, offset, "preparing an upgrade copy");
    }
    target.flush();
  } finally {
    target.close();
  }
}

async function prepare(tree: OpfsTree, handles: UpgradeHandles): Promise<ReadyRecord> {
  await tree.deleteTree([DIRECTORY]);
  const temporary = new OpfsTree(await tree.getDirectory([DIRECTORY], true));
  const controls = [handles.wal, handles.slotA, handles.slotB];
  const source = controls.map(fingerprint);
  let sequence = 0;
  for (const [index, handle] of [handles.slotA, handles.slotB].entries()) {
    const converted = convertCheckpoint(boundedBytes(handle, MAX_OPFS_CHECKPOINT_BYTES));
    if (converted.length > 0) sequence = Math.max(sequence, checkpointSequence(converted));
    await temporary.writeFile([index === 0 ? "checkpoint-a" : "checkpoint-b"], converted, {
      flush: true,
    });
  }
  if (handles.wal.getSize() > MAX_OPFS_WAL_BYTES)
    throw corrupt("Layout-6 WAL exceeds its recovery bound");
  let end = 0;
  // Every old WAL byte is conservatively required. There is no old acknowledgement witness
  // with which to excuse a zero-filled/torn suffix as an unacknowledged append.
  for (const frame of iterateWalFrames(handles.wal, handles.wal.getSize())) {
    const payload = frame.payload as { seq?: unknown } | null;
    if (
      payload === null ||
      typeof payload !== "object" ||
      typeof payload.seq !== "number" ||
      !Number.isSafeInteger(payload.seq) ||
      payload.seq < 1
    )
      throw corrupt("Invalid layout-6 WAL sequence");
    sequence = Math.max(sequence, payload.seq);
    end = frame.frameEnd;
  }
  if (end !== handles.wal.getSize())
    throw corrupt("Layout-6 WAL has an ambiguous incomplete suffix");
  await copyFile(handles.wal, temporary, ["wal"]);
  for await (const { path } of tree.walkFiles([])) {
    if (
      path[0] === DIRECTORY ||
      (path.length === 1 &&
        ["format.json", "wal-acknowledgements", ...CONTROL_NAMES].includes(path[0] ?? ""))
    )
      continue;
    const handle = await tree.openHandle(path, { create: false });
    try {
      await copyFile(handle, temporary, path);
    } finally {
      handle.close();
    }
  }
  await temporary.writeFile(["format.json"], MARKER, { flush: true });
  let wal: SyncFileHandle | undefined;
  let slotA: SyncFileHandle | undefined;
  let slotB: SyncFileHandle | undefined;
  let acknowledgements: SyncFileHandle | undefined;
  let leader: OpfsLeader | undefined;
  try {
    wal = await temporary.openHandle(["wal"], { create: false });
    slotA = await temporary.openHandle(["checkpoint-a"], { create: false });
    slotB = await temporary.openHandle(["checkpoint-b"], { create: false });
    acknowledgements = await temporary.openHandle(["wal-acknowledgements"], { create: true });
    const proof = new WalAcknowledgements(acknowledgements, true);
    proof.publish(sequence, end);
    // Strict validation even for a relaxed opener: conversion never blesses missing payloads.
    leader = await OpfsLeader.recover(temporary, true, { wal, slotA, slotB, acknowledgements });
    const integrity = await leader.checkIntegrity({ mode: "full" });
    if (!integrity.ok)
      throw corrupt(
        `Layout-6 integrity failed: ${integrity.issues[0]?.message ?? "unknown issue"}`,
      );
    leader.checkpointNow();
  } finally {
    if (leader !== undefined) leader.crash();
    else {
      wal?.close();
      slotA?.close();
      slotB?.close();
      acknowledgements?.close();
    }
  }
  const checkpoint = await temporary.readFile(["checkpoint-a"], {
    maxBytes: MAX_OPFS_CHECKPOINT_BYTES,
  });
  const acknowledgement = await temporary.readFile(["wal-acknowledgements"], { maxBytes: 48 });
  if (checkpoint === undefined || acknowledgement === undefined)
    throw corrupt("Prepared upgrade is missing control files");
  const preparedState = decodeSyncCheckpoint(checkpoint) as {
    extents: { tailExtentId: number; tailOffset: number };
  };
  const oldTailSize = await tree.fileSize([
    "extents",
    String(preparedState.extents.tailExtentId).padStart(6, "0"),
  ]);
  if (oldTailSize !== undefined && oldTailSize > preparedState.extents.tailOffset) {
    throw corrupt(
      "Layout-6 payload tail is beyond its validated history; refusing an ambiguous rollback",
    );
  }
  const ready: ReadyRecord = {
    from: 6,
    to: 7,
    source,
    checkpoint: bytesFingerprint(checkpoint),
    acknowledgement: bytesFingerprint(acknowledgement),
    sequence: checkpointSequence(checkpoint),
  };
  // Flush existing source payloads too. A relaxed old writer's bytes must survive power loss
  // once a layout-7 checkpoint becomes durable, not just in the temporary validation copy.
  for await (const { path } of tree.walkFiles([])) {
    if (path[0] === DIRECTORY || path.length === 1) continue;
    const handle = await tree.openHandle(path, { create: false });
    try {
      handle.flush();
    } finally {
      handle.close();
    }
  }
  for (const handle of controls) handle.flush();
  await temporary.writeFile([READY], encodeSyncCheckpoint(ready), { flush: true });
  return ready;
}

function readReady(bytes: Uint8Array): ReadyRecord {
  const decoded = decodeSyncCheckpoint(bytes) as Partial<ReadyRecord> | undefined;
  const validFingerprint = (value: unknown): value is Fingerprint => {
    if (typeof value !== "object" || value === null) return false;
    const entry = value as Partial<Fingerprint>;
    return (
      typeof entry.size === "number" &&
      Number.isSafeInteger(entry.size) &&
      entry.size >= 0 &&
      typeof entry.checksum === "number" &&
      Number.isInteger(entry.checksum) &&
      entry.checksum >= 0 &&
      entry.checksum <= 0xffffffff
    );
  };
  if (
    decoded?.from !== 6 ||
    decoded.to !== 7 ||
    !Array.isArray(decoded.source) ||
    decoded.source.length !== 3 ||
    !decoded.source.every(validFingerprint) ||
    !validFingerprint(decoded.checkpoint) ||
    !validFingerprint(decoded.acknowledgement) ||
    typeof decoded.sequence !== "number" ||
    !Number.isSafeInteger(decoded.sequence) ||
    decoded.sequence < 0
  ) {
    throw corrupt("Automatic-upgrade ready record is corrupt");
  }
  return decoded as ReadyRecord;
}

/** May return false without mutation for an ordinary current-layout open. */
export async function upgradeOpfsLayout(tree: OpfsTree, handles: UpgradeHandles): Promise<boolean> {
  const marker = await tree.readFile(["format.json"], { maxBytes: 1024 });
  const text = new TextDecoder().decode(marker);
  const current = text === new TextDecoder().decode(MARKER);
  const legacy = text === '{"formatVersion":6}';
  const readyBytes = await tree.readFile([DIRECTORY, READY], { maxBytes: 4096 });
  if (current && readyBytes === undefined) return false;
  if (!current && !legacy && readyBytes === undefined)
    throw corrupt("Missing upgrade witness for a torn marker");
  let ready: ReadyRecord;
  if (legacy) {
    // Until publication changes the marker, an older writer may have advanced the source.
    // Re-prepare under the held locks instead of installing a stale validation copy.
    try {
      ready = await prepare(tree, handles);
    } catch (error) {
      if (
        error instanceof DOMException ||
        error instanceof StorageFormatVersionError ||
        error instanceof StorageCorruptionError
      )
        throw error;
      throw corrupt(
        `Layout-6 upgrade validation failed: ${error instanceof Error ? error.message : String(error)}`,
        error,
      );
    }
  } else {
    if (readyBytes === undefined) throw corrupt("Automatic-upgrade ready record is missing");
    ready = readReady(readyBytes);
  }
  const completed = await tree.readFile([COMPLETE], { maxBytes: 4096 });
  if (completed !== undefined) {
    let receipt: ReadyRecord | undefined;
    try {
      receipt = readReady(completed);
    } catch (error) {
      if (!(error instanceof StorageCorruptionError)) throw error;
    }
    if (receipt !== undefined) {
      if (
        !current ||
        receipt.sequence !== ready.sequence ||
        !same(receipt.checkpoint, ready.checkpoint)
      )
        throw corrupt("Automatic-upgrade completion receipt is inconsistent");
      // Once the completion receipt is durable, a resurrected or partially removed staging
      // tree is cleanup debt only. Current recovery must verify current history; no replay of
      // the old prepared checkpoint is permitted, even if today's native copies are damaged.
      return true;
    }
  }

  // A completed ready record can reappear after namespace writeback or cleanup failure. Never
  // replay its checkpoint over later acknowledged writes. Both native mirrors and the native
  // witness must prove that installation already completed before that record can be discarded.
  let installed = false;
  let newestObservedSequence = ready.sequence;
  // A torn/missing marker does not erase newer native history. Inspect its checkpoints and
  // witness too; an old ready record alone is never permission to roll that history back.
  if (!legacy) {
    try {
      const checkpoints = [handles.slotA, handles.slotB].map((handle) =>
        boundedBytes(handle, MAX_OPFS_CHECKPOINT_BYTES),
      );
      const sequences = checkpoints.map((bytes) => {
        try {
          const version =
            bytes.length >= 12 ? new DataView(bytes.buffer).getUint32(8, true) : undefined;
          const sequence = checkpointSequence(version === 6 ? convertCheckpoint(bytes) : bytes);
          newestObservedSequence = Math.max(newestObservedSequence, sequence);
          return { sequence, version };
        } catch (error) {
          if (!(error instanceof StorageCorruptionError)) throw error;
          return { sequence: -1, version: undefined };
        }
      });
      const proofHandle = await tree.openHandle(["wal-acknowledgements"], { create: false });
      try {
        const proof = new WalAcknowledgements(proofHandle, false);
        newestObservedSequence = Math.max(newestObservedSequence, proof.latest.sequence);
        installed =
          current &&
          sequences.every(({ sequence, version }) => version === 7 && sequence >= ready.sequence) &&
          proof.latest.sequence >= ready.sequence;
      } finally {
        proofHandle.close();
      }
    } catch (error) {
      // Incomplete installation is repairable from the validated copy. Explicit future formats
      // remain refusals; never interpret a different upgrade as this one's interrupted write.
      if (error instanceof StorageFormatVersionError && error.actualVersion === 6) {
        // A not-yet-replaced legacy checkpoint is expected during publication.
      } else if (
        !(error instanceof StorageCorruptionError) &&
        !(error instanceof WalAcknowledgementCorruptionError) &&
        !isDomError(error, "NotFoundError")
      ) {
        throw error;
      }
    }
  }
  if (!installed) {
    if (newestObservedSequence > ready.sequence)
      throw corrupt("Newer history exists beside an interrupted upgrade; refusing a rollback");
    const temporary = new OpfsTree(await tree.getDirectory([DIRECTORY], false));
    const checkpoint = await temporary.readFile(["checkpoint-a"], {
      maxBytes: MAX_OPFS_CHECKPOINT_BYTES,
    });
    const acknowledgement = await temporary.readFile(["wal-acknowledgements"], { maxBytes: 48 });
    if (
      checkpoint === undefined ||
      acknowledgement === undefined ||
      !same(bytesFingerprint(checkpoint), ready.checkpoint) ||
      !same(bytesFingerprint(acknowledgement), ready.acknowledgement) ||
      checkpointSequence(checkpoint) !== ready.sequence
    )
      throw corrupt("Prepared upgrade control bytes are corrupt");
    const wal = fingerprint(handles.wal);
    const originalWal = ready.source[0];
    if (originalWal === undefined || (!same(wal, originalWal) && wal.size !== 0)) {
      throw corrupt("WAL changed during interrupted automatic upgrade; refusing a rollback");
    }
    // Reset followed both checkpoint flushes and a valid proof. If a nonempty source WAL
    // has disappeared, damaged native installation evidence cannot be an earlier publish
    // interruption. Do not heal that evidence from an older staging tree.
    if (wal.size === 0 && originalWal.size > 0)
      throw corrupt("Native WAL was reset without intact upgrade installation proof");
    // This is the version barrier: the old reader sees an unsupported marker before any
    // current-format native checkpoint or acknowledgement replaces its control files.
    await tree.writeFile(["format.json"], MARKER, { flush: true });
    for (const handle of [handles.slotA, handles.slotB]) {
      handle.truncate(0);
      writeFully(handle, checkpoint, 0, "publishing an automatic upgrade checkpoint");
      handle.flush();
    }
    await tree.writeFile(["wal-acknowledgements"], acknowledgement, { flush: true });
    handles.wal.truncate(0);
    handles.wal.flush();
  }
  await tree.writeFile([COMPLETE], encodeSyncCheckpoint(ready), { flush: true });
  return true;
}

/** Called only after native current-layout recovery and full integrity validation succeed. */
export async function finishOpfsUpgrade(tree: OpfsTree): Promise<void> {
  await tree.deleteTree([DIRECTORY]);
}

export function canUpgradeOpfsLayout(version: number): boolean {
  return OPFS_UPGRADES.some(({ from }) => from === version);
}

export async function hasPreparedOpfsUpgrade(tree: OpfsTree): Promise<boolean> {
  const bytes = await tree.readFile([DIRECTORY, READY], {
    maxBytes: 4096,
    lockedMeansAbsent: true,
  });
  if (bytes === undefined) return false;
  readReady(bytes);
  return true;
}
