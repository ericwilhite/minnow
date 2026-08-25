import { describe, expect, it } from "vitest";
import { crc32, encodeBlock } from "../../block-format/index.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import {
  extendSnapshotFrameStreamChecksum,
  encodeSnapshotMetadataPage,
  snapshotFrameEnvelopeParts,
  snapshotFrameStreamHeaderIdentity,
} from "../snapshot-stream.js";
import {
  MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_ENTRIES,
  type SnapshotFrame,
  type SnapshotFrameFooter,
  type SnapshotFrameStreamHeader,
  type SnapshotMetadataItem,
  SNAPSHOT_FRAME_KINDS,
} from "../types.js";
import { OpfsBlockStore } from "./index.js";

interface ExportedFrames {
  header: SnapshotFrameStreamHeader;
  frames: SnapshotFrame[];
  footer: SnapshotFrameFooter;
}

function lifetime(): { createdAt: string; expiresAt: string } {
  const created = Date.now();
  return {
    createdAt: new Date(created).toISOString(),
    expiresAt: new Date(created + 30 * 60_000).toISOString(),
  };
}

async function seed(store: OpfsBlockStore): Promise<Uint8Array> {
  const clock = lifetime();
  await store.addTable({
    id: "table",
    name: "items",
    managed: false,
    columns: [{ id: "value", name: "value", type: "number", nullable: false }],
    revision: 0,
    createdAt: clock.createdAt,
  });
  await store.beginTransaction({
    record: {
      id: "transaction",
      ownerId: "writer",
      expiresAt: clock.expiresAt,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: clock.createdAt,
      updatedAt: clock.createdAt,
      committedVersion: null,
    },
  });
  const bytes = await encodeBlock({ type: "number", values: [41, 42] }, "raw");
  const staged = await store.stageTransactionArtifacts({
    transactionId: "transaction",
    expectedRevision: 0,
    blocks: [{ id: "block", bytes }],
    segments: [
      {
        id: "segment",
        tableId: "table",
        transactionId: "transaction",
        rowCount: 2,
        rowIdStart: 1n,
        rowIdEndExclusive: 3n,
        columnBlockIds: { value: ["block"] },
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
        createdAt: clock.createdAt,
      },
    ],
    updatedAt: clock.createdAt,
  });
  await store.commitTransaction({
    transactionId: "transaction",
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: null,
    levelZeroSegmentLimits: [{ tableId: "table", limit: 4_096 }],
    committedAt: clock.createdAt,
  });
  return bytes;
}

async function exportFrames(store: OpfsBlockStore): Promise<ExportedFrames> {
  const clock = lifetime();
  const ownerId = "backup-owner";
  const session = await store.beginSnapshotFrameExport({ ownerId, ...clock });
  const frameCount = Object.values(session.header.kinds).reduce(
    (total, summary) => total + summary.frameCount,
    0,
  );
  const frames: SnapshotFrame[] = [];
  let checksum = 0;
  let itemCount = 0;
  let storedBytes = 0;
  for (let sequence = 0; sequence < frameCount; sequence += 1) {
    const frame = await store.readSnapshotExportFrame({
      sessionId: session.sessionId,
      ownerId,
      sequence,
      expiresAtCutoff: clock.createdAt,
      expiresAt: clock.expiresAt,
    });
    if (frame === undefined) throw new Error(`Missing snapshot frame ${String(sequence)}`);
    frames.push(frame);
    checksum = extendSnapshotFrameStreamChecksum(checksum, snapshotFrameEnvelopeParts(frame));
    itemCount += frame.itemCount;
    storedBytes += frame.payload.byteLength;
  }
  await store.closeSnapshotFrameExport({ sessionId: session.sessionId, ownerId });
  return {
    header: session.header,
    frames,
    footer: { frameCount, itemCount, storedBytes, checksum },
  };
}

function oversizedAcceleratorSnapshot(): ExportedFrames {
  const table = {
    kind: "table",
    record: {
      id: "snapshot-unique-table",
      name: "snapshot_unique_table",
      columns: [{ id: "key", name: "key", type: "string", nullable: false }],
      uniqueKeyColumnId: "key",
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    },
    nextRowId: 1n,
    autoIncrement: [],
  } satisfies SnapshotMetadataItem;
  const tokenCount = MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_ENTRIES + 1;
  const items: SnapshotMetadataItem[] = [
    table,
    {
      kind: "unique-generation",
      tableId: table.record.id,
      indexId: null,
      namespaceId: table.record.id,
      generationId: "snapshot-unique-generation",
      chunkCount: 1,
      tokenCount,
    },
  ];
  const frames = items.map((item, sequence): SnapshotFrame => {
    const payload = encodeSnapshotMetadataPage([item]);
    return {
      sequence,
      kind: item.kind === "table" ? "catalog-page" : "unique-page",
      itemCount: 1,
      key: null,
      payload,
      checksum: crc32(payload),
    };
  });
  const kinds = Object.fromEntries(
    SNAPSHOT_FRAME_KINDS.map((kind) => {
      const selected = frames.filter((frame) => frame.kind === kind);
      return [
        kind,
        {
          frameCount: selected.length,
          itemCount: selected.length,
          storedBytes: selected.reduce((total, frame) => total + frame.payload.byteLength, 0),
        },
      ];
    }),
  ) as SnapshotFrameStreamHeader["kinds"];
  let checksum = 0;
  for (const frame of frames) {
    checksum = extendSnapshotFrameStreamChecksum(checksum, snapshotFrameEnvelopeParts(frame));
  }
  const storedBytes = frames.reduce((total, frame) => total + frame.payload.byteLength, 0);
  return {
    header: {
      formatVersion: 1,
      databaseVersion: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
      kinds,
    },
    frames,
    footer: { frameCount: frames.length, itemCount: frames.length, storedBytes, checksum },
  };
}

describe("OPFS framed snapshots", () => {
  it("atomically purges an expired foreign import before a different header takes over", async () => {
    const source = await OpfsBlockStore.open({
      name: "snapshot-takeover-source",
      root: new MemoryOpfs().root,
      checkpointEntries: 1_000,
    });
    await seed(source);
    const prior = await exportFrames(source);
    source.close();

    const shim = new MemoryOpfs();
    const name = "snapshot-takeover-target";
    let target = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    const priorIdentity = snapshotFrameStreamHeaderIdentity(prior.header);
    await target.beginSnapshotFrameImport({
      identity: priorIdentity,
      ownerId: "expired-owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T00:30:00.000Z",
      header: prior.header,
    });
    await target.appendSnapshotImportFrames({
      identity: priorIdentity,
      ownerId: "expired-owner",
      expiresAtCutoff: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T00:30:00.000Z",
      frames: prior.frames,
    });

    const emptyHeader: SnapshotFrameStreamHeader = {
      formatVersion: 1,
      databaseVersion: 9,
      createdAt: "2026-08-24T01:00:00.000Z",
      kinds: Object.fromEntries(
        SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
      ) as SnapshotFrameStreamHeader["kinds"],
    };
    const identity = snapshotFrameStreamHeaderIdentity(emptyHeader);
    await expect(
      target.beginSnapshotFrameImport({
        identity,
        ownerId: "replacement-owner",
        createdAt: "2026-08-24T00:30:00.000Z",
        expiresAt: "2026-08-24T01:30:00.000Z",
        header: emptyHeader,
      }),
    ).resolves.toMatchObject({ identity, nextSequence: 0, stagedBytes: 0 });
    expect(await target.getBlock("block")).toBeUndefined();
    await target.finishSnapshotFrameImport({
      identity,
      ownerId: "replacement-owner",
      expiresAtCutoff: "2026-08-24T00:30:00.000Z",
      footer: { frameCount: 0, itemCount: 0, storedBytes: 0, checksum: 0 },
    });
    target._crashForTests();
    target = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    expect(await target.getBlock("block")).toBeUndefined();
    expect(await target.listTables()).toEqual([]);
    await expect(target.checkIntegrity({ mode: "full" })).resolves.toMatchObject({ ok: true });
    target.close();
  });

  it("refuses an aggregate accelerator import before atomic publication", async () => {
    const snapshot = oversizedAcceleratorSnapshot();
    const shim = new MemoryOpfs();
    const name = "snapshot-accelerator-cap";
    const clock = lifetime();
    const identity = snapshotFrameStreamHeaderIdentity(snapshot.header);
    const ownerId = "restore-owner";
    let store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    await store.beginSnapshotFrameImport({ identity, ownerId, header: snapshot.header, ...clock });
    await store.appendSnapshotImportFrames({
      identity,
      ownerId,
      expiresAtCutoff: clock.createdAt,
      expiresAt: clock.expiresAt,
      frames: snapshot.frames,
    });
    await expect(
      store.finishSnapshotFrameImport({
        identity,
        ownerId,
        expiresAtCutoff: clock.createdAt,
        footer: snapshot.footer,
      }),
    ).rejects.toMatchObject({
      name: "StorageResourceLimitError",
      resource: "snapshot accelerator entry",
    });
    expect(await store.listTables()).toEqual([]);
    store._crashForTests();
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    expect(await store.listTables()).toEqual([]);
    await expect(store.checkIntegrity({ mode: "full" })).resolves.toMatchObject({ ok: true });
    store.close();
  });

  it("publishes one validated generation and reconciles a lost finish acknowledgement", async () => {
    const source = await OpfsBlockStore.open({
      name: "snapshot-source",
      root: new MemoryOpfs().root,
      checkpointEntries: 1,
    });
    const expected = await seed(source);
    const snapshot = await exportFrames(source);
    source.close();

    const shim = new MemoryOpfs();
    const name = "snapshot-target";
    let target = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    const identity = snapshotFrameStreamHeaderIdentity(snapshot.header);
    const clock = lifetime();
    const ownerId = "restore-owner";
    const begin = { identity, ownerId, header: snapshot.header, ...clock };
    await target.beginSnapshotFrameImport(begin);
    for (let offset = 0; offset < snapshot.frames.length; offset += 2) {
      await target.appendSnapshotImportFrames({
        identity,
        ownerId,
        expiresAtCutoff: clock.createdAt,
        expiresAt: clock.expiresAt,
        frames: snapshot.frames.slice(offset, offset + 2),
      });
      if (offset === 0) {
        target._crashForTests();
        target = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
      }
    }
    await target.finishSnapshotFrameImport({
      identity,
      ownerId,
      expiresAtCutoff: clock.createdAt,
      footer: snapshot.footer,
    });
    target._crashForTests();

    target = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    expect(await target.getBlock("block")).toEqual(expected);
    await target.beginSnapshotFrameImport(begin);
    for (let offset = 0; offset < snapshot.frames.length; offset += 2) {
      await target.appendSnapshotImportFrames({
        identity,
        ownerId,
        expiresAtCutoff: clock.createdAt,
        expiresAt: clock.expiresAt,
        frames: snapshot.frames.slice(offset, offset + 2),
      });
    }
    await expect(
      target.finishSnapshotFrameImport({
        identity,
        ownerId,
        expiresAtCutoff: clock.createdAt,
        footer: snapshot.footer,
      }),
    ).resolves.toBeUndefined();
    expect(await target.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    target.close();
  });
});
