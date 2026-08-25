import { describe, expect, it } from "vitest";
import { crc32, encodeBlock } from "../block-format/index.js";
import {
  extendSnapshotFrameStreamChecksum,
  snapshotFrameEnvelopeParts,
  snapshotFrameStreamHeaderIdentity,
} from "./snapshot-stream.js";
import {
  MemoryBlockStore,
  PostingBuildConflictError,
  SnapshotImportConflictError,
  type SnapshotFrame,
  type SnapshotFrameFooter,
  type SnapshotFrameStreamHeader,
  type TableRecord,
  type TransactionRecord,
} from "./index.js";

const CREATED_AT = "2026-08-24T12:00:00.000Z";
const EXPIRES_AT = "2026-08-24T12:30:00.000Z";

function activeTransaction(id: string, snapshotVersion: number | null): TransactionRecord {
  return {
    id,
    ownerId: `${id}-owner`,
    expiresAt: EXPIRES_AT,
    snapshotVersion,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: CREATED_AT,
    updatedAt: CREATED_AT,
    committedVersion: null,
  };
}

async function publishBlock(store: MemoryBlockStore, id: string, bytes: Uint8Array): Promise<void> {
  const transaction = activeTransaction(`publish-${id}`, null);
  await store.createTransaction(transaction);
  const staged = await store.stageTransactionArtifacts({
    transactionId: transaction.id,
    expectedRevision: 0,
    blocks: [{ id, bytes }],
    segments: [],
    updatedAt: CREATED_AT,
  });
  await store.commitTransaction({
    transactionId: transaction.id,
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: null,
    removedBlockIds: [],
    committedAt: CREATED_AT,
  });
}

interface ExportedFrames {
  header: SnapshotFrameStreamHeader;
  frames: SnapshotFrame[];
  footer: SnapshotFrameFooter;
}

async function exportFrames(store: MemoryBlockStore): Promise<ExportedFrames> {
  const ownerId = "export-owner";
  const session = await store.beginSnapshotFrameExport({
    ownerId,
    createdAt: CREATED_AT,
    expiresAt: EXPIRES_AT,
  });
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
      expiresAtCutoff: "2026-08-24T12:01:00.000Z",
      expiresAt: EXPIRES_AT,
    });
    if (frame === undefined) throw new Error(`Missing exported frame ${String(sequence)}`);
    frames.push(frame);
    checksum = extendSnapshotFrameStreamChecksum(checksum, snapshotFrameEnvelopeParts(frame));
    itemCount += frame.itemCount;
    storedBytes += frame.payload.byteLength;
  }
  return {
    header: session.header,
    frames,
    footer: { frameCount, itemCount, storedBytes, checksum },
  };
}

function postingsTable(id = "articles"): TableRecord {
  return {
    id,
    name: id,
    managed: false,
    revision: 0,
    columns: [{ id: "title", name: "title", type: "string", nullable: false }],
    ftsColumns: {
      title: {
        storage: "fts-chunks-v1",
        tokenizerVersion: 1,
        state: "building",
        buildFromVersion: -1,
      },
    },
    createdAt: CREATED_AT,
  };
}

describe("MemoryBlockStore snapshot recovery hardening", () => {
  it("round-trips frames, makes acknowledged batches idempotent, and verifies completed replay", async () => {
    const source = new MemoryBlockStore();
    await source.addTable({
      id: "events",
      name: "events",
      managed: false,
      revision: 0,
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      createdAt: CREATED_AT,
    });
    const block = await encodeBlock({ type: "number", values: [1, 2, 3] });
    await publishBlock(source, "events/value/0", block);

    const live = await source.beginSnapshotFrameExport({
      ownerId: "live-export",
      createdAt: CREATED_AT,
      expiresAt: EXPIRES_AT,
    });
    await expect(
      source.beginSnapshotFrameExport({
        ownerId: "competing-export",
        createdAt: "2026-08-24T12:01:00.000Z",
        expiresAt: EXPIRES_AT,
      }),
    ).rejects.toBeInstanceOf(SnapshotImportConflictError);
    await expect(
      source.readSnapshotExportFrame({
        sessionId: live.sessionId,
        ownerId: "wrong-owner",
        sequence: 0,
        expiresAtCutoff: "2026-08-24T12:01:00.000Z",
        expiresAt: EXPIRES_AT,
      }),
    ).rejects.toBeInstanceOf(SnapshotImportConflictError);
    await expect(
      source.readSnapshotExportFrame({
        sessionId: live.sessionId,
        ownerId: live.ownerId,
        sequence: 1,
        expiresAtCutoff: "2026-08-24T12:01:00.000Z",
        expiresAt: EXPIRES_AT,
      }),
    ).rejects.toThrow("contiguous order");
    await expect(
      source.closeSnapshotFrameExport({ sessionId: live.sessionId, ownerId: "wrong-owner" }),
    ).rejects.toBeInstanceOf(SnapshotImportConflictError);
    await expect(
      source.closeSnapshotFrameExport({ sessionId: live.sessionId, ownerId: live.ownerId }),
    ).resolves.toBe(true);
    await expect(
      source.closeSnapshotFrameExport({ sessionId: live.sessionId, ownerId: live.ownerId }),
    ).resolves.toBe(false);

    const snapshot = await exportFrames(source);
    const finalFrame = snapshot.frames.at(-1);
    if (finalFrame === undefined) throw new Error("Snapshot unexpectedly has no frames");
    await expect(
      source.readSnapshotExportFrame({
        sessionId: (
          await source.beginSnapshotFrameExport({
            ownerId: "replacement-export",
            createdAt: "2026-08-24T13:00:00.000Z",
            expiresAt: "2026-08-24T13:30:00.000Z",
          })
        ).sessionId,
        ownerId: "replacement-export",
        sequence: -1,
        expiresAtCutoff: "2026-08-24T13:01:00.000Z",
        expiresAt: "2026-08-24T13:30:00.000Z",
      }),
    ).rejects.toThrow("sequence is invalid");

    const target = new MemoryBlockStore();
    const identity = snapshotFrameStreamHeaderIdentity(snapshot.header);
    await expect(
      target.beginSnapshotFrameImport({
        identity: "wrong-identity",
        ownerId: "import-owner",
        createdAt: CREATED_AT,
        expiresAt: EXPIRES_AT,
        header: snapshot.header,
      }),
    ).rejects.toThrow("identity does not match");
    const begin = {
      identity,
      ownerId: "import-owner",
      createdAt: CREATED_AT,
      expiresAt: EXPIRES_AT,
      header: snapshot.header,
    } as const;
    await expect(target.beginSnapshotFrameImport(begin)).resolves.toMatchObject({
      identity,
      nextSequence: 0,
      stagedBytes: 0,
    });
    await expect(target.beginSnapshotFrameImport(begin)).resolves.toMatchObject({ identity });
    await expect(
      target.beginSnapshotFrameImport({ ...begin, ownerId: "foreign-owner" }),
    ).rejects.toBeInstanceOf(SnapshotImportConflictError);
    await expect(
      target.renewSnapshotFrameImport({
        identity,
        ownerId: "import-owner",
        expiresAtCutoff: "2026-08-24T12:01:00.000Z",
        expiresAt: "2026-08-24T12:40:00.000Z",
      }),
    ).resolves.toMatchObject({ expiresAt: "2026-08-24T12:40:00.000Z" });

    const first = snapshot.frames[0];
    if (first === undefined) throw new Error("Snapshot unexpectedly has no first frame");
    const appendInput = {
      identity,
      ownerId: "import-owner",
      expiresAtCutoff: "2026-08-24T12:02:00.000Z",
      expiresAt: "2026-08-24T12:40:00.000Z",
      frames: [first],
    } as const;
    await expect(target.appendSnapshotImportFrames(appendInput)).resolves.toMatchObject({
      nextSequence: 1,
    });
    await expect(target.appendSnapshotImportFrames(appendInput)).resolves.toMatchObject({
      nextSequence: 1,
    });
    const changedPayload = first.payload.slice();
    changedPayload[0] = (changedPayload[0] ?? 0) ^ 1;
    await expect(
      target.appendSnapshotImportFrames({
        ...appendInput,
        frames: [{ ...first, payload: changedPayload, checksum: crc32(changedPayload) }],
      }),
    ).rejects.toBeInstanceOf(SnapshotImportConflictError);
    const second = snapshot.frames[1];
    if (second === undefined) throw new Error("Snapshot unexpectedly has only one frame");
    await expect(
      target.appendSnapshotImportFrames({
        ...appendInput,
        frames: [{ ...second, sequence: second.sequence + 1 }],
      }),
    ).rejects.toThrow("not the next contiguous sequence");
    await target.appendSnapshotImportFrames({
      ...appendInput,
      frames: snapshot.frames.slice(1),
    });
    await expect(
      target.finishSnapshotFrameImport({
        identity,
        ownerId: "import-owner",
        expiresAtCutoff: "2026-08-24T12:03:00.000Z",
        footer: { ...snapshot.footer, itemCount: snapshot.footer.itemCount + 1 },
      }),
    ).rejects.toThrow("footer does not match");
    await target.finishSnapshotFrameImport({
      identity,
      ownerId: "import-owner",
      expiresAtCutoff: "2026-08-24T12:03:00.000Z",
      footer: snapshot.footer,
    });
    await expect(target.getCurrentManifestVersion()).resolves.toBe(0);
    await expect(target.getTable("events")).resolves.toMatchObject({ name: "events" });
    await expect(target.getBlock("events/value/0")).resolves.toEqual(block);

    const replay = await target.beginSnapshotFrameImport(begin);
    expect(replay.nextSequence).toBe(0);
    await target.appendSnapshotImportFrames({ ...appendInput, frames: snapshot.frames });
    await target.finishSnapshotFrameImport({
      identity,
      ownerId: "import-owner",
      expiresAtCutoff: "2026-08-24T12:03:00.000Z",
      footer: snapshot.footer,
    });
    await target.beginSnapshotFrameImport(begin);
    await expect(
      target.cancelSnapshotFrameImport({ identity, ownerId: "import-owner" }),
    ).resolves.toEqual({ identity, removedBlockCount: 0, removedBytes: 0 });
    source.close();
    target.close();
  });

  it("reports and atomically aborts an interrupted import with staged physical bytes", async () => {
    const source = new MemoryBlockStore();
    const block = await encodeBlock({ type: "string", values: ["recover"] });
    await publishBlock(source, "recovery-block", block);
    const snapshot = await exportFrames(source);
    const identity = snapshotFrameStreamHeaderIdentity(snapshot.header);
    const blockStart = snapshot.frames.findIndex((frame) => frame.kind === "block");
    expect(blockStart).toBeGreaterThanOrEqual(0);

    const target = new MemoryBlockStore();
    await target.beginSnapshotFrameImport({
      identity,
      ownerId: "interrupted-owner",
      createdAt: CREATED_AT,
      expiresAt: EXPIRES_AT,
      header: snapshot.header,
    });
    await target.appendSnapshotImportFrames({
      identity,
      ownerId: "interrupted-owner",
      expiresAtCutoff: "2026-08-24T12:01:00.000Z",
      expiresAt: EXPIRES_AT,
      frames: snapshot.frames,
    });
    await expect(target.inspectInterruptedImport()).resolves.toMatchObject({
      identity,
      stagedBlockCount: 1,
    });
    await expect(target.abortInterruptedImport("wrong-identity")).rejects.toThrow(
      "Interrupted snapshot import not found",
    );
    await expect(target.abortInterruptedImport(identity)).resolves.toMatchObject({
      identity,
      removedBlockCount: 1,
      removedBytes: block.byteLength,
    });
    await expect(target.getLogicalStorageBytes()).resolves.toBe(0);
    source.close();
    target.close();
  });
});

describe("MemoryBlockStore postings build hardening", () => {
  it("enforces live ownership, exact chunk replay, ordering, publication, and abort cleanup", async () => {
    const store = new MemoryBlockStore();
    await store.addTable(postingsTable());
    const begin = {
      tableId: "articles",
      columnId: "title",
      buildId: "postings-build",
      ownerId: "postings-owner",
      createdAt: CREATED_AT,
      expiresAt: EXPIRES_AT,
    } as const;
    await store.beginFtsBaseBuild(begin);
    await expect(store.beginFtsBaseBuild(begin)).resolves.toBeUndefined();
    await expect(
      store.beginFtsBaseBuild({ ...begin, buildId: "foreign-build", ownerId: "foreign-owner" }),
    ).rejects.toBeInstanceOf(PostingBuildConflictError);
    await expect(
      store.renewFtsBaseBuild({
        ...begin,
        ownerId: "foreign-owner",
        expiresAtCutoff: "2026-08-24T12:01:00.000Z",
        expiresAt: "2026-08-24T12:40:00.000Z",
        updatedAt: "2026-08-24T12:01:00.000Z",
      }),
    ).rejects.toBeInstanceOf(PostingBuildConflictError);
    await store.renewFtsBaseBuild({
      ...begin,
      expiresAtCutoff: "2026-08-24T12:01:00.000Z",
      expiresAt: "2026-08-24T12:40:00.000Z",
      updatedAt: "2026-08-24T12:01:00.000Z",
    });

    const chunk = [{ term: "alpha", rowIds: [1n], tf: [2] }];
    const append = {
      ...begin,
      expiresAtCutoff: "2026-08-24T12:02:00.000Z",
      expiresAt: "2026-08-24T12:40:00.000Z",
      updatedAt: "2026-08-24T12:02:00.000Z",
      ordinal: 0,
      chunk,
    } as const;
    await expect(store.writeFtsBaseBuildChunk({ ...append, ordinal: -1 })).rejects.toThrow(
      "ordinal is invalid",
    );
    await store.writeFtsBaseBuildChunk(append);
    await expect(store.writeFtsBaseBuildChunk(append)).resolves.toBeUndefined();
    await expect(
      store.writeFtsBaseBuildChunk({
        ...append,
        chunk: [{ term: "changed", rowIds: [1n], tf: [1] }],
      }),
    ).rejects.toBeInstanceOf(PostingBuildConflictError);
    await expect(
      store.writeFtsBaseBuildChunk({
        ...append,
        ordinal: 2,
        chunk: [{ term: "omega", rowIds: [2n], tf: [1] }],
      }),
    ).rejects.toThrow("out of order");
    await expect(
      store.finishFtsBaseBuild({
        ...begin,
        expiresAtCutoff: "2026-08-24T12:03:00.000Z",
        completedAt: "2026-08-24T12:03:00.000Z",
        coversVersion: 0,
        chunkCount: 2,
        totalTokens: 1,
      }),
    ).rejects.toThrow("incomplete");
    await expect(
      store.finishFtsBaseBuild({
        ...begin,
        expiresAtCutoff: "2026-08-24T12:03:00.000Z",
        completedAt: "2026-08-24T12:03:00.000Z",
        coversVersion: -2,
        chunkCount: 1,
        totalTokens: 1,
      }),
    ).rejects.toThrow("metadata is invalid");
    await store.finishFtsBaseBuild({
      ...begin,
      expiresAtCutoff: "2026-08-24T12:03:00.000Z",
      completedAt: "2026-08-24T12:03:00.000Z",
      coversVersion: 0,
      chunkCount: 1,
      totalTokens: 2,
    });
    await expect(
      store.readFtsCandidates("articles", "title", [{ term: "alpha", prefix: false }], 0),
    ).resolves.toMatchObject({ rowIdsByTerm: [[1n]], coversVersion: 0, totalTokens: 2 });

    await store.updateTable("articles", 0, {
      ftsColumns: {
        title: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "building",
          buildFromVersion: 0,
        },
      },
    });
    await store.beginFtsBaseBuild({ ...begin, buildId: "abort-build", ownerId: "abort-owner" });
    await expect(
      store.abortFtsBaseBuild({
        tableId: "articles",
        columnId: "title",
        buildId: "abort-build",
        ownerId: "foreign-owner",
        expiresAtCutoff: "2026-08-24T12:04:00.000Z",
      }),
    ).rejects.toBeInstanceOf(PostingBuildConflictError);
    await expect(
      store.abortFtsBaseBuild({
        tableId: "articles",
        columnId: "title",
        buildId: "abort-build",
        ownerId: "abort-owner",
        expiresAtCutoff: "2026-08-24T12:04:00.000Z",
      }),
    ).resolves.toBeUndefined();
    await expect(
      store.abortFtsBaseBuild({
        tableId: "articles",
        columnId: "title",
        buildId: "abort-build",
        ownerId: "abort-owner",
        expiresAtCutoff: "2026-08-24T12:04:00.000Z",
      }),
    ).resolves.toBeUndefined();
    store.close();
  });
});
