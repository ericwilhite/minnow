import { expect, it } from "vitest";
import { encodeBlock } from "../../block-format/index.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { MAX_POSTING_BUILD_TTL_MS, type TableRecord } from "../types.js";
import { decodeSyncCheckpoint, encodeSyncCheckpoint } from "../toolkit/wire.js";
import { OpfsBlockStore } from "./store.js";

interface Checkpoint {
  generation: number;
  lastSeq: number;
}

type PostingOwnerKind = "fts-column" | "secondary-index";

/** The postings storage column each owner kind builds in `checkpointedStore`'s table. */
const POSTING_COLUMN: Record<PostingOwnerKind, string> = {
  "fts-column": "posting",
  "secondary-index": "secondary-index:by-id",
};

async function waitForEmptyWal(shim: MemoryOpfs, prefix: string): Promise<void> {
  for (let attempt = 0; attempt < 400; attempt++) {
    if (shim.readFileBytes(`${prefix}wal`)?.byteLength === 0) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("The mirrored checkpoint did not finish");
}

async function checkpointedStore(
  name: string,
  ownerKind: PostingOwnerKind = "fts-column",
): Promise<{
  shim: MemoryOpfs;
  store: OpfsBlockStore;
  prefix: string;
}> {
  const shim = new MemoryOpfs();
  const prefix = `minnowdb/${name}/`;
  const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
  await store.addTable({
    id: "durable-table",
    name: "durable",
    columns: [{ id: "id", name: "id", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-09-14T00:00:00.000Z",
    ...(ownerKind === "fts-column"
      ? {
          ftsColumns: {
            posting: {
              storage: "fts-chunks-v1",
              tokenizerVersion: 1,
              state: "building",
              buildFromVersion: -1,
            },
          },
        }
      : {
          secondaryIndexes: {
            "by-id": {
              name: "durable_by_id",
              columnId: "id",
              columnIds: ["id"],
              directions: ["asc"],
              termEncoding: "tuple-v2",
              storage: "postings-v1",
              storageColumnId: POSTING_COLUMN["secondary-index"],
              locator: "row-id",
              state: "building",
              buildId: "build",
              buildFromVersion: -1,
            },
          },
        }),
  });
  await waitForEmptyWal(shim, prefix);
  return { shim, store, prefix };
}

function postingState(table: TableRecord | undefined, ownerKind: PostingOwnerKind): string {
  return (
    (ownerKind === "fts-column"
      ? table?.ftsColumns?.posting?.state
      : table?.secondaryIndexes?.["by-id"]?.state) ?? "missing"
  );
}

/** Commits one segment with no postings delta: the stale-writer rule invalidates the index. */
async function commitStaleWriterSegment(store: OpfsBlockStore): Promise<void> {
  const timestamp = "2026-09-14T00:00:01.000Z";
  const transactionId = "stale-writer";
  await store.createTransaction({
    id: transactionId,
    ownerId: "stale-writer/owner",
    expiresAt: "2026-09-14T00:30:00.000Z",
    snapshotVersion: null,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: timestamp,
    updatedAt: timestamp,
    committedVersion: null,
  });
  const created = await store.getTransaction(transactionId);
  if (created === undefined) throw new Error("The stale writer's transaction is missing");
  const staged = await store.stageTransactionArtifacts({
    transactionId,
    expectedRevision: created.revision,
    blocks: [
      { id: "stale-block", bytes: await encodeBlock({ type: "number", values: [1] }, "raw") },
    ],
    segments: [
      {
        id: "stale-segment",
        tableId: "durable-table",
        transactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        rowIdSpans: [],
        columnBlockIds: { id: ["stale-block"] },
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        createdAt: timestamp,
      },
    ],
    updatedAt: timestamp,
  });
  await store.commitTransaction({
    transactionId,
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: null,
    changedTableIds: ["durable-table"],
    levelZeroSegmentLimits: [{ tableId: "durable-table", limit: 4096 }],
    committedAt: timestamp,
  });
}

it("recovers an interrupted WAL-empty checkpoint without losing acknowledged catalog state", async () => {
  const name = "interrupted-empty-checkpoint";
  const { shim, store, prefix } = await checkpointedStore(name);
  const before = shim.readFileBytes(`${prefix}checkpoint-a`);
  expect(before).toEqual(shim.readFileBytes(`${prefix}checkpoint-b`));
  let writes = 0;
  const interruption = { reached: false };
  shim.setWriteFault((path, phase) => {
    if (path.startsWith(`${prefix}checkpoint-`) && phase === "write" && ++writes === 3) {
      // The alternate slot's truncate/write/flush finished. Refuse the mirror's truncate
      // before it mutates the older valid copy, exactly the state at this interruption point.
      interruption.reached = true;
      throw new Error("Stop before replacing the second checkpoint mirror");
    }
  });
  store.close();
  for (let attempt = 0; attempt < 400 && !interruption.reached; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  expect(interruption.reached).toBe(true);
  shim.setWriteFault(null);
  const checkpoints = ["checkpoint-a", "checkpoint-b"].map(
    (slot) =>
      decodeSyncCheckpoint(
        shim.readFileBytes(`${prefix}${slot}`) ?? new Uint8Array(),
      ) as Checkpoint,
  );
  const [first, second] = checkpoints;
  if (first === undefined || second === undefined)
    throw new Error("Expected two checkpoint mirrors");
  expect(Math.abs(first.generation - second.generation)).toBe(1);
  expect(first.lastSeq).toBe(second.lastSeq);
  expect(shim.readFileBytes(`${prefix}wal`)).toHaveLength(0);
  const reopened = await OpfsBlockStore.open({ name, root: shim.root });
  try {
    expect((await reopened.listTables()).map(({ name }) => name)).toEqual(["durable"]);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
  } finally {
    reopened.close();
  }
});

it.each(["sequence", "generation gap"] as const)(
  "refuses checkpoint %s disagreement without a WAL bridge",
  async (disagreement) => {
    const name = `invalid-checkpoint-${disagreement.replaceAll(" ", "-")}`;
    const { shim, store, prefix } = await checkpointedStore(name);
    store._crashForTests();
    const checkpoint = decodeSyncCheckpoint(
      shim.readFileBytes(`${prefix}checkpoint-a`) ?? new Uint8Array(),
    ) as Checkpoint;
    checkpoint.generation += disagreement === "sequence" ? 1 : 2;
    if (disagreement === "sequence") checkpoint.lastSeq += 1;
    shim.writeFileBytes(`${prefix}checkpoint-a`, encodeSyncCheckpoint(checkpoint));
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toMatchObject({
      name: "StorageCorruptionError",
      message:
        "opfs storage corruption at recovery: OPFS checkpoint copies disagree without a WAL bridge; refusing a silent rollback",
    });
  },
);

it.each(["renew", "append"] as const)(
  "recovers a renewed postings checkpoint after a backward clock during %s",
  async (operation) => {
    const name = `renewed-postings-${operation}`;
    const { shim, store, prefix } = await checkpointedStore(name);
    const start = Date.now();
    const time = (offset: number): string => new Date(start + offset).toISOString();
    const identity = {
      tableId: "durable-table",
      columnId: "posting",
      buildId: "build",
      ownerId: "owner",
    };
    const lease = (offset: number) => ({
      ...identity,
      expiresAtCutoff: time(offset),
      updatedAt: time(offset),
      expiresAt: time(offset + 3_600_000),
    });
    await store.beginFtsBaseBuild({ ...identity, createdAt: time(0), expiresAt: time(3_600_000) });
    await store.writeFtsBaseBuildChunk({
      ...lease(10),
      ordinal: 0,
      chunk: [{ term: "alpha", rowIds: [3n], tf: [1] }],
    });
    if (operation === "renew") await store.renewFtsBaseBuild(lease(5));
    else
      await store.writeFtsBaseBuildChunk({
        ...lease(5),
        ordinal: 1,
        chunk: [{ term: "beta", rowIds: [7n], tf: [1] }],
      });
    for (let attempt = 0; attempt < 400; attempt++) {
      if (shim.readFileBytes(`${prefix}wal`)?.byteLength === 0) break;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    expect(shim.readFileBytes(`${prefix}wal`)).toHaveLength(0);
    store._crashForTests();
    const checkpoint = decodeSyncCheckpoint(
      shim.readFileBytes(`${prefix}checkpoint-a`) ?? new Uint8Array(),
    ) as {
      ftsBuilds: Array<[string, { createdAt: string; updatedAt: string; expiresAt: string }]>;
    };
    expect(checkpoint.ftsBuilds[0]?.[1]).toMatchObject({
      createdAt: time(0),
      updatedAt: time(10),
      expiresAt: time(3_600_010),
    });
    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    try {
      const count = operation === "renew" ? 1 : 2;
      await reopened.finishFtsBaseBuild({
        ...identity,
        expiresAtCutoff: time(20),
        coversVersion: 0,
        chunkCount: count,
        totalTokens: count,
        completedAt: time(20),
      });
      expect(
        await reopened.readFtsCandidates(
          "durable-table",
          "posting",
          [
            { term: "alpha", prefix: false },
            { term: "beta", prefix: false },
          ],
          0,
        ),
      ).toMatchObject({
        rowIdsByTerm: [[3n], operation === "renew" ? [] : [7n]],
        coversVersion: 0,
      });
    } finally {
      reopened.close();
    }
  },
);

it.each([
  ["renew", "after-expiry"],
  ["renew", "before-ttl-window"],
  ["append", "after-expiry"],
  ["append", "before-ttl-window"],
] as const)(
  "refuses a postings %s whose update time is %s without writing durable state",
  async (operation, invalidUpdate) => {
    const name = `invalid-postings-${operation}-${invalidUpdate}`;
    const { shim, store, prefix } = await checkpointedStore(name);
    const start = Date.now();
    const time = (offset: number): string => new Date(start + offset).toISOString();
    const identity = {
      tableId: "durable-table",
      columnId: "posting",
      buildId: "build",
      ownerId: "owner",
    };
    await store.beginFtsBaseBuild({
      ...identity,
      createdAt: time(0),
      expiresAt: time(MAX_POSTING_BUILD_TTL_MS),
    });
    for (let attempt = 0; attempt < 400; attempt++) {
      if (shim.readFileBytes(`${prefix}wal`)?.byteLength === 0) break;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    expect(shim.readFileBytes(`${prefix}wal`)).toHaveLength(0);
    const beforeStats = await store.getStorageStats();
    const beforeCheckpoints = ["checkpoint-a", "checkpoint-b"].map((slot) =>
      shim.readFileBytes(`${prefix}${slot}`),
    );
    const expiresAt = time(MAX_POSTING_BUILD_TTL_MS + 1);
    const invalidLease = {
      ...identity,
      expiresAtCutoff: time(1),
      expiresAt,
      updatedAt: invalidUpdate === "after-expiry" ? time(MAX_POSTING_BUILD_TTL_MS + 2) : time(0),
    };
    const rejected =
      operation === "renew"
        ? store.renewFtsBaseBuild(invalidLease)
        : store.writeFtsBaseBuildChunk({
            ...invalidLease,
            ordinal: 0,
            chunk: [{ term: "rejected", rowIds: [99n], tf: [1] }],
          });
    await expect(rejected).rejects.toThrow("Posting build renewal interval is invalid");
    expect(await store.getStorageStats()).toEqual(beforeStats);
    expect(
      ["checkpoint-a", "checkpoint-b"].map((slot) => shim.readFileBytes(`${prefix}${slot}`)),
    ).toEqual(beforeCheckpoints);
    expect(shim.readFileBytes(`${prefix}wal`)).toHaveLength(0);

    await store.writeFtsBaseBuildChunk({
      ...identity,
      expiresAtCutoff: time(2),
      expiresAt: time(MAX_POSTING_BUILD_TTL_MS + 2),
      updatedAt: time(2),
      ordinal: 0,
      chunk: [{ term: "alpha", rowIds: [3n], tf: [1] }],
    });
    await store.finishFtsBaseBuild({
      ...identity,
      expiresAtCutoff: time(3),
      coversVersion: 0,
      chunkCount: 1,
      totalTokens: 1,
      completedAt: time(3),
    });
    for (let attempt = 0; attempt < 400; attempt++) {
      if (shim.readFileBytes(`${prefix}wal`)?.byteLength === 0) break;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    expect(shim.readFileBytes(`${prefix}wal`)).toHaveLength(0);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    try {
      expect(
        await reopened.readFtsCandidates(
          "durable-table",
          "posting",
          [{ term: "alpha", prefix: false }],
          0,
        ),
      ).toMatchObject({ rowIdsByTerm: [[3n]], coversVersion: 0, totalTokens: 1 });
      expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    } finally {
      reopened.close();
    }
  },
);

it.each([
  ["fts-column", "catalog update"],
  ["fts-column", "stale writer commit"],
  ["secondary-index", "catalog update"],
  ["secondary-index", "stale writer commit"],
] as const)(
  "reopens a checkpoint holding a %s build whose index a %s invalidated",
  async (ownerKind, invalidation) => {
    const name = `abandoned-${ownerKind}-build-${invalidation.replaceAll(" ", "-")}`;
    const { shim, store, prefix } = await checkpointedStore(name, ownerKind);
    const start = Date.now();
    const time = (offset: number): string => new Date(start + offset).toISOString();
    const identity = {
      tableId: "durable-table",
      columnId: POSTING_COLUMN[ownerKind],
      buildId: "build",
      ownerId: "owner",
    };
    await store.beginFtsBaseBuild({ ...identity, createdAt: time(0), expiresAt: time(3_600_000) });
    await store.writeFtsBaseBuildChunk({
      ...identity,
      expiresAtCutoff: time(1),
      expiresAt: time(3_600_001),
      updatedAt: time(1),
      ordinal: 0,
      chunk: [{ term: "alpha", rowIds: [3n], tf: [1] }],
    });
    if (invalidation === "catalog update") {
      const current = await store.getTable("durable-table");
      const fts = current?.ftsColumns?.posting;
      const index = current?.secondaryIndexes?.["by-id"];
      if (current === undefined) throw new Error("The indexed table is missing");
      if (fts !== undefined) {
        await store.updateTable(current.id, current.revision, {
          ftsColumns: { posting: { ...fts, state: "invalid" } },
        });
      } else {
        if (index === undefined) throw new Error("The secondary index is missing");
        const { buildId: _abandonedBuild, ...invalid } = index;
        void _abandonedBuild;
        await store.updateTable(current.id, current.revision, {
          secondaryIndexes: { "by-id": { ...invalid, state: "invalid" } },
        });
      }
    } else {
      await commitStaleWriterSegment(store);
    }
    await waitForEmptyWal(shim, prefix);
    store._crashForTests();
    // The live store keeps an abandoned build's lease until its owner aborts, finishes, or the
    // lease expires, so a checkpoint can hold an invalid index together with its staged build.
    const checkpoint = decodeSyncCheckpoint(
      shim.readFileBytes(`${prefix}checkpoint-a`) ?? new Uint8Array(),
    ) as { core: { tables: TableRecord[] }; ftsBuilds: Array<[string, unknown]> };
    expect(checkpoint.ftsBuilds).toHaveLength(1);
    expect(postingState(checkpoint.core.tables[0], ownerKind)).toBe("invalid");

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    try {
      expect(postingState(await reopened.getTable("durable-table"), ownerKind)).toBe("invalid");
      expect(await reopened.checkIntegrity({ mode: "full" })).toMatchObject({
        ok: true,
        issueCount: 0,
      });
      // Its owner can still release it, and recovery replays that release over the checkpoint.
      await reopened.abortFtsBaseBuild({ ...identity, expiresAtCutoff: time(2) });
    } finally {
      reopened._crashForTests();
    }
    const recovered = await OpfsBlockStore.open({ name, root: shim.root });
    try {
      expect(postingState(await recovered.getTable("durable-table"), ownerKind)).toBe("invalid");
      expect((await recovered.checkIntegrity({ mode: "full" })).ok).toBe(true);
    } finally {
      recovered.close();
    }
  },
);
