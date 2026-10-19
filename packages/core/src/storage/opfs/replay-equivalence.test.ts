/**
 * The state a leader reaches live equals the state the next leader rebuilds from checkpoint plus
 * WAL replay. The live path runs large commits through `prepareCommit` with lease operations in
 * the lane between prepare and apply, renumbers staged frames, carries follower identities, and
 * checkpoints mid-way; replay reconstructs through `iterateWalFramesSliced`, `prepareCommit` and
 * apply. The two RecordCore dumps must be byte-identical, and both states must pass a full
 * integrity check.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { RecordCore } from "../toolkit/record-core.js";
import { walStagingTestHooks, type OpfsLeader } from "./leader.js";
import { encodeBlock } from "../../block-format/index.js";
import type { LeaseRecord, TableRecord } from "../types.js";
import { leaderHandles, leaderTable, walFrameRanges } from "./leader-test-helpers.js";

const DEFAULT_PIECE_BYTES = walStagingTestHooks.pieceBytes;
afterEach(() => {
  walStagingTestHooks.pieceBytes = DEFAULT_PIECE_BYTES;
  vi.restoreAllMocks();
});

const t0 = "2026-08-19T00:00:00.000Z";
const t1 = "2026-08-19T00:30:00.000Z";

function keyed(name: string): TableRecord {
  return { ...leaderTable(name), uniqueKeyColumnId: "c1" };
}

function lease(id: string, manifestVersion: number | null): LeaseRecord {
  return {
    id,
    kind: "reader",
    manifestVersion,
    ownerId: "reader",
    createdAt: t0,
    expiresAt: t1,
    revision: 0,
  };
}

async function probe(leader: OpfsLeader): Promise<{ catalogEpoch: number; schemaEpoch: number }> {
  return (await (
    leader as unknown as { _readCoreGenerated(m: string, a: unknown[]): Promise<unknown> }
  )._readCoreGenerated("getCatalogProbe", [])) as { catalogEpoch: number; schemaEpoch: number };
}

async function dump(leader: OpfsLeader): Promise<unknown> {
  return (
    leader as unknown as { _readCoreGenerated(m: string, a: unknown[]): Promise<unknown> }
  )._readCoreGenerated("dump", []);
}

function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) =>
    typeof item === "bigint" ? `${item.toString()}n` : item,
  );
}

let txOrdinal = 0;
async function twoPhaseCommit(
  leader: OpfsLeader,
  tableId: string,
  keyTokens: string[],
  options: { remove?: boolean; expectedManifestVersion: number | null; serve?: boolean },
): Promise<number> {
  txOrdinal += 1;
  const transactionId = `tx-${String(txOrdinal)}`;
  await leader._loggedGenerated("createTransaction", [
    {
      id: transactionId,
      ownerId: `owner-${transactionId}`,
      expiresAt: t1,
      snapshotVersion: options.expectedManifestVersion,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: t0,
      updatedAt: t0,
      committedVersion: null,
    },
  ]);
  const blockId = `${transactionId}-block`;
  const staged = await leader.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks: [
      { id: blockId, bytes: await encodeBlock({ type: "number", values: [txOrdinal] }, "raw") },
    ],
    segments: [
      {
        id: `${transactionId}-segment`,
        tableId,
        transactionId,
        rowCount: 1,
        rowIdStart: BigInt(txOrdinal),
        rowIdEndExclusive: BigInt(txOrdinal + 1),
        columnBlockIds: { c1: [blockId] },
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
        createdAt: t0,
      },
    ],
    updatedAt: t0,
  });
  if (options.serve === true) {
    leader.servingRequest = {
      key: `follower:${transactionId}`,
      method: "commitTransaction",
      signature: `sig-${transactionId}`,
      requestBytes: 32,
      sentAt: Date.now(),
    };
  }
  const summary = (await leader._loggedGenerated("commitTransaction", [
    {
      transactionId,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: options.expectedManifestVersion,
      levelZeroSegmentLimits: [{ tableId, limit: 4096 }],
      uniqueKeyChanges: [
        {
          tableId,
          keyTokens,
          requireAbsent: options.remove !== true,
          ...(options.remove === true ? { remove: true } : {}),
        },
      ],
      committedAt: t1,
    },
  ])) as { version: number };
  if (options.serve === true) {
    leader.servingRequest = undefined;
    await leader.completeServed(`follower:${transactionId}`, summary);
  }
  return summary.version;
}

describe("live state equals checkpoint + WAL replay", () => {
  it("across staged commits, lane leases, served identities, writes, and a mid-way checkpoint", async () => {
    walStagingTestHooks.pieceBytes = 4096;
    const shim = new MemoryOpfs();
    const h = await leaderHandles(shim, "replay-eq");
    const leader = await h.recover({ strict: true });
    await leader.addTable(keyed("k"));
    await leader.addTable(leaderTable("p"));
    await leader.addTable(leaderTable("gone"));
    // A lease logged in the lane while the first large commit prepares, and one while it encodes.
    // Keep the original receiver: the implementation below calls it on the intercepted core.
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const original = RecordCore.prototype.prepareCommit;
    let lanePending: Promise<unknown> | undefined;
    let hooked = false;
    vi.spyOn(RecordCore.prototype, "prepareCommit").mockImplementation(async function (
      this: RecordCore,
      input,
      pause,
    ) {
      let first = !hooked;
      hooked = true;
      await original.call(this, input, async () => {
        if (first) {
          first = false;
          lanePending = Promise.all([
            leader._loggedGenerated("createLease", [lease("lane-a", null)]),
            leader._loggedGenerated("createLease", [lease("lane-b", null)]),
          ]);
          await lanePending;
        }
        await pause();
      });
    });
    const keys = (from: number, count: number) =>
      Array.from({ length: count }, (_, i) => `number:${String(from + i)}`);
    let version = await twoPhaseCommit(leader, "table-k", keys(0, 20_000), {
      expectedManifestVersion: null,
      serve: true,
    });
    expect(lanePending).toBeDefined();
    await lanePending;
    version = await twoPhaseCommit(leader, "table-k", keys(20_000, 10), {
      expectedManifestVersion: version,
    });
    await leader._loggedGenerated("renewLease", [
      { id: "lane-a", expectedRevision: 0, expiresAtCutoff: t0, expiresAt: t1 },
    ]);
    await leader._loggedGenerated("reserveRowIds", ["table-p", 7]);
    // Single-shot writes with level-zero segments on the plain table.
    for (let i = 0; i < 3; i += 1) {
      const id = `w-${String(i)}`;
      const bytes = await encodeBlock({ type: "number", values: [i, i + 1] }, "raw");
      const summary = (await leader.writeTransaction({
        transaction: {
          record: {
            id,
            ownerId: `owner-${id}`,
            schemaEpochGuard: (await probe(leader)).schemaEpoch,
            expiresAt: t1,
            pendingBlockIds: [],
            pendingSegmentIds: [],
            status: "active",
            revision: 0,
            startedAt: t0,
            updatedAt: t0,
            committedVersion: null,
          },
        },
        blocks: [{ id: `${id}-block`, bytes }],
        segments: [
          {
            id: `${id}-segment`,
            tableId: "table-p",
            transactionId: id,
            rowCount: 2,
            rowIdStart: BigInt(1 + i * 2),
            rowIdEndExclusive: BigInt(3 + i * 2),
            columnBlockIds: { c1: [`${id}-block`] },
            kind: "insert",
            level: 0,
            logicalOrder: i,
            commitOrdinal: 0,
            rowIdSpans: [],
            createdAt: t0,
          },
        ],
        expectedManifestVersion: version,
        changedTableIds: ["table-p"],
        levelZeroSegmentLimits: [{ tableId: "table-p", limit: 4096 }],
        committedAt: t1,
      })) as { version: number };
      version = summary.version;
    }
    leader.checkpointNow();
    // After the checkpoint: a large removal (staged), another lane-less commit, lease churn,
    // a staged-then-rolled-back journal, and a dropped table.
    version = await twoPhaseCommit(leader, "table-k", keys(0, 20_000), {
      expectedManifestVersion: version,
      remove: true,
      serve: true,
    });
    version = await twoPhaseCommit(leader, "table-k", keys(0, 5), {
      expectedManifestVersion: version,
    });
    await leader._loggedGenerated("removeLease", [{ id: "lane-b", ownerId: "reader" }]);
    await leader._loggedGenerated("createLease", [lease("late", version)]);
    txOrdinal += 1;
    const rolled = `tx-${String(txOrdinal)}`;
    await leader._loggedGenerated("createTransaction", [
      {
        id: rolled,
        ownerId: "owner-rolled",
        expiresAt: t1,
        snapshotVersion: version,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: t0,
        updatedAt: t0,
        committedVersion: null,
      },
    ]);
    const stagedRolled = await leader.stageTransactionArtifacts({
      transactionId: rolled,
      expectedRevision: 0,
      blocks: [
        { id: "rolled-block", bytes: await encodeBlock({ type: "number", values: [9] }, "raw") },
      ],
      segments: [],
      updatedAt: t0,
    });
    await leader.rollbackTransactionArtifacts({
      transactionId: rolled,
      expectedRevision: stagedRolled.revision,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      removeBlockIds: ["rolled-block"],
      removeSegmentIds: [],
      updatedAt: t1,
    });
    const epoch = (await probe(leader)).catalogEpoch;
    await leader.dropTable({
      tableId: "table-gone",
      expectedTableRevision: 0,
      expectedManifestVersion: version,
      expectedCatalogEpoch: epoch,
      committedAt: t1,
    });
    // Sanity: the log really holds continuation frames and a served identity after the checkpoint.
    const walMagics = walFrameRanges(
      shim.readFileBytes("minnowdb/replay-eq/wal") ?? new Uint8Array(),
    ).map((f) => f.magic);
    expect(walMagics.filter((m) => m === "MNWC").length).toBeGreaterThan(10);
    expect(leader.servedOutcome("follower:tx-3")?.settled).toBe(true);
    const live = canonical(await dump(leader));
    const liveIntegrity = await leader.checkIntegrity({ mode: "full" });
    leader.crash();
    const reopened = await h.recover({ strict: true });
    const replayed = canonical(await dump(reopened));
    expect(reopened.servedOutcome("follower:tx-3")?.settled).toBe(true);
    expect(reopened.servedOutcome("follower:tx-1")?.settled).toBe(true);
    const replayedIntegrity = await reopened.checkIntegrity({ mode: "full" });
    reopened.crash();
    expect(liveIntegrity.ok).toBe(true);
    expect(replayedIntegrity.ok).toBe(true);
    if (live !== replayed) {
      // Show the first divergence rather than two megabytes of JSON.
      let at = 0;
      while (at < live.length && live[at] === replayed[at]) at += 1;
      console.log(
        "divergence at",
        at,
        "\nlive:",
        live.slice(Math.max(0, at - 200), at + 300),
        "\nreplay:",
        replayed.slice(Math.max(0, at - 200), at + 300),
      );
    }
    expect(replayed.length).toBe(live.length);
    expect(replayed === live).toBe(true);
  }, 60_000);
});
