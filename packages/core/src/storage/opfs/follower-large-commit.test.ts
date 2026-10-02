/**
 * A follower's commit reaches the leader as one request. Its key and index changes were sent
 * as structured values, and a large write ran past the message's structural limit
 * (`OPFS RPC value exceeds its structural limit`, about 367,000 indexed rows). They now travel
 * as record JSON in pieces, and the leader merges them back. These pin that a follower can
 * write what the leader can, that the request keeps one identity across re-sends, and that
 * what it wrote survives a crash.
 */
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { heavyTestTimeout } from "../../engine/storage-test-helpers.js";
import { encodeBlock } from "../../block-format/index.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore } from "./index.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

async function waitFor(condition: () => boolean, what: string): Promise<void> {
  for (let attempt = 0; attempt < 2_000; attempt += 1) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error(`Timed out waiting for ${what}`);
}

async function pair(name: string): Promise<{
  shim: MemoryOpfs;
  leader: OpfsBlockStore;
  follower: OpfsBlockStore;
}> {
  const shim = new MemoryOpfs();
  const leader = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 30_000 });
  const follower = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 30_000 });
  await follower.listTables();
  expect(leader._isLeaderForTests()).toBe(true);
  expect(follower._isLeaderForTests()).toBe(false);
  return { shim, leader, follower };
}

describe("large commits from a follower", () => {
  it("writes more indexed rows than one message could carry", async () => {
    const name = "follower-large";
    const { shim, leader, follower } = await pair(name);
    const db = new MinnowDatabase(follower, { autoCompact: false, autoCollect: false });
    await db.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
      ],
    });
    await db.execute("CREATE INDEX items_amount ON items (amount)");
    const count = 400_000;
    await db.insertBatch(
      "items",
      Array.from({ length: count }, (_, id) => ({ id, amount: id % 100 })),
    );
    expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: count }]);
    expect((await db.query("SELECT COUNT(*) AS n FROM items WHERE amount = 42")).rows).toEqual([
      { n: count / 100 },
    ]);
    await expect(db.insertBatch("items", [{ id: 5, amount: 1 }])).rejects.toThrow(
      /unique|duplicate/i,
    );
    await db.close();
    follower.close();
    leader._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const reader = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect((await reader.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: count }]);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    await reader.close();
    reopened.close();
  });

  it("runs a re-sent large commit once, under the identity of its first delivery", async () => {
    const name = "follower-resend";
    const { leader, follower } = await pair(name);
    const createdAt = new Date().toISOString();
    const later = new Date(Date.now() + 60 * 60_000).toISOString();
    await follower.addTable({
      id: "keyed",
      name: "keyed",
      columns: [{ id: "id", name: "id", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt,
      uniqueKeyColumnId: "id",
    });
    const transaction = await follower.beginTransaction({
      record: {
        id: "resent",
        ownerId: "owner",
        expiresAt: later,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: createdAt,
        updatedAt: createdAt,
        committedVersion: null,
      },
    });
    const staged = await follower.stageTransactionArtifacts({
      transactionId: transaction.record.id,
      expectedRevision: transaction.record.revision,
      blocks: [{ id: "block", bytes: await encodeBlock({ type: "number", values: [1] }, "raw") }],
      segments: [
        {
          id: "segment",
          tableId: "keyed",
          transactionId: transaction.record.id,
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { id: ["block"] },
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt,
        },
      ],
      updatedAt: createdAt,
    });
    const version = await follower.getCurrentManifestVersion();
    const release = leader._holdServedMutationsForTests();
    const committing = follower.commitTransaction({
      transactionId: transaction.record.id,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: version,
      levelZeroSegmentLimits: [{ tableId: "keyed", limit: 4096 }],
      uniqueKeyChanges: [
        {
          tableId: "keyed",
          keyTokens: Array.from({ length: 40_000 }, (_, index) => `number:${String(index)}`),
          requireAbsent: true,
        },
      ],
      committedAt: later,
    });
    await waitFor(
      () => leader._residentStateForTests().inFlightMutations === 1,
      "the leader to admit the commit",
    );
    // The same request again, pieces and all: it attaches to the one execution.
    follower._resendOldestPendingForTests();
    release();
    const summary = await committing;
    // A second execution would have refused the spent transaction; the one outcome stands.
    expect(summary.version).toBe(version === null ? 0 : version + 1);
    expect(await leader.getCurrentManifestVersion()).toBe(summary.version);
    expect(await leader.getExistingUniqueKeys("keyed", ["number:0", "number:39999"])).toEqual([
      "number:0",
      "number:39999",
    ]);
    expect(follower._residentStateForTests().pendingRequests).toBe(0);
    follower.close();
    leader.close();
  });
});
