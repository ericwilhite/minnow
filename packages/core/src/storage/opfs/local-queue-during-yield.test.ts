/**
 * The leader's own mutations, queued behind another of its operations, when a foreground bid
 * arrives and the leader yields.
 *
 * The served (follower) path re-sends a declined request. The local path has no requester to
 * re-send, so the store itself must carry a queued local write across the yield: it runs on the
 * new leader through the normal follower route, and never surfaces "connection is closed" from
 * a store that is still open.
 *
 * The gap is real in browsers: `stageTransactionArtifacts` awaits `createSyncAccessHandle`
 * (IPC) when the extent tail seals. The shim answers in a microtask, so the test stretches that
 * one open into a 60 ms macrotask delay.
 */
import { expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import type { OpfsBlockStore } from "./store.js";
import {
  delaySyncHandleOpens,
  opener,
  outcomeMessage,
  sleep,
  table,
  waitFor,
} from "./coordination-helpers.js";

async function stageBlock(store: OpfsBlockStore, id: string, bytes: Uint8Array): Promise<unknown> {
  const transactionId = `tx-${id}`;
  await store.beginTransaction({
    record: {
      id: transactionId,
      ownerId: `owner-${id}`,
      expiresAt: "2026-08-24T01:00:00.000Z",
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    },
  });
  return store.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks: [{ id, bytes }],
    segments: [],
    updatedAt: "2026-08-24T00:00:01.000Z",
  });
}

it("a hidden leader's own queued writes survive yielding to a foreground bidder", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "local-yield", { handoverGraceMs: 50 });
  const background = await open();
  const foreground = await open();
  // Extent files are named by zero-padded id ("000001"); the WAL and checkpoints are not.
  const restore = await delaySyncHandleOpens(shim, (name) => /^\d{6}$/.test(name), 60);
  try {
    await background.addTable(table("seed"));
    await foreground.getCurrentManifestVersion();
    // Fill the tail past the 8 MiB seal point so the next stage must open a new extent handle.
    await stageBlock(background, "filler", new Uint8Array(8 * 1024 * 1024 + 1).fill(1));
    // First local op: inside the leader's queue, awaiting the (delayed) extent open.
    const first = outcomeMessage(stageBlock(background, "during", new Uint8Array(16).fill(2)));
    await sleep(5);
    // Second local op: queued at the mutation turn behind the first.
    const second = outcomeMessage(background.addTable(table("queued")));
    await sleep(5);
    // The user switches to the other tab; it bids; the leader yields while both are queued.
    foreground.setForeground(true);
    const results = await Promise.all([first, second]);
    await waitFor(() => foreground._isLeaderForTests(), "the bidder to take over");
    expect(results).toEqual(["ok", "ok"]);
    expect(background._residentStateForTests().closed).toBe(false);
    const names = (await foreground.listTables()).map((record) => record.name).sort();
    expect(names).toEqual(["queued", "seed"]);
    expect(await foreground.getBlock("during")).toEqual(new Uint8Array(16).fill(2));
  } finally {
    restore();
    background.close();
    foreground.close();
  }
});
