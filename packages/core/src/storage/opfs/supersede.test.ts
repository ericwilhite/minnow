/**
 * A newer build opening a database that tabs on an older build hold.
 *
 * The newer opener asks every older-layout connection to step aside. Each one finishes the work
 * it already accepted — its own (`onSuperseding`) and, for a leader, its followers' — stops
 * leading for good, closes as superseded, and says so (`onSuperseded`). The opener then upgrades
 * at once instead of waiting out its dispatch budget. Only a connection that answers nothing (a
 * frozen tab, or a build before the request existed) still blocks it, and that is reported as a
 * typed `StorageUpgradeBlockedError`.
 *
 * One build plays both sides: `layoutVersion` makes a connection run the previous layout, as an
 * older build's would, on a database whose marker names it.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { StorageConnectionClosedError } from "../types.js";
import { sleep, table, waitFor } from "./coordination-helpers.js";
import { parseStoreRpcMessage } from "./rpc.js";
import { OpfsBlockStore, type OpfsBlockStoreOptions } from "./store.js";
import { OPFS_LAYOUT_VERSION } from "./upgrades.js";

const OLDER = OPFS_LAYOUT_VERSION - 1;
const decode = (bytes: Uint8Array | undefined): string => new TextDecoder().decode(bytes);

function olderTab(
  shim: MemoryOpfs,
  name: string,
  options: Partial<OpfsBlockStoreOptions> = {},
): {
  open: () => Promise<OpfsBlockStore>;
  superseded: () => number;
} {
  let superseded = 0;
  return {
    open: () =>
      OpfsBlockStore.open({
        name,
        root: shim.root,
        layoutVersion: OLDER,
        rpcTimeoutMs: 200,
        onSuperseded: () => {
          superseded += 1;
        },
        ...options,
      }),
    superseded: () => superseded,
  };
}

async function failure(work: Promise<unknown>): Promise<unknown> {
  return work.then(
    () => "ok",
    (error: unknown) => error,
  );
}

describe("a newer build opening a database older tabs hold", () => {
  it("has the older leader and follower step aside, and upgrades at once", async () => {
    const shim = new MemoryOpfs();
    const leaderTab = olderTab(shim, "aside");
    const followerTab = olderTab(shim, "aside");
    const leader = await leaderTab.open();
    const follower = await followerTab.open();
    await leader.addTable(table("seed"));
    await follower.getCurrentManifestVersion();
    expect(leader._isLeaderForTests()).toBe(true);
    expect(decode(shim.readFileBytes("minnowdb/aside/format.json"))).toBe(
      JSON.stringify({ formatVersion: OLDER }),
    );

    const started = Date.now();
    // A budget short enough that an opener nobody steps aside for fails with the typed error.
    const newer = await OpfsBlockStore.open({
      name: "aside",
      root: shim.root,
      dispatchBudgetMs: 2_000,
    });
    try {
      // Well inside the ten-second dispatch budget the opener used to spend and then fail.
      expect(Date.now() - started).toBeLessThan(2_000);
      expect(decode(shim.readFileBytes("minnowdb/aside/format.json"))).toBe(
        JSON.stringify({ formatVersion: OPFS_LAYOUT_VERSION }),
      );
      expect(newer._isLeaderForTests()).toBe(true);
      expect((await newer.listTables()).map(({ name }) => name)).toEqual(["seed"]);
      await waitFor(
        () => leaderTab.superseded() === 1 && followerTab.superseded() === 1,
        "both older tabs to report",
      );
      for (const old of [leader, follower]) {
        expect(old._isLeaderForTests()).toBe(false);
        expect(await failure(old.listTables())).toMatchObject({
          name: "StorageConnectionClosedError",
          reason: "superseded",
        });
      }
      // It stays the leader: neither older tab tried to take the database back.
      await sleep(300);
      expect(newer._isLeaderForTests()).toBe(true);
    } finally {
      newer._crashForTests();
    }
  });

  it("lets the older tabs finish writing what they accepted before letting go", async () => {
    const shim = new MemoryOpfs();
    const tabs: { leader?: OpfsBlockStore; follower?: OpfsBlockStore } = {};
    const drained: string[] = [];
    // The worker host flushes buffered writers here: rows the tab accepted still get written.
    const leaderTab = olderTab(shim, "drain", {
      onSuperseding: async () => {
        await tabs.leader?.addTable(table("leader-accepted"));
        drained.push("leader");
      },
    });
    const followerTab = olderTab(shim, "drain", {
      onSuperseding: async () => {
        await tabs.follower?.addTable(table("follower-accepted"));
        drained.push("follower");
      },
    });
    const leader = await leaderTab.open();
    const follower = await followerTab.open();
    tabs.leader = leader;
    tabs.follower = follower;
    await leader.addTable(table("seed"));
    // A follower write in flight when the newer build arrives is answered, not left uncertain.
    const inFlight = follower.addTable(table("in-flight"));
    // A budget short enough that an opener nobody steps aside for fails with the typed error.
    const newer = await OpfsBlockStore.open({
      name: "drain",
      root: shim.root,
      dispatchBudgetMs: 2_000,
    });
    try {
      await expect(inFlight).resolves.toBeUndefined();
      expect(drained.sort()).toEqual(["follower", "leader"]);
      expect((await newer.listTables()).map(({ name }) => name).sort()).toEqual([
        "follower-accepted",
        "in-flight",
        "leader-accepted",
        "seed",
      ]);
      expect(await newer.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    } finally {
      newer._crashForTests();
    }
  });

  it("answers a follower's write that was in flight when the request arrived", async () => {
    const shim = new MemoryOpfs();
    const leader = await olderTab(shim, "in-flight").open();
    const followerTab = olderTab(shim, "in-flight");
    const follower = await followerTab.open();
    await leader.addTable(table("seed"));
    // Called, not yet sent: the follower is still finding the leader when the newer tab asks.
    const inFlight = follower.addTable(table("in-flight"));
    const newer = await OpfsBlockStore.open({
      name: "in-flight",
      root: shim.root,
      dispatchBudgetMs: 2_000,
    });
    try {
      await expect(inFlight).resolves.toBeUndefined();
      expect((await newer.listTables()).map(({ name }) => name).sort()).toEqual([
        "in-flight",
        "seed",
      ]);
      await waitFor(() => followerTab.superseded() === 1, "the follower to step aside");
    } finally {
      newer._crashForTests();
    }
  });

  it("reports an older tab that answers nothing as StorageUpgradeBlockedError", async () => {
    const shim = new MemoryOpfs();
    const frozenTab = olderTab(shim, "frozen");
    const frozen = await frozenTab.open();
    await frozen.addTable(table("seed"));
    // A frozen background tab: it holds the handles and hears nothing.
    const resume = frozen._pauseCoordinationForTests();
    const outcome = await failure(
      OpfsBlockStore.open({ name: "frozen", root: shim.root, dispatchBudgetMs: 150 }),
    );
    expect(outcome).toMatchObject({
      name: "StorageUpgradeBlockedError",
      backend: "opfs",
      databaseName: "frozen",
      oldVersion: OLDER,
      requestedVersion: OPFS_LAYOUT_VERSION,
    });
    // Nothing changed while it waited, and the frozen tab still has its database.
    expect(decode(shim.readFileBytes("minnowdb/frozen/format.json"))).toBe(
      JSON.stringify({ formatVersion: OLDER }),
    );
    resume();
    expect((await frozen.listTables()).map(({ name }) => name)).toEqual(["seed"]);
    expect(frozenTab.superseded()).toBe(0);
    // Once it is awake, the next open asks again and succeeds.
    const newer = await OpfsBlockStore.open({ name: "frozen", root: shim.root });
    try {
      expect((await newer.listTables()).map(({ name }) => name)).toEqual(["seed"]);
      await waitFor(() => frozenTab.superseded() === 1, "the woken tab to step aside");
    } finally {
      newer._crashForTests();
    }
  });

  it("is ignored by a connection already on the requested layout", async () => {
    const shim = new MemoryOpfs();
    let superseded = 0;
    const current = await OpfsBlockStore.open({
      name: "same",
      root: shim.root,
      onSuperseded: () => {
        superseded += 1;
      },
    });
    try {
      await current.addTable(table("seed"));
      const channel = new BroadcastChannel("minnowdb-store:same");
      channel.postMessage({ kind: "upgrade", layout: OPFS_LAYOUT_VERSION });
      channel.close();
      await sleep(200);
      expect(superseded).toBe(0);
      expect(current._isLeaderForTests()).toBe(true);
      expect((await current.listTables()).map(({ name }) => name)).toEqual(["seed"]);
    } finally {
      current._crashForTests();
    }
  });

  it("steps aside when a newer layout asks over the channel, as a newer tab would", async () => {
    const shim = new MemoryOpfs();
    let superseded = 0;
    const current = await OpfsBlockStore.open({
      name: "asked",
      root: shim.root,
      onSuperseded: () => {
        superseded += 1;
      },
    });
    await current.addTable(table("seed"));
    const channel = new BroadcastChannel("minnowdb-store:asked");
    channel.postMessage({ kind: "upgrade", layout: OPFS_LAYOUT_VERSION + 1 });
    channel.close();
    await waitFor(() => superseded === 1, "the connection to step aside");
    expect(current._isLeaderForTests()).toBe(false);
    const error = await failure(current.listTables());
    expect(error).toBeInstanceOf(StorageConnectionClosedError);
    expect(error).toMatchObject({ reason: "superseded" });
  });
});

describe("the upgrade request on the wire", () => {
  it("carries the requester's layout, which must be a positive integer", () => {
    const methods = new Set<string>();
    expect(parseStoreRpcMessage({ kind: "upgrade", layout: 14 }, methods)).toEqual({
      kind: "upgrade",
      layout: 14,
    });
    expect(parseStoreRpcMessage({ kind: "upgrade" }, methods)).toBeUndefined();
    expect(parseStoreRpcMessage({ kind: "upgrade", layout: 0 }, methods)).toBeUndefined();
    expect(parseStoreRpcMessage({ kind: "upgrade", layout: 1.5 }, methods)).toBeUndefined();
    expect(
      parseStoreRpcMessage({ kind: "upgrade", layout: 14, extra: true }, methods),
    ).toBeUndefined();
  });
});
