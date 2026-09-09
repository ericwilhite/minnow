import { describe, expect, it, vi } from "vitest";
import { OpfsLeader } from "./leader.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { deleteOpfsDatabase, OpfsBlockStore, OpfsDatabaseInUseError } from "./index.js";

// Node 22 has no Web Locks; the browser suite also exercises this across real workers.
describe.skipIf(
  (globalThis as { navigator?: { locks?: LockManager } }).navigator?.locks === undefined,
)("OPFS coordinated deletion", () => {
  it("refuses removal with a leader or an idle follower, then permits a clean reopen", async () => {
    const shim = new MemoryOpfs();
    const options = { name: `delete-${crypto.randomUUID()}`, root: shim.root };
    const leader = await OpfsBlockStore.open(options);
    const follower = await OpfsBlockStore.open(options);
    const wal = shim.readFileBytes(`minnowdb/${options.name}/wal`);
    try {
      await expect(deleteOpfsDatabase(options)).rejects.toBeInstanceOf(OpfsDatabaseInUseError);
      expect(shim.readFileBytes(`minnowdb/${options.name}/wal`)).toEqual(wal);
      leader._crashForTests();
      await expect(deleteOpfsDatabase(options)).rejects.toBeInstanceOf(OpfsDatabaseInUseError);
    } finally {
      leader._crashForTests();
      follower._crashForTests();
    }
    await vi.waitFor(() => deleteOpfsDatabase(options));
    expect(shim.readFileBytes(`minnowdb/${options.name}/wal`)).toBeUndefined();
    const reopened = await OpfsBlockStore.open(options);
    expect(await reopened.listTables()).toEqual([]);
    reopened._crashForTests();
    await vi.waitFor(() => deleteOpfsDatabase(options));
    await vi.waitFor(() => deleteOpfsDatabase(options));
  });

  it("keeps deletion blocked when close races a leadership handoff", async () => {
    const options = { name: `handoff-delete-${crypto.randomUUID()}`, root: new MemoryOpfs().root };
    const leader = await OpfsBlockStore.open(options);
    const follower = await OpfsBlockStore.open(options);
    let resume!: () => void;
    let started!: () => void;
    const waiting = new Promise<void>((resolve) => {
      started = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      resume = resolve;
    });
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Restored with the mock receiver via call below.
    const original = OpfsLeader.prototype.shutdown;
    const shutdown = vi.spyOn(OpfsLeader.prototype, "shutdown").mockImplementation(async function (
      this: OpfsLeader,
    ) {
      started();
      await gate;
      await original.call(this);
    });
    try {
      follower.setForeground(true);
      await waiting;
      follower.close();
      leader.close();
      await expect(deleteOpfsDatabase(options)).rejects.toBeInstanceOf(OpfsDatabaseInUseError);
      resume();
      await vi.waitFor(() => deleteOpfsDatabase(options));
    } finally {
      resume();
      shutdown.mockRestore();
      leader.close();
      follower.close();
    }
  });

  it("retains the deletion guard until a failed opener finishes closing its handles", async () => {
    const options = { name: `failed-owner-${crypto.randomUUID()}`, root: new MemoryOpfs().root };
    let resume!: () => void;
    const gate = new Promise<void>((resolve) => {
      resume = resolve;
    });
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with the mock receiver below.
    const original = OpfsLeader.prototype.shutdown;
    const shutdown = vi.spyOn(OpfsLeader.prototype, "shutdown").mockImplementation(async function (
      this: OpfsLeader,
    ) {
      await gate;
      await original.call(this);
    });
    const post = vi.spyOn(BroadcastChannel.prototype, "postMessage").mockImplementationOnce(() => {
      throw new Error("injected announcement failure");
    });
    try {
      await expect(OpfsBlockStore.open(options)).rejects.toThrow("injected announcement failure");
      await expect(deleteOpfsDatabase(options)).rejects.toBeInstanceOf(OpfsDatabaseInUseError);
    } finally {
      resume();
      post.mockRestore();
      shutdown.mockRestore();
      await vi.waitFor(() => deleteOpfsDatabase(options));
    }
  });

  it("serializes an opener behind deletion and releases failed-open locks", async () => {
    const shim = new MemoryOpfs();
    const options = { name: `delete-race-${crypto.randomUUID()}`, root: shim.root };
    let release!: () => void;
    let acquired!: () => void;
    const started = new Promise<void>((resolve) => {
      acquired = resolve;
    });
    const held = navigator.locks.request(`minnowdb-opfs-connections:${options.name}`, async () => {
      acquired();
      await new Promise<void>((resolve) => {
        release = resolve;
      });
    });
    await started;
    let opened = false;
    const pending = OpfsBlockStore.open(options).then((store) => {
      opened = true;
      return store;
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(opened).toBe(false);
    release();
    await held;
    (await pending)._crashForTests();
    await vi.waitFor(() => deleteOpfsDatabase(options));
    shim.setWriteFault(() => {
      throw new Error("injected open failure");
    });
    await expect(OpfsBlockStore.open(options)).rejects.toThrow("injected open failure");
    shim.setWriteFault(null);
    await vi.waitFor(() => deleteOpfsDatabase(options));
  });
});

it("refuses unsafe deletion when Web Locks is unavailable", async () => {
  const shim = new MemoryOpfs();
  shim.writeFileBytes("minnowdb/no-locks/anchor", Uint8Array.of(1));
  vi.stubGlobal("navigator", {});
  try {
    await expect(deleteOpfsDatabase({ name: "no-locks", root: shim.root })).rejects.toThrow(
      "requires Web Locks",
    );
    expect(shim.readFileBytes("minnowdb/no-locks/anchor")).toEqual(Uint8Array.of(1));
  } finally {
    vi.unstubAllGlobals();
  }
});
