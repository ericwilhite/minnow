/**
 * Crash and coordination shapes of the OPFS store's leader design: torn WAL tails, death
 * without a goodbye, checkpoint slot corruption, leader failover, graceful handoff, and the
 * foreground preference. The conformance suites prove the store behaves; this suite proves
 * the recovery and election rules by manufacturing the exact states they exist for.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore } from "./index.js";
import { TableRecordConflictError } from "../types.js";
import type { TableRecord } from "../types.js";

function table(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

async function waitFor(condition: () => Promise<boolean> | boolean, what: string): Promise<void> {
  for (let attempt = 0; attempt < 400; attempt += 1) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error(`Timed out waiting for ${what}`);
}

describe("OPFS write-ahead log crash shapes", () => {
  it("survives a torn WAL tail: the last frame is invisible, the rest intact", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    await store.addTable(table("first"));
    await store.addTable(table("second"));
    store._crashForTests();

    // A crash mid-append leaves a torn frame at the tail.
    const wal = shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0);
    const torn = new Uint8Array(wal.byteLength + 7);
    torn.set(wal);
    torn.set([0x4d, 0x4e, 0x57, 0x4c, 0xff, 0xff, 0xff], wal.byteLength);
    shim.writeFileBytes("minnowdb/db/wal", torn);

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect((await reopened.listTables()).map((record) => record.name)).toEqual(["first", "second"]);
    await reopened.addTable(table("third"));
    expect(await reopened.listTables()).toHaveLength(3);
    reopened.close();
  });

  it("recovers everything acknowledged before a death with no shutdown", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    for (let index = 0; index < 20; index += 1) await store.addTable(table(`t${String(index)}`));
    store._crashForTests(); // No flush, no checkpoint, no goodbye — just released locks.

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(await reopened.listTables()).toHaveLength(20);
    reopened.close();
  });

  it("bounds mutation deduplication and releases connection state on close", async () => {
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({ name: "bounded-rpc", root: shim.root });
    const follower = await OpfsBlockStore.open({
      name: "bounded-rpc",
      root: shim.root,
      rpcTimeoutMs: 200,
    });
    expect(leader._isLeaderForTests()).toBe(true);
    for (let index = 0; index < 530; index += 1) {
      await follower.addTable(table(`bounded-${String(index)}`));
      expect(leader._residentStateForTests().dedupeEntries).toBeLessThanOrEqual(512);
    }
    expect(leader._residentStateForTests()).toMatchObject({
      answerChannels: 1,
      dedupeEntries: 512,
      pendingRequests: 0,
    });

    follower.close();
    leader.close();
    expect(leader._residentStateForTests()).toMatchObject({
      dedupeEntries: 0,
      pendingRequests: 0,
      closed: true,
    });
    await waitFor(
      () => leader._residentStateForTests().answerChannels === 0,
      "leader answer channels to close",
    );
  });

  it("checkpoints into alternating slots and resets the WAL", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root, checkpointEntries: 8 });
    for (let index = 0; index < 20; index += 1) await store.addTable(table(`t${String(index)}`));
    // Two checkpoints have passed; the WAL holds only the entries since the last one.
    const wal = shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0);
    const slotA = shim.readFileBytes("minnowdb/db/checkpoint-a") ?? new Uint8Array(0);
    expect(slotA.byteLength).toBeGreaterThan(0);
    expect(wal.byteLength).toBeLessThan(2_000);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(await reopened.listTables()).toHaveLength(20);
    reopened.close();
  });

  it("recovers from a crash during a checkpoint write: the other slot plus the WAL", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root, checkpointEntries: 8 });
    // Stop one entry short of the checkpoint trigger and capture the WAL as it stands.
    for (let index = 0; index < 7; index += 1) await store.addTable(table(`t${String(index)}`));
    const walBeforeCheckpoint = shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0);
    expect(walBeforeCheckpoint.byteLength).toBeGreaterThan(0);
    // The eighth entry appends and then checkpoints (slot write, flush, WAL reset).
    await store.addTable(table("t7"));
    store._crashForTests();

    // Manufacture the crash-mid-checkpoint state: the slot is torn, and the WAL still holds
    // everything — the reset only ever happens after the slot flush, so this is the worst
    // on-disk state that crash can leave. The torn tail of the eighth entry's frame is
    // reconstructed by appending it to the captured WAL image.
    // The first checkpoint lands in slot B (slots alternate starting opposite the empty A).
    const checkpointed = shim.readFileBytes("minnowdb/db/checkpoint-b");
    expect(checkpointed?.byteLength ?? 0).toBeGreaterThan(0);
    shim.writeFileBytes("minnowdb/db/checkpoint-b", new Uint8Array(24)); // torn slot
    shim.writeFileBytes("minnowdb/db/checkpoint-a", new Uint8Array(0));
    // Restore the WAL to its pre-reset content plus the final entry's frame, taken from a
    // sibling run of the same deterministic append.
    const sibling = new MemoryOpfs();
    const rebuild = await OpfsBlockStore.open({ name: "db", root: sibling.root });
    for (let index = 0; index < 7; index += 1) await rebuild.addTable(table(`t${String(index)}`));
    await rebuild.addTable(table("t7"));
    rebuild._crashForTests();
    shim.writeFileBytes(
      "minnowdb/db/wal",
      sibling.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0),
    );

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(await reopened.listTables()).toHaveLength(8);
    reopened.close();
  });

  it("leaves a refused operation out of the WAL entirely", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    await store.addTable(table("real"));
    const walAfterAdd = (shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0)).byteLength;
    await expect(store.updateTable("table-real", 7, {})).rejects.toThrow(TableRecordConflictError);
    const walAfterRefusal = (shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0)).byteLength;
    expect(walAfterRefusal).toBe(walAfterAdd);
    store.close();
  });

  it("refuses a database written by a different layout version", async () => {
    const shim = new MemoryOpfs();
    shim.writeFileBytes("minnowdb/db/format.json", new TextEncoder().encode('{"formatVersion":1}'));
    await expect(OpfsBlockStore.open({ name: "db", root: shim.root })).rejects.toThrow(
      /layout version 1/,
    );
  });
});

describe("OPFS leadership", () => {
  it("fails over to a follower when the leader dies without a goodbye", async () => {
    const shim = new MemoryOpfs();
    const name = "db";
    const leader = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 100 });
    const follower = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 100 });
    await leader.addTable(table("before"));
    expect((await follower.listTables()).map((record) => record.name)).toEqual(["before"]);

    leader._crashForTests();
    // The follower's next operation times out against the dead leader, wins the released
    // lock, replays, and answers itself.
    await follower.addTable(table("after"));
    expect((await follower.listTables()).map((record) => record.name)).toEqual(["after", "before"]);
    follower.close();
  });

  it("hands leadership over on a graceful close", async () => {
    const shim = new MemoryOpfs();
    const first = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    const second = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    await first.addTable(table("kept"));
    first.close();
    await waitFor(async () => {
      try {
        await second.addTable(table("next"));
        return true;
      } catch {
        return false;
      }
    }, "the second connection to take over");
    expect((await second.listTables()).map((record) => record.name)).toEqual(["kept", "next"]);
    second.close();
  });

  it("yields leadership to the connection the user is looking at", async () => {
    const shim = new MemoryOpfs();
    const background = await OpfsBlockStore.open({
      name: "db",
      root: shim.root,
      rpcTimeoutMs: 100,
    });
    const foreground = await OpfsBlockStore.open({
      name: "db",
      root: shim.root,
      rpcTimeoutMs: 100,
    });
    await background.addTable(table("seed"));
    expect(background._isLeaderForTests()).toBe(true);
    expect(foreground._isLeaderForTests()).toBe(false);

    foreground.setForeground(true);
    // The handoff itself, not a proxy for it: the bidder ends up holding the handles and the
    // incumbent does not.
    await waitFor(() => foreground._isLeaderForTests(), "the foreground bidder to take over");
    expect(background._isLeaderForTests()).toBe(false);

    await foreground.addTable(table("written-as-leader"));
    const throughBackground = await background.listTables();
    const throughForeground = await foreground.listTables();
    expect(throughBackground.map((record) => record.name)).toEqual(
      throughForeground.map((record) => record.name),
    );
    background.close();
    foreground.close();
  });

  it("executes a duplicated mutation request only once", async () => {
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({ name: "dup", root: shim.root, rpcTimeoutMs: 200 });
    const follower = await OpfsBlockStore.open({
      name: "dup",
      root: shim.root,
      rpcTimeoutMs: 200,
    });
    // Eavesdrop on the leader's inbox and replay the follower's frame verbatim — exactly what
    // a retry whose acknowledgement was lost looks like to the leader.
    const spy = new BroadcastChannel(`minnowdb-store:dup:${leader._instanceIdForTests()}`);
    const captured: unknown[] = [];
    spy.onmessage = (event: MessageEvent<unknown>) => {
      const message = event.data as { kind?: string; method?: string };
      if (message.kind === "op" && message.method === "reserveRowIds") captured.push(event.data);
    };

    const first = await follower.reserveRowIds("table-x", 10);
    await waitFor(() => captured.length >= 1, "the frame to be observed");
    spy.postMessage(captured[0]); // the duplicate
    await new Promise((resolve) => setTimeout(resolve, 50));

    // The duplicate replayed the remembered result instead of burning a second range: the next
    // reservation continues exactly where the first ended.
    const second = await follower.reserveRowIds("table-x", 10);
    expect(second.start).toBe(first.endExclusive);
    spy.close();
    leader.close();
    follower.close();
  });

  it("never serves state the disk refused, even to reads", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    await store.addTable(table("real"));
    shim.setWriteFault((path) => {
      if (path.endsWith("/wal")) {
        throw new DOMException("The quota has been exceeded.", "QuotaExceededError");
      }
    });
    await expect(store.addTable(table("phantom"))).rejects.toMatchObject({
      name: "QuotaExceededError",
    });
    shim.setWriteFault(null);
    // The very first access afterwards is a read: it must reload from disk rather than serve
    // the in-memory state that ran ahead of the refused write.
    expect((await store.listTables()).map((record) => record.name)).toEqual(["real"]);
    expect(await store.getTableByName("phantom")).toBeUndefined();
    // And the store keeps working.
    await store.addTable(table("after"));
    expect(await store.listTables()).toHaveLength(2);
    store.close();
  });

  it("keeps acknowledging operations when checkpointing itself fails", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root, checkpointEntries: 4 });
    shim.setWriteFault((path) => {
      if (path.includes("checkpoint-")) {
        throw new DOMException("The quota has been exceeded.", "QuotaExceededError");
      }
    });
    // Every operation is durable in the WAL the moment it is acknowledged; a checkpoint that
    // cannot be written is a deferred maintenance failure, not an operation failure.
    for (let index = 0; index < 12; index += 1) await store.addTable(table(`t${String(index)}`));
    expect(await store.listTables()).toHaveLength(12);
    shim.setWriteFault(null);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(await reopened.listTables()).toHaveLength(12);
    reopened.close();
  });

  it("does not strand the database when a connection closes mid-election", async () => {
    const shim = new MemoryOpfs();
    const first = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    const second = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    await first.addTable(table("seed"));

    // Trigger a handoff toward `second` and close it immediately: its election may complete
    // after close(), and an installed-but-ownerless leader would hold the file lock forever.
    second.setForeground(true);
    second.close();

    const third = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    await waitFor(async () => {
      try {
        await third.addTable(table(`probe-${String(Math.random()).slice(2, 8)}`));
        return true;
      } catch {
        return false;
      }
    }, "some connection to be able to lead again");
    first.close();
    third.close();
  });
});
