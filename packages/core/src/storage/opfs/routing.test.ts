/**
 * Where the OPFS store's messages go. Leadership traffic is broadcast on the database's shared
 * channel; requests and answers are addressed — an operation into the leader's inbox, its
 * answer into the requester's — so a follower hears its own answers and nobody else's, and a
 * block read's bytes are cloned into one tab rather than every tab. These tests watch every
 * `BroadcastChannel` the stores construct and count what each one hears.
 */
import { afterEach, describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore } from "./index.js";
import type { StoreRpcMessage } from "./rpc.js";

interface ObservedChannel {
  name: string;
  /** The `label` in force when the store constructed this channel. */
  label: string;
  heard: StoreRpcMessage[];
}

const RealBroadcastChannel = globalThis.BroadcastChannel;

/**
 * Replaces `BroadcastChannel` with a subclass that records every message each object hears.
 * `label` tags objects by the phase of the test that constructed them; `inboxOf` finds the
 * objects that hear a given connection's inbox name.
 */
function observeChannels(): {
  objects: ObservedChannel[];
  setLabel(label: string): void;
  inboxOf(store: OpfsBlockStore): ObservedChannel[];
  restore(): void;
} {
  const objects: ObservedChannel[] = [];
  let label = "";
  class ObservedBroadcastChannel extends RealBroadcastChannel {
    constructor(name: string) {
      super(name);
      const entry: ObservedChannel = { name, label, heard: [] };
      objects.push(entry);
      this.addEventListener("message", (event) => {
        entry.heard.push((event as MessageEvent<StoreRpcMessage>).data);
      });
    }
  }
  globalThis.BroadcastChannel = ObservedBroadcastChannel;
  return {
    objects,
    setLabel(next) {
      label = next;
    },
    inboxOf(store) {
      const suffix = `:${store._instanceIdForTests()}`;
      return objects.filter((object) => object.name.endsWith(suffix));
    },
    restore() {
      globalThis.BroadcastChannel = RealBroadcastChannel;
    },
  };
}

function heard(objects: ObservedChannel[], kind: StoreRpcMessage["kind"]): StoreRpcMessage[] {
  return objects.flatMap((object) => object.heard.filter((message) => message.kind === kind));
}

async function waitFor(condition: () => boolean, what: string): Promise<void> {
  for (let attempt = 0; attempt < 400; attempt += 1) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error(`Timed out waiting for ${what}`);
}

let transactionOrdinal = 0;

async function stageBlock(store: OpfsBlockStore, id: string, bytes: Uint8Array): Promise<void> {
  transactionOrdinal += 1;
  const transactionId = `routing-transaction-${String(transactionOrdinal)}`;
  await store.beginTransaction({
    record: {
      id: transactionId,
      ownerId: `routing-owner-${String(transactionOrdinal)}`,
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
  await store.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks: [{ id, bytes }],
    segments: [],
    updatedAt: "2026-08-24T00:00:01.000Z",
  });
}

describe("OPFS request routing", () => {
  afterEach(() => {
    globalThis.BroadcastChannel = RealBroadcastChannel;
  });

  it("delivers a follower's answers to that follower alone", async () => {
    const observed = observeChannels();
    const shim = new MemoryOpfs();
    const open = () => OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 200 });
    observed.setLabel("leader");
    const leader = await open();
    observed.setLabel("asker");
    const asker = await open();
    observed.setLabel("bystander");
    const bystander = await open();
    const bytes = new Uint8Array(64 * 1024).fill(7);
    await stageBlock(leader, "x", bytes);
    // Both followers learn the leader, so the bystander is a fully connected follower — the
    // shape in which the old broadcast design cloned every answer into it.
    await bystander.getCurrentManifestVersion();
    observed.setLabel("work");

    const read = await asker.getBlock("x");
    expect(read).toEqual(bytes);

    // The operation reached the leader's inbox, addressed from the asker.
    const ops = heard(observed.inboxOf(leader), "op").filter(
      (message) => message.kind === "op" && message.from === asker._instanceIdForTests(),
    );
    expect(ops).toHaveLength(1);
    expect(ops[0]).toMatchObject({ method: "getBlock" });
    // Its answer reached the asker's inbox once, bytes intact.
    const answers = heard(observed.inboxOf(asker), "result");
    expect(answers).toHaveLength(1);
    expect(answers[0]).toMatchObject({ ok: true, value: bytes });
    // No other object anywhere heard those bytes: the bystander's inbox heard only its own
    // earlier answer, and the leader's own channels hear nothing they post.
    const everythingElse = observed.objects.filter(
      (object) => !observed.inboxOf(asker).includes(object),
    );
    const strays = heard(everythingElse, "result");
    expect(strays).toHaveLength(1);
    expect(strays[0]).toMatchObject({ ok: true, value: null });
    expect(observed.inboxOf(bystander).flatMap((object) => object.heard)).toEqual(strays);
    // The shared channel carried leadership traffic only.
    const shared = observed.objects.filter((object) => object.name === "minnowdb-store:db");
    const sharedKinds = new Set(shared.flatMap((object) => object.heard.map((m) => m.kind)));
    expect([...sharedKinds].filter((kind) => kind === "op" || kind === "result")).toEqual([]);

    leader.close();
    asker.close();
    bystander.close();
    observed.restore();
  });

  it("keeps a follower's outbound channel closed between operations", async () => {
    // Every follower posts into the same leader inbox. An outbound channel kept open would hear
    // every other follower's operations — block writes included — so one is opened per message
    // and closed at once: after any number of operations, the only open object on the leader's
    // inbox name is the leader's own.
    const observed = observeChannels();
    const shim = new MemoryOpfs();
    const open = () => OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 200 });
    const leader = await open();
    const first = await open();
    const second = await open();
    const bytes = new Uint8Array(16 * 1024).fill(1);
    for (let round = 0; round < 3; round += 1) {
      await stageBlock(first, `first-${String(round)}`, bytes);
      await stageBlock(second, `second-${String(round)}`, bytes);
    }
    const [leaderInbox, ...outbound] = observed.inboxOf(leader);
    expect(leaderInbox?.heard.filter((message) => message.kind === "op")).toHaveLength(12);
    // The transient outbound objects heard nothing: each was closed before any other
    // follower's operation could reach it.
    expect(outbound).toHaveLength(12);
    expect(heard(outbound, "op")).toEqual([]);
    leader.close();
    first.close();
    second.close();
    observed.restore();
  });

  it("stops an ex-leader's answer channels hearing the next leader's answers", async () => {
    const observed = observeChannels();
    const shim = new MemoryOpfs();
    const open = () => OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 200 });
    observed.setLabel("open");
    const background = await open();
    const foreground = await open();
    const follower = await open();
    observed.setLabel("before-handoff");
    await follower.getCurrentManifestVersion();
    // The background leader answered through a channel it opened into the follower's inbox.
    const oldAnswerChannels = observed
      .inboxOf(follower)
      .filter((object) => object.label === "before-handoff");
    expect(oldAnswerChannels).toHaveLength(1);

    foreground.setForeground(true);
    await waitFor(() => foreground._isLeaderForTests(), "the handoff");
    observed.setLabel("after-handoff");
    await follower.getCurrentManifestVersion();
    await follower.getCurrentManifestVersion();

    // The new leader's answers reached the follower's inbox, and the demoted leader's old
    // channel on that name — closed at demotion — heard none of them. (An object never hears
    // its own posts, so before the handoff that channel heard nothing either.)
    const inbox = observed.inboxOf(follower).find((object) => object.label === "open");
    expect(heard(inbox === undefined ? [] : [inbox], "result")).toHaveLength(3);
    expect(heard(oldAnswerChannels, "result")).toEqual([]);
    background.close();
    foreground.close();
    follower.close();
    observed.restore();
  });

  it("re-sends an in-flight request to a leader elected while it waits", async () => {
    const observed = observeChannels();
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 200 });
    // A long patience: if this follower's request completes well inside it, the completion
    // came from the announcement re-send, not from a timeout retry.
    const follower = await OpfsBlockStore.open({
      name: "db",
      root: shim.root,
      rpcTimeoutMs: 3_000,
    });
    const successor = await OpfsBlockStore.open({
      name: "db",
      root: shim.root,
      rpcTimeoutMs: 200,
    });
    const bytes = new Uint8Array(1024).fill(3);
    await stageBlock(leader, "x", bytes);
    await follower.getCurrentManifestVersion();
    await successor.getCurrentManifestVersion();

    // The leader dies silently; the follower still believes in it and posts into a dead inbox.
    leader._crashForTests();
    const started = Date.now();
    const pending = follower.getBlock("x");
    // The successor's own operation finds no leader, takes the lock, and announces itself; the
    // follower hears that on the shared channel and re-sends its request to the new inbox.
    expect(await successor.getBlock("x")).toEqual(bytes);
    expect(await pending).toEqual(bytes);
    expect(Date.now() - started).toBeLessThan(2_000);
    const arrived = heard(observed.inboxOf(successor), "op").filter(
      (message) => message.kind === "op" && message.from === follower._instanceIdForTests(),
    );
    expect(arrived.map((message) => (message.kind === "op" ? message.method : ""))).toEqual([
      "getBlock",
    ]);
    follower.close();
    successor.close();
    observed.restore();
  });

  it("fails a follower's request the same way when the answer never comes", async () => {
    // The timeout and the dispatch-attempt budget are untouched by addressing: a dead leader
    // costs the follower its timeout, after which it takes the lock and answers itself.
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    const follower = await OpfsBlockStore.open({
      name: "db",
      root: shim.root,
      rpcTimeoutMs: 100,
    });
    await stageBlock(leader, "x", new Uint8Array([1, 2, 3]));
    expect(await follower.getBlock("x")).toEqual(new Uint8Array([1, 2, 3]));
    leader._crashForTests();
    const started = Date.now();
    expect(await follower.getBlock("x")).toEqual(new Uint8Array([1, 2, 3]));
    expect(Date.now() - started).toBeGreaterThanOrEqual(90);
    expect(follower._isLeaderForTests()).toBe(true);
    follower.close();
  });
});
