import { OpfsLeader } from "./leader.js";
import { encodeBlock } from "../../block-format/index.js";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { StorageCorruptionError, OpfsUncertainOutcomeError } from "../types.js";
import { OpfsBlockStore } from "./store.js";
import { OpfsTree } from "./files.js";
import { iterateWalFrames } from "../toolkit/wal.js";

it.each([
  "zero payload",
  "zero frame",
  "truncate frame",
  "truncate log",
  "zero acknowledgement",
  "delete acknowledgement",
] as const)("refuses %s damage after acknowledging a strict write", async (damage) => {
  const shim = new MemoryOpfs();
  const store = await OpfsBlockStore.open({
    name: "acknowledged",
    root: shim.root,
    durability: "strict",
    checkpointEntries: 1000,
  });
  const db = new MinnowDatabase(store, {
    autoCompact: false,
    autoCollect: false,
    compression: "raw",
  });
  await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)");
  await db.execute("INSERT INTO t VALUES (1,10),(2,20)");
  expect((await db.query("SELECT * FROM t ORDER BY id")).rows).toEqual([
    { id: 1, n: 10 },
    { id: 2, n: 20 },
  ]);
  store._crashForTests();
  await db.close();
  const prefix = "minnowdb/acknowledged";
  const directory = await (
    await shim.root.getDirectoryHandle("minnowdb")
  ).getDirectoryHandle("acknowledged");
  const handle = await new OpfsTree(directory).openHandle(["wal"], { create: false });
  const frames = [...iterateWalFrames(handle)];
  handle.close();
  let commit = -1;
  for (let index = 0; index < frames.length; index += 1) {
    if ((present(frames[index]).payload as { op: string }).op === "writeTransaction")
      commit = index;
  }
  expect(commit).toBeGreaterThanOrEqual(0);
  const start = commit === 0 ? 0 : present(frames[commit - 1]).frameEnd;
  const bytes = present(shim.readFileBytes(`${prefix}/wal`));
  if (damage === "zero payload") bytes.fill(0, start + 12);
  if (damage === "zero frame") bytes.fill(0, start);
  if (damage === "truncate frame") shim.writeFileBytes(`${prefix}/wal`, bytes.slice(0, start + 13));
  else if (damage === "truncate log") shim.writeFileBytes(`${prefix}/wal`, new Uint8Array());
  else shim.writeFileBytes(`${prefix}/wal`, bytes);
  if (damage === "zero acknowledgement")
    shim.writeFileBytes(`${prefix}/wal-acknowledgements`, new Uint8Array(48));
  if (damage === "delete acknowledgement") await directory.removeEntry("wal-acknowledgements");
  const damaged = shim.readFileBytes(`${prefix}/wal`);
  await expect(
    OpfsBlockStore.open({ name: "acknowledged", root: shim.root, durability: "strict" }),
  ).rejects.toBeInstanceOf(StorageCorruptionError);
  expect(shim.readFileBytes(`${prefix}/wal`)).toEqual(damaged);
});

function present<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Required fixture bytes are missing");
  return value;
}

it("reports an unknown outcome and preserves published extent bytes when acknowledgement flush fails", async () => {
  const shim = new MemoryOpfs();
  const tree = new OpfsTree(shim.root);
  const diagnostics: Array<{ error: unknown; context: string }> = [];
  const leader = await OpfsLeader.recover(
    tree,
    true,
    {
      wal: await tree.openHandle(["wal"], { create: true }),
      slotA: await tree.openHandle(["checkpoint-a"], { create: true }),
      slotB: await tree.openHandle(["checkpoint-b"], { create: true }),
      acknowledgements: await tree.openHandle(["wal-acknowledgements"], { create: true }),
    },
    1000,
    undefined,
    (error, context) => diagnostics.push({ error, context }),
  );
  try {
    const begun = await leader.beginTransaction({
      record: {
        id: "transaction",
        ownerId: "owner",
        expiresAt: "2026-09-29T00:30:00.000Z",
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: "2026-09-29T00:00:00.000Z",
        updatedAt: "2026-09-29T00:00:00.000Z",
        committedVersion: null,
      },
    });
    const block = await encodeBlock({ type: "number", values: [42] }, "raw");
    let refused = false;
    shim.setWriteFault((path, phase) => {
      if (!refused && path === "wal-acknowledgements" && phase === "flush") {
        refused = true;
        throw new DOMException("acknowledgement flush refused", "QuotaExceededError");
      }
    });
    const error = await leader
      .stageTransactionArtifacts({
        transactionId: begun.record.id,
        expectedRevision: begun.record.revision,
        blocks: [{ id: "payload", bytes: block }],
        segments: [],
        updatedAt: "2026-09-29T00:00:01.000Z",
      })
      .catch((error: unknown) => error);
    expect(refused).toBe(true);
    expect(error).toBeInstanceOf(OpfsUncertainOutcomeError);
    expect((error as Error).cause).toMatchObject({ name: "QuotaExceededError" });
    expect(diagnostics).toEqual([
      { error: (error as Error).cause, context: "opfs WAL acknowledgement" },
    ]);
    shim.setWriteFault(null);
    expect(await leader.getBlock("payload")).toEqual(block);
    expect((await leader.checkIntegrity({ mode: "full" })).ok).toBe(true);
  } finally {
    shim.setWriteFault(null);
    leader.crash();
  }
});

it("protects a relaxed shutdown checkpoint when both checkpoint copies later disappear", async () => {
  const shim = new MemoryOpfs();
  const store = await OpfsBlockStore.open({
    name: "relaxed-flushed",
    root: shim.root,
    durability: "relaxed",
  });
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  await db.execute("CREATE TABLE t (n INTEGER)");
  await db.execute("INSERT INTO t VALUES (42)");
  await db.close();
  store.close();
  // close() is synchronous; wait for its queued shutdown to finish publishing both mirrors.
  await expect.poll(() => shim.readFileBytes("minnowdb/relaxed-flushed/wal")?.byteLength).toBe(0);
  // Shutdown checkpointed the acknowledged rows and reset the WAL. The independent witness
  // must prevent empty recovery if both checkpoint files are subsequently lost or zeroed.
  for (const slot of ["checkpoint-a", "checkpoint-b"])
    shim.writeFileBytes(`minnowdb/relaxed-flushed/${slot}`, new Uint8Array());
  await expect(
    OpfsBlockStore.open({ name: "relaxed-flushed", root: shim.root, durability: "relaxed" }),
  ).rejects.toBeInstanceOf(StorageCorruptionError);
});
