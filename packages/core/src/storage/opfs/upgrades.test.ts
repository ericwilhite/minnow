/* eslint-disable no-restricted-imports -- Frozen writer fixture and failure-injection tests run only in Node. */
import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { OpfsBlockStore as Layout6Store } from "@minnowdb/core-layout6/storage/opfs";
import { decodeBlock } from "../../block-format/index.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { decodePostingChunk, decodeSyncCheckpoint, encodePostingChunk } from "../toolkit/wire.js";
import type { TableRecord } from "../types.js";
import { StorageCorruptionError, StorageFormatVersionError } from "../types.js";
import { OpfsTree } from "./files.js";
import { OpfsBlockStore } from "./index.js";
import { PowerLossModel } from "./power-loss-model.js";
import { OPFS_LAYOUT_VERSION } from "./upgrades.js";

const fixture = JSON.parse(
  readFileSync(new URL("../../../format-fixtures/opfs-layout6.json", import.meta.url), "utf8"),
) as {
  files: Record<string, string>;
  expectations: { tables: string[]; blockId: string; blockValues: Array<string | null> };
};
const NAME = "native-fixture";
const PREFIX = `minnowdb/${NAME}/`;
const failure = new DOMException("injected upgrade I/O failure", "QuotaExceededError");

function hydrate(): MemoryOpfs {
  const shim = new MemoryOpfs();
  for (const [path, bytes] of Object.entries(fixture.files)) {
    shim.writeFileBytes(
      path,
      Uint8Array.from(atob(bytes), (char) => char.charCodeAt(0)),
    );
  }
  return shim;
}
function present(bytes: Uint8Array | undefined): Uint8Array {
  if (bytes === undefined) throw new Error("Required test file is missing");
  return bytes;
}
function table(name: string): TableRecord {
  return {
    id: name,
    name,
    managed: false,
    columns: [{ id: "value", name: "value", type: "string", nullable: true }],
    revision: 0,
    createdAt: "2026-09-29T12:00:00.000Z",
  };
}
async function verify(shim: MemoryOpfs, extra: string[] = []): Promise<OpfsBlockStore> {
  const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
  try {
    expect((await store.listTables()).map(({ name }) => name)).toEqual(
      [...fixture.expectations.tables, ...extra].sort(),
    );
    const block = present(await store.getBlock(fixture.expectations.blockId));
    expect((await decodeBlock(block)).column.values).toEqual(fixture.expectations.blockValues);
    // This block exists only in the old WAL tail, not its checkpoint. Losing/replaying that
    // tail incorrectly cannot pass merely by preserving the checkpointed rows.
    expect(await store.getBlock("fixture-follower-block")).toEqual(block);
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true, issueCount: 0 });
    expect(new TextDecoder().decode(shim.readFileBytes(`${PREFIX}format.json`))).toBe(
      JSON.stringify({ formatVersion: OPFS_LAYOUT_VERSION }),
    );
    return store;
  } catch (error) {
    store._crashForTests();
    throw error;
  }
}

describe("automatic OPFS upgrades", () => {
  it("opens with the actual released 0.10.0 reader before upgrade and refuses that reader afterward without mutation", async () => {
    const shim = hydrate();
    const old = await Layout6Store.open({ name: NAME, root: shim.root });
    expect((await old.listTables()).map(({ name }) => name)).toEqual(fixture.expectations.tables);
    old._crashForTests();
    (await verify(shim))._crashForTests();
    const writes: string[] = [];
    shim.setWriteFault((path, phase) => {
      if (phase !== "flush") writes.push(`${phase}: ${path}`);
    });
    shim.setDeleteFault((path) => writes.push(`delete: ${path}`));
    await expect(Layout6Store.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: OPFS_LAYOUT_VERSION,
      supportedVersion: 6,
      relation: "newer",
    });
    expect(writes).toEqual([]);
    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    (await verify(shim))._crashForTests();
  });
  it("upgrades an empty layout-6 database without inventing rows", async () => {
    const shim = new MemoryOpfs();
    shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVersion":6}'));
    const store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(await store.listTables()).toEqual([]);
    await store.addTable(table("first-write"));
    store._crashForTests();
    const reopened = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect((await reopened.listTables()).map(({ name }) => name)).toEqual(["first-write"]);
    reopened._crashForTests();
  });
  it("upgrades the released layout-6 fixture through the ordinary open API, then writes and reopens", async () => {
    const shim = hydrate();
    const store = await verify(shim);
    await store.addTable(table("after-upgrade"));
    store._crashForTests();
    (await verify(shim, ["after-upgrade"]))._crashForTests();
    expect(shim.readFileBytes(`${PREFIX}upgrade-6-7/ready`)).toBeUndefined();
  });

  it("keeps the frozen immutable layout-6 postings reader while writing current envelopes", () => {
    const entries = [{ term: "exact", rowIds: [1n, 9007199254740993n], tf: [1, 3] }];
    const bytes = encodePostingChunk(entries);
    expect(new DataView(bytes.buffer).getUint32(8, true)).toBe(7);
    new DataView(bytes.buffer).setUint32(8, 6, true);
    expect(decodePostingChunk(bytes)).toEqual(entries);
    new DataView(bytes.buffer).setUint32(8, 5, true);
    expect(() => decodePostingChunk(bytes)).toThrow(StorageFormatVersionError);
  });

  it("serializes concurrent openers instead of admitting a connection to the old layout", async () => {
    const shim = hydrate();
    const stores = await Promise.all(
      Array.from({ length: 4 }, () => OpfsBlockStore.open({ name: NAME, root: shim.root })),
    );
    try {
      for (const store of stores)
        expect((await store.listTables()).map(({ name }) => name)).toEqual(
          fixture.expectations.tables,
        );
      expect(new TextDecoder().decode(shim.readFileBytes(`${PREFIX}format.json`))).toBe(
        JSON.stringify({ formatVersion: OPFS_LAYOUT_VERSION }),
      );
      await stores[2]?.addTable(table("concurrent-write"));
      expect((await stores[0]?.listTables())?.map(({ name }) => name)).toContain(
        "concurrent-write",
      );
    } finally {
      for (const store of stores) store._crashForTests();
    }
    (await verify(shim, ["concurrent-write"]))._crashForTests();
  });

  it("reports an older connection blocking conversion and automatically succeeds after it closes", async () => {
    const shim = hydrate();
    const root = await (await shim.root.getDirectoryHandle("minnowdb")).getDirectoryHandle(NAME);
    const wal = await (await root.getFileHandle("wal")).createSyncAccessHandle();
    try {
      await expect(
        OpfsBlockStore.open({ name: NAME, root: shim.root, dispatchBudgetMs: 50 }),
      ).rejects.toThrow("close older connections");
      expect(new TextDecoder().decode(shim.readFileBytes(`${PREFIX}format.json`))).toBe(
        '{"formatVersion":6}',
      );
    } finally {
      wal.close();
    }
    (await verify(shim))._crashForTests();
  });

  it("keeps a concurrent opener waiting while a slow conversion is alive beyond its dispatch budget", async () => {
    const shim = hydrate();
    let signalEntered: () => void = () => undefined;
    let resumeConversion: () => void = () => undefined;
    const entered = new Promise<void>((resolve) => {
      signalEntered = resolve;
    });
    const resume = new Promise<void>((resolve) => {
      resumeConversion = resolve;
    });
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Forwarded below with the actual tree as its receiver.
    const original = OpfsTree.prototype.getDirectory;
    const spy = vi.spyOn(OpfsTree.prototype, "getDirectory").mockImplementation(async function (
      this: OpfsTree,
      path,
      create,
    ) {
      if (path.length === 1 && path[0] === "upgrade-6-7" && create) {
        signalEntered();
        await resume;
      }
      return original.call(this, path, create);
    });
    const primary = OpfsBlockStore.open({ name: NAME, root: shim.root });
    await entered;
    let followerFinished = false;
    const follower = OpfsBlockStore.open({
      name: NAME,
      root: shim.root,
      dispatchBudgetMs: 100,
    }).finally(() => {
      followerFinished = true;
    });
    const outcomes = Promise.allSettled([primary, follower]);
    try {
      await new Promise((resolve) => setTimeout(resolve, 350));
      expect(followerFinished).toBe(false);
      resumeConversion();
      for (const outcome of await outcomes) {
        expect(outcome.status).toBe("fulfilled");
        if (outcome.status === "fulfilled") {
          expect((await outcome.value.listTables()).map(({ name }) => name)).toEqual(
            fixture.expectations.tables,
          );
        }
      }
    } finally {
      resumeConversion();
      for (const outcome of await outcomes)
        if (outcome.status === "fulfilled") outcome.value._crashForTests();
      spy.mockRestore();
    }
    (await verify(shim))._crashForTests();
  });

  for (const kind of [
    "checkpoint",
    "payload",
    "zero-WAL",
    "empty-WAL",
    "short-WAL",
    "sequence-gap",
  ] as const) {
    it(`refuses ${kind} damage without changing existing layout-6 files, including relaxed open`, async () => {
      const shim = hydrate();
      if (kind === "checkpoint") shim.corruptFileByte(`${PREFIX}checkpoint-a`, 30);
      if (kind === "payload") shim.corruptFileByte(`${PREFIX}extents/000000`, 60);
      if (kind === "zero-WAL")
        shim.writeFileBytes(
          `${PREFIX}wal`,
          new Uint8Array(present(shim.readFileBytes(`${PREFIX}wal`)).length),
        );
      if (kind === "short-WAL")
        shim.writeFileBytes(
          `${PREFIX}wal`,
          present(shim.readFileBytes(`${PREFIX}wal`)).slice(0, -1),
        );
      if (kind === "empty-WAL") shim.writeFileBytes(`${PREFIX}wal`, new Uint8Array());
      if (kind === "sequence-gap") {
        const wal = present(shim.readFileBytes(`${PREFIX}wal`));
        const firstLength = 12 + new DataView(wal.buffer).getUint32(4, true);
        shim.writeFileBytes(`${PREFIX}wal`, wal.slice(firstLength));
      }
      const before = Object.keys(fixture.files).map(
        (path) => [path, present(shim.readFileBytes(path))] as const,
      );
      await expect(
        OpfsBlockStore.open({ name: NAME, root: shim.root, durability: "relaxed" }),
      ).rejects.toBeInstanceOf(StorageCorruptionError);
      for (const [path, bytes] of before) expect(shim.readFileBytes(path)).toEqual(bytes);
    });
  }

  for (const mode of ["tab-death", "power-loss"] as const) {
    it(`recovers every injected preparation/publication ${mode} boundary and retains old WAL-only data`, async () => {
      const trace: Array<{ path: string; phase: "create" | "write" | "flush" }> = [];
      const successful = hydrate();
      successful.setWriteFault((path, phase) => trace.push({ path, phase }));
      (await verify(successful))._crashForTests();
      expect(trace.some(({ path }) => path.endsWith("upgrade-6-7/ready"))).toBe(true);
      expect(trace.some(({ path }) => path.endsWith("upgrade-6-7-complete"))).toBe(true);
      expect(trace.length).toBeGreaterThan(30);
      for (let cut = 0; cut < trace.length; cut += 1) {
        const shim = hydrate();
        let armed = false;
        let operation = 0;
        const model = new PowerLossModel(shim, () => {
          if (armed && operation++ === cut) throw failure;
        });
        // Frozen files are already durable; seed that state in the power-loss model without
        // using the new writer to generate or rewrite the old fixture.
        const tree = new OpfsTree(shim.root);
        for (const path of Object.keys(fixture.files)) {
          const handle = await tree.openHandle(path.split("/"), { create: false });
          handle.flush();
          handle.close();
        }
        armed = true;
        await expect(
          OpfsBlockStore.open({ name: NAME, root: shim.root }),
          `${mode} cut ${String(cut)}: ${JSON.stringify(trace[cut])}`,
        ).rejects.toThrow();
        armed = false;
        if (mode === "power-loss") model.powerLoss();
        shim.setWriteFault(null);
        const resumed = await verify(shim);
        await resumed.addTable(table("after-retry"));
        resumed._crashForTests();
        (await verify(shim, ["after-retry"]))._crashForTests();
      }
      console.info(`Automatic upgrade ${mode}: ${String(trace.length)} I/O boundaries verified`);
    }, 60_000);
  }

  for (const name of [
    "format.json",
    "checkpoint-a",
    "checkpoint-b",
    "wal-acknowledgements",
    "upgrade-6-7-complete",
  ]) {
    it(`resumes a partially transferred ${name} publication`, async () => {
      const shim = hydrate();
      shim.setTransferLimit((_path, operation, requested) =>
        operation === "write" ? Math.min(requested, 3) : undefined,
      );
      let writes = 0;
      shim.setWriteFault((path, phase) => {
        if (path === `${PREFIX}${name}` && phase === "write" && ++writes === 3) throw failure;
      });
      await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toThrow(failure);
      shim.setWriteFault(null);
      shim.setTransferLimit(null);
      (await verify(shim))._crashForTests();
    });
  }

  it("does not roll back later writes when a completed ready record reappears, even after partial cleanup", async () => {
    const shim = hydrate();
    shim.setDeleteFault((path) => {
      if (path.endsWith("upgrade-6-7")) throw failure;
    });
    await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toThrow(failure);
    const ready = present(shim.readFileBytes(`${PREFIX}upgrade-6-7/ready`));
    shim.setDeleteFault(null);
    const store = await verify(shim);
    await store.addTable(table("later-acknowledged"));
    store._crashForTests();
    // Only the ready record survived namespace rollback; prepared files need not survive.
    shim.writeFileBytes(`${PREFIX}upgrade-6-7/ready`, ready);
    (await verify(shim, ["later-acknowledged"]))._crashForTests();
    shim.writeFileBytes(`${PREFIX}upgrade-6-7/ready`, ready);
    shim.writeFileBytes(`${PREFIX}wal`, new Uint8Array());
    await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
  });

  for (const marker of ["missing", "torn"] as const) {
    for (const receipt of ["missing", "corrupt"] as const) {
      it(`refuses stale upgrade replay over newer checkpoints with a ${marker} marker and ${receipt} receipt`, async () => {
        const shim = hydrate();
        shim.setDeleteFault((path) => {
          if (path.endsWith("upgrade-6-7")) throw failure;
        });
        await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toThrow(failure);
        const tree = new OpfsTree(shim.root);
        const staged = new Map<string, Uint8Array>();
        for await (const { path } of tree.walkFiles(["minnowdb", NAME, "upgrade-6-7"])) {
          const filename = `${PREFIX}upgrade-6-7/${path.join("/")}`;
          staged.set(filename, present(shim.readFileBytes(filename)));
        }
        const oldCheckpoint = decodeSyncCheckpoint(
          present(staged.get(`${PREFIX}upgrade-6-7/checkpoint-a`)),
        ) as { lastSeq: number };
        shim.setDeleteFault(null);
        const store = await verify(shim);
        await store.addTable(table("later-checkpointed"));
        store.close();
        (await verify(shim, ["later-checkpointed"]))._crashForTests();
        const checkpoint = present(shim.readFileBytes(`${PREFIX}checkpoint-a`));
        expect((decodeSyncCheckpoint(checkpoint) as { lastSeq: number }).lastSeq).toBeGreaterThan(
          oldCheckpoint.lastSeq,
        );
        expect(present(shim.readFileBytes(`${PREFIX}wal`))).toHaveLength(0);
        for (const [path, bytes] of staged) shim.writeFileBytes(path, bytes);
        if (marker === "missing") await tree.deleteFile(["minnowdb", NAME, "format.json"]);
        else shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"format'));
        if (receipt === "missing")
          await tree.deleteFile(["minnowdb", NAME, "upgrade-6-7-complete"]);
        else shim.corruptFileByte(`${PREFIX}upgrade-6-7-complete`, 30);
        const controls = ["checkpoint-a", "checkpoint-b", "wal", "wal-acknowledgements"].map(
          (name) => [name, present(shim.readFileBytes(`${PREFIX}${name}`))] as const,
        );
        await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
          StorageCorruptionError,
        );
        for (const [name, bytes] of controls)
          expect(shim.readFileBytes(`${PREFIX}${name}`)).toEqual(bytes);
        shim.writeFileBytes(
          `${PREFIX}format.json`,
          new TextEncoder().encode('{"formatVersion":7}'),
        );
        (await verify(shim, ["later-checkpointed"]))._crashForTests();
      });
    }
  }

  it("refuses an invalid native proof after WAL reset instead of repairing it from stale staging", async () => {
    const shim = hydrate();
    shim.setDeleteFault((path) => {
      if (path.endsWith("upgrade-6-7")) throw failure;
    });
    await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toThrow(failure);
    shim.setDeleteFault(null);
    expect(present(shim.readFileBytes(`${PREFIX}wal`))).toHaveLength(0);
    expect(present(shim.readFileBytes(`${PREFIX}upgrade-6-7/ready`))).not.toHaveLength(0);
    const tree = new OpfsTree(shim.root);
    await tree.deleteFile(["minnowdb", NAME, "upgrade-6-7-complete"]);
    const originalProof = present(shim.readFileBytes(`${PREFIX}wal-acknowledgements`));
    shim.corruptFileByte(`${PREFIX}wal-acknowledgements`, 0);
    const damagedProof = present(shim.readFileBytes(`${PREFIX}wal-acknowledgements`));
    const checkpoint = present(shim.readFileBytes(`${PREFIX}checkpoint-a`));
    await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    expect(shim.readFileBytes(`${PREFIX}wal-acknowledgements`)).toEqual(damagedProof);
    expect(shim.readFileBytes(`${PREFIX}checkpoint-a`)).toEqual(checkpoint);
    shim.writeFileBytes(`${PREFIX}wal-acknowledgements`, originalProof);
    (await verify(shim))._crashForTests();
  });

  it("refuses a future writer and keeps every old byte intact", async () => {
    const shim = hydrate();
    shim.writeFileBytes(`${PREFIX}format.json`, new TextEncoder().encode('{"formatVersion":99}'));
    await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
      StorageFormatVersionError,
    );
    for (const [path, base64] of Object.entries(fixture.files)) {
      if (path.endsWith("format.json")) continue;
      expect(shim.readFileBytes(path)).toEqual(
        Uint8Array.from(atob(base64), (char) => char.charCodeAt(0)),
      );
    }
  });

  for (const file of ["ready", "checkpoint-a"]) {
    it(`refuses damaged prepared ${file} bytes without discarding source history`, async () => {
      const shim = hydrate();
      shim.setWriteFault((path, phase) => {
        if (path === `${PREFIX}checkpoint-a` && phase === "write") throw failure;
      });
      await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toThrow(failure);
      shim.setWriteFault(null);
      shim.corruptFileByte(`${PREFIX}upgrade-6-7/${file}`, 30);
      const wal = present(shim.readFileBytes(`${PREFIX}wal`));
      await expect(OpfsBlockStore.open({ name: NAME, root: shim.root })).rejects.toBeInstanceOf(
        StorageCorruptionError,
      );
      expect(shim.readFileBytes(`${PREFIX}wal`)).toEqual(wal);
    });
  }
});
