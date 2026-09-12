/**
 * Opening never mutates files before validation completes.
 *
 * The docs: "A recognized but unsupported envelope or checkpoint version stops before
 * redundant-slot fallback and before any truncation or deletion." These trace every write,
 * truncate, create, and delete during a refused open and assert the file tree is byte-identical
 * afterwards, for an unsupported checkpoint version, a newer format marker, and a corrupt WAL.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore } from "./index.js";
import { decodeSyncCheckpoint, encodeSyncCheckpoint } from "../toolkit/wire.js";
import { StorageFormatVersionError, type TableRecord } from "../types.js";
import { bytesEqual } from "./power-loss-model.js";

function table(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

async function snapshotTree(shim: MemoryOpfs, prefix: string): Promise<Map<string, Uint8Array>> {
  const files = new Map<string, Uint8Array>();
  const walk = async (dir: FileSystemDirectoryHandle, path: string): Promise<void> => {
    for await (const [name, entry] of dir as unknown as AsyncIterable<[string, { kind: string }]>) {
      const child = `${path}/${name}`;
      if (entry.kind === "file") {
        const bytes = shim.readFileBytes(child.replace(/^\//, ""));
        if (bytes !== undefined) files.set(child, bytes);
      } else {
        await walk(entry as unknown as FileSystemDirectoryHandle, child);
      }
    }
  };
  const root = await shim.root.getDirectoryHandle("minnowdb");
  await walk(await root.getDirectoryHandle(prefix), `/minnowdb/${prefix}`);
  return files;
}

function diffTrees(before: Map<string, Uint8Array>, after: Map<string, Uint8Array>): string[] {
  const changes: string[] = [];
  for (const [path, bytes] of before) {
    const now = after.get(path);
    if (now === undefined) changes.push(`deleted ${path}`);
    else if (!bytesEqual(now, bytes))
      changes.push(`changed ${path} (${String(bytes.byteLength)} -> ${String(now.byteLength)})`);
  }
  for (const path of after.keys()) if (!before.has(path)) changes.push(`created ${path}`);
  return changes;
}

describe("open never mutates before validation completes", () => {
  it("unsupported checkpoint state version: refused with no file change", async () => {
    const shim = new MemoryOpfs();
    const name = "guard-ckpt";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 2 });
    await store.addTable(table("a"));
    await store.addTable(table("b"));
    await store.addTable(table("c")); // checkpoint happened; WAL has a tail frame
    await store.addTable(table("d"));
    store._crashForTests();
    for (const slot of ["checkpoint-a", "checkpoint-b"]) {
      const path = `minnowdb/${name}/${slot}`;
      const decoded = decodeSyncCheckpoint(shim.readFileBytes(path) ?? new Uint8Array()) as Record<
        string,
        unknown
      >;
      decoded.formatVersion = 2;
      shim.writeFileBytes(path, encodeSyncCheckpoint(decoded));
    }
    const before = await snapshotTree(shim, name);
    const writes: string[] = [];
    shim.setWriteFault((path, phase) => {
      if (phase !== "flush") writes.push(`${phase} ${path}`);
    });
    shim.setDeleteFault((path) => {
      writes.push(`delete ${path}`);
    });
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toBeInstanceOf(
      StorageFormatVersionError,
    );
    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    const after = await snapshotTree(shim, name);
    expect(diffTrees(before, after)).toEqual([]);
    expect(writes.filter((w) => !w.startsWith("create "))).toEqual([]);
  });

  it("newer format marker: refused with no file change", async () => {
    const shim = new MemoryOpfs();
    const name = "guard-marker";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    await store.addTable(table("a"));
    store._crashForTests();
    const markerPath = `minnowdb/${name}/format.json`;
    const marker = new TextDecoder().decode(shim.readFileBytes(markerPath));
    shim.writeFileBytes(
      markerPath,
      new TextEncoder().encode(marker.replace(/"formatVersion":\d+/, '"formatVersion":99')),
    );
    const before = await snapshotTree(shim, name);
    const writes: string[] = [];
    shim.setWriteFault((path, phase) => {
      if (phase !== "flush") writes.push(`${phase} ${path}`);
    });
    shim.setDeleteFault((path) => {
      writes.push(`delete ${path}`);
    });
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toBeInstanceOf(
      StorageFormatVersionError,
    );
    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    expect(diffTrees(before, await snapshotTree(shim, name))).toEqual([]);
    expect(writes).toEqual([]);
  });

  it("corrupt WAL frame after a valid checkpoint: refused with no truncation or deletion", async () => {
    const shim = new MemoryOpfs();
    const name = "guard-wal";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 2 });
    await store.addTable(table("a"));
    await store.addTable(table("b"));
    await store.addTable(table("c"));
    await store.addTable(table("d"));
    await store.addTable(table("e")); // one frame past the last checkpoint
    store._crashForTests();
    const walPath = `minnowdb/${name}/wal`;
    const wal = shim.readFileBytes(walPath);
    if (wal === undefined || wal.byteLength === 0) throw new Error("expected a WAL tail");
    shim.corruptFileByte(walPath, 20); // inside the first frame's payload
    const before = await snapshotTree(shim, name);
    const writes: string[] = [];
    shim.setWriteFault((path, phase) => {
      if (phase !== "flush") writes.push(`${phase} ${path}`);
    });
    shim.setDeleteFault((path) => {
      writes.push(`delete ${path}`);
    });
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toThrow(/checksum/);
    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    expect(diffTrees(before, await snapshotTree(shim, name))).toEqual([]);
    expect(writes.filter((w) => !w.startsWith("create "))).toEqual([]);
  });
});
