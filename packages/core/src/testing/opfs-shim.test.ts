import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "./opfs-shim.js";

async function readAll(handle: FileSystemFileHandle): Promise<Uint8Array> {
  return new Uint8Array(await (await handle.getFile()).arrayBuffer());
}

describe("MemoryOpfs", () => {
  it("persists bytes across separate directory handles on one shim", async () => {
    const shim = new MemoryOpfs();
    const first = await shim.root.getDirectoryHandle("db", { create: true });
    const file = await first.getFileHandle("data", { create: true });
    const handle = await file.createSyncAccessHandle();
    handle.write(new TextEncoder().encode("hello"), { at: 0 });
    handle.close();

    // A "reopen": a fresh chain of handles from the root sees the same tree.
    const second = await shim.root.getDirectoryHandle("db");
    const reread = await second.getFileHandle("data");
    expect(new TextDecoder().decode(await readAll(reread))).toBe("hello");
  });

  it("enforces the exclusive sync-access lock across handles", async () => {
    const shim = new MemoryOpfs();
    const file = await shim.root.getFileHandle("locked", { create: true });
    const sameFile = await shim.root.getFileHandle("locked");
    const held = await file.createSyncAccessHandle();

    // Error identity matters: the store branches on the DOMException name.
    const failure = await sameFile.createSyncAccessHandle().then(
      () => undefined,
      (reason: unknown) => reason,
    );
    expect(failure).toBeInstanceOf(DOMException);
    expect((failure as DOMException).name).toBe("NoModificationAllowedError");

    // The strict reading of the spec: reads and removals refuse too while locked.
    await expect(sameFile.getFile()).rejects.toMatchObject({
      name: "NoModificationAllowedError",
    });
    await expect(shim.root.removeEntry("locked")).rejects.toMatchObject({
      name: "NoModificationAllowedError",
    });

    held.close();
    const reacquired = await sameFile.createSyncAccessHandle();
    reacquired.close();
  });

  it("reads, writes, truncates, and sizes through the sync handle", async () => {
    const shim = new MemoryOpfs();
    const file = await shim.root.getFileHandle("f", { create: true });
    const handle = await file.createSyncAccessHandle();
    handle.write(new Uint8Array([1, 2, 3, 4, 5]), { at: 0 });
    handle.write(new Uint8Array([9]), { at: 7 });
    expect(handle.getSize()).toBe(8);

    const buffer = new Uint8Array(8);
    expect(handle.read(buffer, { at: 0 })).toBe(8);
    expect([...buffer]).toEqual([1, 2, 3, 4, 5, 0, 0, 9]);

    handle.truncate(3);
    expect(handle.getSize()).toBe(3);
    const shorter = new Uint8Array(8);
    expect(handle.read(shorter, { at: 0 })).toBe(3);
    handle.close();
    handle.close(); // idempotent
    expect(() => handle.getSize()).toThrow(DOMException);
  });

  it("raises NotFoundError and TypeMismatchError with real identities", async () => {
    const shim = new MemoryOpfs();
    await shim.root.getDirectoryHandle("dir", { create: true });
    await expect(shim.root.getFileHandle("missing")).rejects.toMatchObject({
      name: "NotFoundError",
    });
    await expect(shim.root.getFileHandle("dir")).rejects.toMatchObject({
      name: "TypeMismatchError",
    });
    await expect(shim.root.getDirectoryHandle("missing")).rejects.toMatchObject({
      name: "NotFoundError",
    });
    await expect(shim.root.removeEntry("missing")).rejects.toMatchObject({
      name: "NotFoundError",
    });
  });

  it("refuses to remove a non-empty directory without recursive, then obliges with it", async () => {
    const shim = new MemoryOpfs();
    const dir = await shim.root.getDirectoryHandle("dir", { create: true });
    await dir.getFileHandle("child", { create: true });
    await expect(shim.root.removeEntry("dir")).rejects.toMatchObject({
      name: "InvalidModificationError",
    });
    await shim.root.removeEntry("dir", { recursive: true });
    await expect(shim.root.getDirectoryHandle("dir")).rejects.toMatchObject({
      name: "NotFoundError",
    });
  });

  it("iterates entries", async () => {
    const shim = new MemoryOpfs();
    const dir = await shim.root.getDirectoryHandle("dir", { create: true });
    await dir.getFileHandle("b", { create: true });
    await dir.getDirectoryHandle("a", { create: true });
    const seen: Array<[string, string]> = [];
    for await (const [name, handle] of dir) seen.push([name, handle.kind]);
    seen.sort((left, right) => left[0].localeCompare(right[0]));
    expect(seen).toEqual([
      ["a", "directory"],
      ["b", "file"],
    ]);
  });

  it("throws injected faults as real DOMExceptions from inside the write", async () => {
    const shim = new MemoryOpfs();
    const file = await shim.root.getFileHandle("f", { create: true });
    const handle = await file.createSyncAccessHandle();
    handle.write(new Uint8Array([1, 2]), { at: 0 });
    shim.setWriteFault((path) => {
      if (path === "f")
        throw new DOMException("The quota has been exceeded.", "QuotaExceededError");
    });
    const failure = (() => {
      try {
        handle.write(new Uint8Array([3]), { at: 2 });
        return undefined;
      } catch (reason) {
        return reason;
      }
    })();
    expect(failure).toBeInstanceOf(DOMException);
    expect((failure as DOMException).name).toBe("QuotaExceededError");
    shim.setWriteFault(null);
    handle.write(new Uint8Array([3]), { at: 2 });
    expect(handle.getSize()).toBe(3);
    handle.close();
  });

  it("supports test-side torn writes for crash shapes", async () => {
    const shim = new MemoryOpfs();
    shim.writeFileBytes("db/log/000000000001", new Uint8Array([1, 2, 3]));
    const dir = await (await shim.root.getDirectoryHandle("db")).getDirectoryHandle("log");
    const file = await dir.getFileHandle("000000000001");
    expect([...(await readAll(file))]).toEqual([1, 2, 3]);
    expect([...(shim.readFileBytes("db/log/000000000001") ?? [])]).toEqual([1, 2, 3]);
  });
});
