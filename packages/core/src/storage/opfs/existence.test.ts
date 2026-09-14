import { expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { opfsDatabaseExists } from "./store.js";

it("distinguishes an absent database from a failed OPFS lookup", async () => {
  const shim = new MemoryOpfs();
  const namespace = await shim.root.getDirectoryHandle("minnowdb", { create: true });
  await namespace.getDirectoryHandle("orders", { create: true });

  await expect(opfsDatabaseExists({ name: "orders", root: shim.root })).resolves.toBe(true);
  await expect(opfsDatabaseExists({ name: "missing", root: shim.root })).resolves.toBe(false);

  await namespace.getFileHandle("damaged", { create: true });
  await expect(opfsDatabaseExists({ name: "damaged", root: shim.root })).rejects.toMatchObject({
    name: "TypeMismatchError",
  });

  const unavailable = {
    getDirectoryHandle: async () => {
      throw new DOMException("OPFS backend unavailable", "UnknownError");
    },
  } as unknown as FileSystemDirectoryHandle;
  await expect(opfsDatabaseExists({ name: "orders", root: unavailable })).rejects.toMatchObject({
    name: "UnknownError",
  });
});

it("answers absent when this context has no OPFS API", async () => {
  const originalNavigator = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  Object.defineProperty(globalThis, "navigator", { configurable: true, value: {} });
  try {
    await expect(opfsDatabaseExists({ name: "orders" })).resolves.toBe(false);
  } finally {
    if (originalNavigator === undefined) delete (globalThis as { navigator?: Navigator }).navigator;
    else Object.defineProperty(globalThis, "navigator", originalNavigator);
  }
});
