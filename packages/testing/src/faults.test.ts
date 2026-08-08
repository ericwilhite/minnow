import { expect, it } from "vitest";
import { MemoryBlockStore } from "@browserdatabase/storage-idb";
import { FaultInjectingBlockStore } from "./index.js";

it("leaves a complete but unreachable block when publication fails", async () => {
  const inner = new MemoryBlockStore();
  const store = new FaultInjectingBlockStore(inner, (point) => {
    if (point === "beforeManifestCommit") throw new Error("injected");
  });
  await store.addBlock("pending", Uint8Array.of(1, 2, 3));
  await expect(
    store.publishManifest({ expectedVersion: null, blockIds: ["pending"] }),
  ).rejects.toThrow("injected");
  expect(await inner.getBlock("pending")).toEqual(Uint8Array.of(1, 2, 3));
  expect(await inner.getCurrentManifest()).toBeUndefined();
});

it("models an uncertain outcome after a successful manifest commit", async () => {
  const inner = new MemoryBlockStore();
  await inner.addBlock("ready", Uint8Array.of(1));
  const store = new FaultInjectingBlockStore(inner, (point) => {
    if (point === "afterManifestCommit") throw new Error("response lost");
  });
  await expect(
    store.publishManifest({ expectedVersion: null, blockIds: ["ready"] }),
  ).rejects.toThrow("response lost");
  expect((await inner.getCurrentManifest())?.blockIds).toEqual(["ready"]);
});
