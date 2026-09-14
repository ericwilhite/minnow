import { IndexedDbBlockStore, OpfsBlockStore } from "@minnowdb/core/storage";
import { deleteOpfsDatabase } from "@minnowdb/core/storage/opfs";
import { runFaultSweep, type FaultSweepStore } from "../dist/testing/fault-sweep.js";

self.addEventListener("message", (event: MessageEvent<"indexeddb" | "opfs">) => {
  void runFaultSweep(async (): Promise<FaultSweepStore> => {
    const name = `fault-sweep-${crypto.randomUUID()}`;
    const open = () =>
      event.data === "indexeddb"
        ? IndexedDbBlockStore.open({ name })
        : OpfsBlockStore.open({ name });
    const store = await open();
    return {
      store,
      reopen: () => {
        store.close();
        return open();
      },
      cleanup: () =>
        event.data === "opfs"
          ? deleteOpfsDatabase({ name })
          : new Promise<void>((resolve, reject) => {
              const request = indexedDB.deleteDatabase(name);
              request.onsuccess = () => resolve();
              request.onerror = () =>
                reject(request.error ?? new Error("Fault-sweep database cleanup failed"));
            }),
    };
  }).then(
    (result) => self.postMessage({ result }),
    (error: unknown) => {
      const messages: string[] = [];
      while (error instanceof Error) {
        messages.push(error.stack ?? error.message);
        error = error.cause;
      }
      self.postMessage({ error: messages.join("\nCaused by: ") });
    },
  );
});
