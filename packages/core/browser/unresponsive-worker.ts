// An IndexedDB worker whose store gives up after twelve seconds of silence instead of thirty, so a
// browser test can hold the database and watch initialization report the stall.
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import { attachDatabaseWorker, singleStoreFactory } from "@minnowdb/core/worker-host";

attachDatabaseWorker(self, {
  createStore: singleStoreFactory("indexeddb", (descriptor) =>
    IndexedDbBlockStore.open({ name: descriptor.name, unresponsiveAfterMs: 12_000 }),
  ),
});
