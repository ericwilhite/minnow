// An IndexedDB worker whose store gives up after two seconds of silence instead of thirty, so a
// browser test can hold the database and watch initialization report the stall quickly.
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import { attachDatabaseWorker, singleStoreFactory } from "@minnowdb/core/worker-host";

attachDatabaseWorker(self, {
  createStore: singleStoreFactory("indexeddb", (descriptor) =>
    IndexedDbBlockStore.open({ name: descriptor.name, unresponsiveAfterMs: 2_000 }),
  ),
});
