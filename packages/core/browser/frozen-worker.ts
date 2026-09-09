import { OpfsBlockStore } from "@minnowdb/core/storage/opfs";
import { attachDatabaseWorker, singleStoreFactory } from "@minnowdb/core/worker-host";

attachDatabaseWorker(self, {
  createStore: singleStoreFactory("opfs", async (descriptor) => {
    const store = await OpfsBlockStore.open({ name: descriptor.name, durability: "strict" });
    store.setForeground(true);
    // Exercise suspension before a cooperative handoff. The first owner keeps its handles;
    // later peers remain followers even when Playwright changes which page is visible.
    store.setForeground = () => undefined;
    return store;
  }),
});
