import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { StorageUnresponsiveError } from "@minnowdb/core/storage/contracts";
import { attachDatabaseWorker, exposeDatabase } from "@minnowdb/core/worker-host";

class SlowClosingDatabase extends MinnowDatabase {
  override async close(): Promise<void> {
    await new Promise<void>((resolve) => setTimeout(resolve, 5_500));
    await super.close();
  }
}

class StuckClosingDatabase extends MinnowDatabase {
  override async close(): Promise<void> {
    await new Promise<void>(() => undefined);
  }
}

self.addEventListener("message", function initialize(event: MessageEvent<{ mode?: unknown }>) {
  const mode = event.data.mode;
  if (mode !== "slow" && mode !== "stuck" && mode !== "init-failure") return;
  self.removeEventListener("message", initialize);
  if (mode === "init-failure") {
    attachDatabaseWorker(self, {
      createStore: async () => {
        throw new StorageUnresponsiveError("indexeddb", "failed-initialization", 30_000);
      },
    });
  } else {
    const database =
      mode === "slow"
        ? new SlowClosingDatabase(new MemoryBlockStore())
        : new StuckClosingDatabase(new MemoryBlockStore());
    exposeDatabase(database, self, { keepaliveIntervalMs: 5_000 });
  }
  self.postMessage({ kind: "dispose-test-ready" });
});
