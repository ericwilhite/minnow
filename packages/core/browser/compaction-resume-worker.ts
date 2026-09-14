import { MinnowDatabase } from "@minnowdb/core";
import type { BlockStore } from "@minnowdb/core/storage/contracts";
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import { OpfsBlockStore } from "@minnowdb/core/storage/opfs";
import { serializeError } from "@minnowdb/core/worker-protocol";

async function run(kind: "indexeddb" | "opfs"): Promise<unknown> {
  const name = `compaction-resume-${crypto.randomUUID()}`;
  const nativeStore = await (kind === "indexeddb" ? IndexedDbBlockStore : OpfsBlockStore).open({
    name,
  });
  let blockedId: string | undefined;
  let blocked = false;
  let reached: (() => void) | undefined;
  let release: (() => void) | undefined;
  const renewalReached = new Promise<void>((resolve) => {
    reached = resolve;
  });
  const renewalRelease = new Promise<void>((resolve) => {
    release = resolve;
  });
  // OPFS exposes immutable methods. Intercept through a forwarding facade, keeping the real
  // store as every method's receiver and leaving its native operations and results intact.
  const store = new Proxy({} as typeof nativeStore, {
    has(_target, property) {
      return property in nativeStore;
    },
    get(_target, property) {
      if (property === "renewTransaction")
        return async (input: Parameters<BlockStore["renewTransaction"]>[0]) => {
          if (input.transactionId === blockedId && !blocked) {
            blocked = true;
            reached?.();
            await renewalRelease;
          }
          return nativeStore.renewTransaction(input);
        };
      const value: unknown = Reflect.get(nativeStore, property, nativeStore);
      return typeof value === "function"
        ? (value as (...args: unknown[]) => unknown).bind(nativeStore)
        : value;
    },
  });
  const winner = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  const contender = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await winner.execute("CREATE TABLE events(value INTEGER)");
    for (let value = 1; value <= 4; value++) await winner.insert("events", { value });
    const partial = await winner.compactTableStep("events", {
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (partial.jobId === null || partial.result !== null)
      throw new Error("Expected a partial compaction");
    const running = await store.getCompactionJob(partial.jobId);
    if (running?.transactionId === null || running?.transactionId === undefined)
      throw new Error("Expected a linked compaction transaction");
    blockedId = running.transactionId;
    const racedResume = contender.resumeCompactionJob(partial.jobId, { maxBlocks: 1 });
    await renewalReached;
    const published = await winner.resumeCompactionJob(partial.jobId, { maxBlocks: 64 });
    release?.();
    const resumed = await racedResume;
    return {
      published,
      resumed,
      rows: await contender.readTable("events"),
      integrity: (await store.checkIntegrity({ mode: "full" })).ok,
    };
  } finally {
    release?.();
    await Promise.all([winner.close(), contender.close()]);
    store.close();
  }
}

self.addEventListener("message", (event: MessageEvent<"indexeddb" | "opfs">) => {
  void run(event.data).then(
    (result) => self.postMessage({ result }),
    (error: unknown) => self.postMessage({ error: serializeError(error) }),
  );
});
