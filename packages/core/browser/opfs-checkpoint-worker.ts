import { OpfsBlockStore } from "@minnowdb/core/storage/opfs";
import { serializeError } from "@minnowdb/core/worker-protocol";
import { OpfsTree } from "../dist/storage/opfs/files.js";
import { decodeSyncCheckpoint } from "../dist/storage/toolkit/wire.js";

type Mode = "renew" | "append" | "mirror";
interface Request {
  name: string;
  mode: Mode;
  phase: "prepare" | "verify";
  start: number;
}
const identity = { tableId: "durable", columnId: "posting", buildId: "build", ownerId: "owner" };

async function run({ name, mode, phase, start }: Request): Promise<unknown> {
  const time = (offset: number): string => new Date(start + offset).toISOString();
  const lease = (offset: number) => ({
    ...identity,
    expiresAtCutoff: time(offset),
    updatedAt: time(offset),
    expiresAt: time(offset + 3_600_000),
  });
  let armed = false;
  if (mode === "mirror" && phase === "prepare") {
    // Keep every native file operation intact; stop only after one checkpoint mirror flushes.
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const openHandle = OpfsTree.prototype.openHandle;
    OpfsTree.prototype.openHandle = async function (path, options) {
      const handle = await openHandle.call(this, path, options);
      if (path.at(-1)?.startsWith("checkpoint-") === true) {
        const flush = handle.flush.bind(handle);
        handle.flush = () => {
          flush();
          if (armed) {
            armed = false;
            self.postMessage({ ready: true });
            // The parent ends this real worker at the durable first-mirror boundary.
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
            throw new Error("The stopped checkpoint worker unexpectedly resumed");
          }
        };
      }
      return handle;
    };
  }
  let checkpointPair: Array<{ generation: number; lastSeq: number }> | undefined;
  if (phase === "verify") {
    const root = await navigator.storage.getDirectory();
    const directory = await (await root.getDirectoryHandle("minnowdb")).getDirectoryHandle(name);
    const tree = new OpfsTree(directory);
    checkpointPair = await Promise.all(
      ["checkpoint-a", "checkpoint-b"].map(
        async (slot) =>
          decodeSyncCheckpoint((await tree.readFile([slot])) ?? new Uint8Array()) as {
            generation: number;
            lastSeq: number;
          },
      ),
    );
  }
  const store = await OpfsBlockStore.open({ name, checkpointEntries: 1 });
  if (phase === "prepare") {
    await store.addTable({
      id: "durable",
      name: "durable",
      columns: [{ id: "id", name: "id", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt: time(0),
      ftsColumns: {
        posting: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "building",
          buildFromVersion: -1,
        },
      },
    });
    if (mode !== "mirror") {
      await store.beginFtsBaseBuild({
        ...identity,
        createdAt: time(0),
        expiresAt: time(3_600_000),
      });
      await store.writeFtsBaseBuildChunk({
        ...lease(10),
        ordinal: 0,
        chunk: [{ term: "alpha", rowIds: [3n], tf: [1] }],
      });
      if (mode === "renew") await store.renewFtsBaseBuild(lease(5));
      else
        await store.writeFtsBaseBuildChunk({
          ...lease(5),
          ordinal: 1,
          chunk: [{ term: "beta", rowIds: [7n], tf: [1] }],
        });
    }
    for (let attempt = 0; attempt < 400; attempt++) {
      if ((await store.getStorageStats()).walBytes === 0) {
        if (mode === "mirror") {
          armed = true;
          store.close();
        } else self.postMessage({ ready: true });
        return undefined;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    throw new Error("The native checkpoint did not reset its WAL");
  }
  try {
    const tables = (await store.listTables()).map(({ name }) => name);
    let rowIds: string[][] | undefined;
    if (mode !== "mirror") {
      const count = mode === "append" ? 2 : 1;
      await store.finishFtsBaseBuild({
        ...identity,
        expiresAtCutoff: time(20),
        coversVersion: 0,
        chunkCount: count,
        totalTokens: count,
        completedAt: time(20),
      });
      const candidates = await store.readFtsCandidates(
        "durable",
        "posting",
        [
          { term: "alpha", prefix: false },
          { term: "beta", prefix: false },
        ],
        0,
      );
      rowIds = candidates.rowIdsByTerm.map((ids) => ids.map(String));
    }
    return {
      checkpointPair,
      tables,
      rowIds,
      integrity: (await store.checkIntegrity({ mode: "full" })).ok,
    };
  } finally {
    store.close();
  }
}

self.addEventListener("message", (event: MessageEvent<Request>) => {
  void run(event.data).then(
    (result) => {
      if (result !== undefined) self.postMessage({ result });
    },
    (error: unknown) => self.postMessage({ error: serializeError(error) }),
  );
});
