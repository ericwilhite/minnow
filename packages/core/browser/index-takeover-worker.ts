// One tab's engine for index-takeover.spec.ts, in a dedicated worker as applications host it.
// The worker can be told to park its index build inside a postings chunk write, so the tab is
// provably mid-build -- its catalog mark, build session, and staged key set durable -- when the
// test closes it.
import { MinnowDatabase } from "@minnowdb/core";
import { IndexedDbBlockStore, type BlockStore } from "@minnowdb/core/storage";
import { OpfsBlockStore } from "@minnowdb/core/storage/opfs";
import { serializeError } from "@minnowdb/core/worker-protocol";

type Request =
  | { id: number; op: "open"; kind: "indexeddb" | "opfs"; name: string; park: boolean }
  | { id: number; op: "execute"; sql: string; params?: unknown[] };

let database: MinnowDatabase | undefined;

function parking(store: BlockStore): BlockStore {
  return new Proxy(store, {
    get(target, property) {
      if (property === "writeFtsBaseBuildChunk") {
        return async () => {
          self.postMessage({ parked: true });
          // Never resumes: the test closes this tab mid-build.
          await new Promise(() => undefined);
        };
      }
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? (value as () => unknown).bind(target) : value;
    },
  });
}

async function handle(request: Request): Promise<unknown> {
  if (request.op === "open") {
    const store: BlockStore =
      request.kind === "indexeddb"
        ? await IndexedDbBlockStore.open({ name: request.name })
        : await OpfsBlockStore.open({ name: request.name });
    database = new MinnowDatabase(request.park ? parking(store) : store, { rowsPerBlock: 64 });
    return null;
  }
  if (database === undefined) throw new Error("Open the database first");
  const started = performance.now();
  await database.execute(request.sql, request.params as never);
  return { ms: performance.now() - started };
}

self.onmessage = (event: MessageEvent<Request>) => {
  const request = event.data;
  handle(request).then(
    (result) => self.postMessage({ id: request.id, result }),
    (error: unknown) => self.postMessage({ id: request.id, error: serializeError(error) }),
  );
};
