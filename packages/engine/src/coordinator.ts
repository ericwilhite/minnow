import type { BlockStore, PublishManifestInput } from "@browserdatabase/storage-idb";
import {
  failure,
  parseRequest,
  success,
  type WorkerRequest,
} from "@browserdatabase/worker-protocol";

export interface WorkerReply {
  message: unknown;
  transfer?: Transferable[];
}

export function createCoordinator(store: BlockStore) {
  return async (rawRequest: unknown): Promise<WorkerReply> => {
    let request: WorkerRequest;
    try {
      request = parseRequest(rawRequest);
    } catch (error) {
      const requestId = getRequestId(rawRequest);
      return { message: failure(requestId, error) };
    }

    try {
      switch (request.operation) {
        case "ping":
          return { message: success(request.requestId, { ready: true }) };
        case "writeBlock": {
          const payload = request.payload as { id: string; bytes: Uint8Array };
          await store.addBlock(payload.id, payload.bytes);
          return { message: success(request.requestId, { stored: payload.bytes.byteLength }) };
        }
        case "readBlock": {
          const payload = request.payload as { id: string };
          const bytes = await store.getBlock(payload.id);
          if (bytes === undefined) throw new Error(`Block not found: ${payload.id}`);
          return {
            message: success(request.requestId, { bytes }),
            transfer: [bytes.buffer],
          };
        }
        case "publishManifest": {
          const manifest = await store.publishManifest(request.payload as PublishManifestInput);
          return { message: success(request.requestId, manifest) };
        }
        case "benchmark":
          throw new Error("Benchmark operation must be supplied by the benchmark worker");
        case "compareEngines":
          throw new Error("Engine comparison must be supplied by the benchmark worker");
        case "cancelBenchmark":
          throw new Error("Cancellation must be supplied by the benchmark worker");
        case "adHocQuery":
          throw new Error("Ad-hoc queries must be supplied by the benchmark worker");
        case "datasetStatus":
          throw new Error("Dataset status must be supplied by the benchmark worker");
        case "wipeDatasets":
          throw new Error("Dataset wiping must be supplied by the benchmark worker");
      }
    } catch (error) {
      return { message: failure(request.requestId, error) };
    }
  };
}

export function attachCoordinator(
  scope: Pick<DedicatedWorkerGlobalScope, "addEventListener" | "postMessage">,
  store: BlockStore,
): void {
  const coordinate = createCoordinator(store);
  scope.addEventListener("message", (event: MessageEvent<unknown>) => {
    void coordinate(event.data).then((reply) => {
      scope.postMessage(reply.message, reply.transfer ?? []);
    });
  });
}

function getRequestId(value: unknown): string {
  if (typeof value === "object" && value !== null && "requestId" in value) {
    const requestId = value.requestId;
    if (typeof requestId === "string") return requestId;
  }
  return "unknown";
}
