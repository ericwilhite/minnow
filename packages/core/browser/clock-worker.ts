/** A real worker transport that reports progress forever without completing one request. */
import { protocolVersion, type RpcRequest, type RpcResponse } from "@minnowdb/core/worker-protocol";

const scope = self as unknown as DedicatedWorkerGlobalScope;
const keepalives = new Set<ReturnType<typeof setInterval>>();

scope.postMessage("clock-worker-ready");

function send(response: RpcResponse): void {
  scope.postMessage(response);
}

scope.addEventListener("message", (event: MessageEvent<unknown>) => {
  const request = event.data as RpcRequest;
  if (request.kind === "rpc-cancel") return;
  if (request.kind === "rpc-init") {
    send({
      version: protocolVersion,
      requestId: request.requestId,
      kind: "rpc-result",
      result: { store: "memory" },
    });
    return;
  }
  if (request.method === "listTables") {
    const report = (): void => {
      send({
        version: protocolVersion,
        requestId: request.requestId,
        kind: "rpc-event",
        handleId: "$worker",
        event: "keepalive",
        payload: null,
      });
    };
    report();
    keepalives.add(setInterval(report, 20));
    return;
  }
  if (request.method === "dispose") {
    for (const interval of keepalives) clearInterval(interval);
    keepalives.clear();
  }
  send({
    version: protocolVersion,
    requestId: request.requestId,
    kind: "rpc-result",
    result: undefined,
  });
});
