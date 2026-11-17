/** Side-effect-free production worker entry glue. Per-store barrels call this at cutover. */
import type { StorageBackend } from "../storage/contract.js";
import type {
  OwnerHost,
  OwnerHostDeps,
  OwnerHostInit,
  OwnerHostStatus,
  OwnerHostCommand,
} from "./host.js";
import { startNativeOwnerHost, type NativeBootOptions } from "./native-boot.js";
import { ClientClosedError, toWireError } from "./errors.js";
export interface NativeWorkerScope {
  onmessage: ((event: { readonly data: unknown }) => void) | null;
  postMessage(status: OwnerHostStatus): void;
}
function initialization(data: unknown): OwnerHostInit {
  if (typeof data !== "object" || data === null)
    throw new TypeError("Invalid native worker initialization");
  const input = data as Partial<OwnerHostInit>;
  const port = (value: unknown) =>
    typeof value === "object" &&
    value !== null &&
    "postMessage" in value &&
    typeof value.postMessage === "function" &&
    "close" in value &&
    typeof value.close === "function";
  const settings = (value: unknown) =>
    typeof value === "object" && value !== null && !Array.isArray(value);
  if (
    input.t !== "minnow:host" ||
    typeof input.database !== "string" ||
    typeof input.build !== "string" ||
    !port(input.clientPort) ||
    (input.brokerPort !== null && !port(input.brokerPort)) ||
    !settings(input.settings)
  )
    throw new TypeError("Invalid native worker initialization");
  return input as OwnerHostInit;
}
export function installNativeOwnerWorker(
  scope: NativeWorkerScope,
  backendFor: (database: string) => StorageBackend | Promise<StorageBackend>,
  options: Omit<NativeBootOptions, "backend" | "signal">,
  deps: Omit<OwnerHostDeps, "post">,
) {
  let starting: Promise<OwnerHost> | null = null,
    closed = false,
    reported = false,
    closing: Promise<void> | null = null;
  let resolveReady: (host: OwnerHost) => void = () => undefined,
    rejectReady: (error: unknown) => void = () => undefined;
  const ready = new Promise<OwnerHost>((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });
  ready.catch(() => undefined);
  const post = (status: OwnerHostStatus) => {
    if (status.t === "minnow:failed") reported = true;
    scope.postMessage(status);
  };
  const failure = (error: unknown) => {
    rejectReady(error);
    if (!reported) post({ t: "minnow:failed", error: toWireError(error) });
  };
  const previous = scope.onmessage;
  const handler = ({ data }: { readonly data: unknown }) => {
    if (closed) return;
    const message = data as Partial<OwnerHostInit | OwnerHostCommand> | null;
    if (message?.t === "minnow:host") {
      try {
        const init = initialization(data);
        if (starting !== null) {
          init.clientPort.close();
          init.brokerPort?.close();
          throw new TypeError("Native worker already initialized");
        }
        starting = startNativeOwnerHost(
          init,
          async (database) => {
            const backend = await backendFor(database);
            if (closed) throw new ClientClosedError();
            return backend;
          },
          options,
          { ...deps, post },
        );
        starting.then(resolveReady, failure);
      } catch (error) {
        failure(error);
      }
      return;
    }
    if (starting === null) {
      failure(new TypeError("Initialize the native worker before sending commands"));
      return;
    }
    if (message?.t === "minnow:stepDown")
      void starting.then((host) => host.stop(message.reason ?? "closing", true)).catch(failure);
    else if (message?.t === "minnow:lifecycle")
      void starting
        .then((host) => {
          if (message.event !== undefined && message.platform !== undefined)
            host.lifecycle(message.event, message.platform);
        })
        .catch(failure);
  };
  scope.onmessage = handler;
  return {
    ready,
    close: () =>
      (closing ??= (async () => {
        closed = true;
        rejectReady(new ClientClosedError());
        if (scope.onmessage === handler) scope.onmessage = previous;
        if (starting === null) {
          rejectReady(new ClientClosedError());
          return;
        }
        const host = await starting.catch(() => null);
        if (host !== null) await host.stop("closing", true);
      })()),
  };
}
