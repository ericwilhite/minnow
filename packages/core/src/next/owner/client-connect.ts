/** Client-only native connector glue. Backend/worker/broker selection stays with the caller;
 * the public export map and defaults are a separate cutover step. */
import { ProtocolClient, type ProtocolClientOptions } from "./client.js";
import {
  OwnerConnector,
  relayPageLifecycle,
  type ConnectorOptions,
  type OwnerHandle,
} from "./connector.js";
import { MinnowDatabaseClient } from "./database-client.js";
import { ClientClosedError, OwnerUnreachableError } from "./errors.js";
import type { ClientStatus } from "./api.js";
export interface NativeClientConnectionOptions extends Omit<ConnectorOptions, "client"> {
  readonly protocolOptions?: ProtocolClientOptions;
  readonly onOwnerError?: ProtocolClient["onOwnerError"];
  /** Explicit null suppresses page listeners (e.g. a host which relays them itself). */
  readonly lifecycleTarget?: Parameters<typeof relayPageLifecycle>[1] | null;
}
/** A single existing protocol/election state machine; never a second dispatcher or executor. */
export function connectNativeClient(options: NativeClientConnectionOptions): MinnowDatabaseClient {
  const { protocolOptions, onOwnerError, lifecycleTarget, ...input } = options;
  const settings = input.settings === undefined ? undefined : structuredClone(input.settings);
  const protocol = new ProtocolClient({
    ...protocolOptions,
    ...(input.build === undefined ? {} : { build: input.build }),
  });
  if (onOwnerError !== undefined) protocol.onOwnerError = onOwnerError;
  const lifetimes = new Set<Promise<void>>();
  let closed = false,
    failure: { reason: unknown } | null = null;
  // Host construction and awaits may reenter close; read current state at each boundary.
  const isClosed = () => closed;
  const recordFailure = (reason: unknown) => {
    failure ??= { reason };
  };
  const originalStart = input.startOwner;
  const startOwner: ConnectorOptions["startOwner"] =
    originalStart === null
      ? null
      : (init) => {
          if (isClosed()) throw new ClientClosedError();
          let resolve: () => void = () => undefined,
            reject: (reason: unknown) => void = () => undefined;
          // Install ownership BEFORE invoking user/host construction: it may reenter close().
          const retired = new Promise<void>((yes, no) => {
            resolve = yes;
            reject = no;
          });
          lifetimes.add(retired);
          retired.then(
            () => lifetimes.delete(retired),
            (reason: unknown) => {
              recordFailure(reason);
              lifetimes.delete(retired);
            },
          );
          let handle: OwnerHandle;
          try {
            handle = originalStart(init);
          } catch (error) {
            resolve();
            throw error;
          }
          handle.stopped.then(() => resolve(), reject);
          if (isClosed()) {
            const terminate = () => {
              try {
                handle.terminate();
              } catch (error) {
                recordFailure(error);
                reject(error);
              }
            };
            try {
              const stopping = handle.stepDown?.("closing");
              if (stopping)
                stopping.catch((reason: unknown) => {
                  recordFailure(reason);
                  terminate();
                });
              else terminate();
            } catch (error) {
              recordFailure(error);
              terminate();
            }
          }
          return handle;
        };
  let connector: OwnerConnector;
  try {
    connector = new OwnerConnector({
      ...input,
      ...(settings === undefined ? {} : { settings }),
      client: protocol,
      startOwner,
    });
  } catch (error) {
    protocol.close();
    throw error;
  }
  let unlisten: () => void = () => undefined;
  const target =
    lifecycleTarget === undefined
      ? typeof window === "undefined"
        ? null
        : window
      : lifecycleTarget;
  try {
    if (target !== null) unlisten = relayPageLifecycle(connector, target);
  } catch (error) {
    connector.close();
    throw error;
  }
  const status = (): ClientStatus => {
    if (closed || connector.status === "closed") return "closed";
    if (connector.status === "superseded") return "superseded";
    if (connector.status === "connected") return "ready";
    if (protocol.gaveUp) return "unresponsive";
    return protocol.ownerId === null ? "connecting" : "reconnecting";
  };
  const ready = async () => {
    if (isClosed()) throw new ClientClosedError();
    if (connector.status !== "connected") await connector.start();
    if (isClosed()) throw new ClientClosedError();
    if (connector.status !== "connected" || !protocol.connected)
      throw new OwnerUnreachableError("no compatible native owner connected");
  };
  return new MinnowDatabaseClient(protocol, {
    get ready() {
      return ready();
    },
    status,
    async close() {
      closed = true;
      unlisten();
      connector.close();
      // No later owner start is allowed. Pending construction reserved its token before
      // calling out, so even reentrant close cannot miss a late returned native handle.
      while (lifetimes.size) await Promise.allSettled([...lifetimes]);
      if (failure) throw failure.reason;
    },
  });
}
