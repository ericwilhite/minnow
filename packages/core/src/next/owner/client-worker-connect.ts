/** Browser-worker adapter over the existing connector. No engine, backend or worker URL default. */
import { connectNativeClient, type NativeClientConnectionOptions } from "./client-connect.js";
import { workerOwner } from "./connector.js";
import type { MinnowDatabaseClient } from "./database-client.js";
import type { HostContext } from "./election.js";

export interface NativeWorkerClientOptions extends Omit<
  NativeClientConnectionOptions,
  "startOwner"
> {
  /** Application-relative worker wrapper; it explicitly installs one native store recipe. */
  readonly createOwnerWorker: () => Worker;
  readonly platform: HostContext["platform"];
}

export function connectNativeWorkerClient(
  options: NativeWorkerClientOptions,
): MinnowDatabaseClient {
  const { createOwnerWorker, platform, ...connection } = options;
  // Capture before asynchronous election. Callers cannot retarget lifecycle policy mid-start.
  const capturedPlatform = { ...platform };
  return connectNativeClient({
    ...connection,
    startOwner: workerOwner(createOwnerWorker, capturedPlatform),
  });
}
