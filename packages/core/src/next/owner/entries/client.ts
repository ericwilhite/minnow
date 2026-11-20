/** Internal client-only entry. The release commit selects the public export map. */
export { MinnowDatabaseClient } from "../database-client.js";
export { connectNativeClient } from "../client-connect.js";
export type { NativeClientConnectionOptions } from "../client-connect.js";
export { connectNativeWorkerClient } from "../client-worker-connect.js";
export type { NativeWorkerClientOptions } from "../client-worker-connect.js";
export type {
  Result,
  Row,
  Value,
  Param,
  SqlExecutor,
  Cursor,
  CallOptions,
  CursorOptions,
} from "../api.js";

export { AmbiguousResultColumnError } from "../result.js";

export type { CatalogDescription, ExplainDescription } from "../inspection.js";
