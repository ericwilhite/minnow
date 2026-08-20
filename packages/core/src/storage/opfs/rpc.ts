import {
  CompactionJobConflictError,
  GarbageCollectionJobConflictError,
  LeaseConflictError,
  SnapshotManifestMissingError,
  TableRecordConflictError,
  TempOwnerConflictError,
  TransactionRecordConflictError,
  UniqueKeyConflictError,
  WriteConflictError,
} from "../types.js";

/**
 * The follower↔leader message vocabulary. Leadership traffic (`leader`, `ping`, `bid`, `yield`,
 * `released`) travels a `BroadcastChannel` named for the database, which every connection
 * hears. Request traffic is addressed: an `op` is posted into the leader's own channel (named
 * for the database plus the leader's instance id) and its `result` or `busy` into the
 * requester's, so block bytes are cloned once, into the tab that asked, rather than into every
 * tab. Correctness never rides on delivery: an operation is acknowledged only after the
 * leader's write-ahead log holds it, a lost message costs a retry, and a dead leader costs a
 * failover arbitrated by the storage lock, not by these messages.
 */

export type StoreRpcMessage =
  /**
   * A follower's operation request. `requestId` doubles as the retry-dedupe key; `from` is the
   * requester's instance id, naming the channel its answer goes to.
   */
  | { kind: "op"; requestId: string; from: string; method: string; args: unknown[] }
  | { kind: "result"; requestId: string; ok: true; value: unknown }
  | { kind: "result"; requestId: string; ok: false; error: SerializedStoreError }
  /** The leader is still executing this request — reset the caller's patience. */
  | { kind: "busy"; requestId: string }
  /** The leader exists (sent on takeover and in answer to pings). */
  | { kind: "leader"; leaderId: string }
  | { kind: "ping" }
  /** A foreground follower asks the (background) leader to hand over. */
  | { kind: "bid"; bidderId: string; foreground: boolean }
  /** The leader has drained and released its handles for this bidder. */
  | { kind: "yield"; to: string }
  /** The leader closed or demoted; followers may race for the lock. */
  | { kind: "released"; leaderId: string };

export interface SerializedStoreError {
  name: string;
  message: string;
  /** True for platform exceptions (quota!), whose `instanceof DOMException` must survive. */
  domException?: boolean;
  props?: Record<string, unknown>;
}

const errorRegistry = new Map<string, new (...args: never[]) => Error>(
  [
    WriteConflictError,
    UniqueKeyConflictError,
    TableRecordConflictError,
    TransactionRecordConflictError,
    LeaseConflictError,
    CompactionJobConflictError,
    GarbageCollectionJobConflictError,
    TempOwnerConflictError,
    SnapshotManifestMissingError,
  ].map((constructor) => [constructor.name, constructor]),
);

export function serializeStoreError(error: unknown): SerializedStoreError {
  if (error instanceof DOMException) {
    return { name: error.name, message: error.message, domException: true };
  }
  if (!(error instanceof Error)) return { name: "Error", message: String(error) };
  const props: Record<string, unknown> = {};
  for (const key of Object.keys(error)) {
    const value = (error as unknown as Record<string, unknown>)[key];
    try {
      structuredClone(value);
      props[key] = value;
    } catch {
      // A non-cloneable field would poison the whole message; drop it.
    }
  }
  return {
    name: error.name,
    message: error.message,
    ...(Object.keys(props).length === 0 ? {} : { props }),
  };
}

export function rehydrateStoreError(serialized: SerializedStoreError): Error {
  if (serialized.domException === true) {
    return new DOMException(serialized.message, serialized.name);
  }
  const constructor = errorRegistry.get(serialized.name);
  const error: Error =
    constructor === undefined
      ? new Error(serialized.message)
      : (Object.create(constructor.prototype as object) as Error);
  Object.defineProperty(error, "message", {
    value: serialized.message,
    writable: true,
    configurable: true,
  });
  Object.defineProperty(error, "name", {
    value: serialized.name,
    writable: true,
    configurable: true,
  });
  if (serialized.props !== undefined) Object.assign(error, serialized.props);
  return error;
}
