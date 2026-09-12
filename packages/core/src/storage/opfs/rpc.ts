import {
  BlockReadBatchTooLargeError,
  CompactionBacklogError,
  CompactionJobConflictError,
  ConnectionLostError,
  GarbageCollectionJobConflictError,
  IndexedDbSchemaUpgradeBlockedError,
  LeaseConflictError,
  LeaseExpiredError,
  LeaseOwnerConflictError,
  OpfsCoordinationError,
  OpfsDatabaseInUseError,
  OpfsUncertainOutcomeError,
  PostingBuildConflictError,
  SchemaConflictError,
  SnapshotImportConflictError,
  SnapshotManifestMissingError,
  StorageCorruptionError,
  StorageFormatVersionError,
  StorageResourceLimitError,
  TableInUseError,
  TableRecordConflictError,
  TempOwnerConflictError,
  TransactionRecordConflictError,
  UniqueIndexCoverageError,
  UniqueKeyBuildConflictError,
  UniqueKeyConflictError,
  UnknownOutcomeError,
  WriteConflictError,
} from "../types.js";
import { dateIsoString } from "../../date-value.js";
import {
  MAX_SERIALIZED_CAUSE_DEPTH,
  rehydrateError,
  serializeError,
  type SerializedError,
} from "../../worker-protocol/index.js";

/**
 * The follower↔leader message vocabulary. Leadership traffic (`leader`, `state`, `ping`, `bid`,
 * `yield`, `released`) travels a `BroadcastChannel` named for the database, which every
 * connection hears. Request traffic is addressed: an `op` is posted into the leader's own
 * channel (named for the database plus the leader's instance id) and its `result`, `busy`,
 * `hold`, or `declined` into the requester's, so block bytes are cloned once, into the tab that
 * asked, rather than into every tab. Correctness never rides on delivery: an operation is
 * acknowledged only after the leader's write-ahead log holds it. A declined request provably
 * never ran and is simply sent again; reads retry after failover; a mutation whose leader
 * vanished mid-flight surfaces an uncertain outcome instead of being executed twice. Every kind
 * is parsed by exact shape, so a connection from another release ignores what it does not
 * know. Leadership is arbitrated by the storage lock, not by these messages.
 */

export type StoreRpcMessage =
  /**
   * A follower's operation request. `requestId` doubles as the retry-dedupe key; `from` is the
   * requester's instance id, naming the channel its answer goes to.
   */
  | {
      kind: "op";
      requestId: string;
      from: string;
      method: string;
      args: unknown[];
      /** When the requester first sent it; a re-send keeps it, so the ledger can vouch. */
      sentAt: number;
    }
  | { kind: "result"; requestId: string; ok: true; value: unknown }
  | { kind: "result"; requestId: string; ok: false; error: SerializedStoreError }
  /** The leader is still executing this request — reset the caller's patience. */
  | { kind: "busy"; requestId: string }
  /**
   * The leader is about to do something synchronous and long (a checkpoint); wait at least
   * this long before assuming it is gone.
   */
  | { kind: "hold"; requestId: string; ms: number }
  /**
   * The receiver is not leading, so the request did not run and never will here. The follower
   * re-discovers the leader and sends it again — safely, because nothing happened.
   */
  | { kind: "declined"; requestId: string; reason: "not-leader" }
  /**
   * A re-sent mutation the leader's ledger neither holds nor can rule out: it was first sent
   * before the ledger's coverage begins, so an earlier leader may have executed it.
   */
  | { kind: "uncertain"; requestId: string }
  /** The leader exists (sent on takeover and in answer to pings). */
  | { kind: "leader"; leaderId: string }
  /** The leader's visibility, so a foreground follower knows when to bid. */
  | { kind: "state"; leaderId: string; foreground: boolean }
  | { kind: "ping" }
  /** A foreground follower asks the (background) leader to hand over. */
  | { kind: "bid"; bidderId: string; foreground: boolean }
  /** The leader has drained and released its handles for this bidder. */
  | { kind: "yield"; to: string }
  /** The leader closed or demoted; followers may race for the lock. */
  | { kind: "released"; leaderId: string };

/** The wire form of a served operation's failure; the worker protocol's shape, validated. */
export type SerializedStoreError = SerializedError;

/** Stack text longer than this is not a stack; it is a payload wearing one. */
const MAX_OPFS_RPC_STACK_CHARACTERS = 64 * 1024;

/** One legal maximum payload plus bounded envelope/catalog metadata. */
export const MAX_OPFS_RPC_MESSAGE_BYTES = 66 * 1024 * 1024;
/** The longest patience a `hold` may ask for; a leader busier than this is treated as gone. */
export const MAX_OPFS_RPC_HOLD_MS = 60_000;
export const MAX_OPFS_RPC_IDENTIFIER_CHARACTERS = 1024;
const MAX_OPFS_RPC_DEPTH = 64;
const MAX_OPFS_RPC_NODES = 1_100_000;

function exactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value);
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}

function boundedRpcString(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MAX_OPFS_RPC_IDENTIFIER_CHARACTERS
  );
}

/** Conservative retained/transfer-byte estimate with total depth/cardinality refusal. */
export function estimateRpcValueBytes(value: unknown): number {
  const seen = new WeakSet();
  let nodes = 0;
  const visit = (current: unknown, depth: number): number => {
    nodes += 1;
    if (nodes > MAX_OPFS_RPC_NODES || depth > MAX_OPFS_RPC_DEPTH) {
      throw new RangeError("OPFS RPC value exceeds its structural limit");
    }
    if (current === null || current === undefined || typeof current === "boolean") return 8;
    if (typeof current === "number" || typeof current === "bigint") return 16;
    if (typeof current === "string") return 16 + current.length * 2;
    if (typeof current !== "object") throw new TypeError("OPFS RPC value is not cloneable data");
    if (seen.has(current)) throw new TypeError("OPFS RPC value must not contain cycles");
    seen.add(current);
    if (typeof SharedArrayBuffer !== "undefined" && current instanceof SharedArrayBuffer) {
      throw new TypeError("OPFS RPC values must not use SharedArrayBuffer");
    }
    if (current instanceof ArrayBuffer) return 32 + current.byteLength;
    if (ArrayBuffer.isView(current)) {
      if (typeof SharedArrayBuffer !== "undefined" && current.buffer instanceof SharedArrayBuffer) {
        throw new TypeError("OPFS RPC values must not use SharedArrayBuffer");
      }
      return 48 + current.byteLength;
    }
    if (current instanceof Date) return 32;
    let bytes = 32;
    if (Array.isArray(current)) {
      for (const item of current) bytes += visit(item, depth + 1);
    } else {
      if (
        Object.getPrototypeOf(current) !== Object.prototype &&
        Object.getPrototypeOf(current) !== null
      ) {
        throw new TypeError("OPFS RPC value has an unsupported prototype");
      }
      for (const [key, item] of Object.entries(current)) {
        bytes += key.length * 2 + visit(item, depth + 1);
      }
    }
    if (!Number.isSafeInteger(bytes) || bytes > MAX_OPFS_RPC_MESSAGE_BYTES) {
      throw new RangeError("OPFS RPC message exceeds its byte limit");
    }
    return bytes;
  };
  const bytes = visit(value, 0);
  if (bytes > MAX_OPFS_RPC_MESSAGE_BYTES) {
    throw new RangeError("OPFS RPC message exceeds its byte limit");
  }
  return bytes;
}

function validSerializedError(value: unknown, depth = 0): value is SerializedStoreError {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  const optional = ["stack", "domException", "props", "cause"] as const;
  const allowed = ["name", "message", ...optional.filter((key) => record[key] !== undefined)];
  return (
    exactKeys(record, allowed) &&
    boundedRpcString(record.name) &&
    typeof record.message === "string" &&
    record.message.length <= MAX_OPFS_RPC_IDENTIFIER_CHARACTERS * 4 &&
    (record.stack === undefined ||
      (typeof record.stack === "string" && record.stack.length <= MAX_OPFS_RPC_STACK_CHARACTERS)) &&
    (record.domException === undefined || record.domException === true) &&
    (record.props === undefined ||
      (typeof record.props === "object" &&
        record.props !== null &&
        !Array.isArray(record.props))) &&
    (record.cause === undefined ||
      (depth < MAX_SERIALIZED_CAUSE_DEPTH && validSerializedError(record.cause, depth + 1)))
  );
}

/** Total non-throwing parser for same-origin-but-untrusted BroadcastChannel traffic. */
export function parseStoreRpcMessage(
  value: unknown,
  knownMethods: ReadonlySet<string>,
): StoreRpcMessage | undefined {
  try {
    if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
    const record = value as Record<string, unknown>;
    estimateRpcValueBytes(record);
    switch (record.kind) {
      case "op":
        if (
          !exactKeys(record, ["kind", "requestId", "from", "method", "args", "sentAt"]) ||
          !boundedRpcString(record.requestId) ||
          !boundedRpcString(record.from) ||
          !boundedRpcString(record.method) ||
          !knownMethods.has(record.method) ||
          !Array.isArray(record.args) ||
          !Number.isSafeInteger(record.sentAt) ||
          (record.sentAt as number) < 0 ||
          estimateRpcValueBytes(record.args) > MAX_OPFS_RPC_MESSAGE_BYTES
        )
          return undefined;
        return record as StoreRpcMessage;
      case "result":
        if (!boundedRpcString(record.requestId) || typeof record.ok !== "boolean") return undefined;
        if (record.ok) {
          if (!exactKeys(record, ["kind", "requestId", "ok", "value"])) return undefined;
          estimateRpcValueBytes(record.value);
        } else if (
          !exactKeys(record, ["kind", "requestId", "ok", "error"]) ||
          !validSerializedError(record.error)
        )
          return undefined;
        return record as StoreRpcMessage;
      case "busy":
        return exactKeys(record, ["kind", "requestId"]) && boundedRpcString(record.requestId)
          ? (record as StoreRpcMessage)
          : undefined;
      case "hold":
        return exactKeys(record, ["kind", "requestId", "ms"]) &&
          boundedRpcString(record.requestId) &&
          typeof record.ms === "number" &&
          Number.isSafeInteger(record.ms) &&
          record.ms >= 0 &&
          record.ms <= MAX_OPFS_RPC_HOLD_MS
          ? (record as StoreRpcMessage)
          : undefined;
      case "uncertain":
        return exactKeys(record, ["kind", "requestId"]) && boundedRpcString(record.requestId)
          ? (record as StoreRpcMessage)
          : undefined;
      case "declined":
        return exactKeys(record, ["kind", "requestId", "reason"]) &&
          boundedRpcString(record.requestId) &&
          record.reason === "not-leader"
          ? (record as StoreRpcMessage)
          : undefined;
      case "state":
        return exactKeys(record, ["kind", "leaderId", "foreground"]) &&
          boundedRpcString(record.leaderId) &&
          typeof record.foreground === "boolean"
          ? (record as StoreRpcMessage)
          : undefined;
      case "leader":
      case "released":
        return exactKeys(record, ["kind", "leaderId"]) && boundedRpcString(record.leaderId)
          ? (record as StoreRpcMessage)
          : undefined;
      case "ping":
        return exactKeys(record, ["kind"]) ? (record as StoreRpcMessage) : undefined;
      case "bid":
        return exactKeys(record, ["kind", "bidderId", "foreground"]) &&
          boundedRpcString(record.bidderId) &&
          typeof record.foreground === "boolean"
          ? (record as StoreRpcMessage)
          : undefined;
      case "yield":
        return exactKeys(record, ["kind", "to"]) && boundedRpcString(record.to)
          ? (record as StoreRpcMessage)
          : undefined;
      default:
        return undefined;
    }
  } catch {
    return undefined;
  }
}

function hex(bytes: Uint8Array): string {
  let result = "";
  for (const byte of bytes) result += byte.toString(16).padStart(2, "0");
  return result;
}

/** Collision-resistant bounded identity; large byte leaves become SHA-256 descriptors. */
export async function fingerprintStoreRequest(
  method: string,
  args: unknown[],
): Promise<{ signature: string; retainedBytes: number }> {
  const retainedBytes = estimateRpcValueBytes(args);
  const seen = new WeakSet();
  const normalize = async (value: unknown, depth: number): Promise<unknown> => {
    if (depth > MAX_OPFS_RPC_DEPTH) throw new RangeError("OPFS RPC value exceeds its depth limit");
    if (typeof value === "bigint") return ["bigint", value.toString()];
    if (value === undefined) return ["undefined"];
    if (typeof value === "number") {
      return ["number", Object.is(value, -0) ? "-0" : String(value)];
    }
    if (value === null || typeof value !== "object") return value;
    if (seen.has(value)) throw new TypeError("OPFS RPC value must not contain cycles");
    seen.add(value);
    if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
      const { byteLength } = value;
      let digestInput: ArrayBuffer;
      if (value instanceof ArrayBuffer) digestInput = value;
      else if (!(value.buffer instanceof ArrayBuffer)) {
        throw new TypeError("OPFS RPC values must not use SharedArrayBuffer");
      } else if (value.byteOffset === 0 && value.byteLength === value.buffer.byteLength) {
        digestInput = value.buffer;
      } else {
        digestInput = value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength);
      }
      const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", digestInput));
      return ["bytes", value.constructor.name, byteLength, hex(digest)];
    }
    if (value instanceof Date) return ["date", dateIsoString(value)];
    if (Array.isArray(value)) {
      return Promise.all(value.map((item) => normalize(item, depth + 1)));
    }
    const entries: Array<readonly [string, unknown]> = [];
    for (const key of Object.keys(value).sort()) {
      entries.push([key, await normalize((value as Record<string, unknown>)[key], depth + 1)]);
    }
    return ["object", entries];
  };
  const canonical = JSON.stringify([method, await normalize(args, 0)]);
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonical)),
  );
  return { signature: `${String(retainedBytes)}:${hex(digest)}`, retainedBytes };
}

const errorRegistry = new Map<string, new (...args: never[]) => Error>(
  [
    BlockReadBatchTooLargeError,
    CompactionBacklogError,
    WriteConflictError,
    SchemaConflictError,
    UniqueKeyConflictError,
    UniqueIndexCoverageError,
    TableRecordConflictError,
    TransactionRecordConflictError,
    LeaseConflictError,
    LeaseExpiredError,
    LeaseOwnerConflictError,
    CompactionJobConflictError,
    GarbageCollectionJobConflictError,
    IndexedDbSchemaUpgradeBlockedError,
    TempOwnerConflictError,
    StorageResourceLimitError,
    TableInUseError,
    SnapshotManifestMissingError,
    SnapshotImportConflictError,
    UniqueKeyBuildConflictError,
    PostingBuildConflictError,
    StorageCorruptionError,
    StorageFormatVersionError,
    OpfsCoordinationError,
    OpfsDatabaseInUseError,
    OpfsUncertainOutcomeError,
    UnknownOutcomeError,
    ConnectionLostError,
  ].map((constructor) => [constructor.name, constructor]),
);

export function serializeStoreError(error: unknown): SerializedStoreError {
  return serializeError(error);
}

export function rehydrateStoreError(serialized: SerializedStoreError): Error {
  return rehydrateError(serialized, errorRegistry);
}
