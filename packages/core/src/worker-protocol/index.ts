export const protocolVersion = 7 as const;
/** Outstanding request/response pairs retained by either side of one database RPC connection. */
export const MAX_DATABASE_RPC_IN_FLIGHT = 256;

// --- Database RPC frames ------------------------------------------------------------------------
//
// The method-oriented message family used by the engine's worker client. An RPC call names a
// target handle and a method so the channel scales with the database API instead of this
// package. Other message families may share the channel: parseRpcRequest and parseRpcResponse
// return null for anything that is not an RPC frame.

export type RpcRequest =
  | {
      version: typeof protocolVersion;
      requestId: string;
      kind: "rpc-init";
      payload: unknown;
    }
  | {
      version: typeof protocolVersion;
      requestId: string;
      kind: "rpc-call";
      /** Null targets the root database; otherwise a handle issued by an earlier response. */
      handleId: string | null;
      method: string;
      args: unknown[];
    }
  | {
      version: typeof protocolVersion;
      /** The request whose work should stop. Cancellation has no response frame of its own. */
      requestId: string;
      kind: "rpc-cancel";
    };

export type RpcResponse =
  | {
      version: typeof protocolVersion;
      requestId: string;
      kind: "rpc-result";
      result: unknown;
    }
  | {
      version: typeof protocolVersion;
      requestId: string;
      kind: "rpc-failure";
      error: SerializedError;
    }
  | {
      version: typeof protocolVersion;
      requestId: string | null;
      kind: "rpc-event";
      handleId: string;
      event: string;
      payload: unknown;
    };

/**
 * A structured-clone-safe error carrying the constructor name and every cloneable own property,
 * so typed engine errors survive the channel and can be rehydrated for instanceof checks. The
 * `cause` chain travels to a bounded depth, and a platform exception keeps its `DOMException`
 * identity, so a quota refusal is still a `DOMException` named `QuotaExceededError` on the
 * other side.
 */
export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
  props?: Record<string, unknown>;
  /** True for platform exceptions, which rehydrate as `DOMException` rather than `Error`. */
  domException?: true;
  cause?: SerializedError;
}

/** How many `cause` links a serialized error carries before the chain is cut. */
export const MAX_SERIALIZED_CAUSE_DEPTH = 8;

export function serializeError(error: unknown, depth = 0): SerializedError {
  if (typeof DOMException !== "undefined" && error instanceof DOMException) {
    return {
      name: error.name,
      message: error.message,
      domException: true,
      ...(typeof error.stack === "string" ? { stack: error.stack } : {}),
    };
  }
  if (!(error instanceof Error)) {
    return { name: "Error", message: String(error) };
  }
  const props: Record<string, unknown> = {};
  for (const key of Object.keys(error)) {
    if (key === "name" || key === "message" || key === "stack" || key === "cause") continue;
    const value = (error as unknown as Record<string, unknown>)[key];
    try {
      structuredClone(value);
      props[key] = value;
    } catch {
      // A non-cloneable field would poison the whole postMessage; drop it.
    }
  }
  const cause =
    error.cause === undefined || depth >= MAX_SERIALIZED_CAUSE_DEPTH
      ? undefined
      : serializeError(error.cause, depth + 1);
  return {
    name: error.name,
    message: error.message,
    ...(error.stack === undefined ? {} : { stack: error.stack }),
    ...(Object.keys(props).length === 0 ? {} : { props }),
    ...(cause === undefined ? {} : { cause }),
  };
}

/** Constructors whose prototype a rehydrated error adopts, keyed by the `name` they carry. */
export type ErrorRegistry = ReadonlyMap<string, new (...args: never[]) => Error>;

const builtinErrorNames = new Set([
  "TypeError",
  "RangeError",
  "SyntaxError",
  "ReferenceError",
  "EvalError",
  "URIError",
  "AggregateError",
]);

/**
 * Rebuilds an `Error` from its serialized form: a registry hit adopts that class's prototype,
 * a platform exception becomes a `DOMException`, a built-in subclass name (`TypeError`) keeps
 * its built-in prototype, and anything else is a plain `Error` carrying the original name.
 * Own properties and the `cause` chain are restored; the stack stays the worker's.
 */
export function rehydrateError(serialized: SerializedError, registry: ErrorRegistry): Error {
  const cause =
    serialized.cause === undefined ? undefined : rehydrateError(serialized.cause, registry);
  if (serialized.domException === true) {
    const exception = new DOMException(serialized.message, serialized.name);
    if (cause !== undefined) {
      Object.defineProperty(exception, "cause", {
        value: cause,
        writable: true,
        configurable: true,
      });
    }
    return exception;
  }
  const constructor = registry.get(serialized.name);
  let error: Error;
  if (constructor !== undefined) {
    error = Object.create(constructor.prototype as object) as Error;
  } else if (builtinErrorNames.has(serialized.name)) {
    const builtin = (globalThis as Record<string, unknown>)[serialized.name];
    error =
      typeof builtin === "function"
        ? (Object.create((builtin as { prototype: object }).prototype) as Error)
        : new Error(serialized.message);
  } else {
    error = new Error(serialized.message);
  }
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
  if (serialized.stack !== undefined) {
    Object.defineProperty(error, "stack", {
      value: serialized.stack,
      writable: true,
      configurable: true,
    });
  }
  if (cause !== undefined) {
    Object.defineProperty(error, "cause", { value: cause, writable: true, configurable: true });
  }
  if (serialized.props !== undefined) Object.assign(error, serialized.props);
  return error;
}

// --- Worker diagnostics ---------------------------------------------------------------------------
//
// Failures that belong to no request — an uncaught exception, an unhandled rejection, a failed
// background checkpoint, a broken leader election — ride the event frame on this reserved
// handle id, so the main thread hears them instead of the worker's own console alone.

/** The event-frame handle id that carries worker diagnostics. Never issued for a real handle. */
export const WORKER_DIAGNOSTIC_HANDLE_ID = "$worker";

export type WorkerErrorKind =
  /** A script error nobody caught, in a timer, a channel listener, or another callback. */
  | "uncaught"
  /** A promise rejected with no handler attached. */
  | "unhandled-rejection"
  /** A frame the worker could not deserialize. */
  | "messageerror"
  /** Background maintenance: a checkpoint, cleanup, or collection step that failed. */
  | "maintenance"
  /** Multi-tab coordination: an election, handover, or served request that failed. */
  | "coordination";

export interface WorkerErrorReport {
  kind: WorkerErrorKind;
  /** Where in the worker it happened, for a log line: "opfs election", "auto collection". */
  context: string;
  error: SerializedError;
}

export function workerErrorEvent(report: WorkerErrorReport): RpcResponse {
  return rpcEvent(WORKER_DIAGNOSTIC_HANDLE_ID, "error", report);
}

/**
 * How often the worker tells the client that a call is still being worked on. The client's
 * request deadline counts silence, not wall time, so a large batch write is not cut off — and
 * its client left permanently failed — merely for being large.
 */
export const WORKER_KEEPALIVE_INTERVAL_MS = 5_000;
/** The shortest pace a client may ask for; below this the reports would be the load. */
export const MIN_WORKER_KEEPALIVE_INTERVAL_MS = 100;

/** A keepalive for one in-flight request: the worker is alive and still holds it. */
export function workerKeepaliveEvent(requestId: string): RpcResponse {
  return {
    version: protocolVersion,
    requestId,
    kind: "rpc-event",
    handleId: WORKER_DIAGNOSTIC_HANDLE_ID,
    event: "keepalive",
    payload: null,
  };
}

export function isWorkerErrorReport(value: unknown): value is WorkerErrorReport {
  if (typeof value !== "object" || value === null) return false;
  const report = value as Partial<Record<keyof WorkerErrorReport, unknown>>;
  if (typeof report.kind !== "string" || typeof report.context !== "string") return false;
  if (typeof report.error !== "object" || report.error === null) return false;
  const error = report.error as Partial<Record<keyof SerializedError, unknown>>;
  return typeof error.name === "string" && typeof error.message === "string";
}

export function rpcResult(requestId: string, result: unknown): RpcResponse {
  return { version: protocolVersion, requestId, kind: "rpc-result", result };
}

export function rpcFailure(requestId: string, error: unknown): RpcResponse {
  return { version: protocolVersion, requestId, kind: "rpc-failure", error: serializeError(error) };
}

export function rpcEvent(handleId: string, event: string, payload: unknown): RpcResponse {
  return { version: protocolVersion, requestId: null, kind: "rpc-event", handleId, event, payload };
}

/** Parses an incoming RPC request; returns null for non-RPC traffic sharing the channel. */
export function parseRpcRequest(value: unknown): RpcRequest | null {
  if (typeof value !== "object" || value === null) return null;
  const candidate = value as Partial<RpcRequest> & { kind?: unknown };
  if (
    candidate.kind !== "rpc-init" &&
    candidate.kind !== "rpc-call" &&
    candidate.kind !== "rpc-cancel"
  ) {
    return null;
  }
  if (candidate.version !== protocolVersion) throw new Error("Unsupported protocol version");
  if (typeof candidate.requestId !== "string" || candidate.requestId.length === 0) {
    throw new TypeError("Request ID must be a non-empty string");
  }
  if (candidate.kind === "rpc-call") {
    const call = candidate as Partial<Extract<RpcRequest, { kind: "rpc-call" }>>;
    if (call.handleId !== null && typeof call.handleId !== "string") {
      throw new TypeError("Handle ID must be a string or null");
    }
    if (typeof call.method !== "string" || call.method.length === 0) {
      throw new TypeError("Method must be a non-empty string");
    }
    if (!Array.isArray(call.args)) throw new TypeError("Arguments must be an array");
  }
  return candidate as RpcRequest;
}

/** Narrows an incoming message to an RPC response; returns null for non-RPC traffic. */
export function parseRpcResponse(value: unknown): RpcResponse | null {
  if (typeof value !== "object" || value === null) return null;
  const candidate = value as Partial<RpcResponse> & { kind?: unknown };
  if (
    candidate.kind !== "rpc-result" &&
    candidate.kind !== "rpc-failure" &&
    candidate.kind !== "rpc-event"
  ) {
    return null;
  }
  if (candidate.version !== protocolVersion) throw new Error("Unsupported protocol version");
  return candidate as RpcResponse;
}
