export const protocolVersion = 3 as const;
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
 * so typed engine errors survive the channel and can be rehydrated for instanceof checks.
 */
export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
  props?: Record<string, unknown>;
}

export function serializeError(error: unknown): SerializedError {
  if (!(error instanceof Error)) {
    return { name: "Error", message: String(error) };
  }
  const props: Record<string, unknown> = {};
  for (const key of Object.keys(error)) {
    if (key === "name" || key === "message" || key === "stack") continue;
    const value = (error as unknown as Record<string, unknown>)[key];
    try {
      structuredClone(value);
      props[key] = value;
    } catch {
      // A non-cloneable field would poison the whole postMessage; drop it.
    }
  }
  return {
    name: error.name,
    message: error.message,
    ...(error.stack === undefined ? {} : { stack: error.stack }),
    ...(Object.keys(props).length === 0 ? {} : { props }),
  };
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
