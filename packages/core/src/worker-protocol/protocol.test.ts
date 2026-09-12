import { expect, it } from "vitest";
import {
  MAX_SERIALIZED_CAUSE_DEPTH,
  WORKER_DIAGNOSTIC_HANDLE_ID,
  isWorkerErrorReport,
  parseRpcRequest,
  parseRpcResponse,
  protocolVersion,
  rehydrateError,
  serializeError,
  workerErrorEvent,
  type SerializedError,
} from "./index.js";

it("validates every RPC request boundary", () => {
  expect(parseRpcRequest(null)).toBeNull();
  expect(parseRpcRequest({ kind: "success" })).toBeNull();
  expect(() => parseRpcRequest({ kind: "rpc-init", version: 99, requestId: "one" })).toThrow(
    "version",
  );
  expect(() =>
    parseRpcRequest({ kind: "rpc-init", version: protocolVersion, requestId: "" }),
  ).toThrow("Request ID");
  expect(() =>
    parseRpcRequest({
      kind: "rpc-call",
      version: protocolVersion,
      requestId: "one",
      handleId: 1,
      method: "query",
      args: [],
    }),
  ).toThrow("Handle ID");
  expect(() =>
    parseRpcRequest({
      kind: "rpc-call",
      version: protocolVersion,
      requestId: "one",
      handleId: null,
      method: "",
      args: [],
    }),
  ).toThrow("Method");
  expect(() =>
    parseRpcRequest({
      kind: "rpc-call",
      version: protocolVersion,
      requestId: "one",
      handleId: null,
      method: "query",
      args: null,
    }),
  ).toThrow("Arguments");
  expect(
    parseRpcRequest({
      kind: "rpc-call",
      version: protocolVersion,
      requestId: "one",
      handleId: null,
      method: "query",
      args: [],
    }),
  ).toMatchObject({ kind: "rpc-call", method: "query" });
  expect(
    parseRpcRequest({
      kind: "rpc-cancel",
      version: protocolVersion,
      requestId: "one",
    }),
  ).toEqual({ kind: "rpc-cancel", version: protocolVersion, requestId: "one" });
});

it("rejects malformed RPC responses and serializes only cloneable error state", () => {
  expect(parseRpcResponse(null)).toBeNull();
  expect(parseRpcResponse({ kind: "rpc-call" })).toBeNull();
  expect(() => parseRpcResponse({ kind: "rpc-result", version: 99 })).toThrow("version");

  expect(serializeError("bad")).toEqual({ name: "Error", message: "bad" });
  const error = new Error("broken") as Error & { code: number; callback: () => void };
  error.code = 7;
  error.callback = () => undefined;
  Object.defineProperty(error, "stack", { value: undefined, configurable: true });
  expect(serializeError(error)).toEqual({
    name: "Error",
    message: "broken",
    props: { code: 7 },
  });
});

it("carries the cause chain, platform identity, and built-in subclasses across the wire", () => {
  const registry = new Map<string, new (...args: never[]) => Error>();
  const quota = new DOMException("disk full", "QuotaExceededError");
  const inner = new TypeError("bad shape", { cause: quota });
  const outer = new Error("write failed", { cause: inner });
  const serialized = serializeError(outer);
  expect(serialized.cause?.name).toBe("TypeError");
  expect(serialized.cause?.cause).toMatchObject({
    name: "QuotaExceededError",
    message: "disk full",
    domException: true,
  });

  const rehydrated = rehydrateError(serialized, registry);
  expect(rehydrated).toBeInstanceOf(Error);
  expect(rehydrated.message).toBe("write failed");
  expect(rehydrated.cause).toBeInstanceOf(TypeError);
  expect((rehydrated.cause as Error).message).toBe("bad shape");
  const platform = (rehydrated.cause as Error).cause;
  expect(platform).toBeInstanceOf(DOMException);
  expect(platform).toMatchObject({ name: "QuotaExceededError", message: "disk full" });

  // A registered class wins over the built-in fallback and keeps its fields.
  class TypedError extends TypeError {
    override readonly name = "TypedError";
    constructor(readonly code: number) {
      super(`typed ${String(code)}`);
    }
  }
  registry.set("TypedError", TypedError);
  const typed = rehydrateError(serializeError(new TypedError(4)), registry);
  expect(typed).toBeInstanceOf(TypedError);
  expect(typed).toBeInstanceOf(TypeError);
  expect(typed).toMatchObject({ code: 4, message: "typed 4" });

  // Non-Error causes and unknown names survive as plain errors.
  const odd = rehydrateError(
    serializeError(new Error("odd", { cause: "just a string" })),
    registry,
  );
  expect(odd.cause).toBeInstanceOf(Error);
  expect((odd.cause as Error).message).toBe("just a string");
});

it("cuts a cause chain at the depth limit instead of walking it forever", () => {
  let error: Error = new Error("leaf");
  for (let depth = 0; depth < MAX_SERIALIZED_CAUSE_DEPTH + 5; depth += 1) {
    error = new Error(`level ${String(depth)}`, { cause: error });
  }
  let serialized: SerializedError = serializeError(error);
  let links = 0;
  while (serialized.cause !== undefined) {
    serialized = serialized.cause;
    links += 1;
  }
  expect(links).toBe(MAX_SERIALIZED_CAUSE_DEPTH);

  // A self-referential cause terminates too.
  const loop = new Error("loop");
  loop.cause = loop;
  expect(() => serializeError(loop)).not.toThrow();
});

it("recognizes a worker error report frame and nothing else", () => {
  const frame = workerErrorEvent({
    kind: "unhandled-rejection",
    context: "worker global scope",
    error: serializeError(new RangeError("out of range")),
  });
  expect(frame).toMatchObject({
    kind: "rpc-event",
    handleId: WORKER_DIAGNOSTIC_HANDLE_ID,
    event: "error",
  });
  expect(isWorkerErrorReport((frame as { payload: unknown }).payload)).toBe(true);
  expect(isWorkerErrorReport({ kind: "uncaught", context: "x" })).toBe(false);
  expect(isWorkerErrorReport({ kind: "uncaught", context: "x", error: { name: "E" } })).toBe(false);
  expect(isWorkerErrorReport(null)).toBe(false);
});
