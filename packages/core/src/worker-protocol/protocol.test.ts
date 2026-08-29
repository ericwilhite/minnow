import { expect, it } from "vitest";
import { parseRpcRequest, parseRpcResponse, protocolVersion, serializeError } from "./index.js";

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
