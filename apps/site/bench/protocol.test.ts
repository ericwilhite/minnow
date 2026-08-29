import { protocolVersion } from "@minnowdb/core/worker-protocol";
import { expect, it } from "vitest";
import { failure, parseRequest, success } from "./protocol";

it("accepts a versioned known operation", () => {
  expect(
    parseRequest({
      version: protocolVersion,
      requestId: "one",
      operation: "runQuery",
      payload: null,
    }),
  ).toMatchObject({ operation: "runQuery" });
});

it("rejects unknown versions and operations", () => {
  expect(() => parseRequest({ version: 99, requestId: "one", operation: "runQuery" })).toThrow(
    "version",
  );
  expect(() =>
    parseRequest({ version: protocolVersion, requestId: "one", operation: "sql" }),
  ).toThrow("operation");
});

it("rejects malformed worker requests and builds clone-safe responses", () => {
  expect(() => parseRequest(null)).toThrow("object");
  expect(() =>
    parseRequest({ version: protocolVersion, requestId: "", operation: "runQuery" }),
  ).toThrow("Request ID");
  expect(success("request", { ok: true })).toEqual({
    version: protocolVersion,
    requestId: "request",
    kind: "success",
    result: { ok: true },
  });
  expect(failure("request", "lost")).toMatchObject({
    kind: "failure",
    error: { name: "Error", message: "lost" },
  });
});
