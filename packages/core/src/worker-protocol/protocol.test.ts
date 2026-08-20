import { expect, it } from "vitest";
import { parseRequest, protocolVersion } from "./index.js";

it("accepts a versioned known operation", () => {
  expect(
    parseRequest({ version: protocolVersion, requestId: "one", operation: "ping", payload: null }),
  ).toMatchObject({ operation: "ping" });
});

it("rejects unknown versions and operations", () => {
  expect(() => parseRequest({ version: 99, requestId: "one", operation: "ping" })).toThrow(
    "version",
  );
  expect(() =>
    parseRequest({ version: protocolVersion, requestId: "one", operation: "sql" }),
  ).toThrow("operation");
});
