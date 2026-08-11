import { expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { protocolVersion } from "../worker-protocol/index.js";
import { createCoordinator } from "./index.js";

it("routes versioned worker requests", async () => {
  const coordinate = createCoordinator(new MemoryBlockStore());
  const reply = await coordinate({
    version: protocolVersion,
    requestId: "ping-1",
    operation: "ping",
    payload: null,
  });
  expect(reply.message).toMatchObject({ kind: "success", result: { ready: true } });
});

it("turns protocol errors into failure responses", async () => {
  const coordinate = createCoordinator(new MemoryBlockStore());
  const reply = await coordinate({ version: 99, requestId: "bad", operation: "ping" });
  expect(reply.message).toMatchObject({ kind: "failure", requestId: "bad" });
});
