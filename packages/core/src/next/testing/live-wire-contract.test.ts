/** Uses only the pre-B8 wire protocol: the parent rejects a promised subscribe request. */
import { expect, it } from "vitest";
import { createLowerer } from "../lower/create.js";
import { chunkReads } from "../exec/scan.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Harness } from "../write/test-support.js";
import { raw } from "./owner-harness.js";

it("serves the subscribe wire contract with an initial result", async () => {
  const h = new Harness("strict", "memory");
  const path = await h.open();
  const engine = composeEngine({
    planner: createLowerer(),
    writePath: path,
    governor: h.governor,
    spill: new MemorySpillService(),
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "live-contract", epoch: 1, build: "test" });
  const connection = raw(server);
  try {
    connection.deliver(connection.statement(1, "SELECT 1", [], { t: "subscribe" }));
    const reply = await connection.reply(1);
    expect(reply.t, "a native SELECT subscription must deliver its initial result").toBe("done");
    if (reply.t === "done")
      expect(reply.frames.reduce((rows, frame) => rows + frame.rowCount, 0)).toBe(1);
  } finally {
    const closing = server.quiesce("closing");
    await closing.drained;
    closing.finish();
    await engine.close();
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});
