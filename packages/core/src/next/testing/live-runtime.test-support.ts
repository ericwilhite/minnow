/** Real native owner/client fixture shared by the live runtime history campaigns. */
import { createLowerer } from "../lower/create.js";
import { chunkReads } from "../exec/scan.js";
import { composeEngine } from "../owner/compose.js";
import { ProtocolClient } from "../owner/client.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Governor } from "../memory/governor.js";
import { Harness } from "../write/test-support.js";

/** Native notification dispatch is a task after commit bookkeeping. Cross that boundary before
 * interpreting a momentarily idle runtime as completion of the just-committed invalidation. */
export const publicationTurn = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

export async function until(check: () => boolean): Promise<void> {
  const deadline = Date.now() + 10000;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("Live result did not settle");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}
export async function setup(
  cap?: number,
  configure?: (engine: ReturnType<typeof composeEngine>, governor: Governor) => void,
) {
  const h = new Harness("strict", "memory");
  if (cap !== undefined) Object.defineProperty(h, "governor", { value: new Governor(cap) });
  await h.open();
  const spill = new MemorySpillService();
  let engine = composeEngine({
    planner: createLowerer(),
    writePath: h.path,
    governor: h.governor,
    spill,
    reads: chunkReads,
  });
  configure?.(engine, h.governor);
  let server = new OwnerServer(engine, { ownerId: "live-1", epoch: 1, build: "test" });
  const client = new ProtocolClient();
  const ports: MessagePort[] = [];
  async function connect(reconnect = false) {
    const { port1, port2 } = new MessageChannel();
    ports.push(port1, port2);
    server.accept(port1);
    if (reconnect) await client.reconnect(port2);
    else await client.connect(port2);
  }
  await connect();
  return {
    h,
    client,
    get server() {
      return server;
    },
    get engine() {
      return engine;
    },
    reconnect: () => connect(true),
    async changeOwner() {
      const stop = server.quiesce("closing");
      await stop.drained;
      stop.finish();
      await engine.close();
      await h.close();
      await h.open();
      engine = composeEngine({
        planner: createLowerer(),
        writePath: h.path,
        governor: h.governor,
        spill,
        reads: chunkReads,
      });
      configure?.(engine, h.governor);
      server = new OwnerServer(engine, { ownerId: "live-2", epoch: 2, build: "test" });
      await connect(true);
    },
    async close() {
      client.close();
      const stop = server.quiesce("closing");
      await stop.drained;
      stop.finish();
      for (const port of ports) port.close();
      await engine.close();
      await h.close();
    },
  };
}
