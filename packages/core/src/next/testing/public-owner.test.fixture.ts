/** Real native write path and executor behind the owner protocol; no stand-in engine. */
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";

export async function publicOwner() {
  const core = await new StorageCore().open();
  const engine = composeEngine({
    planner: core.lowerer,
    writePath: core.harness.path,
    governor: core.harness.governor,
    spill: core.spill,
    reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
  });
  const server = new OwnerServer(engine, { ownerId: "public-owner", epoch: 1, build: "test" });
  const channel = new MessageChannel();
  const client = new ProtocolClient();
  server.accept(channel.port2);
  let closing: Promise<void> | undefined;
  const close = () =>
    (closing ??= (async () => {
      const drain = server.quiesce("closing");
      try {
        await drain.drained;
        client.close();
        await engine.close();
      } finally {
        await core.close();
        drain.finish();
        channel.port1.close();
        channel.port2.close();
      }
    })());
  try {
    await client.connect(channel.port1);
  } catch (error) {
    await close();
    throw error;
  }
  return { core, engine, server, client, channel, close };
}
