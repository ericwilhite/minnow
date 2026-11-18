/** Uses only the pre-B8 owner API, so this semantic RED runs on the parent unchanged. */
import { expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createLowerer } from "../lower/create.js";
import { chunkReads } from "../exec/scan.js";
import { composeEngine } from "../owner/compose.js";
import { ProtocolClient } from "../owner/client.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Harness } from "../write/test-support.js";

const publicationTurn = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

for (const seed of seedsFor("next-live-commit-oracle", [8804, 8805])) {
  it(`every explicit native commit reports surviving tables seed=${String(seed)}`, async () => {
    const h = new Harness("strict", "memory");
    const path = await h.open();
    const engine = composeEngine({
      planner: createLowerer(),
      writePath: path,
      governor: h.governor,
      spill: new MemorySpillService(),
      reads: chunkReads,
    });
    const server = new OwnerServer(engine, { ownerId: "commit-oracle", epoch: 1, build: "test" });
    const { port1, port2 } = new MessageChannel();
    server.accept(port1);
    const client = new ProtocolClient();
    let stop: () => void = () => undefined;
    try {
      await client.connect(port2);
      await client.query("CREATE TABLE a (id INTEGER)");
      await client.query("CREATE TABLE b (id INTEGER)");
      await publicationTurn();
      const observed: number[][] = [];
      stop = engine.onCommit((tables) => {
        observed.push([...tables].sort((a, b) => a - b));
      });
      const random = mulberry32(seed);
      for (let step = 0; step < 12; step++) {
        const table = random() < 0.5 ? "a" : "b",
          other = table === "a" ? "b" : "a";
        const id = path.catalog.tables.find((t) => t.name === table)?.id;
        if (id === undefined) throw new Error("Missing table");
        await client.query("BEGIN");
        await client.query(`INSERT INTO ${table} VALUES (${String(step)})`);
        await client.query("SAVEPOINT kept");
        await client.query(`INSERT INTO ${other} VALUES (${String(step)})`);
        await client.query("ROLLBACK TO kept");
        const before = observed.length;
        if (step % 4 === 0) {
          await client.query("ROLLBACK");
          expect(observed).toHaveLength(before);
        } else {
          await client.query("COMMIT");
          await publicationTurn();
          expect(
            observed.slice(before),
            "one committed event, with rollback survivors only",
          ).toEqual([[id]]);
        }
      }
    } finally {
      stop();
      client.close();
      const closing = server.quiesce("closing");
      await closing.drained;
      closing.finish();
      port1.close();
      port2.close();
      await engine.close();
      await h.close();
    }
    expect(h.governor.usedBytes).toBe(0);
  }, 30000);
}
