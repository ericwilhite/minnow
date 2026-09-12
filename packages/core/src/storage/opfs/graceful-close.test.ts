/**
 * A leader closed gracefully while it is executing a follower's mutation.
 *
 * The documented contract (opfs.mdx, "One leader, held handles"): a leader that hands over or
 * closes says goodbye first, and answers or declines everything it still holds. So the
 * follower's write must resolve "ok" (it was durably appended), never uncertain.
 *
 * The shim's WAL flush fault hook runs synchronously inside the leader's WAL append and calls
 * `close()` on the leader store from there: the frame is written and flushed, the ledger has the
 * identity, and the served-result frame has not been appended yet. That is exactly the instant a
 * `dispose` could land.
 */
import { expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { opener, outcomeMessage, table, waitFor } from "./coordination-helpers.js";

it("answers a served mutation that was mid-flight when the leader closed gracefully", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "graceful-close");
  const leader = await open();
  const follower = await open();
  try {
    await leader.addTable(table("seed"));
    await follower.getCurrentManifestVersion();
    let armed = true;
    shim.setWriteFault((path, phase) => {
      if (armed && phase === "flush" && path.endsWith("/wal")) {
        armed = false;
        // The client disposes the leader's worker at exactly this instant.
        leader.close();
      }
    });
    const result = await outcomeMessage(follower.addTable(table("during-close")));
    shim.setWriteFault(null);
    expect(result).toBe("ok");
    // The frame was durable: the table exists for whoever leads next.
    await waitFor(async () => {
      const names = (await follower.listTables()).map((record) => record.name);
      return names.includes("during-close");
    }, "the durably appended table to be visible");
    const names = (await follower.listTables()).map((record) => record.name).sort();
    expect(names).toEqual(["during-close", "seed"]);
  } finally {
    shim.setWriteFault(null);
    follower.close();
    leader.close();
  }
});
