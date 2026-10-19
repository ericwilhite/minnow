/**
 * A leader whose held handles stop working. A refused append poisons it; before its next
 * operation it reloads from the same handles, and when that reload fails too there is nothing
 * it can ever serve from those handles — the shape of a handle gone stale in a browser. It must
 * release them, so the next election (another tab's, or its own with fresh handles) recovers
 * from the files, instead of holding the database hostage while every write fails.
 */
import { expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { opener, outcomeMessage, table, waitFor } from "./coordination-helpers.js";

it("a leader that cannot reload its log releases the handles, and the database recovers", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "recovery-failure");
  const a = await open();
  const b = await open();
  try {
    await a.addTable(table("seed"));
    await b.getCurrentManifestVersion();
    expect(a._isLeaderForTests()).toBe(true);
    const walPath = "minnowdb/recovery-failure/wal";
    // One refused append poisons the leader (the writer truncates the torn frame back).
    let refuseOnce = true;
    shim.setWriteFault((path, phase) => {
      if (refuseOnce && path === walPath && phase === "write") {
        refuseOnce = false;
        throw new DOMException("handle invalid", "InvalidStateError");
      }
    });
    expect(await outcomeMessage(a.addTable(table("poison")))).toBe(
      "InvalidStateError: handle invalid",
    );
    shim.setWriteFault(null);
    // From now on the log cannot be read: the poisoned leader's reload fails, and so does a
    // fresh election's recovery, so the local write fails with the recovery error — but the
    // handles are released on the way.
    shim.setTransferLimit((path, operation) =>
      path === walPath && operation === "read" ? 0 : undefined,
    );
    const failed = await outcomeMessage(a.addTable(table("while-broken")));
    expect(failed).toMatch(/StorageCorruptionError: .*Unexpected EOF/);
    expect(a._isLeaderForTests()).toBe(false);
    // The storage recovers: whoever next needs the database leads it from the files.
    shim.setTransferLimit(null);
    expect(await outcomeMessage(b.addTable(table("after")))).toBe("ok");
    expect(await outcomeMessage(a.addTable(table("again")))).toBe("ok");
    expect(a._isLeaderForTests() || b._isLeaderForTests()).toBe(true);
    await waitFor(async () => (await a.listTables()).length === 3, "the tables to be visible");
    const names = (await a.listTables()).map((record) => record.name).sort();
    expect(names).toEqual(["after", "again", "seed"]);
  } finally {
    shim.setWriteFault(null);
    shim.setTransferLimit(null);
    a.close();
    b.close();
  }
});
