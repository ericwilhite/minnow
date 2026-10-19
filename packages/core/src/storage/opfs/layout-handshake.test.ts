/**
 * Layout skew between tabs during a rolling deployment.
 *
 * A connection validates the format marker when it opens; a newer build in another tab can
 * advance it afterwards. Such a connection must (1) be refused with the version error, never a
 * corruption report, when it later elects itself; (2) learn from the leader's announcement that
 * a newer build leads and close itself rather than read records it cannot interpret; and a
 * leader must (3) decline operations from a tab on an older layout. The field that carries the
 * layout is optional on the wire: a 0.17.0 tab sends none and reads as layout 13.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { StorageConnectionClosedError, StorageFormatVersionError } from "../types.js";
import { OpfsTree } from "./files.js";
import { parseStoreRpcMessage, type StoreRpcMessage } from "./rpc.js";
import { OPFS_LAYOUT_VERSION, upgradeOpfsLayout } from "./upgrades.js";
import { opener, outcomeMessage, sleep, table, waitFor } from "./coordination-helpers.js";

const NEWER = OPFS_LAYOUT_VERSION + 1;
const marker = (version: number | string) =>
  new TextEncoder().encode(
    typeof version === "string" ? version : JSON.stringify({ formatVersion: version }),
  );

describe("a connection that elects itself after a newer build advanced the marker", () => {
  it("is refused with StorageFormatVersionError, not a corruption report", async () => {
    const shim = new MemoryOpfs();
    const open = opener(shim, "newer-marker");
    const a = await open();
    const b = await open();
    try {
      await a.addTable(table("seed"));
      await b.getCurrentManifestVersion();
      expect(b._isLeaderForTests()).toBe(false);
      shim.writeFileBytes("minnowdb/newer-marker/format.json", marker(NEWER));
      a.close();
      await waitFor(() => !a._isLeaderForTests(), "a to release");
      let error: unknown;
      try {
        await b.addTable(table("after"));
      } catch (caught) {
        error = caught;
      }
      expect(error).toBeInstanceOf(StorageFormatVersionError);
      expect(error).toMatchObject({ actualVersion: NEWER, relation: "newer" });
      // Nothing was written: the marker is as the newer build left it.
      expect(
        new TextDecoder().decode(shim.readFileBytes("minnowdb/newer-marker/format.json")),
      ).toBe(JSON.stringify({ formatVersion: NEWER }));
    } finally {
      b.close();
    }
  });

  it("the upgrade step tells a whole newer marker from a torn one", async () => {
    const shim = new MemoryOpfs();
    const minnow = await shim.root.getDirectoryHandle("minnowdb", { create: true });
    const tree = new OpfsTree(await minnow.getDirectoryHandle("judge", { create: true }));
    const attempt = async (bytes: Uint8Array): Promise<unknown> => {
      await tree.writeFile(["format.json"], bytes, { flush: true });
      const wal = await tree.openHandle(["wal"], { create: true });
      const slotA = await tree.openHandle(["checkpoint-a"], { create: true });
      const slotB = await tree.openHandle(["checkpoint-b"], { create: true });
      try {
        await upgradeOpfsLayout(tree, { wal, slotA, slotB });
        return undefined;
      } catch (error) {
        return error;
      } finally {
        wal.close();
        slotA.close();
        slotB.close();
      }
    };
    expect(await attempt(marker(NEWER))).toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: NEWER,
      relation: "newer",
    });
    expect(await attempt(marker(3))).toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: 3,
      relation: "older",
    });
    expect(await attempt(marker('{"formatVersion":1'))).toMatchObject({
      name: "StorageCorruptionError",
      message: expect.stringContaining("Missing upgrade witness for a torn marker") as string,
    });
    expect(await attempt(marker(OPFS_LAYOUT_VERSION))).toBeUndefined();
  });
});

describe("layout on the wire", () => {
  const methods = new Set(["listTables"]);
  const op = (layout?: number): Record<string, unknown> => ({
    kind: "op",
    requestId: "r1",
    from: "tab",
    method: "listTables",
    args: [],
    sentAt: 1,
    ...(layout === undefined ? {} : { layout }),
  });

  it("is optional on `leader` and `op` messages and must be a positive integer when present", () => {
    expect(parseStoreRpcMessage({ kind: "leader", leaderId: "x" }, methods)).toBeDefined();
    expect(
      parseStoreRpcMessage({ kind: "leader", leaderId: "x", layout: 13 }, methods),
    ).toBeDefined();
    expect(
      parseStoreRpcMessage({ kind: "leader", leaderId: "x", layout: 0 }, methods),
    ).toBeUndefined();
    expect(
      parseStoreRpcMessage({ kind: "leader", leaderId: "x", layout: "13" }, methods),
    ).toBeUndefined();
    expect(parseStoreRpcMessage(op(), methods)).toBeDefined();
    expect(parseStoreRpcMessage(op(13), methods)).toBeDefined();
    expect(parseStoreRpcMessage(op(1.5), methods)).toBeUndefined();
    expect(
      parseStoreRpcMessage({ ...op(13), args: [{}], encoded: { pieces: 1, bytes: 8 } }, methods),
    ).toBeDefined();
  });

  it("a follower that hears a newer leader closes itself; calls it had posted are uncertain", async () => {
    const shim = new MemoryOpfs();
    const open = opener(shim, "superseded");
    const a = await open();
    const b = await open();
    const channel = new BroadcastChannel("minnowdb-store:superseded");
    try {
      await a.addTable(table("seed"));
      await b.getCurrentManifestVersion();
      // A write b has posted and the leader is holding when the newer leader announces.
      const release = a._holdServedMutationsForTests();
      const held = outcomeMessage(b.addTable(table("held")));
      await waitFor(
        () => a._residentStateForTests().inFlightMutations === 1,
        "b's write to arrive",
      );
      // The leader itself ignores the announcement here: in a browser the file lock makes two
      // leaders impossible, and this test is about the follower.
      const resume = a._pauseCoordinationForTests();
      channel.postMessage({ kind: "leader", leaderId: "future-build", layout: NEWER });
      await waitFor(() => b._residentStateForTests().closed, "b to close itself");
      expect(await held).toBe(
        "OpfsUncertainOutcomeError: The OPFS operation addTable could not be acknowledged; the mutation may have committed",
      );
      let error: unknown;
      try {
        await b.listTables();
      } catch (caught) {
        error = caught;
      }
      expect(error).toBeInstanceOf(StorageConnectionClosedError);
      expect(error).toMatchObject({
        reason: "superseded",
        backend: "opfs",
        databaseName: "superseded",
      });
      resume();
      release();
    } finally {
      channel.close();
      a.close();
      b.close();
    }
  });

  it("a leader declines an operation from an older layout and serves one without the field", async () => {
    const shim = new MemoryOpfs();
    const open = opener(shim, "older-op");
    const a = await open();
    const inbox = new BroadcastChannel(`minnowdb-store:older-op:${a._instanceIdForTests()}`);
    const replies = new BroadcastChannel("minnowdb-store:older-op:old-tab");
    const received: StoreRpcMessage[] = [];
    replies.onmessage = (event: MessageEvent<StoreRpcMessage>) => {
      received.push(event.data);
    };
    try {
      await a.addTable(table("seed"));
      const send = (requestId: string, layout?: number) => {
        inbox.postMessage({
          kind: "op",
          requestId,
          from: "old-tab",
          method: "listTables",
          args: [],
          sentAt: Date.now(),
          ...(layout === undefined ? {} : { layout }),
        });
      };
      send("older", OPFS_LAYOUT_VERSION - 1);
      send("unversioned");
      send("current", OPFS_LAYOUT_VERSION);
      await waitFor(() => received.length === 3, "three answers");
      await sleep(10);
      const byId = new Map(
        received.map((message) => [(message as { requestId: string }).requestId, message]),
      );
      expect(byId.get("older")).toMatchObject({
        kind: "result",
        ok: false,
        error: { name: "StorageConnectionClosedError" },
      });
      expect(byId.get("unversioned")).toMatchObject({ kind: "result", ok: true });
      expect(byId.get("current")).toMatchObject({ kind: "result", ok: true });
    } finally {
      inbox.close();
      replies.close();
      a.close();
    }
  });
});
