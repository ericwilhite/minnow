/**
 * The leader dies while holding follower mutations at each stage.
 *
 *  A. Before the WAL append (admitted, held at the gate): every follower's write runs once on
 *     the next leader, "ok", no uncertain outcomes, no duplicates.
 *  B. After the WAL append but before the served-result frame: documented uncertain; the
 *     mutation must not apply twice.
 *  C. After both frames but before the reply: answered from the log (handover.test.ts).
 *
 * The same requester becoming the leader (self-election) at stage A runs its own request once
 * from the local path, and at stage B reports uncertain without re-running it.
 */
import { expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsUncertainOutcomeError } from "../types.js";
import { opener, outcome, sleep, table, waitFor } from "./coordination-helpers.js";

it("runs five followers' writes held at the admission gate exactly once each after the leader crashes", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "stage-a");
  const leader = await open();
  const followers = await Promise.all([open(), open(), open(), open(), open()]);
  await leader.addTable(table("seed"));
  for (const follower of followers) await follower.getCurrentManifestVersion();
  const release = leader._holdServedMutationsForTests();
  const writes = followers.map((follower, index) =>
    outcome(follower.addTable(table(`held${String(index)}`))),
  );
  await waitFor(
    () => leader._residentStateForTests().inFlightMutations === 5,
    "all five writes to be admitted",
  );
  leader._crashForTests();
  release();
  const results = await Promise.all(writes);
  expect(results).toEqual(["ok", "ok", "ok", "ok", "ok"]);
  const survivor = followers.find((follower) => follower._isLeaderForTests());
  if (survivor === undefined) throw new Error("No follower took over");
  const names = (await survivor.listTables()).map((record) => record.name).sort();
  expect(names).toEqual(["held0", "held1", "held2", "held3", "held4", "seed"]);
  for (const follower of followers) follower.close();
});

it("reports a crash between the mutation frame and the served-result frame as uncertain, never double-applied", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "stage-b");
  const leader = await open();
  const follower = await open();
  const successor = await open();
  await leader.addTable(table("counter"));
  await follower.getCurrentManifestVersion();
  await successor.getCurrentManifestVersion();
  let armed = true;
  shim.setWriteFault((path, phase) => {
    if (armed && phase === "flush" && path.endsWith("/wal")) {
      armed = false;
      // The frame's bytes are in the file; the tab dies before the served-result frame.
      queueMicrotask(() => {
        leader._crashForTests();
      });
    }
  });
  const pending = outcome(follower.reserveRowIds("table-counter", 5));
  await waitFor(() => !armed, "the frame to be written");
  shim.setWriteFault(null);
  // The successor's own operation elects it; the follower's re-send is answered from its log.
  const successorReservation = await successor.reserveRowIds("table-counter", 1);
  expect(await pending).toBe(OpfsUncertainOutcomeError.name);
  // The follower's reservation of 5 happened exactly once: the successor's is 6..7.
  expect(successorReservation).toEqual({ start: 6n, endExclusive: 7n });
  follower.close();
  successor.close();
});

it("a requester that elects itself reports its own durable but unanswered frame as uncertain once", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "stage-b-self");
  const leader = await open();
  const follower = await open();
  await leader.addTable(table("counter"));
  await follower.getCurrentManifestVersion();
  let armed = true;
  shim.setWriteFault((path, phase) => {
    if (armed && phase === "flush" && path.endsWith("/wal")) {
      armed = false;
      queueMicrotask(() => {
        leader._crashForTests();
      });
    }
  });
  const pending = outcome(follower.reserveRowIds("table-counter", 5));
  await waitFor(() => !armed, "the frame to be written");
  shim.setWriteFault(null);
  const result = await pending;
  await sleep(10);
  const next = await follower.reserveRowIds("table-counter", 1);
  expect(result).toBe(OpfsUncertainOutcomeError.name);
  expect(next).toEqual({ start: 6n, endExclusive: 7n });
  follower.close();
});

it("a requester that elects itself runs its own write once when the leader died before appending it", async () => {
  const shim = new MemoryOpfs();
  const open = opener(shim, "stage-a-self");
  const leader = await open();
  const follower = await open();
  await leader.addTable(table("counter"));
  await follower.getCurrentManifestVersion();
  const release = leader._holdServedMutationsForTests();
  const pending = outcome(follower.reserveRowIds("table-counter", 5));
  await waitFor(
    () => leader._residentStateForTests().inFlightMutations === 1,
    "the write to be admitted",
  );
  leader._crashForTests();
  release();
  const result = await pending;
  const next = await follower.reserveRowIds("table-counter", 1);
  expect(result).toBe("ok");
  expect(next).toEqual({ start: 6n, endExclusive: 7n });
  follower.close();
});
