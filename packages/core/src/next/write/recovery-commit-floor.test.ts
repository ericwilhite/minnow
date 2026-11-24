/**
 * An open whose growth waits for a temporary holder completes; it never fails because the wait
 * gave away the commit window the log appender is built from.
 *
 * Law: the governor contract (memory/governor.ts rules 2/4/5/7, PROPOSAL "One memory governor"):
 * below the cap, work completes, waits (cancellably) for temporary holdings, or is refused with a
 * typed 53200, never an untyped internal failure. While the write path recovers, its working
 * minimum dropped the commit window to 0, so a growth that waited during the open (its metadata
 * headroom, `#prepareMinimum`) first released the window's idle credit (write/credits.ts
 * `#growTo`: "Dormant row, WAL, metadata and merge forecasts are all revocable"). Recovery then
 * built the log appender, whose header buffer comes from that window, and the open failed with
 * "Bounded commit-buffer minimum was exhausted".
 *
 * The open is made to need 12 MiB of metadata headroom (as a database with a large resident
 * directory does) by asking `prepareMetadata` for it, while a temporary reader holds all other
 * room. The reader is released only once the growth is queued behind it, so the test proves the
 * open waited, with no timer.
 */
import { expect, it, vi } from "vitest";
import type { Grant } from "../memory/governor.js";
import { WriteCredits } from "./credits.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
} from "./test-support.js";

const MiB = 1024 * 1024;
const GROWTH = "complete grow write path metadata";

it("completes an open whose metadata growth waits for a temporary holder", async () => {
  const harness = new Harness("relaxed", "memory", 2);
  let path = await harness.open();
  let table = 0;
  await autocommit(path, async (s) => {
    table =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("m", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  await autocommit(path, (s) => s.table(table).insert(batch(ints([1])), null));
  await harness.close();

  const governor = harness.governor;
  let reader: Grant | null = null;
  let waits = 0,
    blocked = 0;
  const admitGroup = governor.admitGroup.bind(governor);
  const queuedGrowth = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
    if (options.label !== GROWTH || reader === null) return admitGroup(options);
    const seen = { served: false };
    const stop = governor.observeGrants((event) => {
      if (event.label === GROWTH) seen.served = true;
    });
    const admitted = admitGroup(options);
    if (seen.served) {
      stop();
      return admitted;
    }
    // Queued: release the reader once the code that queued it has run, if it still waits.
    waits++;
    const settled = () => stop();
    admitted.then(settled, settled);
    queueMicrotask(() => {
      if (!seen.served) blocked++;
      reader?.close();
      reader = null;
    });
    return admitted;
  });
  // eslint-disable-next-line @typescript-eslint/unbound-method -- called with its receiver below
  const prepare = WriteCredits.prototype.prepareMetadata;
  let asked = false;
  const headroom = vi.spyOn(WriteCredits.prototype, "prepareMetadata").mockImplementation(function (
    this: WriteCredits,
    bytes,
    signal,
    includesProgress,
    holdings,
  ) {
    if (asked) return prepare.call(this, bytes, signal, includesProgress, holdings);
    asked = true;
    // A temporary reader holds every byte the open has not admitted.
    reader = governor.tryAdmit({
      label: "temporary reader",
      minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
      spillable: false,
    });
    expect(reader, "the reader took the remaining room").not.toBeNull();
    return prepare.call(this, 12 * MiB, signal, includesProgress, holdings);
  });
  try {
    path = await harness.open();
  } finally {
    headroom.mockRestore();
    queuedGrowth.mockRestore();
    (reader as Grant | null)?.close();
  }
  expect(asked).toBe(true);
  expect(waits, "the open's metadata growth queued").toBe(1);
  expect(blocked, "behind the temporary reader").toBe(1);
  // The opened path commits through the appender recovery built.
  await autocommit(path, (s) => s.table(table).insert(batch(ints([2])), null));
  const view = path.pin();
  try {
    expect(await rowsOf(view, table)).toEqual([[1], [2]]);
  } finally {
    view.release();
  }
  await harness.close();
  expect(governor.usedBytes).toBe(0);
});
