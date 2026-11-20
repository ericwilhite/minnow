/** One write engine per governor: the precondition for counting memtable rows, rather than
 * spilling them, in write growth's complete minimum (owner-claim.ts, memory/README.md). */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import { Governor, type MemoryGovernor } from "../memory/governor.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import { yieldTask } from "../task.js";
import { WriteCredits } from "./credits.js";
import { WritePathImpl } from "./engine.js";
import { claimWriteOwner, WriteOwnerConflictError } from "./owner-claim.js";
import { Harness, autocommit, column, INT, tableInput } from "./test-support.js";

const assertClaimed = (governor: MemoryGovernor) =>
  expect(() => claimWriteOwner(governor)).toThrow(WriteOwnerConflictError);
const assertFree = (governor: MemoryGovernor) => claimWriteOwner(governor).release();

it("refuses a second write engine on a governor and frees it on close and crash", async () => {
  const h = new Harness("strict", "sim");
  const path = await h.open();
  try {
    assertClaimed(h.governor);
    // The engine's own open path refuses before it touches storage or admits credit.
    const used = h.governor.usedBytes;
    await expect(
      WritePathImpl.open({
        session: undefined as never,
        space: h.space,
        governor: h.governor,
        options: { durability: "strict" },
      }),
    ).rejects.toBeInstanceOf(WriteOwnerConflictError);
    expect(h.governor.usedBytes).toBe(used);
    await autocommit(path, (s) =>
      s.changeCatalog({
        kind: "createTable",
        table: tableInput("owner_claim", [column("id", INT, false)], { primaryKey: [0] }),
      }),
    );
    // A clean reopen and a simulated crash each leave exactly one claimed engine.
    await h.reopen();
    assertClaimed(h.governor);
    await h.crash();
    assertClaimed(h.governor);
  } finally {
    await h.close();
  }
  assertFree(h.governor);
  // Another governor is independent.
  const other = new Harness("strict", "sim");
  await other.open();
  try {
    assertClaimed(other.governor);
  } finally {
    await other.close();
  }
});

/** Storage for a direct engine open, outside Harness (which serializes opens on its lock). */
async function storage(governor: Governor) {
  const backend = new MemoryBackend();
  const lease = await backend.acquire(),
    session = await backend.open(lease);
  const startup = await governor.admit({
    label: "claim startup",
    minimumBytes: 8 << 20,
    spillable: true,
  });
  const space = await openExtentSpace(session, {
    reachable: listedRootExtents(),
    walk: reachableExtents(),
    budget: startup,
    registerCache: (cache) => governor.registerCache(cache),
  });
  return {
    input: { session, space, governor, options: { durability: "strict" as const } },
    async close() {
      try {
        await space.close();
      } finally {
        startup.close();
        await session.close();
        await lease.release();
      }
    },
  };
}

for (const seed of seedsFor("next-write-owner-claim-concurrent", [47309, 47310]))
  it(`admits exactly one of two overlapping engine opens (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: 6 }), async (turns) => {
        const governor = new Governor(64 << 20);
        const store = await storage(governor);
        let path: WritePathImpl | undefined;
        const admissions = vi.spyOn(WriteCredits, "open");
        try {
          const first = WritePathImpl.open(store.input);
          for (let turn = 0; turn < turns; turn++) await yieldTask();
          // The second open overlaps the first at any point of its asynchronous open.
          await expect(WritePathImpl.open(store.input)).rejects.toBeInstanceOf(
            WriteOwnerConflictError,
          );
          path = await first;
          assertClaimed(governor);
          // The refused open never reached credit admission or storage.
          expect(admissions).toHaveBeenCalledTimes(1);
          await path.close();
          path = undefined;
          assertFree(governor);
          // The governor serves a new engine after the first closes.
          path = await WritePathImpl.open(store.input);
          assertClaimed(governor);
        } finally {
          admissions.mockRestore();
          await path?.close();
          await store.close();
        }
        assertFree(governor);
      }),
      { seed, numRuns: 7, examples: [[0], [1], [6]], endOnFailure: true },
    );
  });

it("frees the governor after a failed close, so recovery can reopen", async () => {
  const governor = new Governor(64 << 20);
  const failed = await storage(governor);
  const path = await WritePathImpl.open(failed.input);
  const failure = new Error("close failed before its accounts were returned");
  const settled = vi.spyOn(path.sequences, "settled").mockRejectedValueOnce(failure);
  await expect(path.close()).rejects.toBe(failure);
  settled.mockRestore();
  // owner/native.ts closes the storage and reopens a fresh write path on the same governor.
  await failed.close().catch(() => undefined);
  const fresh = await storage(governor);
  try {
    const recovered = await WritePathImpl.open(fresh.input);
    assertClaimed(governor);
    await recovered.close();
  } finally {
    await fresh.close();
  }
  assertFree(governor);
});
