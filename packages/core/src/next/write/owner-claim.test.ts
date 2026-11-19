/** One write engine per governor: the precondition for counting memtable rows, rather than
 * spilling them, in write growth's complete minimum (owner-claim.ts, memory/README.md). */
import { expect, it } from "vitest";
import type { MemoryGovernor } from "../memory/governor.js";
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
