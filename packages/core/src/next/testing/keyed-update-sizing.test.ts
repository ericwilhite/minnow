/**
 * A keyed UPDATE of a wide value is written or refused typed, never an untyped error.
 *
 * The lookup sizing pass (exec/index-access.ts `readLookup`) bound every statement parameter on
 * its scratch, so a wide value for another column (`UPDATE t SET v = $1 WHERE id = 1`, 2.5 MB)
 * took the room the key's range plan needs, and `IndexRangePlan.open` threw the untyped
 * "Index range grant is smaller than the planned minimum" (write/range-plan.ts). It now binds
 * only the parameters its keys read, and sizes the whole source when a range still does not fit
 * its scratch (sizing is optional).
 *
 * Law (next/memory/README.md, GOVERNOR-WAIT-POLICY point 5): every memory refusal is the typed
 * 53200 (`MemoryAdmissionError`), and a refused statement changes nothing (PostgreSQL statement
 * atomicity).
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createReach, KEYED_UPDATE_SIZING_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
const TYPED = { name: "MemoryAdmissionError", code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" };

afterEach(() => {
  Governor.stallAfterMs = null;
});

/** Row `id`'s value length, or null when its read is refused (typed only). */
async function stored(core: StorageCore, id: number): Promise<number | null> {
  const outcome: unknown = await core.execute(`SELECT v FROM t WHERE id = ${String(id)}`).then(
    (rows) => rows,
    (error: unknown) => error,
  );
  if (typeof outcome === "object" && outcome !== null && "batches" in outcome)
    return String(oracleRows(outcome as Parameters<typeof oracleRows>[0])[0]?.v).length;
  expect(outcome).toMatchObject(TYPED);
  return null;
}

async function history(
  capMiB: number,
  before: number,
  width: number,
  keyByParameter: boolean,
  reach?: ReturnType<typeof createReach>,
): Promise<void> {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(capMiB * MiB) });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')");
    // The table holds wide values first (or is refused doing so, typed).
    const widened: unknown = await core.execute("UPDATE t SET v = $1", ["a".repeat(before)]).then(
      () => null,
      (error: unknown) => error,
    );
    if (widened !== null) expect(widened, "widening the table").toMatchObject(TYPED);
    const old = widened === null ? before : 1;
    const sql = keyByParameter
      ? "UPDATE t SET v = $1 WHERE id = $2"
      : "UPDATE t SET v = $1 WHERE id = 1";
    const outcome: unknown = await core
      .execute(sql, keyByParameter ? ["b".repeat(width), 1] : ["b".repeat(width)])
      .then(
        () => null,
        (error: unknown) => error,
      );
    const context = `${sql} with ${String(width)} bytes over ${String(old)}-byte values at ${String(capMiB)} MiB`;
    if (outcome === null) reach?.reach("written");
    else {
      expect(outcome, context).toMatchObject(TYPED);
      reach?.reach("refused typed");
    }
    // Written exactly, or nothing changed; the other row is untouched.
    for (const [id, length] of [
      [1, outcome === null ? width : old],
      [2, old],
    ] as const) {
      const n = await stored(core, id);
      if (n !== null) expect(n, `${context}: row ${String(id)}`).toBe(length);
    }
    expect(core.harness.path.failure).toBeNull();
    if (old > MiB) reach?.reach("table holds wide values");
    if (width > 2 * MiB) reach?.reach("update over 2 MiB");
    reach?.reach(keyByParameter ? "key by parameter" : "key by literal");
  } finally {
    await core.close();
  }
}

it("writes a 2.5 MB keyed UPDATE over wide values at 64 MiB, or refuses it typed", async () => {
  // The reproduction: an untyped RangeError from the lookup sizing's range plan.
  await history(64, 2_500_000, 2_500_000, false);
}, 60_000);

for (const seed of seedsFor("next-keyed-update-sizing", [0x5e01, 0x5e02]))
  it(`keyed UPDATEs of wide values are written or refused typed (seed ${String(seed)})`, async () => {
    const reach = createReach("keyed update sizing", KEYED_UPDATE_SIZING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 64),
        fc.constantFrom(4096, MiB, 2_500_000, 4 * MiB),
        fc.constantFrom(4096, MiB, 2_500_000, 4 * MiB, 6 * MiB),
        fc.boolean(),
        async (capMiB, before, width, keyByParameter) => {
          reach.beginHistory();
          await history(capMiB, before, width, keyByParameter, reach);
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [64, 2_500_000, 2_500_000, false],
          [32, 2_500_000, 4 * MiB, true],
        ],
      },
    );
    reach.assertReached();
  }, 600_000);
