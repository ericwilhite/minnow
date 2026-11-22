/**
 * Copying a wide value within a row, and reading the table back, through the public owner:
 * never a hang, and never an untyped error.
 *
 * Law (GOVERNOR-WAIT-POLICY point 5, next/memory/README.md): a statement is served, or refused with
 * `MemoryAdmissionError` (SQLSTATE 53200) before any effect when it could never fit beside what
 * it and its writer keep; a finite holder's release is waited for (lifetime-holdings law: an
 * idle engine serves its whole usable capacity). PostgreSQL statement atomicity: a refused
 * statement writes nothing.
 *
 * Two defects found by C1 on main (ed8ad390), pinned here:
 * - `UPDATE t SET b = a WHERE id = 2` with a 2 or 3 MiB `a` at 32 MiB waited forever for its fixed
 *   buffers beside the writer's floors, which stay while the statement holds the writer turn.
 * - After that copy and some reads, a request of exactly the settled usable capacity waited
 *   forever: the idle spill's publication covered the writer's FTS mutation entries only after the
 *   request had asked the metadata reclaimer, and an admission never asked a reclaimer twice, so
 *   the covered entries (1 KiB) were never dropped. The writer's wake now makes queued admissions
 *   ask again.
 */
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

const MiB = 1 << 20;

afterEach(() => {
  Governor.stallAfterMs = null;
});

async function open(capMiB: number) {
  const owner = await publicOwner({ capBytes: capMiB * MiB });
  const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  return { owner, db, governor: owner.core.harness.governor };
}

/** C1's history: a small row, a wide `a`, the copy into `b`, three reads. */
async function history(db: MinnowDatabaseClient, width: number): Promise<unknown> {
  await db.execute("CREATE TABLE t(id INT PRIMARY KEY, a TEXT, b TEXT)");
  await db.execute("INSERT INTO t VALUES (1, 'x', NULL)");
  await insertWide(db, width);
  const copy: unknown = await db.execute("UPDATE t SET b = a WHERE id = 2").then(
    () => null,
    (error: unknown) => error,
  );
  for (const sql of [
    "SELECT a, b FROM t WHERE id = 2",
    "SELECT a FROM t WHERE id = 1",
    "SELECT a FROM t",
  ])
    await db.query(sql).catch((error: unknown) => {
      expect(error, sql).toMatchObject({ sqlState: "53200" });
    });
  return copy;
}

async function insertWide(db: MinnowDatabaseClient, width: number): Promise<void> {
  await db.execute("INSERT INTO t VALUES (2, $1, NULL)", ["a".repeat(width)]);
}

/**
 * The 3 MiB row at 32 MiB is refused before it writes (typed 53200), until statement-sizing PR 2.
 * Its spool takes one step of twice the encoded row (`wideRowStepBytes`, 6.36 MB), and a statement
 * is never refused part way by that step, so its result progress lane reserves it up front: fixed
 * buffers 10.86 MB (6.32 MB before). Beside the idle writer's commit buffers (6.29 MB held, 0.13 MB
 * used) and merge credit (4.73 MB, unused), the memtable's charge (6.30 MB, twice the row) then
 * finds 0.95 MB. Before, this row was written only because the spool's step grew from free memory
 * at the time; with another holder it was refused part way (statement-wait-release.test.ts).
 * PR 2 (the memtable charging what a step allocates, the writer's idle credit) flips this.
 */
it("refuses the 3 MiB row at 32 MiB before it writes, typed, until PR 2", async () => {
  Governor.stallAfterMs = 5_000;
  const { owner, db } = await open(32);
  try {
    await db.execute("CREATE TABLE t(id INT PRIMARY KEY, a TEXT, b TEXT)");
    await db.execute("INSERT INTO t VALUES (1, 'x', NULL)");
    const refused: unknown = await insertWide(db, 3 * MiB).then(
      () => null,
      (error: unknown) => error,
    );
    expect(
      refused,
      "refused at the memtable (6.30 MB asked, 0.95 MB available beside the lane's 6.36 MB step); PR 2 writes it",
    ).toMatchObject({
      code: "MINNOW_MEMORY_ADMISSION",
      sqlState: "53200",
      label: "write path memtable",
    });
    // Nothing written, and the engine stays usable.
    await db.execute("INSERT INTO t VALUES (3, 'y', 'z')");
    const ids = await db.query("SELECT id FROM t ORDER BY id");
    expect(ids.rows().map((row) => Number(row.id))).toEqual([1, 3]);
  } finally {
    await db.close();
    await owner.close();
  }
}, 60_000);

for (const [cap, width] of [
  [32, 2 * MiB],
  [64, 6 * MiB],
] as const)
  it(`copies a ${String(width / MiB)} MiB value at ${String(cap)} MiB, or refuses it typed, never waiting forever`, async () => {
    Governor.stallAfterMs = 5_000;
    const { owner, db } = await open(cap);
    try {
      const copy = await history(db, width);
      if (copy !== null) {
        expect(copy).toMatchObject({ code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" });
        // Nothing written: `b` is still NULL.
        const rows = await db.query("SELECT id FROM t WHERE b IS NULL ORDER BY id");
        expect(rows.rows().map((row) => Number(row.id))).toEqual([1, 2]);
      } else {
        const rows = await db.query("SELECT id FROM t WHERE b IS NOT NULL");
        expect(rows.rows().map((row) => Number(row.id))).toEqual([2]);
      }
      // The engine stays usable.
      await db.execute("INSERT INTO t VALUES (3, 'y', 'z')");
      const ids = await db.query("SELECT id FROM t ORDER BY id");
      expect(ids.rows().map((row) => Number(row.id))).toEqual([1, 2, 3]);
    } finally {
      await db.close();
      await owner.close();
    }
  }, 60_000);

it("serves the settled usable capacity to the first request after the copy and reads", async () => {
  // The engine is deterministic: one run measures the usable capacity once the idle writer has
  // published and reclaimed; a second, identical run asks for exactly that, first.
  const settled = await (async () => {
    const { owner, db, governor } = await open(64);
    try {
      await history(db, 3 * MiB);
      // Requests of the usable capacity make the idle writer spill, publish and reclaim; each
      // may be refused when the publication grows the lifetime holdings.
      for (let round = 0; round < 5 && governor.usedBytes !== governor.permanentBytes; round++) {
        const cancel = new AbortController();
        const settle = governor
          .admitGroup({
            label: "settle",
            includesProgress: true,
            signal: cancel.signal,
            grants: [
              { label: "settle", minimumBytes: governor.usableCapacity(true), spillable: false },
            ],
          })
          .then(
            (grants) => {
              for (const grant of grants) grant.close();
            },
            () => undefined,
          );
        await owner.core.harness.path.quiesce();
        await new Promise((resolve) => setTimeout(resolve, 100));
        cancel.abort(new Error("settled"));
        await settle;
      }
      expect(governor.usedBytes, "an idle writer holds only its lifetime holdings").toBe(
        governor.permanentBytes,
      );
      return governor.usableCapacity(true);
    } finally {
      await db.close();
      await owner.close();
    }
  })();
  Governor.stallAfterMs = 5_000;
  const { owner, db, governor } = await open(64);
  try {
    await history(db, 3 * MiB);
    const outcome = await governor
      .admitGroup({
        label: "read",
        includesProgress: true,
        holdings: () => ({ grants: [] }),
        grants: [{ label: "read", minimumBytes: settled, spillable: false }],
      })
      .then(
        (grants) => {
          for (const grant of grants) grant.close();
          return "served";
        },
        (error: unknown) => error,
      );
    expect(outcome).toBe("served");
  } finally {
    await db.close();
    await owner.close();
  }
}, 60_000);
