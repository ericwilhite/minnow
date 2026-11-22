/**
 * Wide inserts, and reads of what they wrote, at a 32 MiB cap never hang and never fail untyped.
 *
 * Law (GOVERNOR-WAIT-POLICY point 5 and next/memory/README.md): a request that could never be
 * served beside what its own statement and that statement's writer keep held while it waits is
 * refused with `MemoryAdmissionError` (code `MINNOW_MEMORY_ADMISSION`, SQLSTATE 53200,
 * PostgreSQL's out_of_memory), never left waiting; every memory refusal carries that type; and a
 * refused statement writes nothing (PostgreSQL statement atomicity).
 *
 * Three defects this pins, all on main before the fix:
 * - `INSERT INTO t SELECT 2, $1 || $1 || $1` with a 2 MiB parameter queued its fixed buffers
 *   (17.9 MB) forever: only 17.1 MB could ever be free beside the writer's floors, which stay
 *   held while the statement holds the writer turn.
 * - `INSERT INTO t VALUES (2, $1)` with a 3.25 MiB parameter failed with the write path's
 *   internal `MemtableMemoryError` (no SQLSTATE).
 * - After two 2 MiB rows, `SELECT id, v FROM t ORDER BY id` restarted its preparation and waited
 *   forever for a doubled credit of exactly the usable capacity, which its own result grant and
 *   the writer's live bytes kept out of reach.
 *
 * Every statement here either writes its row exactly or is refused with 53200 writing nothing,
 * and every read either returns exactly the written rows or is refused with 53200. A stall fails
 * the run as a `GovernorStallError` (no SQLSTATE) through `Governor.stallAfterMs`. Which sizes
 * fit is the fit model's question, not this file's.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createReach, WIDE_INSERT_ADMISSION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
const CAP = 32 * MiB;

interface Insert {
  readonly form: "values" | "select";
  readonly width: number;
  readonly copies: number;
}

const TYPED_REFUSAL = {
  name: "MemoryAdmissionError",
  code: "MINNOW_MEMORY_ADMISSION",
  sqlState: "53200",
};

afterEach(() => {
  Governor.stallAfterMs = null;
});

/** Runs `sql`; its rows, or null when it was refused (only ever typed). */
async function read(
  core: StorageCore,
  sql: string,
): Promise<Array<Record<string, unknown>> | null> {
  const outcome: unknown = await core.execute(sql).then(
    (rows) => rows,
    (reason: unknown) => reason,
  );
  if (typeof outcome === "object" && outcome !== null && "batches" in outcome)
    return oracleRows(outcome as Parameters<typeof oracleRows>[0]);
  expect(outcome, sql).toMatchObject(TYPED_REFUSAL);
  return null;
}

interface Outcome {
  readonly insert: Insert;
  readonly refused: { readonly label: string } | null;
  /** The written row read back by its key (and matched). */
  readonly pointRead: boolean;
  /** The whole table read back after this statement (and matched), or refused. */
  readonly tableRead: boolean;
  /** The widest row stored after this statement. */
  readonly widest: number;
}

/** Runs `inserts` in order on a fresh database; returns each outcome after checking the law. */
async function run(inserts: readonly Insert[]): Promise<Outcome[]> {
  Governor.stallAfterMs = 2_000;
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(CAP) });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO t VALUES (1, 'old')");
    const outcomes: Outcome[] = [];
    const written = new Map([[1, "old"]]);
    for (const [at, insert] of inserts.entries()) {
      const id = at + 2;
      const expression = Array.from({ length: insert.copies }, () => "$1").join(" || ");
      const sql =
        insert.form === "values"
          ? `INSERT INTO t VALUES (${String(id)}, ${expression})`
          : `INSERT INTO t SELECT ${String(id)}, ${expression}`;
      const parameter = "q".repeat(insert.width);
      const error: unknown = await core.execute(sql, [parameter]).then(
        () => null,
        (reason: unknown) => reason,
      );
      const context = `${sql} with a ${String(insert.width)}-character parameter`;
      if (error === null) written.set(id, parameter.repeat(insert.copies));
      else expect(error, `${context}: typed refusal`).toMatchObject(TYPED_REFUSAL);
      // A refused statement writes nothing; a written one adds exactly its row. Read narrowly:
      // a wide value never sizes this read.
      const ids = await read(core, "SELECT id FROM t ORDER BY id");
      expect(
        ids?.map((row) => Number(row.id)),
        `${context}: rows`,
      ).toEqual([...written.keys()]);
      let pointRead = false;
      if (error === null) {
        const [row] = (await read(core, `SELECT v FROM t WHERE id = ${String(id)}`)) ?? [];
        if (row !== undefined) {
          expect(row.v === written.get(id), `${context}: stored value`).toBe(true);
          pointRead = true;
        }
      }
      const table = await read(core, "SELECT id, v FROM t ORDER BY id");
      if (table !== null)
        expect(
          table.every((row) => row.v === written.get(Number(row.id))) &&
            table.length === written.size,
          `${context}: the table read back`,
        ).toBe(true);
      expect(core.harness.path.failure, context).toBeNull();
      outcomes.push({
        insert,
        refused: error === null ? null : { label: (error as { label: string }).label },
        pointRead,
        tableRead: table !== null,
        widest: Math.max(...[...written.values()].map((value) => value.length)),
      });
    }
    // The engine stays usable after every refusal. (A read of `v`, even of this row, may be
    // refused while a wider value is stored.)
    await core.execute("INSERT INTO t VALUES (100, 'after')");
    expect(
      (await read(core, "SELECT id FROM t ORDER BY id"))?.map((row) => Number(row.id)),
    ).toEqual([...written.keys(), 100]);
    expect(core.harness.path.failure).toBeNull();
    return outcomes;
  } finally {
    Governor.stallAfterMs = null;
    await core.close();
  }
}

it("refuses a statement whose fixed buffers could never fit beside its writer, without waiting", async () => {
  const [outcome] = await run([{ form: "select", width: 2 * MiB, copies: 3 }]);
  expect(outcome?.refused?.label).toMatch(/\/fixed-buffers$/);
}, 60_000);

it("refuses it before waiting while another statement holds memory too", async () => {
  // Another holder could release, so only the check beside the statement's own holdings and
  // its writer's floors can tell, while it is held, that this group never fits. It is released
  // after a second: a refusal must come before that.
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  const governor = new Governor(CAP);
  Object.defineProperty(core.harness, "governor", { value: governor });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO t VALUES (1, 'old')");
    const other = await governor.admit({
      label: "another statement",
      minimumBytes: MiB,
      spillable: false,
    });
    let released = false;
    const release = setTimeout(() => {
      released = true;
      other.close();
    }, 1_000);
    const error: unknown = await core
      .execute("INSERT INTO t SELECT 2, $1 || $1", ["q".repeat(4 * MiB)])
      .then(
        () => null,
        (reason: unknown) => reason,
      );
    const heldThroughout = !released;
    clearTimeout(release);
    other.close();
    expect(error).toMatchObject(TYPED_REFUSAL);
    expect((error as { label: string }).label).toMatch(/\/fixed-buffers$/);
    expect(heldThroughout, "refused while the other statement still held its memory").toBe(true);
    expect((await read(core, "SELECT id FROM t ORDER BY id"))?.map((row) => row.id)).toEqual([1]);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    Governor.stallAfterMs = null;
    await core.close();
  }
}, 60_000);

it("reports the write path's memtable refusal as the typed 53200", async () => {
  const [outcome] = await run([{ form: "values", width: 3_407_872, copies: 1 }]);
  expect(outcome?.refused?.label).toBe("write path memtable");
}, 60_000);

it("reads a table of two 2 MiB rows back, or refuses it typed, without waiting forever", async () => {
  const outcomes = await run([
    { form: "values", width: 2 * MiB, copies: 1 },
    { form: "values", width: 2 * MiB, copies: 1 },
  ]);
  expect(outcomes.map((outcome) => outcome.refused)).toEqual([null, null]);
}, 60_000);

const insert: fc.Arbitrary<Insert> = fc.oneof(
  // VALUES cells take literals and parameters only (no concatenation yet).
  fc.record({
    form: fc.constant("values" as const),
    width: fc.constantFrom(4096, 200_000, MiB, 2 * MiB, 3_407_872, 4 * MiB),
    copies: fc.constant(1),
  }),
  fc.record({
    form: fc.constant("select" as const),
    width: fc.constantFrom(4096, 200_000, 500_000, MiB, 1_572_864, 2 * MiB, 3_407_872),
    copies: fc.integer({ min: 1, max: 4 }),
  }),
);

for (const seed of seedsFor("next-wide-insert-admission", [0x5b01, 0x5b02]))
  it(`sequences of wide inserts write or refuse typed, never wait forever (seed ${String(seed)})`, async () => {
    const reach = createReach("wide insert admission", WIDE_INSERT_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.array(insert, { minLength: 1, maxLength: 5 }), async (inserts) => {
        reach.beginHistory();
        if (inserts.length >= 3) reach.reach("three or more statements");
        let refused = false;
        for (const outcome of await run(inserts)) {
          const { insert: done, refused: refusal } = outcome;
          if (outcome.pointRead && done.width * done.copies > MiB)
            reach.reach("wide row read back");
          if (outcome.widest > MiB)
            reach.reach(outcome.tableRead ? "wide table read back" : "wide table read refused");
          if (refusal === null) {
            reach.reach("written");
            if (done.width * done.copies > MiB) reach.reach("row over a MiB written");
            if (refused) reach.reach("written after a refusal");
          } else {
            refused = true;
            if (refusal.label.endsWith("/fixed-buffers")) reach.reach("refused before waiting");
            if (refusal.label === "write path memtable") reach.reach("memtable refusal");
          }
        }
      }),
      {
        seed,
        numRuns: 32,
        examples: [
          // The five-insert sequence: writes, a refused statement, and writes after it.
          [
            [
              { form: "select", width: 500_000, copies: 3 },
              { form: "values", width: MiB, copies: 1 },
              { form: "select", width: 2 * MiB, copies: 3 },
              { form: "values", width: 3_407_872, copies: 1 },
              { form: "select", width: 4096, copies: 2 },
            ],
          ],
          // The three defects: fixed buffers that waited forever, an untyped memtable refusal,
          // a read-back that waited forever.
          [
            [
              { form: "select", width: 2 * MiB, copies: 3 },
              { form: "select", width: MiB, copies: 1 },
            ],
          ],
          [
            [
              { form: "values", width: 3_407_872, copies: 1 },
              { form: "values", width: 4096, copies: 1 },
            ],
          ],
          [
            [
              { form: "values", width: 2 * MiB, copies: 1 },
              { form: "values", width: 2 * MiB, copies: 1 },
            ],
          ],
        ],
      },
    );
    reach.assertReached();
  }, 600_000);
