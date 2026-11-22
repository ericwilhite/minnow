/**
 * A modification's spool takes a progress loan for each row it holds, and that loan is
 * admitted with the statement as its result progress lane. The lane was sized from the bound
 * parameters' bytes, so a computed row larger than its parameters (`$1 || $1 || $1`), or any wide
 * row beside another holder, was refused with 53200 ("result/progress needs … available") far
 * below the usable cap. The lane is now sized from the modification row's own bound
 * (exec/input-size-plan.ts `columnInputBytes`: concatenation sums its arguments) for the spool's
 * step (exec/modify-spool.ts `wideRowStepBytes`: the row's encoded page twice, and a working
 * page).
 *
 * Law (GOVERNOR-WAIT-POLICY, next/memory/README.md): a step larger than its admitted lane is the
 * statement's own sizing error, never a memory refusal; the lane must cover it. So the result
 * progress lane never refuses a modification here. Whether a row fits the cap otherwise (the
 * memtable's charge, the fixed buffers) is the fit model's question: any other refusal must be
 * the typed 53200, writing nothing.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_ROW_PROGRESS_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const MiB = 1 << 20;
const PENDING = Symbol("pending");
const within = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<typeof PENDING>((resolve) => {
      setTimeout(() => {
        resolve(PENDING);
      }, ms);
    }),
  ]);

afterEach(() => {
  Governor.stallAfterMs = null;
});

/** The stored ids, read narrowly: a wide value never sizes this read. */
async function ids(core: StorageCore): Promise<number[]> {
  const after = await core.execute("SELECT id FROM t ORDER BY id");
  if (!("batches" in after)) throw new Error("rows");
  return oracleRows(after).map((r) => Number(r.id));
}

/** Row `id`'s value, or null when reading it back is refused: whether a wide value's read fits
 * is the read side's sizing (outside this law), so it may only be refused typed. */
async function value(core: StorageCore, id: number): Promise<string | null> {
  const outcome: unknown = await core.execute(`SELECT v FROM t WHERE id = ${String(id)}`).then(
    (rows) => rows,
    (error: unknown) => error,
  );
  if (typeof outcome === "object" && outcome !== null && "batches" in outcome) {
    const [row] = oracleRows(outcome as Parameters<typeof oracleRows>[0]);
    return String(row?.v);
  }
  expect(outcome).toMatchObject({ sqlState: "53200" });
  return null;
}

/** The statement's outcome checked against the lane's law; returns whether it wrote. */
function checked(outcome: unknown, context: string): boolean {
  if (outcome === null) return true;
  expect(outcome, context).toMatchObject({ sqlState: "53200", code: "MINNOW_MEMORY_ADMISSION" });
  expect(
    (outcome as { label: string }).label,
    `${context}: refused by the result lane`,
  ).not.toMatch(/\/result\/progress$/);
  return false;
}

it("serves a 6 MiB UPDATE SET at 64 MiB", async () => {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(64 * MiB) });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO t VALUES (1, 'x')");
    const wide = "b".repeat(6 * MiB);
    await core.execute("UPDATE t SET v = $1 WHERE id = 1", [wide]);
    const stored = await value(core, 1);
    if (stored !== null) expect(stored === wide).toBe(true);
    expect(await ids(core)).toEqual([1]);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    await core.close();
  }
}, 60_000);

it("writes a 2.5 MB row once a background flush that holds memory publishes", async () => {
  Governor.stallAfterMs = 5_000;
  let hold = false;
  let held = 0;
  let resume: (() => void) | null = null;
  // Memtables of two rows: the fourth wide row freezes one and starts its flush.
  const core = new StorageCore(2, {
    beforeFlushPublish: () => {
      if (!hold) return Promise.resolve();
      held++;
      return new Promise<void>((resolve) => {
        resume = resolve;
      });
    },
  });
  Object.defineProperty(core.harness, "governor", { value: new Governor(32 * MiB) });
  await core.open();
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    hold = true;
    for (let id = 1; id <= 4; id++)
      await core.execute(`INSERT INTO t SELECT ${String(id)}, $1`, ["q".repeat(MiB)]);
    for (let i = 0; i < 200 && held === 0; i++)
      await new Promise((resolve) => setTimeout(resolve, 5));
    expect(held, "a flush is held before it publishes").toBe(1);
    const statement = core.execute("INSERT INTO t VALUES (10, $1)", ["w".repeat(2_500_000)]).then(
      () => null,
      (error: unknown) => error,
    );
    // Never refused by its lane: written once the flush publishes (or at once if it fits).
    const early = await within(statement, 500);
    if (early !== PENDING) checked(early, "beside the held flush");
    hold = false;
    (resume as (() => void) | null)?.();
    expect(await statement).toBeNull();
    expect(await ids(core)).toEqual([1, 2, 3, 4, 10]);
    expect(core.harness.path.failure).toBeNull();
  } finally {
    hold = false;
    (resume as (() => void) | null)?.();
    await core.close();
  }
}, 60_000);

for (const seed of seedsFor("next-modify-row-progress", [0x5a01, 0x5a02]))
  it(`never refuses a modification by its result lane (seed ${String(seed)})`, async () => {
    const reach = createReach("modify row progress", MODIFY_ROW_PROGRESS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(1, 2, 3),
        fc.constantFrom(4096, 200_000, 500_000, 1_000_000),
        fc.constantFrom("insert", "update"),
        fc.constantFrom(32, 64),
        async (copies, width, kind, capMiB) => {
          reach.beginHistory();
          Governor.stallAfterMs = 5_000;
          // A fresh database per run: only this run's own rows size its statements.
          const core = new StorageCore();
          Object.defineProperty(core.harness, "governor", { value: new Governor(capMiB * MiB) });
          await core.open();
          try {
            await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
            await core.execute("INSERT INTO t VALUES (1, 'old')");
            const parameter = "q".repeat(width);
            const expression = Array.from({ length: copies }, () => "$1").join(" || ");
            const row = copies * width;
            reach.reach(kind);
            if (copies > 1) reach.reach("row larger than its parameter");
            if (row > MiB) reach.reach("row over a MiB");
            const target = kind === "insert" ? 2 : 1;
            const sql =
              kind === "insert"
                ? `INSERT INTO t SELECT ${String(target)}, ${expression}`
                : `UPDATE t SET v = ${expression} WHERE id = ${String(target)}`;
            const outcome = await core.execute(sql, [parameter]).then(
              () => null,
              (error: unknown) => error,
            );
            if (checked(outcome, `${sql} with a ${String(width)}-byte parameter`)) {
              expect(await ids(core)).toEqual(kind === "insert" ? [1, 2] : [1]);
              const stored = await value(core, target);
              if (stored !== null)
                expect(stored === parameter.repeat(copies), "stored value").toBe(true);
              reach.reach("written");
            } else {
              // Nothing written: no new row, and the old value unchanged.
              expect(await ids(core)).toEqual([1]);
              expect(await value(core, 1)).toBe("old");
            }
            expect(core.harness.path.failure).toBeNull();
          } finally {
            Governor.stallAfterMs = null;
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [3, 1_000_000, "insert", 32],
          [3, 1_000_000, "update", 32],
        ],
      },
    );
    reach.assertReached();
  }, 600_000);
