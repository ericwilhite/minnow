/**
 * `MemoryGovernor.servableNow` beside a writer whose turn the statement holds is exact.
 *
 * Law (GOVERNOR-WAIT-POLICY point 5, next/memory/README.md): what a statement can never be
 * served while it waits is the usable capacity less what it and its writer keep held however
 * long it waits. The writer keeps its floors and its live memtables' rows
 * (`WriteCredits.retainedWhileOpen`). Everything finite is left out: an older snapshot's
 * metadata and run readers, a dropped table's state, a frozen memtable a flush frees.
 *
 * Both sides, with a writable statement holding the turn:
 * - the computed `s` is served once the finite holders release (an overcount of what stays
 *   would leave this waiting forever: a hang);
 * - `s + 1` is never served (an undercount would let it through: the check would refuse
 *   requests that fit, an over-refusal).
 * The review repro (Astra, #51): 700 rows, a snapshot pinning the table's index, the table
 * dropped. The snapshot's state must count as releasable.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SERVABLE_BESIDE_WRITER_REACH } from "../../testing/reach.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rules,
  tableInput,
  TEXT,
  texts,
} from "../write/test-support.js";
import type { WritePathImpl } from "../write/engine.js";

const CAP = 32 << 20;
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

interface Spec {
  readonly rows: number;
  readonly indexed: boolean;
  readonly drop: boolean;
}

/** Wide text rows written last, one per statement: they stay live, or (with small memtables)
 * freeze one, whose flush may be held before it publishes while the statement waits. */
interface Wide {
  readonly bytes: number;
  readonly rows: number;
  readonly holdFlush: boolean;
  /** Flushes run on the memory store; on the simulated disk they wait for durability. */
  readonly store?: "sim" | "memory";
}

async function scenario(
  tables: readonly Spec[],
  memtableRows: number,
  holdSnapshot: boolean,
  reach: ReturnType<typeof createReach>,
  wide: Wide = { bytes: 0, rows: 0, holdFlush: false },
): Promise<void> {
  let hold = false;
  let heldFlushes = 0;
  let resume: (() => void) | null = null;
  const h = new Harness("strict", wide.store ?? "sim", memtableRows, undefined, {
    beforeFlushPublish: () => {
      if (!hold) return Promise.resolve();
      heldFlushes++;
      return new Promise<void>((resolve) => {
        resume = resolve;
      });
    },
  });
  const governor = new Governor(CAP);
  Object.defineProperty(h, "governor", { value: governor });
  const path: WritePathImpl = await h.open();
  let snapshot: ReturnType<WritePathImpl["pin"]> | null = null;
  try {
    for (const [k, spec] of tables.entries()) {
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput(
                `t${String(k)}`,
                [column("id", INT, false), column("v", INT)],
                spec.indexed ? {} : { primaryKey: null },
              ),
            })
          ).id ?? 0;
      });
      const ids = Array.from({ length: spec.rows }, (_, i) => i);
      await autocommit(path, (s) => s.table(t).insert(batch(ints(ids), ints(ids)), null));
      await path.quiesce();
      if (spec.drop) {
        if (holdSnapshot && snapshot === null) {
          snapshot = path.pin();
          if (spec.indexed)
            await snapshot
              .index(t, path.catalog.table(t)?.primaryKey ?? 0)
              .collect({ lower: null, upper: null });
        }
        await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
        await path.quiesce();
        reach.reach(snapshot !== null ? "table dropped under a snapshot" : "table dropped");
      }
    }
    if (wide.rows > 0) {
      let w = 0;
      await autocommit(path, async (s) => {
        w =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("wide", [column("id", INT, false), column("v", TEXT)]),
            })
          ).id ?? 0;
      });
      await path.quiesce();
      hold = wide.holdFlush;
      for (let id = 0; id < wide.rows; id++) {
        const insert = autocommit(path, (s) =>
          s.table(w).insert(batch(ints([id]), texts(["w".repeat(wide.bytes)])), null),
        ).then(
          () => null,
          (error: unknown) => error,
        );
        // Under memory pressure an insert relieves the memtable by waiting for the flush: let a
        // held flush go then (this history then holds none).
        let refused = await within(insert, 1_000);
        if (refused === PENDING) {
          hold = false;
          (resume as (() => void) | null)?.();
          refused = await insert;
        }
        // A setup that does not fit at this cap stops here (typed): the laws hold for any state.
        if (refused !== null) {
          expect(refused).toMatchObject({ sqlState: "53200" });
          break;
        }
      }
      for (let i = 0; i < 100 && hold && heldFlushes === 0 && wide.rows >= memtableRows; i++)
        await new Promise((resolve) => setTimeout(resolve, 5));
      if (hold && heldFlushes > 0) reach.reach("frozen rows under a held flush");
      else {
        // No flush is held: quiescing must not hold the flush it may start.
        hold = false;
        await path.quiesce();
      }
      if (wide.rows < memtableRows && wide.bytes >= 1 << 20) reach.reach("wide live rows");
    }
    if (memtableRows > 700) reach.reach("live memtable rows");
    // A writable statement holds the writer turn while its request waits. `begin` waits while
    // frozen memtables awaiting publication exceed their cap (engine.ts `begin`, ruling 6):
    // a held flush is let go then, as for the inserts above.
    const starting = path.begin();
    let tx = await within(starting, 1_000);
    if (tx === PENDING) {
      hold = false;
      (resume as (() => void) | null)?.();
      tx = await starting;
    }
    const statement = tx.statement({ rules });
    try {
      const lent = statement.writerHoldings();
      if (lent === null) throw new Error("a writable statement holds the writer turn");
      const holdings = { grants: [], lent };
      const admit = (bytes: number) =>
        governor
          .admitGroup({
            label: "statement request",
            includesProgress: true,
            grants: [{ label: "statement request", minimumBytes: bytes, spillable: false }],
            holdings: () => holdings,
          })
          .then(
            (grants) => {
              for (const grant of grants) grant.close();
              return "served" as const;
            },
            (error: unknown) => error,
          );
      // While a flush of the writer's runs, its holdings are not yet known and count nothing.
      const early = governor.servableNow(holdings, true);
      const queued = admit(early);
      snapshot?.release();
      snapshot = null;
      hold = false;
      (resume as (() => void) | null)?.();
      // The writer's own work ends (the turn stays held): its holdings are now exactly known,
      // and the writer wakes the queue. Nothing else touches the governor before the check.
      await path.quiesce();
      const decided = await within(queued, 4_000);
      expect(decided, `${String(early)} bytes queued beside the writer's turn`).not.toBe(PENDING);
      const s = governor.servableNow(holdings, true);
      // Decided: served, or refused only when it exceeds what could ever be served.
      if (decided !== "served") {
        expect(decided).toMatchObject({ name: "MemoryAdmissionError", sqlState: "53200" });
        expect(early).toBeGreaterThan(s);
        reach.reach("refused once the writer settled");
      }
      // Exact: `s` is served once the finite holders release (an overcount of what stays would
      // leave it waiting); one byte more is refused at once (an undercount would serve it).
      expect(await within(admit(s), 4_000), `the servable ${String(s)} bytes`).toBe("served");
      reach.reach("served exactly");
      const beyond = await within(admit(s + 1), 300);
      expect(beyond, `${String(s + 1)} bytes beside the writer's turn`).toMatchObject({
        name: "MemoryAdmissionError",
        sqlState: "53200",
      });
      reach.reach("one more byte refused");
    } finally {
      statement.rollback();
      tx.rollback();
    }
  } finally {
    hold = false;
    (resume as (() => void) | null)?.();
    snapshot?.release();
    await h.close();
  }
}

it("counts a snapshot's state over a dropped table as releasable (review repro)", async () => {
  const reach = createReach("servable beside writer repro", [
    "table dropped under a snapshot",
    "served exactly",
    "one more byte refused",
  ]);
  reach.beginHistory();
  await scenario([{ rows: 700, indexed: true, drop: true }], 16, true, reach);
  reach.assertReached();
}, 60_000);

for (const seed of seedsFor("next-servable-beside-writer", [0x5c01, 0x5c02]))
  it(`is exact beside a writer whose turn the statement holds (seed ${String(seed)})`, async () => {
    const reach = createReach("servable beside writer", SERVABLE_BESIDE_WRITER_REACH);
    const previousStall = Governor.stallAfterMs;
    Governor.stallAfterMs = null;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.array(
            fc.record({
              rows: fc.constantFrom(1, 40, 700),
              indexed: fc.boolean(),
              drop: fc.boolean(),
            }),
            { minLength: 1, maxLength: 2 },
          ),
          fc.constantFrom(2, 16, 1 << 16),
          fc.boolean(),
          fc.record({
            bytes: fc.constantFrom(4096, 1 << 20, 2 << 20),
            rows: fc.constantFrom(0, 1, 2, 3, 4),
            holdFlush: fc.boolean(),
            store: fc.constantFrom("sim" as const, "memory" as const),
          }),
          async (tables, memtableRows, holdSnapshot, wide) => {
            // Excluded, and said so: on the memory store, 700 rows in 2-row memtables leave
            // hundreds of frozen memtables whose flush needs more than this cap (the write path
            // refuses its own step with 53200, "complete grow write path metadata"). That is the
            // write path's limit at 32 MiB, not this law's. (On the simulated disk they stay
            // frozen: that history is kept.)
            fc.pre(
              wide.store !== "memory" ||
                memtableRows > 2 ||
                tables.every((table) => table.rows <= 40),
            );
            reach.beginHistory();
            await scenario(tables, memtableRows, holdSnapshot, reach, wide);
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: [
            [
              [{ rows: 700, indexed: true, drop: true }],
              16,
              true,
              { bytes: 0, rows: 0, holdFlush: false },
            ],
            [
              [{ rows: 1, indexed: true, drop: false }],
              16,
              false,
              { bytes: 2 << 20, rows: 3, holdFlush: false },
            ],
            [
              [{ rows: 1, indexed: true, drop: false }],
              2,
              false,
              { bytes: 1 << 20, rows: 4, holdFlush: true, store: "memory" },
            ],
            // Merge credit grown above its minimum: it stays in full while the turn is held.
            [
              [
                { rows: 700, indexed: true, drop: false },
                { rows: 1, indexed: false, drop: false },
              ],
              2,
              false,
              { bytes: 2 << 20, rows: 3, holdFlush: false, store: "sim" },
            ],
          ],
        },
      );
    } finally {
      Governor.stallAfterMs = previousStall;
    }
    reach.assertReached();
  }, 600_000);
