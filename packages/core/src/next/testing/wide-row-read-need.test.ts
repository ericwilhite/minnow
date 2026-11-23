/**
 * Reads beside and of wide rows, judged against an independently derived need (GOVERNOR-WAIT-POLICY:
 * never refuse below the usable cap). Review of #48's limits and P1's report: one stored wide value
 * made every read of its column refused at a small cap, and a 12 MiB row could not be read back at
 * 64 MiB.
 *
 * The need of a read is derived from the rows it reads, not from the engine's estimate: at most every
 * copy of a row the read holds at once. That is the fetched or scanned row's decoded output twice
 * (its buffers grow), and the result frame's copy once. On top come one bounded read step
 * (`MAX_READ_BYTES`) and an 8 MiB allowance for the result window, the key probe, frame metadata
 * and fixed arrays. A read whose need fits the usable cap must complete with the exact values.
 * Any other outcome may only be a typed 53200 whose need exceeds that cap. A one-row read (by key)
 * also admits no more than that need plus the remaining terms named below: a step funds what it
 * can use (as in #48). What a read holds is measured, not what it asked: the peak of its grants'
 * summed `held` credit (which includes reserve loans), observed at every change (`Governor.observeHoldings`:
 * admissions, growth, transfers, carving, releases), and it must be measured (non-zero).
 * Sizing by the table's widest row, or funding the same row's copies twice, over-admits by several
 * times the row and refuses reads below the cap.
 *
 * Rows are built only in ways that work today:
 * - one wide value per row, inserted at either cap;
 * - a second wide column copied from the first (`SET b = a`) only where it completes.
 *
 * Limits:
 * - at 32 MiB, `UPDATE t SET b = a` over a 2 or 3 MiB value is refused with a typed 53200: its
 *   query cannot fit beside its own statement's holdings (15,117,092 and 21,413,668 bytes, 13.8 MB
 *   available; #51 turned the former endless wait into this refusal);
 * - at 64 MiB, copying a 6 MiB value is refused with a typed 53200 (the write path's memtable,
 *   25,178,288 bytes).
 * - named remaining terms of a one-row read's admission:
 *   - a memtable read that fits without the over-cap retry funds the metadata of a publication
 *     that may happen before it opens: the format's worst case, `MEMORY_READ_RESIDENT_BYTES`
 *     (19,279,872 bytes); its real peak beside a 6 MiB value is about 2.8 MB. Fix 4 retains the
 *     chunk instead;
 *   - such a fitting read also keeps its result step's source envelope (four copies of the row's
 *     values, less the window), because fix 2 applies over the cap only;
 *   - a stored read bound counts a selected large value four times (`4 * values`), while its fetch
 *     allocates two: `2 x` the value (38,203,972 admitted, 18,975,756 real peak, for 6 MiB). The
 *     stored selected-value factor is a follow-up.
 * - at 64 MiB, `ORDER BY` over a 3 MiB value and its 3 MiB copy asks 64,041,638 bytes: within the
 *   usable cap (64,046,142), but the writer's idle live allocations (about 286 KB) can leave only
 *   64,037,596 grantable. Before #51 it could wait there; #51 closed that gap, so it is served or
 *   refused with a typed 53200, and a stall fails the test.
 * - at 32 MiB, inserting a small row and a 3 MiB row in one statement is refused at run time
 *   (`result/progress` needs 5,077,416 bytes, 1,815,232 funded: P1's write-side lane work), so the
 *   shared-group stored case runs at 64 MiB only.
 * Those two-column cases are not built here. A scan's own progress lane, and a sort's, are still
 * the four-copy envelope (the one-row fix is scoped to reads by key), so scans are held to the
 * success law only.
 */
import { afterAll, beforeAll, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { MAX_READ_BYTES } from "../storage/extents.js";
import { MEMORY_READ_RESIDENT_BYTES } from "../write/memory-read.js";
import { publicOwner } from "./public-owner.test.fixture.js";

const KiB = 1 << 10,
  MiB = 1 << 20;
const ALLOWANCE = 8 * MiB;
/** A typed refusal, as it reaches the client (the owner's error, reconstructed). */
const REFUSED = { code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" };

// The stall guard (test builds only) turns a wait that never progresses into a report, so a
// pinned divergence is asserted rather than left to time out.
let stall: number | null = null;
beforeAll(() => {
  stall = Governor.stallAfterMs;
  Governor.stallAfterMs = stall ?? 10_000;
});
afterAll(() => {
  Governor.stallAfterMs = stall;
});

/** Every copy of the read rows the read can hold at once, plus a read step and the allowance. */
function need(rowBytes: number): number {
  return 3 * rowBytes + MAX_READ_BYTES + ALLOWANCE;
}

interface Case {
  readonly cap: number;
  readonly width: number;
  /** A second wide column, copied from the first. */
  readonly copy: boolean;
  /** The rows are flushed into stored groups before they are read. */
  readonly stored?: true;
}

const cases: Case[] = [
  ...[64 * KiB, MiB, 3 * MiB, 6 * MiB].map((width) => ({ cap: 64, width, copy: false })),
  ...[64 * KiB, MiB, 3 * MiB].map((width) => ({ cap: 64, width, copy: true })),
  ...[64 * KiB, MiB, 2 * MiB, 3 * MiB].map((width) => ({ cap: 32, width, copy: false })),
  ...[64 * KiB, MiB].map((width) => ({ cap: 32, width, copy: true })),
  { cap: 64, width: 6 * MiB, copy: false, stored: true },
];

it.each(cases)(
  "reads beside and of a wide row within its independent need (cap $cap MiB, width $width, copy $copy, stored $stored)",
  async ({ cap, width, copy, stored }) => {
    // Every change to any grant's holdings (its `held` credit, loans included: growth, transfers,
    // carving, releases), observed from before the owner opens. A read's holdings are those of
    // the grants first seen while it runs, except the writer's own growth grants: one query runs
    // at a time here, so that set is exactly the query's grants. Their summed holdings, at their
    // peak, are what the read held.
    const seen = new WeakSet();
    const query = new WeakSet();
    const holdings = { on: false, current: 0, peak: 0 };
    Governor.observeHoldings = (grant, delta) => {
      if (!seen.has(grant)) {
        seen.add(grant);
        if (holdings.on && !/^(write|complete grow)/u.test(grant.label)) query.add(grant);
      }
      if (!query.has(grant)) return;
      holdings.current += delta;
      holdings.peak = Math.max(holdings.peak, holdings.current);
    };
    const owner = await publicOwner({ capBytes: cap * MiB });
    const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
    const governor = owner.core.harness.governor;
    const wide = "w".repeat(width);
    try {
      await db.execute("CREATE TABLE t(id INT PRIMARY KEY, a TEXT, b TEXT)");
      // Stored cases insert both rows in one statement, so they share one stored group: a small
      // row's read is then sized from its own pages and values, not the group's widest.
      if (stored === true)
        await db.execute("INSERT INTO t VALUES(1, 'x', NULL), (2, $1, NULL)", [wide]);
      else {
        await db.execute("INSERT INTO t VALUES(1, 'x', NULL)");
        await db.execute("INSERT INTO t VALUES(2, $1, NULL)", [wide]);
      }
      if (copy) await db.execute("UPDATE t SET b = a WHERE id = 2");
      if (stored === true) {
        // An idle writer asked to reclaim freezes and flushes its live memtable: twice, as the
        // flush returns arenas only the second request reclaims (write/range-cursor.test.ts).
        for (let round = 0; round < 2; round++) {
          const reclaim = new AbortController();
          const drained = governor
            .admit({
              label: "flush the rows into groups",
              minimumBytes: governor.usableCapacity(),
              spillable: false,
              signal: reclaim.signal,
            })
            .then(
              (grant) => {
                grant.close();
              },
              () => undefined,
            );
          await new Promise((resolve) => setTimeout(resolve, 0));
          reclaim.abort();
          await drained;
          await owner.core.harness.path.quiesce();
        }
      }
      const b = copy ? wide : null;
      const tiny = 2 + 8,
        row = (copy ? 2 : 1) * (width + 1) + 8;
      const reads: Array<{
        sql: string;
        rowBytes: number;
        expected: unknown[];
        byKey?: true;
        unordered?: true;
      }> = [
        // A small row beside a stored wide value: sized by its own width, not the wide one's.
        {
          sql: "SELECT a, b FROM t WHERE id = 1",
          rowBytes: tiny,
          expected: [{ a: "x", b: null }],
          byKey: true,
        },
        {
          sql: "SELECT a FROM t WHERE id = 1",
          rowBytes: tiny,
          expected: [{ a: "x" }],
          byKey: true,
        },
        // The wide row itself, by key.
        {
          sql: "SELECT a, b FROM t WHERE id = 2",
          rowBytes: row,
          expected: [{ a: wide, b }],
          byKey: true,
        },
        // Every row: a scan holds at most all of them at once. The ordered read runs first:
        // the writer's state it meets is the pinned divergence's (Limits).
        {
          sql: "SELECT id, a, b FROM t ORDER BY id",
          rowBytes: tiny + row,
          expected: [
            { id: 1, a: "x", b: null },
            { id: 2, a: wide, b },
          ],
        },
        {
          sql: "SELECT id, a, b FROM t",
          rowBytes: tiny + row,
          expected: [
            { id: 1, a: "x", b: null },
            { id: 2, a: wide, b },
          ],
          unordered: true,
        },
      ];
      for (const read of reads) {
        const derived = need(read.rowBytes);
        const capacity = governor.usableCapacity(true);
        holdings.on = true;
        holdings.current = 0;
        holdings.peak = 0;
        const outcome = await db.query(read.sql).then(
          (result) =>
            read.unordered === true
              ? [...result.rows()].sort((x, y) => Number(x.id) - Number(y.id))
              : result.rows(),
          (error: unknown) => error,
        );
        holdings.on = false;
        if (cap === 64 && width === 3 * MiB && copy && read.sql.endsWith("ORDER BY id")) {
          // At the edge of the usable-vs-grantable gap (Limits). #51 closed it: the query is
          // served, or refused with a typed 53200, and never waits there.
          expect(
            (outcome as { code?: string }).code,
            "#51 closes the usable-vs-grantable gap: it never stalls",
          ).not.toBe("MINNOW_GOVERNOR_STALL");
          if (Array.isArray(outcome)) expect(outcome).toEqual(read.expected);
          else {
            expect(outcome, "#51 closes the usable-vs-grantable gap").toMatchObject(REFUSED);
            const refused = outcome as { bytes: number; capacity: number };
            expect(refused.bytes, "a refusal only above the cap").toBeGreaterThan(refused.capacity);
          }
          continue;
        }
        if (stored === true && read.sql.endsWith("ORDER BY id")) {
          // Pinned known limit (Limits): a scan's and a sort's own lanes keep the four-copy
          // envelope (fix 1 covers reads by key), so over stored groups this ordered read asks
          // 65,148,872 bytes at 64 MiB for a 31.4 MB need. Scheduled with P1's sort sizing.
          if (Array.isArray(outcome)) expect(outcome).toEqual(read.expected);
          else {
            expect(outcome, "the sort lane's four-copy envelope (P1)").toMatchObject(REFUSED);
            const refused = outcome as { bytes: number; capacity: number };
            expect(refused.bytes, "a refusal only above the cap").toBeGreaterThan(refused.capacity);
          }
          continue;
        }
        if (derived <= capacity) {
          expect(outcome, `${read.sql}: need ${String(derived)} fits ${String(capacity)}`).toEqual(
            read.expected,
          );
        } else if (!Array.isArray(outcome)) {
          expect(outcome, read.sql).toMatchObject(REFUSED);
          const refused = outcome as { bytes: number; capacity: number };
          expect(refused.bytes, "a refusal only above the cap").toBeGreaterThan(refused.capacity);
        } else expect(outcome).toEqual(read.expected);
        // Named remaining terms (Limits), not a blanket allowance:
        // - a memtable read that fit without the over-cap retry funds the resident envelope of a
        //   publication that may happen first (`MEMORY_READ_RESIDENT_BYTES`; fix 4 retains the
        //   chunk instead);
        // - a query that fits keeps its result step's source envelope, four copies of the row's
        //   values (fix 2 applies over the cap only, so fitting queries size as before);
        // - a stored read bound counts a selected large value four times, its fetch allocates two
        //   (`2 x` that value; the stored selected-value factor, a follow-up).
        const allowed =
          derived +
          (stored === true
            ? read.rowBytes === row
              ? 2 * width
              : 0
            : MEMORY_READ_RESIDENT_BYTES + 4 * read.rowBytes);
        if (read.byKey === true && Array.isArray(outcome)) {
          expect(holdings.peak, `${read.sql}: its holdings were measured`).toBeGreaterThan(0);
          expect(holdings.peak, `${read.sql}: holds no more than its need`).toBeLessThanOrEqual(
            allowed,
          );
        }
      }
    } finally {
      await db.close();
      await owner.close();
      Governor.observeHoldings = null;
    }
  },
  300_000,
);
