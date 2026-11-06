/**
 * PROPOSAL "One memory governor": a feasible durable load grants/spills, never size-refuses.
 * Governor rules 4/5/7: free allocations are not live buffers; idle, non-loan holdings must
 * yield to progress. Generate real loads with estimate headroom near the ordinary cap, over
 * memory and the real IDB adapter on sim-idb. Assert accounting, merge progress, rows and reopen.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { Grant } from "../memory/governor.js";
import { Governor, LoanBackedReleaseError } from "../memory/governor.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_LOW_CAP_STATES } from "../../testing/reach.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";

function assertRows(actual: unknown[][], expected: unknown[][]): void {
  expect(actual.length).toBe(expected.length);
  for (let row = 0; row < expected.length; row++) {
    expect(actual[row]?.length).toBe(expected[row]?.length);
    for (let column = 0; column < (expected[row]?.length ?? 0); column++)
      expect(
        actual[row]?.[column] === expected[row]?.[column],
        `complete scalar equality at row ${String(row)}, column ${String(column)}`,
      ).toBe(true);
  }
}

const MiB = 1024 * 1024;
const reach = createReach("low-cap durable write progress", WRITE_LOW_CAP_STATES);
const scenario = fc.record({
  capMiB: fc.constantFrom(32, 48, 64),
  rows: fc.integer({ min: 8, max: 40 }),
  columns: fc.integer({ min: 2, max: 8 }),
  valueBytes: fc.constantFrom(8, 16, 24, 32),
  cycles: fc.integer({ min: 6, max: 12 }),
  fanIn: fc.constantFrom(2, 3),
  loan: fc.boolean(),
  atMerge: fc.boolean(),
  releaseStartup: fc.boolean(),
});

for (const backend of ["memory", "indexeddb"] as const)
  describe(`low-cap load over ${backend}`, () => {
    for (const seed of seedsFor("next-write-low-cap", [0x256]))
      it(`returns idle reservations for metadata and merges (seed ${String(seed)})`, async () => {
        await fc.assert(
          fc.asyncProperty(scenario, async (s) => {
            reach.beginHistory();
            reach.reach(backend);
            reach.reach(s.releaseStartup ? "startup returned" : "startup retained");
            const harness = new Harness("strict", "memory", s.rows, undefined, {
              merges: { fanIn: s.fanIn, stallRuns: 2 * s.fanIn, sliceEntries: 32 },
              logRecycleBytes: 16 * MiB,
            });
            const governor = new Governor(s.capMiB * MiB);
            Object.assign(harness, { governor });
            if (backend === "indexeddb")
              Object.assign(harness, {
                backend: createIndexedDbBackend({
                  database: () => simIndexedDb(harness.disk),
                  name: "low-cap",
                  locks: new SimLocks(),
                  memory: governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
                }),
              });
            const grants: Grant[] = [];
            let minimum = 0;
            const failures: string[] = [];
            const requests: AbortController[] = [];
            const admit = governor.admit.bind(governor);
            let pressure: ((bytes: number) => unknown) | null = null;
            governor.admit = async (options) => {
              if (options.label === "write path") minimum = options.minimumBytes;
              const guard = new AbortController();
              requests.push(guard);
              const write = grants.find((g) => g.label === "write path" && g.held > 0);
              if (s.atMerge && options.label.startsWith("merge") && write && pressure) {
                const growth = governor.capBytes - governor.reserveBytes - governor.usedBytes - 1;
                if (growth > 0) await pressure(write.held - write.allocated + growth);
              }
              const before = write?.held ?? 0;
              const witness =
                options.label.startsWith("merge") &&
                write !== undefined &&
                governor.usedBytes + options.minimumBytes >
                  governor.capBytes - governor.reserveBytes &&
                write.held - write.allocated - write.loaned >= options.minimumBytes;
              const pending = admit({ ...options, signal: guard.signal });
              void pending.catch((error: unknown) => {
                if (error instanceof LoanBackedReleaseError)
                  failures.push(`LoanBackedReleaseError: ${error.message}`);
              });
              if (witness) {
                reach.reach("merge needs returned headroom");
                // Governor reclaim callbacks run synchronously before leaving the request queued.
                // Abort only to clean up a broken parent/mutant; the invariant is asserted below.
                if (write.held >= before) {
                  failures.push("merge queued despite sufficient idle write holdings");
                  guard.abort(new Error(failures.at(-1)));
                }
              }
              const grant = await pending;
              grants.push(grant);
              return grant;
            };
            harness.onSpace = () => {
              const startup = grants.find((g) => g.label === "startup" && g.held > 0);
              if (s.releaseStartup && startup)
                startup.release(startup.held - startup.allocated - startup.loaned);
            };
            let path = await harness.open();
            pressure = (bytes) => path.admit(bytes);
            let scratch: Uint8Array | null = null;
            let loan: Awaited<ReturnType<Grant["borrow"]>> | null = null;
            try {
              let table = 0;
              await autocommit(path, async (statement) => {
                table =
                  (
                    await statement.changeCatalog({
                      kind: "createTable",
                      table: tableInput("t", [
                        column("id", INT, false),
                        ...Array.from({ length: s.columns }, (_, k) =>
                          column(`v${String(k)}`, TEXT),
                        ),
                      ]),
                    })
                  ).id ?? 0;
              });
              const write = grants.find((g) => g.label === "write path" && g.held > 0);
              if (!write) throw new Error("Missing write grant");
              // A real allocated working buffer keeps the reclaim test above its admitted
              // minimum without making the virtual disk itself large.
              if (!write.allocate(6 * MiB)) throw new Error("working fixture did not fit");
              scratch = new Uint8Array(6 * MiB).fill(0x5a);
              if (s.loan) {
                loan = await write.borrow(64 * 1024);
                reach.reach("active progress loan");
              }
              const expected: unknown[][] = [];
              for (let cycle = 0; cycle < s.cycles; cycle++) {
                // A genuine estimate reservation, like the wide-batch estimate in B1's W5M repro.
                // Leave too little ordinary room for metadata growth or the next merge minimum.
                const growth = governor.capBytes - governor.reserveBytes - governor.usedBytes - 1;
                if (growth > 0) await path.admit(write.held - write.allocated + growth);
                reach.reach("idle estimate headroom");
                const ids = Array.from({ length: s.rows }, (_, k) => cycle * s.rows + k);
                const strings = ids.map((id) => `value-${String(id)}-${"x".repeat(s.valueBytes)}`);
                await autocommit(path, (statement) =>
                  statement
                    .table(table)
                    .insert(
                      batch(ints(ids), ...Array.from({ length: s.columns }, () => texts(strings))),
                      null,
                    ),
                );
                if (write.allocated > minimum) reach.reach("allocated write above minimum");
                expected.push(
                  ...ids.map((id, k) => [
                    id,
                    ...Array.from({ length: s.columns }, () => strings[k]),
                  ]),
                );
                await path.quiesce();
                expect(
                  failures,
                  "idle holdings must yield synchronously, not deadlock admission",
                ).toEqual([]);
                expect(path.maintenance.mergeFailures).toBe(0);
                expect(path.failure).toBeNull();
                expect(governor.usedBytes).toBeLessThanOrEqual(governor.capBytes);
                expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
                expect(write.allocated).toBeLessThanOrEqual(write.held);
                expect(write.loaned).toBeLessThanOrEqual(write.held);
                if (loan) expect(write.loaned).toBe(loan.bytes);
              }
              expect(path.maintenance.merges).toBeGreaterThan(0);
              reach.reach("merge completed");
              reach.reach("metadata grew under pressure");
              const view = path.pin();
              try {
                assertRows(await rowsOf(view, table), expected);
              } finally {
                view.release();
              }
              expect(scratch[0]).toBe(0x5a);
              expect(scratch.at(-1)).toBe(0x5a);
              write.free(scratch.byteLength);
              scratch = null;
              loan?.repay();
              loan = null;
              path = await harness.reopen();
              const reopened = path.pin();
              try {
                assertRows(await rowsOf(reopened, table), expected);
              } finally {
                reopened.release();
              }
              reach.reach("durable reopen");
              await harness.space.audit();
            } finally {
              for (const request of requests) request.abort();
              if (scratch) {
                grants
                  .find((g) => g.label === "write path" && g.held > 0)
                  ?.free(scratch.byteLength);
              }
              loan?.repay();
              await harness.close();
            }
            expect(governor.usedBytes).toBe(0);
          }),
          { seed, numRuns: 40 },
        );
      }, 120_000);
  });
it("reaches the low-cap pressure states", () => reach.assertReached());
