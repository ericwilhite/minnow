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
            const grants: Grant[] = [];
            const liveCommitBuffers = new Set<Uint8Array>();
            let independentCommitPeak = 0;
            const harness = new Harness("strict", "memory", s.rows, undefined, {
              onCommitBuffer: (buffer, owned) => {
                if (owned) {
                  expect(liveCommitBuffers.has(buffer)).toBe(false);
                  liveCommitBuffers.add(buffer);
                } else expect(liveCommitBuffers.delete(buffer)).toBe(true);
                const bytes = [...liveCommitBuffers].reduce(
                  (sum, value) => sum + value.buffer.byteLength,
                  0,
                );
                const commit = grants.find((g) => g.label === "write commit buffers" && g.held > 0);
                if (owned && (!commit || commit.allocated < bytes))
                  throw new Error("Low-cap live commit buffers exceed governor allocation");
                if (owned) {
                  const other = grants
                    .filter((g) => g !== commit)
                    .reduce((sum, g) => sum + g.allocated, 0);
                  independentCommitPeak = Math.max(independentCommitPeak, other + bytes);
                  expect(governor.peakBytes).toBeGreaterThanOrEqual(independentCommitPeak);
                  reach.reach("commit buffers independently charged");
                }
              },
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
            let minimum = 0;
            const failures: string[] = [];
            const requests: AbortController[] = [];
            // Reach-event mapping (all existing histories/model/assertions stay unchanged):
            // memory/indexeddb: backend selection; startup returned/retained: the existing
            // startup admit and onSpace release. Startup retained means idle when admission
            // arrives: record it only after the startup owner returns that credit and
            // write admission proceeds. It does not require keeping idle credit through admission.
            // active progress loan: the same real borrow.
            // idle estimate headroom: the same path.admit estimate. allocated write above
            // minimum: the working minimum now comes from admitGroup's row/I/O part, rather than
            // the old standalone admit. Commit-buffer ownership still uses onCommitBuffer,
            // finding the native commit part recorded at that same atomic group split.
            // merge transfers idle row credit under global pressure: observe an actual
            // transferCredit(write, merge, bytes) while global headroom is
            // insufficient. Prepaid merge credit already permits progress; this witnesses the
            // ownership exchange, not a dependency on returned capacity.
            // merge completed, metadata grew under pressure and durable reopen retain the
            // existing completed-merge/publication/reopen observations below. No reach minimum
            // or existing assertion is relaxed; an added per-history transfer witness makes a
            // disabled transfer fail the property as well as the reach assertion.
            const admit = governor.admit.bind(governor);
            const admitGroup = governor.admitGroup.bind(governor);
            const transferCredit = governor.transferCredit.bind(governor);
            let pressure: ((bytes: number) => unknown) | null = null;
            let transferredMergeCredit = 0;
            let pendingWriteAdmissions = 0;
            let startupReclaimedByAdmission = false;
            const watchFailure = (pending: Promise<unknown>) => {
              void pending.catch((error: unknown) => {
                if (error instanceof LoanBackedReleaseError)
                  failures.push(`LoanBackedReleaseError: ${error.message}`);
              });
            };
            governor.admit = async (options) => {
              const guard = new AbortController();
              requests.push(guard);
              const pending = admit({
                ...options,
                signal: options.signal
                  ? AbortSignal.any([options.signal, guard.signal])
                  : guard.signal,
              });
              watchFailure(pending);
              const grant = await pending;
              if (options.label === "startup") {
                const release = grant.release.bind(grant);
                grant.release = (bytes) => {
                  const before = grant.held;
                  release(bytes);
                  if (pendingWriteAdmissions > 0 && grant.held < before)
                    startupReclaimedByAdmission = true;
                };
              }
              grants.push(grant);
              return grant;
            };
            governor.admitGroup = async (options) => {
              const part = options.grants.find((part) => part.label === "write path");
              if (part) minimum = part.minimumBytes;
              const guard = new AbortController();
              requests.push(guard);
              const write = grants.find((g) => g.label === "write path" && g.held > 0);
              if (s.atMerge && options.label.startsWith("merge") && write && pressure) {
                const growth = governor.capBytes - governor.reserveBytes - governor.usedBytes - 1;
                if (growth > 0) await pressure(write.held - write.allocated + growth);
              }
              pendingWriteAdmissions++;
              try {
                const pending = admitGroup({
                  ...options,
                  signal: options.signal
                    ? AbortSignal.any([options.signal, guard.signal])
                    : guard.signal,
                });
                watchFailure(pending);
                const group = await pending;
                grants.push(...group);
                if (!s.releaseStartup && startupReclaimedByAdmission)
                  reach.reach("startup retained");
                return group;
              } finally {
                pendingWriteAdmissions--;
              }
            };
            governor.transferCredit = (source, target, bytes) => {
              const before = source.held;
              const witness =
                source.label === "write path" &&
                target.label.startsWith("merge") &&
                bytes > 0 &&
                governor.usedBytes + bytes > governor.capBytes - governor.reserveBytes &&
                source.held - source.allocated - source.loaned >= bytes;
              const moved = transferCredit(source, target, bytes);
              if (witness) {
                if (moved) {
                  transferredMergeCredit += bytes;
                  reach.reach("merge transfers idle row credit under global pressure");
                }
                if (source.held >= before)
                  failures.push("merge queued despite sufficient idle write holdings");
              }
              return moved;
            };
            harness.onSpace = () => {
              const startup = grants.find((g) => g.label === "startup" && g.held > 0);
              if (s.releaseStartup && startup) {
                const before = startup.held;
                startup.release(startup.held - startup.allocated - startup.loaned);
                if (startup.held < before) reach.reach("startup returned");
              }
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
              if (s.atMerge)
                expect(
                  transferredMergeCredit,
                  "merge must transfer idle row credit",
                ).toBeGreaterThan(0);
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
            expect(liveCommitBuffers.size).toBe(0);
            expect(
              independentCommitPeak,
              "Commit buffers must have independent ownership accounting",
            ).toBeGreaterThan(0);
            expect(governor.peakBytes).toBeGreaterThanOrEqual(independentCommitPeak);
            expect(governor.usedBytes).toBe(0);
          }),
          { seed, numRuns: 40 },
        );
      }, 120_000);
  });
it("reaches the low-cap pressure states", () => reach.assertReached());
