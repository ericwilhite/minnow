/** Governor rule 5: payload and frame construction form one complete admitted step. */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PARAMETER_RESULT_PROGRESS_REACH } from "../../testing/reach.js";
import { QueryRuntime } from "../exec/run.js";
import { StepLoans } from "../memory/step-loans.js";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { PressureCampaign, reserveWorkingSet } from "./compound-progress-history.test.fixture.js";

// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied.
const borrow = StepLoans.prototype.borrow;
// Twelve histories per seed: four bounded bodies, each one example plus two random histories.
// Partition by operation/column count; preserve all four original wide witnesses.
const parts = [
  { kind: "EXCEPT", min: 0, max: 3, example: ["EXCEPT", (1 << 20) + 17, "é", 0] },
  { kind: "JOIN", min: 0, max: 3, example: ["JOIN", (5 << 20) + 29, "é", 0] },
  { kind: "JOIN", min: 4, max: 6, example: ["JOIN", (1 << 20) + 11, "x", 6] },
  { kind: "EXCEPT", min: 4, max: 6, example: ["EXCEPT", (1 << 20) + 31, "x", 4] },
] as const;
for (const seed of seedsFor("next-parameter-result-progress", [49525, 49526])) {
  const reach = createReach("parameter result progress", PARAMETER_RESULT_PROGRESS_REACH);
  afterAll(() => reach.assertReached());
  for (const part of parts)
    it(`funds parameter delivery independently of encoding scratch (${String(seed)}, ${part.kind}, ${String(part.min + 3)}..${String(part.max + 3)} columns)`, async () => {
      const campaign = new PressureCampaign();
      const history = await campaign.history(64 << 20);
      const { core } = history;
      try {
        await fc.assert(
          fc.asyncProperty(
            fc.constant(part.kind),
            fc.integer({ min: (1 << 20) + 1, max: (1 << 20) + 65536 }),
            fc.constantFrom("x", "é"),
            fc.integer({ min: part.min, max: part.max }),
            async (kind, width, atom, extras) => {
              history.guard();
              reach.beginHistory();
              const value = atom.repeat(Math.ceil(width / atom.length));
              const extra = Array.from(
                { length: extras },
                (_, i) => `${i % 2 ? "NULL::int" : String(i)} AS c${String(i)}`,
              );
              const suffix = extra.length ? "," + extra.join(",") : "";
              const sql =
                kind === "JOIN"
                  ? `SELECT l.j,l.n,r.n AS rn${suffix} FROM (SELECT i AS n,$1::text AS j FROM generate_series(1,2)g(i))l JOIN (SELECT i AS n,$1::text AS j FROM generate_series(1,3,2)g(i))r ON l.n=r.n AND l.j=r.j`
                  : `SELECT $1::text AS j,1 AS n,1 AS rn${suffix} FROM generate_series(1,2)g(i) EXCEPT SELECT NULL::text,NULL::int,NULL::int${extra.map(() => ",NULL::int").join("")}`;
              const expected: Record<string, unknown> = { j: value, n: 1, rn: 1 };
              for (let i = 0; i < extras; i++) expected[`c${String(i)}`] = i % 2 ? null : i;
              const governor = new Governor(256 << 20);
              const blocker = await governor.admit({
                label: "retained competitor",
                minimumBytes: 0,
                spillable: true,
              });
              const closeBlocker = history.ownCleanup(() => blocker.close());
              let occupied = false,
                loans = 0;
              const closeReserve = history.observe(() =>
                vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
                  this: QueryRuntime,
                  ...args
                ) {
                  await reserveWorkingSet.apply(this, args);
                  if (this.request.governor === governor && !occupied) {
                    expect(
                      blocker.allocate(
                        governor.capBytes - governor.reserveBytes - governor.usedBytes,
                      ),
                    ).toBe(true);
                    occupied = true;
                  }
                }),
              );
              const closeLoan = history.observe(() =>
                vi.spyOn(StepLoans.prototype, "borrow").mockImplementation(function (
                  this: StepLoans,
                  bytes,
                  signal,
                ) {
                  if (this.label.endsWith("/result/progress")) {
                    expect(bytes).toBeLessThanOrEqual(this.capacity);
                    loans++;
                  }
                  return borrow.call(this, bytes, signal);
                }),
              );
              const prepare = core.executor.prepare.bind(core.executor);
              const closePrepare = history.observe(() =>
                vi
                  .spyOn(core.executor, "prepare")
                  .mockImplementation((plan) => history.wrap(prepare(plan), governor)),
              );
              try {
                const result = await core.execute(sql, [value]);
                if (result.kind !== "rows" || !("batches" in result))
                  throw new Error("Expected rows");
                expect(oracleRows(result)).toEqual([expected]);
                expect(occupied).toBe(true);
                if (loans > 0) reach.reach("frame progress");
                expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
                reach.reach(kind === "JOIN" ? "join" : "except");
                reach.reach(atom === "x" ? "ASCII" : "multibyte");
                reach.reach(extras > 0 ? "additional scalar columns" : "three columns");
                if (width > 5 << 20) reach.reach("five MiB");
                reach.reach("occupied credit");
              } finally {
                closePrepare();
                closeLoan();
                closeReserve();
                await closeBlocker();
                expect(governor.usedBytes).toBe(0);
                expect(core.spill.bytesInUse).toBe(0);
                reach.reach("zero retained");
              }
            },
          ),
          {
            seed,
            numRuns: 3,
            endOnFailure: true,
            examples: [[...part.example]],
          },
        );
      } finally {
        await campaign.close();
      }
    }, 120000);
}
