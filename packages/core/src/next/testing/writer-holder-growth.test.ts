/** Holder priority for ordinary DML (GOVERNOR-WAIT-POLICY.md): the writer always holds its floors
 * and its statement's rows while it grows, and cannot release them during the wait. A foreign
 * request that holds nothing, queued just ahead of the writer's growth, fits beside the writer at
 * rest but not beside what the writer holds mid-statement. Laws: everyone progresses (the stall
 * guard fails a stuck wait), nothing at or below the cap is refused, both are served in full.
 * Limit: this is a liveness check, not a check of the marking. The writer's row and metadata
 * grants are spillable, so the governor asks them to reclaim and spill when the foreign request
 * queues; it is served at once and the writer's growth after it, with or without holder priority.
 * Its commit and merge floors are permanent, so no request is ever served by their release. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITER_HOLDER_GROWTH_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("next-writer-holder-growth", [50111, 50112]))
  it(`serves ordinary writer growth before an earlier foreign request (${String(seed)})`, async () => {
    const reach = createReach("writer holder growth", WRITER_HOLDER_GROWTH_REACH);
    const previousStall = Governor.stallAfterMs;
    Governor.stallAfterMs = 5_000;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 20_000, max: 40_000 }),
          fc.integer({ min: 100, max: 400 }),
          fc.constantFrom("insert", "update"),
          async (rows, width, kind) => {
            reach.beginHistory();
            const governor = new Governor(64 << 20),
              core = new StorageCore(64);
            Object.defineProperty(core.harness, "governor", { value: governor });
            await core.open();
            const state: { foreign: Promise<number> | null; want: number } = {
              foreign: null,
              want: 0,
            };
            try {
              await core.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)");
              if (kind === "update")
                await core.execute(
                  `INSERT INTO t SELECT g, '${"a".repeat(width)}' FROM generate_series(1, ${String(rows)}) AS g`,
                );
              // What the idle writer keeps between statements: the foreign request fits beside it.
              const rest = governor.usedBytes,
                room = governor.capBytes - governor.reserveBytes;
              const admit = governor.admitGroup.bind(governor);
              vi.spyOn(governor, "admitGroup").mockImplementation((request) => {
                if (state.foreign === null && request.label.startsWith("complete grow")) {
                  // Queued ahead of the writer's growth; only the writer's release can serve it.
                  const want = (state.want = room - rest - (64 << 10));
                  if (governor.usedBytes + want > room) {
                    reach.reach("foreign request blocked by the writer's holdings");
                    state.foreign = admit({
                      label: "foreign rest",
                      grants: [{ label: "foreign rest", minimumBytes: want, spillable: false }],
                    }).then((grants: readonly Grant[]) => {
                      const held = grants.reduce((sum, grant) => sum + grant.held, 0);
                      for (const grant of grants) grant.close();
                      return held;
                    });
                  }
                }
                return admit(request);
              });
              const sql =
                kind === "insert"
                  ? `INSERT INTO t SELECT g + ${String(rows)}, '${"b".repeat(width)}' FROM generate_series(1, ${String(rows)}) AS g`
                  : `UPDATE t SET s = '${"c".repeat(width)}'`;
              await core.execute(sql);
              vi.restoreAllMocks();
              const count = await core.execute("SELECT count(*) AS n FROM t");
              if (!("batches" in count)) throw new Error("rows required");
              const value = count.batches[0]?.columns[0];
              expect(value).toBeDefined();
              if (state.foreign !== null) {
                // Admitted in full once the writer returned to rest: nothing refused, all progress.
                expect(await state.foreign).toBe(state.want);
                reach.reach("foreign request admitted after the writer's statement");
                if (kind === "update") reach.reach("update growth");
              } else reach.reach("no growth at this size");
            } finally {
              vi.restoreAllMocks();
              await core.close();
            }
            expect(governor.usedBytes).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [40_000, 300, "insert"],
            [40_000, 300, "update"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      Governor.stallAfterMs = previousStall;
    }
  }, 600_000);
