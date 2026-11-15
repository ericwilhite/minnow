/** Each global/row/FTS checkpoint has its own coverage, and every one needs WAL authority. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_REPLAY_COVERAGE_REACH } from "../../testing/reach.js";
import { Replayer } from "../format/replay.js";
import { LogCorruptionError } from "../format/log-frame.js";
for (const seed of seedsFor("next-fts-replay-coverage", [0xb857, 0xb858]))
  it(`keeps independent FTS replay and log authority ${String(seed)}`, () => {
    const reach = createReach("FTS replay coverage", FTS_REPLAY_COVERAGE_REACH);
    fc.assert(
      fc.property(fc.integer({ min: 5, max: 100000 }), (n) => {
        reach.beginHistory();
        const checkpoint = {
          globalLsn: n - 2,
          ftsThroughLsn: n,
          nextTableId: 2,
          tables: new Map([[1, { flushedThroughLsn: n - 3, schemaVersion: 1 }]]),
        };
        const r = new Replayer(checkpoint),
          effects = [
            { kind: "fts" as const },
            { kind: "rows" as const, tableId: 1, schemaVersion: 1 },
            { kind: "request" as const },
          ];
        expect(r.next(n - 1, effects)).toEqual([false, true, true]);
        expect(r.next(n, effects)).toEqual([false, true, true]);
        reach.reach("FTS covered rows replay");
        expect(r.next(n + 1, effects)).toEqual([true, true, true]);
        reach.reach("new claim");
        expect(() => Replayer.checkCoverage(checkpoint, n - 1)).toThrow(LogCorruptionError);
        reach.reach("missing WAL refused");
        expect(() => Replayer.checkCoverage(checkpoint, n)).not.toThrow();
        reach.reach("complete WAL");
        expect(new Replayer({ ...checkpoint, globalLsn: n + 2 }).next(n + 1, effects)).toEqual([
          false,
          true,
          false,
        ]);
        reach.reach("global independent");
      }),
      { seed, numRuns: 120 },
    );
    reach.assertReached();
  });
