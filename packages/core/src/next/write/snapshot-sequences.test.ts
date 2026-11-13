/** R4: pin-time capture is read-only, synchronous, and preserves reservation horizons. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_SEQUENCE_CAPTURE_REACH } from "../../testing/reach.js";
import type { SequenceDef } from "../access/catalog.js";
import { Governor } from "../memory/governor.js";
import { Sequences } from "./sequences.js";

for (const seed of seedsFor("next-snapshot-sequences", [0xb285, 0xb286]))
  it(`captures charged restart horizons without changing allocation seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot sequence capture", SNAPSHOT_SEQUENCE_CAPTURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 12 }),
        fc.integer({ min: 1, max: 30 }),
        async (count, salt) => {
          reach.beginHistory();
          for (const direction of [-1n, 1n])
            for (const cycle of [false, true]) {
              let lsn = 0;
              const definitions: SequenceDef[] = [1, 2, 3].map((id) => ({
                id,
                name: `s${String(id)}`,
                start: 0n,
                increment: direction,
                min: -1000n,
                max: 1000n,
                cycle,
                ownedBy: null,
              }));
              const history: Array<{ sequenceId: number; lsn: number; through: bigint }> = [];
              const sequences = new Sequences({
                definition: (id) => definitions.find((def) => def.id === id),
                check: () => undefined,
                now: () => 100,
                logReservation(sequenceId, through) {
                  history.push({ sequenceId, lsn: ++lsn, through });
                  return lsn;
                },
              });
              const restored = BigInt(salt) * direction;
              sequences.restore(2, restored, 0);
              history.push({ sequenceId: 2, lsn: 0, through: restored });
              const out = { repr: "int64" as const, values: new BigInt64Array(count) };
              expect(typeof sequences.next(1, count, out, 0)).toBe("number");
              const oldBound = 1;
              expect(typeof sequences.set(1, BigInt(salt) * direction, false)).toBe("number");
              if (cycle) {
                expect(typeof sequences.set(1, direction > 0n ? 1000n : -1000n, true)).toBe(
                  "number",
                );
                expect(typeof sequences.next(1, count, out, 0)).toBe("number");
                expect(out.values[0]).toBe(direction > 0n ? -1000n : 1000n);
              }
              const bound = lsn;
              const expected = (at: number) =>
                definitions.map((def) => ({
                  sequenceId: def.id,
                  reservedThrough:
                    history.filter((e) => e.sequenceId === def.id && e.lsn <= at).at(-1)?.through ??
                    def.start,
                }));
              const governor = new Governor(4 << 20),
                grant = await governor.admit({
                  label: "capture",
                  minimumBytes: 4096,
                  spillable: false,
                });
              const before = history.length,
                captured = sequences.capture(bound, definitions, grant);
              try {
                expect(captured).not.toBeInstanceOf(Promise);
                expect(captured.value).toEqual(expected(bound));
                expect(Object.isFrozen(captured.value)).toBe(true);
                expect(history.length).toBe(before);
                const older = sequences.capture(oldBound, definitions, grant);
                try {
                  expect(older.value).toEqual(expected(oldBound));
                } finally {
                  older.release();
                }
                const frozen = captured.value.map((value) => ({ ...value }));
                expect(typeof sequences.next(1, count, out, 0)).toBe("number");
                sequences.checkpoint(lsn);
                sequences.drop(2);
                expect(captured.value).toEqual(frozen);
                reach.reach(direction > 0n ? "ascending" : "descending");
                if (cycle) reach.reach("cycling");
                for (const state of [
                  "setval",
                  "restored",
                  "uninitialized",
                  "read only",
                  "older horizon preserved",
                ] as const)
                  reach.reach(state);
              } finally {
                captured.release();
                captured.release();
              }
              expect(grant.allocated).toBe(0);
              reach.reach("released");
              expect(grant.allocate(grant.held - 1)).toBe(true);
              expect(() => sequences.capture(bound, definitions, grant)).toThrow(
                expect.objectContaining({ code: "MINNOW_SNAPSHOT_BUDGET" }),
              );
              expect(grant.allocated).toBe(grant.held - 1);
              reach.reach("denied");
              grant.close();
              expect(governor.usedBytes).toBe(0);
            }
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  });
