import { taskClock } from "../task.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { checkpointFtsMutations } from "../write/fts-mutation-checkpoint.js";
import { ftsPreparedEntries, readFtsJournal } from "../format/fts-journal.js";
/** The approved FTS write seam: prepaid, atomic, reference-free keyed invalidation. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, FTS_INLINE_MUTATION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { FtsMutations } from "../write/fts-mutations.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
for (const seed of seedsFor("next-fts-inline-mutations", [0xb871, 0xb872]))
  it(`prepares and checkpoints exact keyed flags without allocation at commit ${String(seed)}`, async (context) => {
    let restoreClock: (() => void) | undefined;
    context.onTestFinished(() => restoreClock?.());
    const reach = createReach("inline FTS mutations", FTS_INLINE_MUTATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(1, 2, 64, 65),
        fc.constantFrom(100, 2 ** 52 - 4),
        async (n, lsn) => {
          reach.beginHistory();
          if (lsn > 2 ** 32) reach.reach("wide commit LSN");
          let live = 0,
            cap = 1e9,
            forbidden = false;
          const budget = {
            allocate(bytes: number) {
              if (forbidden) throw new Error("allocation after WAL");
              if (live + bytes > cap) return false;
              live += bytes;
              return true;
            },
            free(bytes: number) {
              live -= bytes;
              expect(live).toBeGreaterThanOrEqual(0);
            },
          };
          const source = function* () {
            for (let i = 0; i < n; i++) {
              yield null;
              yield { kind: "ftsMutation" as const, tableId: i + 1, schemaVersion: 1 };
            }
          };
          const mutations = FtsMutations.create(budget);
          try {
            cap = 512 + 512 + (n - 1) * 512;
            await expect(mutations.prepare(source)).rejects.toBeInstanceOf(ParseBudgetError);
            expect(live).toBe(512);
            expect(mutations.bytes).toBe(live);
            reach.reach("denied before entries");
            cap = 1e9;
            const cancel = new AbortController();
            let calls = 0;
            await expect(
              mutations.prepare(function* () {
                calls++;
                for (let i = 0; i < n; i++) {
                  yield { kind: "ftsMutation" as const, tableId: i + 1, schemaVersion: 1 };
                  if (calls === 2 && i === Math.floor(n / 2))
                    cancel.abort(new Error("cancel after k"));
                  yield null;
                }
              }, cancel.signal),
            ).rejects.toThrow("cancel after k");
            expect(live).toBe(512);
            reach.reach("cancel after k");
            const first = await mutations.prepare(source);
            expect(live).toBe(mutations.bytes);
            forbidden = true;
            mutations.commit(first, lsn);
            forbidden = false;
            await mutations.release(first);
            reach.reach("constant-time R9");
            const again = await mutations.prepare(source);
            mutations.commit(again, lsn + 1);
            await mutations.release(again);
            const entries = [];
            for await (const e of mutations.pending(0, lsn + 1)) entries.push(e);
            expect(entries).toEqual(
              Array.from({ length: n }, (_, i) => ({ tableId: i + 1, schemaVersion: 1, lsn: lsn })),
            );
            reach.reach("same table idempotent");
            const space = new MemoryExtentSpace(),
              writer = space.beginWrite("inline history");
            try {
              cap = live + 4 * 1024 * 1024;
              const checkpoint = await checkpointFtsMutations(
                mutations,
                0,
                lsn + 1,
                1,
                null,
                budget,
                writer,
                space,
              );
              expect(checkpoint.lsn).toBe(lsn);
              expect(checkpoint.ref).not.toBeNull();
              const ref = checkpoint.ref;
              if (ref === null) throw new Error("Missing keyed checkpoint");
              const parsed = await readFtsJournal(ref, budget, space);
              try {
                expect(parsed.value.kind).toBe("prepared");
                if (parsed.value.kind !== "prepared") throw new Error("Expected prepared history");
                expect(parsed.value.ordinal).toBe(n > 64 ? 2 : 1);
              } finally {
                parsed.release();
              }
              const stored = [];
              for await (const e of ftsPreparedEntries(ref, budget, space))
                stored.push(structuredClone(e));
              expect(stored.sort((a, b) => a.tableId - b.tableId)).toEqual(
                Array.from({ length: n }, (_, i) => ({
                  kind: "keyed",
                  tableId: i + 1,
                  schemaVersion: 1,
                  ftsId: 0,
                  columnId: 0,
                  generation: 0,
                  mutationEpoch: lsn,
                })),
              );
              expect(live).toBe(mutations.bytes);
              reach.reach("bounded journal pages");
              if (n > 64) reach.reach("journal growth boundary");
            } finally {
              await writer.abort();
            }

            const reader = mutations.pending(0, lsn + 1);
            expect((await reader.next()).done).toBe(false);
            try {
              const extra = function* () {
                yield null;
                yield { kind: "ftsMutation" as const, tableId: n + 1, schemaVersion: 2 };
              };
              const cancelled = await mutations.prepare(extra);
              await mutations.release(cancelled);
              const resumed = await mutations.prepare(extra);
              mutations.commit(resumed, lsn + 2);
              await mutations.release(resumed);
              const observed = [];
              for await (const e of mutations.pending(0, lsn + 2)) observed.push(e);
              expect(
                observed.find((e) => e.tableId === n + 1)?.lsn,
                "a cancelled record remains reusable while an older checkpoint borrows the map",
              ).toBe(lsn + 2);
              reach.reach("cancelled record rebound");
              mutations.covered(lsn + 2);
              const borrowed = live;
              await mutations.reclaim();
              expect(live, "borrowed checkpoint must remain charged").toBe(borrowed);
              reach.reach("borrowed checkpoint fenced");
              let visits = 0;
              const spy = vi.spyOn(taskClock, "due").mockImplementation(() => {
                visits++;
                return false;
              });
              restoreClock = () => spy.mockRestore();
              try {
                for await (const _entry of reader) {
                  void _entry;
                }
              } finally {
                restoreClock();
                restoreClock = undefined;
              }
              expect(visits, "checkpoint visits only its borrowed prefix").toBe(n - 1);
              reach.reach("stable checkpoint prefix");
            } finally {
              restoreClock?.();
              restoreClock = undefined;
              await reader.return(undefined);
            }
            await mutations.reclaim();
            expect(live).toBe(512);
            reach.reach("checkpoint reclaims");
            if (n > 64) reach.reach("funded resize");
          } finally {
            forbidden = false;
            await mutations.close();
          }
          expect(live).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [65, 100],
          [65, 2 ** 52 - 4],
        ],
      },
    );
    reach.assertReached();
  });
