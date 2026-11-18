/** A complete export admits native work and transport together (governor rule2). */
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { SnapshotConnection, type OwnerSnapshotOperations } from "../owner/snapshot-connection.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_TRANSPORT_ADMISSION_REACH } from "../../testing/reach.js";

for (const seed of seedsFor("next-snapshot-transport-admission", [0xb2f3, 0xb2f4]))
  it(`complete snapshot export admission ${String(seed)}`, async () => {
    const reach = createReach("snapshot complete admission", SNAPSHOT_TRANSPORT_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("exact", "below", "oversize", "release", "cancel"),
        fc.integer({ min: 1, max: 1024 }),
        async (mode, extra) => {
          reach.beginHistory();
          const workBytes = (8 << 20) + extra,
            total = workBytes + (1 << 20) + 1024,
            cap =
              mode === "oversize"
                ? total - extra
                : mode === "exact"
                  ? total
                  : mode === "below"
                    ? total + extra
                    : 32 << 20,
            governor = new Governor(cap),
            controller = new AbortController();
          let calls = 0,
            blocker: Grant | undefined;
          const operations: OwnerSnapshotOperations & { exportMinimumBytes: number } = {
            governor,
            exportMinimumBytes: workBytes,
            async export(sink, signal, budget?: Grant) {
              calls++;
              const work =
                budget ??
                (await governor.admit({
                  label: "native work",
                  minimumBytes: workBytes,
                  spillable: true,
                  signal,
                }));
              try {
                expect(budget, "native work must use the complete admission").toBeDefined();
                expect(work.held).toBe(workBytes);
                expect(work.allocate(66)).toBe(true);
                reach.reach("native work precharged");
                await sink.write(new Uint8Array([7, 9]));
              } finally {
                if (budget === undefined) work.close();
                else work.free(66);
              }
            },
            import: async () => {
              throw new Error("unused");
            },
          };
          const connection = new SnapshotConnection(operations);
          try {
            if (mode === "oversize") {
              expect(total).toBeGreaterThan(governor.capBytes);
              await expect(
                connection.command("snapshotExportOpen", [], controller.signal),
              ).rejects.toMatchObject({
                code: "MINNOW_MEMORY_ADMISSION",
                bytes: total,
                capacity: cap,
              });
              expect(calls).toBe(0);
              reach.reach("above cap refused");
            } else if (mode === "exact" || mode === "below") {
              expect(total).toBeGreaterThan(governor.capBytes - governor.reserveBytes);
              expect(total).toBeLessThanOrEqual(governor.capBytes);
              const id = await connection.command("snapshotExportOpen", [], controller.signal);
              expect(
                await connection.command("snapshotExportRead", [id as string], controller.signal),
              ).toEqual(new Uint8Array([7, 9]));
              expect(
                await connection.command("snapshotExportRead", [id as string], controller.signal),
              ).toBeNull();
              reach.reach(mode === "exact" ? "at whole cap admitted" : "below whole cap admitted");
            } else {
              const held = governor.capBytes - total + extra;
              blocker = await governor.admit({
                label: "other operation",
                minimumBytes: held,
                spillable: false,
              });
              expect(blocker.allocate(held)).toBe(true);
              let settled = false;
              const opening = connection.command("snapshotExportOpen", [], controller.signal);
              void opening.then(
                () => {
                  settled = true;
                },
                () => {
                  settled = true;
                },
              );
              // A task boundary drains immediately-resolved governor continuations, not a timing race.
              await new Promise<void>((resolve) => setTimeout(resolve, 0));
              expect(governor.usedBytes, "a waiting export holds no partial transport grant").toBe(
                held,
              );
              expect(settled).toBe(false);
              expect(calls).toBe(0);
              reach.reach("wait retains nothing");
              if (mode === "cancel") {
                const reason = new Error("cancel queued export");
                controller.abort(reason);
                await expect(opening).rejects.toBe(reason);
                expect(governor.usedBytes).toBe(held);
                reach.reach("cancel pending admission");
              } else {
                blocker.close();
                const id = await opening;
                expect(
                  await connection.command("snapshotExportRead", [id as string], controller.signal),
                ).toEqual(new Uint8Array([7, 9]));
                expect(
                  await connection.command("snapshotExportRead", [id as string], controller.signal),
                ).toBeNull();
                reach.reach("release starts funded export");
              }
            }
          } finally {
            await connection.close();
            blocker?.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("all credit returned");
        },
      ),
      {
        seed,
        numRuns: 9,
        examples: [
          ["exact", 1],
          ["below", 1024],
          ["oversize", 1],
          ["release", 1024],
          ["cancel", 1],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
