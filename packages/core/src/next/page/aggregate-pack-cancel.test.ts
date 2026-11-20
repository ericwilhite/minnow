/** GroupWriter abort drains started I/O, but cannot start a new summary pack after it.
 * Authority: GroupWriter.ensureOpen's continuation/lifetime contract and R23.5.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, GROUP_AGGREGATE_PACK_CANCEL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { DURABLE_FORMATS } from "../durable-format.js";
import { MemoryExtentSpace } from "./memory-extents.js";
import { vectorOf } from "./test-support.js";
import { GroupWriter } from "./writer.js";

for (const seed of seedsFor("next-group-aggregate-pack-cancel", [0xb2f8, 0xb2f9]))
  it(`does not start a new pack after a cancelled seal seed ${String(seed)}`, async () => {
    const reach = createReach("A1 pack switch cancellation", GROUP_AGGREGATE_PACK_CANCEL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 18, max: 22 }),
        fc.integer({ min: 9000, max: 9400 }),
        async (columns, scale) => {
          reach.beginHistory();
          for (const boundary of ["seal", "open"] as const)
            for (const explicit of [false, true]) {
              const space = new MemoryExtentSpace(),
                extents = space.beginWrite("A1 pack cancellation");
              let held = 0,
                cancelled = false,
                records = 0,
                lateAppends = 0,
                lateOpens = 0,
                firstSealed = false,
                openCancelled = false,
                entered!: () => void,
                resume!: () => void;
              const atSeal = new Promise<void>((resolve) => {
                entered = resolve;
              });
              const gate = new Promise<void>((resolve) => {
                resume = resolve;
              });
              const signal = new AbortController(),
                reason = new Error("cancel aggregate pack");
              let aborting: Promise<void> | undefined;
              const cancel = () => {
                cancelled = true;
                if (explicit) aborting = writer.abort();
                else signal.abort(reason);
              };
              const open = extents.open.bind(extents);
              extents.open = (capacity) => {
                if (cancelled) lateOpens++;
                const builder = open(capacity),
                  append = builder.append.bind(builder),
                  seal = builder.seal.bind(builder);
                if (firstSealed && boundary === "open" && !cancelled) {
                  openCancelled = true;
                  cancel();
                }
                let aggregate = false;
                builder.append = (parts) => {
                  if (
                    parts.some(
                      (part) =>
                        part.length >= 4 &&
                        new DataView(part.buffer, part.byteOffset, part.byteLength).getUint32(
                          0,
                          true,
                        ) === DURABLE_FORMATS.groupAggregate.magic,
                    )
                  ) {
                    aggregate = true;
                    records++;
                    if (cancelled) lateAppends++;
                  }
                  return append(parts);
                };
                builder.seal = async () => {
                  if (aggregate && !cancelled) {
                    entered();
                    await gate;
                  }
                  const result = await seal();
                  if (aggregate) firstSealed = true;
                  return result;
                };
                return builder;
              };
              const type = { kind: "decimal", precision: null, scale: null } as const;
              const writer = new GroupWriter({
                columns: Array.from({ length: columns }, (_, k) => ({ columnId: k + 1, type })),
                writer: extents,
                rawPageBytes: 512,
                pageBytes: 512,
                signal: signal.signal,
                charge: {
                  allocate(bytes) {
                    held += bytes;
                    return true;
                  },
                  free(bytes) {
                    held -= bytes;
                    expect(held).toBeGreaterThanOrEqual(0);
                  },
                },
              });
              await writer.append(
                Array.from({ length: columns }, () =>
                  vectorOf(
                    type,
                    [
                      { coefficient: 1n, special: 0, scale: 0 },
                      { coefficient: 1n, special: 0, scale },
                    ],
                    { tier: "int128" },
                  ),
                ),
                2,
              );
              const finishing = writer.finish().then(
                (value) => ({ value }),
                (error: unknown) => ({ error }),
              );
              try {
                await Promise.race([
                  atSeal,
                  finishing.then((result) => {
                    throw new Error("No pack boundary", { cause: result });
                  }),
                ]);
                expect(records).toBeGreaterThan(0);
                expect(records).toBeLessThan(columns);
                reach.reach("multiple-pack demand");
                reach.reach("blocked first seal");
                if (boundary === "seal") cancel();
                resume();
                const result = await finishing;
                if (!("error" in result)) throw new Error("Partial group was returned");
                if (!explicit) expect(result.error).toBe(reason);
                if (aborting !== undefined) await aborting;
                expect(lateOpens, "cancelled seal must not open another pack").toBe(0);
                if (boundary === "open") {
                  expect(openCancelled).toBe(true);
                  reach.reach("cancel during next open");
                }
                expect(lateAppends, "cancelled pack must not start another append").toBe(0);
                reach.reach("no new opens");
                reach.reach("no new appends");
                reach.reach("no partial group");
                reach.reach(explicit ? "explicit abort" : "signal cancellation");
                expect(extents.bytes).toBe(0);
                expect(space.allocator.reserved).toBe(0);
              } finally {
                resume();
                await Promise.allSettled([finishing, aborting]);
                await writer.abort();
                await extents.abort();
              }
              expect(held).toBe(0);
              reach.reach("zero charges");
            }
        },
      ),
      { seed, numRuns: 4, examples: [[18, 9000]] },
    );
    reach.assertReached();
  }, 60_000);
