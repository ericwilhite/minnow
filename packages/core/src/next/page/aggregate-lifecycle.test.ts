/** R23.5: optional rescaling shares the task clock, including immediately resolved I/O. */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  createReach,
  GROUP_AGGREGATE_TASK_REACH,
  GROUP_AGGREGATE_TAIL_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { DURABLE_FORMATS } from "../durable-format.js";
import { TaskClock } from "../task.js";
import { MemoryExtentSpace } from "./memory-extents.js";
import { GroupWriter } from "./writer.js";
import { vectorOf } from "./test-support.js";

for (const seed of seedsFor("next-group-aggregate-task", [0xb2ed, 0xb2ee]))
  it(`paces wide rescaling and releases cancellation seed ${String(seed)}`, async () => {
    const reach = createReach("A1 task lifetime", GROUP_AGGREGATE_TASK_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 129, max: 256 }),
        fc.integer({ min: 9000, max: 9500 }),
        async (rows, scale) => {
          reach.beginHistory();
          for (const cancel of [false, true]) {
            let held = 0,
              ticks = 0,
              timerRan = false;
            const charge = {
              allocate(bytes: number) {
                held += bytes;
                return true;
              },
              free(bytes: number) {
                held -= bytes;
                expect(held).toBeGreaterThanOrEqual(0);
              },
            };
            const space = new MemoryExtentSpace(),
              extents = space.beginWrite("paced aggregate"),
              signal = new AbortController();
            const reason = new Error("cancel rescale");
            const writer = new GroupWriter({
              columns: [{ columnId: 1, type: { kind: "decimal", precision: null, scale: null } }],
              writer: extents,
              charge,
              maxRows: rows,
              pageBytes: 4096,
              rawPageBytes: 4096,
              clock: new TaskClock(() => ++ticks),
              signal: signal.signal,
              yieldTask: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
            });
            const timer = setTimeout(() => {
              timerRan = true;
              if (cancel) signal.abort(reason);
            }, 0);
            const source = vectorOf(
              { kind: "decimal", precision: null, scale: null },
              Array.from({ length: rows }, (_, row) => ({
                coefficient: 1n,
                special: 0,
                scale: row % 2 ? scale : 0,
              })),
              { tier: "int128" },
            );
            try {
              if (cancel) {
                await expect(writer.append([source], rows)).rejects.toBe(reason);
                expect(extents.bytes).toBe(0);
                expect(space.allocator.reserved).toBe(0);
                reach.reach("cancelled paced fold");
                reach.reach("abort before output");
                reach.reach("no partial metadata");
              } else {
                expect(await writer.append([source], rows)).toBe(rows);
                const group = await writer.finish();
                expect(group.columns[0]?.aggregateSummary).not.toBeNull();
                expect(typeof group.columns[0]?.aggregateSummary?.checksum).toBe("number");
                reach.reach("complete paced fold");
              }
              expect(timerRan).toBe(true);
              reach.reach("timer before append");
            } finally {
              clearTimeout(timer);
              await writer.abort();
            }
            expect(held).toBe(0);
            reach.reach("zero charges");
          }
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60_000);

for (const seed of seedsFor("next-group-aggregate-tail", [0xb2f1, 0xb2f2]))
  it(`holds summary bytes until stalled I/O drains seed ${String(seed)}`, async () => {
    const reach = createReach("A1 tail ownership", GROUP_AGGREGATE_TAIL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 65, max: 129 }), fc.boolean(), async (rows, explicit) => {
        reach.beginHistory();
        for (const decimal of [false, true]) {
          let held = 0,
            entered!: () => void,
            release!: () => void,
            abortSettled = false;
          const atTail = new Promise<void>((resolve) => {
            entered = resolve;
          });
          const gate = new Promise<void>((resolve) => {
            release = resolve;
          });
          const space = new MemoryExtentSpace(),
            extents = space.beginWrite("A1 tail ownership");
          const open = extents.open.bind(extents);
          extents.open = (capacity) => {
            const builder = open(capacity),
              append = builder.append.bind(builder);
            builder.append = async (parts) => {
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
                entered();
                await gate;
              }
              return append(parts);
            };
            return builder;
          };
          const signal = new AbortController(),
            reason = new Error("cancel A1 tail");
          const type = decimal
            ? { kind: "decimal" as const, precision: null, scale: 2 }
            : { kind: "int32" as const, bits: 32 as const };
          const charge = {
            allocate(bytes: number) {
              held += bytes;
              return true;
            },
            free(bytes: number) {
              held -= bytes;
              expect(held).toBeGreaterThanOrEqual(0);
            },
          };
          const writer = new GroupWriter({
            columns: [{ columnId: 1, type }],
            writer: extents,
            charge,
            maxRows: rows,
            rawPageBytes: 512,
            pageBytes: 512,
            signal: signal.signal,
          });
          const values = Array.from({ length: rows }, (_, row) =>
            decimal ? { coefficient: BigInt(row - 31), special: 0 } : row - 31,
          );
          await writer.append([vectorOf(type, values, { tier: "int128", scale: 2 })], rows);
          const finishing = writer.finish().then(
            (value) => ({ value }),
            (error: unknown) => ({ error }),
          );
          let aborting: Promise<void> | undefined;
          try {
            await Promise.race([
              atTail,
              finishing.then((result) => {
                throw new Error("No summary I/O", { cause: result });
              }),
            ]);
            reach.reach("summary append stalled");
            if (explicit) {
              aborting = writer.abort();
              void aborting.then(
                () => {
                  abortSettled = true;
                },
                () => {
                  abortSettled = true;
                },
              );
            } else signal.abort(reason);
            await new Promise<void>((resolve) => setTimeout(resolve, 0));
            expect(abortSettled).toBe(false);
            if (explicit) reach.reach("abort waits for started I/O");
            expect(held).toBeGreaterThan(0);
            reach.reach("charge held through I/O");
            release();
            const result = await finishing;
            if (!("error" in result)) throw new Error("Aborted group was returned");
            if (!explicit) expect(result.error).toBe(reason);
            else expect(result.error).toBeInstanceOf(Error);
            await aborting;
            reach.reach(explicit ? "explicit abort" : "signal cancellation");
            reach.reach("no partial group");
            expect(extents.bytes).toBe(0);
            expect(space.allocator.reserved).toBe(0);
            reach.reach("unpublished extents released");
          } finally {
            release();
            await Promise.allSettled([finishing, aborting]);
            await writer.abort();
            await extents.abort();
          }
          expect(held).toBe(0);
          reach.reach("zero charges");
          reach.reach(decimal ? "decimal" : "int32");
        }
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [65, false],
          [65, true],
        ],
      },
    );
    reach.assertReached();
  }, 60_000);
