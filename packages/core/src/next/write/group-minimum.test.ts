/** Governor rule3: an impossible complete column group never joins admission. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITE_GROUP_MINIMUM_REACH,
  WRITE_GROUP_FITTING_REACH,
  WRITE_GROUP_FORECAST_REACH,
  WRITE_GROUP_TYPES_REACH,
} from "../../testing/reach.js";
import { Governor, MemoryAdmissionError } from "../memory/governor.js";
import { GroupWriter, groupWriterMinimum } from "../page/writer.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { readChunk, sameValue, vectorOf } from "../page/test-support.js";
import { WriteCredits } from "./credits.js";
import { MetadataBudget } from "./budget.js";

for (const seed of seedsFor("next-write-group-minimum", [47203, 47204]))
  it(`refuses an impossible aggregate workspace before allocating or queueing (seed ${String(seed)})`, async () => {
    const reach = createReach("whole group minimum", WRITE_GROUP_MINIMUM_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 60, max: 80 }), async (width) => {
        reach.beginHistory();
        const governor = new Governor(2 << 20);
        const grant = await governor.admit({
          label: "group metadata",
          minimumBytes: 0,
          spillable: true,
        });
        let queued = 0,
          allocations = 0;
        const allocate = grant.allocate.bind(grant);
        grant.allocate = (bytes) => {
          allocations++;
          return allocate(bytes);
        };
        const budget = new MetadataBudget(
          grant,
          () => 0,
          governor.capBytes,
          async () => {
            queued++;
            // Stop the parent's impossible admission immediately, not through a watchdog.
            throw new Error("Impossible complete group attempted to queue");
          },
        );
        const space = new MemoryExtentSpace();
        const writer = space.beginWrite("oversize column group");
        let group: GroupWriter | undefined;
        try {
          const result = await Promise.resolve()
            .then(() =>
              budget.retry(
                () => {
                  group = new GroupWriter({
                    columns: Array.from({ length: width }, (_, columnId) => ({
                      columnId,
                      type: { kind: "text" as const },
                    })),
                    writer,
                    charge: budget,
                  });
                  return group;
                },
                () => undefined,
              ),
            )
            .then(
              () => null,
              (error: unknown) => error,
            );
          expect(result).toMatchObject({ sqlState: "53200" });
          expect(queued).toBe(0);
          expect(allocations).toBe(0);
          reach.reach("typed whole-group refusal");
          reach.reach("no admission queued");
          reach.reach("refused before any buffer allocation");
        } finally {
          await group?.abort();
          await writer.abort();
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 8, endOnFailure: true },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-group-fitting", [47205, 47206]))
  it(`writes fitting wide short-value groups within their complete preflight (seed ${String(seed)})`, async () => {
    const reach = createReach("fitting complete group", WRITE_GROUP_FITTING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 60, max: 80 }),
        fc.integer({ min: 1, max: 32 }),
        fc.integer({ min: 0, max: 256 }),
        async (width, rows, length) => {
          reach.beginHistory();
          const governor = new Governor(4 << 20);
          const grant = await governor.admit({
            label: "fitting wide group",
            minimumBytes: 0,
            spillable: true,
          });
          const budget = new MetadataBudget(grant, () => 0, governor.capBytes);
          const space = new MemoryExtentSpace();
          const writer = space.beginWrite("fitting wide group");
          const bytes = new Uint8Array(length).fill(65);
          const values = Array.from({ length: rows }, (_, row) => (row % 3 === 1 ? null : bytes));
          const vector = vectorOf({ kind: "text" }, values);
          const columns = Array.from({ length: width }, (_, columnId) => ({
            columnId,
            type: { kind: "text" as const },
            valueBytes: length * rows,
          }));
          const options = { columns, maxRows: rows, writer, charge: budget };
          const paid: number[] = [];
          const allocate = grant.allocate.bind(grant);
          grant.allocate = (bytes) => {
            paid.push(bytes);
            return allocate(bytes);
          };
          const open = writer.open.bind(writer);
          let chargedTails = 0;
          writer.open = (capacity) => {
            const builder = open(capacity),
              append = builder.append.bind(builder);
            builder.append = (parts) => {
              for (const part of parts) {
                if (part[0] === 77 && part[1] === 80 && part[2] === 68 && part[3] === 49) {
                  expect(
                    paid.slice(-2),
                    "sketch and exact directory prepaid before append",
                  ).toEqual([7, part.length]);
                  chargedTails++;
                }
              }
              return append(parts);
            };
            return builder;
          };
          const minimum = groupWriterMinimum(options);
          expect(minimum).toBeLessThan(governor.capBytes);
          let emitting = false,
            lateGrowth = 0;
          const grow = grant.tryGrow.bind(grant);
          grant.tryGrow = (bytes) => {
            if (emitting) lateGrowth++;
            return grow(bytes);
          };
          const group = new GroupWriter(options);
          const releases: number[] = [];
          const free = grant.free.bind(grant);
          grant.free = (bytes) => {
            if (bytes > 0 && bytes === grant.allocated)
              releases.push(group.scratch.byteLength + group.limbScratch.byteLength);
            free(bytes);
          };
          try {
            expect(group.retainedBytes).toBeLessThan(width * 2048 + length * rows * width);
            reach.reach("actual short-value buffers");
            emitting = true;
            await group.append(
              Array.from({ length: width }, () => vector),
              rows,
            );
            const written = await group.finish();
            emitting = false;
            expect(group.peakWorkingBytes).toBeLessThanOrEqual(minimum);
            expect(lateGrowth).toBe(0);
            expect(chargedTails).toBe(width);
            reach.reach("serialized tails charged before append");
            reach.reach("no emit-step growth");
            expect(releases).toEqual([0]);
            reach.reach("shared buffers dropped before refund");
            for (const chunk of written.columns) {
              const actual = await readChunk(space, chunk);
              expect(actual.length).toBe(values.length);
              expect(actual.every((value, row) => sameValue(value, values[row] ?? null))).toBe(
                true,
              );
            }
            expect(grant.allocated).toBe(0);
            reach.reach("exact stored rows");
          } finally {
            await group.abort();
            await writer.abort();
            grant.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [60, 1, 0],
          [80, 32, 256],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-group-forecast", [47211, 47212]))
  it(`refuses an above-cap step forecast before native growth (seed ${String(seed)})`, async () => {
    const reach = createReach("native group forecast", WRITE_GROUP_FORECAST_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 1024 * 1024 }), async (extra) => {
        reach.beginHistory();
        const governor = new Governor(2 << 20);
        const credits = await WriteCredits.open(governor, {
          rows: 0,
          metadata: 0,
          commits: 0,
          merge: 0,
        });
        const admit = governor.admitGroup.bind(governor);
        let queued = 0;
        governor.admitGroup = (options) => {
          queued++;
          return admit(options);
        };
        let pending: Promise<void> | undefined,
          error: unknown = null;
        try {
          try {
            const result = credits.prepareMetadata(governor.capBytes + extra);
            if (result instanceof Promise) {
              pending = result;
              void result.catch(() => undefined);
            }
          } catch (cause) {
            error = cause;
          }
          expect(error, "complete forecast must refuse before attempting admission").toBeInstanceOf(
            MemoryAdmissionError,
          );
          expect(error).toMatchObject({ sqlState: "53200", capacity: governor.capBytes });
          expect(queued).toBe(0);
          reach.reach("above whole-cap typed refusal");
          reach.reach("no admission attempted");
        } finally {
          credits.cancel(new Error("forecast history ended"));
          if (pending) await Promise.allSettled([pending]);
          credits.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("zero final charge");
      }),
      { seed, numRuns: 8, endOnFailure: true },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-write-group-types", [47213, 47214]))
  it(`preflights mixed physical buffers and charged tails (seed ${String(seed)})`, async () => {
    const reach = createReach("mixed physical group minimum", WRITE_GROUP_TYPES_REACH);
    const kinds = [
      [{ kind: "bool" }, true],
      [{ kind: "int32", bits: 32 }, -2147483648],
      [{ kind: "int64" }, -(1n << 62n)],
      [{ kind: "float32" }, -0],
      [{ kind: "float64" }, Number.NaN],
      [
        { kind: "decimal", precision: null, scale: null },
        { coefficient: 1n << 100n, special: 0, scale: 5 },
      ],
      [{ kind: "interval" }, { months: -1, days: 2, micros: 1n << 60n }],
      [{ kind: "uuid" }, new Uint8Array(16).fill(99)],
      [{ kind: "text" }, new Uint8Array(13).fill(65)],
    ] satisfies Array<
      [import("../types.js").PhysicalType, import("../page/test-support.js").TestValue]
    >;
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4096 }), async (rows) => {
        reach.beginHistory();
        const governor = new Governor(16 << 20);
        const grant = await governor.admit({
          label: "mixed group",
          minimumBytes: 0,
          spillable: true,
        });
        const budget = new MetadataBudget(grant, () => 0, governor.capBytes);
        const space = new MemoryExtentSpace(),
          writer = space.beginWrite("mixed group");
        const values = kinds.map(([, value]) =>
          Array.from({ length: rows }, (_, row) => (row % 3 === 1 ? null : value)),
        );
        const columns = kinds.map(([type], columnId) => ({
          columnId,
          type,
          ...(type.kind === "text"
            ? {
                valueBytes: 13 * rows,
                maxValueBytes: 13,
                dictionaryValueBytes: 13 * rows,
                largeValues: 0,
              }
            : {}),
        }));
        const options = { columns, maxRows: rows, writer, charge: budget };
        const group = new GroupWriter(options),
          held = grant.held;
        try {
          await group.append(
            kinds.map(([type], i) => vectorOf(type, values[i] ?? [], { tier: "int128" })),
            rows,
          );
          const output = await group.finish();
          expect(group.peakWorkingBytes).toBeLessThanOrEqual(groupWriterMinimum(options));
          expect(grant.held).toBe(held);
          reach.reach("all physical buffers within preflight");
          if (rows === 1) reach.reach("single row");
          if (rows === 4096) reach.reach("multiple pages");
          for (let i = 0; i < output.columns.length; i++) {
            const chunk = output.columns[i];
            if (!chunk) throw new Error("Missing output column");
            const actual = await readChunk(space, chunk),
              expected = values[i] ?? [];
            expect(actual.length).toBe(expected.length);
            expect(
              actual.every((value, row) => sameValue(value, expected[row] ?? null)),
              `physical column ${String(i)}`,
            ).toBe(true);
          }
        } finally {
          await group.abort();
          await writer.abort();
          grant.close();
        }
        expect(governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 8, examples: [[1], [4096]], endOnFailure: true },
    );
    reach.assertReached();
  });
