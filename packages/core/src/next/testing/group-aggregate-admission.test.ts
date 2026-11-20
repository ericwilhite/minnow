/** A1 optional facts may be omitted under pressure; ordinary page output stays exact.
 * Governor rules2/4 and ByteCharge.tryAllocate: never request mandatory admission for them.
 * Real grants occupy the whole ordinary cap. Controlled boundaries model another operation
 * using the metadata grant's idle credit; no synthetic allocation refusal replaces the governor.
 */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, GROUP_AGGREGATE_ADMISSION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import * as codec from "../format/group-aggregate.js";
import { Governor } from "../memory/governor.js";
import { AggregateFold } from "../page/aggregate-fold.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { GroupWriter } from "../page/writer.js";
import { defined, readChunk, vectorOf, writeGroup, type TestValue } from "../page/test-support.js";
import type { PhysicalType } from "../types.js";
import { MetadataBudget } from "../write/budget.js";

const modes = ["state", "arithmetic", "encode", "pending", "ample"] as const;
for (const seed of seedsFor("next-group-aggregate-admission", [0xb2ef, 0xb2f0]))
  it(`keeps optional facts out of mandatory pressure seed ${String(seed)}`, async () => {
    const reach = createReach("A1 real governor admission", GROUP_AGGREGATE_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 65, max: 129 }),
        fc.integer({ min: 1, max: 31 }),
        fc.boolean(),
        async (rows, salt, nullable) => {
          reach.beginHistory();
          for (const decimal of [false, true]) {
            const type: PhysicalType = decimal
              ? { kind: "decimal", precision: null, scale: null }
              : { kind: "int32", bits: 32 };
            const values: TestValue[] = Array.from({ length: rows }, (_, row) =>
              nullable && row % 7 === 0
                ? null
                : decimal
                  ? { coefficient: BigInt(row - salt), scale: row % 2, special: 0 }
                  : row - salt,
            );
            const baseline = await writeGroup([{ type, values, vector: { tier: "int128" } }], {
              rawPageBytes: 512,
              pageBytes: 512,
              inlineBytes: 64,
            });
            const expected = defined(baseline.group.columns[0]);
            for (const mode of modes) {
              if (mode === "arithmetic" && !decimal) continue;
              const governor = new Governor(4 << 20);
              const grant = await governor.admit({
                label: "A1 metadata step",
                minimumBytes: (mode === "arithmetic" ? 96 : 512) << 10,
                spillable: false,
              });
              const holder = await governor.admit({
                label: "competing retained operation",
                minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
                spillable: false,
              });
              const budget = new MetadataBudget(grant, () => 0, governor.capBytes);
              const attempt = budget.tryAllocate.bind(budget);
              let declined = 0,
                competing = 0;
              budget.tryAllocate = (bytes) => {
                const accepted = attempt(bytes);
                if (!accepted) declined++;
                return accepted;
              };
              const occupy = () => {
                expect(competing).toBe(0);
                competing = grant.held - grant.allocated;
                expect(competing).toBeGreaterThan(0);
                expect(grant.allocate(competing)).toBe(true);
              };
              const release = () => {
                grant.free(competing);
                competing = 0;
              };
              const create = AggregateFold.create.bind(AggregateFold);
              const encode = codec.tryEncodeGroupAggregate;
              if (mode === "state")
                vi.spyOn(AggregateFold, "create").mockImplementation((...args) => {
                  occupy();
                  try {
                    return create(...args);
                  } finally {
                    release();
                  }
                });
              if (mode === "encode" || mode === "pending")
                vi.spyOn(codec, "tryEncodeGroupAggregate").mockImplementation((...args) => {
                  if (mode === "encode") {
                    occupy();
                    try {
                      return encode(...args);
                    } finally {
                      release();
                    }
                  }
                  const result = encode(...args);
                  expect(result).not.toBeNull();
                  occupy();
                  return result;
                });
              const space = new MemoryExtentSpace();
              let writer: GroupWriter | undefined;
              try {
                writer = new GroupWriter({
                  columns: [{ columnId: 1, type }],
                  writer: space.beginWrite("optional A1"),
                  charge: budget,
                  maxRows: rows,
                  rawPageBytes: 512,
                  pageBytes: 512,
                  inlineBytes: 64,
                });
                expect(
                  await writer.append([vectorOf(type, values, { tier: "int128" })], rows),
                ).toBe(rows);
                const group = await writer.finish(),
                  actual = defined(group.columns[0]);
                if (mode === "ample") {
                  expect(actual.aggregateSummary).not.toBeNull();
                  reach.reach("ample record");
                } else {
                  expect(declined).toBeGreaterThan(0);
                  expect(actual.aggregateSummary).toBeNull();
                  reach.reach("real governor blocked optional");
                  reach.reach(`${mode} omitted`);
                }
                const bytes = space.bytes.subarray(
                  actual.data.extent.offset + actual.data.offset,
                  actual.data.extent.offset + actual.data.offset + actual.data.length,
                );
                const source = baseline.space.bytes.subarray(
                  expected.data.extent.offset + expected.data.offset,
                  expected.data.extent.offset + expected.data.offset + expected.data.length,
                );
                expect(bytes).toEqual(source);
                reach.reach("ordinary bytes identical");
                expect(await readChunk(space, actual)).toEqual(
                  await readChunk(baseline.space, expected),
                );
                reach.reach("ordinary values identical");
                expect(grant.held).toBe((mode === "arithmetic" ? 96 : 512) << 10);
              } finally {
                vi.restoreAllMocks();
                await writer?.abort();
                release();
                expect(grant.allocated).toBe(0);
                holder.close();
                grant.close();
              }
              expect(governor.usedBytes).toBe(0);
              reach.reach("zero charges");
            }
            reach.reach(decimal ? "decimal" : "int32");
            reach.reach(nullable ? "NULL payload" : "all valid");
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [65, 1, false],
          [65, 1, true],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
