/** Native writer/read integration: inspect stored records and independently re-scan values. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, GROUP_AGGREGATE_WRITER_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import type { ColumnChunk, GroupRef } from "../format/manifest.js";
import type { ByteBudget, ExtentReader } from "../storage/extents.js";
import { xxHash32 } from "../hash/xxhash32.js";
import {
  groupAggregateCoefficient,
  groupAggregateMinimum,
  readGroupAggregate,
} from "./group-aggregate.js";
import {
  defined,
  openChunk,
  readChunk,
  writeGroup,
  type DecimalValue,
  type TestValue,
} from "./test-support.js";

function funding(cap = Number.MAX_SAFE_INTEGER): ByteBudget & { held: number } {
  return {
    held: 0,
    allocate(bytes) {
      if (this.held + bytes > cap) return false;
      this.held += bytes;
      return true;
    },
    free(bytes) {
      this.held -= bytes;
      expect(this.held).toBeGreaterThanOrEqual(0);
    },
  };
}
function reference(column: ColumnChunk, rows: number): GroupRef {
  return {
    groupId: 1,
    firstRowId: 1,
    lastRowId: rows,
    rowCount: rows,
    liveRowCount: rows,
    root: column.directory,
    deletions: null,
  };
}
function valuesEqualSummary(
  values: readonly TestValue[],
  column: ColumnChunk,
  record: NonNullable<Awaited<ReturnType<typeof readGroupAggregate>>>["value"],
): void {
  const finite: Array<{ coefficient: bigint; scale: number }> = [];
  let nan = 0,
    positiveInfinity = 0,
    negativeInfinity = 0,
    nonNull = 0,
    resultScale = 0;
  for (const value of values) {
    if (value === null) continue;
    nonNull++;
    if (typeof value === "number" || typeof value === "bigint")
      finite.push({ coefficient: BigInt(value), scale: 0 });
    else {
      const decimal = value as DecimalValue;
      if (decimal.special === 1) nan++;
      else if (decimal.special === 2) positiveInfinity++;
      else if (decimal.special === 3) negativeInfinity++;
      else {
        const scale =
          decimal.scale ?? (column.type.kind === "decimal" ? (column.type.scale ?? 0) : 0);
        resultScale = Math.max(resultScale, scale);
        finite.push({ coefficient: decimal.coefficient, scale });
      }
    }
  }
  expect(record.nonNullCount).toBe(nonNull);
  const sum = record.sum;
  expect(sum).not.toBeNull();
  if (sum === null) throw new Error("Missing writer SUM capability");
  const expected = finite.reduce(
    (total, v) => total + v.coefficient * 10n ** BigInt(resultScale - v.scale),
    0n,
  );
  expect(
    groupAggregateCoefficient(sum.coefficientBytes) *
      10n ** BigInt(resultScale - sum.coefficientScale),
  ).toBe(expected);
  expect(sum.resultScale).toBe(resultScale);
  expect([sum.nanCount, sum.positiveInfinityCount, sum.negativeInfinityCount]).toEqual([
    nan,
    positiveInfinity,
    negativeInfinity,
  ]);
}
for (const seed of seedsFor("next-group-aggregate-writer", [0xb2e5, 0xb2e6]))
  it(`publishes source-bound packed summaries before payload access seed ${String(seed)}`, async () => {
    const reach = createReach("A1 stored group facts", GROUP_AGGREGATE_WRITER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.bigInt({ min: -(1n << 63n), max: (1n << 63n) - 1n }), {
          minLength: 65,
          maxLength: 200,
        }),
        fc.integer({ min: 1, max: 23 }),
        async (input, batch) => {
          reach.beginHistory();
          for (const nullable of [false, true])
            for (const mixed of [false, true]) {
              const types: PhysicalType[] = [
                { kind: "int32", bits: 16 },
                { kind: "int32", bits: 32 },
                { kind: "int64" },
                { kind: "decimal", precision: null, scale: mixed ? null : 2 },
              ];
              const columns = types.map((type) => ({
                type,
                values: input.map((value, row): TestValue => {
                  if (nullable && row % 7 === 0) return null;
                  if (type.kind === "int32") return Number(BigInt.asIntN(type.bits, value));
                  if (type.kind === "int64") return value;
                  return {
                    coefficient: value,
                    scale: mixed ? row % 8 : 2,
                    special: row < 3 ? row + 1 : 0,
                  };
                }),
                vector: { tier: "int128" as const, ...(mixed ? {} : { scale: 2 }) },
              }));
              const charged = funding();
              const { space, group } = await writeGroup(columns, {
                batch,
                charge: charged,
                pageBytes: 128,
                rawPageBytes: 512,
                inlineBytes: 128,
              });
              expect(charged.held).toBe(0);
              for (const source of group.columns) {
                expect(
                  source.aggregateSummary,
                  "writer must publish an exact supported summary",
                ).toBeTruthy();
                expect(typeof source.aggregateSummary?.checksum).toBe("number");
              }
              const summaryOwners = new Set(
                group.columns.map((c) => defined(c.aggregateSummary ?? undefined).extent),
              );
              expect(summaryOwners.size).toBe(1);
              reach.reach("packed summaries");
              for (let index = 0; index < group.columns.length; index++) {
                const column = defined(group.columns[index]);
                const ref = defined(column.aggregateSummary ?? undefined);
                const raw = space.bytes.subarray(
                  ref.extent.offset + ref.offset,
                  ref.extent.offset + ref.offset + ref.length,
                );
                expect(xxHash32(raw)).toBe(ref.checksum);
                expect(new DataView(raw.buffer, raw.byteOffset).getUint8(24)).toBe(index + 1);
                if (column.type.kind === "decimal")
                  expect(new DataView(raw.buffer, raw.byteOffset).getUint8(100)).toBe(
                    mixed ? 0 : 1,
                  );
                expect(column.statistics.nullCount).toBe(
                  nullable ? Math.ceil(input.length / 7) : 0,
                );
                expect(ref.extent).not.toBe(column.data.extent);
                const pages = space.view({ cache: false }),
                  requests: number[] = [];
                const metadata: ExtentReader = {
                  peek: () => null,
                  async read(refs, options) {
                    expect(refs).toEqual([ref]);
                    requests.push(refs.length);
                    return pages.read(refs, options);
                  },
                };
                const budget = funding(groupAggregateMinimum(column)),
                  held = await readGroupAggregate(
                    metadata,
                    reference(column, group.rowCount),
                    column,
                    { budget },
                  );
                if (held === null) throw new Error("Missing attached summary");
                expect(budget.held).toBe(groupAggregateMinimum(column));
                const observing = await openChunk(space, column);
                try {
                  expect(observing.directory.pages.length).toBeGreaterThan(1);
                  if (column.type.kind === "decimal")
                    expect(observing.directory.scale).toBe(mixed ? null : 2);
                } finally {
                  observing.close();
                }
                const decoded = await readChunk(space, column);
                valuesEqualSummary(decoded, column, held.value);
                reach.reach("source bytes");
                held.release();
                held.release();
                expect(budget.held).toBe(0);
                expect(() => held.value).toThrow();
                expect(requests).toEqual([1]);
                reach.reach("metadata only");
                for (const deleted of [
                  { ...reference(column, group.rowCount), deletions: ref },
                  { ...reference(column, group.rowCount), liveRowCount: group.rowCount - 1 },
                ]) {
                  expect(
                    await readGroupAggregate(metadata, deleted, column, { budget }),
                  ).toBeNull();
                  expect(requests).toEqual([1]);
                }
                reach.reach("bitmap fallback");
                const controller = new AbortController();
                controller.abort(new Error("cancel summary"));
                await expect(
                  readGroupAggregate(metadata, reference(column, group.rowCount), column, {
                    budget,
                    signal: controller.signal,
                  }),
                ).rejects.toThrow("cancel summary");
                expect(budget.held).toBe(0);
                expect(requests).toEqual([1]);
                // Cancellation after an exact read has started must drain and release that
                // lease before returning; the decoder must never expose an aborted record.
                const during = new AbortController();
                let entered!: () => void, resume!: () => void;
                const started = new Promise<void>((resolve) => {
                  entered = resolve;
                });
                const gate = new Promise<void>((resolve) => {
                  resume = resolve;
                });
                const stalled: ExtentReader = {
                  peek: () => null,
                  async read(refs, options) {
                    const borrowed = await pages.read(refs, options);
                    entered();
                    await gate;
                    return borrowed;
                  },
                };
                const reading = readGroupAggregate(
                  stalled,
                  reference(column, group.rowCount),
                  column,
                  { budget, signal: during.signal },
                );
                const completed = reading.then(
                  (result) => {
                    result?.release();
                    return { error: null };
                  },
                  (error: unknown) => ({ error }),
                );
                await started;
                expect(budget.held).toBe(ref.length);
                const cancelled = new Error("cancel started metadata read");
                during.abort(cancelled);
                resume();
                expect(await completed).toEqual({ error: cancelled });
                expect(budget.held).toBe(0);
                reach.reach("cancel release");
                // A checksum failure, then checksum-valid reserved/tag corruption, both before use.
                const address = ref.extent.offset + ref.offset,
                  original = space.bytes[address + 6] ?? 0;
                space.bytes[address + 6] = original ^ 1;
                await expect(
                  readGroupAggregate(pages, reference(column, group.rowCount), column, { budget }),
                ).rejects.toMatchObject({ code: "MINNOW_STORE_CORRUPT" });
                reach.reach("checksum refusal");
                const corrupt = {
                  ...column,
                  aggregateSummary: {
                    ...ref,
                    checksum: xxHash32(space.bytes.subarray(address, address + ref.length)),
                  },
                };
                await expect(
                  readGroupAggregate(pages, reference(column, group.rowCount), corrupt, { budget }),
                ).rejects.toMatchObject({ code: "MINNOW_MANIFEST_CORRUPT" });
                space.bytes[address + 6] = original;
                expect(budget.held).toBe(0);
                reach.reach("typed header refusal");
                reach.reach(
                  ["int16", "int32", "int64", "decimal"][index] as
                    "int16" | "int32" | "int64" | "decimal",
                );
              }
              reach.reach(nullable ? "nullable" : "all valid");
              reach.reach(mixed ? "mixed scale" : "fixed scale");
              reach.reach("specials");
              reach.reach("multiple pages");
              reach.reach("zero charges");
            }
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  }, 60_000);
