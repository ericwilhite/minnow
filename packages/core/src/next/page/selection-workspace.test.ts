import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PAGE_SELECTION_WORKSPACE_REACH } from "../../testing/reach.js";
import type { ByteBudget } from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import { ChunkReader, PageBudgetError, ReaderClosedError } from "./reader.js";
import * as selection from "./select.js";
import { defined, writeGroup, type TestValue } from "./test-support.js";

const pg = new PGlite();
const scratchFactory = vi.spyOn(selection, "newScratch");
beforeAll(() => pg.waitReady);
afterAll(async () => {
  scratchFactory.mockRestore();
  await pg.close();
});

class Budget implements ByteBudget {
  used = 0;
  limit = Infinity;
  denials = 0;
  allocate(bytes: number): boolean {
    if (this.used + bytes > this.limit) {
      this.denials++;
      return false;
    }
    this.used += bytes;
    return true;
  }
  free(bytes: number): void {
    this.used -= bytes;
    expect(this.used).toBeGreaterThanOrEqual(0);
  }
}

it.each(seedsFor("next-page-selection-workspace", [0xb4d1, 0xb4d2]))(
  "uses workspace only for predicates that decode, with charged refusal and reuse (seed %i)",
  async (seed) => {
    const reach = createReach("page selection workspace", PAGE_SELECTION_WORKSPACE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("int32", "int64 FOR", "decimal FOR", "int64 plain", "decimal128 plain"),
        fc.constantFrom("range", "refine", "mask"),
        fc.oneof(fc.integer({ min: 129, max: 600 }), fc.integer({ min: 1025, max: 2048 })),
        fc.integer(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 8 }),
        async (kind, mode, count, salt, nullable, nullPredicate, scale) => {
          reach.beginHistory();
          const wide = kind === "decimal128 plain",
            decimal = kind === "decimal FOR" || wide,
            plain = kind === "int64 plain" || wide,
            type: PhysicalType =
              kind === "int32"
                ? { kind: "int32", bits: 32 }
                : decimal
                  ? { kind: "decimal", precision: null, scale }
                  : { kind: "int64" },
            base = salt < 0 ? -3_000_000_000_000n : 3_000_000_000_000n,
            bound = kind === "int32" ? 500_000_000n : plain ? 0n : base + 500n,
            coefficients: Array<bigint | null> = Array.from({ length: count }, (_, row) => {
              if (nullable && row % 7 === 0) return null;
              const mixed = Math.imul(row ^ salt, 0x9e37_79b1) >>> 0;
              return kind === "int32"
                ? BigInt(mixed & 0x3fff_ffff)
                : plain
                  ? (BigInt(mixed) - 2n ** 31n) * 2n ** (wide ? 80n : 24n)
                  : base + BigInt(mixed % 1000);
            }),
            values: TestValue[] = coefficients.map((v) =>
              v === null
                ? null
                : kind === "int32"
                  ? Number(v)
                  : decimal
                    ? { coefficient: v, special: 0 }
                    : v,
            ),
            written = await writeGroup(
              [
                {
                  type,
                  values,
                  vector: { safe: !plain, scale, ...(wide ? { tier: "int128" as const } : {}) },
                },
              ],
              {
                gzip: false,
                pageBytes: 8192,
                rawPageBytes: 64 * 1024,
              },
            ),
            budget = new Budget(),
            allocationsBefore = scratchFactory.mock.calls.length,
            reader = await ChunkReader.open(written.space, defined(written.group.columns[0]), {
              budget,
            });
          try {
            const [first, end] = reader.pagesInRange(0, count);
            await reader.load(Array.from({ length: end - first }, (_, k) => first + k));
            const predicate = nullPredicate
                ? { op: "isNull" as const }
                : {
                    op: "<" as const,
                    value:
                      kind === "int32"
                        ? Number(bound)
                        : decimal
                          ? { coefficient: bound, scale }
                          : bound,
                  },
              compiled = defined(reader.compilePredicate(predicate) ?? undefined),
              from = 3,
              to = count - 5,
              input = Uint16Array.from(
                { length: Math.ceil((to - from) / 2) },
                (_, k) => from + k * 2,
              ),
              positions = new Uint16Array(count),
              mask = new Uint8Array(to - from),
              expected = Array.from({ length: to - from }, (_, k) => from + k).filter((row) => {
                if (mode === "refine" && (row - from) % 2 !== 0) return false;
                const value = coefficients[row];
                return nullPredicate
                  ? value === null
                  : value !== null && value !== undefined && value < bound;
              }),
              before = budget.used,
              needsScratch = plain && !nullPredicate;
            // PostgreSQL compares exact coefficients/scales independently of the page predicate.
            const params = coefficients.map((value) =>
                value === null ? null : `${String(value)}${decimal ? `e-${String(scale)}` : ""}`,
              ),
              comparison = nullPredicate ? "v IS NULL" : `v < $${String(count + 1)}::numeric`,
              pgRows = await pg.query<{ id: number }>(
                `SELECT id FROM (VALUES ${params.map((_, row) => `(${String(row)},$${String(row + 1)}::numeric)`).join(",")}) AS t(id,v) WHERE id >= ${String(from)} AND id < ${String(to)}${mode === "refine" ? ` AND (id-${String(from)})%2=0` : ""} AND ${comparison} ORDER BY id`,
                nullPredicate
                  ? params
                  : [...params, `${String(bound)}${decimal ? `e-${String(scale)}` : ""}`],
              );
            expect(expected).toEqual(pgRows.rows.map((row) => row.id));
            const run = () => {
              if (mode === "range")
                return Array.from(
                  positions.subarray(0, reader.select(compiled, from, to, positions)),
                );
              if (mode === "refine") {
                positions.set(input);
                return Array.from(
                  positions.subarray(
                    0,
                    reader.refine(compiled, positions, 0, input.length, positions),
                  ),
                );
              }
              const hits = reader.selectMask(compiled, from, to, mask);
              const selected = Array.from(mask.keys())
                .filter((k) => mask[k] === 1)
                .map((k) => k + from);
              expect(hits).toBe(selected.length);
              return selected;
            };
            budget.limit = before;
            if (needsScratch) {
              expect(run).toThrow(PageBudgetError);
              expect(budget.used).toBe(before);
              expect(budget.denials).toBe(1);
              expect(scratchFactory.mock.calls.length).toBe(allocationsBefore);
              reach.reach("fallback refused before allocation");
              budget.limit = Infinity;
            }
            expect(run()).toEqual(expected);
            if (needsScratch) {
              const rows = Math.min(
                4096,
                Math.max(...reader.directory.pages.map((p) => p.rowCount)),
              );
              expect(budget.used - before).toBe(selection.scratchBytes(rows));
              expect(scratchFactory.mock.calls.length - allocationsBefore).toBe(1);
              reach.reach("fallback charged");
            } else {
              expect(budget.used).toBe(before);
              expect(budget.denials).toBe(0);
              expect(scratchFactory.mock.calls.length).toBe(allocationsBefore);
              reach.reach("encoded without workspace");
            }
            const held = budget.used;
            expect(run()).toEqual(expected);
            expect(budget.used).toBe(held);
            expect(scratchFactory.mock.calls.length - allocationsBefore).toBe(needsScratch ? 1 : 0);
            reach.reach("workspace reused");
            reader.close();
            expect(budget.used).toBe(0);
            expect(() => reader.select(compiled, from, to, positions)).toThrow(ReaderClosedError);
            expect(budget.used).toBe(0);
            reach.reach("closed and released");
            reach.reach(kind);
            reach.reach(mode);
            if (reader.directory.pages.length > 1) reach.reach("multiple pages");
            if (nullable) reach.reach("nullable");
            if (nullPredicate) reach.reach("null predicate");
          } finally {
            reader.close();
          }
        },
      ),
      { seed, numRuns: 80 },
    );
    reach.assertReached();
  },
  60_000,
);
