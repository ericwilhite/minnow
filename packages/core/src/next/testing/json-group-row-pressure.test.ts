/** Wide representatives are file-backed between charged semantic comparison steps. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, JSON_GROUP_ROW_PRESSURE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { JsonGroupRow } from "../exec/agg/json-group-row.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { JsonGroupKeys, jsonGroupWorkspaceMultiplier } from "../exec/agg/json-group-keys.js";
import { keysEqual, operandBytes, type SortColumn } from "../exec/sort-keys.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const order: readonly SortColumn[] = [
  { column: 0, direction: "asc", nulls: "last", collation: null },
];
function vector(type: PhysicalType, text: string | Uint8Array): FlatVector {
  const bytes = typeof text === "string" ? new TextEncoder().encode(text) : text;
  return {
    form: "flat",
    type,
    length: 1,
    validity: null,
    data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
  };
}
it.each(seedsFor("next-json-group-row-pressure", [0xb72e, 0xb72f]))(
  "retires wide key loans before each semantic step (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON wide group-key steps", JSON_GROUP_ROW_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 600000, max: 620000 }),
        fc.constantFrom("jsonb", "text", "array"),
        fc.boolean(),
        fc.boolean(),
        async (width, kind, utf8, cancel) => {
          reach.beginHistory();
          const semantic = kind !== "text";
          const element: PhysicalType = { kind: "interval" };
          const type: PhysicalType =
            kind === "jsonb"
              ? { kind: "json", binary: true }
              : kind === "array"
                ? { kind: "array", element }
                : { kind: "text" };
          const payload =
            kind === "array" ? "" : (utf8 ? "汉" : "x").repeat(Math.floor(width / (utf8 ? 3 : 1)));
          const count = Math.floor(width / 16);
          const left =
            kind === "array"
              ? "{" +
                Array.from({ length: count }, (_, i) =>
                  i === count - 1 ? "NULL" : '"1 mon"',
                ).join(",") +
                "}"
              : `{"s":"${payload}","n":1.0}`;
          const right =
            kind === "array"
              ? "{" +
                Array.from({ length: count }, (_, i) =>
                  i === count - 1 ? "NULL" : '"30 days"',
                ).join(",") +
                "}"
              : `{"s":"${payload}","n":1.00}`;
          const encodeIntervals = (months: number, days: number) =>
            encodeArrayValue(element, {
              dimensions: [count],
              lowerBounds: [1],
              elements: Array.from({ length: count }, (_, i) =>
                i === count - 1 ? null : { months, days, micros: 0n },
              ),
            });
          const a = vector(type, kind === "array" ? encodeIntervals(1, 0) : left);
          const b = vector(type, kind === "array" ? encodeIntervals(0, 30) : right),
            types = [type];
          const pgType = kind === "array" ? "interval[]" : kind;
          const expected = (
            await pg.query<{ eq: boolean }>(`SELECT $1::${pgType}=$2::${pgType} eq`, [left, right])
          ).rows[0]?.eq;
          const governor = new Governor((kind === "array" ? 512 : 256) << 20),
            spill = new MemorySpillService(),
            controller = new AbortController();
          const grant = await governor.admit({
            label: "wide group key",
            minimumBytes: 768 << 10,
            spillable: true,
          });
          const pool = new Pool(new Charge(grant), 768 << 10),
            pages = new PageWriter(pool, 16384);
          const files = new SpillFiles(spill, 1, {
            allocate: () => false,
            free: () => {
              throw new Error("No page cache");
            },
          });
          const io = new SpillIo(
            pool,
            files,
            pages,
            { grant, queryId: 1, signal: controller.signal },
            () => controller.signal.throwIfAborted(),
          );
          const row = new JsonGroupRow(types, order, pool, io, grant, spill, controller.signal);
          const blocker = await governor.admit({
            label: "competing retained credit",
            minimumBytes: 0,
            spillable: true,
          });
          expect(
            blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
          ).toBe(true);
          try {
            await row.capture([a], 0);
            expect(row.fileBacked).toBe(true);
            expect(pool.owes, "a representative must not pin progress between rows").toBe(false);
            reach.reach("observed spilled representative");
            reach.reach("no retained key loan");
            const helper = new JsonGroupKeys(
              types,
              order,
              pool,
              grant,
              controller.signal,
              async () => controller.signal.throwIfAborted(),
            );
            const extra = semantic ? row.operandBytes + operandBytes(types, order, [b], 0, 1) : 0;
            const result = { equal: false };
            await row.read(extra, async (columns) => {
              result.equal = semantic
                ? await helper.equal(columns, 0, [b], 0)
                : keysEqual(types, order, columns, 0, [b], 0);
            });
            expect(result.equal).toBe(expected);
            expect(pool.loans).toBeGreaterThan(0);
            expect(pool.owes).toBe(false);
            reach.reach("observed loan repaid");
            const encoded = Math.max(
              a.data.layout === "varlen" ? a.data.bytes.length : 0,
              b.data.layout === "varlen" ? b.data.bytes.length : 0,
            );
            expect(
              pool.largestLoan,
              "the compiler's row envelope must fund the observed compound loan",
            ).toBeLessThanOrEqual(4 * jsonGroupWorkspaceMultiplier(types, [0]) * encoded);
            reach.reach(
              kind === "array"
                ? "semantic interval array key"
                : kind === "jsonb"
                  ? "semantic JSONB key"
                  : "byte text key",
            );
            reach.reach("observed loan within compiled envelope");
            if (kind !== "array") reach.reach(utf8 ? "multibyte key" : "ASCII key");
            if (cancel) {
              const reason = new Error("cancel key restore");
              controller.abort(reason);
              await expect(
                row.read(extra, async () => {
                  throw new Error("An aborted key restore visited its row");
                }),
              ).rejects.toBe(reason);
              reach.reach("cancel next restore");
            } else {
              await row.read(0, async (columns) =>
                expect(keysEqual(types, order, columns, 0, [a], 0)).toBe(true),
              );
              reach.reach("restore for output");
            }
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            reach.reach("exact PG equality");
          } finally {
            await row.close();
            await files.closeAll();
            pages.close();
            pool.close();
            grant.close();
            blocker.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("zero resources");
        },
      ),
      {
        seed,
        // Retain all four existing examples; add two array histories and two random ones.
        numRuns: 8,
        examples: [
          [600000, "jsonb", false, false],
          [600000, "jsonb", true, true],
          [600000, "text", true, false],
          [600000, "text", false, true],
          [600000, "array", false, false],
          [600000, "array", false, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
