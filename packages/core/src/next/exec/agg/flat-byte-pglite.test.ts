/** PostgreSQL GROUP BY/NULL equality is the authority, including exact spill/replay. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach, FLAT_BYTE_PG_REACH } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel } from "../../ir/ir.js";
import { Governor } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, TEXT, DBL, type Row } from "../join-fixtures.js";
import { I64, col } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { drive } from "../pipeline.js";
import { HybridAggregateOperator } from "./hybrid.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const rel: Extract<Rel, { op: "aggregate" }> = {
  op: "aggregate",
  id: 2,
  input: { op: "values", id: 1, output: [], rows: [] },
  output: [],
  groupBy: [0],
  groupingSets: null,
  aggregates: [
    { fn: "count_star", args: [], type: I64, filter: null, distinct: false, orderBy: [] },
    { fn: "sum", args: [col(1, DBL)], type: DBL, filter: null, distinct: false, orderBy: [] },
  ],
};
for (const seed of seedsFor("flat-byte-pglite", [0xb9fd1, 0xb9fd2]))
  it(`matches PG with pinned 8 MiB exact-state spill seed${String(seed)}`, async () => {
    const reach = createReach("flat byte PG spill", FLAT_BYTE_PG_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          groups: fc.integer({ min: 4500, max: 5100 }),
          width: fc.constantFrom(16, 36, 65, 80),
          selected: fc.boolean(),
          nullable: fc.boolean(),
        }),
        async ({ groups, width, selected, nullable }) => {
          reach.beginHistory();
          const rows: Row[] = Array.from({ length: groups * 3 }, (_, i) => [
            nullable && i % groups === 0
              ? null
              : String(i % groups).padStart(width, (i % groups) % 2 === 0 ? "é" : "x"),
            (i % 97) - 48,
          ]);
          const sql = `SELECT k,count(*)::text n,sum(v)::text s FROM (VALUES ${rows.map((r) => `(${r[0] === null ? "NULL" : `'${String(r[0])}'`},${String(r[1])}::float8)`).join(",")})t(k,v) GROUP BY k`;
          const expected = (await pg.query<{ k: string | null; n: string; s: string }>(sql)).rows;
          const governor = new Governor(8 << 20),
            hashMinimum = 512 << 10,
            ioMinimum = 2 << 20;
          const grant = await governor.admit({
            label: "flat byte hybrid",
            minimumBytes: hashMinimum + ioMinimum,
            spillable: true,
          });
          const extras = await governor.admitGroup({
            label: "hybrid growth lanes",
            grants: ["hash", "io"].map((label) => ({ label, minimumBytes: 0, spillable: true })),
          });
          const filler = await governor.admit({
            label: "pinned spill competitor",
            minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
            spillable: false,
          });
          expect(filler.allocate(filler.held)).toBe(true);
          const spill = new MemorySpillService(),
            arena = new OutputArena(grant, 2048),
            sink = new CollectSink();
          const operator = new HybridAggregateOperator(
            rel,
            { arena, input: [TEXT, DBL], params: { slots: [] } },
            { grant, extras, hashMinimum, ioMinimum, spill, queryId: 1 },
            sink,
          );
          try {
            await drive(
              new ArraySource(
                batchesOf([TEXT, DBL], rows, { batchRows: 512, ...(selected ? { skip: 3 } : {}) }),
              ),
              operator,
            );
            expect(new Map(sink.rows.map((r) => [r[0], r.slice(1).map(Number)]))).toEqual(
              new Map(expected.map((r) => [r.k, [Number(r.n), Number(r.s)]])),
            );
            expect(sink.rows).toHaveLength(expected.length);
            expect(operator.profile.spilledRows).toBeGreaterThan(0);
            expect(operator.profile.passes).toBeGreaterThan(0);
            expect(governor.peakBytes).toBeLessThanOrEqual(8 << 20);
            reach.reach("PG exact");
            reach.reach("pinned 8 MiB");
            reach.reach("spill");
            reach.reach("later pass");
            if (selected) reach.reach("selection");
            if (nullable) reach.reach("NULL");
            else reach.reach("non-NULL flat");
            if (width === 36) reach.reach("UUID36 width");
          } finally {
            operator.abort(new Error("test cleanup"));
            await operator.closed;
            arena.close();
            expect(grant.allocated).toBe(0);
            for (const extra of extras) {
              expect(extra.allocated).toBe(0);
              extra.close();
            }
            grant.close();
            filler.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("zero state and spill");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [{ groups: 4500, width: 36, selected: false, nullable: false }],
          [{ groups: 4501, width: 65, selected: true, nullable: true }],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
