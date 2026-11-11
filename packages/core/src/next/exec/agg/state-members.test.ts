/** State rows and exact DISTINCT membership must precede every continuation input row. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, beforeAll, afterAll } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel } from "../../ir/ir.js";
import { Governor } from "../../memory/governor.js";
import { INT, TEXT, DBL, BIG, batchesOf, CollectSink, type Row } from "../join-fixtures.js";
import { col } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { PageWriter, decodePage } from "../spill-pages.js";
import { AggregateOperator } from "./aggregate.js";
import { StateRow, StateReader } from "./state-row.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("aggregate-membership-eviction", [0xb4b9, 0xb4ba]))(
  "roundtrips scalar bytes, seen markers, bitmaps and pairs before continuation (seed %i)",
  async (seed) => {
    const reach = createReach("evicted DISTINCT membership", [
      "scalar byte arena",
      "scalar seen",
      "group bitmaps and pairs",
      "legacy pairs",
      "initially wide pairs",
      "PG exact",
      "all charges returned",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1600, max: 3400 }), async (split) => {
        reach.beginHistory();
        for (const scenario of [
          "scalar byte arena",
          "scalar seen",
          "group bitmaps and pairs",
          "legacy pairs",
          "initially wide pairs",
        ] as const) {
          const grouped =
              scenario === "group bitmaps and pairs" ||
              scenario === "legacy pairs" ||
              scenario === "initially wide pairs",
            valueType = scenario === "scalar seen" ? INT : TEXT,
            input = [INT, valueType, DBL];
          const rel: Extract<Rel, { op: "aggregate" }> = {
            op: "aggregate",
            id: 2,
            input: { op: "values", id: 1, output: [], rows: [] },
            output: [],
            groupBy: grouped ? [0] : [],
            groupingSets: null,
            aggregates: [
              {
                fn: "count",
                args: [col(1, valueType)],
                type: BIG,
                filter: null,
                distinct: true,
                orderBy: [],
              },
              {
                fn: "avg",
                args: [col(2, DBL)],
                type: DBL,
                filter: null,
                distinct: false,
                orderBy: [],
              },
            ],
          };
          const rows: Row[] = Array.from({ length: 6000 }, (_, i) => [
            i % 80,
            i % 29 === 0
              ? null
              : scenario === "scalar seen"
                ? Math.floor(i / 80) % 37
                : `v${String(Math.floor(i / 80) % 37)}`,
            i % 3 === 0 ? 1e16 : i % 3 === 1 ? 1 : -1e16,
          ]);
          const gov = new Governor(32 << 20),
            grant = await gov.admit({
              label: "membership snapshot",
              minimumBytes: 16 << 20,
              spillable: true,
            }),
            arenaA = new OutputArena(grant, 2048),
            arenaB = new OutputArena(grant, 2048),
            sinkA = new CollectSink(),
            sinkB = new CollectSink(),
            writer = new PageWriter(grant);
          const options = {
            typedLayout: false,
            packedAverages: false,
            packedIntegers: false,
            groupedDistinctBitmap: scenario !== "legacy pairs",
          };
          const a = new AggregateOperator(
              rel,
              { arena: arenaA, input, params: { slots: [] } },
              grant,
              sinkA,
              options,
            ),
            b = new AggregateOperator(
              rel,
              { arena: arenaB, input, params: { slots: [] } },
              grant,
              sinkB,
              options,
            );
          const snapshot = (row: StateRow, use: (reader: StateReader) => void) => {
            const page = writer.encode(
              { queryId: 1, run: 0, partition: 0, sequence: 0 },
              row.columns,
              1,
            );
            if (!page) throw new Error("No snapshot page");
            const decoded = decodePage(
              page,
              row.columns.map((c) => c.type),
              grant,
            );
            if ("needed" in decoded) throw new Error("No snapshot decode");
            try {
              const reader = new StateReader(decoded.columns, 0);
              use(reader);
              expect(reader.consumed).toBe(row.columns.length);
            } finally {
              grant.free(decoded.charged);
            }
          };
          try {
            // A small initial domain selects bitmaps; later groups above64 use the disjoint pair path.
            const smallPrefix = scenario === "initially wide pairs" ? 0 : 32;
            for (const batch of batchesOf(input, rows.slice(0, smallPrefix))) a.push(batch);
            for (const batch of batchesOf(input, rows.slice(smallPrefix, split), {
              batchRows: 512,
              dictionary: true,
              skip: 3,
            }))
              a.push(batch);
            const mapped: number[] = [];
            b.restoreStateGroups(a.groupCount);
            for (let group = 0; group < a.groupCount; group++) {
              const row = new StateRow(grant);
              try {
                if (grouped) {
                  const table = a.groupTable;
                  if (!table) throw new Error("Missing grouped state");
                  row.value(table.keyBytes(0, group, 1), () => table.emitKey(0, group, 1));
                }
                a.saveGroup(group, row);
                snapshot(row, (reader) => {
                  const id = grouped
                    ? (b.assignGroups({
                        length: 1,
                        columns: [reader.value()],
                        selection: null,
                      })[0] ?? 0)
                    : 0;
                  mapped[group] = id;
                  b.restoreGroup(id, reader);
                });
              } finally {
                row.close();
              }
            }
            expect(b.distinctMode(0)).toBe(a.distinctMode(0));
            if (scenario === "initially wide pairs") expect(b.distinctMode(0)).toBe(0);
            for (const value of a.distinctValues()) {
              const row = new StateRow(grant);
              try {
                a.saveMember(value.call, value.id, row);
                snapshot(row, (reader) =>
                  b.restoreDistinctValue(value.call, reader.value(), reader.row),
                );
              } finally {
                row.close();
              }
            }
            for (const member of a.distinctMembers()) {
              if (!member) continue;
              const row = new StateRow(grant);
              try {
                a.saveMember(member.call, member.id, row);
                snapshot(row, (reader) =>
                  b.restoreMember(
                    member.call,
                    mapped[member.group] ?? 0,
                    reader.value(),
                    reader.row,
                  ),
                );
              } finally {
                row.close();
              }
            }
            // Same first-seen value IDs avoid wider bitmap/arena growth at restore.
            expect([...b.distinctValues()]).toEqual([...a.distinctValues()]);
            for (const value of a.distinctValues()) {
              const original = new StateRow(grant),
                restored = new StateRow(grant);
              try {
                a.saveMember(value.call, value.id, original);
                b.saveMember(value.call, value.id, restored);
                expect(restored.columns).toEqual(original.columns);
              } finally {
                original.close();
                restored.close();
              }
            }
            for (const batch of batchesOf(input, rows.slice(split), {
              batchRows: 512,
              dictionary: true,
              skip: 3,
            })) {
              a.push(batch);
              b.push(batch);
            }
            await a.finish();
            await b.finish();
            expect(
              new Map(sinkB.rows.map((r) => [grouped ? r[0] : 0, r.slice(grouped ? 1 : 0)])),
            ).toEqual(
              new Map(sinkA.rows.map((r) => [grouped ? r[0] : 0, r.slice(grouped ? 1 : 0)])),
            );
            const values = rows
              .map(
                (r) =>
                  `(${String(r[0])},${r[1] === null ? "NULL" : valueType === TEXT ? `'${String(r[1])}'` : String(r[1])})`,
              )
              .join(",");
            const expected = await pg.query<{ k?: number; n: string }>(
              `SELECT ${grouped ? "k," : ""}count(DISTINCT v)::text n FROM (VALUES ${values}) t(k,v)${grouped ? " GROUP BY k" : ""}`,
            );
            expect(
              new Map(
                sinkB.rows.map((r) => [grouped ? Number(r[0]) : 0, Number(r[grouped ? 1 : 0])]),
              ),
            ).toEqual(new Map(expected.rows.map((r) => [r.k ?? 0, Number(r.n)])));
            reach.reach(scenario);
            reach.reach("PG exact");
          } finally {
            a.abort(undefined);
            b.abort(undefined);
            arenaA.close();
            arenaB.close();
            writer.close();
            expect(grant.allocated).toBe(0);
            grant.close();
          }
          expect(gov.usedBytes).toBe(0);
          reach.reach("all charges returned");
        }
      }),
      { seed, numRuns: 3 },
    );
    reach.assertReached();
  },
  60000,
);
