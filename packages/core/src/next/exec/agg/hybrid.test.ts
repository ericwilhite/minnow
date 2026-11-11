/** PG hash aggregation admits complete groups and stably spills only unadmitted groups. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, beforeAll, afterAll } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel } from "../../ir/ir.js";
import { StepLoans } from "../../memory/step-loans.js";
import { ReservedGrant } from "../admission.js";
import { Governor } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, TEXT, DBL, type Row } from "../join-fixtures.js";
import { I64, col, sqlLiteral } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { drive } from "../pipeline.js";
import { HybridAggregateOperator } from "./hybrid.js";
import { KeyBatch, encodeKeyBatch } from "../../keys/key-encoding.js";
import { xxHash32 } from "../../hash/xxhash32.js";
import { texts, UNCHARGED } from "../kernels/test-support.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const input = [TEXT, DBL];
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
    { fn: "avg", args: [col(1, DBL)], type: DBL, filter: null, distinct: false, orderBy: [] },
  ],
};
it.each(seedsFor("aggregate-hybrid-new-groups", [0xb4b5, 0xb4b6]))(
  "finishes resident groups and recursively spills the others in row order (seed %i)",
  async (seed) => {
    const reach = createReach("hybrid new groups", [
      "resident update",
      "new group spill",
      "later pass",
      "recursive repartition",
      "bounded job directory",
      "selected rows",
      "PG exact",
      "all charges returned",
      "native private helper lanes",
      "ordinary helper lanes",
      "random group domain",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4000, max: 6000 }),
        fc.boolean(),
        fc.boolean(),
        async (groups, reverse, privateHelpers) => {
          reach.beginHistory();
          const governor = new Governor(32 << 20),
            hashMinimum = 512 << 10,
            ioMinimum = 2 << 20;
          const grant = await governor.admit({
            label: "hybrid",
            minimumBytes: hashMinimum + ioMinimum,
            spillable: true,
          });
          const native = await governor.admitGroup({
            label: "hybrid extras",
            grants: ["hash", "io"].flatMap((name) => [
              { label: `${name}/fixed`, minimumBytes: 0, spillable: false },
              { label: `${name}/growth`, minimumBytes: 0, spillable: true },
              {
                label: `${name}/progress`,
                minimumBytes: privateHelpers ? 1 << 20 : 0,
                spillable: false,
              },
            ]),
          });
          const part = (index: number) => {
            const value = native[index];
            if (!value) throw new Error("Missing hybrid native part");
            return value;
          };
          // Production helpers hold a private progress lane even when it is NOT borrowed.
          // Its held capacity is not ordinary state headroom on the next hash pass.
          const extras = [0, 3].map(
            (offset) =>
              new ReservedGrant(
                part(offset),
                part(offset + 1),
                0,
                privateHelpers
                  ? new StepLoans(part(offset + 2), 1 << 20, "private hybrid progress")
                  : null,
                false,
                () => part(offset + 2).close(),
                true,
              ),
          );
          const filler = await governor.admit({
            label: "temporarily pinned competitor",
            minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
            spillable: false,
          });
          const spill = new MemorySpillService(),
            arena = new OutputArena(grant, 2048),
            sink = new CollectSink();
          const operator = new HybridAggregateOperator(
            rel,
            { arena, input, params: { slots: [] } },
            {
              grant,
              extras,
              hashMinimum,
              ioMinimum,
              spill,
              queryId: 100,
              pendingPasses: reverse ? 1 : 64,
            },
            sink,
          );
          const keys: string[] = [],
            encoder = new KeyBatch(UNCHARGED, Infinity);
          try {
            for (let from = 0; keys.length < groups; from += 2048) {
              const candidates = Array.from({ length: 2048 }, (_, i) => `k${String(from + i)}`),
                vector = texts(candidates);
              const encoded = encodeKeyBatch(
                "equality",
                [{ vector, direction: "asc", nulls: "last" }],
                { length: 2048, selection: null },
                encoder,
              );
              if (!encoded.done) throw new Error("Fixture key encoding failed");
              for (let i = 0; i < 2048 && keys.length < groups; i++)
                if (xxHash32(encoder.key(i)) >>> 28 === 0) keys.push(candidates[i] ?? "");
            }
          } finally {
            encoder.close();
          }
          const rows: Row[] = Array.from({ length: groups * 3 }, (_, i) => [
            keys[i % groups] ?? "",
            Math.floor(i / groups) === 0
              ? 1e16
              : Math.floor(i / groups) === 1
                ? reverse
                  ? -1e16
                  : 1
                : reverse
                  ? 1
                  : -1e16,
          ]);
          try {
            await drive(
              new ArraySource(
                batchesOf(input, rows, { batchRows: 512, skip: 3, dictionary: true }),
              ),
              operator,
            );
            const sql = `SELECT k,count(*)::text n,sum(v)::text s,avg(v)::text a FROM (VALUES ${rows.map((r) => `('${String(r[0])}',${sqlLiteral("float8", Number(r[1]))})`).join(",")}) t(k,v) GROUP BY k`;
            const expected = await pg.query<{ k: string; n: string; s: string; a: string }>(sql);
            expect(new Map(sink.rows.map((r) => [r[0], Number(r[2])]))).toEqual(
              new Map(expected.rows.map((r) => [r.k, Number(r.s)])),
            );
            expect(sink.rows).toHaveLength(expected.rows.length);
            expect(new Map(sink.rows.map((r) => [r[0], r.slice(1).map(Number)]))).toEqual(
              new Map(expected.rows.map((r) => [r.k, [Number(r.n), Number(r.s), Number(r.a)]])),
            );
            expect(operator.profile.spilledRows).toBeGreaterThan(0);
            expect(operator.profile.passes).toBeGreaterThan(0);
            if (groups === 10000) expect(operator.profile.maxLevel).toBeGreaterThanOrEqual(2);
            if (operator.profile.maxLevel >= 2) reach.reach("recursive repartition");
            if (reverse) {
              expect(operator.profile.collapsed).toBeGreaterThan(0);
              reach.reach("bounded job directory");
            }
            reach.reach("resident update");
            reach.reach("new group spill");
            reach.reach("later pass");
            reach.reach("selected rows");
            reach.reach("PG exact");
            reach.reach(privateHelpers ? "native private helper lanes" : "ordinary helper lanes");
            if (groups !== 10000) reach.reach("random group domain");
          } finally {
            operator.abort(new Error("cleanup"));
            await operator.closed;
            arena.close();
            expect(grant.allocated + extras.reduce((n, g) => n + g.allocated, 0)).toBe(0);
            for (const g of extras) g.close();
            grant.close();
            filler.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("all charges returned");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [10000, false, false],
          [10000, true, false],
          [10000, false, true],
          [10000, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  180000,
);
