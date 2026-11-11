/** Evicted transition fields and membership restore before the continuation. Native I/O
 * buffers are dropped between phases, so the saved files do not pin the previous minimum.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, beforeAll, afterAll } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel, AggregateCall } from "../../ir/ir.js";
import { Governor, type Grant } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import {
  BIG,
  DBL,
  INT,
  NUM,
  ANYNUM,
  TEXT,
  batchesOf,
  CollectSink,
  type Row,
} from "../join-fixtures.js";
import { col, sqlLiteral } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { PageWriter, SpillFiles } from "../spill-pages.js";
import { Charge, Pool, SpillIo } from "../spill-pool.js";
import { AggregateOperator } from "./aggregate.js";
import { GroupCheckpoint } from "./checkpoint.js";
import { TapeDirectory, HashTape } from "./hash-tape.js";
import { RestoredAggregateOperator, restoredMinimum, restoredLoans } from "./restored.js";
import type { HybridPhase } from "./hybrid.js";
import { ParameterCheckpoint } from "../parameter-checkpoint.js";
import { ReservedGrant } from "../admission.js";
import { ColumnStore } from "../column-store.js";
import { StepLoans } from "../../memory/step-loans.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});

it.each(seedsFor("aggregate-checkpoint-pass", [0xb4bf, 0xb4c0]))(
  "closes old buffers and restores exact fields and membership before continuation (seed %i)",
  async (seed) => {
    const reach = createReach("checkpoint pass", [
      "scalar",
      "grouped",
      "restored prefix",
      "membership before suffix",
      "native I/O handoff",
      "PG exact floats",
      "all charges returned",
      "stable pressure merge",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 75, max: 130 }), fc.boolean(), async (split, stable) => {
        reach.beginHistory();
        for (const grouped of [false, true]) {
          const input = [DBL, TEXT, INT, BIG, NUM],
            calls: AggregateCall[] = [
              { fn: "count_star", args: [], type: BIG, distinct: false, filter: null, orderBy: [] },
              ...(["sum", "avg"] as const).map((fn): AggregateCall => ({
                fn,
                args: [col(0, DBL)],
                type: DBL,
                distinct: false,
                filter: null,
                orderBy: [],
              })),
              {
                fn: "count",
                args: [col(1, TEXT)],
                type: BIG,
                distinct: true,
                filter: null,
                orderBy: [],
              },
              ...(["sum", "avg"] as const).map((fn): AggregateCall => ({
                fn,
                args: [col(3, BIG)],
                type: ANYNUM,
                distinct: false,
                filter: null,
                orderBy: [],
              })),
              ...(["sum", "avg"] as const).map((fn): AggregateCall => ({
                fn,
                args: [col(4, NUM)],
                type: ANYNUM,
                distinct: false,
                filter: null,
                orderBy: [],
              })),
              {
                fn: "min",
                args: [col(1, TEXT)],
                type: TEXT,
                distinct: false,
                filter: null,
                orderBy: [],
              },
            ],
            rel: Extract<Rel, { op: "aggregate" }> = {
              id: 2,
              op: "aggregate",
              input: { id: 1, op: "values", output: [], rows: [] },
              output: [],
              groupBy: grouped ? [2] : [],
              groupingSets: null,
              aggregates: calls,
            },
            rows: Row[] = Array.from({ length: 211 }, (_, i) => [
              i % 3 === 0 ? 1e16 : i % 3 === 1 ? 1 : -1e16,
              i % 17 === 0 ? null : `v${String(i % 13)}`,
              i % 7,
              (1n << 62n) + BigInt(i),
              BigInt(i * 157 - 3000),
            ]);
          const governor = new Governor(48 << 20),
            controlGrant = await governor.admit({
              label: "uninterrupted",
              minimumBytes: 8 << 20,
              spillable: true,
            }),
            oldGrant = await governor.admit({
              label: "old hash",
              minimumBytes: 8 << 20,
              spillable: true,
            }),
            oldIoGrant = await governor.admit({
              label: "old I/O",
              minimumBytes: 3 << 20,
              spillable: false,
            }),
            spill = new MemorySpillService(),
            files = new SpillFiles(spill, 1, {
              allocate: () => false,
              free: () => {
                throw new Error("unbuffered");
              },
            }),
            makeIo = (grant: Grant) => {
              const pool = new Pool(new Charge(grant), 3 << 20),
                pages = new PageWriter(pool);
              return {
                pool,
                pages,
                io: new SpillIo(pool, files, pages, { grant, queryId: 1 }, () => undefined),
              };
            },
            oldIo = makeIo(oldIoGrant),
            directory = new TapeDirectory(oldIo.io, oldIo.pool),
            checkpoint = new GroupCheckpoint(directory, rel.groupBy.length),
            controlArena = new OutputArena(controlGrant, 2048),
            oldArena = new OutputArena(oldGrant, 2048),
            controlSink = new CollectSink(),
            result = new CollectSink(),
            options = { typedLayout: false, packedAverages: false, packedIntegers: false },
            control = new AggregateOperator(
              rel,
              { arena: controlArena, input, params: { slots: [] } },
              controlGrant,
              controlSink,
              options,
            ),
            old = new AggregateOperator(
              rel,
              { arena: oldArena, input, params: { slots: [] } },
              oldGrant,
              result,
              options,
            );
          let oldClosed = false,
            nextGrant: Grant | undefined,
            nextIoGrant: Grant | undefined,
            nextIo: ReturnType<typeof makeIo> | undefined,
            nextArena: OutputArena | undefined,
            restored: AggregateOperator | RestoredAggregateOperator | undefined;
          const helpers: Grant[] = [];
          let phase: HybridPhase | undefined;
          try {
            const batches = (part: readonly Row[]) =>
              batchesOf(input, part, { batchRows: 31, dictionary: true, skip: 3, int64: "int64" });
            for (const batch of batches(rows)) control.push(batch);
            for (const batch of batches(rows.slice(0, split))) old.push(batch);
            await checkpoint.save(old, old.groupCount, oldIo.pool);
            if (stable) {
              const parameters = new ParameterCheckpoint(directory),
                suffix = new HashTape(directory);
              await parameters.save([]);
              const staging = new ColumnStore(input, oldIo.pool, {
                chunkRows: 32,
                targetBytes: 32768,
              });
              try {
                for (const batch of batches(rows.slice(split)))
                  await oldIo.io.stage(
                    staging,
                    suffix,
                    { run: 0, partition: 0 },
                    batch.columns,
                    batch.selection?.rows ?? null,
                    0,
                    batch.selection?.count ?? batch.length,
                  );
                await oldIo.io.flush(staging, suffix, { run: 0, partition: 0 });
              } finally {
                staging.close();
              }
              phase = {
                files,
                directory,
                checkpoint,
                parameters,
                suffix,
                partitions: null,
                pending: [],
                output: null,
                resident: old.groupCount,
                level: 0,
                frozen: false,
                minimum: 8 << 20,
                progressBytes: 1 << 20,
                profile: {
                  spilledBytes: files.written,
                  passes: 1,
                  evictions: 1,
                  maxLevel: 0,
                  residentRows: split,
                  spilledRows: rows.length - split,
                  collapsed: 0,
                },
              };
            }
            old.abort(undefined);
            oldArena.close();
            directory.detach();
            oldIo.pages.close();
            oldIo.pool.close();
            expect(oldGrant.allocated + oldIoGrant.allocated).toBe(0);
            oldGrant.close();
            oldIoGrant.close();
            oldClosed = true;
            if (stable) {
              if (!phase) throw new Error("Missing saved phase");
              const minimum = restoredMinimum(rel, input),
                count = restoredLoans(rel);
              const native = await governor.admitGroup({
                label: "one-group restore",
                includesProgress: true,
                grants: [
                  { label: "fixed", minimumBytes: minimum, spillable: true },
                  ...Array.from({ length: count }, (_, i) => [
                    { label: `helper${String(i)}/fixed`, minimumBytes: 0, spillable: false },
                    { label: `helper${String(i)}/growth`, minimumBytes: 0, spillable: true },
                    {
                      label: `helper${String(i)}/progress`,
                      minimumBytes: 1 << 20,
                      spillable: false,
                    },
                  ]).flat(),
                ],
              });
              nextGrant = native[0];
              if (!nextGrant) throw new Error("Missing restore minimum");
              const part = (i: number) => {
                const grant = native[i];
                if (!grant) throw new Error("Missing helper");
                return grant;
              };
              for (let i = 0; i < count; i++) {
                const at = 1 + 3 * i;
                helpers.push(
                  new ReservedGrant(
                    part(at),
                    part(at + 1),
                    0,
                    new StepLoans(part(at + 2), 1 << 20, "saved-group restore"),
                    false,
                    () => part(at + 2).close(),
                    true,
                  ),
                );
              }
              nextArena = new OutputArena(nextGrant, 2048);
              restored = new RestoredAggregateOperator(
                rel,
                { arena: nextArena, input, params: { slots: [] } },
                {
                  grant: nextGrant,
                  loans: helpers,
                  spill,
                  queryId: 1,
                  signal: new AbortController().signal,
                },
                phase,
                result,
              );
              await restored.finish();
              reach.reach("stable pressure merge");
            } else {
              [nextGrant, nextIoGrant] = await governor.admitGroup({
                label: "complete restore pass",
                includesProgress: true,
                grants: [
                  { label: "saved state", minimumBytes: 8 << 20, spillable: true },
                  { label: "bounded I/O", minimumBytes: 3 << 20, spillable: false },
                ],
              });
              if (!nextGrant || !nextIoGrant) throw new Error("Missing restore minimum");
              nextIo = makeIo(nextIoGrant);
              directory.attach(nextIo.io, nextIo.pool);
              nextArena = new OutputArena(nextGrant, 2048);
              restored = new AggregateOperator(
                rel,
                { arena: nextArena, input, params: { slots: [] } },
                nextGrant,
                result,
                options,
              );
              const hash = restored;
              await checkpoint.resume(hash, async () => {
                for (const batch of batches(rows.slice(split))) hash.push(batch);
              });
              await hash.finish();
            }
            await control.finish();
            // Check transition order before counts, so the restore-after-input mutant
            // is rejected for an order-sensitive float result, not merely lost row counts.
            expect(
              new Map(result.rows.map((r) => [grouped ? r[0] : 0, r[(grouped ? 1 : 0) + 1]])),
            ).toEqual(
              new Map(controlSink.rows.map((r) => [grouped ? r[0] : 0, r[(grouped ? 1 : 0) + 1]])),
            );
            expect(result.rows).toHaveLength(grouped ? 7 : 1);
            expect(
              new Map(result.rows.map((r) => [grouped ? r[0] : 0, r.slice(grouped ? 1 : 0)])),
            ).toEqual(
              new Map(controlSink.rows.map((r) => [grouped ? r[0] : 0, r.slice(grouped ? 1 : 0)])),
            );
            // PG can reorder input for COUNT(DISTINCT). Per-call ordinals witness
            // exactly the required native pass's transition sequence.
            const values = rows
                .map(
                  (r, ordinal) =>
                    `(${String(ordinal)},${String(r[2])},${sqlLiteral("float8", Number(r[0]))},${r[1] === null ? "NULL::text" : `'${String(r[1])}'`})`,
                )
                .join(","),
              expected = await pg.query<{ k?: number; n: string; s: string; a: string; d: string }>(
                `SELECT ${grouped ? "k," : ""}count(*)::text n,sum(v ORDER BY ord)::text s,avg(v ORDER BY ord)::text a,count(DISTINCT t)::text d FROM (VALUES ${values}) x(ord,k,v,t)${grouped ? " GROUP BY k" : ""}`,
              );
            expect(
              new Map(
                result.rows.map((r) => [
                  grouped ? Number(r[0]) : 0,
                  r.slice(grouped ? 1 : 0, (grouped ? 1 : 0) + 4).map(Number),
                ]),
              ),
            ).toEqual(
              new Map(
                expected.rows.map((r) => [
                  r.k ?? 0,
                  [Number(r.n), Number(r.s), Number(r.a), Number(r.d)],
                ]),
              ),
            );
            reach.reach(grouped ? "grouped" : "scalar");
            reach.reach("restored prefix");
            reach.reach("membership before suffix");
            reach.reach("native I/O handoff");
            reach.reach("PG exact floats");
          } finally {
            restored?.abort(undefined);
            nextArena?.close();
            control.abort(undefined);
            controlArena.close();
            if (!oldClosed) {
              old.abort(undefined);
              oldArena.close();
            }
            await checkpoint.close().catch(async (error: unknown) => {
              await files.closeAll();
              throw error;
            });
            await files.closeAll();
            directory.detach();
            if (nextIo) {
              nextIo.pages.close();
              nextIo.pool.close();
            } else if (!oldClosed) {
              oldIo.pages.close();
              oldIo.pool.close();
            }
            expect(
              controlGrant.allocated + (nextGrant?.allocated ?? 0) + (nextIoGrant?.allocated ?? 0),
            ).toBe(0);
            controlGrant.close();
            oldGrant.close();
            oldIoGrant.close();
            nextGrant?.close();
            nextIoGrant?.close();
            for (const helper of helpers) helper.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("all charges returned");
        }
      }),
      {
        seed,
        numRuns: 6,
        examples: [
          [75, false],
          [130, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
