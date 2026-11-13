/** Borrowed spill rows obey the staging schema, including a projected source prefix. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, COMPOUND_PROJECTION_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ColumnStore } from "../exec/column-store.js";
import { ExceptTape } from "../exec/except-tape.js";
import { RecordCheckpoint } from "../exec/record-checkpoint.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
import { INT, TEXT, CollectSink, batchesOf, type Row } from "../exec/join-fixtures.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-compound-projected-spill", [0xb728, 0xb729]))(
  "spills only declared columns from selected wide source rows (seed %i)",
  async (seed) => {
    const reach = createReach<string>("projected spill rows", COMPOUND_PROJECTION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 80000, max: 120000 }),
        fc.boolean(),
        fc.boolean(),
        async (width, hiddenText, dictionary) => {
          const prefix = "é".repeat(width),
            hidden = "h".repeat(width * 3),
            types = [INT, TEXT],
            sourceTypes = [...types, hiddenText ? TEXT : INT];
          const source: Row[] = [
            [1, prefix + "a", hiddenText ? hidden + "z" : 17],
            [2, prefix + "b", hiddenText ? hidden + "y" : 34],
            [3, prefix + "c", hiddenText ? hidden + "x" : 51],
          ];
          const batch = batchesOf(sourceTypes, source, { batchRows: 8, dictionary, skip: 2 })[0];
          if (!batch) throw new Error("Missing projected spill input");
          const row = batch.selection?.rows[1] ?? 1;
          expect(row).toBeGreaterThan(0);
          const expected = (
            await pg.query("SELECT $1::int AS i,$2::text AS t", [2, prefix + "b"], {
              rowMode: "array",
            })
          ).rows;
          for (const kind of ["record", "except"] as const) {
            reach.beginHistory();
            const governor = new Governor(64 << 20),
              spill = new MemorySpillService(),
              grant = await governor.admit({
                label: "projected spill",
                minimumBytes: 256 << 10,
                spillable: true,
              }),
              pool = new Pool(new Charge(grant), 256 << 10),
              pages = new PageWriter(pool, 16 << 10),
              files = new SpillFiles(spill, 1, {
                allocate: () => false,
                free: () => {
                  throw new Error("Unbuffered projected spill");
                },
              }),
              io = new SpillIo(pool, files, pages, { grant, queryId: 1 }, () => undefined),
              records = kind === "record" ? new RecordCheckpoint(io, spill, types) : null,
              tape = kind === "except" ? new ExceptTape(io, 0) : null,
              staging = tape
                ? new ColumnStore(types, pool, {
                    chunkRows: 1,
                    targetBytes: 16 << 10,
                    varlenBytes: 256,
                  })
                : null,
              blocker = await governor.admit({
                label: "other owner",
                minimumBytes: 0,
                spillable: true,
              }),
              sink = new CollectSink();
            expect(
              blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
            ).toBe(true);
            try {
              // The producer's full read/copy step can include hidden source keys. Its
              // checkpoint schema still contains only the declared payload prefix.
              await io.need(4 * (2 * width + (hiddenText ? 3 * width : 0) + (64 << 10)));
              const release = pool.pinStep();
              try {
                if (records) {
                  await records.write(batch.columns, row);
                  await records.seal();
                } else if (tape && staging) {
                  await tape.stage(staging, batch.columns, row);
                  await tape.flush(staging);
                }
              } finally {
                release();
              }
              const collect = async (
                columns: Parameters<typeof sink.push>[0]["columns"],
                count: number,
              ) => {
                expect(columns).toHaveLength(types.length);
                if (sink.push({ columns, length: count, selection: null }) === "blocked")
                  await sink.ready();
              };
              if (records) {
                // Fund this single-page read before borrowing, keeping the step
                // through decoder/callback cleanup and generator advancement.
                expect(records.tape.pageCount).toBe(1);
                await io.need(6 * records.maxPageBytes + (64 << 10));
                const releaseRead = pool.pinStep();
                try {
                  await records.read(io, collect);
                } finally {
                  releaseRead();
                }
              } else if (tape)
                await tape.each(types, collect, async (ref) => {
                  await io.need(6 * ref.length + (64 << 10));
                });
              expect(sink.rows).toEqual(expected);
              expect(pool.loans).toBeGreaterThan(0);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              reach.reach(kind);
              reach.reach(hiddenText ? "hidden large text" : "hidden integer");
              reach.reach(dictionary ? "selected dictionary" : "selected flat");
              reach.reach("wide UTF8 prefix");
              reach.reach("nonzero slot");
              reach.reach("loan");
              reach.reach("PostgreSQL exact");
            } finally {
              await records?.close();
              await tape?.close();
              staging?.close();
              await files.closeAll();
              pages.close();
              pool.close();
              grant.close();
              blocker.close();
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
            }
            reach.reach("zero resources");
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [80000, false, true],
          [120000, true, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
