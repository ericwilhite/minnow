/** The same PG bag law must survive changing hash capacity, batch layout and spill passes. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXCEPT_SPILL_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { SCRATCH_CONTROL_BYTES } from "../spill/scratch-ranges.js";
import { SimBackend, SimDisk } from "../storage/sim-disk.js";
import { ExceptOperator, exceptMinimumBytes } from "../exec/except.js";
import { INT, TEXT, ArraySource, CollectSink, batchesOf, type Row } from "../exec/join-fixtures.js";
import { drive } from "../exec/pipeline.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("CREATE TEMP TABLE l(n int4,t text);CREATE TEMP TABLE r(n int4,t text)");
}, 60_000);
afterAll(async () => {
  await pg.close();
});
interface Shape {
  groups: number;
  left: number;
  right: number;
  all: boolean;
  tight: boolean;
  dictionary: boolean;
  batch: number;
  collision: boolean;
  scratch: boolean;
  oversized: boolean;
}
const canonical = (rows: readonly Row[]) => rows.map((row) => JSON.stringify(row)).sort();
async function insert(table: string, rows: readonly Row[]) {
  for (let from = 0; from < rows.length; from += 256) {
    const part = rows.slice(from, from + 256);
    await pg.query(
      `INSERT INTO ${table} VALUES ${part.map((_, i) => `($${String(2 * i + 1)},$${String(2 * i + 2)})`).join(",")}`,
      part.flat(),
    );
  }
}
it.each(seedsFor("next-except-spill", [0xb62f, 0xb630]))(
  "keeps PG multiplicities through grant-or-spill (seed %i)",
  async (seed) => {
    const reach = createReach<string>("EXCEPT spill", EXCEPT_SPILL_REACH);
    const base: Shape = {
      groups: 2200,
      left: 2,
      right: 1,
      all: true,
      tight: true,
      dictionary: false,
      batch: 127,
      collision: false,
      scratch: false,
      oversized: false,
    };
    const examples: Array<[Shape]> = [
      [base],
      [{ ...base, oversized: true, collision: true, scratch: true }],
      [{ ...base, scratch: true }],
      [{ ...base, collision: true }],
      [{ ...base, tight: false, dictionary: true, batch: 1 }],
      [{ ...base, all: false, right: 0 }],
      [{ ...base, right: 3, dictionary: true }],
      [{ ...base, all: false, tight: false }],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          groups: fc.integer({ min: 1200, max: 2400 }),
          left: fc.integer({ min: 1, max: 3 }),
          right: fc.integer({ min: 0, max: 4 }),
          all: fc.boolean(),
          tight: fc.boolean(),
          dictionary: fc.boolean(),
          batch: fc.constantFrom(17, 127, 511, 2048),
          collision: fc.boolean(),
          scratch: fc.boolean(),
          oversized: fc.boolean(),
        }),
        async (shape) => {
          reach.beginHistory();
          const source: Row[] = Array.from(
            {
              length: shape.oversized
                ? 3
                : shape.collision
                  ? 32 + (shape.groups % 17)
                  : shape.groups,
            },
            (_, i) => [
              i % 7 === 0 ? null : i,
              i % 11 === 0
                ? null
                : `${String(i)}:${"s".repeat(shape.oversized ? 700000 + (shape.groups % 3) * 80000 : shape.collision ? 40000 : 512)}`,
            ],
          );
          const left = Array.from({ length: shape.left }, () => source).flat();
          const right = Array.from({ length: shape.right }, () => source).flat();
          await pg.exec("TRUNCATE l,r");
          await insert("l", left);
          await insert("r", right);
          const expected = (
            await pg.query<{ n: number | null; t: string | null }>(
              `SELECT n,t FROM l EXCEPT${shape.all ? " ALL" : ""} SELECT n,t FROM r`,
            )
          ).rows.map((row) => [row.n, row.t] as Row);
          const types = [INT, TEXT],
            minimum = exceptMinimumBytes(types),
            sharedMinimum = shape.scratch ? SCRATCH_CONTROL_BYTES : 0;
          const cap = shape.tight
            ? // Preserve the tight operator allowance after admitting the shared owner.
              Math.max(
                minimum + sharedMinimum + (1 << 20),
                Math.ceil(((minimum + sharedMinimum) * 8) / 7),
              )
            : 64 << 20;
          const governor = new Governor(cap);
          const spillControl = shape.scratch
            ? await governor.admit({
                label: "shared scratch metadata",
                minimumBytes: SCRATCH_CONTROL_BYTES,
                spillable: false,
              })
            : null;
          const backend = shape.scratch ? new SimBackend(new SimDisk()) : null;
          const lease = await backend?.acquire();
          const scratch = lease === undefined ? null : backend?.scratch(lease);
          const spill =
            scratch && spillControl
              ? new ScratchSpillService(scratch, { budget: spillControl })
              : new MemorySpillService();
          const grant = await governor.admit({
            label: "except PG",
            minimumBytes: minimum,
            spillable: true,
          });
          const sink = new CollectSink(23),
            op = new ExceptOperator(
              types,
              shape.all,
              { grant, spill, queryId: 1, hashMask: shape.collision ? 0 : 0xffff_ffff },
              sink,
            );
          try {
            await drive(
              new ArraySource(
                batchesOf(types, left, { dictionary: shape.dictionary, batchRows: shape.batch }),
              ),
              op.left,
            );
            await drive(
              new ArraySource(
                batchesOf(types, right, {
                  dictionary: shape.dictionary,
                  batchRows: shape.batch === 17 ? 511 : 17,
                }),
              ),
              op.right,
            );
            await op.closed;
            expect(canonical(sink.rows)).toEqual(canonical(expected));
            reach.reach(op.profile.spilledBytes > 0 ? "spill" : "resident");
            if (op.profile.collisionPasses > 0) reach.reach("collision fallback");
          } finally {
            op.abort(new Error("test complete"));
            await op.closed;
            grant.close();
            if (spill instanceof ScratchSpillService) await spill.close();
            spillControl?.close();
            await lease?.release();
          }
          expect(governor.peakBytes).toBeLessThanOrEqual(cap);
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach(shape.all ? "ALL" : "DISTINCT");
          if (shape.oversized) reach.reach("oversized page");
          reach.reach(shape.scratch ? "scratch backend" : "memory backend");
          reach.reach(shape.dictionary ? "dictionary" : "flat");
          reach.reach("NULL keys");
          reach.reach("unequal batches");
          reach.reach("exact PG");
          reach.reach("cleanup");
          if (shape.left > 1) reach.reach("left duplicates");
          if (shape.right > shape.left) reach.reach("right exceeds left");
        },
      ),
      { seed, numRuns: 6 + examples.length, examples },
    );
    reach.assertReached();
  },
  120_000,
);
