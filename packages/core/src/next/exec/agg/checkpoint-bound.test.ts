/** A compound snapshot row must be fully funded before any key/state copy. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel } from "../../ir/ir.js";
import { Governor } from "../../memory/governor.js";
import { StepLoans } from "../../memory/step-loans.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ReservedGrant } from "../admission.js";
import { TEXT, BIG, batchesOf, CollectSink } from "../join-fixtures.js";
import { OutputArena } from "../kernels/vectors.js";
import { PageWriter, SpillFiles } from "../spill-pages.js";
import { Charge, Pool, SpillIo } from "../spill-pool.js";
import { AggregateOperator } from "./aggregate.js";
import { GroupCheckpoint } from "./checkpoint.js";
import { TapeDirectory } from "./hash-tape.js";

it.each(seedsFor("aggregate-compound-checkpoint-bound", [0xb4d7, 0xb4d8]))(
  "funds a many-column saved row before materializing it under pinned pressure (seed %i)",
  async (seed) => {
    const reach = createReach("compound checkpoint bound", [
      "compound row",
      "private progress",
      "exact keys",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 28, max: 34 }),
        fc.integer({ min: 60000, max: 65000 }),
        async (width, bytes) => {
          reach.beginHistory();
          const governor = new Governor(32 << 20);
          const parts = await governor.admitGroup({
            label: "checkpoint",
            includesProgress: true,
            grants: [
              { label: "hash", minimumBytes: 8 << 20, spillable: true },
              { label: "io", minimumBytes: 2 << 20, spillable: false },
              { label: "growth", minimumBytes: 0, spillable: true },
              { label: "private", minimumBytes: 8 << 20, spillable: false },
            ],
          });
          const [hashGrant, fixed, growth, progress] = parts;
          if (!hashGrant || !fixed || !growth || !progress)
            throw new Error("Missing native account");
          const ioGrant = new ReservedGrant(
            fixed,
            growth,
            2 << 20,
            new StepLoans(progress, 8 << 20, "checkpoint row"),
            false,
            () => progress.close(),
            true,
          );
          const pinned = await governor.admit({
            label: "pinned",
            minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
            spillable: false,
          });
          const input = Array.from({ length: width }, () => TEXT);
          const rel: Extract<Rel, { op: "aggregate" }> = {
            id: 2,
            op: "aggregate",
            input: { id: 1, op: "values", rows: [], output: [] },
            output: [],
            groupBy: input.map((_, i) => i),
            groupingSets: null,
            aggregates: [
              { fn: "count_star", args: [], type: BIG, distinct: false, filter: null, orderBy: [] },
            ],
          };
          const spill = new MemorySpillService();
          const files = new SpillFiles(spill, 71, {
            allocate: () => false,
            free: () => {
              throw new Error("unbuffered");
            },
          });
          const pool = new Pool(new Charge(ioGrant), 2 << 20);
          const pages = new PageWriter(pool);
          const io = new SpillIo(
            pool,
            files,
            pages,
            { grant: ioGrant, queryId: 71 },
            () => undefined,
          );
          const directory = new TapeDirectory(io, pool);
          const checkpoint = new GroupCheckpoint(directory, width);
          const arena = new OutputArena(hashGrant, 2048);
          const hash = new AggregateOperator(
            rel,
            { arena, input, params: { slots: [] } },
            hashGrant,
            new CollectSink(),
            { typedLayout: false },
          );
          const values = input.map((_, i) => String.fromCharCode(65 + (i % 26)).repeat(bytes));
          try {
            for (const batch of batchesOf(input, [values])) hash.push(batch);
            await checkpoint.save(hash, 1, pool);
            expect(pool.loans).toBeGreaterThan(0);
            reach.reach("private progress");
            let seen = 0;
            for await (const record of checkpoint.records()) {
              expect(record.kind).toBe("state");
              for (let i = 0; i < width; i++) {
                const column = record.columns[i];
                if (column?.data.layout !== "varlen") throw new Error("Expected native text key");
                expect(
                  new TextDecoder().decode(
                    column.data.bytes.subarray(column.data.offsets[0], column.data.offsets[1]),
                  ),
                ).toBe(values[i]);
              }
              seen++;
            }
            expect(seen).toBe(1);
            reach.reach("compound row");
            reach.reach("exact keys");
          } finally {
            hash.abort(undefined);
            arena.close();
            await files.closeAll();
            directory.detach();
            pages.close();
            pool.close();
            hashGrant.close();
            ioGrant.close();
            pinned.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("zero");
        },
      ),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  },
  60000,
);
