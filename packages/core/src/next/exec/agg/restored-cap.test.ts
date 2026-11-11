/** The saved set can exceed a restore cap even though every individual group fits. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import type { Rel } from "../../ir/ir.js";
import { Governor } from "../../memory/governor.js";
import { StepLoans } from "../../memory/step-loans.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ReservedGrant } from "../admission.js";
import { ColumnStore } from "../column-store.js";
import { INT, TEXT, batchesOf, CollectSink } from "../join-fixtures.js";
import { col } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { ParameterCheckpoint } from "../parameter-checkpoint.js";
import { PageWriter, SpillFiles } from "../spill-pages.js";
import { Charge, Pool, SpillIo } from "../spill-pool.js";
import { AggregateOperator } from "./aggregate.js";
import { GroupCheckpoint } from "./checkpoint.js";
import { HashTape, TapeDirectory } from "./hash-tape.js";
import { RestoredAggregateOperator, restoredLoans, restoredMinimum } from "./restored.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});

it.each(seedsFor("aggregate-restored-set-over-cap", [0xb4dd, 0xb4de]))(
  "restores individually fitting groups when the saved set exceeds the new cap (seed %i)",
  async (seed) => {
    const reach = createReach("restore set over cap", [
      "prefix above cap",
      "one group fits",
      "bounded sort spill",
      "uninterrupted exact",
      "PG exact",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 520, max: 560 }), async (groups) => {
        reach.beginHistory();
        const input = [INT, TEXT],
          size = 16384,
          high = "z".repeat(size),
          low = "a".repeat(size);
        const prefix = Array.from({ length: groups }, (_, g) => [g, high]);
        const suffix = Array.from({ length: groups }, (_, g) => [g, g % 2 ? high : low]);
        const rel: Extract<Rel, { op: "aggregate" }> = {
          id: 2,
          op: "aggregate",
          input: { id: 1, op: "values", rows: [], output: [] },
          output: [],
          groupBy: [0],
          groupingSets: null,
          aggregates: [
            {
              fn: "min",
              args: [col(1, TEXT)],
              type: TEXT,
              filter: null,
              distinct: false,
              orderBy: [],
            },
          ],
        };
        const oldGovernor = new Governor(96 << 20);
        const oldGrant = await oldGovernor.admit({
          label: "old saved set",
          minimumBytes: 40 << 20,
          spillable: true,
        });
        const oldIo = await oldGovernor.admit({
          label: "old I/O",
          minimumBytes: 3 << 20,
          spillable: false,
        });
        const spill = new MemorySpillService(),
          files = new SpillFiles(spill, 91, {
            allocate: () => false,
            free: () => {
              throw new Error("unbuffered");
            },
          });
        const pool = new Pool(new Charge(oldIo), 3 << 20),
          pages = new PageWriter(pool);
        const io = new SpillIo(pool, files, pages, { grant: oldIo, queryId: 91 }, () => undefined);
        const directory = new TapeDirectory(io, pool),
          checkpoint = new GroupCheckpoint(directory, 1);
        const parameters = new ParameterCheckpoint(directory),
          tail = new HashTape(directory);
        const arena = new OutputArena(oldGrant, 2048),
          control = new CollectSink(),
          result = new CollectSink();
        const hash = new AggregateOperator(
          rel,
          { arena, input, params: { slots: [] } },
          oldGrant,
          control,
          { typedLayout: false },
        );
        const staging = new ColumnStore(input, pool, { chunkRows: 16, targetBytes: 65536 });
        let retired = false;
        const governor = new Governor(8 << 20),
          native: Array<ReturnType<typeof governor.tryAdmit>> = [],
          helpers: ReservedGrant[] = [];
        let operator: RestoredAggregateOperator | undefined, nextArena: OutputArena | undefined;
        try {
          for (const batch of batchesOf(input, prefix, { batchRows: 31 })) hash.push(batch);
          const residentBytes = oldGrant.allocated;
          expect(residentBytes).toBeGreaterThan(governor.capBytes);
          reach.reach("prefix above cap");
          expect(4 * size + (128 << 10)).toBeLessThan(governor.capBytes);
          reach.reach("one group fits");
          await checkpoint.save(hash, groups, pool);
          await parameters.save([]);
          for (const batch of batchesOf(input, suffix, { batchRows: 31 })) {
            await io.stage(
              staging,
              tail,
              { run: 0, partition: 0 },
              batch.columns,
              null,
              0,
              batch.length,
            );
            hash.push(batch);
          }
          await io.flush(staging, tail, { run: 0, partition: 0 });
          staging.close();
          await hash.finish();
          hash.abort(undefined);
          arena.close();
          directory.detach();
          pages.close();
          pool.close();
          oldGrant.close();
          oldIo.close();
          retired = true;
          expect(oldGovernor.usedBytes).toBe(0);
          const count = restoredLoans(rel),
            minimum = restoredMinimum(rel, input);
          const parts = await governor.admitGroup({
            label: "single-group restore",
            includesProgress: true,
            grants: [
              { label: "fixed", minimumBytes: minimum, spillable: true },
              ...Array.from({ length: count }, (_, i) => [
                { label: `h${String(i)}/fixed`, minimumBytes: 0, spillable: false },
                { label: `h${String(i)}/growth`, minimumBytes: 0, spillable: true },
                { label: `h${String(i)}/progress`, minimumBytes: 1 << 20, spillable: false },
              ]).flat(),
            ],
          });
          native.push(...parts);
          const part = (i: number) => {
            const value = parts[i];
            if (!value) throw new Error("Missing native restore grant");
            return value;
          };
          for (let i = 0; i < count; i++) {
            const at = 1 + 3 * i;
            helpers.push(
              new ReservedGrant(
                part(at),
                part(at + 1),
                0,
                new StepLoans(part(at + 2), 1 << 20, "restore"),
                false,
                () => part(at + 2).close(),
                true,
              ),
            );
          }
          const pinned = await governor.admit({
            label: "pinned competitor",
            minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
            spillable: false,
          });
          native.push(pinned);
          nextArena = new OutputArena(part(0), 2048);
          const initialWritten = files.written;
          operator = new RestoredAggregateOperator(
            rel,
            { arena: nextArena, input, params: { slots: [] } },
            {
              grant: part(0),
              loans: helpers,
              spill,
              queryId: 91,
              signal: new AbortController().signal,
            },
            {
              files,
              directory,
              checkpoint,
              parameters,
              suffix: tail,
              partitions: null,
              pending: [],
              output: null,
              resident: groups,
              level: 0,
              frozen: false,
              minimum: residentBytes,
              progressBytes: 1 << 20,
              profile: {
                spilledBytes: initialWritten,
                passes: 1,
                evictions: 1,
                maxLevel: 0,
                residentRows: groups,
                spilledRows: groups,
                collapsed: 0,
              },
            },
            result,
          );
          await operator.finish();
          expect(operator.profile.spilledBytes).toBeGreaterThan(initialWritten);
          reach.reach("bounded sort spill");
          const exact = (rows: ReadonlyArray<readonly unknown[]>) =>
            new Map(rows.map((row) => [row[0], row[1]]));
          expect(exact(result.rows)).toEqual(exact(control.rows));
          reach.reach("uninterrupted exact");
          const expected = await pg.query<{ g: number; v: string }>(
            `SELECT g,min(v) v FROM (SELECT g,repeat('z',${String(size)}) v FROM generate_series(0,${String(groups - 1)}) g UNION ALL SELECT g,repeat(CASE WHEN g%2=0 THEN 'a' ELSE 'z' END,${String(size)}) FROM generate_series(0,${String(groups - 1)}) g) t GROUP BY g`,
          );
          expect(exact(result.rows)).toEqual(new Map(expected.rows.map((row) => [row.g, row.v])));
          reach.reach("PG exact");
        } finally {
          operator?.abort(undefined);
          await operator?.closed;
          nextArena?.close();
          if (!retired) {
            hash.abort(undefined);
            arena.close();
            staging.close();
            directory.detach();
            pages.close();
            pool.close();
            oldGrant.close();
            oldIo.close();
          }
          await files.closeAll();
          for (const helper of helpers) helper.close();
          for (const grant of native) grant?.close();
        }
        expect(governor.usedBytes).toBe(0);
        expect(oldGovernor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
        reach.reach("zero");
      }),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  },
  120000,
);
