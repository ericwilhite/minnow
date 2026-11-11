/** EXCEPT's accumulated left side is state, not a producer that may be discarded. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { batchesOf, INT, BIG, DBL } from "../exec/join-fixtures.js";
import { ExceptOperator } from "../exec/except.js";
import { QueryRuntime } from "../exec/run.js";
import type { Execution, PreparedPlan } from "../exec/executor.js";
import { Governor } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { FileScratch } from "../storage/file-store.js";
import { MemoryFile } from "../storage/memory-store.js";
import type { Batch } from "../vector/vector.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

function count(value: unknown): string | null {
  if (value === null) return null;
  if (typeof value === "bigint" || typeof value === "number" || typeof value === "string")
    return String(value);
  throw new TypeError("Expected exact SQL count");
}
const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows
    .map((row) => ({
      g: row.g === null ? null : Number(row.g),
      n: count(row.n),
      s: row.s === null ? null : Number(row.s),
    }))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
async function collect(prepared: PreparedPlan, execution: Execution) {
  const batches: Batch[] = [];
  for (;;) {
    const frame = await execution.results.next();
    if (!frame) break;
    try {
      const copy = structuredClone(frame.payload);
      batches.push({
        length: copy.rowCount,
        selection: null,
        columns: copy.columns.map((c, i) => {
          const type = prepared.columns[i]?.type;
          if (c.form !== "flat" || !type) throw new Error("Expected flat EXCEPT result");
          return { ...c, type };
        }),
      });
    } finally {
      frame.release();
    }
  }
  await execution.executed;
  await execution.delivered;
  return canonical(oracleRows({ columns: prepared.columns, batches }));
}
async function deadline<T>(job: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      job,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("EXCEPT phase handoff stalled")), 45000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

it.each(seedsFor("next-except-aggregate-handoff", [0xb4d3, 0xb4d4]))(
  "preserves a spilled left arm across right-aggregate handoff (seed %i)",
  async (seed) => {
    const reach = createReach("EXCEPT right aggregate phases", [
      "distinct",
      "all",
      "left completed",
      "left spilled first",
      "parent checkpoint",
      "restore admission",
      "same snapshot",
      "pending cancellation",
      "PG exact",
      "zero",
    ]);
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    await pg.waitReady;
    try {
      const ddl =
        "CREATE TABLE l(g INTEGER,n BIGINT,s DOUBLE PRECISION);CREATE TABLE r(g INTEGER,v TEXT,x DOUBLE PRECISION)";
      for (const statement of ddl.split(";")) await core.execute(statement);
      await pg.exec(ddl);
      const right = Array.from({ length: 8192 }, (_, i) => ({
        g: i % 4,
        v: `v${String(i)}${"x".repeat(120)}`,
        x: i % 13,
      }));
      const sums = Array.from({ length: 4 }, (_, g) =>
        right.reduce((n, row) => n + (row.g === g ? row.x : 0), 0),
      );
      const left: Array<{ g: number | null; n: bigint | null; s: number | null }> = Array.from(
        { length: 16000 },
        (_, i) => ({
          g: i,
          n: i < 4 ? 2048n : (1n << 60n) + BigInt(i),
          s: i < 4 ? (sums[i] ?? 0) : i / 8,
        }),
      );
      for (let g = 0; g < 4; g++)
        for (let i = 0; i < 2; i++) left.push({ g, n: 2048n, s: sums[g] ?? 0 });
      left.push({ g: null, n: null, s: null }, { g: null, n: null, s: null });
      const leftTable = core.harness.path.catalog.tableByName("l");
      if (!leftTable) throw new Error("Missing left fixture table");
      for (const batch of batchesOf(
        [INT, BIG, DBL],
        left.map((row) => [row.g, row.n, row.s]),
        { batchRows: 1024, int64: "int64" },
      ))
        await core.insertVectors(leftTable, batch);
      await core.writeBatch("insertBatch", "r", right);
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      for (const [name, rows] of [
        ["l", left.map((r) => [r.g, r.n, r.s])],
        ["r", right.map((r) => [r.g, r.v, r.x])],
      ] as const) {
        for (let from = 0; from < rows.length; from += 256) {
          const batch = rows.slice(from, from + 256);
          await pg.query(
            `INSERT INTO ${name} VALUES ${batch.map((_, i) => `($${String(3 * i + 1)},$${String(3 * i + 2)},$${String(3 * i + 3)})`).join(",")}`,
            batch.flat(),
          );
        }
      }
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 8000, max: 14000 }),
          fc.boolean(),
          fc.boolean(),
          async (limit, all, cancel) => {
            reach.beginHistory();
            const sql = `SELECT g,n,s FROM l WHERE g < ${String(limit)} OR g IS NULL EXCEPT ${all ? "ALL " : ""}SELECT g,count(DISTINCT v) n,sum(x) s FROM r GROUP BY g`;
            const expected = canonical((await pg.query<Record<string, unknown>>(sql)).rows),
              prepared = core.prepare(sql),
              governor = new Governor(32 << 20),
              snapshot = core.harness.path.pin();
            const spill = new ScratchSpillService(
              new FileScratch(
                new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
                () => undefined,
              ),
            );
            const pinned = await governor.admit({
              label: "pinned EXCEPT competitor",
              minimumBytes: 0,
              spillable: true,
            });
            let filled = false,
              checkpoints = 0,
              admissions = 0,
              execution: Execution | null = null;
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with the original receiver below.
            const checkpoint = ExceptOperator.prototype.checkpoint;
            const observe = vi
              .spyOn(ExceptOperator.prototype, "checkpoint")
              .mockImplementation(async function (this: ExceptOperator) {
                expect(this.leftComplete).toBe(true);
                expect(this.profile.rowsIn).toBe(limit + 10);
                expect(this.spilledBytes).toBeGreaterThan(0);
                reach.reach("left completed");
                reach.reach("left spilled first");
                const saved = await checkpoint.call(this);
                checkpoints++;
                reach.reach("parent checkpoint");
                return saved;
              });
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with the original receiver below.
            const suspend = QueryRuntime.prototype.suspendProducer;
            const retirement = vi
              .spyOn(QueryRuntime.prototype, "suspendProducer")
              .mockImplementation(async function (this: QueryRuntime, sink, pool) {
                await suspend.call(this, sink, pool);
                if (this.request.governor === governor && cancel) {
                  expect(
                    pinned.allocate(
                      Math.max(0, governor.capBytes - governor.reserveBytes - governor.usedBytes),
                    ),
                  ).toBe(true);
                }
              });
            const admit = governor.admitGroup.bind(governor);
            governor.admitGroup = (options) => {
              const wanted = options.grants.reduce((n, part) => n + part.minimumBytes, 0),
                available =
                  governor.capBytes - governor.usedBytes + (options.idleCredit?.held ?? 0);
              const pending = admit(options);
              if (options.idleCredit && wanted > 0) {
                admissions++;
                expect(checkpoints).toBeGreaterThan(0);
                expect(snapshot.released).toBe(false);
                reach.reach("restore admission");
                reach.reach("same snapshot");
                if (cancel) expect(wanted).toBeGreaterThan(available);
                queueMicrotask(() => {
                  if (cancel) {
                    execution?.cancel(new Error("cancel EXCEPT restore wait"));
                    reach.reach("pending cancellation");
                  }
                  pinned.close();
                });
              }
              return pending;
            };
            let job: Promise<unknown> | null = null;
            try {
              execution = prepared.execute({
                snapshot,
                statement: null,
                parameters: [],
                statementTime: 0n,
                governor,
                spill,
                queryId: 1,
                sequences: null,
                sequenceSession: null,
                returning: "atExecuted",
                reads: {
                  open: (pages, chunk, options) => {
                    if (!filled) {
                      filled = true;
                      expect(
                        pinned.allocate(
                          Math.max(
                            0,
                            governor.capBytes - governor.reserveBytes - governor.usedBytes,
                          ),
                        ),
                      ).toBe(true);
                    }
                    return ChunkReader.open(pages, chunk, options);
                  },
                },
              });
              job = collect(prepared, execution).catch((error: unknown) => error);
              const result = await deadline(job);
              if (cancel)
                expect(result).toMatchObject({ code: "MINNOW_QUERY_CANCELLED", sqlState: "57014" });
              else {
                if (result instanceof Error) throw result;
                expect(result).toEqual(expected);
                reach.reach("PG exact");
              }
              expect(checkpoints).toBeGreaterThan(0);
              expect(admissions).toBeGreaterThan(0);
              reach.reach(all ? "all" : "distinct");
            } finally {
              observe.mockRestore();
              retirement.mockRestore();
              pinned.close();
              execution?.cancel(new Error("EXCEPT pressure cleanup"));
              if (job) await job;
              if (execution) await Promise.allSettled([execution.executed, execution.delivered]);
              snapshot.release();
              await spill.close();
            }
            expect(governor.usedBytes).toBe(0);
            expect(spill.bytesInUse).toBe(0);
            reach.reach("zero");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [8000, false, false],
            [14000, true, false],
            [12000, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await core.close();
    }
  },
  300000,
);
