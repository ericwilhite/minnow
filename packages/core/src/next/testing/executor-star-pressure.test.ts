/** Complete optional star minima wait atomically and survive reclaim, never split joins. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STAR_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { Execution, PreparedPlan } from "../exec/executor.js";
import { StarDictionaryBooks } from "../exec/star-dictionary.js";
import { HashJoin } from "../exec/join.js";
import { AggregateOperator } from "../exec/agg/aggregate.js";
import { QueryRuntime } from "../exec/run.js";
import { ChunkReader } from "../page/reader.js";
import { StreamDictionaries } from "../owner/frame.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { StorageCore } from "./storage-core.js";

// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with .call(this).
const finishBuild = HashJoin.prototype.finishBuild;
// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with .call(this).
const finishAggregate = AggregateOperator.prototype.finish;
// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with .call(this).
const reserve = QueryRuntime.prototype.reserve;
// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with .call(this).
const encode = StarDictionaryBooks.prototype.encode;
const live = new Set<Execution>();
afterEach(() => {
  vi.restoreAllMocks();
  for (const run of live) run.cancel(new Error("Star test ended"));
  live.clear();
});
const modes = [
  "release",
  "wait-cancel",
  "read-cancel",
  "read-fault",
  "output-cancel",
  "concurrent",
  "publish",
  "reclaim",
] as const;
const canonical = (rows: Array<Record<string, unknown>>) =>
  rows
    .map((r): Record<string, unknown> => ({ ...r, n: Number(r.n), s: Number(r.s) }))
    .sort((a, b) => JSON.stringify([a.x, a.y, a.z]).localeCompare(JSON.stringify([b.x, b.y, b.z])));

async function collect(plan: PreparedPlan, run: Execution, advance: () => void) {
  const dictionaries = new StreamDictionaries(),
    rows: Array<Record<string, unknown>> = [];
  for (;;) {
    const frame = await run.results.next();
    if (!frame) break;
    try {
      const payload = structuredClone(frame.payload);
      dictionaries.apply(payload);
      for (let row = 0; row < payload.rowCount; row++) {
        rows.push(
          Object.fromEntries(
            payload.columns.map((column, i) => {
              const output = plan.columns[i];
              if (!output || column.form === "constant") throw new Error("Unexpected star frame");
              if (column.form === "flat")
                return [output.name, valueAt({ ...column, type: output.type }, row)];
              if (
                column.validity !== null &&
                ((column.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0
              )
                return [output.name, null];
              const found = dictionaries.locate(column.dictionary, column.codes[row] ?? 0);
              return [output.name, valueAt({ ...found.entries, type: output.type }, found.index)];
            }),
          ),
        );
      }
    } finally {
      frame.release();
      advance();
    }
  }
  const summary = await run.executed;
  expect(
    summary.profile.operators.filter((op) => op.op === "join").every((op) => op.spilledBytes === 0),
  ).toBe(true);
  await run.delivered;
  return canonical(rows);
}

it.each(seedsFor("next-star-pressure", [0xb76f, 0xb770]))(
  "keeps complete star credit and cleans every pressured lifetime (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    const reach = createReach("star admitted progress", STAR_PRESSURE_REACH);
    try {
      await pg.waitReady;
      await core.execute("CREATE TABLE f(ka TEXT,kb TEXT,kc TEXT,v DOUBLE PRECISION)");
      await pg.exec("CREATE TABLE f(ka TEXT,kb TEXT,kc TEXT,v DOUBLE PRECISION)");
      for (const name of ["a", "b", "c"]) {
        const ddl = `CREATE TABLE ${name}(k TEXT PRIMARY KEY,t TEXT,label TEXT)`;
        await core.execute(ddl);
        await pg.exec(ddl);
        const rows = Array.from({ length: 24 }, (_, i) => ({
          k: `k${String(i)}`,
          t: `t${String(i)}`,
          label: `l${String(i)}`,
        }));
        await core.writeBatch("insertBatch", name, rows);
        await pg.exec(
          `INSERT INTO ${name} SELECT 'k'||i::text,'t'||i::text,'l'||i::text FROM generate_series(0,23) i`,
        );
      }
      const rows = Array.from({ length: 4097 }, (_, i) => ({
        ka: `k${String(i % 24)}`,
        kb: `k${String(Math.floor(i / 24) % 24)}`,
        kc: `k${String(Math.floor(i / 576) % 24)}`,
        v: i / 4,
      }));
      await core.writeBatch("insertBatch", "f", rows);
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await pg.exec(
        "INSERT INTO f SELECT 'k'||(i%24)::text,'k'||((i/24)%24)::text,'k'||((i/576)%24)::text,i::double precision/4 FROM generate_series(0,4096) i",
      );
      const sql =
        "SELECT a.t AS x,b.t AS y,c.label AS z,count(*) AS n,sum(f.v) AS s FROM f JOIN a ON a.k=f.ka JOIN b ON b.k=f.kb JOIN c ON c.k=f.kc GROUP BY a.t,b.t,c.label";
      const prepared = core.prepare(sql);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...modes),
          fc.integer({ min: 1, max: 3 }),
          async (mode, competitors) => {
            reach.beginHistory();
            const expected = canonical(
              (await pg.query(sql)).rows as Array<Record<string, unknown>>,
            );
            const governor = new Governor(64 << 20),
              executions: Execution[] = [],
              pins: Array<() => void> = [];
            const error = new Error(`star ${mode}`);
            let holder: Grant | undefined,
              rival: Grant | undefined,
              timer: ReturnType<typeof setTimeout> | undefined;
            let rejectStall: (error: Error) => void = () => undefined;
            const stalled = new Promise<never>((_, reject) => {
              rejectStall = reject;
            });
            const advance = () => {
              clearTimeout(timer);
              timer = setTimeout(
                () => rejectStall(new Error("Star pressure made no progress for30s")),
                30000,
              );
            };
            const guard = <T>(job: Promise<T>) => Promise.race([job, stalled]);
            let entered: () => void = () => undefined,
              wake: () => void = () => undefined;
            const blocked = new Promise<void>((resolve) => {
              entered = resolve;
            });
            const gate = new Promise<void>((resolve) => {
              wake = resolve;
            });
            let gated = false,
              reclaimed = 0,
              pinned = 0;
            const admitted = new Set<number>();
            vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
              this: QueryRuntime,
              ...args
            ) {
              await reserve.call(this, ...args);
              admitted.add(this.request.queryId);
              advance();
            });
            const closed = vi.spyOn(StarDictionaryBooks.prototype, "close");
            const admit = governor.admitGroup.bind(governor);
            governor.admitGroup = (options) => {
              if (options.label.endsWith("/fixed-buffers")) {
                entered();
                reach.reach("atomic wait");
              }
              return admit(options).then((parts) => {
                advance();
                return parts;
              });
            };
            const start = () => {
              const queryId = executions.length + 1;
              const snapshot = core.harness.path.pin();
              pins.push(() => snapshot.release());
              const run = prepared.execute({
                snapshot,
                statement: null,
                parameters: [],
                statementTime: 0n,
                governor,
                spill: core.spill,
                queryId,
                sequences: null,
                sequenceSession: null,
                returning: "atExecuted",
                resultWindowBytes: 1,
                reads: {
                  open: (pages, chunk, options) => {
                    advance();
                    if (
                      !gated &&
                      ["read-cancel", "read-fault", "publish"].includes(mode) &&
                      admitted.has(queryId)
                    ) {
                      gated = true;
                      entered();
                      return gate.then(() => {
                        if (mode === "read-fault") throw error;
                        return ChunkReader.open(pages, chunk, options);
                      });
                    }
                    return ChunkReader.open(pages, chunk, options);
                  },
                },
              });
              executions.push(run);
              live.add(run);
              void run.executed.catch(() => undefined);
              void run.delivered.catch(() => undefined);
              return run;
            };
            try {
              advance();
              if (mode === "release" || mode === "wait-cancel") {
                holder = await governor.admit({
                  label: "temporarily pinned competing holder",
                  minimumBytes: 48 << 20,
                  spillable: false,
                });
              }
              if (mode === "reclaim") {
                rival = await governor.admit({
                  label: "reclaim competing holder",
                  minimumBytes: 0,
                  spillable: true,
                });
                vi.spyOn(StarDictionaryBooks.prototype, "encode").mockImplementation(function (
                  this: StarDictionaryBooks,
                  ...args
                ) {
                  if (pinned === 0) {
                    pinned = Math.max(
                      0,
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    );
                    if (!rival || pinned === 0 || !rival.tryGrow(pinned) || !rival.allocate(pinned))
                      throw new Error("Missing pinned growth witness");
                    reach.reach("pinned growth");
                  }
                  return encode.call(this, ...args);
                });
                const reclaim = async () => {
                  if (!rival || pinned === 0) throw new Error("Missing pinned reclaim witness");
                  await rival.reclaimRoom?.(1);
                  reclaimed++;
                  advance();
                };
                vi.spyOn(HashJoin.prototype, "finishBuild").mockImplementation(async function (
                  this: HashJoin,
                ) {
                  await finishBuild.call(this);
                  await reclaim();
                });
                vi.spyOn(AggregateOperator.prototype, "finish").mockImplementation(async function (
                  this: AggregateOperator,
                ) {
                  await reclaim();
                  await finishAggregate.call(this);
                });
              }
              const run = start();
              if (mode === "release" || mode === "wait-cancel") {
                await guard(blocked);
                expect(closed).not.toHaveBeenCalled();
                if (mode === "wait-cancel") run.cancel(error);
                holder?.close();
                reach.reach("holder release");
              }
              if (mode === "read-cancel" || mode === "read-fault" || mode === "publish") {
                await guard(blocked);
                expect(gated).toBe(true);
                if (mode === "read-cancel") run.cancel(error);
                if (mode === "publish") {
                  await core.writeBatch("insertBatch", "f", [
                    { ka: "k0", kb: "k0", kc: "k0", v: 2048.25 },
                  ]);
                  await core.harness.path.recycleLog();
                  await core.harness.path.quiesce();
                }
                wake();
              }
              if (mode === "output-cancel") {
                const frame = await guard(run.results.next());
                expect(frame).not.toBeNull();
                run.cancel(error);
                frame?.release();
              }
              if (["wait-cancel", "read-cancel", "read-fault", "output-cancel"].includes(mode)) {
                const result = expect(guard(collect(prepared, run, advance))).rejects;
                if (mode === "read-fault") await result.toBe(error);
                else
                  await result.toMatchObject({ code: "MINNOW_QUERY_CANCELLED", sqlState: "57014" });
              } else {
                const all = [run];
                if (mode === "concurrent")
                  for (let i = 1; i < competitors + 1; i++) all.push(start());
                const actual = await guard(
                  Promise.all(all.map((execution) => collect(prepared, execution, advance))),
                );
                for (const result of actual) expect(result).toEqual(expected);
                reach.reach("PG exact");
                if (mode === "reclaim") {
                  expect(reclaimed).toBeGreaterThanOrEqual(4);
                  reach.reach("protected prepared credit");
                }
              }
              if (mode === "publish") await pg.exec("INSERT INTO f VALUES('k0','k0','k0',2048.25)");
              reach.reach(mode);
            } finally {
              wake();
              holder?.close();
              if (pinned > 0) rival?.free(pinned);
              rival?.close();
              for (const run of executions) run.cancel(error);
              await Promise.allSettled(executions.flatMap((run) => [run.executed, run.delivered]));
              for (const run of executions) live.delete(run);
              for (const release of pins) release();
              vi.restoreAllMocks();
              clearTimeout(timer);
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero resources");
            }
          },
        ),
        { seed, numRuns: 12, examples: modes.map((mode) => [mode, 2]) },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
  180000,
);
