/** One engine for `scripts/head-to-head.mts`, served through `lib/independent-cpu.mts`.
 *
 * `next` is the 1.0 rebuild from this checkout's `packages/core/dist`: StorageCore over the real
 * write path and memory-backed extent storage, the production executor defaults and the default
 * 256 MiB governor. `shipped` is a published package (normally `@minnowdb/core@0.20.0` unpacked
 * from npm): `MinnowDatabase` over `MemoryBlockStore`, `queryCursor` with `memoize: false` and the
 * same nominal 256 MiB execution budget. Each worker builds its own fixture from the committed
 * deterministic generators (`next-executor-workload`, `apps/site/bench`), so both engines answer
 * the same logical data.
 *
 * Boundaries: `rows` materializes every result row on both engines (the matched comparison);
 * `frames` exists only on `next` and stops at released columnar result frames. Fixture loading,
 * verification and IPC are outside the clock.
 */
import { createHash } from "node:crypto";
import { cpus } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import { AnswerCollector, checkFixtureRows } from "./lib/answers.mts";

export interface HeadToHeadData {
  readonly engine: "next" | "shipped";
  /** next: repository root with a built packages/core/dist. shipped: unpacked package root. */
  readonly root: string;
  readonly dataset: "W1M" | "S1";
  readonly rows: number;
  readonly scale: number;
  readonly capBytes: number;
  readonly deadlineMs: number;
  readonly workloads: readonly string[];
  /** next only: restore this captured fixture image instead of building the fixture. */
  readonly image?: string;
  /** next only: the image's identity; restore refuses an image written for another one. */
  readonly imageIdentity?: string;
  /** Complete executions per timed bracket, per cell (default 1). Same for both arms. */
  readonly repeats?: Readonly<Record<string, number>>;
}
interface Query {
  readonly id: string;
  readonly sql: string;
  readonly ordered?: boolean;
  /** "count": no ORDER BY under LIMIT, so only the row count is comparable. */
  readonly check?: string;
  readonly kind?: string;
}
interface Catalogue {
  readonly W_QUERIES: readonly Query[];
  readonly S_QUERIES: readonly Query[];
  readonly S_TABLES: readonly string[];
  sqlFor(query: Query, engine: string, rows: number): string;
}
interface WideGenerator {
  wideSpec(columns: number): Array<{ name: string; pg: string }>;
  wideBatch(
    spec: Array<{ name: string; pg: string }>,
    start: number,
    count: number,
    total: number,
  ): Record<string, unknown[]>;
  dimTables(): Array<{
    name: string;
    rows: Array<{ key: string; label: string; tier: string; weight: number; active: boolean }>;
  }>;
}
interface Entity {
  readonly name: string;
  readonly primaryKey: string;
  readonly columns: ReadonlyArray<{
    name: string;
    type: "number" | "string" | "boolean" | "datetime";
  }>;
  rows(scale: number): number;
}
interface Star {
  readonly commerceEntities: readonly Entity[];
  generateEntityBatch(
    entity: Entity,
    start: number,
    count: number,
    total: number,
    scale: number,
  ): Record<string, unknown[]>;
}
interface Engine {
  /** next only: capture the loaded fixture as a native image (fresh fixtures only). */
  capture?(directory: string, identity: string, signal: AbortSignal): Promise<unknown>;
  /** Runs one query to completion; `consume` sees each materialized row when given. */
  run(
    sql: string,
    boundary: string,
    signal: AbortSignal,
    consume?: (columns: readonly string[], row: Record<string, unknown>) => void,
  ): Promise<number>;
  close(): Promise<void>;
}

const WIDE_DDL_DIM =
  "(k TEXT PRIMARY KEY,label TEXT,tier TEXT,weight DOUBLE PRECISION,active BOOLEAN)";
const SQL_TYPES = {
  number: "DOUBLE PRECISION",
  string: "TEXT",
  boolean: "BOOLEAN",
  datetime: "TIMESTAMP WITH TIME ZONE",
} as const;
const repository = resolve(import.meta.dirname, "..");
const workload = join(repository, "scripts/next-executor-workload");

/** The committed S_q7 uses ROUND(double, int), which PostgreSQL rejects; both engines run the
 * PostgreSQL-valid form, as `next-executor-bench.mts` does. */
export function sqlFor(catalogue: Catalogue, query: Query, rows: number): string {
  const sql = catalogue.sqlFor(query, "minnow", rows);
  return query.id === "S_q7" ? sql.replace("ROUND(revenue, 4)", "ROUND(revenue::numeric, 4)") : sql;
}

async function nextEngine(data: HeadToHeadData, signal: AbortSignal): Promise<Engine> {
  const dist = (path: string) => pathToFileURL(join(data.root, "packages/core/dist", path)).href;
  type StorageModule = typeof import("../packages/core/dist/next/testing/storage-core.js");
  type InputModule = typeof import("../packages/core/dist/next/testing/executor-input.js");
  type OracleModule = typeof import("../packages/core/dist/next/testing/differential-adapter.js");
  type VectorModule = typeof import("../packages/core/dist/next/vector/vector.js");
  type GovernorModule = typeof import("../packages/core/dist/next/memory/governor.js");
  const [storage, input, oracle, vector, governor] = await Promise.all([
    import(dist("next/testing/storage-core.js")) as Promise<StorageModule>,
    import(dist("next/testing/executor-input.js")) as Promise<InputModule>,
    import(dist("next/testing/differential-adapter.js")) as Promise<OracleModule>,
    import(dist("next/vector/vector.js")) as Promise<VectorModule>,
    import(dist("next/memory/governor.js")) as Promise<GovernorModule>,
  ]);
  const core = new storage.StorageCore(8192);
  if (core.harness.governor.capBytes !== data.capBytes)
    (core.harness as { governor: InstanceType<GovernorModule["Governor"]> }).governor =
      new governor.Governor(data.capBytes);
  const images = await import("./lib/native-storage-image.mts");
  // A restored image skips the load; a fresh load records file capacities for its capture.
  const monitor =
    data.image === undefined ? images.monitorNativeCapacity(core.harness.backend) : null;
  if (data.image !== undefined)
    await images.restoreNativeImage(
      core.harness.backend,
      data.image,
      data.imageIdentity ?? "",
      signal,
    );
  await core.open();
  if (data.image === undefined)
    try {
      const load = async (
        name: string,
        batches: (start: number, count: number) => object,
        total: number,
      ) => {
        const table = core.harness.path.catalog.tableByName(name);
        if (table === undefined) throw new Error(`${name} was not created`);
        const grant = await core.harness.governor.admit({
          label: `head-to-head/input/${name}`,
          minimumBytes: 8 * 1024 * 1024,
          spillable: true,
        });
        try {
          for (let start = 0; start < total; start += vector.BATCH_ROWS) {
            signal.throwIfAborted();
            const count = Math.min(vector.BATCH_ROWS, total - start);
            const batch = input.inputBatch(table, batches(start, count) as never, grant);
            try {
              await core.insertVectors(table, batch.batch);
            } finally {
              batch.close();
            }
          }
        } finally {
          grant.close();
        }
      };
      if (data.dataset === "W1M") {
        const generator = (await import(
          pathToFileURL(join(workload, "gen.mjs")).href
        )) as WideGenerator;
        const spec = generator.wideSpec(200);
        await core.execute(
          `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
        );
        await load(
          "events_wide",
          (start, count) => generator.wideBatch(spec, start, count, data.rows),
          data.rows,
        );
        for (const dimension of generator.dimTables()) {
          await core.execute(`CREATE TABLE ${dimension.name} ${WIDE_DDL_DIM}`);
          await core.writeBatch(
            "insertBatch",
            dimension.name,
            dimension.rows.map((row) => ({
              k: row.key,
              label: row.label,
              tier: row.tier,
              weight: row.weight,
              active: row.active,
            })),
          );
        }
      } else {
        const star = (await import(
          pathToFileURL(join(repository, "apps/site/bench/benchmark.ts")).href
        )) as Star;
        const catalogue = (await import(
          pathToFileURL(join(workload, "queries.mjs")).href
        )) as Catalogue;
        for (const entity of star.commerceEntities.filter((e) =>
          catalogue.S_TABLES.includes(e.name),
        )) {
          const count = entity.rows(data.scale);
          await core.execute(
            `CREATE TABLE ${entity.name} (${entity.columns.map((c) => `${c.name} ${SQL_TYPES[c.type]}${c.name === entity.primaryKey ? " PRIMARY KEY" : ""}`).join(",")})`,
          );
          await load(
            entity.name,
            (start, n) => star.generateEntityBatch(entity, start, n, count, data.scale),
            count,
          );
        }
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
    } catch (error) {
      await core.close();
      throw error;
    }
  return {
    async run(sql, boundary, querySignal, consume) {
      const lowered = core.lowerer.lower(sql, core.context());
      if (lowered.kind !== "query") throw new Error("expected a query");
      const prepared = core.executor.prepare(lowered.plan);
      const names = prepared.columns.map((column) => column.name);
      const run = core.start(prepared, [], querySignal);
      const dictionaries = new Map<
        number,
        Array<import("../packages/core/dist/next/owner/frame.js").WireFlat>
      >();
      let rows = 0;
      try {
        for (;;) {
          const frame = await run.execution.results.next();
          if (frame === null) break;
          try {
            if (boundary === "frames") {
              rows += frame.payload.rowCount;
              continue;
            }
            // StorageCore's tested materialization, as in next-executor-bench.mts.
            for (const delta of frame.payload.dictionaries) {
              const parts = dictionaries.get(delta.id) ?? [];
              parts.push(structuredClone(delta.values));
              dictionaries.set(delta.id, parts);
            }
            const decoded = oracle.oracleRows({
              columns: prepared.columns,
              batches: [
                {
                  length: frame.payload.rowCount,
                  selection: null,
                  columns: frame.payload.columns.map((column, index) => {
                    const flat =
                      column.form === "dictionary"
                        ? storage.expandTextDictionary(
                            column,
                            dictionaries.get(column.dictionary) ?? [],
                          )
                        : column;
                    if (flat.form !== "flat") throw new Error("unexpected constant result frame");
                    const type = prepared.columns[index]?.type;
                    if (type === undefined) throw new Error("result type missing");
                    return { ...flat, type };
                  }),
                },
              ],
            });
            rows += decoded.length;
            if (consume !== undefined) for (const row of decoded) consume(names, row);
          } finally {
            frame.release();
          }
        }
        await run.execution.executed;
        await run.execution.delivered;
        return rows;
      } catch (error) {
        run.execution.cancel(error);
        await Promise.allSettled([run.execution.executed, run.execution.delivered]);
        throw error;
      } finally {
        run.release();
      }
    },
    async capture(directory, identity, captureSignal) {
      if (monitor === null) throw new Error("Only a freshly loaded fixture can be captured");
      await core.harness.path.quiesce();
      return images.captureNativeLease(
        monitor.lease,
        directory,
        identity,
        captureSignal,
        monitor.stop(),
      );
    },
    close: async () => {
      monitor?.stop();
      await core.close();
    },
  };
}

async function shippedEngine(data: HeadToHeadData, signal: AbortSignal): Promise<Engine> {
  const dist = (path: string) => pathToFileURL(join(data.root, "dist", path)).href;
  interface Database {
    execute(sql: string): Promise<unknown>;
    insertBatch(table: string, input: { columns: Record<string, unknown[]> }): Promise<unknown>;
    queryCursor(
      sql: string,
      options: Record<string, unknown>,
    ): AsyncIterable<{ columns: string[]; rows: Array<Record<string, unknown>> }>;
    close(): Promise<void>;
  }
  const { MinnowDatabase } = (await import(dist("engine/database.js"))) as {
    MinnowDatabase: new (store: unknown) => Database;
  };
  const { MemoryBlockStore } = (await import(dist("storage/memory.js"))) as {
    MemoryBlockStore: new () => { close(): void };
  };
  const store = new MemoryBlockStore();
  const db = new MinnowDatabase(store);
  try {
    if (data.dataset === "W1M") {
      const generator = (await import(
        pathToFileURL(join(workload, "gen.mjs")).href
      )) as WideGenerator;
      const spec = generator.wideSpec(200);
      await db.execute(
        `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
      );
      for (let at = 0; at < data.rows; at += 65536) {
        signal.throwIfAborted();
        const columns = generator.wideBatch(spec, at, Math.min(65536, data.rows - at), data.rows);
        columns.ts = (columns.ts ?? []).map((value) => new Date(value as number));
        await db.insertBatch("events_wide", { columns });
      }
      for (const d of generator.dimTables()) {
        await db.execute(`CREATE TABLE ${d.name} ${WIDE_DDL_DIM}`);
        await db.insertBatch(d.name, {
          columns: {
            k: d.rows.map((r) => r.key),
            label: d.rows.map((r) => r.label),
            tier: d.rows.map((r) => r.tier),
            weight: d.rows.map((r) => r.weight),
            active: d.rows.map((r) => r.active),
          },
        });
      }
    } else {
      const star = (await import(
        pathToFileURL(join(repository, "apps/site/bench/benchmark.ts")).href
      )) as Star;
      const catalogue = (await import(
        pathToFileURL(join(workload, "queries.mjs")).href
      )) as Catalogue;
      for (const entity of star.commerceEntities.filter((e) =>
        catalogue.S_TABLES.includes(e.name),
      )) {
        const count = entity.rows(data.scale);
        await db.execute(
          `CREATE TABLE ${entity.name} (${entity.columns.map((c) => `${c.name} ${SQL_TYPES[c.type]}${c.name === entity.primaryKey ? " PRIMARY KEY" : ""}`).join(",")})`,
        );
        for (let at = 0; at < count; at += 65536) {
          signal.throwIfAborted();
          await db.insertBatch(entity.name, {
            columns: star.generateEntityBatch(
              entity,
              at,
              Math.min(65536, count - at),
              count,
              data.scale,
            ),
          });
        }
      }
    }
  } catch (error) {
    await db.close();
    store.close();
    throw error;
  }
  return {
    async run(sql, _boundary, querySignal, consume) {
      let rows = 0;
      for await (const frame of db.queryCursor(sql, {
        memoize: false,
        batchRows: 2048,
        executionMemoryBudgetBytes: data.capBytes,
        signal: querySignal,
      })) {
        rows += frame.rows.length;
        if (consume !== undefined) for (const row of frame.rows) consume(frame.columns, row);
      }
      return rows;
    },
    async close() {
      await db.close();
      store.close();
    },
  };
}

export default async function create(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const data = context.data as HeadToHeadData;
  const catalogue = (await import(pathToFileURL(join(workload, "queries.mjs")).href)) as Catalogue;
  const queries = new Map(
    [...catalogue.W_QUERIES, ...catalogue.S_QUERIES].map((query) => [query.id, query]),
  );
  for (const id of data.workloads) if (!queries.has(id)) throw new Error(`Unknown cell ${id}`);
  const started = performance.now();
  // The wide fixture's generator, for checking rows an unordered LIMIT may pick.
  let expectedRow: ((id: number) => Record<string, unknown>) | null = null;
  if (data.dataset === "W1M") {
    const generator = (await import(
      pathToFileURL(join(workload, "gen.mjs")).href
    )) as WideGenerator;
    const spec = generator.wideSpec(200);
    expectedRow = (id) => {
      // Row index i carries id i + 1.
      const batch = generator.wideBatch(spec, id - 1, 1, data.rows);
      return Object.fromEntries(Object.entries(batch).map(([name, values]) => [name, values[0]]));
    };
  }
  const engine =
    data.engine === "next"
      ? await nextEngine(data, context.signal)
      : await shippedEngine(data, context.signal);
  const loadMs = performance.now() - started;
  let active: AbortController | null = null;
  const execute = async (
    id: string,
    boundary: string,
    consume?: (columns: readonly string[], row: Record<string, unknown>) => void,
  ) => {
    const query = queries.get(id);
    if (query === undefined) throw new Error(`Unknown cell ${id}`);
    const controller = new AbortController();
    active = controller;
    const timer = setTimeout(
      () => controller.abort(new Error(`deadline ${data.deadlineMs} ms`)),
      data.deadlineMs,
    );
    try {
      return await engine.run(
        sqlFor(catalogue, query, data.rows),
        boundary,
        controller.signal,
        consume,
      );
    } finally {
      clearTimeout(timer);
      active = null;
    }
  };
  return {
    revision: context.revision,
    identity: {
      engine: data.engine,
      root: data.root,
      dataset: data.dataset,
      image: data.image ?? null,
      cpu: cpus()[0]?.model ?? "unknown",
      loadMs,
      memoryAfterLoad: process.memoryUsage(),
    },
    workloads: data.workloads,
    boundaries: data.engine === "next" ? ["rows", "frames"] : ["rows"],
    async verify(id) {
      // Every value of the full result (lib/answers.mts): inline with tolerance when small,
      // an exact ordered or multiset hash when large. An unordered LIMIT may return any rows,
      // so each returned row is checked against the generator instead.
      const query = queries.get(id);
      if (query?.check === "count") {
        if (expectedRow === null) throw new Error(`No fixture check for ${id}`);
        const returned: Array<Record<string, unknown>> = [];
        let names: readonly string[] = [];
        const rows = await execute(id, "rows", (columns, row) => {
          names = columns;
          returned.push(row);
        });
        const answer = checkFixtureRows(returned, names, expectedRow);
        return {
          fingerprint: `fixture:${String(rows)}:${String(answer.mismatches?.length ?? 0)}`,
          rows,
          answer,
        };
      }
      const collector = new AnswerCollector(query?.ordered === true);
      const rows = await execute(id, "rows", (columns, row) => {
        collector.add(columns.map((column) => row[column]));
      });
      const answer = collector.finish();
      const fingerprint =
        answer.hash ?? createHash("sha256").update(JSON.stringify(answer.values)).digest("hex");
      return { fingerprint, rows, answer };
    },
    async run(id, boundary) {
      // One bracket: a fixed number of complete executions, identical for both arms.
      const repeats = data.repeats?.[id] ?? 1;
      let rows = 0;
      for (let k = 0; k < repeats; k++) rows = await execute(id, boundary);
      return { rows, detail: { repeats } };
    },
    async command(name, value) {
      if (name === "capture-image") {
        const { directory, identity } = value as { directory: string; identity: string };
        if (engine.capture === undefined) throw new Error("This engine cannot capture images");
        return engine.capture(directory, identity, context.signal);
      }
      if (name === "time-cells") {
        // One complete frames execution per cell, wall clock, to size brackets.
        const times: Record<string, number> = {};
        for (const id of data.workloads) {
          const started = performance.now();
          await execute(id, "frames");
          times[id] = performance.now() - started;
        }
        return times;
      }
      throw new Error(`Unknown worker command ${name}`);
    },
    cancel(reason) {
      active?.abort(reason);
    },
    close: () => engine.close(),
  };
}
