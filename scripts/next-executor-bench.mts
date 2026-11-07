/** Node only: real committed storage -> SQL lowering -> executor -> materialized rows.
 * --frozen replays committed SQL verbatim, including captured row-bound literals. Gate B's
 * 1M join/window cells have no size-dependent SQL literals; select them with --only.
 */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { join, resolve } from "node:path";
import { cpus, homedir, loadavg } from "node:os";
import { pathToFileURL } from "node:url";
import { Session } from "node:inspector/promises";
import { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import { inputBatch, type InputValue } from "../packages/core/dist/next/testing/executor-input.js";
import { oracleRows } from "../packages/core/dist/next/testing/differential-adapter.js";
import { PlanningError } from "../packages/core/dist/next/lower/errors.js";
import { CATALOGUE_AUTHORITY_REFUSALS } from "../packages/core/dist/next/testing/divergences.js";
import { UnsupportedPlanError } from "../packages/core/dist/next/errors.js";
import { BATCH_ROWS, type Batch } from "../packages/core/dist/next/vector/vector.js";

interface Column {
  readonly name: string;
  readonly pg: string;
  readonly fam: string;
}
interface CatalogueQuery {
  readonly id: string;
  readonly sql: string;
}
interface Generator {
  wideSpec(columns: number): Column[];
  wideBatch(
    spec: Column[],
    start: number,
    count: number,
    total: number,
  ): Record<string, InputValue[]>;
  dimTables(): Array<{
    name: string;
    rows: Array<{ key: string; label: string; tier: string; weight: number; active: boolean }>;
  }>;
}
interface Catalogue {
  W_QUERIES: CatalogueQuery[];
  S_QUERIES: CatalogueQuery[];
  S_TABLES: string[];
  sqlFor(query: CatalogueQuery, engine: string, rows: number): string;
}
const args = process.argv.slice(2);
const option = (name: string, fallback: string) => {
  const k = args.indexOf(name);
  return k < 0 ? fallback : (args[k + 1] ?? fallback);
};
const total = Number(option("--rows", "1000000")),
  rounds = Number(option("--repeats", "5"));
if (!Number.isSafeInteger(total) || total < 1 || !Number.isSafeInteger(rounds) || rounds < 5)
  throw new RangeError("positive rows and at least five CPU rounds required");
const harness = resolve(
  option("--harness", join(homedir(), ".local/share/minnow-plan/analytics-harness")),
);
const only = new Set(option("--only", "").split(",").filter(Boolean));
const profileId = option("--profile", "");
const frozenSQL = args.includes("--frozen");
const dataset = option("--dataset", "wide"),
  scale = Number(option("--scale", "333"));
if (dataset !== "wide" && dataset !== "star" && dataset !== "retail")
  throw new Error("--dataset must be wide, star or retail");
if (dataset === "retail" && !frozenSQL) throw new Error("retail requires --frozen");
if (!Number.isFinite(scale) || scale <= 0) throw new RangeError("positive star scale required");
const cache = resolve(option("--output", join(homedir(), ".cache/b4-executor/baseline")));
if (!cache.startsWith(join(homedir(), ".cache/")))
  throw new Error("benchmark output must be under ~/.cache/");
await mkdir(cache, { recursive: true });
const committed = frozenSQL
  ? (JSON.parse(
      await readFile(resolve("packages/core/src/next/testing/executor-catalogue.json"), "utf8"),
    ) as {
      queries: Array<CatalogueQuery & { dataset: string }>;
      datasets: Record<
        string,
        Array<{
          name: string;
          ddl: string;
          rows: Array<Record<string, boolean | string | number | null>>;
        }>
      >;
    })
  : null;
const generator = (await import(pathToFileURL(join(harness, "gen.js")).href)) as Generator;
const catalogue = (await import(pathToFileURL(join(harness, "queries.js")).href)) as Catalogue;
const core = await new StorageCore(8192).open();
const spec = generator.wideSpec(200);
const metadata = {
  lane: "Node",
  revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  storage: "MemoryBackend + extent space + WritePathImpl, strict commits, flushed",
  dataset,
  rows: dataset === "wide" ? total : null,
  columns: dataset === "wide" ? 200 : null,
  scale: dataset === "star" ? scale : null,
  rounds,
  fixtureRowCounts:
    dataset === "retail"
      ? Object.fromEntries(
          (committed?.datasets.retail ?? []).map((table) => [table.name, table.rows.length]),
        )
      : null,
  timing:
    "process user+system CPU; SQL/lower/prepare/bind/pin/execute/rows/credit/cleanup included; ingestion excluded",
  node: process.version,
  cpuModel: cpus()[0]?.model,
  governorCapBytes: core.harness.governor.capBytes,
  sourceHashes: Object.fromEntries(
    await Promise.all(
      [
        join(harness, "gen.js"),
        join(harness, "queries.js"),
        resolve("apps/site/bench/benchmark.ts"),
        resolve("packages/core/src/next/testing/executor-catalogue.json"),
      ].map(async (file) => [
        file,
        createHash("sha256")
          .update(await readFile(file))
          .digest("hex"),
      ]),
    ),
  ),
  load: loadavg(),
  profileId,
  sqlSource: frozenSQL
    ? "committed executor-catalogue.json (unchanged SQL)"
    : "analytics-harness/queries.js",
  harness,
};
console.log(JSON.stringify(metadata));
const beforeLoad = process.cpuUsage();
try {
  if (dataset === "wide") {
    await core.execute(
      `CREATE TABLE events_wide (${spec.map((column) => `${column.name} ${column.pg}`).join(",")})`,
    );
    const table = core.harness.path.catalog.tableByName("events_wide");
    if (table === undefined) throw new Error("wide table was not created");
    const grant = await core.harness.governor.admit({
      label: "bench/input",
      minimumBytes: 8 * 1024 * 1024,
      spillable: true,
    });
    try {
      for (let start = 0; start < total; start += BATCH_ROWS) {
        const source = generator.wideBatch(spec, start, Math.min(BATCH_ROWS, total - start), total);
        const input = inputBatch(table, source, grant);
        try {
          await core.insertVectors(table, input.batch);
        } finally {
          input.close();
        }
        if (start % (BATCH_ROWS * 50) === 0)
          console.error(
            JSON.stringify({ loaded: start + input.batch.length, total, load: loadavg() }),
          );
      }
    } finally {
      grant.close();
    }
    for (const dimension of generator.dimTables()) {
      await core.execute(
        `CREATE TABLE ${dimension.name} (k TEXT PRIMARY KEY,label TEXT,tier TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
      );
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
  } else if (dataset === "retail") {
    // The docs catalogue has a frozen fixture rather than a scale parameter. Keep it exact;
    // these small-fixture timings are not comparable to the wide/star Gate B targets.
    for (const fixture of committed?.datasets.retail ?? []) {
      await core.execute(fixture.ddl);
      const table = core.harness.path.catalog.tableByName(fixture.name);
      if (table === undefined) throw new Error("retail table was not created");
      if (fixture.rows.length > 0)
        await core.writeBatch(
          "insertBatch",
          fixture.name,
          fixture.rows.map((row) =>
            Object.fromEntries(
              table.columns.map((column) => {
                const value = row[column.name] ?? null;
                return [
                  column.name,
                  value !== null && column.type.kind === "timestamp"
                    ? new Date(String(value))
                    : value,
                ];
              }),
            ),
          ),
        );
      console.error(
        JSON.stringify({ loadedTable: fixture.name, rows: fixture.rows.length, load: loadavg() }),
      );
    }
  } else {
    const { commerceEntities, generateEntityBatch } =
      await import("../apps/site/bench/benchmark.ts");
    const sqlTypes = {
      number: "DOUBLE PRECISION",
      string: "TEXT",
      boolean: "BOOLEAN",
      datetime: "TIMESTAMP WITH TIME ZONE",
    };
    for (const entity of commerceEntities.filter((entity) =>
      catalogue.S_TABLES.includes(entity.name),
    )) {
      const count = entity.rows(scale);
      await core.execute(
        `CREATE TABLE ${entity.name} (${entity.columns.map((column) => `${column.name} ${sqlTypes[column.type]}${column.name === entity.primaryKey ? " PRIMARY KEY" : ""}`).join(",")})`,
      );
      const table = core.harness.path.catalog.tableByName(entity.name);
      if (table === undefined) throw new Error("star table was not created");
      const grant = await core.harness.governor.admit({
        label: `bench/input/${entity.name}`,
        minimumBytes: 1024 * 1024,
        spillable: true,
      });
      try {
        for (let start = 0; start < count; start += BATCH_ROWS) {
          const source = generateEntityBatch(
            entity,
            start,
            Math.min(BATCH_ROWS, count - start),
            count,
            scale,
          );
          const input = inputBatch(table, source, grant);
          try {
            await core.insertVectors(table, input.batch);
          } finally {
            input.close();
          }
        }
      } finally {
        grant.close();
      }
      console.error(
        JSON.stringify({ loadedTable: entity.name, rows: count, scale, load: loadavg() }),
      );
    }
  }
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
  const loadCpu = process.cpuUsage(beforeLoad);
  console.log(
    JSON.stringify({
      ingestionCpuMs: (loadCpu.user + loadCpu.system) / 1000,
      governorPeak: core.harness.governor.peakBytes,
      load: loadavg(),
    }),
  );
  const sourceQueries =
    committed === null
      ? dataset === "wide"
        ? catalogue.W_QUERIES
        : catalogue.S_QUERIES
      : committed.queries.filter((query) => query.dataset === dataset);
  const queries = sourceQueries.filter((query) => only.size === 0 || only.has(query.id));
  const records: unknown[] = [];
  for (const query of queries) {
    const sql = frozenSQL ? query.sql : catalogue.sqlFor(query, "minnow", total);
    try {
      core.prepare(sql);
    } catch (error) {
      if (
        error instanceof PlanningError &&
        query.id in CATALOGUE_AUTHORITY_REFUSALS &&
        error.sqlState === "42883"
      ) {
        const record = {
          id: query.id,
          status: "authority-refusal",
          sqlState: error.sqlState,
          message: error.message,
        };
        records.push(record);
        console.log(JSON.stringify(record));
        continue;
      }
      if (!(error instanceof UnsupportedPlanError)) throw error;
      const record = {
        id: query.id,
        status: "unsupported",
        stage: error.stage,
        construct: error.construct,
        message: error.message,
      };
      records.push(record);
      console.log(JSON.stringify(record));
      continue;
    }
    const consume = async () => {
      const prepared = core.prepare(sql),
        run = core.start(prepared);
      let rows = 0,
        checksum = 0;
      try {
        for (;;) {
          const frame = await run.execution.results.next();
          if (frame === null) break;
          try {
            const batch: Batch = {
              length: frame.payload.rowCount,
              selection: null,
              columns: frame.payload.columns.map((vector, index) => {
                if (vector.form !== "flat") throw new Error("unexpected encoded result frame");
                const type = prepared.columns[index]?.type;
                if (type === undefined) throw new Error("result type missing");
                return { ...vector, type };
              }),
            };
            const decoded = oracleRows({ columns: prepared.columns, batches: [batch] });
            rows += decoded.length;
            for (const row of decoded) {
              const firstName = prepared.columns[0]?.name;
              const first = firstName === undefined ? undefined : row[firstName];
              checksum +=
                typeof first === "number" ? first : typeof first === "string" ? first.length : 0;
            }
          } finally {
            frame.release();
          }
        }
        const summary = await run.execution.executed;
        await run.execution.delivered;
        return { rows, checksum, profile: summary.profile };
      } catch (error) {
        run.execution.cancel(error);
        await Promise.allSettled([run.execution.executed, run.execution.delivered]);
        throw error;
      } finally {
        run.release();
      }
    };
    try {
      console.error(JSON.stringify({ id: query.id, phase: "warmup", load: loadavg() }));
      const warm = await consume();
      const samples: number[] = [],
        wall: number[] = [];
      let last = warm;
      for (let round = 0; round < rounds; round++) {
        const cpu = process.cpuUsage(),
          started = performance.now();
        last = await consume();
        const elapsed = process.cpuUsage(cpu);
        samples.push((elapsed.user + elapsed.system) / 1000);
        wall.push(performance.now() - started);
        console.error(
          JSON.stringify({
            id: query.id,
            round,
            cpuMs: samples.at(-1),
            wallMs: wall.at(-1),
            rows: last.rows,
            load: loadavg(),
          }),
        );
        if (last.rows !== warm.rows || last.checksum !== warm.checksum)
          throw new Error("baseline result changed across CPU rounds");
      }
      if (profileId === query.id) {
        const profiler = new Session();
        profiler.connect();
        await profiler.post("Profiler.enable");
        await profiler.post("Profiler.setSamplingInterval", { interval: 100 });
        await profiler.post("Profiler.start");
        try {
          await consume();
        } finally {
          const { profile } = await profiler.post("Profiler.stop");
          profiler.disconnect();
          await writeFile(join(cache, `${query.id}.cpuprofile`), JSON.stringify(profile));
        }
      }
      const sorted = [...samples].sort((a, b) => a - b),
        median = sorted[sorted.length >>> 1] ?? NaN;
      const record = {
        id: query.id,
        status: "executed",
        cpuMs: {
          median,
          spread: (sorted.at(-1) ?? 0) - (sorted[0] ?? 0),
          min: sorted[0],
          max: sorted.at(-1),
          samples,
        },
        wallMs: wall,
        rows: last.rows,
        checksum: last.checksum,
        profile: last.profile,
        load: loadavg(),
      };
      records.push(record);
      console.log(JSON.stringify(record));
    } catch (error) {
      const record = {
        id: query.id,
        status: "failed",
        error: error instanceof Error ? error.name : String(error),
        message: error instanceof Error ? error.message : String(error),
      };
      records.push(record);
      console.log(JSON.stringify(record));
    }
  }
  await writeFile(join(cache, "baseline.json"), JSON.stringify({ ...metadata, records }, null, 2));
} finally {
  await core.close();
}
