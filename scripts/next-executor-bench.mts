/** Node only: real committed storage -> SQL lowering -> executor -> materialized rows.
 * --frozen replays committed SQL verbatim, including captured row-bound literals. Gate B's
 * 1M join/window cells have no size-dependent SQL literals; select them with --only.
 * --profile all records one setup-excluded V8 region per executing cell after CPU rounds.
 * --compare-window-inputs / --compare-window-scalars alternate parent and candidate,
 * hash every output cell, and record per-run CPU, wall time and load (use --repeats 7).
 * Run under node --cpu-prof as well for a separately labelled whole-process diagnostic.
 */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { join, resolve } from "node:path";
import { cpus, loadavg, tmpdir } from "node:os";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Session } from "node:inspector/promises";
import {
  StorageCore,
  expandTextDictionary,
} from "../packages/core/dist/next/testing/storage-core.js";
import { inputBatch, type InputValue } from "../packages/core/dist/next/testing/executor-input.js";
import { oracleRows } from "../packages/core/dist/next/testing/differential-adapter.js";
import { createExecutor } from "../packages/core/dist/next/exec/create.js";
import type { WireFlat } from "../packages/core/dist/next/owner/frame.js";
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
  option("--harness", fileURLToPath(new URL("./next-executor-workload/", import.meta.url))),
);
const generatorPath = join(harness, args.includes("--harness") ? "gen.js" : "gen.mjs"),
  cataloguePath = join(harness, args.includes("--harness") ? "queries.js" : "queries.mjs");
const only = new Set(option("--only", "").split(",").filter(Boolean));
const profileId = option("--profile", "");
const frozenSQL = args.includes("--frozen");
const compareScalars = args.includes("--compare-window-scalars");
const compareInputs = args.includes("--compare-window-inputs") || compareScalars;
const parentExecutor = createExecutor(
  compareScalars ? { windowScalarArguments: false } : { windowInputDeduplication: false },
);
const dataset = option("--dataset", "wide"),
  scale = Number(option("--scale", "333"));
if (dataset !== "wide" && dataset !== "star" && dataset !== "retail")
  throw new Error("--dataset must be wide, star or retail");
if (dataset === "retail" && !frozenSQL) throw new Error("retail requires --frozen");
if (!Number.isFinite(scale) || scale <= 0) throw new RangeError("positive star scale required");
const cache = resolve(option("--output", join(tmpdir(), "b4-executor/baseline")));
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
const generator = (await import(pathToFileURL(generatorPath).href)) as Generator;
const catalogue = (await import(pathToFileURL(cataloguePath).href)) as Catalogue;
const core = await new StorageCore(8192).open();
const lateSort = option("--late-sort", "on") !== "off";
const keyOrder = option("--key-order", "on") !== "off";
core.executor.prepare = createExecutor({ lateSort, keyOrder }).prepare;
const spec = generator.wideSpec(200);
const metadata = {
  lane: "Node",
  sqlCorrections: frozenSQL ? ["S_q7", "guide-4", "guide-5"] : dataset === "star" ? ["S_q7"] : [],
  lateSort,
  keyOrder,
  revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  workingTreePatchSha256: createHash("sha256")
    .update(execFileSync("git", ["diff", "HEAD"]))
    .digest("hex"),
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
        resolve("scripts/next-executor-bench.mts"),
        resolve("scripts/next-executor-components.mts"),
        resolve("scripts/next-late-fetch-component.mts"),
        resolve("scripts/next-aggregation-components.mts"),
        resolve("scripts/next-scalar-distinct-bench.mts"),
        resolve("scripts/next-aggregation-light.mts"),
        resolve("scripts/lib/aggregation-light-kernels.mts"),
        resolve("packages/core/dist/next/hash/xxhash32.js"),
        generatorPath,
        cataloguePath,
        resolve("apps/site/bench/benchmark.ts"),
        resolve("packages/core/src/next/testing/executor-catalogue.json"),
        resolve("scripts/next-executor-bench.mts"),
        ...[
          "plan",
          "limit",
          "table-scan",
          "memory-scan",
          "key-order",
          "late-fetch",
          "agg/aggregate",
          "agg/group-top-n",
          "agg/groups",
          "agg/states",
          "agg/packed-averages",
          "agg/packed-integers",
          "agg/distinct",
          "agg/scalar-byte-distinct",
        ].map((name) => resolve(`packages/core/dist/next/exec/${name}.js`)),
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
  execArgv: process.execArgv,
  sqlSource: frozenSQL
    ? "committed executor-catalogue.json (three PG-valid ROUND corrections; frozen data)"
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
    const sourceSQL = frozenSQL ? query.sql : catalogue.sqlFor(query, "minnow", total);
    // The frozen harness predates the authorized PG-valid S_q7 correction.
    const sql =
      !frozenSQL && query.id === "S_q7"
        ? sourceSQL.replace("ROUND(revenue, 4)", "ROUND(revenue::numeric, 4)")
        : sourceSQL;
    try {
      core.prepare(sql);
    } catch (error) {
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
    const consume = async (parent = false) => {
      const lowered = core.lowerer.lower(sql, core.context());
      if (lowered.kind !== "query") throw new Error("expected query");
      const prepared = (parent ? parentExecutor : core.executor).prepare(lowered.plan),
        run = core.start(prepared);
      const digest = createHash("sha256");
      const dictionaries = new Map<number, WireFlat[]>();
      let rows = 0,
        checksum = 0;
      try {
        for (;;) {
          const frame = await run.execution.results.next();
          if (frame === null) break;
          try {
            for (const delta of frame.payload.dictionaries) {
              const parts = dictionaries.get(delta.id) ?? [];
              if (delta.from !== parts.reduce((size, part) => size + part.length, 0))
                throw new RangeError("out-of-order result dictionary delta");
              parts.push(structuredClone(delta.values));
              dictionaries.set(delta.id, parts);
            }
            // Match StorageCore's tested materialization. Prepare descriptors once per frame,
            // retain owned dictionary deltas across releases, and preserve the flat fast path.
            const batch: Batch = {
              length: frame.payload.rowCount,
              selection: null,
              columns: frame.payload.columns.map((vector, index) => {
                const flat =
                  vector.form === "dictionary"
                    ? expandTextDictionary(vector, dictionaries.get(vector.dictionary) ?? [])
                    : vector;
                if (flat.form !== "flat") throw new Error("unexpected constant result frame");
                const type = prepared.columns[index]?.type;
                if (type === undefined) throw new Error("result type missing");
                return { ...flat, type };
              }),
            };
            const decoded = oracleRows({ columns: prepared.columns, batches: [batch] });
            rows += decoded.length;
            for (const row of decoded) {
              digest.update(
                JSON.stringify(row, (_, value: unknown) =>
                  typeof value === "bigint"
                    ? { bigint: String(value) }
                    : typeof value === "number" && !Number.isFinite(value)
                      ? { number: String(value) }
                      : value,
                ),
              );
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
        return { rows, checksum, digest: digest.digest("hex"), profile: summary.profile };
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
      if (compareInputs) {
        const parentWarm = await consume(true);
        if (warm.digest !== parentWarm.digest)
          throw new Error("different parent/candidate results");
        const samples: Array<{ variant: string; cpuMs: number; wallMs: number; load: number[] }> =
          [];
        for (let round = 0; round < rounds; round++) {
          for (const parent of round % 2 === 0 ? [true, false] : [false, true]) {
            const load = loadavg(),
              cpu = process.cpuUsage(),
              start = performance.now();
            const result = await consume(parent);
            const used = process.cpuUsage(cpu);
            if (result.digest !== warm.digest || result.rows !== warm.rows)
              throw new Error("different complete output across paired rounds");
            const sample = {
              variant: parent ? "parent" : "candidate",
              cpuMs: (used.user + used.system) / 1000,
              wallMs: performance.now() - start,
              load,
            };
            samples.push(sample);
            console.error(JSON.stringify({ id: query.id, round, ...sample }));
          }
        }
        const record = {
          id: query.id,
          status: "paired",
          rows: warm.rows,
          digest: warm.digest,
          samples,
        };
        records.push(record);
        console.log(JSON.stringify(record));
        continue;
      }
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
      if (profileId === "all" || profileId.split(",").includes(query.id)) {
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
      let wholeComparison = null;
      if (
        [
          "late-sort",
          "key-order",
          "late-fetch",
          "group-top-n",
          "grouped-distinct",
          "scalar-distinct",
          "grouped-pairs",
          "wide-typed",
          "aggregation",
        ].includes(option("--whole-compare", "")) &&
        (option("--whole-compare", "") !== "late-fetch" || query.id === "sort_topn_wide")
      ) {
        const saved = core.executor.prepare;
        const strategies = [
          createExecutor(
            option("--whole-compare", "") === "wide-typed"
              ? { packedAverages: false, packedIntegers: false, batchDictionaryMinMax: false }
              : option("--whole-compare", "") === "grouped-pairs"
                ? { groupedUpdateFusion: false }
                : option("--whole-compare", "") === "scalar-distinct"
                  ? { scalarByteDistinct: false }
                  : option("--whole-compare", "") === "aggregation"
                    ? { groupTopN: false, groupedDistinctBitmap: false }
                    : option("--whole-compare", "") === "grouped-distinct"
                      ? { groupedDistinctBitmap: false }
                      : option("--whole-compare", "") === "group-top-n"
                        ? { groupTopN: false }
                        : option("--whole-compare", "") === "key-order"
                          ? { keyOrder: false }
                          : option("--whole-compare", "") === "late-fetch"
                            ? { lateFetchOnce: false }
                            : { lateSort: false },
          ),
          createExecutor(
            option("--whole-compare", "") === "wide-typed"
              ? { packedAverages: true, packedIntegers: true, batchDictionaryMinMax: true }
              : option("--whole-compare", "") === "key-order"
                ? { keyOrder: true }
                : {},
          ),
        ];
        const before: number[] = [],
          after: number[] = [],
          order: string[] = [];
        let beforeProfile = last.profile,
          afterProfile = last.profile;
        try {
          for (const strategy of strategies) {
            core.executor.prepare = strategy.prepare;
            const result = await consume();
            if (result.rows !== warm.rows || result.checksum !== warm.checksum)
              throw new Error("whole-query comparison warmups differ");
          }
          if (profileId.split(",").includes(query.id)) {
            for (let index = 0; index < strategies.length; index++) {
              const strategy = strategies[index];
              if (!strategy) throw new Error("profile policy");
              core.executor.prepare = strategy.prepare;
              const profiler = new Session();
              profiler.connect();
              await profiler.post("Profiler.enable");
              await profiler.post("Profiler.setSamplingInterval", { interval: 100 });
              await profiler.post("Profiler.start");
              const result = await consume();
              const { profile } = await profiler.post("Profiler.stop");
              profiler.disconnect();
              if (result.rows !== warm.rows || result.checksum !== warm.checksum)
                throw new Error("profile policy changed output");
              await writeFile(
                join(cache, `${query.id}.${index === 0 ? "before" : "after"}.cpuprofile`),
                JSON.stringify(profile),
              );
            }
          }
          for (let round = 0; round < rounds; round++)
            for (const late of round % 2 === 0 ? [false, true] : [true, false]) {
              const strategy = strategies[late ? 1 : 0];
              if (!strategy) throw new Error("missing comparison strategy");
              core.executor.prepare = strategy.prepare;
              const started = process.cpuUsage();
              const result = await consume();
              const used = process.cpuUsage(started);
              (late ? after : before).push((used.user + used.system) / 1000);
              if (result.rows !== warm.rows || result.checksum !== warm.checksum)
                throw new Error("whole-query comparison lost rows or changed checksum");
              if (late) afterProfile = result.profile;
              else beforeProfile = result.profile;
              order.push(
                option("--whole-compare", "") === "wide-typed"
                  ? late
                    ? "packed-averages-batch-dictionary"
                    : "separate-states"
                  : option("--whole-compare", "") === "grouped-pairs"
                    ? late
                      ? "fused-grouped-pairs"
                      : "separate-grouped-updates"
                    : option("--whole-compare", "") === "scalar-distinct"
                      ? late
                        ? "scalar-byte-membership"
                        : "scalar-value-interning"
                      : option("--whole-compare", "") === "aggregation"
                        ? late
                          ? "aggregation-default"
                          : "aggregation-control"
                        : option("--whole-compare", "") === "grouped-distinct"
                          ? late
                            ? "bitmaps-enabled"
                            : "pair-set-control"
                          : option("--whole-compare", "") === "group-top-n"
                            ? late
                              ? "selected-groups"
                              : "all-groups"
                            : option("--whole-compare", "") === "key-order"
                              ? late
                                ? "index-cursor"
                                : "scan-sort-locators"
                              : option("--whole-compare", "") === "late-fetch"
                                ? late
                                  ? "one-pass-payload"
                                  : "cached-payload"
                                : late
                                  ? "keys-and-locators"
                                  : "full-payload",
              );
              console.error(
                JSON.stringify({ id: query.id, comparison: true, round, late, load: loadavg() }),
              );
            }
        } finally {
          core.executor.prepare = saved;
        }
        const distribution = (values: readonly number[]) => {
          const sorted = [...values].sort((a, b) => a - b);
          return {
            samples: values,
            median: sorted[sorted.length >>> 1] ?? NaN,
            spread: (sorted.at(-1) ?? 0) - (sorted[0] ?? 0),
          };
        };
        const baseline = distribution(before),
          candidate = distribution(after);
        wholeComparison = {
          lane: "Node",
          strategy: option("--whole-compare", ""),
          rounds,
          order,
          timing: metadata.timing,
          before: { ...baseline, profile: beforeProfile },
          after: { ...candidate, profile: afterProfile },
          medianDeltaMs: baseline.median - candidate.median,
          // The acceptance rule uses the parent's spread, even when the candidate is noisier.
          regressionWithinParentSpread: candidate.median <= baseline.median + baseline.spread,
        };
      }
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
        wholeComparison,
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
  // Run components only AFTER whole-query timing/profile, so their extra warmups cannot bias it.
  if (["aggregation", "aggregation-light"].includes(option("--component", ""))) {
    const { aggregationComponents } = await import(
      pathToFileURL(resolve("scripts/next-aggregation-components.mts")).href
    );
    const components = await aggregationComponents(
      core,
      rounds,
      queries.map((query) => ({
        id: query.id,
        sql: catalogue.sqlFor(query, "minnow", total),
      })),
      option("--component", "") === "aggregation-light" ? cache : undefined,
    );
    await writeFile(join(cache, "component-aggregation.json"), JSON.stringify(components, null, 2));
    console.log(JSON.stringify(components));
  }

  if (option("--component", "") === "scan-limit") {
    const { scanLimitComponent } = await import(
      pathToFileURL(resolve("scripts/next-executor-components.mts")).href
    );
    const component = await scanLimitComponent(
      core,
      rounds,
      option("--component-profile", "") === "1"
        ? join(cache, "scan-limit-full-batch.cpuprofile")
        : null,
    );
    await writeFile(join(cache, "component-scan-limit.json"), JSON.stringify(component, null, 2));
    console.log(JSON.stringify(component));
  }
  if (["late-sort", "key-order", "late-fetch", "group-top-n"].includes(option("--component", ""))) {
    const { lateSortComponent } = await import(
      pathToFileURL(resolve("scripts/next-executor-components.mts")).href
    );
    const kind = option("--component", "late-sort");
    const component = await lateSortComponent(core, rounds, kind);
    await writeFile(join(cache, `component-${kind}.json`), JSON.stringify(component, null, 2));
    console.log(JSON.stringify(component));
  }
} finally {
  await core.close();
}
