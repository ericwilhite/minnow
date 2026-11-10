/** Node compiled-dist paired sort bench. Input/validation are untimed; both result boundaries
 * are reported. --fixture <v8 capture> selects the isolated operator; absent it loads W1M.
 * TMPDIR controls scratch. No benchmark assertion is a substitute for generated PG suites.
 */
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { deserialize } from "node:v8";
import { createHash } from "node:crypto";
import { loadavg, tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { execFileSync } from "node:child_process";
import { pairedCPU } from "./lib/paired-cpu.mts";
import { Governor } from "../packages/core/dist/next/memory/governor.js";
import { MemorySpillService } from "../packages/core/dist/next/spill/memory-spill.js";
import { SortOperator, sortMinimumBytes } from "../packages/core/dist/next/exec/sort.js";
import { drive } from "../packages/core/dist/next/exec/pipeline.js";
import { ArraySource } from "../packages/core/dist/next/exec/join-fixtures.js";
import { createExecutor } from "../packages/core/dist/next/exec/create.js";
import {
  StorageCore,
  expandTextDictionary,
} from "../packages/core/dist/next/testing/storage-core.js";
import { inputBatch, type InputValue } from "../packages/core/dist/next/testing/executor-input.js";
import { oracleRows } from "../packages/core/dist/next/testing/differential-adapter.js";
import {
  BATCH_ROWS,
  type Batch,
  type FlatVector,
} from "../packages/core/dist/next/vector/vector.js";
import type { PhysicalType } from "../packages/core/dist/next/types.js";
import {
  ResultChannel,
  type ResultStream,
  type WireFlat,
} from "../packages/core/dist/next/exec/result.js";
import { ResultSink } from "../packages/core/dist/next/exec/result-sink.js";

const args = process.argv.slice(2);
const option = (key: string, fallback: string) => {
  const at = args.indexOf(key);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
};
const pairs = Number(option("--pairs", "9")),
  rows = Number(option("--rows", "1000000"));
if (!Number.isSafeInteger(pairs) || pairs < 9 || !Number.isSafeInteger(rows) || rows < 1)
  throw new RangeError("Nine or more pairs and positive rows required");
const output = resolve(option("--output", join(tmpdir(), "minnow-columnar-sort.json")));
await mkdir(resolve(output, ".."), { recursive: true });
type Boundary = "frames" | "rows" | "verify";
interface Result {
  rows: number;
  digest: string;
  columnarRows?: number;
}
const records: unknown[] = [];
const compareStage = option("--compare-stage", "numeric");
if (!["numeric", "ownership"].includes(compareStage))
  throw new RangeError("Unknown comparison stage");
const compileFlags = (candidate: boolean) => ({
  columnarSort: compareStage !== "numeric" || candidate,
  sortOwnership: compareStage !== "ownership" || candidate,
});
const metadata = {
  revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  patchSha256: createHash("sha256")
    .update(execFileSync("git", ["diff", "HEAD"]))
    .digest("hex"),
  node: process.version,
  compareStage,
  componentWithResults: args.includes("--with-results"),
  control: "candidate build, selected path disabled (not a separately built parent binary)",
  candidate: "same candidate build, selected path enabled",
  pairs,
  rows,
  capBytes: 256 << 20,
  timing:
    "all-thread process user+system CPU including GC; input construction and complete SHA validation untimed; frames delivered/released is acceptance, row materialization measured separately",
  sourceHashes: Object.fromEntries(
    await Promise.all(
      [
        "packages/core/dist/next/exec/sort.js",
        "packages/core/dist/next/exec/sort-columnar.js",
        "packages/core/dist/next/exec/sort-columnar-data.js",
        "packages/core/dist/next/exec/plan.js",
        "scripts/next-sort-columnar-bench.mts",
      ].map(async (path) => [
        path,
        createHash("sha256")
          .update(await readFile(path))
          .digest("hex"),
      ]),
    ),
  ),
};
async function save() {
  await writeFile(output, JSON.stringify({ ...metadata, records }, null, 2));
}
function consumer(
  columns: ReadonlyArray<{ readonly name: string; readonly type: PhysicalType }>,
  boundary: Boundary,
) {
  let rows = 0;
  const digest = createHash("sha256");
  return {
    batch(batch: Batch) {
      rows += batch.selection?.count ?? batch.length;
      if (boundary === "frames") return;
      const decoded = oracleRows({
        columns: columns.map((column) => ({ ...column, nullable: true })),
        batches: [batch],
      });
      if (boundary === "verify")
        for (const row of decoded)
          digest.update(
            JSON.stringify(row, (_, v: unknown) =>
              typeof v === "bigint"
                ? String(v)
                : typeof v === "number" && !Number.isFinite(v)
                  ? String(v)
                  : v,
            ),
          );
    },
    result: (): Result => ({ rows, digest: digest.digest("hex") }),
  };
}
async function compare(
  id: string,
  run: (candidate: boolean, boundary: Boundary) => Promise<Result>,
) {
  const first = await run(false, "verify"),
    second = await run(true, "verify");
  if (first.digest !== second.digest || first.rows !== second.rows)
    throw new Error(`${id}: full output mismatch before timing`);
  if (args.includes("--verify-only")) {
    records.push({ id, digest: first.digest, rows: first.rows, verificationOnly: true });
    await save();
    return;
  }
  const samples: Array<{
    candidate: boolean;
    boundary: string;
    round: number;
    cpuMs: number;
    userMs: number;
    systemMs: number;
    wallMs: number;
    load: number[];
    rows: number;
  }> = [];
  const acceptance: Record<string, ReturnType<typeof pairedCPU>> = {};
  for (const boundary of ["frames", "rows"] as const) {
    const before: number[] = [],
      after: number[] = [];
    for (let round = 0; round < pairs; round++)
      for (const candidate of round % 2 === 0 ? [false, true] : [true, false]) {
        const load = loadavg(),
          cpu = process.cpuUsage(),
          wall = performance.now();
        const result = await run(candidate, boundary),
          used = process.cpuUsage(cpu);
        if (result.rows !== first.rows) throw new Error(`${id}: changed row count`);
        const sample = {
          candidate,
          boundary,
          round,
          cpuMs: (used.user + used.system) / 1000,
          userMs: used.user / 1000,
          systemMs: used.system / 1000,
          wallMs: performance.now() - wall,
          load,
          rows: result.rows,
        };
        samples.push(sample);
        (candidate ? after : before).push(sample.cpuMs);
        console.error(JSON.stringify({ id, ...sample }));
      }
    acceptance[boundary] = pairedCPU(before, after);
  }
  for (const candidate of [false, true]) {
    const result = await run(candidate, "verify");
    if (result.digest !== first.digest || result.rows !== first.rows)
      throw new Error(`${id}: full output mismatch after timing`);
  }
  records.push({ id, digest: first.digest, rows: first.rows, samples, acceptance });
  await save();
}

async function consumeFrames(
  stream: Pick<ResultStream, "next">,
  columns: ReadonlyArray<{ type: PhysicalType }>,
  boundary: Boundary,
  sink: ReturnType<typeof consumer>,
) {
  const dictionaries = new Map<number, WireFlat[]>();
  for (;;) {
    const frame = await stream.next();
    if (frame === null) break;
    try {
      if (boundary === "frames")
        sink.batch({ length: frame.payload.rowCount, selection: null, columns: [] });
      else {
        for (const delta of frame.payload.dictionaries) {
          const parts = dictionaries.get(delta.id) ?? [];
          if (delta.from !== parts.reduce((sum, p) => sum + p.length, 0))
            throw new Error("dictionary order");
          parts.push(structuredClone(delta.values));
          dictionaries.set(delta.id, parts);
        }
        sink.batch({
          length: frame.payload.rowCount,
          selection: null,
          columns: frame.payload.columns.map((v, c): FlatVector => {
            const flat =
              v.form === "dictionary"
                ? expandTextDictionary(v, dictionaries.get(v.dictionary) ?? [])
                : v;
            if (flat.form !== "flat") throw new Error("unexpected constant wire frame");
            const type = columns[c]?.type;
            if (!type) throw new Error("missing output type");
            return { ...flat, type };
          }),
        });
      }
    } finally {
      frame.release();
    }
  }
}

function queryRun(core: StorageCore, sql: string) {
  const factories = [createExecutor(compileFlags(false)), createExecutor(compileFlags(true))];
  return async (candidate: boolean, boundary: Boundary): Promise<Result> => {
    const lowered = core.lowerer.lower(sql, core.context());
    if (lowered.kind !== "query") throw new Error("not query");
    const prepared = factories[candidate ? 1 : 0]?.prepare(lowered.plan);
    if (!prepared) throw new Error("missing factory");
    const handle = core.start(prepared),
      sink = consumer(prepared.columns, boundary);
    try {
      await consumeFrames(handle.execution.results, prepared.columns, boundary, sink);
      await handle.execution.executed;
      await handle.execution.delivered;
      return sink.result();
    } catch (error) {
      handle.execution.cancel(error);
      await Promise.allSettled([handle.execution.executed, handle.execution.delivered]);
      throw error;
    } finally {
      handle.release();
    }
  };
}

interface NumericInput {
  metadata: { names: string[]; n: number };
  types: PhysicalType[];
  batches: Batch[];
}
async function measureComponent(fixture: NumericInput) {
  const types = fixture.types;
  await compare("numeric-operator", async (candidate, boundary) => {
    const governor = new Governor(256 << 20),
      spill = new MemorySpillService();
    const grant = await governor.admit({
      label: "sort-pair",
      minimumBytes: sortMinimumBytes(types),
      spillable: true,
    });
    const sink = consumer(
      types.map((type, c) => ({ type, name: fixture.metadata.names[c] ?? String(c) })),
      boundary,
    );
    const resultGrant = args.includes("--with-results")
      ? await governor.admit({ label: "result-pair", minimumBytes: 4 << 20, spillable: true })
      : undefined;
    const channel = resultGrant
      ? new ResultChannel(
          types.map((type, c) => ({
            name: fixture.metadata.names[c] ?? String(c),
            type,
            nullable: true,
          })),
          { windowBytes: 2 << 20 },
        )
      : undefined;
    const receiver: import("../packages/core/dist/next/exec/pipeline.js").Operator =
      resultGrant && channel
        ? new ResultSink(types, resultGrant, channel, 0)
        : {
            push: (batch) => {
              sink.batch(batch);
              return "ready";
            },
            ready: () => Promise.resolve(),
            finish: () => undefined,
            abort: () => undefined,
          };
    const op = new SortOperator(
      {
        types,
        keys: [
          { column: 1, direction: "asc", nulls: "last", collation: null },
          { column: 0, direction: "asc", nulls: "last", collation: null },
        ],
        limit: null,
        offset: 0,
        withTies: false,
      },
      {
        grant,
        spill,
        queryId: 1,
        columnar: compareStage !== "numeric" || candidate,
        columnarOwnership: compareStage !== "ownership" || candidate,
      },
      receiver,
    );
    try {
      const feeding = Promise.resolve(drive(new ArraySource(fixture.batches), op)).catch(
        (error: unknown) => {
          channel?.cancel(error);
          throw error;
        },
      );
      if (channel)
        await Promise.all([
          feeding,
          consumeFrames(
            channel,
            types.map((type) => ({ type })),
            boundary,
            sink,
          ),
        ]);
      else await feeding;
      await op.closed;
      resultGrant?.close();
      if (governor.usedBytes !== 0 || spill.bytesInUse !== 0)
        throw new Error("operator leaked memory/spill");
      if (candidate && op.profile.columnarRows !== fixture.metadata.n)
        throw new Error("numeric component fell back");
      return { ...sink.result(), columnarRows: op.profile.columnarRows ?? 0 };
    } finally {
      op.abort(new Error("benchmark finished"));
      await op.closed;
      resultGrant?.close();
    }
  });
}

const fixturePath = option("--fixture", "");
if (fixturePath) {
  await measureComponent(deserialize(await readFile(resolve(fixturePath))) as NumericInput);
} else if (args.includes("--catalogue")) {
  const { CATALOGUE_REFUSALS } = await import("../packages/core/dist/next/testing/divergences.js");
  const fixture = JSON.parse(
    await readFile(
      new URL("../packages/core/src/next/testing/executor-catalogue.json", import.meta.url),
      "utf8",
    ),
  ) as {
    datasets: Record<
      string,
      Array<{ name: string; ddl: string; rows: Array<Record<string, InputValue>> }>
    >;
    queries: Array<{ id: string; dataset: string; sql: string }>;
  };
  for (const [dataset, tables] of Object.entries(fixture.datasets)) {
    const core = await new StorageCore(32).open();
    try {
      for (const table of tables) {
        await core.execute(table.ddl);
        const def = core.harness.path.catalog.tableByName(table.name);
        if (!def) throw new Error("missing catalogue table");
        await core.writeBatch(
          "insertBatch",
          table.name,
          table.rows.map((row) =>
            Object.fromEntries(
              def.columns.map((column) => [
                column.name,
                column.type.kind === "timestamp"
                  ? new Date(String(row[column.name]))
                  : (row[column.name] ?? null),
              ]),
            ),
          ),
        );
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      const selected = new Set(option("--only", "").split(",").filter(Boolean));
      for (const query of fixture.queries.filter(
        (q) => q.dataset === dataset && (selected.size === 0 || selected.has(q.id)),
      )) {
        const refusal = CATALOGUE_REFUSALS[query.id];
        if (refusal) {
          records.push({ id: query.id, refusal, timed: false });
          await save();
        } else await compare(query.id, queryRun(core, query.sql));
      }
    } finally {
      await core.close();
    }
  }
} else {
  interface Column {
    name: string;
    pg: string;
  }
  interface Generator {
    wideSpec(n: number): Column[];
    wideBatch(
      spec: Column[],
      start: number,
      count: number,
      total: number,
    ): Record<string, InputValue[]>;
  }
  const workload = fileURLToPath(new URL("./next-executor-workload/", import.meta.url));
  const generator = (await import(pathToFileURL(join(workload, "gen.mjs")).href)) as Generator;
  const catalogue = (await import(pathToFileURL(join(workload, "queries.mjs")).href)) as {
    W_QUERIES: Array<{ id: string; sql: string }>;
    sqlFor(query: { id: string; sql: string }, engine: string, n: number): string;
  };
  const core = await new StorageCore(8192).open(),
    spec = generator.wideSpec(200);
  try {
    await core.execute(
      `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
    );
    const table = core.harness.path.catalog.tableByName("events_wide");
    if (!table) throw new Error("missing table");
    const inputGrant = await core.harness.governor.admit({
      label: "sort-bench/input",
      minimumBytes: 8 << 20,
      spillable: true,
    });
    try {
      for (let start = 0; start < rows; start += BATCH_ROWS) {
        const input = inputBatch(
          table,
          generator.wideBatch(spec, start, Math.min(BATCH_ROWS, rows - start), rows),
          inputGrant,
        );
        try {
          await core.insertVectors(table, input.batch);
        } finally {
          input.close();
        }
        if (start % (50 * BATCH_ROWS) === 0)
          console.error(
            JSON.stringify({ loaded: Math.min(rows, start + BATCH_ROWS), load: loadavg() }),
          );
      }
    } finally {
      inputGrant.close();
    }
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
    const selected = new Set(
      option(
        "--only",
        "sort_full_numeric,sort_full_text,sort_deep_offset,sort_deep_offset_pk,sort_topn,sort_topn_wide,win_topk_row_number,win_rank,win_running_sum,win_lag_lead,win_moving_avg,win_median_pct",
      ).split(","),
    );
    if (args.includes("--component")) {
      // Capture the actual decoded/projected input at the sort edge before timing. This keeps
      // flat/dictionary/validity forms and makes --component reproducible without a cache file.
      const query = catalogue.W_QUERIES.find((q) => q.id === "sort_full_numeric");
      if (!query) throw new Error("missing numeric workload");
      const batches: Batch[] = [];
      const push = SortOperator.prototype.push;
      SortOperator.prototype.push = function (this: SortOperator, batch, transfer) {
        batches.push(structuredClone(batch));
        return push.call(this, batch, transfer);
      };
      try {
        await queryRun(core, catalogue.sqlFor(query, "minnow", rows))(false, "frames");
      } finally {
        SortOperator.prototype.push = push;
      }
      const types = batches[0]?.columns.map((column) => column.type);
      if (!types || types.length !== 3) throw new Error("unexpected numeric physical layout");
      await measureComponent({
        metadata: { names: ["id", "f09", "d06"], n: rows },
        types,
        batches,
      });
    } else
      for (const query of catalogue.W_QUERIES.filter((q) => selected.has(q.id))) {
        const sql = catalogue.sqlFor(query, "minnow", rows);
        await compare(query.id, queryRun(core, sql));
      }
  } finally {
    await core.close();
  }
}
await save();
