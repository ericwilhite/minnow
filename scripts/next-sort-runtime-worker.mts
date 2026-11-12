/** Adapter for independent-cpu.mts. Every value import comes from the requested frozen
 * compiled runtime; the shared harness/helpers import only Node or erased core types.
 */
import { createHash } from "node:crypto";
import { readFile, realpath } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness, Verification } from "./lib/independent-cpu.mts";
import {
  captureNativeLease,
  restoreNativeImage,
  monitorNativeCapacity,
} from "./lib/native-storage-image.mts";
import type { InputValue } from "../packages/core/dist/next/testing/executor-input.js";

type StorageModule = typeof import("../packages/core/dist/next/testing/storage-core.js");
type ExecutorModule = typeof import("../packages/core/dist/next/exec/create.js");
type InputModule = typeof import("../packages/core/dist/next/testing/executor-input.js");
type OracleModule = typeof import("../packages/core/dist/next/testing/differential-adapter.js");
interface Generator {
  wideSpec(width: number): Array<{ name: string; pg: string }>;
  wideBatch(
    spec: ReadonlyArray<{ name: string; pg: string }>,
    at: number,
    count: number,
    total: number,
  ): Readonly<Record<string, readonly InputValue[]>>;
}
interface Catalogue {
  W_QUERIES: Array<{ id: string }>;
  sqlFor(query: { id: string }, engine: string, rows: number): string;
}
export interface Config {
  runtimeRoot: string;
  workloadRoot: string;
  rows?: number;
  image?: string;
  imageIdentity?: string;
  expected?: Readonly<Record<string, string>>;
}
type Boundary = "frames" | "rows" | "verify";
const digest = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");

export default async function createHarness(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const config = context.data as Config;
  if (typeof config.runtimeRoot !== "string" || typeof config.workloadRoot !== "string")
    throw new TypeError("Missing compiled runtime/workload root");
  const root = resolve(config.runtimeRoot),
    rows = config.rows ?? 1_000_000;
  const source = (path: string) => pathToFileURL(join(root, "packages/core/dist", path)).href;
  const [storage, executor, input, oracle, generator, catalogue] = await Promise.all([
    import(source("next/testing/storage-core.js")) as Promise<StorageModule>,
    import(source("next/exec/create.js")) as Promise<ExecutorModule>,
    import(source("next/testing/executor-input.js")) as Promise<InputModule>,
    import(source("next/testing/differential-adapter.js")) as Promise<OracleModule>,
    import(pathToFileURL(join(config.workloadRoot, "gen.mjs")).href) as Promise<Generator>,
    import(pathToFileURL(join(config.workloadRoot, "queries.mjs")).href) as Promise<Catalogue>,
  ]);
  const core = new storage.StorageCore(8192),
    monitor = config.image === undefined ? monitorNativeCapacity(core.harness.backend) : null;
  let opened = false;
  try {
    if (config.image !== undefined)
      await restoreNativeImage(
        core.harness.backend,
        config.image,
        config.imageIdentity ?? context.revision,
        context.signal,
      );
    await core.open();
    opened = true;
    context.signal.throwIfAborted();
    if (config.image === undefined) {
      const spec = generator.wideSpec(200);
      await core.execute(
        `CREATE TABLE events_wide (${spec.map((column) => `${column.name} ${column.pg}`).join(",")})`,
      );
      const table = core.harness.path.catalog.tableByName("events_wide");
      if (table === undefined) throw new Error("Missing wide benchmark table");
      const grant = await core.harness.governor.admit({
        label: "CPU image input",
        minimumBytes: 8 << 20,
        spillable: true,
      });
      try {
        for (let at = 0; at < rows; at += 2048) {
          context.signal.throwIfAborted();
          const batch = input.inputBatch(
            table,
            generator.wideBatch(spec, at, Math.min(2048, rows - at), rows),
            grant,
          );
          try {
            await core.insertVectors(table, batch.batch);
          } finally {
            batch.close();
          }
          if (at % (50 * 2048) === 0)
            console.error(
              JSON.stringify({ inputRows: Math.min(rows, at + 2048), revision: context.revision }),
            );
        }
      } finally {
        grant.close();
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
    }
    const factory = executor.createExecutor();
    let active: ReturnType<typeof core.start> | null = null;
    const run = async (
      id: string,
      boundary: Boundary,
    ): Promise<Verification & { rows: number }> => {
      context.signal.throwIfAborted();
      const query = catalogue.W_QUERIES.find((query) => query.id === id);
      let sql: string;
      if (id === "scan_plain3") sql = "SELECT f02,f06,f10 FROM events_wide";
      else {
        if (query === undefined) throw new Error(`Unknown frozen query ${id}`);
        sql = catalogue.sqlFor(query, "minnow", rows);
      }
      const lowered = core.lowerer.lower(sql, core.context());
      if (lowered.kind !== "query") throw new Error("Expected benchmark query");
      const prepared = factory.prepare(lowered.plan),
        handle = core.start(prepared),
        hash = createHash("sha256");
      active = handle;
      const dictionaries = new Map<
        number,
        Array<Parameters<StorageModule["expandTextDictionary"]>[1][number]>
      >();
      let count = 0;
      try {
        for (;;) {
          const frame = await handle.execution.results.next();
          if (frame === null) break;
          try {
            count += frame.payload.rowCount;
            if (boundary === "frames") continue;
            for (const delta of frame.payload.dictionaries) {
              const parts = dictionaries.get(delta.id) ?? [];
              if (delta.from !== parts.reduce((n, part) => n + part.length, 0))
                throw new Error("Dictionary delta gap");
              parts.push(structuredClone(delta.values));
              dictionaries.set(delta.id, parts);
            }
            const columns = frame.payload.columns.map((vector, c) => {
              const flat =
                vector.form === "dictionary"
                  ? storage.expandTextDictionary(vector, dictionaries.get(vector.dictionary) ?? [])
                  : vector;
              const type = prepared.columns[c]?.type;
              if (flat.form !== "flat" || type === undefined)
                throw new Error("Unexpected wire layout");
              return { ...flat, type };
            });
            const decoded = oracle.oracleRows({
              columns: prepared.columns,
              batches: [{ length: frame.payload.rowCount, selection: null, columns }],
            });
            if (boundary === "verify")
              for (const row of decoded)
                hash.update(
                  JSON.stringify(row, (_, value: unknown) =>
                    typeof value === "bigint"
                      ? { bigint: String(value) }
                      : typeof value === "number" &&
                          (!Number.isFinite(value) || Object.is(value, -0))
                        ? { float: Object.is(value, -0) ? "-0" : String(value) }
                        : value,
                  ),
                );
          } finally {
            frame.release();
          }
        }
        await handle.execution.executed;
        await handle.execution.delivered;
        return { rows: count, fingerprint: hash.digest("hex") };
      } catch (error) {
        handle.execution.cancel(error);
        await Promise.allSettled([handle.execution.executed, handle.execution.delivered]);
        throw error;
      } finally {
        if (active === handle) active = null;
        handle.release();
      }
    };
    const packagePath = createRequire(join(root, "benchmark.cjs")).resolve(
      "@minnowdb/core/package.json",
    );
    if (
      (await realpath(packagePath)) !== (await realpath(join(root, "packages/core/package.json")))
    )
      throw new Error("Core package escaped the frozen runtime");
    const sort = join(root, "packages/core/dist/next/exec/sort.js"),
      window = join(root, "packages/core/dist/next/exec/window.js");
    const identity = {
      root,
      packagePath,
      sort,
      window,
      sortSha256: digest(await readFile(sort)),
      windowSha256: digest(await readFile(window)),
      image: config.image ?? null,
      rows,
      generatorSha256: digest(await readFile(join(config.workloadRoot, "gen.mjs"))),
      queriesSha256: digest(await readFile(join(config.workloadRoot, "queries.mjs"))),
    };
    return {
      revision: context.revision,
      identity,
      workloads: ["scan_plain3", ...catalogue.W_QUERIES.map((query) => query.id)],
      boundaries: ["frames", "rows"],
      async verify(id) {
        const value = await run(id, "verify"),
          expected = config.expected?.[id];
        if (expected !== undefined && value.fingerprint !== expected)
          throw new Error(`Restored input changed frozen output ${id}`);
        return value;
      },
      async run(id, boundary) {
        if (boundary !== "frames" && boundary !== "rows")
          throw new Error("Unknown SQL CPU boundary");
        const value = await run(id, boundary);
        return { rows: value.rows };
      },
      async command(name, data) {
        if (name !== "capture-image" || monitor === null)
          throw new Error("Unknown input image command");
        const request = data as { directory: string; identity: string; workloads: string[] };
        const fingerprints: Record<string, string> = {};
        for (const id of request.workloads)
          fingerprints[id] = (await run(id, "verify")).fingerprint;
        await core.harness.path.quiesce();
        const capacities = monitor.stop();
        const image = await captureNativeLease(
          monitor.lease,
          request.directory,
          request.identity,
          context.signal,
          capacities,
        );
        return { image, fingerprints, identity };
      },
      async close() {
        monitor?.stop();
        await core.close();
        opened = false;
        if (core.harness.governor.usedBytes !== 0 || core.spill.bytesInUse !== 0)
          throw new Error("CPU worker leaked native resources");
      },
      cancel(reason) {
        active?.execution.cancel(reason);
      },
    };
  } catch (error) {
    monitor?.stop();
    if (opened) await core.close();
    throw error;
  }
}
