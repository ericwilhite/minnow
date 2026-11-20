/** Branch-only B4 probe. Engine values import only from the requested compiled root.
 * Timed frames retain the historical lower -> prepare -> execute -> release boundary.
 * Historical canonical hashes and lossless hashes are separate, both outside CPU brackets.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, realpath, writeFile } from "node:fs/promises";
import { Session } from "node:inspector/promises";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { canonicalValue } from "./browser-perf/page/canonical.ts";
import {
  captureNativeLease,
  monitorNativeCapacity,
  restoreNativeImage,
} from "./lib/native-storage-image.mts";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import type { InputValue } from "../packages/core/dist/next/testing/executor-input.js";

type Storage = typeof import("../packages/core/dist/next/testing/storage-core.js");
type Executor = typeof import("../packages/core/dist/next/exec/create.js");
type Input = typeof import("../packages/core/dist/next/testing/executor-input.js");
type Oracle = typeof import("../packages/core/dist/next/testing/differential-adapter.js");
interface Spec {
  name: string;
  pg: string;
}
interface Generator {
  wideSpec(width: number): Spec[];
  wideBatch(
    spec: readonly Spec[],
    at: number,
    count: number,
    rows: number,
  ): Record<string, InputValue[]>;
  dimTables(): Array<{
    name: string;
    rows: Array<{
      key: string;
      label: string;
      tier: string;
      weight: number;
      active: boolean;
    }>;
  }>;
  value(spec: Spec, row: number, rows: number): InputValue;
}
interface Query {
  id: string;
  ordered?: boolean;
}
interface Catalogue {
  W_QUERIES: Query[];
  sqlFor(query: Query, engine: string, rows: number): string;
}
export interface B4WorkerData {
  runtimeRoot: string;
  workloadRoot: string;
  rows: number;
  capBytes: number;
  repeats: number;
  imageIdentity: string;
  image?: string;
  expected?: Readonly<Record<string, string>>;
}
export interface B4Verification {
  /** Historical canonical result, multiset where the SQL has no total ORDER BY. */
  fingerprint: string;
  /** Original historical traversal hash for retaining the pinned plain3 observation. */
  rawFingerprint: string;
  exactFingerprint: string;
  columns: Array<{ name: string; type: unknown }>;
  rows: number;
}
export interface B4Capture {
  image: unknown;
  fingerprints: Record<string, string>;
  identity: unknown;
}
const digest = (value: Uint8Array | string) => createHash("sha256").update(value).digest("hex");
const shaRows = (rows: readonly string[]) => {
  const hash = createHash("sha256");
  for (const row of rows) hash.update(row);
  return hash.digest("hex");
};
function exactValue(value: unknown): unknown {
  if (value === null) return ["null"];
  if (value instanceof Date) {
    assert(Number.isFinite(value.getTime()), "invalid timestamp in output");
    return ["date-ms", String(value.getTime())];
  }
  if (typeof value === "number") return ["number", Object.is(value, -0) ? "-0" : String(value)];
  if (typeof value === "string" || typeof value === "bigint" || typeof value === "boolean")
    return [typeof value, String(value)];
  throw new TypeError(`Unexpected oracle value ${Object.prototype.toString.call(value)}`);
}

export default async function create(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const data = context.data as B4WorkerData;
  assert.equal(data.rows, 1_000_000, "W1M population must not shrink");
  assert.equal(data.capBytes, 256 * 2 ** 20);
  assert.equal(data.repeats, 10);
  const url = (file: string) =>
    pathToFileURL(join(data.runtimeRoot, "packages/core/dist", file)).href;
  const [storage, executor, input, oracle, generator, catalogue] = await Promise.all([
    import(url("next/testing/storage-core.js")) as Promise<Storage>,
    import(url("next/exec/create.js")) as Promise<Executor>,
    import(url("next/testing/executor-input.js")) as Promise<Input>,
    import(url("next/testing/differential-adapter.js")) as Promise<Oracle>,
    import(pathToFileURL(join(data.workloadRoot, "gen.mjs")).href) as Promise<Generator>,
    import(pathToFileURL(join(data.workloadRoot, "queries.mjs")).href) as Promise<Catalogue>,
  ]);
  const packagePath = createRequire(join(data.runtimeRoot, "benchmark.cjs")).resolve(
    "@minnowdb/core/package.json",
  );
  assert.equal(
    await realpath(packagePath),
    await realpath(join(data.runtimeRoot, "packages/core/package.json")),
  );
  const core = new storage.StorageCore(8192);
  assert.equal(core.harness.governor.capBytes, data.capBytes, "ordinary harness governor cap");
  const monitor = data.image === undefined ? monitorNativeCapacity(core.harness.backend) : null;
  let opened = false,
    active: ReturnType<typeof core.start> | null = null;
  try {
    if (data.image !== undefined)
      await restoreNativeImage(
        core.harness.backend,
        data.image,
        data.imageIdentity,
        context.signal,
      );
    await core.open();
    opened = true;
    if (data.image === undefined) {
      const spec = generator.wideSpec(200);
      await core.execute(
        `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
      );
      const table = core.harness.path.catalog.tableByName("events_wide");
      assert(table);
      const funding = await core.harness.governor.admit({
        label: "B4 W1M writer",
        minimumBytes: 8 << 20,
        spillable: true,
      });
      try {
        for (let at = 0; at < data.rows; at += 2048) {
          context.signal.throwIfAborted();
          const batch = input.inputBatch(
            table,
            generator.wideBatch(spec, at, Math.min(2048, data.rows - at), data.rows),
            funding,
          );
          try {
            await core.insertVectors(table, batch.batch);
          } finally {
            batch.close();
          }
          if (at % (50 * 2048) === 0)
            console.error(
              JSON.stringify({
                phase: "fixture",
                rows: Math.min(data.rows, at + 2048),
              }),
            );
        }
      } finally {
        funding.close();
      }
      // Match the frozen adapter's ordinary dimension write API and write order.
      for (const dim of generator.dimTables()) {
        await core.execute(
          `CREATE TABLE ${dim.name} (k TEXT PRIMARY KEY,label TEXT,tier TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
        );
        await core.writeBatch(
          "insertBatch",
          dim.name,
          dim.rows.map((r) => ({
            k: r.key,
            label: r.label,
            tier: r.tier,
            weight: r.weight,
            active: r.active,
          })),
        );
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
    }
    const factory = executor.createExecutor();
    const run = async (id: string, verify: boolean): Promise<B4Verification> => {
      context.signal.throwIfAborted();
      const query = catalogue.W_QUERIES.find((q) => q.id === id);
      let sql: string;
      if (id === "scan_plain3") sql = "SELECT f02,f06,f10 FROM events_wide";
      else {
        assert(query, `Unknown cell ${id}`);
        sql = catalogue.sqlFor(query, "minnow", data.rows);
      }
      const lowered = core.lowerer.lower(sql, core.context());
      assert.equal(lowered.kind, "query");
      if (lowered.kind !== "query") throw new Error("Expected SELECT");
      const prepared = factory.prepare(lowered.plan),
        handle = core.start(prepared),
        dictionaries = new Map<
          number,
          Array<Parameters<Storage["expandTextDictionary"]>[1][number]>
        >(),
        canonical = createHash("sha256"),
        historical = verify ? createHash("sha256") : null,
        exact = verify ? createHash("sha256") : null,
        unorderedCanonical: string[] = [],
        unorderedExact: string[] = [];
      let rows = 0;
      active = handle;
      try {
        for (;;) {
          const frame = await handle.execution.results.next();
          if (frame === null) break;
          try {
            rows += frame.payload.rowCount;
            if (!verify) continue;
            for (const delta of frame.payload.dictionaries) {
              const parts = dictionaries.get(delta.id) ?? [];
              assert.equal(
                delta.from,
                parts.reduce((n, p) => n + p.length, 0),
                "dictionary delta gap",
              );
              parts.push(structuredClone(delta.values));
              dictionaries.set(delta.id, parts);
            }
            const vectors = frame.payload.columns.map((vector, index) => {
              const flat =
                  vector.form === "dictionary"
                    ? storage.expandTextDictionary(
                        vector,
                        dictionaries.get(vector.dictionary) ?? [],
                      )
                    : vector,
                type = prepared.columns[index]?.type;
              assert(flat.form === "flat" && type !== undefined);
              return { ...flat, type };
            });
            // Positional labels prevent oracleRows' object keys from dropping duplicate SQL labels.
            const columns = prepared.columns.map((column, index) => ({
              ...column,
              name: `c${String(index)}`,
            }));
            const decoded = oracle.oracleRows({
              columns,
              batches: [
                {
                  length: frame.payload.rowCount,
                  selection: null,
                  columns: vectors,
                },
              ],
            });
            for (const row of decoded) {
              const values = columns.map((column) => row[column.name]),
                old = JSON.stringify(values.map(canonicalValue)),
                lossless = JSON.stringify(values.map(exactValue));
              if (query?.ordered) {
                canonical.update(old);
                exact?.update(lossless);
                historical?.update(old);
              } else {
                unorderedCanonical.push(old);
                unorderedExact.push(lossless);
                if (id === "scan_plain3") historical?.update(old);
              }
            }
          } finally {
            frame.release();
          }
        }
        core.lastProfile = (await handle.execution.executed).profile;
        await handle.execution.delivered;
        if (verify && unorderedCanonical.length) {
          unorderedCanonical.sort();
          unorderedExact.sort();
          for (const row of unorderedCanonical) canonical.update(row);
          for (const row of unorderedExact) exact?.update(row);
          if (id !== "scan_plain3") for (const row of unorderedCanonical) historical?.update(row);
        }
        return {
          rows,
          fingerprint: canonical.digest("hex"),
          rawFingerprint: historical?.digest("hex") ?? "",
          exactFingerprint: exact?.digest("hex") ?? "",
          columns: verify ? prepared.columns.map(({ name, type }) => ({ name, type })) : [],
        };
      } catch (error) {
        handle.execution.cancel(error);
        await Promise.allSettled([handle.execution.executed, handle.execution.delivered]);
        throw error;
      } finally {
        if (active === handle) active = null;
        handle.release();
      }
    };
    const identity = {
      root: data.runtimeRoot,
      packagePath,
      rows: data.rows,
      capBytes: data.capBytes,
      image: data.image ?? null,
      generatorSha256: digest(await readFile(join(data.workloadRoot, "gen.mjs"))),
      queriesSha256: digest(await readFile(join(data.workloadRoot, "queries.mjs"))),
    };
    return {
      revision: context.revision,
      identity,
      workloads: ["scan_plain3", ...catalogue.W_QUERIES.map((q) => q.id)],
      boundaries: ["frames"],
      async verify(id) {
        const value = await run(id, true),
          pin = data.expected?.[id];
        // Plain3 has no ORDER BY. Keep its original traversal observation, but require
        // canonical AND lossless multisets in the controller instead of imposing row order.
        if (pin !== undefined && id !== "scan_plain3")
          assert.equal(value.rawFingerprint, pin, "original writer-image output pin changed");
        return value;
      },
      async run(id, boundary) {
        assert.equal(boundary, "frames");
        let rows = 0;
        for (let repeat = 0; repeat < data.repeats; repeat++) rows += (await run(id, false)).rows;
        return {
          rows,
          detail: {
            repeats: data.repeats,
            governorPeak: core.harness.governor.peakBytes,
            governorCap: core.harness.governor.capBytes,
            profile: core.lastProfile,
          },
        };
      },
      async command(name, raw) {
        if (name === "capture") {
          assert(monitor);
          const request = raw as { directory: string; cells: string[] },
            fingerprints: Record<string, string> = {};
          for (const id of request.cells) fingerprints[id] = (await run(id, true)).rawFingerprint;
          await core.harness.path.quiesce();
          const capacities = monitor.stop();
          return {
            image: await captureNativeLease(
              monitor.lease,
              request.directory,
              data.imageIdentity,
              context.signal,
              capacities,
            ),
            fingerprints,
            identity,
          } satisfies B4Capture;
        }
        if (name === "plain-generator") {
          const spec = generator.wideSpec(200),
            selected = ["f02", "f06", "f10"].map((name) => {
              const column = spec.find((c) => c.name === name);
              assert(column);
              return column;
            }),
            canonical: string[] = [],
            exact: string[] = [];
          for (let row = 0; row < data.rows; row++) {
            if (row % 2048 === 0) context.signal.throwIfAborted();
            const values = selected.map((column) => generator.value(column, row, data.rows));
            canonical.push(JSON.stringify(values.map(canonicalValue)));
            exact.push(JSON.stringify(values.map(exactValue)));
          }
          canonical.sort();
          exact.sort();
          return {
            fingerprint: shaRows(canonical),
            exactFingerprint: shaRows(exact),
            rows: data.rows,
          };
        }
        if (name === "profile") {
          const request = raw as { workload: string; output: string },
            session = new Session();
          session.connect();
          try {
            await session.post("Profiler.enable");
            await session.post("Profiler.setSamplingInterval", {
              interval: 100,
            });
            await session.post("Profiler.start");
            for (let repeat = 0; repeat < data.repeats; repeat++)
              await run(request.workload, false);
            const { profile } = await session.post("Profiler.stop");
            await writeFile(request.output, JSON.stringify(profile));
            return { output: request.output, profile: core.lastProfile };
          } finally {
            session.disconnect();
          }
        }
        throw new Error(`Unknown B4 command ${name}`);
      },
      async close() {
        monitor?.stop();
        await core.close();
        opened = false;
        assert.equal(core.harness.governor.usedBytes, 0, "retained governor credit");
        assert.equal(core.spill.bytesInUse, 0, "retained spill bytes");
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
