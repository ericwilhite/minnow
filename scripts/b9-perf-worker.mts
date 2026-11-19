/** Branch-only B9 W1M fixture, timing, profile and representation-trace worker.
 * Runtime imports come exclusively from runtimeRoot; shared workload/helpers are harness code.
 */
import { createHash } from "node:crypto";
import { Session } from "node:inspector/promises";
import { writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import {
  captureNativeLease,
  monitorNativeCapacity,
  restoreNativeImage,
} from "./lib/native-storage-image.mts";
import type { Vector } from "../packages/core/dist/next/vector/vector.js";
import type { InputValue } from "../packages/core/dist/next/testing/executor-input.js";
import type { DomainCountSumOperator } from "../packages/core/dist/next/exec/agg/domain-count-sum.js";
import type { GroupTable } from "../packages/core/dist/next/exec/agg/groups.js";
import type { JoinTable } from "../packages/core/dist/next/exec/join-table.js";
import type { Operator } from "../packages/core/dist/next/exec/pipeline.js";
import {
  unorderedFloatColumns,
  rawRow,
  exactJSON,
  samePlanBits,
  type RawResult,
} from "./b9-float-law.mts";
import type { ScalarByteDistinct } from "../packages/core/dist/next/exec/agg/scalar-byte-distinct.js";

type StorageModule = typeof import("../packages/core/dist/next/testing/storage-core.js");
type ExecutorModule = typeof import("../packages/core/dist/next/exec/create.js");
type InputModule = typeof import("../packages/core/dist/next/testing/executor-input.js");
type OracleModule = typeof import("../packages/core/dist/next/testing/differential-adapter.js");
export interface B9WorkerData {
  runtimeRoot: string;
  workloadRoot: string;
  rows: number;
  dataset?: "wide" | "star";
  scale?: number;
  commerceSource?: string;
  image?: string;
  imageIdentity: string;
  capBytes: number;
}
interface CatalogueQuery {
  id: string;
  ordered?: boolean;
}
interface Generator {
  wideSpec(width: number): Array<{ name: string; pg: string }>;
  wideBatch(
    spec: Array<{ name: string; pg: string }>,
    at: number,
    count: number,
    rows: number,
  ): Record<string, InputValue[]>;
  dimTables(): Array<{ name: string; rows: Array<Record<string, InputValue>> }>;
}
interface Catalogue {
  W_QUERIES: CatalogueQuery[];
  S_QUERIES: CatalogueQuery[];
  S_TABLES: string[];
  sqlFor(query: CatalogueQuery, engine: string, rows: number): string;
}
const nativeLittleEndian = new Uint8Array(Uint32Array.of(1).buffer)[0] === 1;
const vectorShape = (v: Vector) => ({
  type: v.type.kind,
  form: v.form,
  layout: (v.form === "dictionary" ? v.dictionary : v.form === "constant" ? v.value : v).data
    .layout,
  ...(v.form === "dictionary"
    ? { dictionaryId: v.dictionaryId, entries: v.dictionary.length }
    : {}),
});

export default async function create(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const config = context.data as B9WorkerData;
  const url = (path: string) =>
    pathToFileURL(join(config.runtimeRoot, "packages/core/dist", path)).href;
  const [storage, executor, input, oracle, generator, catalogue] = await Promise.all([
    import(url("next/testing/storage-core.js")) as Promise<StorageModule>,
    import(url("next/exec/create.js")) as Promise<ExecutorModule>,
    import(url("next/testing/executor-input.js")) as Promise<InputModule>,
    import(url("next/testing/differential-adapter.js")) as Promise<OracleModule>,
    import(pathToFileURL(join(config.workloadRoot, "gen.mjs")).href) as Promise<Generator>,
    import(pathToFileURL(join(config.workloadRoot, "queries.mjs")).href) as Promise<Catalogue>,
  ]);
  const star = config.dataset === "star";
  const queries = star ? catalogue.S_QUERIES : catalogue.W_QUERIES;
  const core = new storage.StorageCore(8192);
  if (core.harness.governor.capBytes !== config.capBytes)
    throw new Error("Requested cap differs from the runtime's ordinary harness cap");
  const monitor = config.image === undefined ? monitorNativeCapacity(core.harness.backend) : null;
  let opened = false;
  try {
    if (config.image !== undefined)
      await restoreNativeImage(
        core.harness.backend,
        config.image,
        config.imageIdentity,
        context.signal,
      );
    await core.open();
    opened = true;
    if (config.image === undefined && star) {
      if (!config.commerceSource || !config.scale || config.scale <= 0)
        throw new Error("Missing commerce source/scale");
      const commerce = (await import(pathToFileURL(config.commerceSource).href)) as {
        commerceEntities: Array<{
          name: string;
          rows(scale: number): number;
          primaryKey?: string;
          columns: Array<{ name: string; type: "number" | "string" | "boolean" | "datetime" }>;
        }>;
        generateEntityBatch(
          entity: unknown,
          start: number,
          count: number,
          total: number,
          scale: number,
        ): Record<string, InputValue[]>;
      };
      const sqlTypes = {
        number: "DOUBLE PRECISION",
        string: "TEXT",
        boolean: "BOOLEAN",
        datetime: "TIMESTAMP WITH TIME ZONE",
      };
      for (const entity of commerce.commerceEntities.filter((e) =>
        catalogue.S_TABLES.includes(e.name),
      )) {
        const count = entity.rows(config.scale);
        await core.execute(
          `CREATE TABLE ${entity.name} (${entity.columns.map((c) => `${c.name} ${sqlTypes[c.type]}${c.name === entity.primaryKey ? " PRIMARY KEY" : ""}`).join(",")})`,
        );
        const table = core.harness.path.catalog.tableByName(entity.name);
        if (!table) throw new Error("Missing commerce table");
        const grant = await core.harness.governor.admit({
          label: "B9 commerce fixture",
          minimumBytes: 1 << 20,
          spillable: true,
        });
        try {
          for (let at = 0; at < count; at += 2048) {
            context.signal.throwIfAborted();
            const batch = input.inputBatch(
              table,
              commerce.generateEntityBatch(
                entity,
                at,
                Math.min(2048, count - at),
                count,
                config.scale,
              ),
              grant,
            );
            try {
              await core.insertVectors(table, batch.batch);
            } finally {
              batch.close();
            }
          }
        } finally {
          grant.close();
        }
        console.error(
          JSON.stringify({
            phase: "fixture",
            table: entity.name,
            rows: count,
            scale: config.scale,
          }),
        );
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
    }
    if (config.image === undefined && !star) {
      const spec = generator.wideSpec(200);
      await core.execute(
        `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
      );
      const table = core.harness.path.catalog.tableByName("events_wide");
      if (!table) throw new Error("Missing fact table");
      const grant = await core.harness.governor.admit({
        label: "B9 fixture",
        minimumBytes: 8 << 20,
        spillable: true,
      });
      try {
        for (let at = 0; at < config.rows; at += 2048) {
          context.signal.throwIfAborted();
          const batch = input.inputBatch(
            table,
            generator.wideBatch(spec, at, Math.min(2048, config.rows - at), config.rows),
            grant,
          );
          try {
            await core.insertVectors(table, batch.batch);
          } finally {
            batch.close();
          }
          if (at % (100 * 2048) === 0)
            console.error(
              JSON.stringify({ phase: "fixture", rows: Math.min(at + 2048, config.rows) }),
            );
        }
      } finally {
        grant.close();
      }
      for (const dimension of generator.dimTables()) {
        await core.execute(
          `CREATE TABLE ${dimension.name} (k TEXT PRIMARY KEY,label TEXT,tier TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
        );
        const target = core.harness.path.catalog.tableByName(dimension.name);
        if (!target) throw new Error("Missing dimension");
        const funding = await core.harness.governor.admit({
          label: "B9 dimension",
          minimumBytes: 8 << 20,
          spillable: true,
        });
        try {
          const values = Object.fromEntries(
            ["k", "label", "tier", "weight", "active"].map((key) => [
              key,
              dimension.rows.map((row) => row[key === "k" ? "key" : key] ?? null),
            ]),
          );
          const batch = input.inputBatch(target, values, funding);
          try {
            await core.insertVectors(target, batch.batch);
          } finally {
            batch.close();
          }
        } finally {
          funding.close();
        }
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
    }
    const factory = executor.createExecutor();
    let active: ReturnType<typeof core.start> | null = null;
    const prepareQuery = (id: string, certify: boolean) => {
      const query = queries.find((q) => q.id === id);
      if (!query) throw new Error(`Unknown cell ${id}`);
      const sourceSQL = catalogue.sqlFor(query, "minnow", config.rows);
      // Match the existing executor benchmark's documented PostgreSQL-valid correction.
      const sql =
        star && id === "S_q7"
          ? sourceSQL.replace("ROUND(revenue, 4)", "ROUND(revenue::numeric, 4)")
          : sourceSQL;
      const lowered = core.lowerer.lower(sql, core.context());
      if (lowered.kind !== "query") throw new Error("Expected SELECT");
      return {
        query,
        prepared: factory.prepare(lowered.plan),
        eligible: certify ? unorderedFloatColumns(lowered.plan.root) : [],
        planIdentity: certify
          ? context.revision +
            ":" +
            createHash("sha256").update(exactJSON(lowered.plan.root)).digest("hex")
          : "",
      };
    };
    const run = async (id: string, verify: boolean, saved?: ReturnType<typeof prepareQuery>) => {
      const { query, prepared, eligible, planIdentity } = saved ?? prepareQuery(id, verify),
        handle = core.start(prepared),
        hash = verify ? createHash("sha256") : null,
        rawHash = verify ? createHash("sha256") : null;
      active = handle;
      let rows = 0;
      const dictionaries = new Map<
          number,
          Array<Parameters<StorageModule["expandTextDictionary"]>[1][number]>
        >(),
        unordered: string[] = [],
        rawRows: string[] = [];
      try {
        for (;;) {
          const frame = await handle.execution.results.next();
          if (frame === null) break;
          try {
            rows += frame.payload.rowCount;
            if (!verify) continue;
            for (const delta of frame.payload.dictionaries) {
              const parts = dictionaries.get(delta.id) ?? [];
              if (delta.from !== parts.reduce((n, p) => n + p.length, 0))
                throw new Error("Dictionary gap");
              parts.push(structuredClone(delta.values));
              dictionaries.set(delta.id, parts);
            }
            const columns = frame.payload.columns.map((v, c) => {
              const flat =
                v.form === "dictionary"
                  ? storage.expandTextDictionary(v, dictionaries.get(v.dictionary) ?? [])
                  : v;
              const type = prepared.columns[c]?.type;
              if (flat.form !== "flat" || !type) throw new Error("Bad frame");
              return { ...flat, type };
            });
            for (let rowIndex = 0; rowIndex < frame.payload.rowCount; rowIndex++) {
              const values = columns.map((column) => oracle.oracleValue(column, rowIndex)),
                kinds = prepared.columns.map((c) => c.type.kind),
                bits = columns.map((column, c) => {
                  if (values[c] === null) return undefined;
                  const kind = kinds[c],
                    data = column.data;
                  if (kind === "float64" && data.layout === "f64") {
                    const view = new DataView(
                      data.values.buffer,
                      data.values.byteOffset + 8 * rowIndex,
                      8,
                    );
                    return view.getBigUint64(0, nativeLittleEndian).toString(16).padStart(16, "0");
                  }
                  if (kind === "float32" && data.layout === "f32") {
                    const view = new DataView(
                      data.values.buffer,
                      data.values.byteOffset + 4 * rowIndex,
                      4,
                    );
                    return view.getUint32(0, nativeLittleEndian).toString(16).padStart(8, "0");
                  }
                  if (kind === "float32" || kind === "float64")
                    throw new Error("B9 floating wire layout changed");
                  return undefined;
                }),
                exact = rawRow(values, kinds, bits);
              rawRows.push(exact);
              rawHash?.update(exact);
              const value = JSON.stringify(values, (_key, value: unknown) => {
                if (typeof value === "bigint") return { bigint: value.toString() };
                if (typeof value === "number" && (!Number.isFinite(value) || Object.is(value, -0)))
                  return { float: Object.is(value, -0) ? "-0" : String(value) };
                return value;
              });
              if (query.ordered) hash?.update(value);
              else unordered.push(value);
            }
          } finally {
            frame.release();
          }
        }
        const summary = await handle.execution.executed;
        await handle.execution.delivered;
        for (const row of unordered.sort()) hash?.update(row);
        return {
          rows,
          fingerprint: hash?.digest("hex") ?? "",
          determinismFingerprint: rawHash?.digest("hex") ?? "",
          detail: {
            raw: verify
              ? ({
                  version: 1,
                  schema: prepared.columns.map((c) => ({
                    name: c.name,
                    kind: c.type.kind,
                    type: exactJSON(c.type),
                  })),
                  eligible,
                  rows: rawRows,
                  planIdentity,
                } satisfies RawResult)
              : undefined,
            governorCap: core.harness.governor.capBytes,
            governorPeak: core.harness.governor.peakBytes,
            profile: summary.profile,
          },
        };
      } catch (error) {
        handle.execution.cancel(error);
        await Promise.allSettled([handle.execution.executed, handle.execution.delivered]);
        throw error;
      } finally {
        active = null;
        handle.release();
      }
    };
    return {
      revision: context.revision,
      identity: {
        runtimeRoot: config.runtimeRoot,
        rows: config.rows,
        dataset: config.dataset ?? "wide",
        scale: config.scale ?? null,
        capBytes: config.capBytes,
        image: config.image ?? null,
      },
      workloads: queries.map((q) => q.id),
      boundaries: ["frames"],
      verify: (id) => run(id, true),
      run: (id) => run(id, false),
      async command(name, raw) {
        if (name === "determinism") {
          const request = raw as { id: string },
            saved = prepareQuery(request.id, true),
            repeats = [];
          for (let i = 0; i < 3; i++) repeats.push(await run(request.id, true, saved));
          const first = repeats[0];
          if (!first || repeats.some((value) => !samePlanBits(first, value)))
            throw new Error("B9 same prepared plan changed output bits");
          return { planIdentity: saved.planIdentity, repeats };
        }
        if (name === "capture") {
          if (!monitor) throw new Error("Capture requires newly generated fixture");
          const request = raw as { directory: string };
          await core.harness.path.quiesce();
          return captureNativeLease(
            monitor.lease,
            request.directory,
            config.imageIdentity,
            context.signal,
            monitor.stop(),
          );
        }
        if (name === "profile") {
          const request = raw as { id: string; path: string };
          const inspector = new Session();
          inspector.connect();
          await inspector.post("Profiler.enable");
          await inspector.post("Profiler.start");
          try {
            for (let i = 0; i < 3; i++) await run(request.id, false);
            return null;
          } finally {
            const result = await inspector.post("Profiler.stop");
            inspector.disconnect();
            await writeFile(request.path, JSON.stringify(result.profile));
          }
        }
        if (name !== "trace") throw new Error("Unknown B9 command");
        const request = raw as { id: string },
          records = new Map<string, number>(),
          undo: Array<() => void> = [];
        const add = (kind: string, value: object) => {
          const key = JSON.stringify({ kind, ...value });
          records.set(key, (records.get(key) ?? 0) + 1);
        };
        // Optional candidate-only witness. It is installed only for untimed
        // representation tracing, and old/frozen builds do not import it.
        if (
          existsSync(join(config.runtimeRoot, "packages/core/dist/next/exec/join-star-lookups.js"))
        ) {
          interface LookupTrace {
            readonly matchedRows: number;
            readonly bound: {
              readonly facts: number;
              readonly domains: readonly number[];
              readonly bytes: number;
            };
            replay(downstream: Operator): Promise<void>;
          }
          const star = (await import(url("next/exec/join-star-lookups.js"))) as {
              StarLookups: { prototype: LookupTrace };
            },
            replay = star.StarLookups.prototype.replay;
          star.StarLookups.prototype.replay = async function (...args: Parameters<typeof replay>) {
            add("star-lookups", {
              matchedRows: this.matchedRows,
              factsBound: this.bound.facts,
              domains: this.bound.domains,
              allocatedBound: this.bound.bytes,
              keys: "charged query I32",
              schedule: "B ascending/A descending/fact ascending",
            });
            await replay.apply(this, args);
          };
          undo.push(() => {
            star.StarLookups.prototype.replay = replay;
          });
        }
        const nativeJoin = (await import(
          url("next/exec/join.js")
        )) as typeof import("../packages/core/dist/next/exec/join.js");
        const nativeProbe = nativeJoin.HashJoin.prototype.probe;
        nativeJoin.HashJoin.prototype.probe = function (...args: Parameters<typeof nativeProbe>) {
          const spec = this.spec as typeof this.spec & {
            readonly outputDemand?: readonly number[];
          };
          if (spec.outputDemand !== undefined)
            add("join-output-demand", {
              columns: spec.outputDemand,
              probeTypes: spec.probeTypes.map((t) => t.kind),
              buildTypes: spec.buildTypes.map((t) => t.kind),
              keys: "unchanged native key/row-ID pipeline",
              schedule: "native pair order",
            });
          const operator = nativeProbe.apply(this, args);
          if (!("alignedViewBatches" in this)) return operator;
          const record = () => {
            const batches = Number(Reflect.get(this, "alignedViewBatches") ?? 0);
            if (batches > 0)
              add("join-aligned-build-views", {
                batches,
                probeTypes: this.spec.probeTypes.map((t) => t.kind),
                buildTypes: this.spec.buildTypes.map((t) => t.kind),
                keys: "unchanged native query IDs and row IDs",
                schedule: "native join and aggregate order",
                payload: "immutable single-root dictionaries; retaining sink gathers once",
              });
          };
          return {
            push: (...values) => operator.push(...values),
            ready: () => operator.ready(),
            finish: () => {
              const done = operator.finish();
              if (done instanceof Promise) return done.then(record);
              record();
              return done;
            },
            abort: (reason) => operator.abort(reason),
          };
        };
        undo.push(() => {
          nativeJoin.HashJoin.prototype.probe = nativeProbe;
        });
        const domain = (await import(
          url("next/exec/agg/domain-count-sum.js")
        )) as typeof import("../packages/core/dist/next/exec/agg/domain-count-sum.js");
        const domainPush = domain.DomainCountSumOperator.prototype.push;
        domain.DomainCountSumOperator.prototype.push = function (
          this: DomainCountSumOperator,
          ...args: Parameters<typeof domainPush>
        ) {
          const before = this.directRows;
          const result = domainPush.apply(this, args);
          add("domain-count-sum", {
            input: args[0].columns.map(vectorShape),
            directRows: this.directRows - before,
          });
          return result;
        };
        undo.push(() => {
          domain.DomainCountSumOperator.prototype.push = domainPush;
        });
        const groups = (await import(
          url("next/exec/agg/groups.js")
        )) as typeof import("../packages/core/dist/next/exec/agg/groups.js");
        const assign = groups.GroupTable.prototype.assign;
        groups.GroupTable.prototype.assign = function (
          this: GroupTable,
          ...args: Parameters<typeof assign>
        ) {
          assign.apply(this, args);
          add("group", {
            keys: args[0].map(vectorShape),
            direct: this.directMapped,
            intHistory: args[0].map((_, i) => [...this.keyHistory(i)]),
            dictionaryRows: this.dictionaryRows > 0,
            callers: new Error().stack?.split("\n").slice(2, 6),
          });
        };
        undo.push(() => {
          groups.GroupTable.prototype.assign = assign;
        });
        const joins = (await import(
          url("next/exec/join-table.js")
        )) as typeof import("../packages/core/dist/next/exec/join-table.js");
        const build = joins.JoinTable.buildJob;
        joins.JoinTable.buildJob = function* (...args: Parameters<typeof build>) {
          const store = args[0],
            keys = args[1];
          const shapes =
            store.rows === 0 ? [] : keys.map((column) => vectorShape(store.chunk(column, 0)));
          const table = yield* build(...args);
          add("join-build", {
            keys: shapes,
            rows: store.rows,
            table: table?.kind ?? "spill",
            family: args[2].family,
            unique: table?.unique ?? null,
          });
          return table;
        };
        undo.push(() => {
          joins.JoinTable.buildJob = build;
        });
        const lookup = joins.JoinTable.prototype.lookupJob;
        joins.JoinTable.prototype.lookupJob = function* (
          this: JoinTable,
          ...args: Parameters<typeof lookup>
        ) {
          add("join", {
            table: this.kind,
            family: args[0].layout.family,
            width: args[0].layout.width,
          });
          return yield* lookup.apply(this, args);
        };
        undo.push(() => {
          joins.JoinTable.prototype.lookupJob = lookup;
        });
        const vectorLookup = joins.JoinTable.prototype.lookupVector;
        joins.JoinTable.prototype.lookupVector = function (
          this: JoinTable,
          ...args: Parameters<typeof vectorLookup>
        ) {
          const result = vectorLookup.apply(this, args);
          add("join-vector", {
            table: this.kind,
            key: vectorShape(args[0]),
            applicable: result >= 0,
          });
          return result;
        };
        undo.push(() => {
          joins.JoinTable.prototype.lookupVector = vectorLookup;
        });
        const distinct = (await import(
          url("next/exec/agg/scalar-byte-distinct.js")
        )) as typeof import("../packages/core/dist/next/exec/agg/scalar-byte-distinct.js");
        const filter = distinct.ScalarByteDistinct.prototype.filter;
        distinct.ScalarByteDistinct.prototype.filter = function (
          this: ScalarByteDistinct,
          ...args: Parameters<typeof filter>
        ) {
          add("scalar-byte-distinct", { key: vectorShape(args[0]) });
          return filter.apply(this, args);
        };
        undo.push(() => {
          distinct.ScalarByteDistinct.prototype.filter = filter;
        });
        try {
          const result = await run(request.id, false);
          return {
            result,
            records: [...records].map(([key, calls]) => ({
              ...(JSON.parse(key) as object),
              calls,
            })),
          };
        } finally {
          for (const restore of undo) restore();
        }
      },
      async close() {
        monitor?.stop();
        await core.close();
        opened = false;
        if (core.harness.governor.usedBytes !== 0 || core.spill.bytesInUse !== 0)
          throw new Error("B9 worker retained resources");
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
