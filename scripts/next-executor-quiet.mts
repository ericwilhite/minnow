/** Frozen complete-core comparisons. --check uses seven rows and requests no CPU samples. */
import { fork, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, mkdir, writeFile, mkdtemp, access, appendFile } from "node:fs/promises";
import { realpathSync } from "node:fs";
import { createRequire } from "node:module";
import { loadavg, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { percentileComponent } from "./next-percentile-component.mts";
import { pairedCPU } from "./lib/paired-cpu.mts";
import { assessQuiet, type QuietRecord } from "./lib/next-quiet-policy.mts";
import type { InputValue } from "../packages/core/dist/next/testing/executor-input.js";
import type { Batch, FlatVector } from "../packages/core/dist/next/vector/vector.js";
import type { WireFlat } from "../packages/core/dist/next/exec/result.js";

const componentQuery = (query: string) => query === "pk-source" || query === "percentile-component";
const hash = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
const option = (key: string, fallback = "") => {
  const at = process.argv.indexOf(key);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
interface Manifest {
  revision: string;
  tree: string;
  node: string;
  files: Record<string, string>;
}
export async function manifest(root: string): Promise<Manifest> {
  const value = JSON.parse(await readFile(join(root, "manifest.json"), "utf8")) as Manifest;
  if (
    value.node !== process.version ||
    !/^[0-9a-f]{40}$/.test(value.revision) ||
    !/^[0-9a-f]{40}$/.test(value.tree)
  )
    throw new Error("Frozen runtime/toolchain mismatch");
  for (const file of [
    "source.tar",
    "packages/core/package.json",
    "packages/core/dist/next/exec/create.js",
    "packages/core/dist/next/exec/plan.js",
    "packages/core/dist/next/exec/key-order.js",
    "packages/core/dist/next/testing/storage-core.js",
  ])
    if (!Object.hasOwn(value.files, file)) throw new Error(`Unpinned runtime input: ${file}`);
  for (const [file, expected] of Object.entries(value.files)) {
    if (
      resolve(root, file).startsWith(root + "/") &&
      hash(await readFile(join(root, file))) === expected
    )
      continue;
    throw new Error(`Frozen input changed: ${root}/${file}`);
  }
  return value;
}
interface Observation {
  query: string;
  boundary: string;
  rows: number;
  digest: string;
  repeats: number;
  completed: number;
  totalRows: number;
  cpuMsTotal?: number;
  wallMsTotal?: number;
  cpuMs?: number;
  userMs?: number;
  systemMs?: number;
  wallMs?: number;
  load: number[];
  identity: { root: string; revision: string; pid: number; module: string; sha256: string };
}
interface Message {
  id: number;
  op: string;
  query?: string;
  boundary?: string;
  timed?: boolean;
  repeats?: number;
}

async function arm(root: string, count: number, queries: string[]): Promise<void> {
  const candidate = process.argv.includes("--candidate-arm");
  const frozen = await manifest(root);
  const module = join(root, "packages/core/dist/next/testing/storage-core.js");
  const runtime = (await import(
    pathToFileURL(module).href
  )) as typeof import("../packages/core/dist/next/testing/storage-core.js");
  const input = (await import(
    pathToFileURL(join(root, "packages/core/dist/next/testing/executor-input.js")).href
  )) as typeof import("../packages/core/dist/next/testing/executor-input.js");
  const vector = (await import(
    pathToFileURL(join(root, "packages/core/dist/next/vector/vector.js")).href
  )) as typeof import("../packages/core/dist/next/vector/vector.js");
  const adapter = (await import(
    pathToFileURL(join(root, "packages/core/dist/next/testing/differential-adapter.js")).href
  )) as typeof import("../packages/core/dist/next/testing/differential-adapter.js");
  const keyOrder = (await import(
    pathToFileURL(join(root, "packages/core/dist/next/exec/key-order.js")).href
  )) as typeof import("../packages/core/dist/next/exec/key-order.js");
  const arenas = (await import(
    pathToFileURL(join(root, "packages/core/dist/next/exec/kernels/vectors.js")).href
  )) as typeof import("../packages/core/dist/next/exec/kernels/vectors.js");
  const memory = (await import(
    pathToFileURL(join(root, "packages/core/dist/next/memory/governor.js")).href
  )) as typeof import("../packages/core/dist/next/memory/governor.js");
  const keys = (await import(
    pathToFileURL(join(root, "packages/core/dist/next/keys/key-encoding.js")).href
  )) as typeof import("../packages/core/dist/next/keys/key-encoding.js");
  const ownRequire = createRequire(join(root, "package.json"));
  if (
    realpathSync(ownRequire.resolve("@minnowdb/core/package.json")) !==
    join(root, "packages/core/package.json")
  )
    throw new Error("Worker resolves a foreign core");
  const generator = (await import(
    new URL("./next-executor-workload/gen.mjs", import.meta.url).href
  )) as {
    wideSpec(n: number): Array<{ name: string; pg: string }>;
    wideBatch(
      spec: unknown,
      from: number,
      rows: number,
      total: number,
    ): Record<string, InputValue[]>;
  };
  const catalogue = (await import(
    new URL("./next-executor-workload/queries.mjs", import.meta.url).href
  )) as {
    W_QUERIES: Array<{ id: string; sql: string }>;
    sqlFor(query: { sql: string }, engine: string, count: number): string;
  };
  const core = await new runtime.StorageCore(8192).open();
  const spec = generator.wideSpec(200);
  await core.execute(
    `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
  );
  const table = core.harness.path.catalog.tableByName("events_wide");
  if (!table) throw new Error("Fixture table missing");
  const grant = await core.harness.governor.admit({
    label: "quiet/input",
    minimumBytes: 8 << 20,
    spillable: true,
  });
  try {
    for (let first = 0; first < count; first += vector.BATCH_ROWS) {
      const data = input.inputBatch(
        table,
        generator.wideBatch(spec, first, Math.min(vector.BATCH_ROWS, count - first), count),
        grant,
      );
      try {
        await core.insertVectors(table, data.batch);
      } finally {
        data.close();
      }
    }
  } finally {
    grant.close();
  }
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
  const sql = new Map(
    queries
      .filter((id) => !componentQuery(id))
      .map((id) => {
        // Plain projection companion to the aggregation/retaining guards; real executor frames.
        if (id === "scan_plain3") return [id, "SELECT f02,f06,f10 FROM events_wide"];
        const query = catalogue.W_QUERIES.find((q) => q.id === id);
        if (!query) throw new Error(`Unknown query: ${id}`);
        return [id, catalogue.sqlFor(query, "minnow", count)];
      }),
  );
  const percentiles = queries.includes("percentile-component")
    ? await percentileComponent(root, core, count, candidate, generator, spec)
    : null;
  const identity = {
    root,
    revision: frozen.revision,
    pid: process.pid,
    module,
    sha256: hash(await readFile(module)),
  };
  const component = async (
    captured: Float64Array,
    pin: ReturnType<typeof core.harness.path.pin>,
  ) => {
    const index = table.primaryKey;
    if (index === null) throw new Error("Fixture PK missing");
    const plan = pin.index(table.id, index).planRange?.({ lower: null, upper: null }, "asc");
    if (!plan) throw new Error("Component requires the admitted cursor API in both complete cores");
    const governor = new memory.Governor(256 << 20);
    const grants = await governor.admitGroup({
      label: "quiet/pk-source",
      includesProgress: true,
      grants: [
        { label: "cursor", minimumBytes: plan.minimumBytes, spillable: false },
        { label: "locators", minimumBytes: 128 * 1024, spillable: false },
      ],
    });
    const cursorGrant = grants[0],
      outputGrant = grants[1];
    if (!cursorGrant || !outputGrant) throw new Error("Component admission missing");
    const arena = new arenas.OutputArena(outputGrant, vector.BATCH_ROWS);
    const source = new keyOrder.IndexOrderSource(
      plan.open({ budget: cursorGrant, signal: new AbortController().signal }),
      arena.flat({ kind: "locator", tableId: table.id }).vector,
      new AbortController().signal,
      100,
      Math.floor(count * 0.9),
    );
    let rows = 0;
    try {
      for (;;) {
        const item = await source.next();
        if (!item) break;
        const value = item.batch.columns[0];
        if (value?.form !== "flat" || value.data.layout !== "f64")
          throw new Error("Component locator layout");
        captured.set(value.data.values.subarray(0, item.batch.length), rows);
        rows += item.batch.length;
      }
    } finally {
      source.abort();
      await source.closed;
      arena.close();
      for (const part of grants) part.close();
    }
    if (governor.usedBytes !== 0) throw new Error("Component grant leak");
    return { rows, digest: "" };
  };
  const verifyComponent = async (
    captured: Float64Array,
    rows: number,
    pin: ReturnType<typeof core.harness.path.pin>,
  ) => {
    const expected = Math.min(100, count - Math.floor(count * 0.9));
    if (rows !== expected || table.primaryKey === null) throw new Error("Component count changed");
    const governor = new memory.Governor(8 << 20);
    const grant = await governor.admit({
      label: "quiet/validation",
      minimumBytes: 1 << 20,
      spillable: false,
    });
    const batch = new keys.KeyBatch(grant);
    const ids = Float64Array.from({ length: rows }, (_, i) => Math.floor(count * 0.9) + i + 1);
    const type = table.columns.find((c) => c.name === "id")?.type;
    if (type?.kind !== "int64") throw new Error("Component catalogue PK changed");
    try {
      keys.encodeKeyBatch(
        "equality",
        [
          {
            vector: {
              form: "flat",
              type,
              length: rows,
              validity: null,
              data: { layout: "int64", values: { repr: "safe", values: ids } },
            },
            direction: "asc",
            nulls: "first",
          },
        ],
        { length: rows, selection: null },
        batch,
      );
      const out = { probes: new Int32Array(rows), locators: new Float64Array(rows), count: 0 };
      await pin.index(table.id, table.primaryKey).probe(batch, out);
      if (out.count !== rows) throw new Error("Component key probe count changed");
      for (let row = 0; row < rows; row++)
        if (out.probes[row] !== row || out.locators[row] !== captured[row])
          throw new Error("Component skipped the wrong visible key");
      return hash(JSON.stringify([...ids]));
    } finally {
      batch.close();
      grant.close();
    }
  };
  const run = async (query: string, boundary: string) => {
    const text = sql.get(query);
    if (!text) throw new Error("Unprepared query");
    const prepared = core.prepare(text);
    const active = core.start(prepared);
    let rows = 0;
    const digest = createHash("sha256");
    const dictionaries = new Map<number, WireFlat[]>();
    try {
      for (;;) {
        const frame = await active.execution.results.next();
        if (!frame) break;
        try {
          rows += frame.payload.rowCount;
          if (boundary !== "frames") {
            for (const delta of frame.payload.dictionaries) {
              const parts = dictionaries.get(delta.id) ?? [];
              if (delta.from !== parts.reduce((n, p) => n + p.length, 0))
                throw new Error("Dictionary sequence changed");
              parts.push(structuredClone(delta.values));
              dictionaries.set(delta.id, parts);
            }
            const batch: Batch = {
              length: frame.payload.rowCount,
              selection: null,
              columns: frame.payload.columns.map((value, column): FlatVector => {
                const flat =
                  value.form === "dictionary"
                    ? runtime.expandTextDictionary(value, dictionaries.get(value.dictionary) ?? [])
                    : value;
                if (flat.form !== "flat") throw new Error("Non-flat wire result");
                const type = prepared.columns[column]?.type;
                if (!type) throw new Error("Output type missing");
                return { ...flat, type };
              }),
            };
            const decoded = adapter.oracleRows({ columns: prepared.columns, batches: [batch] });
            if (boundary === "verify")
              for (const row of decoded)
                digest.update(
                  JSON.stringify(row, (_, value: unknown) =>
                    typeof value === "number"
                      ? ["number", Object.is(value, -0) ? "-0" : String(value)]
                      : typeof value === "bigint"
                        ? ["bigint", String(value)]
                        : value,
                  ),
                );
          }
        } finally {
          frame.release();
        }
      }
      await active.execution.executed;
      await active.execution.delivered;
      if (core.spill.bytesInUse !== 0) throw new Error("Query spill leak");
      return { rows, digest: digest.digest("hex") };
    } catch (error) {
      active.execution.cancel(error);
      await Promise.allSettled([active.execution.executed, active.execution.delivered]);
      throw error;
    } finally {
      active.release();
    }
  };
  let serial = Promise.resolve();
  process.on("message", (request: Message) => {
    serial = serial.then(async () => {
      try {
        if (request.op === "close") {
          percentiles?.close();
          await core.close();
          process.send?.({ id: request.id, ok: true, value: { closed: true } });
          process.disconnect?.();
          return;
        }
        if (
          request.op !== "run" ||
          !request.query ||
          !["frames", "rows", "verify"].includes(request.boundary ?? "")
        )
          throw new Error("Unknown request");
        if (!request.timed && request.boundary !== "verify")
          throw new Error("Setup only validates full output");
        const repeats = request.repeats ?? 1;
        if (
          !Number.isSafeInteger(repeats) ||
          repeats < 1 ||
          repeats > 1000 ||
          (repeats !== 1 &&
            (!request.timed ||
              request.boundary !== "frames" ||
              request.query === "pk-source" ||
              request.query === "percentile-component"))
        )
          throw new Error("Invalid complete-query repeat count");
        // Input/output capture and validation are outside the isolated component clock.
        const componentPin = request.query === "pk-source" ? core.harness.path.pin() : null;
        const captured = componentPin ? new Float64Array(100) : null;
        const load = loadavg();
        const wall = request.timed ? performance.now() : 0;
        const start = request.timed ? process.cpuUsage() : null;
        let result: { rows: number; digest: string }, used: NodeJS.CpuUsage | null, elapsed: number;
        let completed = 0,
          totalRows = 0;
        try {
          const once = () =>
            componentPin && captured
              ? component(captured, componentPin)
              : request.query === "percentile-component" && percentiles
                ? percentiles.run()
                : run(request.query ?? "", request.boundary ?? "verify");
          result = await once();
          completed++;
          totalRows += result.rows;
          for (let repeat = 1; repeat < repeats; repeat++) {
            const next = await once();
            if (next.rows !== result.rows) throw new Error("Repeated query row count drift");
            completed++;
            totalRows += next.rows;
          }
          used = start ? process.cpuUsage(start) : null;
          elapsed = performance.now() - wall;
          if (componentPin && captured)
            result.digest = await verifyComponent(captured, result.rows, componentPin);
          if (request.query === "percentile-component" && percentiles)
            result.digest = percentiles.digest();
        } finally {
          componentPin?.release();
        }
        const value: Observation = {
          ...result,
          repeats,
          completed,
          totalRows,
          query: request.query,
          boundary: request.boundary ?? "verify",
          load,
          identity,
          ...(used
            ? {
                cpuMs: (used.user + used.system) / 1000 / repeats,
                userMs: used.user / 1000 / repeats,
                systemMs: used.system / 1000 / repeats,
                wallMs: elapsed / repeats,
                cpuMsTotal: (used.user + used.system) / 1000,
                wallMsTotal: elapsed,
              }
            : {}),
        };
        process.send?.({ id: request.id, ok: true, value });
      } catch (error) {
        process.send?.({
          id: request.id,
          ok: false,
          error: String(error instanceof Error ? error.stack : error),
        });
      }
    });
  });
  process.send?.({ ready: true, identity, queries });
}

async function controller(): Promise<void> {
  const checking = process.argv.includes("--check");
  const baseline = resolve(option("--baseline")),
    candidate = resolve(option("--candidate"));
  if (!option("--baseline") || !option("--candidate"))
    throw new Error("Frozen --baseline and --candidate roots required");
  const before = await manifest(baseline),
    after = await manifest(candidate);
  const queries = option("--only", "sort_deep_offset_pk,pk-source").split(",");
  const primary = option("--primary", "sort_deep_offset_pk");
  if (
    new Set(queries).size !== queries.length ||
    !queries.includes(primary) ||
    componentQuery(primary)
  )
    throw new Error("A unique query set containing a whole-query primary is required");
  if (!checking && (baseline === candidate || before.revision === after.revision))
    throw new Error("Measured arms must be distinct frozen revisions");
  const specified = option("--output", process.env.MINNOW_QUIET_ITEM_OUT ?? "");
  const output = specified
    ? resolve(specified)
    : await mkdtemp(join(tmpdir(), "minnow-executor-quiet-"));
  if (!checking && !process.env.MINNOW_QUIET_ITEM_OUT)
    throw new Error("Timing is admitted only by the quiet-host runner");
  await mkdir(output, { recursive: true });
  for (const file of ["result.json", "setup.json", "observations.jsonl"]) {
    const exists = await access(join(output, file)).then(
      () => true,
      () => false,
    );
    if (exists) throw new Error(`Existing evidence would be overwritten: ${file}`);
  }
  const children: Array<{ child: ChildProcess; exited: Promise<void> }> = [];
  const start = async (root: string, revision: string, candidateArm: boolean) => {
    const child = fork(
      fileURLToPath(import.meta.url),
      [
        "--arm",
        root,
        "--rows",
        checking ? "7" : "1000000",
        "--only",
        queries.join(","),
        ...(candidateArm ? ["--candidate-arm"] : []),
      ],
      { execArgv: [], stdio: ["ignore", "inherit", "inherit", "ipc"] },
    );
    let next = 0;
    const requests = new Map<
      number,
      { resolve: (value: Observation) => void; reject: (reason: Error) => void }
    >();
    const exited = new Promise<void>((resolveExit) =>
      child.once("close", () => {
        for (const p of requests.values()) p.reject(new Error("Own worker exited"));
        resolveExit();
      }),
    );
    children.push({ child, exited });
    const ready = new Promise<void>((resolveReady, reject) => {
      child.once("error", reject);
      child.once("exit", () => reject(new Error("Own worker exited before readiness")));
      child.on(
        "message",
        (message: {
          ready?: boolean;
          identity?: Observation["identity"];
          id: number;
          ok?: boolean;
          value: Observation;
          error?: string;
        }) => {
          if (message.ready) {
            if (message.identity?.root !== root || message.identity.revision !== revision) {
              reject(new Error("Foreign worker identity"));
              return;
            }
            resolveReady();
            return;
          }
          const request = requests.get(message.id);
          if (request) {
            requests.delete(message.id);
            if (message.ok) request.resolve(message.value);
            else request.reject(new Error(message.error));
          }
        },
      );
    });
    await ready;
    const ask = (message: Omit<Message, "id">) =>
      new Promise<Observation>((resolveRequest, reject) => {
        const id = ++next;
        requests.set(id, { resolve: resolveRequest, reject });
        child.send({ ...message, id });
      });
    return {
      ask,
      async close() {
        await ask({ op: "close" });
        await exited;
      },
    };
  };
  const records: QuietRecord[] = [];
  let measuredRequests = 0;
  let a: Awaited<ReturnType<typeof start>> | undefined,
    b: Awaited<ReturnType<typeof start>> | undefined;
  try {
    a = await start(baseline, before.revision, false);
    b = await start(candidate, after.revision, true);
    for (const query of queries) {
      const expected = await a.ask({ op: "run", query, boundary: "verify", timed: false });
      const warm = await b.ask({ op: "run", query, boundary: "verify", timed: false });
      if (warm.digest !== expected.digest || warm.rows !== expected.rows)
        throw new Error(`Different full output: ${query}`);
      if (checking) continue;
      for (const boundary of componentQuery(query) ? ["frames"] : ["frames", "rows"]) {
        const samples: Array<{ control: Observation; candidate: Observation; order: string }> = [];
        for (let pair = 0; pair < 9; pair++) {
          const order = pair % 2 === 0 ? [a, b] : [b, a];
          const observed = [];
          for (const worker of order) {
            const sample = await worker.ask({ op: "run", query, boundary, timed: true });
            observed.push(sample);
            measuredRequests++;
            await appendFile(
              join(output, "observations.jsonl"),
              JSON.stringify({ query, boundary, pair, sample }) + "\n",
            );
          }
          const control = observed[pair % 2],
            result = observed[1 - (pair % 2)];
          if (
            !control ||
            !result ||
            control.rows !== expected.rows ||
            result.rows !== expected.rows
          )
            throw new Error("Timed output count changed");
          if (
            (boundary === "rows" || componentQuery(query)) &&
            (control.digest !== expected.digest || result.digest !== expected.digest)
          )
            throw new Error("Timed materialized output changed");
          samples.push({ control, candidate: result, order: pair % 2 === 0 ? "AB" : "BA" });
          // Validate after the adjacent timed pair, never between its two arms.
          for (const worker of [a, b]) {
            const full = await worker.ask({ op: "run", query, boundary: "verify", timed: false });
            if (full.digest !== expected.digest || full.rows !== expected.rows)
              throw new Error("Post-pair output changed");
          }
        }
        records.push({
          query,
          boundary,
          samples,
          paired: pairedCPU(
            samples.map((p) => p.control.cpuMs ?? NaN),
            samples.map((p) => p.candidate.cpuMs ?? NaN),
          ),
        });
        await writeFile(
          join(output, "result.json"),
          JSON.stringify(
            {
              baseline: before.revision,
              candidate: after.revision,
              rows: 1000000,
              primaryBoundary: "frames",
              records,
            },
            null,
            2,
          ) + "\n",
        );
      }
    }
    await a.close();
    a = undefined;
    await b.close();
    b = undefined;
    await manifest(baseline);
    await manifest(candidate);
    if (!checking) {
      const assessment = assessQuiet(records, {
        primary,
        targetMs: primary === "win_median_pct" ? 75 : 20,
        queries,
        control: {
          root: baseline,
          revision: before.revision,
          moduleSha: before.files["packages/core/dist/next/testing/storage-core.js"] ?? "",
        },
        candidate: {
          root: candidate,
          revision: after.revision,
          moduleSha: after.files["packages/core/dist/next/testing/storage-core.js"] ?? "",
        },
      });
      await writeFile(join(output, "assessment.json"), JSON.stringify(assessment, null, 2) + "\n");
    }
    await writeFile(
      join(output, "setup.json"),
      JSON.stringify({
        setupOnly: checking,
        measuredRequests,
        baseline: before.revision,
        candidate: after.revision,
        queries,
      }) + "\n",
    );
  } finally {
    // Retained ChildProcess handles identify only the workers this controller started.
    for (const { child } of children)
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    await Promise.all(children.map(({ exited }) => exited));
  }
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  if (process.argv.includes("--arm"))
    await arm(resolve(option("--arm")), Number(option("--rows")), option("--only").split(","));
  else await controller();
}
