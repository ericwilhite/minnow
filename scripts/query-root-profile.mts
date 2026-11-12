/** Setup-excluded V8 scan attribution. Sampling is diagnostic, never paired CPU acceptance.
 * Capture uses the same native image and warm query adapter as query-root-bench.mts.
 * Counter instrumentation runs separately AFTER profiling; no forced GC in either pass.
 */
import { createHash } from "node:crypto";
import { readFile, writeFile, mkdtemp, mkdir, access } from "node:fs/promises";
import { Session } from "node:inspector/promises";
import { loadavg, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { manifest } from "./next-executor-quiet.mts";
import createRootHarness from "./query-root-runtime-worker.mts";

interface Frame {
  functionName: string;
  url: string;
  lineNumber: number;
}
interface Profile {
  nodes: Array<{ id: number; callFrame: Frame; children?: number[] | undefined }>;
  samples?: number[] | undefined;
  timeDeltas?: number[] | undefined;
}
const option = (key: string, fallback = "") => {
  const at = process.argv.indexOf(key);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
if (option("--summarize")) {
  console.log(
    JSON.stringify(
      summarize(JSON.parse(await readFile(resolve(option("--summarize")), "utf8")) as Profile),
      null,
      2,
    ),
  );
  process.exit(0);
}
const runtime = resolve(option("--runtime")),
  input = option("--bundle");
if (!option("--runtime") || !input)
  throw new Error("Required: --runtime <frozen-core> --bundle <input.json>");
const repeats = Number(option("--repeats", "30")),
  warmups = Number(option("--warmups", "10"));
if (![repeats, warmups].every((n) => Number.isSafeInteger(n) && n > 0 && n <= 1000))
  throw new Error("Invalid profile volume");
const frozen = await manifest(runtime),
  bundle = JSON.parse(await readFile(resolve(input), "utf8")) as {
    rows: number;
    image: string;
    imageIdentity: string;
    expected: Record<string, string>;
  };
if (
  bundle === null ||
  !Number.isSafeInteger(bundle.rows) ||
  bundle.rows < 1 ||
  bundle.rows > 1_000_000 ||
  typeof bundle.image !== "string" ||
  bundle.image.length === 0 ||
  typeof bundle.imageIdentity !== "string" ||
  bundle.imageIdentity.length === 0 ||
  typeof bundle.expected?.scan_plain3 !== "string"
)
  throw new Error("A verified native query input bundle is required");
const output = option("--output") || (await mkdtemp(join(tmpdir(), "minnow-root-profile-")));
await mkdir(output, { recursive: true });
if (
  await access(join(output, "scan.cpuprofile")).then(
    () => true,
    () => false,
  )
)
  throw new Error("Refusing to overwrite profile evidence");
const abort = new AbortController(),
  stop = () => abort.abort(new Error("Profile cancelled"));
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
const harness = await createRootHarness({
  arm: "control",
  revision: frozen.revision,
  cohort: 0,
  workload: "scan_plain3",
  boundary: "frames",
  signal: abort.signal,
  data: {
    runtimeRoot: runtime,
    workloadRoot: fileURLToPath(new URL("./next-executor-workload", import.meta.url)),
    rows: bundle.rows,
    repeats,
    warmups,
    image: bundle.image,
    imageIdentity: bundle.imageIdentity,
    expected: bundle.expected,
  },
});
const cancel = () => harness.cancel?.(abort.signal.reason);
abort.signal.addEventListener("abort", cancel, { once: true });
const session = new Session();
session.connect();
try {
  const before = await harness.verify("scan_plain3");
  await session.post("Profiler.enable");
  await session.post("Profiler.setSamplingInterval", { interval: 100 });
  await session.post("Profiler.start");
  const cpu = process.cpuUsage(),
    wall = performance.now(),
    load = loadavg();
  const result = await harness.run("scan_plain3", "frames");
  const usage = process.cpuUsage(cpu),
    wallMs = performance.now() - wall;
  const { profile } = await session.post("Profiler.stop");
  await writeFile(join(output, "scan.cpuprofile"), JSON.stringify(profile));
  const after = await harness.verify("scan_plain3");
  if (JSON.stringify(before) !== JSON.stringify(after)) throw new Error("Profile output drift");
  const counts: Record<string, number> = {},
    inc = (key: string, n = 1) => {
      counts[key] = (counts[key] ?? 0) + n;
    };
  const loadModule = (path: string) =>
    import(pathToFileURL(join(runtime, "packages/core/dist/next", path + ".js")).href);
  const { MetadataReader } = (await loadModule(
    "format/metadata-reader",
  )) as typeof import("../packages/core/dist/next/format/metadata-reader.js");
  const { ChunkReader } = (await loadModule(
    "page/reader",
  )) as typeof import("../packages/core/dist/next/page/reader.js");
  const codec = (await loadModule(
    "format/manifest-codec",
  )) as typeof import("../packages/core/dist/next/format/manifest-codec.js");
  const root = MetadataReader.prototype.groupRoot,
    open = ChunkReader.open,
    decode = ChunkReader.prototype.decodeRange,
    loadPages = ChunkReader.prototype.load;
  const ids = new WeakMap<object, number>();
  MetadataReader.prototype.groupRoot = function (ref, budget) {
    const got = root.call(this, ref, budget);
    const keep = (held: Awaited<typeof got>) => {
      inc("rootBorrows");
      const lazy = Object.getOwnPropertyDescriptor(held.value, "columns")?.get !== undefined;
      inc(lazy ? "lazyRootBorrows" : "eagerRootBorrows");
      // Never force all columns merely to count a lazy view. Older runtimes have no helper.
      inc(
        "metadataColumnSlots",
        typeof codec.groupRootColumnCount === "function"
          ? codec.groupRootColumnCount(held.value)
          : held.value.columns.length,
      );
      if (lazy) return held;
      const extents = new Set<object>();
      if (held.value.rowIds.kind === "column") extents.add(held.value.rowIds.ref.extent);
      for (const column of held.value.columns) {
        extents.add(column.data.extent);
        extents.add(column.directory.extent);
        if (column.distinctSketch !== null) extents.add(column.distinctSketch.extent);
      }
      inc("completeExtentPairs", extents.size);
      return held;
    };
    return got instanceof Promise ? got.then(keep) : keep(got);
  };
  ChunkReader.open = function (pages, chunk, options) {
    inc("chunkOpens");
    inc(`openedColumn:${String(chunk.columnId)}`);
    const got = open.call(this, pages, chunk, options);
    const keep = (reader: Awaited<typeof got>) => {
      ids.set(reader, chunk.columnId);
      inc("directoryPageEntries", reader.directory.pages.length);
      return reader;
    };
    return got instanceof Promise ? got.then(keep) : keep(got);
  };
  ChunkReader.prototype.decodeRange = function (from, to, target, at) {
    inc("decodeCalls");
    inc("decodedRows", to - from);
    inc(`decodedColumn:${String(ids.get(this))}`, to - from);
    return decode.call(this, from, to, target, at);
  };
  ChunkReader.prototype.load = function (indices) {
    inc("loadCalls");
    inc("requestedPages", indices.length);
    return loadPages.call(this, indices);
  };
  try {
    await harness.run("scan_plain3", "frames");
  } finally {
    MetadataReader.prototype.groupRoot = root;
    ChunkReader.open = open;
    ChunkReader.prototype.decodeRange = decode;
    ChunkReader.prototype.load = loadPages;
  }
  if (JSON.stringify(await harness.verify("scan_plain3")) !== JSON.stringify(before))
    throw new Error("Counter-pass output drift");
  const report = {
    revision: frozen.revision,
    tree: frozen.tree,
    sourceSha256: createHash("sha256")
      .update(await readFile(new URL(import.meta.url)))
      .digest("hex"),
    rows: bundle.rows,
    repeats,
    warmups,
    before,
    result,
    usage,
    wallMs,
    load,
    identity: harness.identity,
    memory: process.memoryUsage(),
    counts,
    countsPerQuery: Object.fromEntries(
      Object.entries(counts).map(([key, value]) => [key, value / repeats]),
    ),
    attribution: summarize(profile),
    limits:
      "Main-isolate V8 sample-count shares, not exact allocation of all-thread process CPU. Loaded-host timeDeltas include scheduling/wait; GC stays unassigned. Counters are a separate untimed pass. Native warm executor/ResultSink/ResultChannel, not owner/OPFS/Gate A.",
  };
  await writeFile(join(output, "report.json"), JSON.stringify(report, null, 2) + "\n");
  console.log(
    JSON.stringify({
      output,
      revision: frozen.revision,
      cpuMsPerQuery: (usage.user + usage.system) / 1000 / repeats,
      countsPerQuery: report.countsPerQuery,
      phases: report.attribution.phases,
    }),
  );
} finally {
  session.disconnect();
  await harness.close();
  abort.signal.removeEventListener("abort", cancel);
  process.off("SIGINT", stop);
  process.off("SIGTERM", stop);
}

function category(stack: readonly Frame[]): string {
  const first = stack[0]?.functionName;
  if (first === "(garbage collector)") return "GC (unassigned)";
  if (first !== undefined && ["(program)", "(idle)", "(root)"].includes(first))
    return "idle/program";
  if (
    stack.some(
      (frame) =>
        frame.url.includes("/next/format/manifest-codec.js") &&
        ["select", "selectGroupRootColumns"].includes(frame.functionName),
    )
  )
    return "validated metadata materialization";
  for (const frame of stack) {
    const u = frame.url,
      n = frame.functionName;
    if (u.endsWith("/next/exec/run.js")) {
      if (["observe", "grant.allocate", "#meter"].includes(n)) return "governor";
      if (n === "readBoundary") return "pacing/pipeline";
      // Reduce/cancellation closures inherit their metering or scan-facade caller.
      if (n === "") continue;
    }
    if (u.endsWith("/next/exec/plan.js")) {
      if (["push", "next"].includes(n)) return "batch assembly";
      if (n === "") continue;
    }
    if (u.includes("/next/format/manifest-codec.js")) return "root/manifest parse";
    if (u.includes("/next/page/directory.js")) return "page directory parse";
    if (u.includes("/next/page/decode.js"))
      return stack.some((f) =>
        ["parsePage", "parseDictionary", "checkDictionaryCodes"].includes(f.functionName),
      )
        ? "page validation"
        : "column decode";
    if (u.includes("/next/memory/") || u.includes("/next/exec/admission.js")) return "governor";
    if (
      ["/next/exec/result", "/next/exec/column-store.js", "/next/exec/dictionaries.js"].some((p) =>
        u.includes(p),
      )
    )
      return "frame assembly/delivery";
    if (u.includes("/next/page/read-bounds.js")) return "read admission sizing";
    if (
      [
        "/next/exec/plan.js",
        "/next/exec/create.js",
        "/next/exec/run.js",
        "/next/exec/parameters.js",
        "/next/lower/",
      ].some((p) => u.includes(p))
    )
      return "plan/query setup";
    if (
      ["/next/exec/table-scan.js", "/next/exec/column-target.js", "/next/exec/pushdown.js"].some(
        (p) => u.includes(p),
      )
    )
      return "batch assembly";
    if (["/next/storage/", "/next/hash/"].some((p) => u.includes(p))) return "storage/cache/verify";
    if (["/next/access/", "/next/exec/read-table.js"].some((p) => u.includes(p)))
      return "snapshot/visibility";
    if (
      [
        "/next/format/metadata-reader.js",
        "/next/page/query-roots.js",
        "/next/page/group-read.js",
      ].some((p) => u.includes(p))
    )
      return "root hold/selection";
    if (u.includes("/next/page/reader.js")) return "page reader/ranges";
    if (u.includes("/next/task.js") || u.includes("/next/exec/pipeline.js"))
      return "pacing/pipeline";
    if (n === "(garbage collector)") return "GC (unassigned)";
  }
  return "other";
}
function summarize(profile: Profile) {
  const nodes = new Map(profile.nodes.map((n) => [n.id, n])),
    parents = new Map<number, number>();
  for (const node of profile.nodes)
    for (const child of node.children ?? []) parents.set(child, node.id);
  const phases = new Map<string, { samples: number; sampledWallMs: number }>(),
    functions = new Map<string, { frame: Frame; samples: number; sampledWallMs: number }>();
  const samples = profile.samples ?? [],
    deltas = profile.timeDeltas ?? [];
  if (samples.length === 0 || samples.length !== deltas.length)
    throw new Error("Missing V8 samples");
  for (let k = 0; k < samples.length; k++) {
    const stack: Frame[] = [];
    for (let id = samples[k]; id !== undefined; id = parents.get(id)) {
      const node = nodes.get(id);
      if (node === undefined) throw new Error("Missing V8 node");
      stack.push(node.callFrame);
    }
    const name = category(stack),
      ms = (deltas[k] ?? 0) / 1000;
    const phase = phases.get(name) ?? { samples: 0, sampledWallMs: 0 };
    phase.samples++;
    phase.sampledWallMs += ms;
    phases.set(name, phase);
    const frame = stack[0];
    if (frame === undefined) throw new Error("Missing self frame");
    const key = `${frame.url}:${String(frame.lineNumber)}:${frame.functionName}`,
      entry = functions.get(key) ?? { frame, samples: 0, sampledWallMs: 0 };
    entry.samples++;
    entry.sampledWallMs += ms;
    functions.set(key, entry);
  }
  return {
    sampleCount: samples.length,
    phases: [...phases]
      .map(([phase, v]) => ({ phase, ...v, share: v.samples / samples.length }))
      .sort((a, b) => b.samples - a.samples),
    selfFunctions: [...functions.values()]
      .map((v) => ({ ...v, share: v.samples / samples.length }))
      .sort((a, b) => b.samples - a.samples)
      .slice(0, 40),
  };
}
