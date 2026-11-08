/** Setup-excluded V8 executor profiles, including bundled component harnesses.
 * Usage: node scripts/exec-profile.mts region.cpuprofile [...]
 * Phases are exclusive nearest classified ancestors. Sample counts estimate hot shares;
 * timeDeltas are sampled wall time (including descheduling/waits), never phase CPU seconds.
 */
import { readFileSync } from "node:fs";
import { basename } from "node:path";

interface Frame {
  functionName: string;
  url: string;
  lineNumber: number;
}
interface Node {
  id: number;
  callFrame: Frame;
  children?: number[];
}
interface Profile {
  nodes: Node[];
  samples: number[];
  timeDeltas: number[];
}
interface Count {
  samples: number;
  sampledWallMs: number;
}
function phase({ functionName: n, url: u }: Frame): string | null {
  if (n === "(garbage collector)") return "GC";
  if (["(idle)", "(program)"].includes(n)) return "idle/program";
  if (!u && n === "read") return "native read (CPU/wait inseparable)";
  if (/\/exec\/agg\//u.test(u)) return "post-window/group aggregation";
  if (
    /^(?:gather|writeFromStore)/u.test(n) ||
    ["#gatherFrom", "#emit", "#gather", "#appendAllFrom"].includes(n)
  )
    return "gather/output copying";
  if (["frameBounds", "rangeBounds", "floatRangeContains", "offsetOf"].includes(n))
    return "frame bounds";
  if (["aggregate", "ranking", "values", "isRunning", "bigExtremes"].includes(n))
    return "window evaluation";
  if (["fillPeers", "markChanges", "#flag", "#markPeers"].includes(n))
    return "peer/boundary detection";
  if (["#normalize", "encode", "encodeKeyBatch", "#encode"].includes(n)) return "key normalization";
  if (["#partitionIds", "#idOf", "#newGroup", "#nullId"].includes(n)) return "partition IDs";
  if (
    /^(?:msdSort|lsdSort|insertionWords|insertionSort|scatter16)/u.test(n) ||
    [
      "leaf",
      "refineRuns",
      "firstDifference",
      "msdWords",
      "sortJob",
      "#byRank",
      "#sortRange",
      "#orderedKey",
      "difference",
      "histogram",
      "scatter",
    ].includes(n) ||
    /\/exec\/(?:sort-keys|sort-lsd|window-sort)\.[jt]s$/u.test(u)
  )
    return "ordering/partition ranking";
  if (["#extract", "#bigReservation", "#scratchFor"].includes(n))
    return "argument extraction/scratch";
  if (["append", "#writeVectors", "#prepareVectors", "#appendWith", "writeVector"].includes(n))
    return "store arrival";
  if (
    /\/exec\/(?:result|column-target)\.[jt]s$/u.test(u) ||
    /^(?:oracleRows|oracleValue|flatValue)$/u.test(n) ||
    u.includes("/owner/frame.")
  )
    return "result materialization/delivery";
  if (/\/exec\/table-scan\.[jt]s$/u.test(u) || /\/next\/(?:page|storage|format|access)\//u.test(u))
    return "scan/storage/decoding";
  if (/\/exec\/join(?:-|\.)/u.test(u)) return "join";
  if (/\/exec\/sort\.[jt]s$/u.test(u)) return "sort/top-k";
  if (/\/exec\/window\.[jt]s$/u.test(u)) return "window coordination";
  if (/\/exec\/window-frames\.[jt]s$/u.test(u)) return "window frame work";
  if (/\/exec\/column-store\.[jt]s$/u.test(u)) return "column store setup/copying";
  if (/\/exec\/(?:pipeline|slices)\.[jt]s$/u.test(u)) return "pipeline/task clock";
  if (u.endsWith("/scripts/next-executor-bench.mts")) return "bench result materialization";
  if (/\/lower\//u.test(u) || /\/engine\/(?:query|optimizer)\./u.test(u))
    return "planning/lowering";
  return null;
}
for (const path of process.argv.slice(2)) {
  const p = JSON.parse(readFileSync(path, "utf8")) as Profile;
  const nodes = new Map(p.nodes.map((node) => [node.id, node]));
  const parents = new Map<number, number>();
  for (const node of p.nodes) for (const child of node.children ?? []) parents.set(child, node.id);
  const phases = new Map<string, Count>(),
    functions = new Map<string, Count>();
  let observerMs = 0,
    samples = 0,
    sampledWallMs = 0;
  const add = (map: Map<string, Count>, key: string, ms: number) => {
    const count = map.get(key) ?? { samples: 0, sampledWallMs: 0 };
    count.samples++;
    count.sampledWallMs += ms;
    map.set(key, count);
  };
  for (const [index, id] of p.samples.entries()) {
    const leaf = nodes.get(id);
    if (leaf === undefined) throw new Error(`Missing sample node ${String(id)}`);
    const stack: Frame[] = [];
    for (let at: number | undefined = id; at !== undefined; at = parents.get(at)) {
      const node = nodes.get(at);
      if (node === undefined) throw new Error("Missing ancestor");
      stack.push(node.callFrame);
    }
    const ms = (p.timeDeltas[index] ?? 0) / 1000;
    if (stack.some((f) => f.url.startsWith("node:inspector"))) {
      observerMs += ms;
      continue;
    }
    // Fail if a supposedly isolated operator profile includes a fixture constructor.
    if (
      basename(path).startsWith("region-") &&
      stack.some((f) =>
        ["dictText", "numbers", "flatText", "windowShape", "shape", "batches"].includes(
          f.functionName,
        ),
      )
    )
      throw new Error("Input construction in an operator profile");
    samples++;
    sampledWallMs += ms;
    add(phases, stack.map(phase).find((value) => value !== null) ?? "runtime/other", ms);
    const f = leaf.callFrame;
    add(functions, `${f.functionName || "(anonymous)"} ${f.url}:${String(f.lineNumber + 1)}`, ms);
  }
  const table = (map: Map<string, Count>) =>
    [...map]
      .sort((a, b) => b[1].samples - a[1].samples)
      .map(([name, count]) => ({
        name,
        ...count,
        sampleSharePct: (100 * count.samples) / samples,
      }));
  console.log(
    JSON.stringify({
      path,
      samples,
      sampledWallMs,
      observerMs,
      method:
        "exclusive nearest classified ancestor; sample-count shares and sampled wall durations, not phase CPU seconds",
      phases: table(phases),
      self: table(functions).slice(0, 15),
    }),
  );
}
