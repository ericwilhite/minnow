/** Summarize a setup-excluded CDP scan-worker profile (export-cpu-N attachment).
 * npx vite-node scripts/scan-profile.mts /path/to/export-cpu-0.json [...]
 * CDP timeDeltas are sampled wall duration, not exact CPU attribution. Keep native read
 * (CPU + wait inseparable), idle/program and GC explicit. Active sample-count shares are
 * a hotspot estimate, never an exact allocation of /proc thread CPU seconds.
 */
import { readFileSync } from "node:fs";

interface Frame {
  functionName: string;
  url: string;
}
interface Profile {
  nodes: Array<{ id: number; callFrame: Frame; children?: number[] }>;
  samples: number[];
  timeDeltas: number[];
}

function category(f: Frame, parent?: Frame): string | null {
  const n = f.functionName,
    u = f.url;
  if (n === "(garbage collector)") return "GC";
  if (["(idle)", "(program)", "(root)"].includes(n)) return "idle/program/unattributed";
  if (!u && n === "read") return "native read: CPU/wait inseparable";
  if (!u && n === "postMessage")
    return /\/(task|exec\/pipeline)\.ts/u.test(parent?.url ?? "")
      ? "task clock / pacing"
      : "structured clone / transfer";
  if (u.includes("/next/task.ts") && !["sortInSlices", "sift", "swap", "item"].includes(n))
    return "task clock / pacing";
  if (u.includes("/owner-frames.ts"))
    return ["flatBytes", "validityBytes", "textFlattenBytes"].includes(n)
      ? "bench frame sizing"
      : "bench frame building / ownership copies";
  if (u.includes("/exec/result.ts")) return "ResultChannel / frameByteLength";
  if (u.includes("/owner/frame.ts")) return "owner transfer-list policy";
  if (/\/(pushdown|filter|column-target)\.ts/u.test(u)) {
    if (u.includes("column-target") && ["decode", "decodeInto", "allValid"].includes(n))
      return "vector materialization";
    return "pushdown / shape and descriptor work";
  }
  if (u.includes("/exec/table-scan.ts"))
    return ["#bytesFor", "#workingSet"].includes(n)
      ? "pushdown / shape and descriptor work"
      : "table-scan batch / column overhead";
  if (u.includes("/page/reader.ts")) {
    if (["#together", "compare", "#loadCached"].includes(n))
      return "pushdown / shape and descriptor work";
    if (
      [
        "pagesForRows",
        "pagesInRange",
        "isSafe",
        "isDictionaryRange",
        "gatherBytes",
        "varlenBytes",
        "largeRowsOf",
        "largeRows",
        "largeValueBytesOf",
        "largeValueBytes",
      ].includes(n)
    )
      return "pushdown / shape and descriptor work";
    if (["gather", "gatherCodes", "decodeRange", "decodeCodes"].includes(n))
      return "vector materialization";
    if (n === "#forPages") return null; // Inherit its sizing/decode/value-loading caller.
    return "storage / page loading and validation";
  }
  if (u.includes("/page/decode.ts") || u.includes("/page/bits.ts")) return "vector materialization";
  if (u.includes("/page/directory.ts")) return "pushdown / shape and descriptor work";
  if (/\/next\/(storage|format|hash|memory)\//u.test(u))
    return "storage / page loading and validation";
  if (u.includes("/exec/pipeline.ts") || u.includes("/scan-export.ts"))
    return "pipeline / bench delivery coordination";
  return null;
}

for (const path of process.argv.slice(2)) {
  const p = JSON.parse(readFileSync(path, "utf8")) as Profile;
  const nodes = new Map(p.nodes.map((n) => [n.id, n])),
    parents = new Map<number, number>();
  for (const n of p.nodes) for (const c of n.children ?? []) parents.set(c, n.id);
  const buckets = new Map<string, { samples: number; sampledWallMs: number }>();
  const functions = new Map<string, { samples: number; sampledWallMs: number }>();
  let sampledWallMs = 0,
    activeSamples = 0;
  for (let k = 0; k < p.samples.length; k++) {
    const id = p.samples[k],
      node = id === undefined ? undefined : nodes.get(id);
    if (!node) throw new Error("Missing profile node");
    let at: number | undefined = id,
      bucket: string | null = null;
    while (at !== undefined && bucket === null) {
      const n = nodes.get(at);
      if (n) bucket = category(n.callFrame, nodes.get(parents.get(at) ?? -1)?.callFrame);
      at = parents.get(at);
    }
    bucket ??= "idle/program/unattributed";
    const ms = (p.timeDeltas[k] ?? 0) / 1000;
    sampledWallMs += ms;
    if (!bucket.startsWith("native read") && !bucket.startsWith("idle/")) activeSamples++;
    const b = buckets.get(bucket) ?? { samples: 0, sampledWallMs: 0 };
    b.samples++;
    b.sampledWallMs += ms;
    buckets.set(bucket, b);
    const f = node.callFrame,
      key = `${f.functionName || "(anonymous)"} ${f.url.split("?")[0] ?? ""}`.trim();
    const v = functions.get(key) ?? { samples: 0, sampledWallMs: 0 };
    v.samples++;
    v.sampledWallMs += ms;
    functions.set(key, v);
  }
  console.log(
    JSON.stringify({
      path,
      samples: p.samples.length,
      sampledWallMs,
      activeSamples,
      method:
        "exclusive nearest classified ancestor; self-time functions; CDP sample durations include descheduling and native waits, not per-bucket CPU seconds; active count shares omit native read and idle/program",
      buckets: [...buckets]
        .map(([bucket, v]) => ({
          bucket,
          ...v,
          wallShare: (100 * v.sampledWallMs) / sampledWallMs,
          activeCountShare:
            bucket.startsWith("native read") || bucket.startsWith("idle/")
              ? null
              : (100 * v.samples) / activeSamples,
        }))
        .sort((a, b) => b.sampledWallMs - a.sampledWallMs),
      topSelf: [...functions]
        .map(([name, v]) => ({ name, ...v, wallShare: (100 * v.sampledWallMs) / sampledWallMs }))
        .sort((a, b) => b.sampledWallMs - a.sampledWallMs)
        .slice(0, 40),
    }),
  );
}
