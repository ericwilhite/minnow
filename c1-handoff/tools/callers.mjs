// callers.mjs PROFILE FUNCTION DEPTH: time in FUNCTION grouped by its caller chain (DEPTH frames up).
import { readFileSync } from "node:fs";
const [file, target, depth = "4"] = process.argv.slice(2);
const prof = JSON.parse(readFileSync(file, "utf8"));
const byId = new Map(prof.nodes.map((n) => [n.id, n]));
const parent = new Map(); for (const n of prof.nodes) for (const c of n.children ?? []) parent.set(c, n.id);
const self = new Map(); let total = 0;
prof.samples.forEach((id, i) => { const d = prof.timeDeltas[i] ?? 0; total += d; self.set(id, (self.get(id) ?? 0) + d); });
const name = (id) => { const cf = byId.get(id).callFrame; return `${cf.functionName || "(anon)"} ${cf.url.split("/").slice(-2).join("/")}:${cf.lineNumber + 1}`; };
const groups = new Map();
for (const [id, t] of self) {
  let cur = id, hit = null;
  while (cur !== undefined) { if (byId.get(cur).callFrame.functionName === target) { hit = cur; break; } cur = parent.get(cur); }
  if (hit === null) continue;
  const chain = []; let up = parent.get(hit);
  while (up !== undefined && chain.length < Number(depth)) { chain.push(name(up)); up = parent.get(up); }
  const key = chain.join(" < ");
  groups.set(key, (groups.get(key) ?? 0) + t);
}
[...groups].sort((a, b) => b[1] - a[1]).slice(0, 8).forEach(([k, t]) => console.log(((100 * t) / total).toFixed(1).padStart(5), k));
