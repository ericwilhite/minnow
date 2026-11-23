import { readFileSync } from "node:fs";
const p = JSON.parse(readFileSync(process.argv[2], "utf8"));
const byId = new Map(p.nodes.map((n) => [n.id, n]));
const parent = new Map(); for (const n of p.nodes) for (const c of n.children ?? []) parent.set(c, n.id);
const self = new Map(); let total = 0;
p.samples.forEach((id, i) => { const d = p.timeDeltas[i] ?? 0; total += d; self.set(id, (self.get(id) ?? 0) + d); });
// outermost engine file (closest to the root of the sampled stack)
const outer = new Map(), inner2 = new Map();
for (const [id, t] of self) {
  const chain = []; let cur = id;
  while (cur !== undefined) { const u = byId.get(cur).callFrame.url; if (/src\/next\/.*\.ts$/.test(u)) chain.push(`${u.split("/src/next/")[1]}:${byId.get(cur).callFrame.functionName || "anon"}`); cur = parent.get(cur); }
  const k = chain.length ? chain[chain.length - 1] : "(no engine frame: " + (byId.get(id).callFrame.functionName || byId.get(id).callFrame.url.split("/").pop() || "?") + ")";
  outer.set(k, (outer.get(k) ?? 0) + t);
}
console.log("total ms", (total / 1000).toFixed(0));
[...outer].sort((a, b) => b[1] - a[1]).slice(0, Number(process.argv[3] ?? 40)).forEach(([k, t]) => console.log(((100 * t) / total).toFixed(1).padStart(5), k));
