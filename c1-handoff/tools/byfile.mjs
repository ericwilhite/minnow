import { readFileSync } from "node:fs";
const p = JSON.parse(readFileSync(process.argv[2], "utf8"));
const byId = new Map(p.nodes.map((n) => [n.id, n]));
const parent = new Map(); for (const n of p.nodes) for (const c of n.children ?? []) parent.set(c, n.id);
const self = new Map(); let total = 0;
p.samples.forEach((id, i) => { const d = p.timeDeltas[i] ?? 0; total += d; self.set(id, (self.get(id) ?? 0) + d); });
// attribute node-internal / anonymous frames to nearest engine (.ts) ancestor
const owner = (id) => { let cur = id; while (cur !== undefined) { const u = byId.get(cur).callFrame.url; if (/\.ts$/.test(u)) return u.split("/src/next/")[1] ?? u; cur = parent.get(cur); } const cf = byId.get(id).callFrame; return cf.url ? cf.url.split("/").slice(-1)[0] : cf.functionName; };
const files = new Map();
for (const [id, t] of self) { const k = owner(id); files.set(k, (files.get(k) ?? 0) + t); }
console.log("total ms", (total / 1000).toFixed(0));
[...files].sort((a, b) => b[1] - a[1]).slice(0, Number(process.argv[3] ?? 30)).forEach(([k, t]) => console.log(((100 * t) / total).toFixed(1).padStart(5), k));
