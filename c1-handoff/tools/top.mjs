import { readFileSync } from "node:fs";
const prof = JSON.parse(readFileSync(process.argv[2], "utf8"));
const byId = new Map(prof.nodes.map((n) => [n.id, n]));
const self = new Map();
const dt = prof.timeDeltas; let total = 0;
prof.samples.forEach((id, i) => { const d = dt[i] ?? 0; total += d; self.set(id, (self.get(id) ?? 0) + d); });
// self time by function
const fn = new Map();
for (const [id, t] of self) { const n = byId.get(id); const cf = n.callFrame; const key = `${cf.functionName || "(anon)"} ${cf.url.split("/").slice(-2).join("/")}:${cf.lineNumber + 1}`; fn.set(key, (fn.get(key) ?? 0) + t); }
// inclusive time by function (count once per stack)
const parent = new Map(); for (const n of prof.nodes) for (const c of n.children ?? []) parent.set(c, n.id);
const incl = new Map();
for (const [id, t] of self) { const seen = new Set(); let cur = id; while (cur !== undefined) { const cf = byId.get(cur).callFrame; const key = `${cf.functionName || "(anon)"} ${cf.url.split("/").slice(-2).join("/")}:${cf.lineNumber + 1}`; if (!seen.has(key)) { seen.add(key); incl.set(key, (incl.get(key) ?? 0) + t); } cur = parent.get(cur); } }
const pct = (t) => ((100 * t) / total).toFixed(1).padStart(5);
console.log("total ms", (total / 1000).toFixed(0));
console.log("--- self"); [...fn].sort((a, b) => b[1] - a[1]).slice(0, Number(process.argv[3] ?? 30)).forEach(([k, t]) => console.log(pct(t), k));
console.log("--- inclusive"); [...incl].sort((a, b) => b[1] - a[1]).filter(([k]) => /\.ts:/.test(k)).slice(0, Number(process.argv[4] ?? 60)).forEach(([k, t]) => console.log(pct(t), k));
