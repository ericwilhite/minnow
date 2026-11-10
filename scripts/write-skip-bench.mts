/** Streaming OFFSET: nine AB/BA pairs, parent next/discard vs candidate skip, exact locators.
 * taskset -c 3,7 node --expose-gc scripts/write-skip-bench.mts --baseline <core-dist>
 * Setup, one warmup per arm, and explicit GC are outside clocks. Three queries per sample.
 * --full checks the unchanged full-range next path. No planner, payload fetch or transport.
 */
import { deepStrictEqual } from "node:assert";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { loadavg } from "node:os";
import { summarizePairDeltas } from "./write-bench-pairs.mts";
function need<T>(value: T | undefined | null): T {
  if (value === undefined || value === null) throw new Error("Missing benchmark value");
  return value;
}
const args = process.argv.slice(2);
function option(name: string, fallback: string): string {
  const at = args.indexOf(`--${name}`);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
}
const rows = Number(option("rows", "1000000")),
  full = args.includes("--full");
if (!Number.isSafeInteger(rows) || rows < 256) throw new Error("--rows must be an integer >=256");
const baseline = option("baseline", ""),
  candidate = option("candidate", "packages/core/dist");
if (!baseline) throw new Error("--baseline core-dist is required");
type Support = typeof import("../packages/core/dist/next/write/test-support.js");
async function prepare(dist: string) {
  const s = (await import(
    pathToFileURL(resolve(dist, "next/write/test-support.js")).href
  )) as Support;
  const h = new s.Harness("strict", "memory"),
    p = await h.open();
  let table = 0;
  await s.autocommit(p, async (st) => {
    table =
      (
        await st.changeCatalog({
          kind: "createTable",
          table: s.tableInput("t", [s.column("id", s.INT, false)]),
        })
      ).id ?? 0;
  });
  const tx = await p.begin(),
    st = tx.statement({ rules: s.rules });
  for (let from = 0; from < rows; from += 5000)
    await st
      .table(table)
      .insert(
        s.batch(s.ints(Array.from({ length: Math.min(5000, rows - from) }, (_, k) => from + k))),
        null,
      );
  await st.end();
  await (
    await tx.commit(null)
  ).durable;
  await p.quiesce();
  await p.recycleLog();
  await p.quiesce();
  const view = p.pin(),
    reader = view.index(table, need(need(p.catalog.table(table)).primaryKey));
  const out = new Float64Array(2048);
  return {
    async run(skip: boolean) {
      const cursor = reader.range({ lower: null, upper: null }, "asc");
      let remaining = full ? 0 : rows - 128,
        count = 0,
        checksum = 0;
      const tail: number[] = [];
      try {
        if (skip && remaining > 0) {
          const n = await cursor.skip(remaining);
          if (n !== remaining) throw new Error(`skip returned ${n}`);
          remaining = 0;
        }
        while (remaining > 0) {
          const n = await cursor.next(out.subarray(0, Math.min(remaining, out.length)));
          if (!n) throw new Error("early EOF while discarding");
          remaining -= n;
        }
        for (;;) {
          const n = await cursor.next(out);
          if (!n) break;
          for (let k = 0; k < n; k++) {
            const row = reader.table.rowId(need(out[k]));
            count++;
            checksum = (Math.imul(checksum, 33) + row) >>> 0;
            if (!full) tail.push(row);
          }
        }
        return { count, checksum, tail };
      } finally {
        cursor.close();
      }
    },
    async close() {
      view.release();
      await h.close();
    },
  };
}
const arms = { baseline: await prepare(baseline), candidate: await prepare(candidate) };
const samples: Record<"baseline" | "candidate", Array<{ cpuMs: number; wallMs: number }>> = {
  baseline: [],
  candidate: [],
};
try {
  const expected = await arms.baseline.run(false);
  deepStrictEqual(await arms.candidate.run(true), expected);
  for (let pair = 0; pair < 9; pair++) {
    const order =
      pair % 2 === 0 ? (["baseline", "candidate"] as const) : (["candidate", "baseline"] as const);
    for (const label of order) {
      global.gc?.();
      const host = loadavg(),
        cpu = process.cpuUsage(),
        wall = performance.now();
      const results = [];
      for (let sample = 0; sample < 3; sample++)
        results.push(await arms[label].run(label === "candidate"));
      const wallMs = (performance.now() - wall) / 3,
        used = process.cpuUsage(cpu),
        cpuMs = (used.user + used.system) / 3000;
      for (const result of results) deepStrictEqual(result, expected);
      samples[label].push({ cpuMs, wallMs });
      console.log(
        JSON.stringify({ pair, label, rows, full, cpuMs, wallMs, host, result: expected }),
      );
    }
  }
  const cpu = summarizePairDeltas(
    samples.baseline.map((s, i) => s.cpuMs - need(samples.candidate[i]).cpuMs),
  );
  const wall = summarizePairDeltas(
    samples.baseline.map((s, i) => s.wallMs - need(samples.candidate[i]).wallMs),
  );
  console.log(JSON.stringify({ summary: true, rows, full, samples, cpu, wall }));
} finally {
  await arms.baseline.close();
  await arms.candidate.close();
}
