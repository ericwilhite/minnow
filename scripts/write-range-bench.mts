/**
 * First-page range CPU: retained eager reference vs bounded cursor on the same pinned index.
 * node --expose-gc scripts/write-range-bench.mts --rows 1000000 --stored --rounds 7
 * No payload fetch, SQL, disk fsync or owner transport. Setup/GC/profile serialization excluded.
 * Warmup then >=5 AB/BA rounds; output equality, CPU/wall/spread and load average reported.
 * --full --out 2048 drains the range (ordered count/checksum), for whole-scan regression.
 * --profile under ~/.cache records one extra eager/streamed pair, outside measured rounds.
 */
import { deepStrictEqual } from "node:assert";
import { loadavg, homedir } from "node:os";
import { Session, type Profiler } from "node:inspector";
import { writeFile } from "node:fs/promises";
import { resolve, relative, join } from "node:path";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rules,
  tableInput,
} from "../packages/core/dist/next/write/test-support.js";
const args = process.argv.slice(2);
const option = (name: string, fallback: string) => {
  const at = args.indexOf(`--${name}`);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
};
const integer = (name: string, fallback: number) => {
  const n = Number(option(name, String(fallback)));
  if (!Number.isSafeInteger(n) || n < 1) throw new Error(`Invalid --${name}`);
  return n;
};
const rows = integer("rows", 1000000),
  rounds = integer("rounds", 7),
  output = integer("out", 128);
if (rounds < 5) throw new Error("At least five interleaved rounds required");
const h = new Harness("strict", "memory"),
  p = await h.open();
const samples: Record<string, number[]> = { eager: [], streamed: [] };
const all = { lower: null, upper: null };
const profileFile = option("profile", "");
const full = args.includes("--full");
let t = 0;
try {
  await autocommit(p, async (st) => {
    t =
      (
        await st.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  const tx = await p.begin();
  for (let from = 0; from < rows; from += 2048) {
    const st = tx.statement({ rules });
    await st
      .table(t)
      .insert(
        batch(ints(Array.from({ length: Math.min(2048, rows - from) }, (_, k) => from + k))),
        null,
      );
    await st.end();
  }
  await (
    await tx.commit(null)
  ).durable;
  await p.quiesce();
  if (args.includes("--stored")) {
    await p.recycleLog();
    await p.quiesce();
  }
  const view = p.pin(),
    reader = view.index(t, p.catalog.table(t)?.primaryKey ?? 0),
    out = new Float64Array(Math.min(rows, output));
  const run = async (label: string): Promise<number[]> => {
    let count = 0,
      checksum = 0;
    const consume = (at: number) => {
      count++;
      checksum = (Math.imul(checksum, 33) + at) >>> 0;
    };
    if (label === "eager") {
      const entries = await reader.collect(all);
      if (!full) return entries.slice(0, out.length).map((e) => reader.table.locator(e.rowId));
      for (const entry of entries) consume(reader.table.locator(entry.rowId));
      return [count, checksum];
    }
    const cursor = reader.range(all, "asc");
    try {
      for (;;) {
        const got = cursor.next(out),
          n = got instanceof Promise ? await got : got;
        if (!full) return Array.from(out.subarray(0, n));
        if (n === 0) return [count, checksum];
        for (let i = 0; i < n; i++) consume(out[i] ?? 0);
      }
    } finally {
      cursor.close();
    }
  };
  try {
    deepStrictEqual(await run("eager"), await run("streamed"));
    for (let round = -1; round < rounds; round++)
      for (const label of round % 2 === 0 ? ["eager", "streamed"] : ["streamed", "eager"]) {
        global.gc?.();
        const loadAverageBefore = loadavg(),
          wall = performance.now(),
          cpu = process.cpuUsage();
        const result = await run(label),
          used = process.cpuUsage(cpu),
          cpuMs = (used.user + used.system) / 1000;
        if (round >= 0) samples[label]?.push(cpuMs);
        console.log(
          JSON.stringify({
            round,
            label,
            rows,
            output,
            stored: args.includes("--stored"),
            full,
            cpuMs,
            wallMs: performance.now() - wall,
            count: full ? result[0] : result.length,
            first: full ? undefined : result[0],
            checksum: full ? result[1] : undefined,
            loadAverageBefore,
            loadAverageAfter: loadavg(),
          }),
        );
      }
    if (profileFile) {
      const file = resolve(profileFile);
      if (relative(join(homedir(), ".cache"), file).startsWith(".."))
        throw new Error("Profile belongs under ~/.cache");
      for (const label of ["eager", "streamed"]) {
        global.gc?.();
        const session = new Session();
        session.connect();
        await new Promise<void>((ok, no) =>
          session.post("Profiler.enable", (e) => (e ? no(e) : ok())),
        );
        await new Promise<void>((ok, no) =>
          session.post("Profiler.start", (e) => (e ? no(e) : ok())),
        );
        for (let repeat = 0; repeat < (label === "streamed" ? (full ? 4 : 2048) : 1); repeat++)
          await run(label);
        const profile = await new Promise<Profiler.Profile>((ok, no) =>
          session.post("Profiler.stop", (e, r) => (e ? no(e) : ok(r.profile))),
        );
        session.disconnect();
        await writeFile(`${file}.${label}.cpuprofile`, JSON.stringify(profile));
      }
    }
  } finally {
    view.release();
  }
  const stats = (values: number[]) => {
    const sorted = [...values].sort((a, b) => a - b);
    return { median: sorted[Math.floor(sorted.length / 2)], min: sorted[0], max: sorted.at(-1) };
  };
  console.log(
    JSON.stringify({
      summary: true,
      full,
      rows,
      output,
      cpuMs: { eager: stats(samples.eager ?? []), streamed: stats(samples.streamed ?? []) },
      governorPeak: h.governor.peakBytes,
      formatChange: false,
    }),
  );
} finally {
  await h.close();
}
