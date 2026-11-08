/**
 * Production-built write core on real disk, in a persistent Chromium OPFS worker.
 * Build core first. No Vite transforms or test cached-byte guard. All generated bundles,
 * profiles and logs go under --output (must be under os.tmpdir()). One process/worker at a time.
 *
 * node scripts/write-browser-bench.mts --baseline "$TMPDIR/b3-old/dist" \
 *   --output "$TMPDIR/b3-browser" --mode single --durability relaxed
 * Modes/durability/load-mode/rows/singles match write-insert-bench.mts. Defaults: 6 measured
 * interleaved AB/BA pairs after a discarded warmup pair, 20k relaxed / 2k strict singles,
 * 1M load rows. The input
 * batches and 100k-row single-insert preload are outside the clock. Singles flush at the end;
 * strict singles await each durable receipt. Loads include quiesce (maintenance on).
 *
 * Each pass records host load averages (1/5/15 minute) immediately before and after timing.
 * CPU is Linux /proc utime+stime, including JIT/GC threads: renderer CPU (owner process), whole
 * browser CPU (also storage IPC), and the dedicated worker thread separately. 10 ms USER_HZ
 * granularity is reported. Wall is measured inside the worker; CPU brackets its RPC. Profiles
 * are separate extra passes, excluded from timings. Each pass closes, terminates the writer,
 * reopens in a fresh worker and checks exact live-row and index-entry counts before removing
 * its database. This checks clean reopen, not abrupt crash/power-loss safety. No SQL, executor,
 * client transport, SQLite comparison, or concurrency claim; the unexported write core only.
 */
import { createServer } from "node:http";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { readdirSync, readFileSync, statfsSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join, resolve, relative } from "node:path";
import { tmpdir, loadavg } from "node:os";
import { build } from "esbuild";
import { chromium, type CDPSession } from "@playwright/test";
import type { WriteBenchInput } from "../packages/core/browser/next-write-bench-worker.js";

const args = process.argv.slice(2);
const option = (name: string, fallback: string) => {
  const at = args.indexOf(`--${name}`);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
};
const integer = (name: string, fallback: number, minimum = 1) => {
  const n = Number(option(name, String(fallback)));
  if (!Number.isSafeInteger(n) || n < minimum) throw new Error(`Invalid --${name}`);
  return n;
};
const baseline = option("baseline", "");
if (!baseline) throw new Error("--baseline must name the parent's built core dist");
const scratchRoot = tmpdir();
const output = resolve(option("output", join(scratchRoot, "minnow-write-browser")));
if (relative(scratchRoot, output).startsWith(".."))
  throw new Error("--output must be under os.tmpdir(); set TMPDIR to choose its disk");
await mkdir(output, { recursive: true });
process.env.TMPDIR = join(output, "tmp");
await mkdir(process.env.TMPDIR, { recursive: true });
const fsType = execFileSync("findmnt", ["-n", "-o", "FSTYPE", "-T", output], {
  encoding: "utf8",
}).trim();
if (["tmpfs", "ramfs"].includes(fsType)) throw new Error("A real disk profile is required");
const ticks = Number(execFileSync("getconf", ["CLK_TCK"], { encoding: "utf8" }).trim());
const rounds = integer("rounds", 6, 5);
const mode = option("mode", "single");
const durability = option("durability", "relaxed");
const loadMode = option("load-mode", "tx");
if (mode !== "single" && mode !== "load") throw new Error("Invalid --mode");
if (durability !== "strict" && durability !== "relaxed") throw new Error("Invalid --durability");
if (loadMode !== "tx" && loadMode !== "commits") throw new Error("Invalid --load-mode");
const config: WriteBenchInput = {
  dir: "",
  mode,
  durability,
  loadMode,
  rows: integer("rows", 1_000_000),
  singles: integer("singles", durability === "strict" ? 2_000 : 20_000),
};
const directories = {
  baseline: resolve(baseline),
  candidate: resolve(option("candidate", "packages/core/dist")),
};
type Label = keyof typeof directories;
const bundles = new Map<string, string>();
for (const label of ["baseline", "candidate"] as const) {
  const result = await build({
    entryPoints: ["packages/core/browser/next-write-bench-worker.ts"],
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    plugins: [
      {
        name: "built-core",
        setup(builder) {
          builder.onResolve({ filter: /^\.\.\/dist\// }, (args) => ({
            path: join(directories[label], args.path.slice("../dist/".length)),
          }));
        },
      },
    ],
  });
  const js = result.outputFiles[0]?.text;
  if (!js) throw new Error("Worker build is empty");
  bundles.set(`/${label}.js`, js);
  await writeFile(join(output, `${label}.js`), js);
}
const server = createServer((request, response) => {
  const js = bundles.get(request.url ?? "");
  response.setHeader("content-type", js ? "text/javascript" : "text/html");
  response.end(
    js ??
      "<!doctype html><title>Write-core disk benchmark</title><p>OPFS worker benchmark; CPU and disk results are recorded by the runner.</p>",
  );
});
await new Promise<void>((done) => server.listen(integer("port", 4186), "127.0.0.1", done));
const address = server.address();
if (!address || typeof address === "string") throw new Error("Missing server address");
const url = `http://127.0.0.1:${String(address.port)}`;
console.log(
  JSON.stringify({
    server: url,
    output,
    fsType,
    fsMagic: statfsSync(output).type,
    ticks,
    config,
    directories,
  }),
);
const profile = join(output, "profile");
const context = await chromium
  .launchPersistentContext(profile, { headless: true })
  .catch((error: unknown) => {
    server.close();
    throw error;
  });
try {
  const page = await context.newPage();
  await page.goto(url);

  /** All processes descended from this persistent profile's browser, with CPU and RSS. */
  function cpu() {
    const processes = new Map<number, { parent: number; cmd: string; ticks: number }>();
    for (const name of readdirSync("/proc")) {
      if (!/^\d+$/u.test(name)) continue;
      try {
        const raw = readFileSync(`/proc/${name}/stat`, "utf8");
        const fields = raw.slice(raw.lastIndexOf(")") + 2).split(" ");
        processes.set(Number(name), {
          parent: Number(fields[1]),
          ticks: Number(fields[11]) + Number(fields[12]),
          cmd: readFileSync(`/proc/${name}/cmdline`, "utf8").replaceAll("\0", " "),
        });
      } catch {
        /* Process exited. */
      }
    }
    const selected = new Set<number>();
    const add = (id: number) => {
      if (selected.has(id)) return;
      selected.add(id);
      for (const [child, p] of processes) if (p.parent === id) add(child);
    };
    for (const [id, p] of processes)
      if (p.cmd.includes(`--user-data-dir=${profile}`) && !p.cmd.includes("--type=")) add(id);
    if (selected.size === 0) throw new Error("Cannot find the benchmark browser CPU counters");
    let browser = 0,
      renderer = 0,
      worker = 0;
    for (const id of selected) {
      const p = processes.get(id);
      if (!p) continue;
      browser += p.ticks;
      if (!p.cmd.includes("--type=renderer")) continue;
      renderer += p.ticks;
      for (const tid of readdirSync(`/proc/${String(id)}/task`)) {
        try {
          if (
            !readFileSync(`/proc/${String(id)}/task/${tid}/comm`, "utf8").startsWith(
              "DedicatedWorker",
            )
          )
            continue;
          const raw = readFileSync(`/proc/${String(id)}/task/${tid}/stat`, "utf8");
          const fields = raw.slice(raw.lastIndexOf(")") + 2).split(" ");
          worker += Number(fields[11]) + Number(fields[12]);
        } catch {
          /* Worker exited. */
        }
      }
    }
    return { browser, renderer, worker };
  }

  interface Rpc {
    call(op: string, input: WriteBenchInput, profile?: boolean): Promise<Record<string, unknown>>;
    stop(): void;
  }
  async function worker(label: Label) {
    await page.evaluate((url) => {
      const w = new Worker(url, { type: "module" });
      let next = 0;
      const pending = new Map<
        number,
        { resolve(value: Record<string, unknown>): void; reject(error: Error): void }
      >();
      w.onmessage = (
        event: MessageEvent<{ id: number; value: Record<string, unknown>; error?: string }>,
      ) => {
        const p = pending.get(event.data.id);
        pending.delete(event.data.id);
        if (event.data.error) p?.reject(new Error(event.data.error));
        else p?.resolve(event.data.value);
      };
      w.onerror = (event) => {
        for (const p of pending.values()) p.reject(new Error(event.message));
        pending.clear();
      };
      (globalThis as unknown as { b3: Rpc }).b3 = {
        call(op, input, profile) {
          const id = ++next;
          return new Promise((resolve, reject) => {
            pending.set(id, { resolve, reject });
            w.postMessage({ id, op, input, profile });
          });
        },
        stop() {
          w.terminate();
        },
      };
    }, `${url}/${label}.js`);
  }
  const call = (op: string, input: WriteBenchInput, profile = false) =>
    page.evaluate(
      ({ op, input, profile }) =>
        (globalThis as unknown as { b3: Rpc }).b3.call(op, input, profile),
      { op, input, profile },
    );
  const stop = () => page.evaluate(() => (globalThis as unknown as { b3: Rpc }).b3.stop());

  // Auto-attach Profiler to dedicated workers. Extra console.profile passes are outside A/B timing.
  const cdp: CDPSession = await context.newCDPSession(page);
  const profiles: unknown[] = [];
  let messageId = 0;
  cdp.on("Target.attachedToTarget", (e) => {
    if (e.targetInfo.type !== "worker") return;
    void cdp
      .send("Target.sendMessageToTarget", {
        sessionId: e.sessionId,
        message: JSON.stringify({ id: ++messageId, method: "Profiler.enable" }),
      })
      .catch(() => undefined);
  });
  cdp.on("Target.receivedMessageFromTarget", (e) => {
    const m = JSON.parse(e.message) as { method?: string; params?: { profile?: unknown } };
    if (m.method === "Profiler.consoleProfileFinished" && m.params?.profile)
      profiles.push(m.params.profile);
  });
  await cdp.send("Target.setAutoAttach", {
    autoAttach: true,
    waitForDebuggerOnStart: false,
    flatten: false,
  });

  const log = join(output, "rounds.jsonl");
  await writeFile(log, "");
  interface Result {
    rendererCpuMs: number;
    browserCpuMs: number;
    workerCpuMs: number;
    wallMs: number;
  }
  const results: Record<Label, Result[]> = { baseline: [], candidate: [] };
  async function pass(label: Label, round: number, profiled = false) {
    const input = { ...config, dir: `b3-${label}-${String(round)}-${String(Date.now())}` };
    await worker(label);
    await call("prepare", input);
    const loadAverageBefore = loadavg();
    const before = cpu();
    const result = await call("run", input, profiled);
    const after = cpu();
    const loadAverageAfter = loadavg();
    const measured = {
      rendererCpuMs: ((after.renderer - before.renderer) * 1000) / ticks,
      browserCpuMs: ((after.browser - before.browser) * 1000) / ticks,
      workerCpuMs: ((after.worker - before.worker) * 1000) / ticks,
      wallMs: Number(result.wallMs),
    };
    await call("close", input);
    await stop();
    await worker(label);
    const verified = await call("verify", input);
    await call("remove", input);
    await stop();
    const record = {
      label,
      round,
      warmup: round < 0,
      profiled,
      loadAverageBefore,
      loadAverageAfter,
      ...measured,
      ...result,
      ...verified,
    };
    console.log(JSON.stringify(record));
    const old = await readFile(log, "utf8");
    await writeFile(log, old + JSON.stringify(record) + "\n");
    if (round >= 0 && !profiled) results[label].push(measured);
  }
  for (let round = -1; round < rounds; round++)
    for (const label of (round % 2 === 0
      ? ["baseline", "candidate"]
      : ["candidate", "baseline"]) as Label[])
      await pass(label, round);
  if (args.includes("--profile")) {
    await pass("baseline", rounds, true);
    await pass("candidate", rounds, true);
    await writeFile(join(output, "profiles.json"), JSON.stringify(profiles));
    if (profiles.length !== 2)
      throw new Error(`Expected two profiles, got ${String(profiles.length)}`);
  }
  const median = (values: number[]) => {
    const v = [...values].sort((a, b) => a - b);
    const at = Math.floor(v.length / 2);
    return v.length % 2 ? (v[at] ?? 0) : ((v[at - 1] ?? 0) + (v[at] ?? 0)) / 2;
  };
  const summary = (label: Label) =>
    Object.fromEntries(
      (["rendererCpuMs", "browserCpuMs", "workerCpuMs", "wallMs"] as const).map((key) => [
        key,
        {
          median: median(results[label].map((v) => v[key])),
          min: Math.min(...results[label].map((v) => v[key])),
          max: Math.max(...results[label].map((v) => v[key])),
        },
      ]),
    );
  const out = {
    browserVersion: context.browser()?.version(),
    fsType,
    ticks,
    rounds,
    config,
    directories,
    baseline: summary("baseline"),
    candidate: summary("candidate"),
    profile,
  };
  await writeFile(join(output, "summary.json"), JSON.stringify(out, null, 2));
  console.log(JSON.stringify({ summary: true, ...out }));
} finally {
  await context.close();
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
}
