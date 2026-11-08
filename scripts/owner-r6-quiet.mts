/** One command, sequential engines; no process termination, retries or contaminated summaries. */
import { spawn, execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { availableParallelism, loadavg, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { QuietHost } from "./owner-r6-policy.js";

const args = process.argv.slice(2);
if (args.length !== 0 && (args.length !== 2 || args[0] !== "--out" || !args[1]))
  throw new Error("Usage: npm run benchmark:owner:r6 -- [--out directory]");
const out = args[1] === undefined ? mkdtempSync(join(tmpdir(), "minnow-r6-")) : resolve(args[1]);
mkdirSync(out, { recursive: true });
// A fresh output directory prevents stale accepted results surviving a refused invocation.
const accepted = join(out, "report.json");
try {
  readFileSync(accepted);
  throw new Error("Choose a fresh output directory");
} catch (error) {
  if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
}
const host = new QuietHost();
function sample() {
  host.observe(loadavg());
}
function save(name: string, value: unknown) {
  writeFileSync(join(out, name), JSON.stringify(value, null, 2) + "\n");
}
async function command(
  name: string,
  argv: string[],
  extra: Record<string, string> = {},
  measure = true,
) {
  if (measure) {
    sample();
    host.assertQuiet();
  }
  console.log(name);
  const child = spawn(process.execPath, argv, {
    cwd: process.cwd(),
    env: { ...process.env, ...extra },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let log = "";
  let stdout = "";
  child.stdout.on("data", (chunk: Buffer) => {
    stdout += chunk.toString();
    log += chunk.toString();
  });
  child.stderr.on("data", (chunk: Buffer) => {
    log += chunk.toString();
  });
  const timer = measure ? setInterval(sample, 1000) : null;
  try {
    const code = await new Promise<number>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code) => resolve(code ?? 1));
    });
    writeFileSync(join(out, name + ".log"), log);
    writeFileSync(join(out, name + ".stdout.log"), stdout);
    if (measure) {
      sample();
      save("host.json", host.samples);
      host.assertQuiet();
    }
    return { code, log, stdout };
  } finally {
    if (timer !== null) clearInterval(timer);
  }
}
try {
  if (process.platform === "win32")
    throw new Error("R6 requires an OS load average; Windows does not supply one");
  sample();
  host.assertQuiet();
  save("preflight.json", host.samples);
  const built = await command(
    "build-core",
    [resolve("node_modules/typescript/bin/tsc"), "-b", "packages/core"],
    {},
    false,
  );
  if (built.code !== 0) throw new Error("Core build failed");
  // Build CPU is not a measurement; wait for its load history to decay, bounded to ten minutes.
  for (let attempt = 0; (loadavg()[0] ?? Infinity) > 1; attempt++) {
    if (attempt === 120) throw new Error("Build cooldown exceeded ten minutes");
    if (attempt % 3 === 0) console.log("Waiting for build load to decay below 1...");
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
  host.samples.length = 0;
  const records = [];
  let passed = true;
  for (const browser of ["chromium", "firefox", "webkit"]) {
    for (const lane of ["latency", "scheduling"]) {
      const name = `${browser}-${lane}`;
      const result = await command(
        name,
        [
          resolve("node_modules/@playwright/test/cli.js"),
          "test",
          "--config",
          "playwright.next-owner.config.ts",
          `next-owner-${lane}.spec.ts`,
          `--project=${browser}`,
          "--workers=1",
          "--retries=0",
          ...(lane === "latency" ? ["--repeat-each=5"] : []),
          "--reporter=line,json",
          `--output=${join(out, name)}`,
        ],
        {
          MINNOW_PERF_LANE: "1",
          MINNOW_OWNER_SCHEDULING_BENCH: "1",
          PLAYWRIGHT_JSON_OUTPUT_FILE: join(out, name + ".json"),
          ...(browser === "webkit" && process.env.MINNOW_R6_WEBKIT_BROWSERS_PATH
            ? { PLAYWRIGHT_BROWSERS_PATH: process.env.MINNOW_R6_WEBKIT_BROWSERS_PATH }
            : {}),
        },
      );
      const metrics = result.stdout
        .split("\n")
        .filter((line) => line.startsWith('{"browser"') || line.startsWith('{"schemaVersion"'))
        .map((line) => JSON.parse(line) as unknown);
      if (metrics.length !== (lane === "latency" ? 5 : 1))
        throw new Error(`${name}: incomplete observations`);
      if (lane === "scheduling" && result.code !== 0)
        throw new Error(`${name}: workload validation failed`);
      // These are protocol/fixture invariants, not numerical latency budgets. In particular,
      // the lock-call assertion also uses toBeLessThanOrEqual in the browser test.
      if (lane === "latency") {
        for (const metric of metrics) {
          const run = metric as {
            browser: string;
            timers: { isolated: boolean };
            lockCalls: number;
          };
          if (
            run.browser !== browser ||
            run.timers.isolated !== true ||
            !Number.isFinite(run.lockCalls) ||
            run.lockCalls < 0 ||
            run.lockCalls > 8
          )
            throw new Error(`${name}: protocol/fixture invariant failed`);
        }
      }
      if (lane === "latency" && result.code !== 0) {
        const report = JSON.parse(readFileSync(join(out, name + ".json"), "utf8")) as {
          errors: unknown[];
          suites: unknown[];
        };
        const errors: string[] = [];
        const visit = (value: unknown): void => {
          if (Array.isArray(value)) {
            for (const v of value) visit(v);
          } else if (value !== null && typeof value === "object") {
            for (const [key, v] of Object.entries(value)) {
              if (key === "errors" && Array.isArray(v))
                for (const e of v) errors.push(String((e as { message?: string }).message));
              else visit(v);
            }
          }
        };
        visit(report);
        if (errors.length === 0 || errors.some((e) => !e.includes("toBeLessThanOrEqual")))
          throw new Error(`${name}: non-budget failure`);
      }
      passed &&= result.code === 0;
      records.push({ browser, lane, exit: result.code, metrics });
    }
  }
  sample();
  host.assertQuiet();
  save("host.json", host.samples);
  const median = (values: number[]) => {
    if (values.length === 0 || values.some((x) => !Number.isFinite(x)))
      throw new Error("Invalid metrics");
    return [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
  };
  const summary = records.map((record) => {
    if (record.lane === "latency") {
      const runs = record.metrics as Array<{
        uncontended: { p50: number };
        loaded: { p95: number };
      }>;
      return {
        browser: record.browser,
        lane: record.lane,
        warmP50Us: median(runs.map((x) => x.uncontended.p50)),
        loadedP95Us: median(runs.map((x) => x.loaded.p95)),
      };
    }
    const runs = (
      record.metrics[0] as {
        records: Array<{
          variant: { name: string };
          work: { units: number; wallMs: number };
          p95: number;
        }>;
      }
    ).records;
    return {
      browser: record.browser,
      lane: record.lane,
      quanta: [...new Set(runs.map((x) => x.variant.name))].map((name) => {
        const a = runs.filter((x) => x.variant.name === name);
        return {
          name,
          echoP95Us: median(a.map((x) => x.p95)),
          completionMs: median(a.map((x) => x.work.wallMs)),
          workUnitsPerSecond: median(a.map((x) => (1000 * x.work.units) / x.work.wallMs)),
        };
      }),
    };
  });
  save("report.json", {
    schemaVersion: 1,
    revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
    node: process.version,
    cpus: availableParallelism(),
    status: passed ? "pass" : "targets-missed",
    quantum: "candidate only; decide from quiet latency and fixed-work completion times",
    records,
    summary,
  });
  console.log(`Quiet-host report: ${accepted}`);
  process.exitCode = passed ? 0 : 1;
} catch (error) {
  save("host.json", host.samples);
  save("refused.json", { reason: String(error) });
  console.error(`${String(error)}. Raw evidence: ${out}; no report emitted.`);
  process.exitCode = 2;
}
