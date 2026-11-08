/** One quiet-host acceptance command. Preparation is cached; busy runs never publish metrics. */
import { spawn } from "node:child_process";
import { createWriteStream, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { loadavg, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { finished } from "node:stream/promises";
import { prepareFixture } from "./browser-perf/gate-a-fixture.mjs";
import { assessGateA, QuietLoad } from "./browser-perf/gate-a-policy.mjs";
const seed = Number(process.argv.find((a) => a.startsWith("--seed="))?.slice(7) ?? 0xb2a00001);
const load = new QuietLoad();
let fixtureHash: string | null = null;
try {
  if (process.platform !== "linux")
    throw new Error("This command requires Linux /proc, fincore and dd cache control");
  load.observe(loadavg()[0] ?? NaN, "startup");
  if (!process.argv.includes("--prepare-only")) load.assert();
  const fixture = await prepareFixture(seed);
  fixtureHash = fixture.manifest.hash;
  if (process.argv.includes("--prepare-only")) {
    console.log(
      JSON.stringify({
        status: "prepared",
        seed,
        fixtureHash: fixture.manifest.hash,
        directory: fixture.dir,
        reused: fixture.reused,
      }),
    );
  } else {
    load.observe(loadavg()[0] ?? NaN, "after-fixture-preparation");
    load.assert();
    const out = mkdtempSync(join(tmpdir(), "minnow-gate-a-run-")),
      logPath = join(out, "runner.log"),
      log = createWriteStream(logPath, { flags: "wx" });
    const inherited = Object.fromEntries(
      Object.entries(process.env).filter(
        ([key]) => !key.startsWith("MINNOW_EXPORT_") && key !== "MINNOW_NEXT_EXPORT_BENCH",
      ),
    );
    const env = {
      ...inherited,
      MINNOW_NEXT_EXPORT_BENCH: "1",
      MINNOW_EXPORT_FOCUS: "1",
      MINNOW_EXPORT_OWNER_API: "1",
      MINNOW_EXPORT_GATE_A: "1",
      MINNOW_EXPORT_ROWS: "1048576",
      MINNOW_EXPORT_DISTINCT: "17",
      MINNOW_EXPORT_FIXTURE_DIR: fixture.dir,
      MINNOW_EXPORT_FIXTURE_SEED: String(seed),
      MINNOW_EXPORT_FIXTURE_HASH: fixture.manifest.hash,
    };
    const child = spawn(
      process.execPath,
      [
        resolve("node_modules/@playwright/test/cli.js"),
        "test",
        "--config",
        "playwright.library.config.ts",
        "packages/core/browser-tests/next-export-bench.spec.ts",
        "--project=chromium",
        "--workers=1",
        "--retries=0",
        "--reporter=line",
        "--output=" + join(out, "browser"),
      ],
      { env, stdio: ["ignore", "pipe", "pipe"] },
    );
    child.stdout.pipe(log, { end: false });
    child.stderr.pipe(log, { end: false });
    const timer = setInterval(() => load.observe(loadavg()[0] ?? NaN, "runner"), 1000);
    let code;
    try {
      code = await new Promise<number | null>((yes, no) => {
        child.once("error", no);
        child.once("close", yes);
      });
    } finally {
      clearInterval(timer);
      log.end();
      await finished(log);
    }
    load.observe(loadavg()[0] ?? NaN, "complete");
    writeFileSync(join(out, "host-load.json"), JSON.stringify(load.samples, null, 2));
    load.assert();
    if (code !== 0) throw new Error(`Export runner failed; diagnostics: ${logPath}`);
    const line = readFileSync(logPath, "utf8")
      .split("\n")
      .find((l) => l.includes("EXPORT_BENCH "));
    if (!line) throw new Error(`Missing export report: ${logPath}`);
    const report = JSON.parse(line.slice(line.indexOf("EXPORT_BENCH ") + 13)) as Parameters<
      typeof assessGateA
    >[0];
    const summary = assessGateA(report, seed, fixture.manifest.hash, load);
    const accepted = summary.allRatesMeetTarget && summary.allRssWithinBound;
    writeFileSync(
      join(out, "report.json"),
      JSON.stringify({ status: accepted ? "pass" : "target-not-met", summary, report }, null, 2),
    );
    console.log(
      JSON.stringify({
        status: accepted ? "pass" : "target-not-met",
        ...summary,
        reportPath: join(out, "report.json"),
      }),
    );
    if (!accepted) process.exitCode = 1;
  }
} catch (error) {
  console.error(
    JSON.stringify({
      status: "refused",
      seed,
      fixtureHash,
      error: String(error),
      hostLoad: load.samples,
    }),
  );
  process.exitCode = 2;
}
