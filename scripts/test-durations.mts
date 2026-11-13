/**
 * Refreshes the measured durations that balance the CI test parts (scripts/test-parts.mjs).
 *
 *   gh run download <run id> --pattern 'durations-*' --dir .cache/durations
 *   npm run test:durations -- .cache/durations
 *
 * CI uploads one JSON report per part: `vitest-coverage-*.json` from the coverage parts,
 * `vitest-plain-*.json` from the oldest-Node parts, and `playwright-<browser>-*.json` from each
 * real-browser part. A file's time varies by half from run to run with what shares its runner,
 * so a measured file's new weight is the mean of its committed weight and this measurement, and the
 * table settles over refreshes rather than chasing one run. A file measured for the first time
 * takes its measurement; a file this refresh did not measure keeps its weight while it exists and
 * leaves the table once it is deleted. The weights only steer the split, never which files run, so
 * a stale table costs time, not coverage.
 */
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const vitestTable = join(root, "scripts/test-durations.json");
const browserTable = join(root, "scripts/browser-durations.json");

type Profile = Record<string, number>;

interface VitestReport {
  testResults: Array<{ name: string; startTime: number; endTime: number }>;
}

interface PlaywrightSuite {
  file?: string;
  suites?: PlaywrightSuite[];
  specs?: Array<{
    file: string;
    tests: Array<{ projectName: string; results: Array<{ duration: number; retry: number }> }>;
  }>;
}

interface PlaywrightReport {
  config: { rootDir: string };
  suites: PlaywrightSuite[];
}

function files(path: string): string[] {
  if (!statSync(path).isDirectory()) return [path];
  return readdirSync(path).flatMap((name) => files(join(path, name)));
}

/** Repository-relative with forward slashes, whatever machine wrote the report. */
function repositoryPath(path: string): string {
  const posix = path.split("\\").join("/");
  // CI checkouts live at /home/runner/work/minnow/minnow/; local ones anywhere.
  for (const marker of ["/packages/", "/scripts/", "/apps/"]) {
    const at = posix.indexOf(marker);
    if (at >= 0) return posix.slice(at + 1);
  }
  return relative(root, path).split("\\").join("/");
}

/** The committed profile updated with a refresh's measurements (see the header). */
function settle(committed: Profile | undefined, measured: Profile): Profile {
  const next: Profile = {};
  for (const [file, weight] of Object.entries(committed ?? {}))
    if (!(file in measured) && existsSync(join(root, file))) next[file] = weight;
  for (const [file, weight] of Object.entries(measured)) {
    const before = committed?.[file];
    next[file] = before === undefined ? weight : Math.round((before + weight) / 2);
  }
  return sorted(next);
}

function sorted(profile: Profile): Profile {
  return Object.fromEntries(Object.entries(profile).sort(([a], [b]) => (a < b ? -1 : 1)));
}

function addPlaywright(suite: PlaywrightSuite, dir: string, into: Map<string, Profile>): void {
  for (const spec of suite.specs ?? [])
    for (const test of spec.tests) {
      // The first attempt: a retry is a flaky sample the reporter already fails on.
      const first = test.results.find((result) => result.retry === 0);
      if (first === undefined) continue;
      const profile = into.get(test.projectName) ?? {};
      const key = repositoryPath(join(dir, spec.file));
      profile[key] = (profile[key] ?? 0) + Math.round(first.duration);
      into.set(test.projectName, profile);
    }
  for (const child of suite.suites ?? []) addPlaywright(child, dir, into);
}

const inputs = process.argv
  .slice(2)
  .flatMap(files)
  .filter((path) => path.endsWith(".json"));
if (inputs.length === 0) {
  console.error("usage: npm run test:durations -- <report files or directories>");
  process.exit(2);
}

const vitest = new Map<string, Profile>();
const browsers = new Map<string, Profile>();
for (const input of inputs) {
  const name = input.split(/[\\/]/u).at(-1) ?? "";
  const report = JSON.parse(readFileSync(input, "utf8")) as unknown;
  const profile = /^vitest-(coverage|plain)-/u.exec(name)?.[1];
  if (profile !== undefined) {
    const into = vitest.get(profile) ?? {};
    for (const file of (report as VitestReport).testResults)
      into[repositoryPath(file.name)] = Math.max(0, Math.round(file.endTime - file.startTime));
    vitest.set(profile, into);
  } else if (name.startsWith("playwright-")) {
    const { config, suites } = report as PlaywrightReport;
    for (const suite of suites) addPlaywright(suite, config.rootDir, browsers);
  } else console.warn(`skipping ${input}: not a vitest-* or playwright-* report`);
}

if (vitest.size > 0) {
  const table = JSON.parse(readFileSync(vitestTable, "utf8")) as Record<string, Profile>;
  for (const [profile, measured] of vitest) table[profile] = settle(table[profile], measured);
  writeFileSync(vitestTable, `${JSON.stringify(table, null, 2)}\n`);
  for (const [profile, measured] of vitest)
    console.log(`vitest ${profile}: ${String(Object.keys(measured).length)} files`);
}
if (browsers.size > 0) {
  const table = JSON.parse(readFileSync(browserTable, "utf8")) as Record<string, Profile>;
  for (const [browser, measured] of browsers) table[browser] = settle(table[browser], measured);
  writeFileSync(browserTable, `${JSON.stringify(table, null, 2)}\n`);
  for (const [browser, measured] of browsers)
    console.log(`playwright ${browser}: ${String(Object.keys(measured).length)} files`);
}
