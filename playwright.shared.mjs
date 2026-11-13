/** Shared defaults for every Playwright runner in this repository. */
import { readdirSync, readFileSync } from "node:fs";
import { isAbsolute, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { availableParallelism } from "node:os";
import { assignParts, defaultWeight, parsePart } from "./scripts/test-parts.mjs";

/** Resolved absolutely, so both root configs load the same file whatever directory runs them. */
const flakyReporter = fileURLToPath(new URL("./playwright-flaky-reporter.mjs", import.meta.url));

/** @type {{ name: string; use: { browserName: "chromium" | "firefox" | "webkit" } }[]} */
export const browserProjects = [
  { name: "chromium", use: { browserName: "chromium" } },
  { name: "firefox", use: { browserName: "firefox" } },
  { name: "webkit", use: { browserName: "webkit" } },
];

/**
 * @type {{
 *   forbidOnly: boolean;
 *   preserveOutput: "failures-only";
 *   retries: number;
 *   workers: number;
 *   reporter: import("@playwright/test").ReporterDescription[];
 * }}
 */
export const runnerDefaults = {
  // Scenarios create their own concurrent tabs/workers. Bound independent scenarios so several
  // persistent storage stress tests cannot turn host-wide contention into unrelated timeouts.
  workers: Math.max(1, Math.min(2, Math.floor(availableParallelism() / 2))),
  forbidOnly: Boolean(process.env.CI),
  // Profiles and other per-test files are useful only when that attempt fails. Playwright gives
  // retries distinct output directories, so a failed first attempt remains available even when
  // its retry passes, while successful persistent browser profiles do not fill CI artifacts.
  preserveOutput: "failures-only",
  // One retry absorbs a single noisy sample; the flaky reporter then fails the run anyway, so a
  // test that needed the retry is reported rather than hidden behind a green result.
  retries: process.env.CI ? 1 : 0,
  // Persist seed plans and result attachments as well as failures; console reporters discard bodies.
  reporter: process.env.CI
    ? [
        ["github"],
        ["line"],
        ["html", { outputFolder: "playwright-report", open: "never" }],
        [flakyReporter],
      ]
    : [["line"], ["html", { outputFolder: "playwright-report", open: "never" }], [flakyReporter]],
};

export function localUrl(port) {
  return `http://127.0.0.1:${String(port)}`;
}

/** Playwright's default `testMatch`, as a pattern over a file name. */
const specFile = /\.(?:spec|test)\.[cm]?[jt]sx?$/u;

/** @param {string} dir @returns {string[]} */
function listFiles(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) =>
    entry.isDirectory() ? listFiles(join(dir, entry.name)) : [join(dir, entry.name)],
  );
}

/**
 * The spec files one CI part runs, when `MINNOW_BROWSER_PART=index/count` is set; otherwise
 * undefined (Playwright's default: every spec).
 *
 * Playwright's `--shard` cuts the file list into equal counts in file order, and three specs hold
 * about half of each engine's time, so one WebKit part ran 18 minutes beside another's 11. Here
 * the parts are chosen by the measured per-browser durations in scripts/browser-durations.json
 * (`MINNOW_BROWSER` names the browser; `MINNOW_BROWSER_RESERVE` is other work the first part also
 * runs, in the same units). Every spec the config would run goes to exactly one part,
 * whatever the table says: it only steers the split (scripts/test-parts.mjs).
 *
 * @param {string} testDir the config's testDir
 * @param {RegExp} testIgnore the config's testIgnore
 * @returns {RegExp[] | undefined}
 */
export function partTestMatch(testDir, testIgnore) {
  const part = parsePart(process.env.MINNOW_BROWSER_PART);
  if (part === null) return undefined;
  /** @type {Record<string, Record<string, number>>} */
  const table = JSON.parse(
    readFileSync(new URL("./scripts/browser-durations.json", import.meta.url), "utf8"),
  );
  const browser = process.env.MINNOW_BROWSER ?? "";
  const weights = table[browser];
  if (weights === undefined)
    throw new Error(`MINNOW_BROWSER must name a browser in browser-durations.json, got ${browser}`);
  const root = fileURLToPath(new URL(".", import.meta.url));
  const specs = listFiles(isAbsolute(testDir) ? testDir : join(root, testDir))
    .filter((file) => specFile.test(file) && !testIgnore.test(file))
    .map((file) => relative(root, file).split("\\").join("/"))
    .sort();
  const fallback = defaultWeight(weights);
  // Work the first part also runs besides the library (another runner's suites), so it takes
  // correspondingly less of the library.
  const reserve = Number(process.env.MINNOW_BROWSER_RESERVE ?? "") || 0;
  const parts = assignParts(
    specs,
    specs.map((spec) => weights[spec] ?? fallback),
    part.count,
    [reserve],
  );
  return specs
    .filter((_, i) => parts[i] === part.index - 1)
    .map(
      (spec) => new RegExp(`(?:^|[\\\\/])${spec.replace(/[.*+?^${}()|[\]\\/]/gu, "\\$&")}$`, "u"),
    );
}

/**
 * In a CI part, a JSON report of its per-spec durations in `durations/`, which CI uploads for
 * `npm run test:durations` to refresh the part weights from. Nothing otherwise.
 *
 * @returns {import("@playwright/test").ReporterDescription[]}
 */
export function partReporters() {
  const part = parsePart(process.env.MINNOW_BROWSER_PART);
  if (part === null || !process.env.CI) return [];
  const browser = process.env.MINNOW_BROWSER ?? "browser";
  return [["json", { outputFile: `durations/playwright-${browser}-${String(part.index)}.json` }]];
}
