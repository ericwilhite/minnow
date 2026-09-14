/** Shared defaults for every Playwright runner in this repository. */
import { fileURLToPath } from "node:url";
import { availableParallelism } from "node:os";

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
