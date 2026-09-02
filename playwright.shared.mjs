/** Shared defaults for every Playwright runner in this repository. */
import { fileURLToPath } from "node:url";

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
 *   retries: number;
 *   reporter: import("@playwright/test").ReporterDescription[];
 * }}
 */
export const runnerDefaults = {
  forbidOnly: Boolean(process.env.CI),
  // One retry absorbs a single noisy sample; the flaky reporter then fails the run anyway, so a
  // test that needed the retry is reported rather than hidden behind a green result.
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [["github"], ["line"], [flakyReporter]] : [["line"], [flakyReporter]],
};

export function localUrl(port) {
  return `http://127.0.0.1:${String(port)}`;
}
