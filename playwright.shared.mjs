/** Shared defaults for every Playwright runner in this repository. */

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
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? [["github"], ["line"]] : [["line"]],
};

export function localUrl(port) {
  return `http://127.0.0.1:${String(port)}`;
}
