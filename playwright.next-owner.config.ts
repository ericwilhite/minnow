import { defineConfig } from "@playwright/test";
import { browserProjects, localUrl, runnerDefaults } from "./playwright.shared.mjs";

/**
 * The next core's owner lane (B5): cross-tab election, owner death and resends, lifecycle, skew
 * (next-owner.spec.ts) and the cross-tab point-read latency lane (next-owner-latency.spec.ts).
 * Its own port, so it runs beside the library runner.
 */
const port = 4186;

export default defineConfig({
  ...runnerDefaults,
  // One browser at a time: the latency lane must not measure the other engines' load.
  workers: 1,
  testDir: "./packages/core/browser-tests",
  testMatch: /next-owner.*\.spec\.ts$/u,
  webServer: {
    command: `npx vite --config vite.next-owner.config.ts --host 127.0.0.1 --port ${String(port)} --strictPort`,
    url: `${localUrl(port)}/packages/core/browser/`,
    reuseExistingServer: false,
  },
  use: {
    baseURL: localUrl(port),
    trace: { mode: "retain-on-failure", screenshots: false, snapshots: false, sources: true },
  },
  projects: browserProjects,
});
