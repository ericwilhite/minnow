import { defineConfig } from "@playwright/test";
import { browserProjects, localUrl, runnerDefaults } from "./playwright.shared.mjs";

const port = 4180;

export default defineConfig({
  ...runnerDefaults,
  testDir: "./packages/core/browser-tests",
  webServer: {
    command: `npx vite --host 127.0.0.1 --port ${String(port)} --strictPort`,
    url: `${localUrl(port)}/packages/core/browser/`,
    reuseExistingServer: false,
  },
  use: { baseURL: localUrl(port) },
  // All three, including WebKit: this runner is the only place real IndexedDB is exercised, and
  // Safari's is the implementation most likely to differ. It was excluded until the harness
  // stopped treating a transient `blocked` on deleteDatabase as a failure -- see run.ts.
  projects: browserProjects,
});
