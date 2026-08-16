import { defineConfig } from "@playwright/test";
import { browserProjects, localUrl, runnerDefaults } from "./playwright.shared.mjs";

const port = 4175;

export default defineConfig({
  ...runnerDefaults,
  testDir: "./apps/bench/tests",
  workers: 1,
  webServer: {
    command: `npm run dev --workspace @minnowdb/bench -- --host 127.0.0.1 --port ${String(port)} --strictPort`,
    url: localUrl(port),
    reuseExistingServer: false,
  },
  use: { baseURL: localUrl(port) },
  projects: browserProjects,
});
