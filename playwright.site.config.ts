import { defineConfig } from "@playwright/test";
import { browserProjects, localUrl, runnerDefaults } from "./playwright.shared.mjs";

const port = 4185;

export default defineConfig({
  ...runnerDefaults,
  testDir: "./apps/site/tests",
  workers: 1,
  webServer: {
    // Build, then serve dist with a plain foreground server. `astro dev` refuses to start
    // when another dev daemon is running, and `astro preview` daemonizes itself when stdout
    // is not a TTY — both break Playwright's webServer supervision.
    command: `npm run build --workspace @minnowdb/site && node apps/site/tests/serve-dist.mjs ${String(port)}`,
    url: localUrl(port),
    reuseExistingServer: false,
    timeout: 180_000,
  },
  use: { baseURL: localUrl(port) },
  projects: browserProjects,
});
