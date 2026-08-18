import { defineConfig } from "@playwright/test";
import { browserProjects, localUrl, runnerDefaults } from "./playwright.shared.mjs";

const port = 4185;

export default defineConfig({
  ...runnerDefaults,
  testDir: "./apps/site/tests",
  workers: 1,
  // Generating a dataset and running a benchmark suite in a real browser takes minutes, not
  // the default thirty seconds.
  timeout: 300_000,
  webServer: {
    // Build, then serve the static export with a plain foreground server that also applies the
    // cross-origin isolation headers Vercel serves from vercel.json, so the
    // benchmarks page is tested on the code path it ships on.
    command: `npm run build --workspace @minnowdb/site && node apps/site/tests/serve-dist.mjs ${String(port)}`,
    url: localUrl(port),
    reuseExistingServer: false,
    timeout: 300_000,
  },
  use: { baseURL: localUrl(port) },
  projects: browserProjects,
});
