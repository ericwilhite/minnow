/**
 * Engine-comparison capture harness. Runs the bench worker's dataset build and reference
 * suite in real browsers with disk-backed persistent profiles, and writes one result
 * bundle per browser project.
 *
 *   npm run capture                                  # all engines, scale 100, both browsers
 *   CAPTURE_ENGINES=minnow npm run capture           # fast iteration: Minnow only (~1 min)
 *   CAPTURE_SCALE=10 npm run capture -- --project chromium
 *   CAPTURE_OUT=apps/site/src/data/benchmarks npm run capture   # publishing run
 *
 * Bundles default into .captures/ at the repo root (gitignored). Point CAPTURE_OUT at
 * apps/site/src/data/benchmarks only for a publishing run — the docs site is the single
 * home for published results.
 */
import { defineConfig } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export default defineConfig({
  testDir: ".",
  timeout: 5_400_000,
  workers: 1,
  // First cold-cache serve of a large worker dependency can trigger one dev-server
  // reload mid-run; the retry then runs against the warmed optimizer.
  retries: 1,
  reporter: [["line"]],
  webServer: {
    command: "npm run dev --workspace @minnowdb/bench -- --host 127.0.0.1 --port 4176 --strictPort",
    cwd: repoRoot,
    url: "http://127.0.0.1:4176",
    reuseExistingServer: true,
    timeout: 120_000,
  },
  projects: [{ name: "chromium" }, { name: "firefox" }],
});
