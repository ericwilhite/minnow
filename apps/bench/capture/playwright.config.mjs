/**
 * Engine-comparison capture harness. Runs the bench worker's dataset build and reference
 * suite in real browsers with disk-backed persistent profiles, and writes one result
 * bundle per browser project.
 *
 * Bundles default into .captures/ at the repo root (gitignored). Point CAPTURE_OUT at
 * apps/site/src/data/benchmarks only for a publishing run — the docs site is the single
 * home for published results.
 */
import { defineConfig } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { captureProjects, localUrl, runnerDefaults } from "../../../playwright.shared.mjs";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export default defineConfig({
  ...runnerDefaults,
  testDir: ".",
  timeout: 5_400_000,
  workers: 1,
  // First cold-cache serve of a large worker dependency can trigger one dev-server
  // reload mid-run; the retry then runs against the warmed optimizer.
  retries: 1,
  webServer: {
    command: "npm run dev --workspace @minnowdb/bench -- --host 127.0.0.1 --port 4176 --strictPort",
    cwd: repoRoot,
    url: localUrl(4176),
    reuseExistingServer: true,
    timeout: 120_000,
  },
  projects: captureProjects,
});
