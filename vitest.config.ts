import { fileURLToPath } from "node:url";
import { availableParallelism } from "node:os";
import { configDefaults, defineConfig } from "vitest/config";

const maintenanceWorkload = "packages/core/src/engine/maintenance-under-load.test.ts";

export default defineConfig({
  // The site's own modules import each other through the `@/` alias its tsconfig declares, so a
  // test of one of them resolves the same way the browser build does.
  resolve: {
    alias: { "@/": `${fileURLToPath(new URL("./apps/site", import.meta.url))}/` },
  },
  test: {
    // Each file may start whole engines and durable-store simulations. Unbounded file-level
    // parallelism turns CPU/heap contention into unrelated timeouts on large developer hosts.
    // Keep the existing deadlines and bound simultaneous heavy suites instead.
    maxWorkers: Math.min(4, availableParallelism()),
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: [
            "packages/**/*.test.ts",
            "scripts/**/*.test.ts",
            "apps/site/{lib,bench,components}/**/*.test.ts",
          ],
          exclude: [...configDefaults.exclude, maintenanceWorkload],
          // Nonzero groups also preserve this order with Vitest's single-worker shortcut.
          sequence: { groupOrder: 1 },
        },
      },
      {
        extends: true,
        test: {
          name: "maintenance",
          include: [maintenanceWorkload],
          // The 20,000-row update loop drives its own writer/maintenance concurrency. Running
          // unrelated database simulators beside it under coverage exhausted its existing CI
          // deadline. Run the unchanged workload alone, then merge coverage from both projects.
          fileParallelism: false,
          sequence: { groupOrder: 2 },
        },
      },
    ],
    /**
     * Heavy tests drive whole databases and simulated durable stores. Hosted CPU contention and
     * V8 coverage make the same work substantially slower than an uninstrumented local run. Keep
     * these existing deadlines while isolating the maintenance workload from unrelated files;
     * a stalled engine still fails, and growing the regression corpus cannot starve that test.
     *
     * This is not the performance guard. A test that gets slower still passes here; the
     * benchmark gate is what reports that, and it runs on its own schedule for the same reason
     * these numbers have to be generous.
     */
    testTimeout: process.env.CI ? 300_000 : 20_000,
    hookTimeout: process.env.CI ? 90_000 : 20_000,
    coverage: {
      reporter: ["text", "html", "json-summary"],
      // A failed run is exactly when the report is worth reading, and CI uploads it either way.
      reportOnFailure: true,
      // Only the shipped library. The site is covered by its own browser runner, and measuring
      // it here would average a docs app into the engine's numbers.
      include: ["packages/*/src/**/*.ts"],
      exclude: ["**/*.test.ts", "**/*.spec.ts"],
      /**
       * A ratchet, not a target. Each floor sits just under what the suite measured when it was
       * written, so the gate fails when coverage falls rather than when it merely stops rising.
       * Raise a floor after a run comes in comfortably above it; never lower one to make a red
       * gate green -- that is the failure the floor exists to report.
       *
       * The per-package floors matter more than the global one: averaging a thinner package in
       * would let the engine's coverage fall a long way while the total still looked healthy.
       * The devtools' DOM modules run under happy-dom (each test file opts in with a
       * `@vitest-environment` comment); the site browser runner covers what a real browser adds.
       */
      thresholds: {
        statements: 82,
        branches: 75,
        functions: 84,
        lines: 84,
        "packages/core/src/**": { statements: 87, branches: 78, functions: 91, lines: 89 },
        "packages/devtools/src/**": { statements: 50, branches: 48, functions: 47, lines: 51 },
        "packages/export/src/**": { statements: 91, branches: 81, functions: 99, lines: 97 },
        "packages/kysely/src/**": { statements: 93, branches: 84, functions: 99, lines: 95 },
        "packages/react/src/**": { statements: 70, branches: 49, functions: 59, lines: 79 },
      },
    },
  },
});
