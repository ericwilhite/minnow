import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  // The site's own modules import each other through the `@/` alias its tsconfig declares, so a
  // test of one of them resolves the same way the browser build does.
  resolve: {
    alias: { "@/": `${fileURLToPath(new URL("./apps/site", import.meta.url))}/` },
  },
  test: {
    include: [
      "packages/**/*.test.ts",
      "scripts/**/*.test.ts",
      "apps/site/{lib,bench,components}/**/*.test.ts",
    ],
    /**
     * The heavy tests here drive whole databases over a simulated IndexedDB, and the slowest
     * takes about nine seconds on a developer machine. A hosted runner has two cores, so several
     * test files share them and every one of those tests stretches -- the same work has measured
     * five to seven times slower there. A limit tuned on the machine that wrote the test is
     * therefore a flake generator on CI, which is what these numbers answer: enough room that
     * only a genuine hang reaches the ceiling, while a local run still fails fast.
     *
     * This is not the performance guard. A test that gets slower still passes here; the
     * benchmark gate is what reports that, and it runs on its own schedule for the same reason
     * these numbers have to be generous.
     */
    testTimeout: process.env.CI ? 90_000 : 20_000,
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
       * The per-package floors matter more than the global one: devtools is a DOM layer proved
       * by the site browser runner rather than by Vitest, and averaging it in would let the
       * engine's coverage fall a long way while the total still looked healthy.
       */
      thresholds: {
        statements: 82,
        branches: 75,
        functions: 84,
        lines: 84,
        "packages/core/src/**": { statements: 87, branches: 78, functions: 91, lines: 89 },
        "packages/client/src/**": { statements: 89, branches: 81, functions: 94, lines: 91 },
      },
    },
  },
});
