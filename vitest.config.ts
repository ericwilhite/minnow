import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["packages/**/*.test.ts", "apps/site/{lib,bench,components}/**/*.test.ts"],
    coverage: {
      reporter: ["text", "html", "json-summary"],
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
