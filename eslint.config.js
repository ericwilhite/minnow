import eslint from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    ignores: [
      "**/dist/**",
      "**/coverage/**",
      "**/playwright-report/**",
      "**/test-results/**",
      "**/.next/**",
      "**/out/**",
      "**/.source/**",
      // Agent worktrees the Claude Code harness checks out inside the repository.
      ".claude/worktrees/**",
      // Third-party engine builds, copied in verbatim by scripts/vendor-engines.mjs.
      "apps/site/public/vendor/**",
    ],
  },
  eslint.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  ...tseslint.configs.stylisticTypeChecked,
  {
    languageOptions: {
      globals: { ...globals.browser, ...globals.worker },
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
    },
    rules: {
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: ["node:*", "*sqlite*", "*duckdb*"],
              message:
                "Shipped MinnowDatabase packages must remain browser-only and engine-independent.",
            },
          ],
        },
      ],
      "@typescript-eslint/array-type": ["error", { default: "array-simple" }],
      "@typescript-eslint/consistent-type-definitions": ["error", "interface"],
      // `interface DB extends InferDatabase<typeof appSchema> {}` is the documented standard:
      // the empty extending interface names the database type so hovers and declaration emit
      // print `Minnow<DB>` instead of the expanded schema.
      "@typescript-eslint/no-empty-object-type": [
        "error",
        { allowInterfaces: "with-single-extends" },
      ],
      "@typescript-eslint/no-confusing-void-expression": "off",
      "@typescript-eslint/require-await": "off",
    },
  },
  {
    files: ["*.js", "*.ts", "**/*.mjs", "scripts/**/*.{ts,mts}"],
    extends: [tseslint.configs.disableTypeChecked],
    languageOptions: { globals: { ...globals.node } },
  },
  {
    // Bench engines compare against SQLite/PGlite on purpose, the conformance harness uses
    // node:sqlite as its differential oracle, the site test server and the vendoring script are
    // Node processes that never ship, the measurement scripts are Node tooling by definition,
    // and the docs test reads the .mdx pages off disk to run the SQL printed in them.
    files: [
      "apps/site/bench/engines/**/*.ts",
      "apps/site/lib/dataset/docs-sql.test.ts",
      // Reads the generated declaration bundle off disk and runs the compiler over it, which is
      // how the console's type checking is proved without a browser.
      "apps/site/components/playground/snippets.test.ts",
      "vitest.config.ts",
      "apps/site/tests/serve-dist.mjs",
      "apps/site/scripts/*.mjs",
      "packages/core/src/engine/sql-conformance.test.ts",
      "packages/core/src/engine/sql-mutation-conformance.test.ts",
      // Neither executor runs a correlated subquery without the rewrite, so the rewrite's
      // differential tests need node:sqlite as the oracle for the original statement.
      "packages/core/src/engine/decorrelate.test.ts",
      "packages/core/src/engine/delta-scan.test.ts",
      "packages/core/src/engine/scan-kernels.test.ts",
      // Reads the test sources and the soak runner off disk to prove every recorded regression
      // seed is iterated by exactly one suite the soak explores.
      "packages/core/src/testing/seeds.test.ts",
      // The Playwright runner's shared config and its flaky-test reporter are Node processes.
      "playwright*.mjs",
      // Reads the checked-in format fixtures off disk; they are the databases earlier builds
      // wrote, and there is nowhere else to keep them.
      "packages/core/src/storage/format-compatibility.test.ts",
      // Reads the package's own sources off disk to assert the layering rules between the
      // storage contract, the adapter toolkit, and the engine.
      "packages/core/src/storage/contract-boundaries.test.ts",
      // Bundles the built package with esbuild to prove the storage adapters tree-shake.
      "packages/core/src/tree-shaking.test.ts",
      // Bundles the built Kysely adapter and applies deterministic gzip size ratchets.
      "packages/kysely/src/tree-shaking.test.ts",
      "scripts/**/*.{ts,mts,mjs}",
    ],
    rules: { "no-restricted-imports": "off" },
  },
);
