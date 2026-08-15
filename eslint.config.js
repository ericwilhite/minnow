import eslint from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["**/dist/**", "**/coverage/**", "**/playwright-report/**", "**/.astro/**"] },
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
    files: ["*.js", "*.ts", "**/*.mjs", "scripts/**/*.mts"],
    extends: [tseslint.configs.disableTypeChecked],
    languageOptions: { globals: { ...globals.node } },
  },
  {
    // Bench engines compare against SQLite/PGlite on purpose, the conformance harness uses
    // node:sqlite as its differential oracle, and the site test server is a Node process
    // that never ships.
    files: [
      "apps/bench/src/engines/**/*.ts",
      "apps/bench/vite.config.ts",
      "apps/bench/capture/*.mjs",
      "apps/site/tests/serve-dist.mjs",
      "packages/core/src/engine/sql-conformance.test.ts",
      "packages/core/src/engine/sql-mutation-conformance.test.ts",
      "scripts/perf-gate.mts",
    ],
    rules: { "no-restricted-imports": "off" },
  },
);
