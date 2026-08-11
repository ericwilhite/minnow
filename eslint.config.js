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
      "@typescript-eslint/no-confusing-void-expression": "off",
      "@typescript-eslint/require-await": "off",
    },
  },
  {
    files: ["*.js", "*.ts", "**/*.mjs"],
    extends: [tseslint.configs.disableTypeChecked],
    languageOptions: { globals: { ...globals.node } },
  },
  {
    files: ["apps/bench/src/engines/**/*.ts", "apps/bench/vite.config.ts"],
    rules: { "no-restricted-imports": "off" },
  },
);
