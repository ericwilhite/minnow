import eslint from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["**/dist/**", "**/coverage/**", "**/playwright-report/**"] },
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
                "Shipped BrowserDatabase packages must remain browser-only and engine-independent.",
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
    files: ["*.js", "*.ts"],
    extends: [tseslint.configs.disableTypeChecked],
  },
  {
    files: ["apps/bench/src/engine-adapters/**/*.ts"],
    rules: { "no-restricted-imports": "off" },
  },
);
