import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["packages/**/*.test.ts", "apps/site/{lib,bench,components}/**/*.test.ts"],
    coverage: { reporter: ["text", "html"] },
  },
});
