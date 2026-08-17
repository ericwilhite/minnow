import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["packages/**/*.test.ts", "apps/site/{lib,bench}/**/*.test.ts"],
    coverage: { reporter: ["text", "html"] },
  },
});
