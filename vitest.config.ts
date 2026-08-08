import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["packages/**/*.test.ts", "apps/bench/src/**/*.test.ts"],
    coverage: { reporter: ["text", "html"] },
  },
});
