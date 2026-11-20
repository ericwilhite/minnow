/** A dedicated runner owns the original wall-clock guarantees; no parallel unit work. */
import { defineConfig } from "vitest/config";
import base from "./vitest.config.js";
export default defineConfig({
  ...base,
  test: {
    ...base.test,
    maxWorkers: 1,
    fileParallelism: false,
    projects: [
      {
        extends: true,
        test: {
          name: "stall-latency",
          include: ["packages/core/src/engine/event-loop-latency.test.ts"],
          maxWorkers: 1,
          fileParallelism: false,
        },
      },
    ],
  },
});
