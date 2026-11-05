import { writeFileSync } from "node:fs";
import { currentEngine } from "../packages/core/src/testing/differential-current.js";
import { runWorkload } from "../packages/core/src/testing/differential-harness.js";
import { workloads } from "../packages/core/src/testing/differential-sources.js";

// Refuse ambient seed overrides: the committed oracle always represents the default seeds.
if (process.env.MINNOW_SEED) throw new Error("Unset MINNOW_SEED to freeze committed seeds");
const corpus = [];
for (const workload of await workloads())
  corpus.push({ workload, outcomes: await runWorkload(currentEngine, "memory", workload) });
const path = "packages/core/testdata/differential-corpus.json";
const bytes = JSON.stringify({ version: 1, corpus }, function (key, value: unknown) {
  const original = (this as Record<string, unknown>)[key];
  return original instanceof Date ? { $date: original.toISOString() } : value;
});
writeFileSync(path, bytes + "\n");
console.log(
  `${path}: ${bytes.length} bytes; ${corpus.length} workloads; ${corpus.reduce((n, item) => n + item.workload.operations.length, 0)} operations`,
);
