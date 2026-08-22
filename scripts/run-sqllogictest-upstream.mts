import { spawnSync } from "node:child_process";
import { resolve } from "node:path";

import { ensureSqlLogicCorpus } from "./lib/sqllogictest-corpus.mts";

const directory = await ensureSqlLogicCorpus();
const result = spawnSync(
  "npm",
  [
    "run",
    "test:sql:logic",
    "--",
    "--directory",
    resolve(directory, "test"),
    ...process.argv.slice(2),
  ],
  { stdio: "inherit" },
);
process.exit(result.status ?? 1);
