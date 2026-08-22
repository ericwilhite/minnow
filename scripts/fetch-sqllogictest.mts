import { resolve } from "node:path";

import {
  describeCorpus,
  ensureSqlLogicCorpus,
  listSqlLogicTestFiles,
} from "./lib/sqllogictest-corpus.mts";

const directory = await ensureSqlLogicCorpus();
const files = listSqlLogicTestFiles(resolve(directory, "test"));
console.log(
  `${describeCorpus(directory)}: ${String(files.length)} test files ready at ${directory}`,
);
