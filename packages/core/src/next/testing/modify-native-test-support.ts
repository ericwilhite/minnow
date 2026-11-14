/** Native execution without the adapter's commit policy, for statement/delivery laws. */
import type { StorageCore } from "./storage-core.js";
import type { ParameterValue } from "../exec/executor.js";
import { compileTableRules } from "../exec/rules.js";
import { ChunkReader } from "../page/reader.js";
export async function startModification(
  core: StorageCore,
  sql: string,
  returning: "atCommit" | "atExecuted",
  parameters: readonly ParameterValue[] = [],
) {
  const lowered = core.lowerer.lower(sql, core.context());
  if (lowered.kind !== "modify") throw new TypeError("Expected modification");
  const prepared = core.executor.prepare(lowered.plan);
  const tx = await core.harness.path.begin();
  const statement = tx.statement({
    rules: (id) => {
      const table = statement.catalog.table(id);
      if (!table) throw new TypeError("Missing table");
      return compileTableRules(
        table,
        statement.catalog.version,
        core.lowerer.lowerTableRules(table, statement.catalog),
      );
    },
  });
  try {
    const execution = prepared.execute({
      snapshot: null,
      statement,
      parameters,
      statementTime: 0n,
      governor: core.harness.governor,
      spill: core.spill,
      queryId: 1000000,
      reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
      sequences: null,
      sequenceSession: null,
      returning,
      resultWindowBytes: 4096,
    });
    return { tx, statement, execution };
  } catch (error) {
    statement.rollback();
    tx.rollback();
    throw error;
  }
}
