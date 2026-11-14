/** Native execution without the adapter's commit policy, for statement/delivery laws. */
import type { StorageCore } from "./storage-core.js";
import type { ParameterValue } from "../exec/executor.js";
import { StatementRules } from "../exec/statement-rules.js";
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
  if (lowered.plan.root.op !== "modify") throw new TypeError("Missing modification root");
  const rowRules = new StatementRules(tx.catalog, lowered.plan.root.tableId, (id) => {
    const table = tx.catalog.table(id);
    if (!table) throw new TypeError("Missing rule table");
    return compileTableRules(
      table,
      tx.catalog.version,
      core.lowerer.lowerTableRules(table, tx.catalog),
    );
  });
  const statement = tx.statement({ rules: (id) => rowRules.table(id), targets: rowRules.targets });
  try {
    const execution = prepared.execute({
      snapshot: null,
      rowRules,
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
