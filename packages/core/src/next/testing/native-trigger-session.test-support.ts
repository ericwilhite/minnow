/** Actual native graph caller for multiple statements/savepoints. Test funding only;
 * no synthetic body/RI adapter and no production admission recipe. */
import type { BoundParameters, Execution } from "../exec/executor.js";
import { compileTableRules } from "../exec/rules.js";
import { bindTableRules } from "../exec/row-rules.js";
import { StatementImpl } from "../write/transaction.js";
import { runTriggerStatement } from "../owner/trigger-statement.js";
import { oracleRows, oracleValue } from "./differential-adapter.js";
import { StreamDictionaries } from "../owner/frame.js";
import { isValid } from "../vector/validity.js";
import { releaseIdle } from "../write/budget.js";
import { NO_RULES } from "../write/test-support.js";
import { commandRootFixture } from "./command-root.test-support.js";

export async function nativeTriggerSession(
  setup: readonly string[],
  sql: string,
  published: boolean,
) {
  const f = await commandRootFixture(setup, sql, published, 64 << 20, true);
  const metadataFloor = f.metadata.held;
  // The fixture installs an initial graph. Retire it before taking a real savepoint,
  // without rolling back the transaction; all later graphs use the SAME native writer.
  await f.root.close();
  f.statement.rollback();
  for (const binding of f.scope.tables) binding.rules.close();
  const programs = f.scope.tables.map(({ table }) => ({
    table,
    program: compileTableRules(
      table,
      f.tx.catalog.version,
      f.core.lowerer.lowerTableRules(table, f.tx.catalog),
    ),
  }));
  let identity = f.scope.rootId;
  const execute = async (parameters: BoundParameters) => {
    const tables: Array<(typeof f.scope.tables)[number]> = [];
    try {
      // Each fresh graph requires the complete caller reservation again. Queries between
      // graphs may return idle credit; setupBytes alone excludes producer metadata.
      await f.scope.funding.prepareMetadata(metadataFloor, f.scope.signal);
      for (const p of programs)
        tables.push({
          table: p.table,
          rules: bindTableRules(p.program, f.metadata, { capacity: 1, signal: f.scope.signal }),
        });
      const opened = f.tx.statement({
        signal: f.scope.signal,
        rules: (id) => {
          const found = tables.find((t) => t.table.id === id);
          if (!found) throw new Error("Missing native test rule binding");
          return found.rules;
        },
      });
      if (!(opened instanceof StatementImpl)) throw new TypeError("Native statement required");
      const statement = opened;
      return await runTriggerStatement(
        {
          ...f.scope,
          rootId: ++identity,
          statement,
          tables,
          funding: {
            ...f.scope.funding,
            releaseIdleStep: () => {
              releaseIdle(f.source);
              releaseIdle(f.metadata, Math.max(metadataFloor, f.metadata.allocated + 65536));
            },
          },
        },
        parameters,
      );
    } finally {
      for (const table of tables) table.rules.close();
    }
  };
  const query = async (text: string) => {
    const lowered = f.core.lowerer.lower(text, { ...f.core.context(), catalog: f.tx.catalog });
    if (lowered.kind !== "query") throw new TypeError("Native observation must be a SELECT");
    f.scope.funding.releaseIdleStep();
    const statement = f.tx.statement({ signal: f.scope.signal, rules: () => NO_RULES });
    const pin = statement.readOwn();
    let execution: Execution | null = null;
    try {
      const prepared = f.core.executor.prepare(lowered.plan);
      execution = prepared.execute({
        ...f.scope.execution,
        parameters: [],
        snapshot: pin,
        statement: null,
        governor: f.core.harness.governor,
        signal: f.scope.signal,
      });
      const dictionaries = new StreamDictionaries(),
        rows: Array<Record<string, unknown>> = [];
      for (;;) {
        const frame = await execution.results.next();
        if (frame === null) break;
        try {
          const payload = frame.payload;
          dictionaries.apply(payload);
          for (let i = 0; i < payload.rowCount; i++) {
            const row: Record<string, unknown> = {};
            for (const [slot, meta] of prepared.columns.entries()) {
              const column = payload.columns[slot];
              if (!column) throw new Error("Missing native observation column");
              if (column.form === "dictionary") {
                if (!isValid(column.validity, i)) {
                  row[meta.name] = null;
                  continue;
                }
                const value = dictionaries.locate(column.dictionary, column.codes[i] ?? -1);
                row[meta.name] = oracleValue({ ...value.entries, type: meta.type }, value.index);
              } else if (column.form === "constant")
                row[meta.name] = oracleValue({ ...column.value, type: meta.type }, 0);
              else row[meta.name] = oracleValue({ ...column, type: meta.type }, i);
            }
            rows.push(row);
          }
        } finally {
          frame.release();
        }
      }
      await execution.executed;
      await execution.delivered;
      pin.release();
      await statement.end();
      return rows;
    } catch (error) {
      if (execution !== null) {
        execution.cancel(error);
        await Promise.allSettled([execution.executed, execution.delivered]);
      }
      statement.rollback();
      throw error;
    } finally {
      pin.release();
    }
  };
  return {
    execute,
    query,
    savepoint: () => f.tx.savepoint(),
    rollbackTo: (save: ReturnType<typeof f.tx.savepoint>) => f.tx.rollbackTo(save),
    release: (save: ReturnType<typeof f.tx.savepoint>) => f.tx.release(save),
    async committedQuery(sql: string) {
      const result = await f.core.execute(sql);
      if (!("batches" in result)) throw new TypeError("Native committed observation must SELECT");
      return oracleRows(result);
    },
    async commit() {
      const receipt = await f.tx.commit(null);
      await receipt.durable;
    },
    close: () => f.close(),
  };
}
