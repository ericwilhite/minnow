/** Differential/Node-bench adapter over B3's actual write path and B1's extent storage. */
import type { CreateTableInput, DatabaseRow } from "../../engine/database.js";
import type { QueryParameter } from "../../engine/query.js";
import type { StoreKind } from "../../testing/differential-harness.js";
import type { CatalogChange, TableDef } from "../access/catalog.js";
import type { Statement } from "../access/modify.js";
import { createExecutor } from "../exec/create.js";
import {
  EXECUTOR_COSTS,
  type Execution,
  type ExecutionProfile,
  type PreparedPlan,
  type ParameterValue,
} from "../exec/executor.js";
import { UnsupportedPlanError } from "../exec/errors.js";
import { bindParameter } from "../exec/parameters.js";
import { ColumnStore } from "../exec/column-store.js";
import { compileTableRules } from "../exec/rules.js";
import { createLowerer } from "../lower/create.js";
import { ChunkReader } from "../page/reader.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { BATCH_ROWS, type Batch, type FlatVector } from "../vector/vector.js";
import { Harness } from "../write/test-support.js";
import type { NextCore, NextStatementResult } from "./differential-adapter.js";

export class StorageCore implements NextCore {
  lastProfile: ExecutionProfile | null = null;
  readonly harness: Harness;
  readonly lowerer = createLowerer();
  readonly executor = createExecutor();
  readonly spill = new MemorySpillService();
  #queryId = 0;

  constructor(memtableRows?: number) {
    this.harness = new Harness("strict", "memory", memtableRows);
  }
  async open(): Promise<this> {
    await this.harness.open();
    return this;
  }
  context() {
    return {
      catalog: this.harness.path.catalog,
      statistics: { table: () => null },
      costs: EXECUTOR_COSTS,
    };
  }
  async createTable(schema: CreateTableInput): Promise<void> {
    await this.change(this.lowerer.lowerSchemaTable(schema, this.harness.path.catalog));
  }
  async change(change: CatalogChange): Promise<void> {
    await this.write((statement) => statement.changeCatalog(change));
  }
  async write<T>(work: (statement: Statement) => T | Promise<T>): Promise<T> {
    const tx = await this.harness.path.begin();
    const statement = tx.statement({
      rules: (id) => {
        const table = statement.catalog.table(id);
        if (table === undefined) throw new TypeError("missing write table");
        return compileTableRules(
          table,
          statement.catalog.version,
          this.lowerer.lowerTableRules(table, statement.catalog),
        );
      },
    });
    try {
      const result = await work(statement);
      await statement.end();
      const receipt = await tx.commit(null);
      await receipt.durable;
      return result;
    } catch (error) {
      statement.rollback();
      tx.rollback();
      throw error;
    }
  }
  prepare(sql: string): PreparedPlan {
    const lowered = this.lowerer.lower(sql, this.context());
    if (lowered.kind !== "query")
      throw new UnsupportedPlanError("a non-query in the query adapter", {
        construct: lowered.kind,
      });
    return this.executor.prepare(lowered.plan);
  }
  /** Caller consumes results concurrently with execution and releases this pin after executed. */
  start(prepared: PreparedPlan, params: readonly ParameterValue[] = []) {
    const snapshot = this.harness.path.pin();
    let execution: Execution;
    try {
      execution = prepared.execute({
        snapshot,
        statement: null,
        parameters: params,
        statementTime: 0n,
        governor: this.harness.governor,
        spill: this.spill,
        queryId: ++this.#queryId,
        reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
      });
    } catch (error) {
      snapshot.release();
      throw error;
    }
    return { execution, release: () => snapshot.release() };
  }
  async execute(sql: string, params: readonly QueryParameter[] = []): Promise<NextStatementResult> {
    this.lastProfile = null;
    const lowered = this.lowerer.lower(sql, this.context());
    if (lowered.kind === "ddl") {
      if (lowered.followUp !== null)
        throw new UnsupportedPlanError("CREATE TABLE AS in the test adapter", { construct: "ddl" });
      if (lowered.change !== null) await this.change(lowered.change);
      return { kind: "ddl" };
    }
    if (lowered.kind !== "query")
      throw new UnsupportedPlanError(`the adapter's ${lowered.kind}`, { construct: lowered.kind });
    const prepared = this.executor.prepare(lowered.plan);
    const run = this.start(prepared, params);
    const batches: Batch[] = [];
    try {
      for (;;) {
        const frame = await run.execution.results.next();
        if (frame === null) break;
        try {
          const payload = structuredClone(frame.payload);
          batches.push({
            length: payload.rowCount,
            selection: null,
            columns: payload.columns.map((vector, index) => {
              if (vector.form !== "flat")
                throw new TypeError("the result sink must materialize flat frames");
              const type = prepared.columns[index]?.type;
              if (type === undefined) throw new TypeError("missing result type");
              return { ...vector, type };
            }),
          });
        } finally {
          frame.release();
        }
      }
      this.lastProfile = (await run.execution.executed).profile;
      await run.execution.delivered;
      return { kind: "rows", columns: prepared.columns, batches };
    } catch (error) {
      run.execution.cancel(error);
      await Promise.allSettled([run.execution.executed, run.execution.delivered]);
      throw error;
    } finally {
      run.release();
    }
  }
  async insertVectors(table: TableDef, batch: Batch): Promise<number> {
    return this.write((statement) => statement.table(table.id).insert(batch, null));
  }
  async writeBatch(
    kind: "insertBatch" | "upsertBatch",
    name: string,
    rows: readonly DatabaseRow[],
  ): Promise<Readonly<Record<string, number>>> {
    if (kind !== "insertBatch")
      throw new UnsupportedPlanError("upsert in the storage adapter", { construct: "upsert" });
    const table = this.harness.path.catalog.tableByName(name);
    if (table === undefined) throw new TypeError(`missing table ${name}`);
    const grant = await this.harness.governor.admit({
      label: "adapter/input",
      minimumBytes: 1024 * 1024,
      spillable: true,
    });
    try {
      let inserted = 0;
      await this.write(async (statement) => {
        const writer = statement.table(table.id);
        for (let start = 0; start < rows.length; start += BATCH_ROWS) {
          const store = new ColumnStore(
            table.columns.map((column) => column.type),
            grant,
            { chunkRows: BATCH_ROWS },
          );
          try {
            for (const row of rows.slice(start, start + BATCH_ROWS)) {
              const before = grant.allocated;
              const vectors: FlatVector[] = table.columns.map((column, slot) =>
                bindParameter(
                  column.type,
                  (row[column.name] ?? null) as ParameterValue,
                  slot,
                  grant,
                ),
              );
              const bindingBytes = grant.allocated - before;
              try {
                if (store.append(vectors, Uint16Array.of(0), 0, 1) !== 1)
                  throw new RangeError("adapter input allocation refused");
              } finally {
                grant.free(bindingBytes);
              }
            }
            inserted += await writer.insert(
              { length: store.rows, columns: store.vectors(), selection: null },
              null,
            );
          } finally {
            store.close();
          }
        }
      });
      return { rowCount: inserted };
    } finally {
      grant.close();
    }
  }
  deleteBatch(): Promise<Readonly<Record<string, number>>> {
    return Promise.reject(
      new UnsupportedPlanError("delete in the storage adapter", { construct: "delete" }),
    );
  }
  async reopen(): Promise<void> {
    await this.harness.reopen();
  }
  close(): Promise<void> {
    return this.harness.close();
  }
}

export async function storageCore(store: StoreKind): Promise<StorageCore> {
  if (store !== "memory")
    throw new UnsupportedPlanError(`the Node adapter's ${store} store`, { construct: "storage" });
  return new StorageCore().open();
}
