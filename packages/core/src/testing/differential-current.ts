import { IDBFactory } from "fake-indexeddb";
import { MinnowDatabase } from "../engine/database.js";
import { IndexedDbBlockStore, MemoryBlockStore, OpfsBlockStore } from "../storage/index.js";
import { MemoryOpfs } from "./opfs-shim.js";
import type { EngineFactory, EngineResult } from "./differential-harness.js";

/** Node adapter. Each factory invocation owns an isolated store and releases it on close. */
export const currentEngine: EngineFactory = async (kind) => {
  const name = crypto.randomUUID();
  const indexedDB = new IDBFactory();
  const root = new MemoryOpfs().root;
  const memory = new MemoryBlockStore();
  const acquire = async () =>
    kind === "memory"
      ? memory
      : kind === "indexeddb"
        ? await IndexedDbBlockStore.open({ name, indexedDB })
        : await OpfsBlockStore.open({ name, root });
  let store = await acquire();
  let database = new MinnowDatabase(store);
  return {
    async run(operation): Promise<EngineResult> {
      if (operation.kind === "reopen") {
        await database.close();
        if (kind !== "memory") {
          store.close();
          store = await acquire();
        }
        database = new MinnowDatabase(store);
        return { kind: "reopen" };
      }
      if (operation.kind === "schema") {
        await database.createTable(operation.schema);
        return { kind: "schema" };
      }
      if (operation.kind !== "sql") {
        const result =
          operation.kind === "deleteBatch"
            ? await database.deleteBatch(operation.table, { keys: operation.keys })
            : await database[operation.kind](operation.table, operation.rows);
        const counts: Record<string, number> = {
          rowCount: "rowCount" in result ? result.rowCount : result.deletedRowCount,
        };
        for (const key of [
          "requestedRowCount",
          "insertedRowCount",
          "updatedRowCount",
          "skippedRowCount",
        ] as const)
          if (key in result) {
            const count = (result as unknown as Record<string, number>)[key];
            if (count !== undefined) counts[key] = count;
          }
        return { kind: operation.kind, counts };
      }
      const result = await database.execute(operation.sql, operation.params);
      if (result.kind === "rows")
        return {
          kind: result.kind,
          columns: result.result.columns,
          rows: result.result.rows,
          domains: result.result.columnDomains,
        };
      return {
        kind: result.kind,
        ...("returnedColumnDomains" in result ? { domains: result.returnedColumnDomains } : {}),
        ...("rowCount" in result ? { counts: { rowCount: result.rowCount } } : {}),
        ...("returnedColumns" in result ? { columns: result.returnedColumns } : {}),
        ...("returnedRows" in result ? { rows: result.returnedRows } : {}),
      };
    },
    async close() {
      await database.close();
      store.close();
    },
  };
};
