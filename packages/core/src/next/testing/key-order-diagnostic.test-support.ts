/**
 * The diagnostic ordered-scan plan (exec/plan.ts, `keyOrder: true`): an explicit full-range index
 * scan in place of `SELECT ... ORDER BY <primary key>`'s sorted scan. Its cursor is funded with the
 * query's admission (exec/key-order.ts `IndexShape.cursor`).
 */
import { createExecutor } from "../exec/create.js";
import type { StorageCore } from "./storage-core.js";

export function useDiagnosticOrderedScan(core: StorageCore): void {
  const diagnostic = createExecutor({ keyOrder: true });
  type Plan = Parameters<typeof diagnostic.prepare>[0];
  interface Node {
    op: string;
    input: Node;
    id: number;
    tableId: number;
    columns: number[];
    output: unknown;
  }
  core.executor.prepare = (plan: Plan) => {
    const root = plan.root as unknown as Node;
    const project = root.input,
      scan = project.input;
    if (root.op !== "sort" || project.op !== "project" || scan.op !== "scan")
      throw new Error(`unexpected shape ${root.op}`);
    const indexId = core.harness.path.catalog.table(scan.tableId)?.primaryKey ?? 0;
    const range = {
      id: scan.id,
      op: "indexRange",
      output: scan.output,
      tableId: scan.tableId,
      indexId,
      lower: null,
      upper: null,
      descending: false,
      columns: scan.columns,
      emitLocator: false,
    };
    return diagnostic.prepare({
      ...plan,
      root: { ...root, input: { ...project, input: range } },
    } as unknown as Plan);
  };
}
