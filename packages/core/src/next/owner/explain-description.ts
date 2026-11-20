/** Diagnostic logical operators only. Never executes, dumps literals, or predicts runtime routes. */
import type { Rel } from "../ir/ir.js";
import { stringBytes } from "../memory/governor.js";
import { sqlTypeName } from "../lower/result.js";
import type { PlannedStatement } from "./engine.js";
import type { ExplainDescription } from "./inspection.js";

function walk(
  statement: PlannedStatement,
  visit: (node: Rel, depth: number, reused: boolean) => void,
): void {
  if (!("plan" in statement.lowered)) return;
  const stack: Array<{ node: Rel; depth: number }> = [];
  const plan = statement.lowered.plan;
  for (let i = plan.initPlans.length - 1; i >= 0; i--) {
    const init = plan.initPlans[i];
    if (init) stack.push({ node: init.plan, depth: 0 });
  }
  stack.push({ node: plan.root, depth: 0 });
  const seen = new Set<Rel>();
  while (stack.length) {
    const next = stack.pop();
    if (!next) break;
    const { node, depth } = next;
    const reused = seen.has(node);
    visit(node, depth, reused);
    if (reused) continue;
    seen.add(node);
    if ("input" in node) stack.push({ node: node.input, depth: depth + 1 });
    if ("left" in node) {
      stack.push({ node: node.right, depth: depth + 1 });
      stack.push({ node: node.left, depth: depth + 1 });
    }
    if ("outer" in node) {
      stack.push({ node: node.inner, depth: depth + 1 });
      stack.push({ node: node.outer, depth: depth + 1 });
    }
    if ("base" in node) {
      stack.push({ node: node.step, depth: depth + 1 });
      stack.push({ node: node.base, depth: depth + 1 });
    }
    if ("inputs" in node)
      for (let i = node.inputs.length - 1; i >= 0; i--) {
        const input = node.inputs[i];
        if (input) stack.push({ node: input, depth: depth + 1 });
      }
  }
}
function detail(node: Rel): string {
  let text = "tableId" in node ? ` table=${String(node.tableId)}` : "";
  if ("indexId" in node) text += ` index=${String(node.indexId)}`;
  if ("kind" in node) text += ` kind=${node.kind}`;
  if (node.op === "join") text += ` algorithm=${node.algorithm}`;
  return text;
}
export function describeExplain(statement: PlannedStatement): ExplainDescription {
  const lines = [
    `${statement.lowered.command} (${statement.kind}) — logical operators; not executed`,
  ];
  walk(statement, (node, depth, reused) => {
    // Bound indentation: deeply nested SQL must not produce quadratic text.
    lines.push(
      `${"  ".repeat(Math.min(depth, 12))}${node.op} #${String(node.id)} columns=${String(node.output.length)}${detail(node)}${reused ? " (shared)" : ""}`,
    );
  });
  return {
    version: 1,
    catalogVersion: statement.catalogVersion,
    command: statement.lowered.command,
    kind: statement.kind,
    text: lines.join("\n"),
    parameters: statement.parameters
      .slice(0, statement.parameterization?.text.placeholders ?? statement.parameters.length)
      .map(sqlTypeName),
    columns: statement.columns.map((column) => ({
      name: column.name,
      sqlType: column.sqlType,
      nullable: column.nullable,
    })),
  };
}
/** Two metadata copies plus bounded line-assembly/traversal workspace, not literal vectors. */
export function explainDescriptionBytes(statement: PlannedStatement): number {
  let size = 1024;
  walk(statement, () => {
    size += 768;
  });
  for (const column of statement.columns)
    size += 512 + stringBytes(column.name) + stringBytes(column.sqlType);
  for (const type of statement.lowered.parameters) size += 128 + stringBytes(sqlTypeName(type));
  return 2 * size;
}
