/** Immutable validated facts shared by the catalog cache and execution binding. */
import { bigintBytes, stringBytes, OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import type { CheckDef, TableDef } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import type { ScalarExpr } from "../ir/ir.js";
export interface CompiledCheck {
  readonly def: CheckDef;
  readonly condition: ScalarExpr;
  readonly reads: ReadonlySet<number>;
}
export interface RuleProgram {
  readonly table: TableDef;
  readonly checks: readonly CompiledCheck[];
  readonly generated: ReadonlyArray<{
    readonly columnId: number;
    readonly index: number;
    readonly expr: ScalarExpr;
    readonly reads: ReadonlySet<number>;
  }>;
}
const programs = new WeakMap<TableRules, RuleProgram>();
export function rememberRuleProgram(rules: TableRules, program: RuleProgram): void {
  programs.set(rules, program);
}
export function tableRuleProgram(rules: TableRules): RuleProgram | undefined {
  return programs.get(rules);
}

/** Active bindings retain IR/literals independently of an evictable program cache. */
export function ruleRetentionBytes(program: RuleProgram): number {
  const seen = new Set<object>();
  const size = (v: unknown): number => {
    if (typeof v === "bigint") return bigintBytes(v);
    if (typeof v === "string") return stringBytes(v);
    if (typeof v === "number") return 8;
    if (v === null || typeof v !== "object" || seen.has(v)) return 0;
    seen.add(v);
    if (ArrayBuffer.isView(v)) {
      if (seen.has(v.buffer)) return OBJECT_OVERHEAD_BYTES;
      seen.add(v.buffer);
      return OBJECT_OVERHEAD_BYTES + v.buffer.byteLength;
    }
    let bytes = OBJECT_OVERHEAD_BYTES;
    for (const child of Array.isArray(v) ? v : Object.values(v)) bytes += size(child);
    return bytes;
  };
  let bytes = 0;
  for (const c of program.checks) bytes += size(c.condition);
  for (const g of program.generated) bytes += size(g.expr);
  return bytes;
}
