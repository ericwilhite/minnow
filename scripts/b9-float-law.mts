/** NEVER MERGE. Law: PostgreSQL aggregate inputs have unspecified order unless
 * the aggregate specifies ORDER BY. Lead-authorized comparison only for unordered
 * float4/float8 SUM/AVG and variance/stddev; raw output and determinism stay exact.
 * https://www.postgresql.org/docs/18/sql-expressions.html#SYNTAX-AGGREGATES
 */
import type { Rel, ScalarExpr } from "../packages/core/dist/next/ir/ir.js";
import type { Verification } from "./lib/independent-cpu.mts";
const FUNCTIONS = new Set([
  "sum",
  "avg",
  "variance",
  "var_pop",
  "var_samp",
  "stddev",
  "stddev_pop",
  "stddev_samp",
]);
const floating = (kind: string) => kind === "float32" || kind === "float64";

/** Only direct output lineage qualifies. Group/join keys, computed projections,
 * NUMERIC/integer calls and every window result retain exact comparison.
 */
export function unorderedFloatColumns(root: Rel): readonly boolean[] {
  const memo = new Map<Rel, readonly boolean[]>();
  const referenced = (expr: ScalarExpr, mask: boolean[]) => {
    if (expr.op === "column") {
      mask[expr.index] = false;
      return;
    }
    for (const value of Object.values(expr)) {
      const values = Array.isArray(value) ? value : [value];
      for (const child of values)
        if (child && typeof child === "object" && "op" in child && "type" in child)
          referenced(child as ScalarExpr, mask);
    }
  };
  const visit = (rel: Rel): readonly boolean[] => {
    const old = memo.get(rel);
    if (old) return old;
    let flags: boolean[] = rel.output.map(() => false);
    if (rel.op === "aggregate" && rel.groupingSets === null) {
      for (const [i, call] of rel.aggregates.entries())
        flags[rel.groupBy.length + i] =
          FUNCTIONS.has(call.fn) && floating(call.type.kind) && call.orderBy.length === 0;
    } else if (rel.op === "project") {
      const input = visit(rel.input);
      flags = rel.exprs.map(
        (e) => e.op === "column" && floating(e.type.kind) && input[e.index] === true,
      );
    } else if (rel.op === "filter" || rel.op === "sort" || rel.op === "limit") {
      const input = visit(rel.input);
      flags = rel.output.map((_, i) => input[i] ?? false);
    } else if (rel.op === "window") {
      const input = visit(rel.input);
      flags = rel.output.map((_, i) => input[i] ?? false);
      for (const index of rel.partitionBy) flags[index] = false;
    } else if (rel.op === "join") {
      const left = [...visit(rel.left)],
        right = [...visit(rel.right)];
      for (const key of rel.leftKeys) referenced(key, left);
      for (const key of rel.rightKeys) referenced(key, right);
      flags = rel.output.map((_, i) =>
        i < left.length ? (left[i] ?? false) : (right[i - left.length] ?? false),
      );
    }
    flags = rel.output.map((column, i) => floating(column.type.kind) && flags[i] === true);
    memo.set(rel, flags);
    return flags;
  };
  return visit(root);
}

const scratch = new DataView(new ArrayBuffer(8));
export interface FloatCell {
  readonly floatKind: "float32" | "float64";
  readonly bits: string;
  readonly value: string;
}
const text = (value: number) =>
  Number.isNaN(value) ? "NaN" : Object.is(value, -0) ? "-0" : String(value);
export function floatCell(
  value: number,
  kind: "float32" | "float64",
  wireBits?: string,
): FloatCell {
  let bits = wireBits;
  if (bits === undefined) {
    if (kind === "float32") {
      scratch.setFloat32(0, value, true);
      bits = scratch.getUint32(0, true).toString(16).padStart(8, "0");
      value = scratch.getFloat32(0, true);
    } else {
      scratch.setFloat64(0, value, true);
      bits = scratch.getBigUint64(0, true).toString(16).padStart(16, "0");
    }
  }
  return { floatKind: kind, bits, value: text(value) };
}
function decoded(cell: unknown, kind: string): number | undefined {
  if (cell === null || typeof cell !== "object") return undefined;
  const c = cell as Partial<FloatCell>;
  if (c.floatKind !== kind || typeof c.bits !== "string" || typeof c.value !== "string")
    return undefined;
  if (!new RegExp(kind === "float32" ? "^[0-9a-f]{8}$" : "^[0-9a-f]{16}$").test(c.bits))
    return undefined;
  let value: number;
  if (kind === "float32") {
    scratch.setUint32(0, Number.parseInt(c.bits, 16), true);
    value = scratch.getFloat32(0, true);
  } else {
    scratch.setBigUint64(0, BigInt("0x" + c.bits), true);
    value = scratch.getFloat64(0, true);
  }
  return text(value) === c.value ? value : undefined;
}
export function exactJSON(value: unknown): string {
  return JSON.stringify(value, (_key, v: unknown) =>
    typeof v === "bigint"
      ? { bigint: v.toString() }
      : typeof v === "number" && (!Number.isFinite(v) || Object.is(v, -0))
        ? { float: text(v) }
        : v,
  );
}
export function rawRow(
  values: readonly unknown[],
  kinds: readonly string[],
  bits?: ReadonlyArray<string | undefined>,
): string {
  if (values.length !== kinds.length) throw new Error("B9 result width changed");
  return exactJSON(
    values.map((v, i) =>
      floating(kinds[i] ?? "") && v !== null
        ? typeof v === "number"
          ? floatCell(v, kinds[i] as "float32" | "float64", bits?.[i])
          : (() => {
              throw new Error("Non-numeric floating result");
            })()
        : v,
    ),
  );
}
export interface RawResult {
  readonly version: 1;
  readonly schema: ReadonlyArray<{
    readonly name: string;
    readonly kind: string;
    readonly type: string;
  }>;
  readonly eligible: readonly boolean[];
  readonly rows: readonly string[];
  readonly planIdentity: string;
}
function raw(verification: Verification): RawResult | null {
  if (
    !verification.detail ||
    typeof verification.detail !== "object" ||
    !("raw" in verification.detail)
  )
    return null;
  const value = verification.detail.raw as Partial<RawResult> | null;
  if (
    !value ||
    value.version !== 1 ||
    !Array.isArray(value.schema) ||
    !Array.isArray(value.eligible) ||
    !Array.isArray(value.rows) ||
    typeof value.planIdentity !== "string" ||
    value.schema.length !== value.eligible.length ||
    Object.keys(value).sort().join(",") !== "eligible,planIdentity,rows,schema,version" ||
    typeof verification.determinismFingerprint !== "string" ||
    verification.determinismFingerprint.length === 0
  )
    return null;
  if (value.rows.length !== verification.rows) return null;
  for (const [i, c] of value.schema.entries())
    if (
      !c ||
      typeof c.name !== "string" ||
      typeof c.kind !== "string" ||
      typeof c.type !== "string" ||
      typeof value.eligible[i] !== "boolean" ||
      (value.eligible[i] && !floating(c.kind))
    )
      return null;
  return value as RawResult;
}
/** Exact semantic result bits across different source identities. A native-order
 * copy-elision proposal must meet this stronger rule against its current parent. */
export function sameResultBits(a: Verification, b: Verification): boolean {
  const x = raw(a),
    y = raw(b);
  return (
    x !== null &&
    y !== null &&
    a.rows === b.rows &&
    a.fingerprint === b.fingerprint &&
    a.determinismFingerprint === b.determinismFingerprint &&
    exactJSON(x.schema) === exactJSON(y.schema) &&
    exactJSON(x.eligible) === exactJSON(y.eligible) &&
    exactJSON(x.rows) === exactJSON(y.rows)
  );
}
/** Same physical program/input: compare all semantic bits and the original row sequence. */
export function samePlanBits(a: Verification, b: Verification): boolean {
  const x = raw(a),
    y = raw(b);
  return (
    x !== null &&
    y !== null &&
    a.rows === b.rows &&
    a.fingerprint === b.fingerprint &&
    a.determinismFingerprint === b.determinismFingerprint &&
    exactJSON(x) === exactJSON(y)
  );
}
/** Cross-plan only. Rows are positional; no sorting/matching obtains a tolerant match.
 * This is the reference suite's exact finite rule, including its unit-scale floor.
 * All special values (also signed zero) and every unmarked cell compare exactly.
 */
export function compareFloatResults(a: Verification, b: Verification): boolean {
  const x = raw(a),
    y = raw(b);
  if (
    !x ||
    !y ||
    a.rows !== b.rows ||
    exactJSON(x.schema) !== exactJSON(y.schema) ||
    exactJSON(x.eligible) !== exactJSON(y.eligible)
  )
    return false;
  if (x.planIdentity === y.planIdentity) return samePlanBits(a, b);
  for (let r = 0; r < x.rows.length; r++) {
    let left: unknown, right: unknown;
    try {
      left = JSON.parse(x.rows[r] ?? "");
      right = JSON.parse(y.rows[r] ?? "");
    } catch {
      return false;
    }
    if (
      !Array.isArray(left) ||
      !Array.isArray(right) ||
      left.length !== x.schema.length ||
      right.length !== left.length
    )
      return false;
    for (let c = 0; c < left.length; c++) {
      const l: unknown = left[c],
        v: unknown = right[c],
        kind = x.schema[c]?.kind ?? "";
      if (!x.eligible[c] || l === null || v === null) {
        if (exactJSON(l) !== exactJSON(v)) return false;
        continue;
      }
      const p = decoded(l, kind),
        q = decoded(v, kind);
      if (p === undefined || q === undefined) return false;
      if (!Number.isFinite(p) || !Number.isFinite(q) || Object.is(p, -0) || Object.is(q, -0)) {
        if (exactJSON(l) !== exactJSON(v)) return false;
      } else if (Math.abs(p - q) > Math.max(Math.abs(p), Math.abs(q), 1) * 1e-9) return false;
    }
  }
  return true;
}
