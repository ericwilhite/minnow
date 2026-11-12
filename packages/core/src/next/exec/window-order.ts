/** Prove a window's partition/order prefix, then require its globally sorted route.
 * Computed keys may be duplicated by lowering; only structurally identical nonvolatile
 * expressions over the same bindings prove equality. No scalar work is moved or removed.
 */
import type { Rel, SortKey } from "../ir/ir.js";
import { isByteCollation } from "./sort-keys.js";
import { VOLATILE_FUNCTIONS } from "../ir/functions.js";

function column(value: unknown): value is { readonly op: "column"; readonly index: number } {
  return (
    typeof value === "object" &&
    value !== null &&
    "op" in value &&
    value.op === "column" &&
    "index" in value &&
    typeof value.index === "number"
  );
}
function volatile(value: object): boolean {
  return (
    "op" in value &&
    value.op === "call" &&
    "fn" in value &&
    typeof value.fn === "string" &&
    VOLATILE_FUNCTIONS.has(value.fn)
  );
}
/** Compare syntax without copying literal buffers or evaluating any scalar. */
function same(left: unknown, leftInput: Rel, right: unknown, rightInput: Rel): boolean {
  if (column(left)) {
    if (column(right) && leftInput.id === rightInput.id && left.index === right.index) return true;
    if (leftInput.op === "project") {
      const expression = leftInput.exprs[left.index];
      return expression !== undefined && same(expression, leftInput.input, right, rightInput);
    }
  }
  if (column(right) && rightInput.op === "project") {
    const expression = rightInput.exprs[right.index];
    return expression !== undefined && same(left, leftInput, expression, rightInput.input);
  }
  if (column(left) || column(right)) return false;
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null)
    return Object.is(left, right);
  // A shared materialized column was handled above; duplicated volatile expressions are not equal.
  if (volatile(left) || volatile(right)) return false;
  if (ArrayBuffer.isView(left) || ArrayBuffer.isView(right)) {
    if (
      !ArrayBuffer.isView(left) ||
      !ArrayBuffer.isView(right) ||
      left.constructor !== right.constructor ||
      left.byteLength !== right.byteLength
    )
      return false;
    const a = new Uint8Array(left.buffer, left.byteOffset, left.byteLength),
      b = new Uint8Array(right.buffer, right.byteOffset, right.byteLength);
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
  }
  if (Array.isArray(left) || Array.isArray(right))
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((item, i) => same(item, leftInput, right[i], rightInput))
    );
  const keys = Object.keys(left),
    other = Object.keys(right);
  return (
    keys.length === other.length &&
    keys.every(
      (key) =>
        Object.hasOwn(right, key) &&
        same(
          Reflect.get(left, key) as unknown,
          leftInput,
          Reflect.get(right, key) as unknown,
          rightInput,
        ),
    )
  );
}

export function windowOrderSource(
  input: Rel,
  keys: readonly SortKey[],
): Extract<Rel, { op: "window" }> | null {
  let current = input,
    wanted = keys;
  while (current.op === "project") {
    // Even an unrelated computed expression can throw or be volatile on discarded rows.
    if (current.exprs.some((expr) => expr.op !== "column")) return null;
    const projected: SortKey[] = [];
    for (const key of wanted) {
      if (key.expr.op !== "column") return null;
      const expr = current.exprs[key.expr.index];
      if (expr?.op !== "column") return null;
      projected.push({ ...key, expr });
    }
    wanted = projected;
    current = current.input;
  }
  if (
    current.op !== "window" ||
    wanted.length > current.partitionBy.length + current.orderBy.length
  )
    return null;
  const window = current;
  return wanted.every((key, i) => {
    if (key.expr.op !== "column" || key.expr.index >= window.input.output.length) return false;
    const partition = window.partitionBy[i];
    const order =
      partition === undefined ? window.orderBy[i - window.partitionBy.length] : undefined;
    const column = partition ?? (order?.expr.op === "column" ? order.expr.index : undefined);
    if (column === undefined) return false;

    return (
      same(key.expr, window.input, { op: "column", index: column }, window.input) &&
      key.direction === (partition === undefined ? order?.direction : "asc") &&
      key.nulls === (partition === undefined ? order?.nulls : "last") &&
      (key.collation === (partition === undefined ? order?.collation : null) ||
        (isByteCollation(key.collation) &&
          isByteCollation(partition === undefined ? (order?.collation ?? null) : null)))
    );
  })
    ? window
    : null;
}

export function windowSuppliesOrder(input: Rel, keys: readonly SortKey[]): boolean {
  return windowOrderSource(input, keys) !== null;
}
