/** Pack proven immutable bindings, not equal values. The original producer still evaluates
 * every expression; this projection only removes duplicate buffers from hash/spill rows.
 * The final projection restores every logical key position (including its name/type).
 */
import type { Rel, ScalarExpr } from "../../ir/ir.js";
import { samePhysicalType } from "../../types.js";

type Aggregate = Extract<Rel, { op: "aggregate" }>;
function present<T>(value: T | undefined): T {
  if (value === undefined) throw new TypeError("Aggregate column outside physical layout");
  return value;
}
const children = (r: Rel): readonly Rel[] =>
  "input" in r ? [r.input] : "left" in r ? [r.left, r.right] : "inputs" in r ? r.inputs : [];

export function packAggregateInputs(root: Rel): Rel {
  let id = 0;
  const visitIds = (r: Rel): void => {
    id = Math.max(id, r.id + 1);
    for (const child of children(r)) visitIds(child);
    if (r.op === "recursive") {
      visitIds(r.base);
      visitIds(r.step);
    }
  };
  visitIds(root);
  const expressions = new Map<string, symbol>();
  const numbers = new Map<symbol, number>();
  const intern = (key: string): symbol => {
    let value = expressions.get(key);
    if (!value) {
      value = Symbol();
      expressions.set(key, value);
    }
    return value;
  };
  const number = (value: symbol): string => {
    let n = numbers.get(value);
    if (n === undefined) {
      n = numbers.size;
      numbers.set(value, n);
    }
    return String(n);
  };
  const params = new Map<number, symbol>();
  const cache = new WeakMap<Rel, readonly symbol[]>();
  const origins = (r: Rel): readonly symbol[] => {
    const known = cache.get(r);
    if (known) return known;
    const expression = (e: ScalarExpr, input: readonly symbol[]): symbol => {
      if (e.op === "column") return input[e.index] ?? Symbol();
      if (e.op === "param") {
        let key = params.get(e.slot);
        if (!key) {
          key = Symbol();
          params.set(e.slot, key);
        }
        return key;
      }
      if (e.op === "literal") {
        const d = e.value.data;
        const data =
          d.layout === "varlen" && d.bytes.length <= 128
            ? d.bytes
            : d.layout === "bool" || d.layout === "i32" || d.layout === "f32" || d.layout === "f64"
              ? new Uint8Array(d.values.buffer, d.values.byteOffset, d.values.BYTES_PER_ELEMENT)
              : null;
        if (data && ["text", "bytes", "bool", "int32", "float32", "float64"].includes(e.type.kind))
          return intern(
            `literal/${JSON.stringify(e.type)}/${String(e.value.validity?.[0] ?? 1)}/${Array.from(data).join(",")}`,
          );
      }
      // Immutable structural identity only, never evaluate/fold. The producer still
      // executes BOTH expressions in their original order. Unknown/volatile calls
      // get fresh origins and cannot be made equal by this whitelist.
      if (e.op === "call" && (e.fn === "coalesce" || e.fn === "nullif"))
        return intern(`${e.fn}/${e.args.map((a) => number(expression(a, input))).join(",")}`);
      if (e.op === "compare" || e.op === "distinctFrom")
        return intern(
          `${e.op}/${e.op === "compare" ? `${e.cmp}/${JSON.stringify(e.collation)}` : String(e.negated)}/${number(expression(e.left, input))}/${number(expression(e.right, input))}`,
        );
      if (e.op === "and" || e.op === "or")
        return intern(`${e.op}/${e.args.map((a) => number(expression(a, input))).join(",")}`);
      if (e.op === "not" || e.op === "isNull")
        return intern(
          `${e.op}/${String(e.op === "isNull" && e.negated)}/${number(expression(e.arg, input))}`,
        );
      if (e.op === "case")
        return intern(
          `case/${e.branches.map((b) => `${number(expression(b.when, input))}:${number(expression(b.then, input))}`).join(",")}/${e.otherwise ? number(expression(e.otherwise, input)) : "null"}`,
        );
      // Only identity casts preserve the binding (not truncation/NUMERIC typmods).
      if (e.op === "cast" && samePhysicalType(e.type, e.input.type))
        return expression(e.input, input);
      return Symbol();
    };
    const input = "input" in r ? origins(r.input) : [];
    const result =
      r.op === "project"
        ? r.exprs.map((e) => expression(e, input))
        : r.op === "values" && r.rows.length === 1
          ? r.output.map((_, c) => (r.rows[0]?.[c] ? expression(r.rows[0][c], []) : Symbol()))
          : ["filter", "limit", "sort"].includes(r.op)
            ? r.output.map((_, c) => input[c] ?? Symbol())
            : r.output.map(() => Symbol());
    cache.set(r, result);
    return result;
  };
  const column = (input: Rel, index: number): ScalarExpr => {
    const output = input.output[index];
    if (!output) throw new Error("Aggregate input outside physical layout");
    return { op: "column", index, type: output.type };
  };
  const remap = (e: ScalarExpr, index: readonly number[]): ScalarExpr => {
    switch (e.op) {
      case "column":
        return { ...e, index: index[e.index] ?? e.index };
      case "literal":
      case "param":
        return e;
      case "cast":
        return { ...e, input: remap(e.input, index) };
      case "call":
      case "and":
      case "or":
        return { ...e, args: e.args.map((a) => remap(a, index)) };
      case "compare":
      case "distinctFrom":
        return { ...e, left: remap(e.left, index), right: remap(e.right, index) };
      case "case":
        return {
          ...e,
          branches: e.branches.map((b) => ({
            when: remap(b.when, index),
            then: remap(b.then, index),
          })),
          otherwise: e.otherwise && remap(e.otherwise, index),
        };
      case "inList":
        return { ...e, arg: remap(e.arg, index), list: e.list.map((a) => remap(a, index)) };
      case "like":
        return { ...e, arg: remap(e.arg, index), pattern: remap(e.pattern, index) };
      case "not":
      case "isNull":
        return { ...e, arg: remap(e.arg, index) };
    }
  };
  const pack = (r: Aggregate): Rel => {
    // GROUPING SETS/GROUPING use logical key identities, not merely key equality.
    if (r.groupingSets !== null) return r;
    const fields = origins(r.input),
      sources: number[] = [],
      index: number[] = [];
    for (let c = 0; c < fields.length; c++) {
      const found = sources.findIndex(
        (p) =>
          fields[p] === fields[c] &&
          samePhysicalType(present(r.input.output[p]).type, present(r.input.output[c]).type),
      );
      index.push(found < 0 ? sources.length : found);
      if (found < 0) sources.push(c);
    }
    if (sources.length === r.input.output.length) return r;
    const input: Rel = {
      op: "project",
      id: id++,
      input: r.input,
      output: sources.map((c) => present(r.input.output[c])),
      exprs: sources.map((c) => column(r.input, c)),
    };
    const keys: number[] = [],
      restore: number[] = [],
      output = [];
    for (const [position, key] of r.groupBy.entries()) {
      const mapped = present(index[key]);
      let at = keys.indexOf(mapped);
      if (at < 0) {
        at = keys.length;
        keys.push(mapped);
        output.push(present(r.output[position]));
      }
      restore.push(at);
    }
    output.push(...r.output.slice(r.groupBy.length));
    const physical: Aggregate = {
      ...r,
      input,
      groupBy: keys,
      output,
      aggregates: r.aggregates.map((a) => ({
        ...a,
        args: a.args.map((e) => remap(e, index)),
        filter: a.filter && remap(a.filter, index),
        orderBy: a.orderBy.map((k) => ({ ...k, expr: remap(k.expr, index) })),
      })),
    };
    return {
      op: "project",
      id: id++,
      input: physical,
      output: r.output,
      exprs: [
        ...restore.map((c) => column(physical, c)),
        ...r.aggregates.map((_, c) => column(physical, keys.length + c)),
      ],
    };
  };
  const visit = (r: Rel): Rel => {
    if ("input" in r) r = { ...r, input: visit(r.input) };
    else if ("left" in r) r = { ...r, left: visit(r.left), right: visit(r.right) };
    else if ("inputs" in r) r = { ...r, inputs: r.inputs.map(visit) };
    // Correlated/recursive producers keep their original binding scopes.
    return r.op === "aggregate" ? pack(r) : r;
  };
  return visit(root);
}
