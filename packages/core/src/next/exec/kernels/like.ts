/** LIKE over UTF-8 code points, no regex, recursion, decoded row or pattern-sized scratch.
 * PostgreSQL like_match.c owns wildcard/escape traversal, including reachable 22025 errors.
 */
import { likeLower } from "./like-case.js";
import type { ScalarExpr, Collation } from "../../ir/ir.js";
import type { Batch, Selection, Vector } from "../../vector/vector.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";
import {
  countIn,
  rowsIn,
  flatOf,
  slotIndex,
  isValidSlot,
  setValidity,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

function invalidEscape(): never {
  throw new ExecutionError(
    "LIKE pattern must not end with escape character",
    "MINNOW_INVALID_ESCAPE",
    "22025",
  );
}
function width(c: number): number {
  return c < 128 ? 1 : c < 224 ? 2 : c < 240 ? 3 : 4;
}
function point(b: Uint8Array, i: number): number {
  const c = b[i] ?? 0;
  if (c < 128) return c;
  if (c < 224) return ((c & 31) << 6) | ((b[i + 1] ?? 0) & 63);
  if (c < 240) return ((c & 15) << 12) | (((b[i + 1] ?? 0) & 63) << 6) | ((b[i + 2] ?? 0) & 63);
  return (
    ((c & 7) << 18) |
    (((b[i + 1] ?? 0) & 63) << 12) |
    (((b[i + 2] ?? 0) & 63) << 6) |
    ((b[i + 3] ?? 0) & 63)
  );
}
/** Last-percent retry is equivalent to ordered LIKE search; fixed cursors own all state. */
export function matchLike(
  t: Uint8Array,
  from: number,
  end: number,
  p: Uint8Array,
  start: number,
  stop: number,
  escape: number,
  insensitive: boolean,
  unicode = true,
): boolean {
  let i = from,
    j = start,
    retry = -1,
    suffix = -1;
  while (i < end) {
    let c = j < stop ? point(p, j) : -1;
    if (c === 37 && c !== escape) {
      do {
        j += width(p[j] ?? 0);
        if (j === stop) return true;
        c = point(p, j);
        if (c === 95 && c !== escape) {
          if (i === end) return false;
          i += width(t[i] ?? 0);
        } else if (c !== 37 || c === escape) break;
      } while (j < stop);
      if (c === escape && j + width(p[j] ?? 0) === stop) invalidEscape();
      suffix = j;
      retry = i;
      continue;
    }
    let next = j;
    if (c === escape) {
      next += width(p[next] ?? 0);
      if (next === stop) invalidEscape();
      c = point(p, next);
    }
    if (
      j < stop &&
      ((c === 95 && point(p, j) !== escape) ||
        (insensitive ? likeLower(c, unicode) : c) ===
          (insensitive ? likeLower(point(t, i), unicode) : point(t, i)))
    ) {
      i += width(t[i] ?? 0);
      j = next + width(p[next] ?? 0);
      continue;
    }
    if (suffix < 0 || retry === end) return false;
    retry += width(t[retry] ?? 0);
    i = retry;
    j = suffix;
  }
  while (j < stop && point(p, j) === 37 && escape !== 37) j += 1;
  return j === stop;
}
export function compileLike(
  mode: "like" | "ilike" | "similar",
  negated: boolean,
  arg: Compiled,
  pattern: Compiled,
  escape: string | null,
  collation: Collation | undefined,
  arena: OutputArena,
): Compiled {
  if (mode === "similar") throw new UnsupportedPlanError("SIMILAR TO");
  if (collation != null && !["C", "POSIX", "default"].includes(collation))
    throw new UnsupportedPlanError(`LIKE under collation ${collation}`);
  if (arg.type.kind !== "text" || pattern.type.kind !== "text")
    throw new UnsupportedPlanError("LIKE over non-text input");
  const esc = escape ?? "\\";
  // Invalid ESCAPE is an execution error, after strict NULL propagation, never prepare-time.
  const e = esc.length === 0 ? -1 : (esc.codePointAt(0) ?? -1);
  const validEscape = esc.length === 0 || esc.length === (e > 65535 ? 2 : 1);
  const out = arena.flat({ kind: "bool" });
  const unicode = collation !== "C" && collation !== "POSIX";
  return {
    type: { kind: "bool" },
    evaluate(batch: Batch, rows: Selection | null): Vector {
      const a = arg.evaluate(batch, rows),
        b = pattern.evaluate(batch, rows),
        av = flatOf(a),
        bv = flatOf(b);
      if (av.data.layout !== "varlen" || bv.data.layout !== "varlen")
        throw new TypeError("LIKE text layout");
      const values = (out.vector.data as { values: Uint8Array }).values;
      let nulls = false;
      for (let k = 0; k < countIn(rows, batch.length); k++) {
        const row = rowsIn(rows)[k] ?? 0;
        if (!isValidSlot(b, row)) {
          setValidity(out.validity, row, false);
          nulls = true;
          continue;
        }
        if (!validEscape)
          throw new ExecutionError("invalid escape string", "MINNOW_INVALID_ESCAPE", "22025");
        if (!isValidSlot(a, row)) {
          setValidity(out.validity, row, false);
          nulls = true;
          continue;
        }
        const ai = slotIndex(a, row),
          bi = slotIndex(b, row);
        const matched = matchLike(
          av.data.bytes,
          av.data.offsets[ai] ?? 0,
          av.data.offsets[ai + 1] ?? 0,
          bv.data.bytes,
          bv.data.offsets[bi] ?? 0,
          bv.data.offsets[bi + 1] ?? 0,
          e,
          mode === "ilike",
          unicode,
        );
        values[row] = +(negated ? !matched : matched);
        setValidity(out.validity, row, true);
      }
      out.vector.length = batch.length;
      out.vector.validity = nulls ? out.validity : null;
      return out.vector;
    },
  };
}

/** Star proof eligibility: only constant, nonthrowing patterns over safe original inputs. */
export function safeLikePattern(expr: Extract<ScalarExpr, { op: "like" }>): boolean {
  if (
    expr.mode === "similar" ||
    (expr.collation != null && !["C", "POSIX", "default"].includes(expr.collation)) ||
    expr.pattern.op !== "literal"
  )
    return false;
  const v = expr.pattern.value;
  if (!isValidSlot(v, 0)) return true;
  const e = expr.escape ?? "\\",
    cp = e.codePointAt(0) ?? -1;
  if (e.length !== 0 && e.length !== (cp > 65535 ? 2 : 1)) return false;
  if (v.data.layout !== "varlen") return false;
  let i = v.data.offsets[0] ?? 0;
  const end = v.data.offsets[1] ?? 0;
  while (i < end) {
    const c = point(v.data.bytes, i);
    i += width(v.data.bytes[i] ?? 0);
    if (c === cp) {
      if (i === end) return false;
      i += width(v.data.bytes[i] ?? 0);
    }
  }
  return true;
}
