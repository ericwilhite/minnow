/**
 * Branch-free arithmetic loops shared by the int32, int64 (safe) and float kernels (arith.ts).
 *
 * Each loop writes `out[i] = a[i·as] op b[i·bs]` for the computed rows and returns the largest
 * |result| seen (NaN or Infinity when one occurred, since Math.max propagates them); it also
 * records whether a zero result was seen (`zeroSeen`, for float underflow). The callers compare
 * that one number against their range after the loop and recheck the batch row by row only when
 * it is suspicious, so the per-row work has no data-dependent branch. Dense shapes (every row,
 * column × constant, constant × column, column × column) get their own loops: no row indirection,
 * no stride multiply.
 *
 * Written from one template: keep the four shapes of each operator in step.
 */
import { IDENTITY } from "./vectors.js";

export const ADD = 0;
export const SUB = 1;
export const MUL = 2;
export const DIV = 3;

/**
 * Whether the last loop produced a zero (float underflow candidates): read by the caller right
 * after the loop, in the same synchronous call (R23).
 */
export let zeroSeen = false;

/** IEEE add/sub/mul/div in f64 (REAL rounds afterwards). */
export function floatLoop(
  op: number,
  a: ArrayLike<number>,
  as: number,
  b: ArrayLike<number>,
  bs: number,
  rows: Uint16Array,
  count: number,
  out: Float64Array | Float32Array,
): number {
  let m = 0;
  let z = 0;
  const dense = rows === IDENTITY;
  const q = b[0] ?? 0;
  const p = a[0] ?? 0;
  switch (op) {
    case ADD:
      if (dense && as === 1 && bs === 0) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) + q;
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 0 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = p + (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 1 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) + (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else {
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const v = (a[i * as] ?? 0) + (b[i * bs] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      }
      break;
    case SUB:
      if (dense && as === 1 && bs === 0) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) - q;
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 0 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = p - (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 1 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) - (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else {
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const v = (a[i * as] ?? 0) - (b[i * bs] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      }
      break;
    case MUL:
      if (dense && as === 1 && bs === 0) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) * q;
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 0 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = p * (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 1 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) * (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else {
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const v = (a[i * as] ?? 0) * (b[i * bs] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      }
      break;
    case DIV:
      if (dense && as === 1 && bs === 0) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) / q;
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 0 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = p / (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 1 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) / (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else {
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const v = (a[i * as] ?? 0) / (b[i * bs] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      }
      break;
  }
  zeroSeen = z !== 0;
  return m;
}

/** int32 add/sub/mul computed exactly in f64 (|result| < 2^62), stored wrapped; the caller checks the range from the returned maximum. */
export function int32Loop(
  op: number,
  a: ArrayLike<number>,
  as: number,
  b: ArrayLike<number>,
  bs: number,
  rows: Uint16Array,
  count: number,
  out: Int32Array,
): number {
  let m = 0;
  let z = 0;
  const dense = rows === IDENTITY;
  const q = b[0] ?? 0;
  const p = a[0] ?? 0;
  switch (op) {
    case ADD:
      if (dense && as === 1 && bs === 0) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) + q;
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 0 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = p + (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 1 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) + (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else {
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const v = (a[i * as] ?? 0) + (b[i * bs] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      }
      break;
    case SUB:
      if (dense && as === 1 && bs === 0) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) - q;
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 0 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = p - (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 1 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) - (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else {
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const v = (a[i * as] ?? 0) - (b[i * bs] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      }
      break;
    case MUL:
      if (dense && as === 1 && bs === 0) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) * q;
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 0 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = p * (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else if (dense && as === 1 && bs === 1) {
        for (let i = 0; i < count; i++) {
          const v = (a[i] ?? 0) * (b[i] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      } else {
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const v = (a[i * as] ?? 0) * (b[i * bs] ?? 0);
          out[i] = v;
          m = Math.max(m, Math.abs(v));
          z |= +(v === 0);
        }
      }
      break;
  }
  zeroSeen = z !== 0;
  return m;
}
