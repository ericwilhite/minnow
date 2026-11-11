/** PG numeric JSONB output: exact decimal scale, no float conversion or expanded string. */
import type { JsonBytesSink } from "./json-scan.js";
import type { Slice, SlicedJob } from "../slices.js";
import { ExecutionError } from "../errors.js";
function overflow(): never {
  throw new ExecutionError("value overflows numeric format", "MINNOW_JSONB_NUMERIC", "22003");
}
/** The scanner has validated JSON number grammar. Caller owns/charges the output sink. */
export function* jsonbNumber(
  bytes: Uint8Array,
  start: number,
  end: number,
  sink: JsonBytesSink,
  slice: Slice,
): SlicedJob<void> {
  const negative = bytes[start] === 45;
  const from = start + (negative ? 1 : 0);
  let dot = -1,
    exponentAt = end;
  for (let i = from; i < end; i++) {
    const c = bytes[i];
    if (c === 46) dot = i;
    if (c === 101 || c === 69) {
      exponentAt = i;
      break;
    }
    if ((i & 1023) === 0 && slice.due()) yield;
  }
  let exponent = 0;
  if (exponentAt < end) {
    let at = exponentAt + 1;
    const minus = bytes[at] === 45;
    if (minus || bytes[at] === 43) at++;
    for (; at < end; at++) {
      exponent = exponent * 10 + (bytes[at] ?? 48) - 48;
      if (exponent > 1073741823) overflow();
      if ((at & 1023) === 0 && slice.due()) yield;
    }
    if (minus) exponent = -exponent;
  }
  const integral = (dot < 0 ? exponentAt : dot) - from;
  const digits = exponentAt - from - (dot < 0 ? 0 : 1);
  const scale = Math.max(0, digits - integral - exponent);
  const position = integral + exponent;
  let first = digits;
  for (let i = 0; i < digits; i++) {
    if (bytes[from + i + (dot >= 0 && i >= integral ? 1 : 0)] !== 48) {
      first = i;
      break;
    }
    if ((i & 1023) === 0 && slice.due()) yield;
  }
  // Numeric's display scale is bounded independently of its significant coefficient.
  if (scale > 16383 || (first < digits && position - first > 131072)) overflow();
  if (negative && first < digits) sink.put(45);
  let work = 0;
  const digitAt = (i: number): number =>
    i < 0 || i >= digits ? 48 : (bytes[from + i + (dot >= 0 && i >= integral ? 1 : 0)] ?? 48);
  if (first === digits || position <= first) sink.put(48);
  else
    for (let i = first; i < position; i++) {
      sink.put(digitAt(i));
      if ((++work & 1023) === 0 && slice.due()) yield;
    }
  if (scale > 0) {
    sink.put(46);
    for (let i = position; i < position + scale; i++) {
      sink.put(first === digits ? 48 : digitAt(i));
      if ((++work & 1023) === 0 && slice.due()) yield;
    }
  }
}
