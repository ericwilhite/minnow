/** Execution-owned parameter vectors. Binding uses the accepted forms on executor.ts. */
import type { PhysicalPlan } from "../ir/ir.js";
import {
  INT16_MIN,
  INT16_MAX,
  INT32_MIN,
  INT32_MAX,
  INT64_MIN,
  INT64_MAX,
  type PhysicalType,
} from "../types.js";
import {
  TIMESTAMP_MIN,
  TIMESTAMP_END,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  parseDate,
  parseTimestamp,
  timestampFromDate,
} from "../temporal.js";
import { allocateFlat, dataBytes, type FlatVector, type VectorData } from "../vector/vector.js";
import type { BoundParameters, ParameterValue } from "./executor.js";
import { ParameterBindError, UnsupportedPlanError } from "./errors.js";
import { compileCast } from "./kernels/cast.js";
import { flatOf, OutputArena } from "./kernels/vectors.js";
import { numericFits } from "../vector/array-value.js";
import type { ByteCharge } from "../vector/varlen.js";
import { bigintBytes, MemoryAdmissionError } from "../memory/governor.js";
import { Slice, runSliced, runToEnd, type SlicedJob } from "./slices.js";
import { YIELD_INTERVAL_MS } from "./pipeline.js";
import type { MaybePromise } from "../access/key-index.js";

/** A conservative bound includes vector wrappers, cast buffers and numeric temporaries.
 * The charge survives binding: cast output can alias the arena, and source BigInts can be
 * adopted by the result. It is released with the execution's atomic fixed reservation. */
const SLOT_BYTES = 512;
/** Bounded NUMERIC probes truncate to at most 491521 bits, then inspect bounded strings.
 * 512 KiB covers that truncated BigInt, a negative magnitude, hex/decimal strings and overlap.
 * It is charged temporarily and returned before waiting for the atomic input reservation. */
export const PARAMETER_SIZING_BYTES = 512 * 1024;
export function needsParameterSizing(
  plan: PhysicalPlan,
  values: readonly ParameterValue[],
): boolean {
  return plan.parameters.some((type, slot) => {
    const value = values[slot];
    return (
      type.kind === "decimal" &&
      typeof value === "bigint" &&
      (value < INT64_MIN || value > INT64_MAX)
    );
  });
}

function numericBytes(type: PhysicalType, value: ParameterValue, scratch?: ByteCharge): number {
  const scale = type.kind === "decimal" ? Math.abs(type.scale ?? 0) : 0;
  if (typeof value === "bigint") {
    if (value >= INT64_MIN && value <= INT64_MAX) return SLOT_BYTES + 8 * 24 + 8 * scale;
    if (!scratch?.allocate(PARAMETER_SIZING_BYTES))
      throw new MemoryAdmissionError("parameter sizing", PARAMETER_SIZING_BYTES, 0);
    try {
      return numericFits(value, 0)
        ? SLOT_BYTES + 8 * bigintBytes(value) + 8 * scale
        : SLOT_BYTES + PARAMETER_SIZING_BYTES;
    } finally {
      scratch.free(PARAMETER_SIZING_BYTES);
    }
  }
  if (typeof value !== "string") return SLOT_BYTES + 1024 + 8 * scale;
  // No substrings, coefficient, or power of ten before admission. A positive exponent
  // can expand a short input up to NUMERIC's 131072 integral digits. Saturation is enough
  // for this upper bound; materialization still applies the original grammar/range checks.
  const at = Math.max(value.lastIndexOf("e"), value.lastIndexOf("E"));
  let exponent = 0,
    sign = 1,
    i = at + 1;
  if (at >= 0) {
    if (value.charCodeAt(i) === 45) {
      sign = -1;
      i++;
    } else if (value.charCodeAt(i) === 43) i++;
    if (value.length - i > 6) exponent = 131073;
    for (; i < value.length && exponent < 131073; i++) {
      const digit = value.charCodeAt(i) - 48;
      if (digit < 0 || digit > 9) break;
      exponent = Math.min(131073, exponent * 10 + digit);
    }
  }
  // 16 bytes per input code unit bounds regex/trim/digit strings and source, result,
  // power and product BigInts (log2(10)<4). 8 per expanded decimal digit also bounds
  // typmod rounding's overlap. No decimal coefficient can exceed PG's digit limits.
  return SLOT_BYTES + 16 * value.length + 8 * (Math.max(0, sign * exponent) + scale);
}

function* sizeOf(
  type: PhysicalType,
  value: ParameterValue,
  slice: Slice,
  scratch?: ByteCharge,
): SlicedJob<number> {
  if (value === null) return SLOT_BYTES + dataBytes(type, 1) + 4;
  if (type.kind === "decimal") return numericBytes(type, value, scratch);
  if (type.kind === "bytes" && value instanceof Uint8Array) return SLOT_BYTES + value.byteLength;
  if (type.kind !== "text" || typeof value !== "string")
    return SLOT_BYTES + (typeof value === "string" ? 8 * value.length : 0);
  let bytes = 0,
    nextCheck = 8192;
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c < 128) bytes++;
    else if (c < 2048) bytes += 2;
    else if (
      c >= 0xd800 &&
      c <= 0xdbff &&
      value.charCodeAt(i + 1) >= 0xdc00 &&
      value.charCodeAt(i + 1) <= 0xdfff
    ) {
      bytes += 4;
      i++;
    } else bytes += 3; // Includes replacement characters for lone surrogates, as TextEncoder.
    if (i >= nextCheck) {
      nextCheck = i + 8192;
      if (slice.due()) yield;
    }
  }
  return SLOT_BYTES + bytes + 2 * Math.min(value.length, 8192) + 24;
}

/** All slot and scratch bytes are charged before any owned buffer is materialized. */
function prepaid(charge: ByteCharge, bytes: number): ByteCharge {
  if (!charge.allocate(bytes)) throw new MemoryAdmissionError("parameters", bytes, 0);
  let used = 0;
  return {
    allocate(n) {
      if (used + n > bytes) throw new Error("parameter scratch bound exceeded");
      used += n;
      return true;
    },
    free(n) {
      used -= n;
    },
  };
}

export interface ParameterBinding {
  readonly bytes: number;
  bind(charge: ByteCharge, statementTime?: bigint): MaybePromise<BoundParameters>;
}

/** Only scans caller-owned inputs; does not create encoded copies or numeric scratch. */
export function prepareParameters(
  plan: PhysicalPlan,
  values: readonly ParameterValue[],
  signal?: AbortSignal,
  capacity = Infinity,
  scratch?: ByteCharge,
): MaybePromise<ParameterBinding> {
  if (values.length !== plan.placeholderCount)
    throw new ParameterBindError(
      `Expected ${String(plan.placeholderCount)} parameters, received ${String(values.length)}`,
      null,
    );
  const slice = new Slice(YIELD_INTERVAL_MS);
  function* size(): SlicedJob<ParameterBinding> {
    let bytes = 0;
    const sizes: number[] = [];
    for (let slot = 0; slot < plan.parameters.length; slot++) {
      const type = plan.parameters[slot];
      if (type === undefined) throw new TypeError("missing parameter type");
      const value = slot < plan.placeholderCount ? values[slot] : null;
      if (value === undefined)
        throw new ParameterBindError(
          `$${String(slot + 1)}: undefined is not a parameter value`,
          slot,
        );
      const length =
        typeof value === "string"
          ? value.length
          : value instanceof Uint8Array
            ? value.byteLength
            : 0;
      if (bytes + length > capacity)
        throw new MemoryAdmissionError("parameters", bytes + length, capacity);
      const slotBytes = yield* sizeOf(type, value, slice, scratch);
      sizes.push(slotBytes);
      bytes += slotBytes;
      if (bytes > capacity) throw new MemoryAdmissionError("parameters", bytes, capacity);
      if (!Number.isSafeInteger(bytes)) throw new MemoryAdmissionError("parameters", bytes, 0);
    }
    return {
      bytes,
      bind(charge, statementTime = 0n) {
        const pool = prepaid(charge, bytes);
        function* convert(): SlicedJob<BoundParameters> {
          const slots: FlatVector[] = [];
          for (let slot = 0; slot < plan.parameters.length; slot++) {
            const type = plan.parameters[slot];
            if (type === undefined) throw new TypeError("missing parameter type");
            if (slot === plan.clockSlot) {
              slots.push(clockParameter(type, statementTime));
              continue;
            }
            const value = slot < plan.placeholderCount ? values[slot] : null;
            if (value === undefined) throw new ParameterBindError("missing parameter", slot);
            if (type.kind === "text" && typeof value === "string") {
              if (value.includes("\0"))
                throw new ParameterBindError(
                  `$${String(slot + 1)}: text cannot contain a zero byte`,
                  slot,
                );
              const length =
                (sizes[slot] ?? 0) - SLOT_BYTES - 2 * Math.min(value.length, 8192) - 24;
              const bytes = new Uint8Array(length),
                encoder = new TextEncoder();
              let written = 0;
              for (let at = 0; at < value.length;) {
                let end = Math.min(value.length, at + 8192);
                const c = value.charCodeAt(end - 1),
                  next = value.charCodeAt(end);
                if (
                  end < value.length &&
                  c >= 0xd800 &&
                  c <= 0xdbff &&
                  next >= 0xdc00 &&
                  next <= 0xdfff
                )
                  end--;
                const result = encoder.encodeInto(value.slice(at, end), bytes.subarray(written));
                if (result.read !== end - at)
                  throw new Error("UTF-8 parameter size changed during binding");
                written += result.written;
                at = end;
                signal?.throwIfAborted();
                if (slice.due()) yield;
              }
              slots.push(
                vector(type, { layout: "varlen", bytes, offsets: Int32Array.of(0, length) }),
              );
            } else if (type.kind === "bytes" && value instanceof Uint8Array) {
              const bytes = new Uint8Array(value.byteLength);
              for (let at = 0; at < value.byteLength; at += 8192) {
                bytes.set(value.subarray(at, Math.min(value.byteLength, at + 8192)), at);
                signal?.throwIfAborted();
                if (slice.due()) yield;
              }
              slots.push(
                vector(type, { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) }),
              );
            } else slots.push(materialize(type, value, slot, pool));
            signal?.throwIfAborted();
            if (slice.due()) yield;
          }
          return { slots };
        }
        return runSliced(convert(), slice, () => signal?.throwIfAborted());
      },
    };
  }
  signal?.throwIfAborted();
  return runSliced(size(), slice, () => signal?.throwIfAborted());
}

export function bindParameters(
  plan: PhysicalPlan,
  values: readonly ParameterValue[],
  charge: ByteCharge,
): MaybePromise<BoundParameters> {
  const binding = prepareParameters(plan, values, undefined, Infinity, charge);
  return binding instanceof Promise ? binding.then((b) => b.bind(charge)) : binding.bind(charge);
}

/** Temporary input cells also require a charge; their caller frees it after copying the cell. */
export function bindParameter(
  type: PhysicalType,
  value: ParameterValue,
  slot: number,
  charge: ByteCharge,
): FlatVector {
  const bytes = runToEnd(sizeOf(type, value, new Slice(Infinity), charge));
  return materialize(type, value, slot, prepaid(charge, bytes));
}
const TEXT: PhysicalType = { kind: "text" };
const NUMERIC: PhysicalType = { kind: "decimal", precision: null, scale: null };

/** Refuse unsupported binding types at prepare, before admitting or touching a snapshot. */
export function checkParameterType(type: PhysicalType): void {
  if (
    [
      "bool",
      "int32",
      "int64",
      "float32",
      "float64",
      "decimal",
      "date",
      "timestamp",
      "bytes",
    ].includes(type.kind)
  )
    return;
  if (type.kind === "text" && type.maxLength === undefined) return;
  throw new UnsupportedPlanError(`a ${type.kind} parameter`, { construct: "parameter" });
}

function vector(type: PhysicalType, data: VectorData): FlatVector {
  return { form: "flat", type, length: 1, validity: null, data };
}

export function nullParameter(type: PhysicalType): FlatVector {
  return { ...allocateFlat(type, 1), length: 1, validity: new Uint32Array(1) };
}

/** Statement clocks are stored microseconds, not client timestamp values. */
export function clockParameter(type: PhysicalType, micros: bigint): FlatVector {
  return vector(type, {
    layout: "int64",
    values: { repr: "int64", values: BigInt64Array.of(micros) },
  });
}

function text(value: string, type: PhysicalType = TEXT): FlatVector {
  if (value.includes("\0")) throw new RangeError("text cannot contain a zero byte");
  const bytes = new TextEncoder().encode(value);
  return vector(type, { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) });
}

function numeric(value: string): FlatVector {
  const source = value.trim();
  const special = /^(?:NaN|[+-]?Infinity)$/iu.exec(source);
  if (special !== null)
    return vector(NUMERIC, {
      layout: "decimal",
      scale: 0,
      scales: null,
      coefficients: { repr: "safe", values: Float64Array.of(0) },
      specials: Uint8Array.of(/^nan$/iu.test(source) ? 1 : source.startsWith("-") ? 3 : 2),
    });
  const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:e([+-]?\d+))?$/iu.exec(source);
  if (match === null || ((match[2] ?? "") === "" && (match[3] ?? "") === ""))
    throw new RangeError("numeric text expected");
  const fraction = match[3] ?? "";
  const digits = ((match[2] ?? "") + fraction).replace(/^0+(?=\d)/u, "");
  const exponent = Number(match[4] ?? 0);
  let scale = fraction.length - exponent;
  // PostgreSQL NUMERIC's representable digit/exponent limits. Refuse before constructing a
  // giant coefficient or power of ten; the typmod cast below applies rounding and precision.
  if (!Number.isSafeInteger(exponent) || scale > 16_383 || digits.length - scale > 131_072)
    throw new RangeError("numeric input is out of range");
  let coefficient = BigInt((match[1] === "-" ? "-" : "") + digits);
  if (scale < 0) {
    coefficient *= 10n ** BigInt(-scale);
    scale = 0;
  }
  return vector(NUMERIC, {
    layout: "decimal",
    scale,
    scales: null,
    coefficients: { repr: "wide", values: [coefficient] },
    specials: null,
  });
}

function timestamp(value: string | Date, zoned: boolean): bigint {
  if (value instanceof Date) {
    const micros = timestampFromDate(value);
    if (micros < TIMESTAMP_MIN || micros >= TIMESTAMP_END)
      throw new RangeError("timestamp is out of range");
    return micros;
  }
  const source = value.trim();
  if (/^\+?infinity$/iu.test(source)) return TIMESTAMP_POS_INFINITY;
  if (/^-infinity$/iu.test(source)) return TIMESTAMP_NEG_INFINITY;
  let micros = parseTimestamp(source);
  if (micros === null) {
    // A date alone supplies midnight. Other offsets are normalized exactly in integer µs;
    // timestamp without time zone discards the written offset, as PostgreSQL does.
    const match =
      /^([+-]?\d{1,6})-(\d{2})-(\d{2})(?:[T ](\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?))?(Z|[+-]\d{2}(?::?\d{2})?)?(?:\s+(BC))?$/iu.exec(
        source,
      );
    if (match === null) throw new RangeError("timestamp text expected");
    const year = match[6] === undefined ? Number(match[1]) : 1 - Number(match[1]);
    const isoYear =
      year >= 0 && year <= 9999
        ? String(year).padStart(4, "0")
        : `${year < 0 ? "-" : "+"}${String(Math.abs(year)).padStart(6, "0")}`;
    micros = parseTimestamp(
      `${isoYear}-${String(match[2])}-${String(match[3])}T${match[4] ?? "00:00:00"}Z`,
    );
    if (micros === null) throw new RangeError("timestamp is out of range");
    const zone = match[5] ?? "Z";
    if (zone.toUpperCase() !== "Z") {
      const digits = zone.slice(1).replace(":", "");
      const hours = Number(digits.slice(0, 2)),
        minutes = Number(digits.slice(2) || 0);
      if (hours > 15 || minutes > 59) throw new RangeError("timestamp offset is out of range");
      if (zoned)
        micros -= BigInt((zone.startsWith("-") ? -1 : 1) * (hours * 60 + minutes)) * 60_000_000n;
    }
  }
  if (
    micros !== TIMESTAMP_NEG_INFINITY &&
    micros !== TIMESTAMP_POS_INFINITY &&
    (micros < TIMESTAMP_MIN || micros >= TIMESTAMP_END)
  )
    throw new RangeError("timestamp is out of range");
  return micros;
}

function materialize(
  type: PhysicalType,
  value: ParameterValue,
  slot: number,
  charge: ByteCharge,
): FlatVector {
  checkParameterType(type);
  try {
    if (value === null) return nullParameter(type);
    const invalid = (): never => {
      throw new RangeError(`${type.kind} value expected`);
    };
    switch (type.kind) {
      case "bool":
        return typeof value === "boolean"
          ? vector(type, { layout: "bool", values: Uint8Array.of(value ? 1 : 0) })
          : invalid();
      case "int32":
      case "int64": {
        const integer =
          typeof value === "bigint"
            ? value
            : typeof value === "number" && Number.isSafeInteger(value)
              ? BigInt(value)
              : invalid();
        const min =
          type.kind === "int64" ? INT64_MIN : BigInt(type.bits === 16 ? INT16_MIN : INT32_MIN);
        const max =
          type.kind === "int64" ? INT64_MAX : BigInt(type.bits === 16 ? INT16_MAX : INT32_MAX);
        if (integer < min || integer > max) return invalid();
        return type.kind === "int64"
          ? clockParameter(type, integer)
          : vector(type, { layout: "i32", values: Int32Array.of(Number(integer)) });
      }
      case "float64":
      case "float32": {
        if (typeof value !== "number") return invalid();
        const source = vector(
          { kind: "float64" },
          { layout: "f64", values: Float64Array.of(value) },
        );
        return cast(type, source, charge);
      }
      case "decimal": {
        if (
          typeof value !== "string" &&
          typeof value !== "bigint" &&
          (typeof value !== "number" || !Number.isFinite(value))
        )
          return invalid();
        if (typeof value === "bigint" && !numericFits(value, 0))
          throw new RangeError("numeric input is out of range");
        const source =
          typeof value === "bigint"
            ? vector(NUMERIC, {
                layout: "decimal",
                scale: 0,
                scales: null,
                coefficients: { repr: "wide", values: [value] },
                specials: null,
              })
            : numeric(String(value));
        return cast(type, source, charge);
      }
      case "date":
        return typeof value === "string"
          ? vector(type, { layout: "i32", values: Int32Array.of(parseDate(value)) })
          : invalid();
      case "timestamp":
        return typeof value === "string" || value instanceof Date
          ? clockParameter(type, timestamp(value, type.zoned))
          : invalid();
      case "text":
        return typeof value === "string" ? text(value, type) : invalid();
      case "bytes": {
        if (!(value instanceof Uint8Array)) return invalid();
        const bytes = new Uint8Array(value);
        return vector(type, { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) });
      }
      default:
        return invalid();
    }
  } catch (error) {
    throw new ParameterBindError(
      `$${String(slot + 1)}: ${error instanceof Error ? error.message : String(error)}`,
      slot,
    );
  }
}

function cast(type: PhysicalType, source: FlatVector, charge: ByteCharge): FlatVector {
  const arena = new OutputArena(charge, 1);
  try {
    const compiled = compileCast(type, { type: source.type, evaluate: () => source }, arena);
    return flatOf(compiled.evaluate({ length: 1, columns: [], selection: null }, null));
  } finally {
    arena.close();
  }
}
