/** Discriminated semantic shapes at the durable catalog boundary (not just wire tags). */
import {
  physicalTypeOf,
  samePhysicalType,
  layoutOf,
  type PhysicalType,
  type SqlTypeDescriptor,
} from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { DURABLE_CODECS, requireTag } from "../durable-format.js";
import { StoreCorruptionError } from "../storage/contract.js";
function check(ok: boolean, message: string): asserts ok {
  if (!ok) throw new StoreCorruptionError(message);
}
function shape(value: object, required: readonly string[], optional: readonly string[] = []): void {
  check(
    required.every((k) => Object.hasOwn(value, k)),
    "Missing conditional catalog field",
  );
  check(
    Object.keys(value).every((k) => required.includes(k) || optional.includes(k)),
    "Unexpected conditional catalog field",
  );
}
export function validateCatalogType(value: unknown, depth = 0): void {
  check(depth <= 8 && typeof value === "object" && value !== null, "Invalid stored physical type");
  const type = value as PhysicalType;
  switch (type.kind) {
    case "array":
      shape(type, ["kind", "element"]);
      validateCatalogType(type.element, depth + 1);
      return;
    case "int32":
      shape(type, ["kind", "bits"]);
      requireTag(type.bits, DURABLE_CODECS.int32Bits, "stored integer width", StoreCorruptionError);
      return;
    case "json":
      shape(type, ["kind", "binary"]);
      check(typeof type.binary === "boolean", "Invalid stored JSON mode");
      return;
    case "timestamp":
      shape(type, ["kind", "zoned"]);
      check(typeof type.zoned === "boolean", "Invalid stored timestamp mode");
      return;
    case "decimal":
      shape(type, ["kind", "precision", "scale"]);
      check(
        type.precision === null ||
          (Number.isInteger(type.precision) && type.precision >= 1 && type.precision <= 1000),
        "Invalid stored decimal precision",
      );
      check(
        type.scale === null ||
          (Number.isInteger(type.scale) && type.scale >= -1000 && type.scale <= 1000),
        "Invalid stored decimal scale",
      );
      return;
    case "enum":
      shape(type, ["kind", "name", "labels"]);
      check(
        typeof type.name === "string" &&
          Array.isArray(type.labels) &&
          type.labels.length > 0 &&
          type.labels.every((l) => typeof l === "string") &&
          new Set(type.labels).size === type.labels.length,
        "Invalid stored enum labels",
      );
      return;
    case "text":
      shape(type, ["kind"], ["maxLength", "fixed"]);
      check(
        type.maxLength === undefined ||
          (Number.isSafeInteger(type.maxLength) && type.maxLength >= 1),
        "Invalid stored text width",
      );
      if (type.fixed !== undefined) {
        requireTag(
          type.fixed,
          DURABLE_CODECS.presentFlag,
          "stored fixed text flag",
          StoreCorruptionError,
        );
        check(type.maxLength !== undefined, "Fixed text requires its width");
      }
      return;
    case "bool":
    case "int64":
    case "float32":
    case "float64":
    case "date":
    case "time":
    case "interval":
    case "bytes":
    case "uuid":
      shape(type, ["kind"]);
      return;
    default:
      throw new StoreCorruptionError("Invalid stored physical type");
  }
}
export function validateCatalogDeclared(declared: SqlTypeDescriptor, physical: PhysicalType): void {
  shape(
    declared,
    ["type"],
    ["integer", "sqlDomain", "withoutTimeZone", "enumValues", "declaredTypeName", "declaredLength"],
  );
  check(
    declared.integer === undefined || declared.type === "number",
    "Integer flag on a nonnumeric declaration",
  );
  check(
    declared.withoutTimeZone === undefined || declared.type === "datetime",
    "Timezone flag on a non-temporal declaration",
  );
  check(
    declared.enumValues === undefined || declared.type === "string",
    "Enum labels on a nontext declaration",
  );
  check(
    declared.declaredLength === undefined || declared.type === "string",
    "Text width on a nontext declaration",
  );
  const domain = declared.sqlDomain;
  if (domain) {
    if (domain.kind === "numeric") shape(domain, ["kind"], ["precision", "scale"]);
    else if (domain.kind === "enum") shape(domain, ["kind", "name", "values"]);
    else if (domain.kind === "array") shape(domain, ["kind", "element"]);
    else shape(domain, ["kind"]);
  }
  if (physical.kind === "bytes")
    check(
      declared.sqlDomain === undefined &&
        declared.declaredLength === undefined &&
        declared.enumValues === undefined,
      "A native byte descriptor has conflicting text/domain fields",
    );
  check(
    samePhysicalType(physicalTypeOf(declared), physical),
    "Declared and physical catalog types differ",
  );
}
function integerData(
  value: { repr: string; values?: unknown; limbs?: unknown },
  length: number,
  wide: boolean,
  active: boolean,
): void {
  if (value.repr === "int128") {
    shape(value, ["repr", "limbs"]);
    check(
      wide && value.limbs instanceof Uint32Array && value.limbs.length === length * 4,
      "Invalid stored int128 shape",
    );
  } else {
    shape(value, ["repr", "values"]);
    check(
      value.repr === "safe"
        ? value.values instanceof Float64Array
        : value.repr === "int64"
          ? value.values instanceof BigInt64Array
          : wide &&
            value.repr === "wide" &&
            Array.isArray(value.values) &&
            value.values.every((v) => typeof v === "bigint"),
      "Invalid stored integer representation",
    );
    check(
      (value.values as { length: number }).length === length,
      "Invalid stored integer vector length",
    );
    // Only valid slots are values. In particular, NULL may retain NaN or any raw float word.
    if (active && value.repr === "safe")
      check(
        (value.values as Float64Array).every((v) => Number.isSafeInteger(v) && !Object.is(v, -0)),
        "Invalid stored safe integer value",
      );
  }
}
export function validateCatalogMissing(vector: FlatVector, type: PhysicalType): void {
  shape(vector, ["form", "type", "length", "validity", "data"]);
  requireTag(
    vector.form,
    DURABLE_CODECS.vectorForm,
    "stored missing-vector form",
    StoreCorruptionError,
  );
  check(vector.length === 1, "Invalid stored missing-vector length");
  validateCatalogType(vector.type);
  check(samePhysicalType(vector.type, type), "Stored missing-vector type differs");
  check(
    vector.validity === null ||
      (vector.validity instanceof Uint32Array && vector.validity.length === 1),
    "Invalid stored missing-vector validity",
  );
  const n = vector.length,
    data = vector.data,
    active = vector.validity === null || ((vector.validity[0] ?? 0) & 1) !== 0;
  check(data.layout === layoutOf(type), "Stored missing-vector layout differs");
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64": {
      shape(data, ["layout", "values"]);
      const Type =
        data.layout === "bool"
          ? Uint8Array
          : data.layout === "i32"
            ? Int32Array
            : data.layout === "f32"
              ? Float32Array
              : Float64Array;
      check(
        data.values instanceof Type && data.values.length === n,
        "Invalid stored missing-vector data",
      );
      break;
    }
    case "int64":
      shape(data, ["layout", "values"]);
      integerData(data.values, n, false, active);
      break;
    case "fixed16":
      shape(data, ["layout", "bytes"]);
      check(
        data.bytes instanceof Uint8Array && data.bytes.length === n * 16,
        "Invalid stored fixed16 shape",
      );
      break;
    case "varlen":
      shape(data, ["layout", "offsets", "bytes"]);
      check(
        data.offsets instanceof Int32Array &&
          data.offsets.length === n + 1 &&
          data.bytes instanceof Uint8Array &&
          data.offsets[0] === 0 &&
          data.offsets[n] === data.bytes.length,
        "Invalid stored varlen shape",
      );
      // A missing value has exactly one slot; offsets[0]=0 and offsets[1]=bytes.length above
      // already prove its only range is monotonic and within the owned byte buffer.
      break;
    case "decimal":
      shape(data, ["layout", "coefficients", "scale", "scales", "specials"]);
      integerData(data.coefficients, n, true, active);
      check(
        data.scale === null
          ? data.scales instanceof Uint16Array &&
              data.scales.length === n &&
              (!active || data.scales.every((scale) => scale <= 16383))
          : Number.isInteger(data.scale) &&
              data.scale >= 0 &&
              data.scale <= 16383 &&
              data.scales === null,
        "Invalid stored decimal scales",
      );
      check(
        data.specials === null ||
          (data.specials instanceof Uint8Array &&
            data.specials.length === n &&
            (!active || data.specials.every((s) => s <= 3))),
        "Invalid stored decimal specials",
      );
      break;
    case "interval":
      shape(data, ["layout", "months", "days", "micros"]);
      check(
        data.months instanceof Int32Array &&
          data.months.length === n &&
          data.days instanceof Int32Array &&
          data.days.length === n,
        "Invalid stored interval shape",
      );
      integerData(data.micros, n, false, active);
      break;
  }
}
