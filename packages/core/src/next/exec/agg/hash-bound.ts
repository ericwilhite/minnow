/** Complete hash workspace certificates. Unknown domains return no certificate;
 * the caller funds hybrid hash growth instead. Sketches and input prefixes never authorize a fixed workspace.
 */
import type { Rel } from "../../ir/ir.js";
import { layoutOf, type PhysicalType } from "../../types.js";
import { BATCH_ROWS } from "../../vector/vector.js";
import type { AggregateOptions } from "./aggregate.js";
import type { DictionaryProbeSource } from "./dictionary-probe.js";
import type { PartitionBounds } from "./partition-probe.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;
const isBytes = (type: PhysicalType) => ["text", "bytes", "enum", "uuid"].includes(type.kind);
const fixedNumber = (type: PhysicalType) =>
  ["bool", "i32", "int64", "f32", "f64"].includes(layoutOf(type));
const plain = (rel: AggregateRel, input: readonly PhysicalType[]): boolean =>
  rel.aggregates.every((call) => {
    if (call.distinct || call.filter !== null || call.orderBy.length !== 0) return false;
    if (call.fn === "count_star" && call.args.length === 0) return true;
    const arg = call.args[0],
      type = arg?.op === "column" ? input[arg.index] : undefined;
    if (call.args.length !== 1 || !type) return false;
    return (
      (call.fn === "count" && (fixedNumber(type) || isBytes(type))) ||
      (["sum", "avg", "min", "max", "bool_and", "bool_or"].includes(call.fn) &&
        fixedNumber(type)) ||
      (["min", "max"].includes(call.fn) && isBytes(type))
    );
  });

export function scalarHashMinimum(
  rel: AggregateRel,
  input: readonly PhysicalType[],
  compiler: number,
): number | null {
  if (rel.groupBy.length !== 0 || !plain(rel, input)) return null;
  let source = rel.input;
  while (source.op === "project" || source.op === "filter" || source.op === "limit")
    source = source.input;
  if (source.op !== "scan" && source.op !== "values") return null;
  // Byte MIN/MAX need a source-size certificate; keep this scalar path fixed-width only.
  if (
    rel.aggregates.some((call) => {
      const arg = call.args[0];
      return (
        (call.fn === "min" || call.fn === "max") &&
        arg?.op === "column" &&
        isBytes(input[arg.index] ?? arg.type)
      );
    })
  )
    return null;
  // One scalar slot per accumulator, its growth/output overlap and 128-bit exact integer sum.
  // Counts/physical table positions are safe JS integers; sum(int8) needs at most 116 bits.
  return compiler + 65536 + rel.aggregates.length * 8192;
}

export interface DictionaryHashShape {
  readonly scanId: number;
  readonly source: DictionaryProbeSource;
  /** Aggregate input column per bounded byte MIN/MAX payload. */
  readonly payloadInputs: readonly number[];
}
export function dictionaryHashShape(
  rel: AggregateRel,
  input: readonly PhysicalType[],
): DictionaryHashShape | null {
  if (
    rel.groupBy.length === 0 ||
    rel.groupBy.length > 8 ||
    !plain(rel, input) ||
    !rel.groupBy.every((i) => {
      const type = input[i];
      return type !== undefined && isBytes(type);
    })
  )
    return null;
  const payloadInputs = [
    ...new Set(
      rel.aggregates.flatMap((call) => {
        const arg = call.args[0];
        return (call.fn === "min" || call.fn === "max") &&
          arg?.op === "column" &&
          isBytes(input[arg.index] ?? arg.type)
          ? [arg.index]
          : [];
      }),
    ),
  ];
  let child = rel.input,
    indices = input.map((_, i) => i);
  while (child.op === "project") {
    const projected = child;
    const mapped: number[] = [];
    for (const index of indices) {
      const expr = projected.exprs[index];
      if (expr?.op !== "column") return null;
      mapped.push(expr.index);
    }
    indices = mapped;
    child = projected.input;
  }
  if (
    child.op !== "scan" ||
    child.filter !== null ||
    child.runtimeFilters.length ||
    child.emitLocator
  )
    return null;
  const scan = child;
  const columns = indices.map((i) => scan.columns[i]);
  const keys = rel.groupBy.map((i) => columns[i]),
    payloads = payloadInputs.map((i) => columns[i]);
  if (keys.some((i) => i === undefined) || payloads.some((i) => i === undefined)) return null;
  return {
    scanId: child.id,
    source: {
      tableId: child.tableId,
      keys: keys as number[],
      types: rel.groupBy.map((i) => {
        const type = input[i];
        if (!type) throw new Error("Missing certified key type");
        return type;
      }),
      payloads: payloads as number[],
    },
    payloadInputs,
  };
}

export function dictionaryHashMinimum(
  rel: AggregateRel,
  input: readonly PhysicalType[],
  shape: DictionaryHashShape,
  bounds: PartitionBounds,
  compiler: number,
  options: AggregateOptions,
): number | null {
  if (bounds.passes !== 1 || bounds.groups > 1024 || bounds.rows > 2 ** 21 || !plain(rel, input))
    return null;
  const groups = Math.max(1, bounds.groups),
    capacity = Math.max(64, 2 * groups);
  // <=1024 keys/column: initial 1024-entry arrays, hash-table resize overlap,
  // batch IDs, translations (<=2048 entries), masks and output offsets fit 128KiB.
  // ByteArena grows at most 4x: old+new+output <= 5x all certified key bytes.
  // Composite direct tables have at most 2^16 slots; both tables fit 512KiB.
  let bytes =
    compiler +
    512 * 1024 +
    rel.groupBy.length * 128 * 1024 +
    5 * bounds.keyBytes.reduce((a, b) => a + b, 0);
  // The packed record is 256 B/group; capacity <=2G and old/new overlap <=1.5C.
  // Ordinary promotion arrays coexist until every member restores its prefix.
  bytes += 384 * capacity;
  for (const call of rel.aggregates) {
    const arg = call.args[0],
      type = arg?.op === "column" ? input[arg.index] : undefined;
    if (type && isBytes(type) && (call.fn === "min" || call.fn === "max")) {
      const index = shape.payloadInputs.indexOf(arg?.op === "column" ? arg.index : -1),
        width = bounds.payloadBytes[index];
      if (width === undefined) return null;
      // Live bytes <=G*W. At growth, compact if dead>used/2, otherwise used<=2GW;
      // need<=3GW, new capacity<6GW and old<3GW. Compaction and emit are smaller.
      // Therefore 10GW+4096 covers every arena overlap. Group-indexed fields,
      // dictionary ranking, touched rows and output metadata are charged separately.
      bytes +=
        64 * capacity +
        10 * groups * width +
        4096 +
        32 * Math.max(BATCH_ROWS / 4, bounds.dictionaryEntries[rel.groupBy.length + index] ?? 0) +
        8 * BATCH_ROWS;
    } else {
      // <=33 B/slot for the widest ordinary exact state, <1.5C growth overlap,
      // plus 128-bit finalization, validity and output copies. 128C is conservative.
      bytes += 128 * capacity;
    }
  }
  bytes += 16 * (options.outputTopN?.count ?? 0);
  return Number.isSafeInteger(bytes) ? bytes : null;
}
