/**
 * What a full export of the wide table reads: encoded bytes per row at W1M (200 columns), by
 * column family and by column, and the size two codec candidates would save, measured on the
 * same values with the writer's own page boundaries. Nothing here changes the stored format.
 *
 *   npx vite-node scripts/export-size.mts [--groups 4]
 *
 * `--groups` groups of 65,536 rows of the 1,048,576-row table (the generator is per row, so a
 * prefix has the table's mix) are written twice into the in-memory space: as a flush writes them,
 * and with `gzip` (maintenance compaction, page/gzip.ts). Per column: data + directory bytes and
 * the page encodings. Candidates, per page:
 * - decimal floats: a float64 page whose every value is an integer over 10^e (e ≤ 6) stored as
 *   frame-of-reference integers at the writer's byte widths (u8/u16/u32) instead of 8 bytes;
 * - bit-packed frame of reference: integer pages (int32, int64, date, timestamp, decimal) at
 *   ⌈log2(range + 1)⌉ bits instead of the next byte width (frame of reference or delta, whichever
 *   is smaller, as the writer chooses).
 * Both estimates count value bytes only (headers and validity are the same either way).
 * Last, the export stream (scripts/browser-perf/page/export-stream.ts) runs from memory over both
 * copies: the decode half's rows/s as flushed and with the text pages inflating.
 */
import { BlobCache } from "../packages/core/src/next/format/blob-cache.ts";
import { PageEncoding, type ColumnChunk } from "../packages/core/src/next/format/manifest.ts";
import type { ChunkDirectory } from "../packages/core/src/next/page/directory.ts";
import { MemoryExtentSpace, UNLIMITED } from "../packages/core/src/next/page/memory-extents.ts";
import { ChunkReader } from "../packages/core/src/next/page/reader.ts";
import { GroupWriter } from "../packages/core/src/next/page/writer.ts";
import type { FlatVector } from "../packages/core/src/next/vector/vector.ts";
import { exportStream, type ExportGroup } from "./browser-perf/page/export-stream.ts";
import { wideSpec, type WideColumn } from "./browser-perf/page/wide-data.ts";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.ts";

const args = process.argv.slice(2);
const at = args.indexOf("--groups");
const GROUPS = at >= 0 ? Number(args[at + 1]) : 4;
const ROWS = 1_048_576;
const GROUP = 65_536;
const spec = wideSpec(200);
const types = spec.map((column) => typeOf(column));

interface ColumnSize {
  readonly column: WideColumn;
  stored: number;
  gzipped: number;
  encodings: Map<string, number>;
  /** Value bytes now and under each candidate (only pages the candidate applies to). */
  floatNow: number;
  floatCandidate: number;
  packNow: number;
  packCandidate: number;
}

const sizes: ColumnSize[] = spec.map((column) => ({
  column,
  stored: 0,
  gzipped: 0,
  encodings: new Map(),
  floatNow: 0,
  floatCandidate: 0,
  packNow: 0,
  packCandidate: 0,
}));

const ENCODING_NAMES = new Map<number, string>(
  Object.entries(PageEncoding)
    .filter(([name]) => name !== "gzip")
    .map(([name, value]) => [value, name]),
);
const encodingName = (encoding: number): string =>
  `${ENCODING_NAMES.get(encoding & 0x7f) ?? String(encoding)}${encoding & PageEncoding.gzip ? "+gzip" : ""}`;

const byteWidth = (bits: number): number =>
  bits === 0 ? 0 : bits <= 8 ? 1 : bits <= 16 ? 2 : bits <= 32 ? 4 : 8;
const bitsOf = (range: number): number => (range <= 0 ? 0 : Math.ceil(Math.log2(range + 1)));

/** The numeric values of rows [from, to) of `vector`, valid rows only. */
function numbers(vector: FlatVector, from: number, to: number): number[] {
  const out: number[] = [];
  const data = vector.data;
  const valid = (row: number): boolean =>
    vector.validity === null || (((vector.validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 1;
  for (let row = from; row < to; row++) {
    if (!valid(row)) continue;
    switch (data.layout) {
      case "i32":
      case "f64":
        out.push(data.values[row] ?? 0);
        break;
      case "int64":
        out.push(Number(data.values.values[row] ?? 0));
        break;
      case "decimal": {
        // The generator writes safe coefficients (scripts/browser-perf/page/wide-vectors.ts).
        const coefficients = data.coefficients;
        if (coefficients.repr !== "safe") return [];
        out.push(coefficients.values[row] ?? 0);
        break;
      }
      default:
        return [];
    }
  }
  return out;
}

/** Bits per value of the cheaper of frame of reference and delta over `values`. */
function packedBits(values: readonly number[]): number {
  if (values.length === 0) return 0;
  let min = Infinity;
  let max = -Infinity;
  let dMin = Infinity;
  let dMax = -Infinity;
  for (let k = 0; k < values.length; k++) {
    const value = values[k] ?? 0;
    min = Math.min(min, value);
    max = Math.max(max, value);
    if (k > 0) {
      const delta = value - (values[k - 1] ?? 0);
      dMin = Math.min(dMin, delta);
      dMax = Math.max(dMax, delta);
    }
  }
  const forBits = bitsOf(max - min);
  return values.length > 1 ? Math.min(forBits, bitsOf(dMax - dMin)) : forBits;
}

/** The smallest e ≤ 6 with every value an integer over 10^e, or null. */
function decimalExponent(values: readonly number[]): number | null {
  for (let e = 0; e <= 6; e++) {
    const scale = 10 ** e;
    if (
      values.every(
        (value) =>
          Number.isSafeInteger(Math.round(value * scale)) &&
          Math.round(value * scale) / scale === value,
      )
    )
      return e;
  }
  return null;
}

const plainSpace = new MemoryExtentSpace();
const gzipSpace = new MemoryExtentSpace();
const plainGroups: ExportGroup[] = [];
const gzipGroups: ExportGroup[] = [];

async function write(vectors: FlatVector[], rows: number, gzip: boolean) {
  const space = gzip ? gzipSpace : plainSpace;
  const writer = new GroupWriter({
    columns: spec.map((_, k) => ({ columnId: k + 1, type: types[k] ?? { kind: "int64" } })),
    writer: space.beginWrite("size"),
    gzip,
  });
  await writer.append(vectors, rows);
  return { space, chunks: (await writer.finish()).columns };
}

const directories = new BlobCache<ChunkDirectory>(() => ({
  charge: () => true,
  release: () => undefined,
}));
for (let group = 0; group < GROUPS; group++) {
  const first = group * GROUP;
  const vectors = spec.map((column, k) =>
    vectorOf(column, types[k] ?? { kind: "int64" }, first, GROUP, ROWS),
  );
  const plain = await write(vectors, GROUP, false);
  const gzipped = await write(vectors, GROUP, true);
  plainGroups.push({ rows: GROUP, chunks: plain.chunks });
  gzipGroups.push({ rows: GROUP, chunks: gzipped.chunks });
  for (const [k, chunk] of plain.chunks.entries()) {
    const size = sizes[k];
    const vector = vectors[k];
    if (size === undefined || vector === undefined) continue;
    size.stored += chunk.data.length + chunk.directory.length;
    const other: ColumnChunk | undefined = gzipped.chunks[k];
    size.gzipped += (other?.data.length ?? 0) + (other?.directory.length ?? 0);
    const reader = await ChunkReader.open(plain.space, chunk, { budget: UNLIMITED, directories });
    for (const page of reader.directory.pages) {
      const name = encodingName(page.encoding);
      size.encodings.set(name, (size.encodings.get(name) ?? 0) + page.length);
      const values = numbers(vector, page.firstRow, page.firstRow + page.rowCount);
      if (values.length === 0) continue;
      const kind = chunk.type.kind;
      if (kind === "float64") {
        size.floatNow += values.length * 8;
        const e = decimalExponent(values);
        size.floatCandidate +=
          e === null
            ? values.length * 8
            : values.length *
              byteWidth(packedBits(values.map((value) => Math.round(value * 10 ** e))));
      } else if (["int32", "int64", "date", "timestamp", "decimal"].includes(kind)) {
        const bits = packedBits(values);
        size.packNow += values.length * byteWidth(bits);
        size.packCandidate += Math.ceil((values.length * bits) / 8);
      }
    }
    reader.close();
  }
  console.error(`group ${String(group + 1)}/${String(GROUPS)}`);
}

const rows = GROUPS * GROUP;
const perRow = (bytes: number): string => (bytes / rows).toFixed(1);
const total = sizes.reduce((sum, size) => sum + size.stored, 0);
const totalGzip = sizes.reduce((sum, size) => sum + size.gzipped, 0);
console.log(
  `${String(rows)} rows of W1M × 200: ${perRow(total)} B/row stored as flushed, ${perRow(totalGzip)} B/row with compaction gzip`,
);

const families = new Map<string, ColumnSize[]>();
for (const size of sizes) {
  const key =
    size.column.family === "x"
      ? `x (${["int", "float", "text", "bool"][size.column.mixed] ?? "?"})`
      : size.column.family;
  families.set(key, [...(families.get(key) ?? []), size]);
}
console.log(
  "\nfamily | columns | B/row | share | gzip B/row | encodings (share of the family's page bytes)",
);
for (const [family, members] of [...families].sort(
  (a, b) => sum(b[1], "stored") - sum(a[1], "stored"),
)) {
  const stored = sum(members, "stored");
  const encodings = new Map<string, number>();
  for (const member of members)
    for (const [name, bytes] of member.encodings)
      encodings.set(name, (encodings.get(name) ?? 0) + bytes);
  const pageBytes = [...encodings.values()].reduce((a, b) => a + b, 0);
  console.log(
    [
      family,
      members.length,
      perRow(stored),
      `${((100 * stored) / total).toFixed(0)}%`,
      perRow(sum(members, "gzipped")),
      [...encodings]
        .sort((a, b) => b[1] - a[1])
        .map(([name, bytes]) => `${name} ${((100 * bytes) / pageBytes).toFixed(0)}%`)
        .join(", "),
    ].join(" | "),
  );
}

console.log("\ntop columns | B/row | gzip B/row | encodings");
for (const size of [...sizes].sort((a, b) => b.stored - a.stored).slice(0, 15))
  console.log(
    [
      size.column.name,
      perRow(size.stored),
      perRow(size.gzipped),
      [...size.encodings].map(([name]) => name).join("+"),
    ].join(" | "),
  );

const floatNow = sum(sizes, "floatNow");
const floatCandidate = sum(sizes, "floatCandidate");
const packNow = sum(sizes, "packNow");
const packCandidate = sum(sizes, "packCandidate");
console.log(
  "\ncandidate | value bytes now (B/row) | with it (B/row) | saves (B/row, share of the table)",
);
console.log(
  `decimal floats | ${perRow(floatNow)} | ${perRow(floatCandidate)} | ${perRow(floatNow - floatCandidate)}, ${((100 * (floatNow - floatCandidate)) / total).toFixed(0)}%`,
);
console.log(
  `bit-packed frame of reference | ${perRow(packNow)} | ${perRow(packCandidate)} | ${perRow(packNow - packCandidate)}, ${((100 * (packNow - packCandidate)) / total).toFixed(0)}%`,
);
for (const size of sizes.filter((size) => size.floatNow > 0).slice(0, 4))
  console.log(`  ${size.column.name}: ${perRow(size.floatNow)} → ${perRow(size.floatCandidate)}`);

// The decode half from memory (no storage reads), as flushed and as compacted with gzip: what
// inflating the text pages costs a one-pass export. Medians of 3 passes after a warm-up.
console.log("\nexport stream from memory (once) | rows/s | ms per 65,536 rows");
for (const [label, space, groups] of [
  ["as flushed", plainSpace, plainGroups],
  ["compacted (gzip)", gzipSpace, gzipGroups],
] as const) {
  const view = space.view({ cache: false });
  const options = { budget: UNLIMITED, directories, once: true, readahead: 256 * 1024 };
  await exportStream(view, groups, options);
  const runs: number[] = [];
  for (let pass = 0; pass < 3; pass++) runs.push((await exportStream(view, groups, options)).ms);
  const ms = [...runs].sort((a, b) => a - b)[1] ?? 0;
  console.log(
    `${label} | ${(((rows / ms) * 1000) / 1000).toFixed(0)}k | ${((ms * GROUP) / rows).toFixed(0)}`,
  );
}

function sum(
  list: readonly ColumnSize[],
  key: "stored" | "gzipped" | "floatNow" | "floatCandidate" | "packNow" | "packCandidate",
): number {
  return list.reduce((total, size) => total + size[key], 0);
}
