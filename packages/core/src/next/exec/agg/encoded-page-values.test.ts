/** Real-writer page transitions versus ordinary decode and PostgreSQL's ordered SUM.
 * Kernels keep input order, validity and multiplicity. No reassociation of float additions.
 * Integer AVG is compared to the parent transition; exact SUM/count are checked with PG.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import {
  createReach,
  ENCODED_PAGE_VALUES_REACH,
  ENCODED_PAGE_FLOAT_ERRORS_REACH,
  ENCODED_PAGE_WIDE32_REACH,
  ENCODED_COUNT_VALIDATION_REACH,
} from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { PageEncoding, type PageEntry, type EncodedPageKernel } from "../../page/consume.js";
import {
  DELTA_BLOCK,
  PageFlag,
  ValueTag,
  PageCorruptionError,
  align8,
  validityBytes,
} from "../../page/format.js";
import { parsePage, gatherRows, decodeRange } from "../../page/decode.js";
import { ChunkReader } from "../../page/reader.js";
import { defined, targetFor, writeGroup } from "../../page/test-support.js";
import type { PhysicalType } from "../../types.js";
import { allocateFlat, type FlatVector, type VectorData } from "../../vector/vector.js";
import { F64, I32, I64, UNCHARGED, col, sqlLiteral, valueAt } from "../kernels/test-support.js";
import { IDENTITY, OutputArena } from "../kernels/vectors.js";
import type { Operator } from "../pipeline.js";
import { AggregateOperator } from "./aggregate.js";
import { accumulatorFor, type Accumulator } from "./states.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});
const families = [
  "plain int32",
  "FOR8 int32",
  "FOR16 int32",
  "delta int32",
  "plain float",
  "scaled float",
  "patched float",
  "constant int32",
  "constant float",
  "all-null int32",
  "scaled0 float",
  "scaled8 float",
  "scaled32 float",
  "delta0 int32",
  "delta16 int32",
] as const;

it.each(seedsFor("next-encoded-page-values", [0xb4e3, 0xb4e4]))(
  "preserves exact values, ordered sums and repeated selections (seed %i)",
  async (seed) => {
    const reach = createReach("encoded page values", ENCODED_PAGE_VALUES_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...families),
        fc.integer({ min: 257, max: 1024 }),
        fc.integer(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (family, count, salt, nullable, repeated, range) => {
          reach.beginHistory();
          const integer = family.includes("int32"),
            type: PhysicalType = integer ? I32 : F64,
            values = Array.from({ length: count }, (_, row) => {
              if (family === "all-null int32" || (nullable && row % 13 === 0)) return null;
              const mixed = Math.imul(row ^ salt, 0x9e37_79b1) >>> 0;
              switch (family) {
                case "constant int32":
                  return salt;
                case "constant float":
                  return -0;
                case "plain int32":
                  return mixed | 0;
                case "FOR8 int32":
                  return -1000 + (mixed % 200);
                case "FOR16 int32":
                  return -100_000 + (mixed % 60_000);
                case "delta0 int32":
                  return (salt % 10_000) + row * 19;
                case "delta16 int32":
                  return (salt % 10_000) + row * 40_000 + Math.floor(row / 3) * 1000;
                case "delta int32":
                  return (salt % 10_000) + row * 19 + Math.floor(row / 7);
                case "plain float":
                  return Math.sin(row + salt) * 1e90;
                case "scaled0 float":
                  return row % 127 === 0 ? Math.PI : 1.25;
                case "scaled8 float":
                  return (100 + (mixed % 200)) / 100;
                case "scaled32 float":
                  return (1_000_000_000 + (mixed % 1_000_000_000)) / 100;
                case "patched float":
                  return row % 127 === 0 ? Math.PI : (-1000 + (mixed % 2000)) / 100;
                default:
                  return (-1000 + (mixed % 2000)) / 100;
              }
            }),
            positions = Uint16Array.from(
              Array.from({ length: count }, (_, row) => row)
                .filter((row) => range || row % 3 !== 0)
                .flatMap((row) => (!range && repeated && row % 5 === 0 ? [row, row] : [row])),
            ),
            fixture = await writeGroup([{ type, values }], {
              pageBytes: 2048,
              rawPageBytes: 8192,
              gzip: false,
            }),
            reader = await ChunkReader.open(fixture.space, defined(fixture.group.columns[0]), {
              budget: UNCHARGED,
            }),
            sum = accumulatorFor("sum", integer ? I64 : F64, type, UNCHARGED),
            decoded = accumulatorFor("sum", integer ? I64 : F64, type, UNCHARGED),
            average = integer ? accumulatorFor("avg", F64, type, UNCHARGED) : null,
            decodedAverage = integer ? accumulatorFor("avg", F64, type, UNCHARGED) : null,
            gids = new Int32Array(2048);
          for (const state of [sum, decoded, average, decodedAverage]) state?.grow(1);
          try {
            await reader.load(reader.pagesForRows(positions));
            let k = 0;
            for (
              let page = 0;
              page < reader.directory.pages.length && k < positions.length;
              page++
            ) {
              const entry = reader.pageEntry(page),
                end = entry.firstRow + entry.rowCount,
                from = k;
              while (k < positions.length && (positions[k] ?? 0) < end) k++;
              if (k === from) continue;
              const target = targetFor(reader, k - from, 0);
              reader.gather(positions, target, 0, from, k);
              decoded.update(target, gids, IDENTITY, k - from, true);
              decodedAverage?.update(target, gids, IDENTITY, k - from, true);
              for (const state of [sum, average]) {
                if (!state) continue;
                const kernel = state.pageKernel?.(type, entry);
                expect(kernel, `${family}: expected direct page capability`).not.toBeNull();
                if (!kernel) throw new Error("Missing encoded kernel");
                if (range)
                  reader.consumePage(
                    page,
                    null,
                    (positions[from] ?? 0) - entry.firstRow,
                    (positions[k - 1] ?? 0) + 1 - entry.firstRow,
                    state,
                    kernel,
                  );
                else reader.consumePage(page, positions, from, k, state, kernel);
              }
              const facts: EncodedPageKernel<undefined> = (_state, view) => {
                if (view.allNull) reach.reach("all-null page");
                if (view.kind === PageEncoding.constant) reach.reach("constant page");
                if (view.kind === PageEncoding.plain)
                  reach.reach(integer ? "plain int32" : "plain float");
                if (view.kind === PageEncoding.frameOfReference) {
                  if (view.checkpoints !== null) {
                    reach.reach("delta checkpoints");
                    if (view.width === 0) reach.reach("delta zero-width");
                    if (view.width === 2) reach.reach("delta u16");
                  } else if (view.width === 1) reach.reach("FOR u8");
                  else if (view.width === 2) reach.reach("FOR u16");
                }
                if (view.kind === PageEncoding.scaledFloat) {
                  reach.reach("scaled float");
                  if (view.width === 0) reach.reach("scaled zero-width");
                  if (view.width === 1) reach.reach("scaled u8");
                  if (view.width === 2) reach.reach("scaled u16");
                  if (view.width === 4) reach.reach("scaled u32");
                  if ((view.patches?.length ?? 0) > 0) reach.reach("float patches");
                }
                return undefined;
              };
              reader.consumePage(page, positions, from, k, undefined, facts);
            }
            const ours = valueAt(sum.emit(0, 1), 0),
              parent = valueAt(decoded.emit(0, 1), 0);
            expect(ours).toEqual(parent);
            if (average && decodedAverage)
              expect(valueAt(average.emit(0, 1), 0)).toEqual(valueAt(decodedAverage.emit(0, 1), 0));
            const sql = Array.from(
                positions,
                (row, ordinal) =>
                  `(${String(ordinal)},${sqlLiteral(integer ? "int4" : "float8", values[row] ?? null)})`,
              ).join(","),
              result = await pg.query<{ total: string | null; n: number }>(
                `SELECT sum(v ORDER BY ordinal)::text total,count(v)::int n FROM (VALUES ${sql}) t(ordinal,v)`,
              ),
              expected = defined(result.rows[0]);
            expect(ours).toEqual(
              expected.total === null
                ? null
                : integer
                  ? BigInt(expected.total)
                  : Number(expected.total),
            );
            expect(expected.n).toBe(
              Array.from(positions).filter((row) => values[row] !== null).length,
            );
            if (nullable) reach.reach("nulls");
            if (repeated && !range) reach.reach("repeated positions");
            if (range) reach.reach("contiguous range");
            reach.reach("selected input order");
          } finally {
            reader.close();
            sum.close();
            decoded.close();
            average?.close();
            decodedAverage?.close();
          }
        },
      ),
      {
        seed,
        numRuns: 80,
        examples: families.flatMap<
          [(typeof families)[number], number, number, boolean, boolean, boolean]
        >((family) => [
          [family, 1024, 1, false, true, false],
          [family, 1024, 1, true, false, true],
        ]),
      },
    );
    reach.assertReached();
  },
  120_000,
);

it.each(seedsFor("next-encoded-page-float-errors", [0xb4e5, 0xb4e6]))(
  "preserves demanded overflow, non-finite order and the completed prefix (seed %i)",
  async (seed) => {
    const modes = [
        "positive overflow",
        "negative overflow",
        "infinity first",
        "NaN first",
        "signed zero",
      ] as const,
      reach = createReach("encoded float errors", ENCODED_PAGE_FLOAT_ERRORS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...modes),
        fc.integer({ min: 10, max: 90 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (mode, at, repeated, plain, nullable) => {
          reach.beginHistory();
          const values: Array<number | null> = Array.from({ length: 512 }, (_, row) =>
            mode === "signed zero"
              ? -0
              : nullable && row % 17 === 0
                ? null
                : plain
                  ? Math.sin(row + 1)
                  : 0.01,
          );
          if (mode !== "signed zero") {
            const value = mode === "negative overflow" ? -Number.MAX_VALUE : Number.MAX_VALUE;
            values[at] = value;
            values[at + 2] = value;
            values[at + 4] = -Infinity;
            if (mode === "infinity first") values[at - 1] = Infinity;
            if (mode === "NaN first") values[at - 1] = NaN;
          }
          const positions = Uint16Array.from(
              Array.from({ length: values.length }, (_, row) => row).flatMap((row) =>
                repeated && row === at ? [row, row] : [row],
              ),
            ),
            fixture = await writeGroup([{ type: F64, values }], {
              pageBytes: 8192,
              rawPageBytes: 8192,
              gzip: false,
            }),
            reader = await ChunkReader.open(fixture.space, defined(fixture.group.columns[0]), {
              budget: UNCHARGED,
            }),
            sum = accumulatorFor("sum", F64, F64, UNCHARGED),
            decoded = accumulatorFor("sum", F64, F64, UNCHARGED),
            gids = new Int32Array(2048);
          sum.grow(1);
          decoded.grow(1);
          type Outcome = { error: string } | { value: unknown };
          const evaluate = (state: Accumulator, run: () => void): Outcome => {
            try {
              run();
              return { value: valueAt(state.emit(0, 1), 0) };
            } catch (error) {
              const code = (error as { sqlState?: string }).sqlState;
              if (code !== "22003") throw error;
              return { error: code };
            }
          };
          try {
            await reader.load(reader.pagesForRows(positions));
            if (mode !== "signed zero") {
              // Irregular finite inputs force plain float8; repeated cents retain scaled
              // payloads with sparse exceptional patches. Check the writer's actual choice.
              for (const entry of reader.directory.pages)
                expect(entry.encoding & ~PageEncoding.gzip).toBe(
                  plain ? PageEncoding.plain : PageEncoding.scaledFloat,
                );
              reach.reach(
                plain ? "plain payload error transitions" : "scaled payload error transitions",
              );
              reach.reach(nullable ? "nullable error transitions" : "all-valid error transitions");
            }
            const target = targetFor(reader, positions.length, 0);
            reader.gather(positions, target);
            const parent = evaluate(decoded, () =>
                decoded.update(target, gids, IDENTITY, positions.length, true),
              ),
              ours = evaluate(sum, () => {
                let k = 0;
                for (
                  let page = 0;
                  page < reader.directory.pages.length && k < positions.length;
                  page++
                ) {
                  const entry = reader.pageEntry(page),
                    end = entry.firstRow + entry.rowCount,
                    from = k;
                  while (k < positions.length && (positions[k] ?? 0) < end) k++;
                  if (from === k) continue;
                  const kernel = sum.pageKernel?.(F64, entry);
                  if (!kernel) throw new Error("Missing float page kernel");
                  reader.consumePage(page, positions, from, k, sum, kernel);
                }
              });
            expect(ours).toEqual(parent);
            // A rejected addition must not install infinity or hide already completed values.
            expect(valueAt(sum.emit(0, 1), 0)).toEqual(valueAt(decoded.emit(0, 1), 0));
            let expected: Outcome;
            try {
              const result = await pg.query<{ total: string | null }>(
                  `SELECT sum(v ORDER BY ordinal)::text total FROM (VALUES ${Array.from(
                    positions,
                    (row, ordinal) =>
                      `(${String(ordinal)},${sqlLiteral("float8", values[row] ?? null)})`,
                  ).join(",")}) t(ordinal,v)`,
                ),
                total = defined(result.rows[0]).total;
              expected = { value: total === null ? null : Number(total) };
            } catch (error) {
              const code = (error as { code?: string }).code;
              if (code !== "22003") throw error;
              expected = { error: code };
            }
            expect(ours).toEqual(expected);
            reach.reach(mode);
            if (repeated) reach.reach("repeated positions before failure");
          } finally {
            reader.close();
            sum.close();
            decoded.close();
          }
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: modes.flatMap((mode) =>
          [false, true].flatMap((plain) =>
            [false, true].map<[(typeof modes)[number], number, boolean, boolean, boolean]>(
              (nullable) => [mode, 30, true, plain, nullable],
            ),
          ),
        ),
      },
    );
    reach.assertReached();
  },
  120_000,
);

it("declines a noncanonical scaled header before any transition", async () => {
  const fixture = await writeGroup(
      [
        {
          type: F64,
          values: Array.from({ length: 512 }, (_, row) => (row % 127 === 0 ? Math.PI : 1.25)),
        },
      ],
      { pageBytes: 8192, rawPageBytes: 8192, gzip: false },
    ),
    reader = await ChunkReader.open(fixture.space, defined(fixture.group.columns[0]), {
      budget: UNCHARGED,
    }),
    arena = new OutputArena(UNCHARGED, 2048),
    output: unknown[] = [],
    sink: Operator = {
      push(batch) {
        output.push(valueAt(defined(batch.columns[0]), 0));
        return "ready";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    },
    operator = new AggregateOperator(
      {
        id: 1,
        op: "aggregate",
        input: { id: 0, op: "values", output: [], rows: [] },
        output: [{ name: "total", type: F64, nullable: true }],
        groupBy: [],
        groupingSets: null,
        aggregates: [
          { fn: "sum", type: F64, args: [col(0, F64)], filter: null, distinct: false, orderBy: [] },
        ],
      },
      { arena, input: [F64], params: { slots: [] } },
      UNCHARGED,
      sink,
    ),
    check = reader.checkPage.bind(reader),
    guarded = vi
      .spyOn(reader, "checkPage")
      .mockImplementation((page, callback) =>
        check(page, (view, entry, type) =>
          callback(
            { ...view, kind: PageEncoding.scaledFloat, width: 0, baseNumber: -0 },
            entry,
            type,
          ),
        ),
      );
  try {
    await reader.load(reader.pagesForRows(Uint16Array.of(0)));
    // This synthetic raw-header capability case does not alter stored bytes or fix the old
    // dense/gather codec ambiguity. The writer does not emit this base; S1 declines it.
    expect(operator.canConsumePages([{ pages: reader }], Uint16Array.of(0), 1)).toBe(false);
    expect(guarded).toHaveBeenCalledTimes(1);
    await operator.finish();
    expect(output).toEqual([null]);
  } finally {
    guarded.mockRestore();
    operator.abort(undefined);
    arena.close();
    reader.close();
  }
});

/** The size-choosing int32 writer never emits width4 FOR/delta: plain is smaller. These are
 * reader-legal pages, built from the documented page layout and structurally parsed first.
 * Their SQL values are independently derived with BigInt modulo-32 arithmetic, not decode.
 */
it.each(seedsFor("next-encoded-wide32", [0xb4ed, 0xb4ee]))(
  "preserves legal u32 offsets, signed wrap and duplicate selections (seed %i)",
  async (seed) => {
    const reach = createReach("encoded wide32", ENCODED_PAGE_WIDE32_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 257, max: 1024 }),
        fc.integer(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (count, salt, delta, nullable, repeated, range) => {
          reach.beginHistory();
          const hasNulls = nullable && !delta,
            start = 16 + (hasNulls ? align8(validityBytes(count)) : 0),
            blocks = delta ? Math.ceil(count / DELTA_BLOCK) : 0,
            bytes = new Uint8Array(start + 8 + blocks * 8 + count * 4),
            header = new DataView(bytes.buffer),
            base = salt | 0,
            deltas = new Uint32Array(bytes.buffer, start + 8 + blocks * 8, count),
            expected: Array<number | null> = [];
          bytes[0] = PageEncoding.frameOfReference;
          bytes[1] = ValueTag.i32;
          bytes[2] = (delta ? PageFlag.delta : 0) | (hasNulls ? PageFlag.validity : 0);
          bytes[3] = 4;
          header.setUint32(4, count, true);
          header.setInt32(start, base, true);
          header.setInt32(start + 4, base < 0 ? -1 : 0, true);
          const validity = hasNulls ? new Uint32Array(bytes.buffer, 16, (count + 31) >>> 5) : null;
          let value = 0n,
            nulls = 0,
            wrapped = false;
          for (let row = 0; row < count; row++) {
            const step = Math.imul(row ^ salt, 0x9e37_79b1) >>> 0;
            deltas[row] = step;
            const present = !hasNulls || row % 13 !== 0;
            if (validity && present)
              validity[row >>> 5] = (validity[row >>> 5] ?? 0) | (1 << (row & 31));
            if (delta && row % DELTA_BLOCK === 0) {
              const checkpoint = Math.imul(salt ^ (row / DELTA_BLOCK), 0x85eb_ca77) | 0,
                at = start + 8 + (row / DELTA_BLOCK) * 8;
              header.setInt32(at, checkpoint, true);
              header.setInt32(at + 4, checkpoint < 0 ? -1 : 0, true);
              value = BigInt(checkpoint);
              deltas[row] = 0;
            } else {
              const sum = (delta ? value : 0n) + BigInt(base) + BigInt(step);
              value = BigInt.asIntN(32, sum);
              wrapped ||= value !== sum;
            }
            if (!present) {
              nulls++;
              deltas[row] = 0;
            }
            expected.push(present ? Number(value) : null);
          }
          const entry: PageEntry = {
              offset: 0,
              length: bytes.length,
              firstRow: 101,
              rowCount: count,
              encoding: PageEncoding.frameOfReference,
              nullCount: nulls,
              min: null,
              max: null,
              checksum: 0,
            },
            view = parsePage(bytes, entry, ValueTag.i32),
            positions = Uint16Array.from(
              Array.from({ length: count }, (_, row) => row)
                .filter((row) => range || row % 3 !== 0)
                .flatMap((row) =>
                  !range && repeated && row % 5 === 0 ? [row + 101, row + 101] : [row + 101],
                ),
            ),
            target = allocateFlat(I32, positions.length),
            sum = accumulatorFor("sum", I64, I32, UNCHARGED),
            parent = accumulatorFor("sum", I64, I32, UNCHARGED),
            gids = new Int32Array(2048);
          target.length = positions.length;
          if (hasNulls) target.validity = new Uint32Array((positions.length + 31) >>> 5);
          sum.grow(1);
          parent.grow(1);
          try {
            if (range) decodeRange(view, 0, count, target, 0);
            else gatherRows(view, positions, 0, positions.length, entry.firstRow, target, 0);
            const values = Array.from(positions, (row) => expected[row - entry.firstRow] ?? null);
            expect(values.map((_, row) => valueAt(target, row))).toEqual(values);
            parent.update(target, gids, IDENTITY, positions.length, true);
            const kernel = sum.pageKernel?.(I32, entry);
            expect(kernel).not.toBeNull();
            if (!kernel) throw new Error("Missing u32 kernel");
            kernel(sum, view, entry, I32, null, range ? null : positions, 0, positions.length);
            const result = valueAt(sum.emit(0, 1), 0),
              answer = await pg.query<{ total: string | null }>(
                `SELECT sum(v)::text total FROM (VALUES ${values.map((v) => `(${v === null ? "NULL::int4" : String(v)})`).join(",")}) t(v)`,
              );
            expect(result).toEqual(valueAt(parent.emit(0, 1), 0));
            expect(result).toEqual(
              answer.rows[0]?.total === null ? null : BigInt(answer.rows[0]?.total ?? ""),
            );
            reach.reach(delta ? "delta u32" : "FOR u32");
            if (wrapped) reach.reach("signed modulo wrap");
            if (hasNulls) reach.reach("nullable FOR");
            if (!range && repeated) reach.reach("duplicate positions");
            if (range) reach.reach("contiguous range");
          } finally {
            sum.close();
            parent.close();
          }
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: [
          [1024, -2147483648, false, true, true, false],
          [1024, 2147483647, true, false, false, true],
        ],
      },
    );
    reach.assertReached();
  },
  120_000,
);

/** The safe flag is a decode promise, not permission to hide a corrupt demanded slot.
 * Header parsing deliberately does not inspect these scalar bytes (page/review.test.ts).
 */
it.each(seedsFor("next-encoded-count-validation", [0xb4ef, 0xb4f0]))(
  "keeps safe-number corruption on the ordinary demanded-slot boundary (seed %i)",
  async (seed) => {
    const reach = createReach("encoded count validation", ENCODED_COUNT_VALIDATION_REACH),
      kinds = ["int64", "decimal", "interval"] as const;
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...kinds),
        fc.integer({ min: 129, max: 257 }),
        fc.integer({ min: 1, max: 63 }),
        fc.integer({ min: 1, max: 1000 }),
        async (kind, count, badRow, magnitude) => {
          reach.beginHistory();
          const interval = kind === "interval",
            lane = align8(count * 4),
            type: PhysicalType =
              kind === "decimal" ? { kind: "decimal", precision: 20, scale: 0 } : { kind },
            bytes = new Uint8Array(16 + (interval ? lane * 2 : 0) + count * 8),
            header = new DataView(bytes.buffer),
            payload = 16 + (interval ? lane * 2 : 0),
            unsafe = (1n << 53n) + BigInt(magnitude * 2 + 1),
            tag = interval ? ValueTag.interval : kind === "decimal" ? ValueTag.dec64 : ValueTag.i64;
          bytes[0] = PageEncoding.plain;
          bytes[1] = tag;
          bytes[2] = PageFlag.safe;
          bytes[3] = interval ? 16 : 8;
          header.setUint32(4, count, true);
          for (let row = 0; row < count; row++)
            header.setBigInt64(payload + row * 8, row === badRow ? unsafe : BigInt(row + 1), true);
          const entry: PageEntry = {
              offset: 0,
              length: bytes.length,
              firstRow: 0,
              rowCount: count,
              encoding: PageEncoding.plain,
              nullCount: 0,
              min: null,
              max: null,
              checksum: 0,
            },
            view = parsePage(bytes, entry, tag),
            countState = accumulatorFor("count", I64, type, UNCHARGED),
            wideState = accumulatorFor("count", I64, type, UNCHARGED),
            safeValues = { repr: "safe" as const, values: new Float64Array(2) },
            selected = Uint16Array.of(0, count - 1),
            data: VectorData =
              kind === "decimal"
                ? {
                    layout: "decimal",
                    scale: 0,
                    scales: null,
                    specials: null,
                    coefficients: safeValues,
                  }
                : interval
                  ? {
                      layout: "interval",
                      months: new Int32Array(2),
                      days: new Int32Array(2),
                      micros: safeValues,
                    }
                  : { layout: "int64", values: safeValues },
            target: FlatVector = { form: "flat", type, length: 2, validity: null, data };
          countState.grow(1);
          wideState.grow(1);
          try {
            expect(view.safe).toBe(true);
            expect(view.boundSafe).toBe(false);
            expect(countState.pageCheck?.(view, entry, type)).toBe(false);
            expect(valueAt(countState.emit(0, 1), 0)).toBe(0n);
            reach.reach("declined before count transition");
            expect(() => decodeRange(view, badRow, badRow + 1, target, 0)).toThrow(
              PageCorruptionError,
            );
            reach.reach("demanded unsafe value refused");
            expect(() => gatherRows(view, selected, 0, 2, 0, target, 0)).not.toThrow();
            countState.update(target, new Int32Array(2), IDENTITY, 2, true);
            const pgCount = await pg.query<{ n: string }>(
              "SELECT count(v)::text n FROM (VALUES (1),(2)) t(v)",
            );
            expect(valueAt(countState.emit(0, 1), 0)).toBe(BigInt(pgCount.rows[0]?.n ?? ""));
            reach.reach("unselected unsafe value remains undemanded");
            // Without a safe-f64 promise these are valid int64 coefficients/micros. COUNT
            // may consume validity, just as ordinary wide decoding has no safe-number check.
            const wideBytes = bytes.slice();
            wideBytes[2] = (wideBytes[2] ?? 0) & ~PageFlag.safe;
            const wide = parsePage(wideBytes, entry, tag),
              kernel = wideState.pageKernel?.(type, entry);
            expect(wideState.pageCheck?.(wide, entry, type)).toBe(true);
            if (!kernel) throw new Error("Missing wide COUNT kernel");
            kernel(wideState, wide, entry, type, null, null, 0, count);
            expect(valueAt(wideState.emit(0, 1), 0)).toBe(BigInt(count));
            reach.reach("wide lane retains direct count");
            reach.reach(kind);
          } finally {
            countState.close();
            wideState.close();
          }
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: kinds.map<[(typeof kinds)[number], number, number, number]>((kind) => [
          kind,
          257,
          63,
          1,
        ]),
      },
    );
    reach.assertReached();
  },
  120_000,
);
