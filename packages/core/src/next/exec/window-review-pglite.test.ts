/**
 * Review B7-p2d-1's probes against PostgreSQL itself (PGlite), resident and streamed: exact
 * int64 MIN/MAX past 2^53 (BigInt64Array input), RANGE offsets over int64 keys one apart past
 * 2^53, COUNT over floats that sum past 2^53, a negative LEAD offset past the 4,096-row head,
 * and cancellation-prone float moving sums. The generated campaign over the same dimensions is
 * window.test.ts "exact int64, wide floats and long offsets".
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { BIG, DBL, INT, type Row } from "./join-fixtures.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf } from "./join-fixtures.js";
import { drive } from "./pipeline.js";
import { WindowOperator, windowMinimumBytes, type WindowSpec } from "./window.js";

async function runWindow(spec: WindowSpec, data: readonly Row[]) {
  const minimum = windowMinimumBytes(spec.types, spec.functions.length);
  const governor = new Governor(256 << 20);
  const grant = await governor.admit({ label: "window", minimumBytes: minimum, spillable: true });
  const spill = new MemorySpillService();
  const sink = new CollectSink();
  const op = new WindowOperator(spec, { grant, spill, queryId: 3 }, sink);
  await drive(new ArraySource(batchesOf(spec.types, data, { batchRows: 97, int64: "int64" })), op);
  await op.closed;
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
  return { rows: sink.rows, profile: op.profile };
}

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

describe("review probes", () => {
  for (const streamRows of [100000, 1]) {
    for (const fn of ["min", "max"])
      it(`bigint ${fn} streamRows=${String(streamRows)}`, async () => {
        const data: Row[] = [
          [0, 9007199254740993n],
          [1, 9007199254740995n],
        ];
        const spec: WindowSpec = {
          types: [INT, BIG],
          partitionBy: [],
          orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
          streamRows,
          functions: [{ fn, arg: 1, frame: null, type: BIG }],
        };
        const expected = (
          await pg.query<{ v: unknown }>(
            `SELECT ${fn}(x) OVER (ORDER BY id) AS v FROM (VALUES (0,9007199254740993::bigint),(1,9007199254740995::bigint)) t(id,x)`,
          )
        ).rows.map((r) => String(r.v));
        const result = await runWindow(spec, data);
        expect(result.rows.map((r) => String(r[2]))).toEqual(expected);
      });
    it(`negative lead streamRows=${String(streamRows)}`, async () => {
      const data: Row[] = Array.from({ length: 12000 }, (_, i) => [i]);
      const spec: WindowSpec = {
        types: [INT],
        partitionBy: [],
        orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
        streamRows,
        functions: [{ fn: "lead", arg: 0, amount: -3000, frame: null, type: INT }],
      };
      const result = await runWindow(spec, data);
      expect(result.rows.map((r) => r[1])).toEqual(
        data.map((_, i) => (i < 3000 ? null : i - 3000)),
      );
    });
    for (const fn of ["sum", "avg"])
      it(`adversarial float ${fn} streamRows=${String(streamRows)}`, async () => {
        const data: Row[] = [1e20, 1, -1e20, 3, 1e-20, 4, -4].map((x, i) => [i, x]);
        const spec: WindowSpec = {
          types: [INT, DBL],
          partitionBy: [],
          orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
          streamRows,
          functions: [
            {
              fn,
              arg: 1,
              type: DBL,
              frame: {
                unit: "rows",
                start: { kind: "preceding", offset: 2 },
                end: { kind: "current-row", offset: null },
                exclude: "no-others",
              },
            },
          ],
        };
        const expected = (
          await pg.query<{ v: unknown }>(
            `SELECT ${fn}(x) OVER (ORDER BY id ROWS 2 PRECEDING) AS v FROM (VALUES ${data.map((r) => `(${String(r[0])},${String(r[1])}::float8)`).join(",")}) t(id,x)`,
          )
        ).rows.map((r) => r.v);
        expect((await runWindow(spec, data)).rows.map((r) => r[2])).toEqual(expected);
      });
  }
});

for (const streamRows of [100000, 1])
  it(`bigint RANGE streamRows=${String(streamRows)}`, async () => {
    const data: Row[] = [
      [0, 9007199254740992n],
      [1, 9007199254740993n],
    ];
    const spec: WindowSpec = {
      types: [INT, BIG],
      partitionBy: [],
      orderBy: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
      streamRows,
      functions: [
        {
          fn: "count_star",
          arg: null,
          type: BIG,
          frame: {
            unit: "range",
            start: { kind: "preceding", offset: 0 },
            end: { kind: "following", offset: 0 },
            exclude: "no-others",
          },
        },
      ],
    };
    const expected = (
      await pg.query<{ v: unknown }>(
        `SELECT count(*) OVER (ORDER BY x RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING) AS v FROM (VALUES (9007199254740992::bigint),(9007199254740993::bigint)) t(x)`,
      )
    ).rows.map((r) => String(r.v));
    expect((await runWindow(spec, data)).rows.map((r) => String(r[2]))).toEqual(expected);
  });

it("count float must not sum arguments", async () => {
  const data: Row[] = [
    [0, 0.1],
    [1, 1e20],
  ];
  const spec: WindowSpec = {
    types: [INT, DBL],
    partitionBy: [],
    orderBy: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
    streamRows: 1,
    functions: [{ fn: "count", arg: 1, type: BIG, frame: null }],
  };
  const expected = (
    await pg.query<{ v: unknown }>(
      `SELECT count(x) OVER (ORDER BY id) AS v FROM (VALUES (0,0.1::float8),(1,1e20::float8)) t(id,x)`,
    )
  ).rows.map((r) => String(r.v));
  expect((await runWindow(spec, data)).rows.map((r) => String(r[2]))).toEqual(expected);
});
