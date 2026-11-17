import { required } from "../owner/result-values.js";
/** Actual executor frames -> client Result, with PGlite as the scalar authority.
 * Only D1/D2's declared JS rendering differs; no tolerance or shipped-engine oracle. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_RESULT_PG_REACH } from "../../testing/reach.js";
import { decodeResult } from "../owner/result.js";
import type { FramePayload } from "../owner/frame.js";
import { daysFromCivil } from "../temporal.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("SET TIME ZONE 'UTC'");
}, 60000);
afterAll(async () => {
  await pg.close();
});
const numeric = (coefficient: bigint, scale: number) => {
  const sign = coefficient < 0n ? "-" : "";
  const digits = (coefficient < 0n ? -coefficient : coefficient)
    .toString()
    .padStart(scale + 1, "0");
  return sign + (scale ? `${digits.slice(0, -scale)}.${digits.slice(-scale)}` : digits);
};
const iso = (text: string, zoned: boolean) => {
  const parts = /^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d)(?:\.(\d+))?(?:\+00)?$/u.exec(text);
  if (!parts) throw new Error(`Expected finite ordinary-year PostgreSQL timestamp: ${text}`);
  return `${required(parts[1])}T${required(parts[2])}.${(parts[3] ?? "").padEnd(6, "0")}${zoned ? "Z" : ""}`;
};
it.each(seedsFor("next-public-result-pglite", [0xb732, 0xb733]))(
  "decodes real frames with exact PostgreSQL scalar semantics (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open();
    const reach = createReach("public Result PGlite", PUBLIC_RESULT_PG_REACH);
    try {
      await core.createTable({
        name: "array_source",
        columns: [
          { name: "k", type: "number" },
          { name: "a", type: "string", sqlDomain: { kind: "array", element: "bigint" } },
          { name: "m", type: "string", sqlDomain: { kind: "time" } },
          { name: "span", type: "string", sqlDomain: { kind: "interval" } },
          { name: "u", type: "string", sqlDomain: { kind: "uuid" } },
          { name: "d", type: "string", sqlDomain: { kind: "date" } },
        ],
      });
      await pg.exec(
        "DROP TABLE IF EXISTS array_source; CREATE TABLE array_source(k double precision,a bigint[],m time,span interval,u uuid,d date)",
      );
      let history = 0;
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            big: fc.bigInt({ min: -(1n << 63n), max: (1n << 63n) - 1n }),
            coefficient: fc.bigInt({ min: -(1n << 180n), max: (1n << 180n) - 1n }),
            scale: fc.integer({ min: 0, max: 20 }),
            micro: fc.integer({ min: 1, max: 999999 }),
            day: fc.integer({ min: 1, max: 28 }),
            number: fc.integer({ min: -1000000, max: 1000000 }),
            text: fc.string({ maxLength: 20 }),
            nullable: fc.boolean(),
            months: fc.integer({ min: -30, max: 30 }),
          }),
          async ({ big, coefficient, scale, micro, day, number, text, nullable, months }) => {
            reach.beginHistory();
            const time = `12:34:56.${String(micro).padStart(6, "0")}`;
            const timestamp = `2000-02-${String(day).padStart(2, "0")} ${time}`;
            const json = JSON.stringify({ [text]: number, list: [nullable, null] });
            const interval = `${String(months)} mons ${String(day)} days ${time}`;
            // TIME/INTERVAL/UUID inputs still use typed stored vectors: their text input
            // functions/parameter binders are refused. This is a decoder, not binder enablement.
            const params = [
              big,
              numeric(coefficient, scale),
              timestamp,
              number / 10,
              text,
              nullable,
            ];
            const index = ++history;
            const bytes = encodeArrayValue(
              { kind: "int64" },
              { dimensions: [3], lowerBounds: [1], elements: [big, null, big] },
            );
            const table = required(core.harness.path.catalog.tableByName("array_source"));
            await core.insertVectors(table, {
              length: 1,
              selection: null,
              columns: [
                {
                  form: "flat",
                  length: 1,
                  validity: null,
                  type: { kind: "float64" },
                  data: { layout: "f64", values: Float64Array.of(index) },
                },
                {
                  form: "flat",
                  length: 1,
                  validity: null,
                  type: { kind: "array", element: { kind: "int64" } },
                  data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
                },
                {
                  form: "flat",
                  length: 1,
                  validity: null,
                  type: { kind: "time" },
                  data: {
                    layout: "int64",
                    values: { repr: "safe", values: Float64Array.of(45296000000 + micro) },
                  },
                },
                {
                  form: "flat",
                  length: 1,
                  validity: null,
                  type: { kind: "interval" },
                  data: {
                    layout: "interval",
                    months: Int32Array.of(months),
                    days: Int32Array.of(day),
                    micros: { repr: "safe", values: Float64Array.of(45296000000 + micro) },
                  },
                },
                {
                  form: "flat",
                  length: 1,
                  validity: null,
                  type: { kind: "uuid" },
                  data: { layout: "fixed16", bytes: Uint8Array.from({ length: 16 }, (_, i) => i) },
                },
                {
                  form: "flat",
                  length: 1,
                  validity: null,
                  type: { kind: "date" },
                  data: { layout: "i32", values: Int32Array.of(daysFromCivil(2000, 2, day)) },
                },
              ],
            });
            await pg.query(
              "INSERT INTO array_source VALUES($1,$2::bigint[],$3::time,$4::interval,$5::uuid,$6::date)",
              [
                index,
                `{${String(big)},NULL,${String(big)}}`,
                time,
                interval,
                "00010203-0405-0607-0809-0a0b0c0d0e0f",
                timestamp.slice(0, 10),
              ],
            );
            const sql = `SELECT $1::bigint i, $2::numeric n, $3::timestamp t,
          $3::timestamptz z, m, $4::float8 f, $4::real r,
          $5::text s, $6::boolean b, NULL::bigint missing,
          a, d, span,
          '${json.replaceAll("'", "''")}'::json j, '${json.replaceAll("'", "''")}'::jsonb jb,
          u FROM array_source WHERE k=${String(index)}`;
            const lowered = core.lowerer.lower(sql, core.context());
            if (lowered.kind !== "query") throw new Error("Expected query");
            const run = core.start(core.executor.prepare(lowered.plan), params);
            const frames: FramePayload[] = [];
            try {
              for (;;) {
                const frame = await run.execution.results.next();
                if (!frame) break;
                try {
                  frames.push(structuredClone(frame.payload, { transfer: [...frame.transfer] }));
                  expect(frame.transfer.every((buffer) => buffer.byteLength === 0)).toBe(true);
                } finally {
                  frame.release();
                }
              }
              await run.execution.executed;
              await run.execution.delivered;
              const result = decodeResult({
                columns: lowered.columns,
                frames,
                summary: {
                  command: "SELECT",
                  rowCount: 1,
                  counts: null,
                  lsn: null,
                  replayed: false,
                  closedEarly: false,
                  transaction: "idle",
                  catalogVersion: 0,
                },
              });
              const oracle = required(
                (
                  await pg.query<Record<string, string | null>>(
                    `SELECT i::text,n::text,t::text,z::text,m::text,
            f::text,r::text,s,b::text,missing::text,d::text,span::text,j::text,jb::text,u::text FROM (${sql}) q`,
                    params,
                  )
                ).rows[0],
              );
              const expected = {
                i: BigInt(required(oracle.i)),
                n: oracle.n,
                t: iso(required(oracle.t), false),
                z: iso(required(oracle.z), true),
                m: oracle.m,
                f: Number(oracle.f),
                r: Math.fround(Number(oracle.r)),
                s: oracle.s,
                b: oracle.b === "true",
                missing: null,
                a: [big, null, big],
                d: oracle.d,
                span: oracle.span,
                j: oracle.j,
                jb: oracle.jb,
                u: oracle.u,
              };
              expect(result.rows()).toEqual([expected]);
              expect(result.tuples()).toEqual([Object.values(expected)]);
              expect(result.column("t")).toMatchObject({
                kind: "timestamp",
                unit: "microseconds",
                epoch: "2000-01-01T00:00:00Z",
                zoned: false,
              });
              expect(result.column("i").kind).toBe("int64");
              for (const state of PUBLIC_RESULT_PG_REACH) reach.reach(state);
            } finally {
              run.execution.cancel();
              await Promise.allSettled([run.execution.executed, run.execution.delivered]);
              run.release();
            }
          },
        ),
        { seed, numRuns: 20 },
      );
      reach.assertReached();
      expect(core.spill.bytesInUse).toBe(0);
    } finally {
      await core.close();
      expect(core.harness.governor.usedBytes).toBe(0);
    }
  },
  60000,
);
