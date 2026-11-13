import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
/** Semantic grouping workspace belongs to key fields, never aggregate payload fields. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createReach, JSON_GROUP_WIDTH_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const quote = (s: string) => "'" + s.replaceAll("'", "''") + "'";
describe.each(seedsFor("next-json-group-independent-widths", [0xb738, 0xb739]))(
  "key and payload widths seed %i",
  (seed) => {
    for (const source of ["literal", "parameter", "stored", "native"] as const) {
      it(`admits ${source} grouping keys independently of payloads`, async () => {
        const reach = createReach<string>("independent JSON group widths", JSON_GROUP_WIDTH_REACH);
        await fc.assert(
          fc.asyncProperty(
            fc.constantFrom(1, 4096, 65536, 262144),
            fc.constantFrom(1, 64, 4096, 65536),
            fc.constantFrom(12, 16, 24, 32),
            fc.boolean(),
            async (payloadWidth, keyWidth, mib, exponent) => {
              reach.beginHistory();
              // Native source rows retain their ordinary read/copy reservations: the
              // two262KiB payloads need24MiB, or40MiB together with65KiB keys.
              // Buffered/literal controls keep16MiB; the reviewed65KiB/tiny-key
              // regression stays at16MiB for every source family.
              const core = await new StorageCore(16).open(),
                governor = new Governor(
                  Math.max(
                    mib,
                    keyWidth >= 65536
                      ? source === "native" && payloadWidth >= 262144
                        ? 40
                        : 32
                      : payloadWidth >= 262144
                        ? source === "native"
                          ? 24
                          : 16
                        : 0,
                  ) << 20,
                );
              const payload = "x".repeat(payloadWidth),
                doc = exponent
                  ? "1e" + String(keyWidth)
                  : keyWidth === 1
                    ? "1"
                    : JSON.stringify({ k: "k".repeat(keyWidth), n: 1 });
              let sql: string,
                values: string[] = [];
              try {
                if (source === "native") {
                  // Store PG's canonical JSONB spelling so the source-key sizing path
                  // is exercised directly, without a text-to-JSONB producer handoff.
                  const canonical = (
                    await pg.query<{ k: string }>("SELECT $1::jsonb::text k", [doc])
                  ).rows[0]?.k;
                  if (canonical === undefined) throw new Error("Missing canonical key");
                  await core.createTable({
                    name: "width_rows",
                    columns: [
                      { name: "n", type: "number" },
                      { name: "v", type: "string" },
                      { name: "k", type: "string", sqlDomain: { kind: "jsonb" } },
                    ],
                  });
                  const table = core.harness.path.catalog.tableByName("width_rows");
                  if (!table) throw new Error("Missing native key table");
                  const vector = (text: string, type: PhysicalType): FlatVector => {
                    const one = new TextEncoder().encode(text),
                      bytes = new Uint8Array(2 * one.length);
                    bytes.set(one);
                    bytes.set(one, one.length);
                    return {
                      form: "flat",
                      type,
                      length: 2,
                      validity: null,
                      data: {
                        layout: "varlen",
                        bytes,
                        offsets: Int32Array.of(0, one.length, bytes.length),
                      },
                    };
                  };
                  await core.insertVectors(table, {
                    length: 2,
                    selection: null,
                    columns: [
                      {
                        form: "flat",
                        type: { kind: "float64" },
                        length: 2,
                        validity: null,
                        data: { layout: "f64", values: Float64Array.of(1, 2) },
                      },
                      vector(payload, { kind: "text" }),
                      vector(canonical, { kind: "json", binary: true }),
                    ],
                  });
                  await core.harness.path.recycleLog();
                  await core.harness.path.quiesce();
                  await pg.exec(
                    "DROP TABLE IF EXISTS width_rows; CREATE TABLE width_rows(n DOUBLE PRECISION,v TEXT,k JSONB)",
                  );
                  await pg.query("INSERT INTO width_rows VALUES(1,$1,$2::jsonb),(2,$1,$2::jsonb)", [
                    payload,
                    doc,
                  ]);
                  sql = "SELECT json_agg(v ORDER BY n)::text j FROM width_rows GROUP BY k";
                } else if (source === "stored") {
                  await core.execute("CREATE TABLE width_rows(n INTEGER,v TEXT,k TEXT)");
                  await core.writeBatch("insertBatch", "width_rows", [
                    { n: 1, v: payload, k: doc },
                    { n: 2, v: payload, k: doc.replace(/1}$/, "1.0}") },
                  ]);
                  await core.harness.path.recycleLog();
                  await core.harness.path.quiesce();
                  await pg.exec(
                    "DROP TABLE IF EXISTS width_rows; CREATE TABLE width_rows(n INTEGER,v TEXT,k TEXT)",
                  );
                  await pg.query("INSERT INTO width_rows VALUES(1,$1,$2),(2,$1,$3)", [
                    payload,
                    doc,
                    doc.replace(/1}$/, "1.0}"),
                  ]);
                  sql = "SELECT json_agg(v ORDER BY n)::text j FROM width_rows GROUP BY k::jsonb";
                } else if (source === "parameter") {
                  // JSON document parameters keep the existing bounded-source refusal.
                  // Vary the supported aggregate payload slot against a literal key.
                  sql = `SELECT json_agg($1::text)::text j GROUP BY ${quote(doc)}::jsonb`;
                  values = [payload];
                } else {
                  sql = `SELECT json_agg(${quote(payload)}::text)::text j GROUP BY ${quote(doc)}::jsonb`;
                }
                const expected = (await pg.query<{ j: string }>(sql, values)).rows;
                const prepare = core.executor.prepare.bind(core.executor);
                core.executor.prepare = (plan) => {
                  const p = prepare(plan);
                  return { ...p, execute: (r) => p.execute({ ...r, governor }) };
                };
                const actual = await core.execute(sql, values);
                if (!("batches" in actual)) throw new Error("Expected aggregate rows");
                expect(oracleRows(actual), [source, payloadWidth, keyWidth, mib].join("/")).toEqual(
                  expected,
                );
                expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
                expect(governor.usedBytes).toBe(0);
                expect(core.spill.bytesInUse).toBe(0);
                reach.reach(
                  payloadWidth > keyWidth ? "payload wider than key" : "key at least payload width",
                );
                reach.reach(payloadWidth === 65536 ? "65536-byte payload" : "other payload width");
                if (payloadWidth === 262144) reach.reach("wide restored payload tiny key");
                reach.reach(keyWidth === 1 ? "tiny key" : "wide key");
                reach.reach(mib === 16 ? "sixteen MiB cap" : "independent small cap");
                reach.reach(exponent ? "expanding numeric key" : "ordinary canonical key");
                reach.reach("exact PostgreSQL output");
                reach.reach("zero resources");
              } finally {
                await core.close();
              }
            },
          ),
          {
            seed,
            numRuns: 12,
            examples: [
              [65536, 1, 16, false],
              [1, 4096, 24, false],
              [4096, 64, 12, true],
              [1, 65536, 32, true],
              [262144, 1, 16, false],
            ],
          },
        );
        reach.assertReached();
      }, 120000);
    }
  },
);
