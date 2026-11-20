/** Native lowering/recipes must actually reach S1, preserve profile counts, and compose
 * stored pages with ordinary memory tails. PostgreSQL owns SQL results; neither physical
 * profile counts nor a parent engine can replace that authority.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ENCODED_NATIVE_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import type { Batch } from "../vector/vector.js";
import { AggregateOperator } from "../exec/agg/aggregate.js";
import type { Snapshot } from "../access/snapshot.js";
import type { Execution } from "../exec/executor.js";
import { ChunkReader } from "../page/reader.js";

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});
const selection = (name: string) =>
  `COUNT(*)::text AS n, COUNT(t)::text AS nt, SUM(i)::text AS si, SUM(i)::text AS again, SUM(f) AS sf FROM ${name} WHERE i < 50`;
let queryId = 1_000_000;
/** Execute against the caller's retained pin, without repinning or releasing it. These
 * scalar reductions produce flat frames; disable only the optional page capability for
 * the ordinary scan control. Both arms use the real native executor, readers and leases.
 */
async function atGeneration(core: StorageCore, snapshot: Snapshot, sql: string, encoded: boolean) {
  const prepared = core.prepare(sql),
    ordinary = encoded
      ? null
      : vi.spyOn(AggregateOperator.prototype, "pageConsumer", "get").mockReturnValue(undefined);
  let execution: Execution | undefined;
  try {
    execution = prepared.execute({
      snapshot,
      statement: null,
      parameters: [],
      statementTime: 0n,
      governor: core.harness.governor,
      spill: core.spill,
      queryId: ++queryId,
      reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
      sequences: null,
      sequenceSession: null,
      returning: "atExecuted",
    });
    const batches: Batch[] = [];
    for (;;) {
      const frame = await execution.results.next();
      if (frame === null) break;
      try {
        const payload = structuredClone(frame.payload);
        batches.push({
          length: payload.rowCount,
          selection: null,
          columns: payload.columns.map((vector, index) => {
            const type = prepared.columns[index]?.type;
            if (vector.form !== "flat" || type === undefined)
              throw new Error("Expected flat scalar reduction output");
            return { ...vector, type };
          }),
        });
      } finally {
        frame.release();
      }
    }
    core.lastProfile = (await execution.executed).profile;
    await execution.delivered;
    return { kind: "rows" as const, columns: prepared.columns, batches };
  } catch (error) {
    if (execution) {
      execution.cancel(error);
      await Promise.allSettled([execution.executed, execution.delivered]);
    }
    throw error;
  } finally {
    ordinary?.mockRestore();
  }
}
it.each(seedsFor("next-encoded-native", [0xb4eb, 0xb4ec]))(
  "reaches encoded recipes through native SQL and preserves mixed-source counts (seed %i)",
  async (seed) => {
    const reach = createReach("encoded native recipe", ENCODED_NATIVE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 64, max: 257 }),
        fc.integer(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (count, salt, nullable, withTail, deletions) => {
          reach.beginHistory();
          const core = await new StorageCore(4096).open(),
            rows = Array.from({ length: count + (withTail ? 17 : 0) }, (_, id) => {
              const mixed = Math.imul(id ^ salt, 0x9e37_79b1) >>> 0;
              // Explicit selected/live witnesses: raw consumption and a requested memory tail
              // must not depend on a random filter happening to retain at least one value.
              if (id === 1 || (withTail && id === count + (count % 11 === 0 ? 1 : 0)))
                return { id, i: 1, f: 0.125, t: "key-1" };
              return {
                id,
                i: nullable && id % 13 === 0 ? null : mixed % 100,
                f: nullable && id % 17 === 0 ? null : ((mixed % 1600) - 800) / 8,
                t: nullable && id % 7 === 0 ? null : `key-${String(id % 9)}`,
              };
            });
          const pins: Snapshot[] = [];
          let consumed = 0,
            ordinary = 0;
          // Spies forward with the real invocation's receiver through .call(this, ...).
          // eslint-disable-next-line @typescript-eslint/unbound-method
          const consume = AggregateOperator.prototype.consumePages;
          // eslint-disable-next-line @typescript-eslint/unbound-method
          const push = AggregateOperator.prototype.push;
          const encoded = vi
              .spyOn(AggregateOperator.prototype, "consumePages")
              .mockImplementation(function (
                this: AggregateOperator,
                columns,
                positions,
                size,
                map,
              ) {
                consumed += size;
                return consume.call(this, columns, positions, size, map);
              }),
            fallback = vi.spyOn(AggregateOperator.prototype, "push").mockImplementation(function (
              this: AggregateOperator,
              batch: Batch,
            ) {
              ordinary += batch.selection?.count ?? batch.length;
              return push.call(this, batch);
            });
          try {
            await core.execute(
              "CREATE TABLE encoded_native (id INTEGER PRIMARY KEY,i INTEGER,f DOUBLE PRECISION,t TEXT)",
            );
            await core.writeBatch("insertBatch", "encoded_native", rows.slice(0, count));
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            if (withTail) await core.writeBatch("insertBatch", "encoded_native", rows.slice(count));
            if (deletions) await core.execute("DELETE FROM encoded_native WHERE id % 11 = 0");
            consumed = 0;
            ordinary = 0;
            encoded.mockClear();
            fallback.mockClear();
            const result = await core.execute(`SELECT ${selection("encoded_native")}`);
            if (!("batches" in result)) throw new Error("Expected native SELECT rows");
            const input = rows
                .map(
                  (row) =>
                    `(${String(row.id)},${row.i === null ? "NULL::int4" : String(row.i)},${row.f === null ? "NULL::float8" : `${String(row.f)}::float8`},${row.t === null ? "NULL::text" : `'${row.t}'`},${deletions && row.id % 11 === 0 ? "true" : "false"})`,
                )
                .join(","),
              expected = await pg.query<{
                n: string;
                nt: string;
                si: string | null;
                again: string | null;
                sf: number | null;
              }>(
                `WITH raw(id,i,f,t,gone) AS (VALUES ${input}), expected AS (SELECT * FROM raw WHERE NOT gone) SELECT ${selection("expected")}`,
              ),
              actual = oracleRows(result);
            expect(actual).toEqual(expected.rows);
            const selected = Number(expected.rows[0]?.n);
            expect(consumed).toBeGreaterThan(0);
            expect(consumed + ordinary).toBe(selected);
            const scans =
              core.lastProfile?.operators.filter((operator) => operator.op === "scan") ?? [];
            expect(scans.length).toBeGreaterThan(0);
            expect(scans.reduce((sum, scan) => sum + scan.rowsOut, 0)).toBe(selected);
            expect(scans.some((scan) => scan.pagesRead > 0)).toBe(true);
            reach.reach("native encoded recipe");
            reach.reach("counted scan rows preserved");
            reach.reach("duplicate aggregate column binding");
            if (withTail) {
              expect(ordinary).toBeGreaterThan(0);
              reach.reach("stored and memory transitions");
            }
            if (nullable) reach.reach("null columns");
            if (deletions) reach.reach("deleted rows");

            const retained = core.harness.path.pin();
            pins.push(retained);
            const retainedBound = retained.bound,
              updatedI = 2 + ((salt >>> 0) % 47),
              updatedF = (3 + ((salt >>> 0) % 13)) / 8,
              updatedT = nullable && (salt & 1) !== 0 ? null : "updated-key";
            reach.reach("retained generation pinned before update");
            // id=1 is always live and selected, with i=1, f=0.125 and t=key-1 in G.
            // Its new i remains selected and strictly increases, so G and G+1 cannot
            // accidentally have the same aggregate answer. Retain G across the flush.
            await core.execute(
              `UPDATE encoded_native SET i=${String(updatedI)},f=${String(updatedF)},t=${updatedT === null ? "NULL" : `'${updatedT}'`} WHERE id=1`,
            );
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const newer = core.harness.path.pin();
            pins.push(newer);
            expect(newer.bound).toBeGreaterThan(retainedBound);
            expect(retained.bound).toBe(retainedBound);
            const updatedInput = rows
                .map((row) =>
                  row.id === 1 ? { ...row, i: updatedI, f: updatedF, t: updatedT } : row,
                )
                .map(
                  (row) =>
                    `(${String(row.id)},${row.i === null ? "NULL::int4" : String(row.i)},${row.f === null ? "NULL::float8" : `${String(row.f)}::float8`},${row.t === null ? "NULL::text" : `'${row.t}'`},${deletions && row.id % 11 === 0 ? "true" : "false"})`,
                )
                .join(","),
              updatedExpected = await pg.query<(typeof expected.rows)[number]>(
                `WITH raw(id,i,f,t,gone) AS (VALUES ${updatedInput}), expected AS (SELECT * FROM raw WHERE NOT gone) SELECT ${selection("expected")}`,
              );
            const checkGeneration = async (
              snapshot: Snapshot,
              wanted: typeof expected.rows,
              raw: boolean,
            ) => {
              consumed = 0;
              ordinary = 0;
              encoded.mockClear();
              fallback.mockClear();
              const checked = await atGeneration(
                core,
                snapshot,
                `SELECT ${selection("encoded_native")}`,
                raw,
              );
              const values = oracleRows(checked),
                selected = Number(wanted[0]?.n);
              expect(values).toEqual(wanted);
              expect(snapshot.released).toBe(false);
              expect(consumed + ordinary).toBe(selected);
              if (raw) expect(consumed).toBeGreaterThan(0);
              else {
                expect(consumed).toBe(0);
                expect(ordinary).toBe(selected);
              }
              const scans =
                core.lastProfile?.operators.filter((operator) => operator.op === "scan") ?? [];
              expect(scans.reduce((sum, scan) => sum + scan.rowsOut, 0)).toBe(selected);
              expect(scans.some((scan) => scan.pagesRead > 0)).toBe(true);
              return { values, ordinary };
            };
            const oldEncoded = await checkGeneration(retained, expected.rows, true),
              oldScan = await checkGeneration(retained, expected.rows, false);
            expect(oldEncoded.values).toEqual(oldScan.values);
            reach.reach("encoded retained generation after flush");
            reach.reach("scan retained generation after flush");
            const newEncoded = await checkGeneration(newer, updatedExpected.rows, true),
              newScan = await checkGeneration(newer, updatedExpected.rows, false);
            expect(newEncoded.values).toEqual(newScan.values);
            // The updated row must now be stored, not merely an unflushed memory tail.
            expect(newEncoded.ordinary).toBe(0);
            reach.reach("updated generation flushed");
            reach.reach("encoded newer generation");
            reach.reach("scan newer generation");
            expect(newEncoded.values).not.toEqual(oldEncoded.values);
            reach.reach("generations keep distinct answers");
          } finally {
            encoded.mockRestore();
            fallback.mockRestore();
            for (const pin of pins) pin.release();
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("complete native cleanup");
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: [
          [257, 1, true, true, true],
          [64, 2, false, false, false],
        ],
      },
    );
    reach.assertReached();
  },
  180_000,
);

it("keeps integer AVG's C11 float64 type and scan rounding on encoded pages", async () => {
  // DECISIONS.md C11 and docs/sql/select.mdx: integer AVG is float64. The exact
  // integer total is converted once, then divided by the non-null count; it is
  // not PostgreSQL NUMERIC AVG and must not acquire a different result type.
  for (const values of [
    [2_147_483_647, 2_147_483_647, 0, null],
    [-2_147_483_648, -2_147_483_648, 0, null],
    [1, 0, 0, null],
  ]) {
    const core = await new StorageCore(4096).open();
    let snapshot: Snapshot | undefined;
    let consumed = 0;
    // eslint-disable-next-line @typescript-eslint/unbound-method
    const consume = AggregateOperator.prototype.consumePages;
    const encoded = vi
      .spyOn(AggregateOperator.prototype, "consumePages")
      .mockImplementation(function (this: AggregateOperator, columns, rows, count, map) {
        consumed += count;
        return consume.call(this, columns, rows, count, map);
      });
    try {
      await core.execute("CREATE TABLE c11_avg (id INTEGER PRIMARY KEY,i INTEGER)");
      await core.writeBatch(
        "insertBatch",
        "c11_avg",
        values.map((i, id) => ({ id, i })),
      );
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      snapshot = core.harness.path.pin();
      const raw = await atGeneration(core, snapshot, "SELECT AVG(i) AS mean FROM c11_avg", true);
      expect(consumed).toBeGreaterThan(0);
      const scan = await atGeneration(core, snapshot, "SELECT AVG(i) AS mean FROM c11_avg", false),
        finite = values.filter((value): value is number => value !== null),
        total = finite.reduce((sum, value) => sum + BigInt(value), 0n),
        expected = Number(total) / finite.length,
        rawValue = oracleRows(raw)[0]?.mean,
        scanValue = oracleRows(scan)[0]?.mean;
      expect(raw.columns[0]?.type).toEqual({ kind: "float64" });
      expect(scan.columns[0]?.type).toEqual(raw.columns[0]?.type);
      const output = raw.batches[0]?.columns[0];
      expect(output?.form).toBe("flat");
      if (output?.form !== "flat") throw new Error("Expected flat C11 AVG output");
      expect(output.data.layout).toBe("f64");
      expect(Object.is(rawValue, scanValue)).toBe(true);
      expect(Object.is(rawValue, expected)).toBe(true);
    } finally {
      encoded.mockRestore();
      snapshot?.release();
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
    expect(core.spill.bytesInUse).toBe(0);
  }
});
