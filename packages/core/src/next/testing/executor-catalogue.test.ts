/** Frozen catalogue SQL: execute every presently lowerable shape; expose every typed refusal. */
import fixtureData from "./executor-catalogue.json" with { type: "json" };
import { PGlite } from "@electric-sql/pglite";
import { expect, it, vi } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { encodeValue } from "../../testing/differential-harness.js";
import { UnsupportedPlanError } from "../errors.js";
import { PlanningError } from "../lower/errors.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { assertCatalogueOrder, catalogueOrderColumns } from "./catalogue-order.js";
import { createExecutor } from "../exec/create.js";
import { AggregateOperator } from "../exec/agg/aggregate.js";
import type { Batch } from "../vector/vector.js";
import { createReach } from "../../testing/reach.js";
import {
  CATALOGUE_REFUSALS,
  CATALOGUE_QUERY_AUTHORITY,
  CATALOGUE_REFUSAL_REASONS,
  ROUND_OVERLOAD_AUTHORITY,
  type CatalogueRefusal,
  CATALOGUE_CURRENT_AUTHORITY,
  queryAuthorityValue,
} from "./divergences.js";

interface Table {
  name: string;
  ddl: string;
  rows: Array<Record<string, boolean | string | number | null>>;
}
interface Query {
  id: string;
  dataset: string;
  sql: string;
  lowered: boolean;
  construct?: string;
}
interface Fixture {
  datasets: Record<string, Table[]>;
  queries: Query[];
}
const FIXTURE = fixtureData as unknown as Fixture;
// Authorized refreeze: PostgreSQL has ROUND(numeric, int4), not ROUND(float8, int4).
// No dataset or other SQL changed. Reversing these three corrections must recover the old hash.
const ROUND_CORRECTIONS: Readonly<Record<string, ReadonlyArray<readonly [string, string]>>> = {
  S_q7: [["ROUND(revenue, 4)", "ROUND(revenue::numeric, 4)"]],
  "guide-4": [["ROUND(SUM(o.total), 2)", "ROUND(SUM(o.total)::NUMERIC, 2)"]],
  "guide-5": [
    ["ROUND(SUM(i.line_total), 2)", "ROUND(SUM(i.line_total)::NUMERIC, 2)"],
    ["ROUND(AVG(i.line_total), 2)", "ROUND(AVG(i.line_total)::NUMERIC, 2)"],
  ],
};
function originalRoundSQL(id: string, sql: string): string | null {
  const corrections = ROUND_CORRECTIONS[id];
  if (corrections === undefined) return null;
  for (const [original, corrected] of corrections) {
    sql = sql.replace(corrected, original);
  }
  return sql;
}

// Authorized guide-9 correction: unknown -> unknown is ambiguous in PG (42725).
// Select JSON explicitly; keep the document, path, result name and every dataset unchanged.
function originalJsonSQL(id: string, sql: string): string | null {
  return id === "guide-9" ? sql.replace("'::json -> 'dims'", "' -> 'dims'") : null;
}

const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows.map((row) => encodeValue(Object.values(row))).sort();

for (const [dataset, packed] of [
  ["wide", false],
  ["wide", true],
  ["retail", false],
  ["star", false],
] as const)
  it(`executes the ${dataset} catalogue through actual storage and records unsupported planning${packed ? " (packed wide state)" : ""}`, async () => {
    const core = await new StorageCore(32).open();
    const executor = createExecutor({
      packedAverages: packed,
      packedIntegers: packed,
      batchDictionaryMinMax: packed,
    });
    core.executor.prepare = executor.prepare.bind(executor);
    const reach = packed
      ? createReach("packed SQL catalogue", [
          "write path",
          "lowered",
          "packed float AVG",
          "packed integers",
          "all PG columns",
          "current engine",
        ])
      : null;
    reach?.beginHistory();
    let activeQuery = "";
    const packedRows = new Map<string, { average: number; integer: number }>();
    // Explicit receiver below; do not bind this method to the prototype.
    const originalPush = AggregateOperator.prototype.push; // eslint-disable-line @typescript-eslint/unbound-method
    const spy = packed
      ? vi.spyOn(AggregateOperator.prototype, "push").mockImplementation(function (
          this: AggregateOperator,
          batch: Batch,
        ) {
          const result = originalPush.call(this, batch);
          packedRows.set(activeQuery, {
            average: this.packedAverageRows,
            integer: this.packedIntegerRows,
          });
          return result;
        })
      : null;
    // PGlite's default timestamp-without-zone parser constructs a host-local Date. The new
    // core's fixed session timezone is UTC; adapt the wire representation without shifting SQL
    // wall-clock fields, and keep DATE as PostgreSQL's text instead of a host-local midnight.
    const pg = new PGlite({
      parsers: {
        1114: (value) => new Date(value.replace(" ", "T") + "Z"),
        1082: (value) => value,
        1186: (value) => value,
      },
    });
    await pg.waitReady;
    await pg.exec("SET TIME ZONE 'UTC'");
    const current = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
    });
    const compiled: string[] = [],
      refused: Array<{ id: string } & CatalogueRefusal> = [];
    try {
      for (const table of FIXTURE.datasets[dataset] ?? []) {
        await core.execute(table.ddl);
        await pg.exec(table.ddl);
        await current.execute(table.ddl);
        if (table.rows.length === 0) continue;
        const def = core.harness.path.catalog.tableByName(table.name);
        if (def === undefined) throw new Error("missing fixture table");
        const rows = table.rows.map((row) =>
          Object.fromEntries(
            def.columns.map((column) => [
              column.name,
              column.type.kind === "timestamp"
                ? new Date(String(row[column.name]))
                : (row[column.name] ?? null),
            ]),
          ),
        );
        await core.writeBatch("insertBatch", table.name, rows);
        await current.insertBatch(table.name, rows);
        const names = def.columns.map((column) => column.name);
        const params: unknown[] = [];
        const tuples = table.rows.map(
          (row) =>
            `(${names
              .map((name) => {
                params.push(row[name] ?? null);
                return `$${String(params.length)}`;
              })
              .join(",")})`,
        );
        await pg.query(`INSERT INTO ${table.name} VALUES ${tuples.join(",")}`, params);
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      if (dataset === "retail") {
        // Establish the physical input order used by the ordered PG authority, rather than
        // assuming a heap scan happens to match an unspecified SQL result order.
        const scan = await core.execute("SELECT order_id FROM orders WHERE status = 'completed'");
        if (!("batches" in scan)) throw new Error("Expected stored scan rows");
        expect(oracleRows(scan)).toEqual(
          (
            await pg.query(
              "SELECT order_id FROM orders WHERE status = 'completed' ORDER BY order_id",
            )
          ).rows,
        );
        const grouped = await core.execute(
          "SELECT DATE_TRUNC('month', placed_at) AS month, SUM(total) AS revenue FROM orders WHERE status = 'completed' GROUP BY DATE_TRUNC('month', placed_at)",
        );
        if (!("batches" in grouped)) throw new Error("Expected monthly rows");
        const ordered = await pg.query<Record<string, unknown>>(
          "SELECT DATE_TRUNC('month', placed_at) AS month, SUM(total ORDER BY order_id) AS revenue FROM orders WHERE status = 'completed' GROUP BY DATE_TRUNC('month', placed_at)",
        );
        expect(canonical(oracleRows(grouped))).toEqual(canonical(ordered.rows));
      }
      reach?.reach("write path");
      if (dataset === "wide")
        expect(
          (
            await pg.query<{ type: string }>(
              "SELECT pg_typeof(AVG(i05))::text AS type FROM events_wide",
            )
          ).rows,
        ).toEqual([{ type: "numeric" }]);
      for (const query of FIXTURE.queries.filter((query) => query.dataset === dataset)) {
        const original = originalRoundSQL(query.id, query.sql);
        if (original !== null) {
          // Record the retained legacy extension without crediting it as a PG-valid cell.
          await expect(
            pg.query(original),
            `${query.id}: ${ROUND_OVERLOAD_AUTHORITY}`,
          ).rejects.toMatchObject({ code: "42883" });
          expect(() => core.prepare(original), query.id).toThrow(
            expect.objectContaining({ sqlState: "42883" }),
          );
          expect((await current.query(original)).rows, query.id).toBeDefined();
        }
        const originalJson = originalJsonSQL(query.id, query.sql);
        if (originalJson !== null) {
          await expect(pg.query(originalJson), query.id).rejects.toMatchObject({ code: "42725" });
          const authority = await pg.query(query.sql);
          expect(authority.rows, query.id).toEqual([{ width: "30" }]);
          expect((await current.query(query.sql)).rows, query.id).toEqual(authority.rows);
        }
        activeQuery = query.id;
        const expectedRefusal = CATALOGUE_REFUSALS[query.id];
        let prepared: ReturnType<StorageCore["prepare"]>;
        let result: Awaited<ReturnType<StorageCore["execute"]>>;
        try {
          prepared = core.prepare(query.sql);
          result = await core.execute(query.sql);
        } catch (error) {
          let actual: CatalogueRefusal;
          if (error instanceof PlanningError) {
            expect(error.constructor).toBe(PlanningError);
            expect(error.sqlState, query.id).toBe("42883");
            actual = { class: "PlanningError", sqlState: "42883" };
          } else if (error instanceof UnsupportedPlanError) {
            expect(error.constructor).toBe(UnsupportedPlanError);
            actual = {
              class: "UnsupportedPlanError",
              sqlState: error.sqlState,
              stage: error.stage,
              construct: error.construct,
            };
          } else throw error;
          expect(actual, query.id).toEqual(expectedRefusal);
          expect(query.lowered, query.id).toBe(
            actual.class === "UnsupportedPlanError" && actual.stage === "execute",
          );
          const explanation = CATALOGUE_REFUSAL_REASONS[query.id];
          expect(explanation?.reason.length, query.id).toBeGreaterThan(0);
          if (explanation?.pgState === "42725")
            await expect(pg.query(query.sql), query.id).rejects.toMatchObject({ code: "42725" });
          else expect((await pg.query(query.sql)).rows, query.id).toBeDefined();
          refused.push({ id: query.id, ...actual });
          continue;
        }
        expect(
          expectedRefusal,
          query.id + " unexpectedly executed a recorded refusal",
        ).toBeUndefined();
        expect(query.lowered, query.id).toBe(true);
        if (!("batches" in result)) throw new Error("catalogue SELECT did not return frames");
        const expected = await pg.query<Record<string, unknown>>(
          CATALOGUE_QUERY_AUTHORITY[query.id] ?? query.sql,
        );
        const pgRows = expected.rows.map((row) =>
          Object.fromEntries(
            Object.entries(row).map(([name, value], column) => [
              name,
              queryAuthorityValue(
                prepared.plan,
                column,
                typeof value === "bigint" &&
                  value >= BigInt(Number.MIN_SAFE_INTEGER) &&
                  value <= BigInt(Number.MAX_SAFE_INTEGER)
                  ? Number(value)
                  : value,
              ),
            ]),
          ),
        );
        const actual = oracleRows(result);
        assertCatalogueOrder(
          query.sql,
          result.columns.map((column) => column.name),
          actual,
          pgRows,
        );
        const a = canonical(actual),
          b = canonical(pgRows);
        if (JSON.stringify(a) !== JSON.stringify(b)) {
          const index = a.findIndex((row, at) => row !== b[at]);
          const av = (JSON.parse(a[index] ?? '["array",[]]') as [string, string[]])[1],
            bv = (JSON.parse(b[index] ?? '["array",[]]') as [string, string[]])[1];
          const cell = av.findIndex((value: string, at: number) => value !== bv[at]);
          throw new Error(
            `${query.id} row ${String(index)}, column ${String(cell)} (${String(result.columns[cell]?.name)}): next=${String(av[cell])} PG=${String(bv[cell])}; counts ${String(a.length)}/${String(b.length)}`,
          );
        }
        const witnessSQL = CATALOGUE_CURRENT_AUTHORITY[query.id];
        const legacyExpected =
          witnessSQL === undefined
            ? pgRows
            : (await pg.query<Record<string, unknown>>(witnessSQL)).rows;
        const legacy = await current.query(query.sql);
        assertCatalogueOrder(query.sql, legacy.columns, legacy.rows, legacyExpected);
        expect(
          canonical(legacy.rows),
          `${query.id} against independent PostgreSQL${witnessSQL === undefined ? "" : " R21 witness"}`,
        ).toEqual(canonical(legacyExpected));
        if (packed && query.id === "agg_3dims_20aggs") {
          reach?.reach("lowered");
          const rows = packedRows.get(query.id);
          expect(rows?.average ?? 0).toBeGreaterThan(0);
          expect(rows?.integer ?? 0).toBeGreaterThan(0);
          reach?.reach("packed float AVG");
          reach?.reach("packed integers");
          reach?.reach("all PG columns");
          reach?.reach("current engine");
        }
        compiled.push(query.id);
      }
      const cells = FIXTURE.queries.filter((query) => query.dataset === dataset);
      expect(compiled).toEqual(
        cells
          .filter((query) => CATALOGUE_REFUSALS[query.id] === undefined)
          .map((query) => query.id),
      );
      expect(refused.map((cell) => cell.id)).toEqual(
        cells
          .filter((query) => CATALOGUE_REFUSALS[query.id] !== undefined)
          .map((query) => query.id),
      );
      expect(compiled.length + refused.length).toBe(cells.length);
      reach?.assertReached();
      console.info(
        JSON.stringify({
          lane: "Node correctness",
          dataset,
          executed: compiled,
          ordered: cells
            .filter(
              (cell) =>
                compiled.includes(cell.id) &&
                catalogueOrderColumns(
                  cell.sql,
                  core.prepare(cell.sql).columns.map((column) => column.name),
                ).length > 0,
            )
            .map((cell) => cell.id),
          unsupported: refused,
        }),
      );
    } finally {
      spy?.mockRestore();
      await current.close();
      await pg.close();
      await core.close();
    }
  }, 60_000);

it("keeps frozen data and every SQL cell except the authorized ROUND and JSON corrections", async () => {
  const source = fixtureData;
  const frozen = {
    source: source.source,
    datasets: source.datasets,
    queries: source.queries.map(({ id, dataset, sql }) => ({ id, dataset, sql })),
  };
  const fingerprint = async (value: unknown): Promise<string> => {
    const hash = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(JSON.stringify(value)),
    );
    return Array.from(new Uint8Array(hash), (byte) => byte.toString(16).padStart(2, "0")).join("");
  };
  expect(await fingerprint(frozen)).toBe(
    "c5b7d4af2c34ab8b6408624d6fe160659ed5dc02f0950c93db12bdaf0c9ce1e6",
  );
  expect(
    await fingerprint({
      ...frozen,
      queries: frozen.queries.map((query) => ({
        ...query,
        sql:
          originalRoundSQL(query.id, query.sql) ??
          originalJsonSQL(query.id, query.sql) ??
          query.sql,
      })),
    }),
  ).toBe("8d3af125ed1af20672fe678e0fdc9a40e5ce180bae68c64a099b601ae7b7e9a1");
});

it("documents exactly every retained catalogue refusal", () => {
  expect(Object.keys(CATALOGUE_REFUSAL_REASONS).sort()).toEqual(
    Object.keys(CATALOGUE_REFUSALS).sort(),
  );
});
