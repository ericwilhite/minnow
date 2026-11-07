/** Frozen catalogue SQL: execute every presently lowerable shape; expose every typed refusal. */
import fixtureData from "./executor-catalogue.json" with { type: "json" };
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { encodeValue } from "../../testing/differential-harness.js";
import { UnsupportedPlanError } from "../errors.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { queryAuthorityValue } from "./divergences.js";

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
const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows.map((row) => encodeValue(Object.values(row))).sort();

for (const dataset of ["wide", "retail", "star"])
  it(`executes the ${dataset} catalogue through actual storage and records unsupported planning`, async () => {
    const core = await new StorageCore(32).open();
    // PGlite's default timestamp-without-zone parser constructs a host-local Date. The new
    // core's fixed session timezone is UTC; adapt the wire representation without shifting SQL
    // wall-clock fields, and keep DATE as PostgreSQL's text instead of a host-local midnight.
    const pg = new PGlite({
      parsers: { 1114: (value) => new Date(value.replace(" ", "T") + "Z"), 1082: (value) => value },
    });
    await pg.waitReady;
    await pg.exec("SET TIME ZONE 'UTC'");
    const current = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
    });
    const compiled: string[] = [],
      refused: string[] = [];
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
      if (dataset === "wide")
        expect(
          (
            await pg.query<{ type: string }>(
              "SELECT pg_typeof(AVG(i05))::text AS type FROM events_wide",
            )
          ).rows,
        ).toEqual([{ type: "numeric" }]);
      for (const query of FIXTURE.queries.filter((query) => query.dataset === dataset)) {
        if (!query.lowered) {
          expect(() => core.prepare(query.sql), query.id).toThrow(UnsupportedPlanError);
          refused.push(query.id);
          continue;
        }
        if (query.id === "S_q2") {
          // R19: cast bindings explicitly refuse text -> DATE. This is an unfinished kernel,
          // reported as unsupported, not an R21 intended semantic divergence or a passing query.
          expect(() => core.prepare(query.sql), query.id).toThrow(UnsupportedPlanError);
          refused.push(query.id);
          continue;
        }
        const prepared = core.prepare(query.sql);
        const result = await core.execute(query.sql);
        if (!("batches" in result)) throw new Error("catalogue SELECT did not return frames");
        const expected = await pg.query<Record<string, unknown>>(query.sql);
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
        expect(canonical(actual), `${query.id} against current engine`).toEqual(
          canonical((await current.query(query.sql)).rows),
        );
        compiled.push(query.id);
      }
      expect(compiled.length).toBe(dataset === "wide" ? 20 : 2);
      console.info(
        JSON.stringify({
          lane: "Node correctness",
          dataset,
          executed: compiled,
          unsupported: refused,
        }),
      );
    } finally {
      await current.close();
      await pg.close();
      await core.close();
    }
  }, 60_000);
