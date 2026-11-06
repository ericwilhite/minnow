/**
 * PostgreSQL (PGlite) as the oracle for lowering: the same setup and statements run in PGlite and
 * through lowering + the IR reference interpreter (lowering-harness.ts); outcomes must match —
 * the same SQLSTATE, or the same rows and result types. Used by lowering-pglite*.test.ts.
 */
import type { PGlite } from "@electric-sql/pglite";
import { expect } from "vitest";
import { LoweredDatabase } from "./lowering-harness.js";

let schema = 0;

export const OID_NAMES: Readonly<Record<number, string>> = {
  16: "boolean",
  20: "bigint",
  21: "smallint",
  23: "integer",
  25: "text",
  700: "real",
  701: "double precision",
  1043: "character varying",
  1700: "numeric",
  2950: "uuid",
};

const baseName = (sqlType: string) => sqlType.replace(/\(.*\)$/u, "");

export type Outcome =
  | { readonly error: string }
  | { readonly rows: unknown[]; readonly types: string[]; readonly command: string };

export interface Case {
  readonly sql: string;
  /** Compare only result types (values that are random, or NUMERIC text). */
  readonly typesOnly?: boolean;
  /** R17.1 render hints of the result columns (lowering's own rule; PostgreSQL has none). */
  readonly render?: ReadonlyArray<"bigint" | "number" | null>;
}

function normalized(value: unknown): unknown {
  if (typeof value === "number" && !Number.isInteger(value)) return Number(value.toPrecision(12));
  if (typeof value === "bigint") return Number(value);
  return value;
}

export async function pgOutcome(pg: PGlite, sql: string): Promise<Outcome> {
  try {
    const result = await pg.query<Record<string, unknown>>(sql);
    // NUMERIC arrives as text; the lowering harness returns a number. Compared as numbers (the
    // cases' values are small, so the conversion is exact).
    const numeric = result.fields.map((field) => field.dataTypeID === 1700);
    return {
      rows: result.rows.map((row) =>
        Object.values(row).map((value, index) =>
          normalized(numeric[index] === true && typeof value === "string" ? Number(value) : value),
        ),
      ),
      types: result.fields.map(
        (field) => OID_NAMES[field.dataTypeID] ?? `oid ${String(field.dataTypeID)}`,
      ),
      command: (result as { command?: string }).command ?? "",
    };
  } catch (error) {
    return { error: (error as { code?: string }).code ?? String(error) };
  }
}

export function nextOutcome(next: LoweredDatabase, sql: string): Outcome {
  try {
    const lowered = next.lower(sql);
    const rows = next.execute(sql);
    return {
      rows: rows.map((row) => Object.values(row)),
      types:
        lowered.kind === "query" || lowered.kind === "modify"
          ? lowered.columns.map((column) => baseName(column.sqlType))
          : [],
      command: lowered.command,
    };
  } catch (error) {
    const state = (error as { sqlState?: string }).sqlState;
    return { error: state ?? String(error) };
  }
}

/** Runs the setup in a fresh schema of both, then each case; returns the lowered database. */
export async function check(
  pg: PGlite,
  setup: readonly string[],
  cases: readonly Case[],
): Promise<LoweredDatabase> {
  schema += 1;
  await pg.exec(`CREATE SCHEMA s${String(schema)}; SET search_path TO s${String(schema)}`);
  const next = new LoweredDatabase();
  for (const sql of setup) {
    await pg.exec(sql);
    next.execute(sql);
  }
  for (const entry of cases) {
    const expected = await pgOutcome(pg, entry.sql);
    const actual = nextOutcome(next, entry.sql);
    if ("error" in expected) {
      expect(actual, entry.sql).toEqual(expected);
      continue;
    }
    if ("error" in actual) throw new Error(`${entry.sql}: lowering failed with ${actual.error}`);
    expect(actual.types, `${entry.sql} types`).toEqual(expected.types);
    if (entry.typesOnly !== true) expect(actual.rows, `${entry.sql} rows`).toEqual(expected.rows);
    if (
      ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "SELECT"].includes(expected.command) &&
      !entry.sql.startsWith("CREATE")
    )
      expect(actual.command, `${entry.sql} command`).toBe(expected.command);
    if (entry.render !== undefined) {
      const lowered = next.lower(entry.sql);
      if (lowered.kind !== "query" && lowered.kind !== "modify") throw new Error("Expected a plan");
      expect(
        lowered.columns.map((column) => column.render),
        `${entry.sql} render`,
      ).toEqual(entry.render);
    }
  }
  return next;
}
