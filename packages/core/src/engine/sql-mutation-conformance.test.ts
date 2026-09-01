/**
 * Differential DML conformance harness — the mutation counterpart to sql-conformance.test.ts.
 * A seeded generator produces a script of INSERT / UPDATE / DELETE / upsert statements and
 * atomic write scopes; every step applies to MinnowDatabase and to two independent oracles,
 * SQLite (node:sqlite) and PGlite (real PostgreSQL), with equivalent AFTER triggers installed
 * on all three engines. After every step the full table state, the trigger-written audit
 * trail, statement outcomes (success or rejection), and affected-row counts must agree.
 *
 * The triggers are the one dialect split: PostgreSQL has no bare `CREATE TRIGGER … BEGIN … END`
 * form, so the SQLite-flavored fixture triggers are translated into plpgsql trigger functions
 * with the same body. Both spellings write the same audit rows, and the audit table is diffed
 * like any other. MERGE (a PairStep) runs verbatim on Minnow and PGlite but is spelled as
 * UPDATE + INSERT for SQLite; a PairStep compares outcomes and resulting state only, since the
 * spellings report their affected rows differently.
 *
 * The script sticks to the shared semantic surface: quarter-valued amounts keep arithmetic
 * exact, unique keys collide deliberately to exercise conflict paths, and failed statements
 * must leave every engine untouched (per-statement atomicity on all sides).
 */
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { mulberry32, seedFor } from "../testing/seeds.js";
import { positionalToNumbered } from "../testing/oracle.js";
import { MinnowDatabase } from "./database.js";
import { type QueryValue } from "./query.js";

function pick<T>(rng: () => number, values: readonly T[]): T {
  const value = values[Math.floor(rng() * values.length)];
  if (value === undefined) throw new Error("empty pick pool");
  return value;
}

const REGIONS = ["west", "east", "north", "south"] as const;
const LABELS = ["alpha", "bravo", "charlie", "delta", "echo"] as const;

/** A quarter-valued amount in [0, 100): sums and differences stay exact in doubles. */
function quarter(rng: () => number): number {
  return Math.floor(rng() * 400) / 4;
}

const TRIGGERS = [
  "CREATE TRIGGER items_ins AFTER INSERT ON items BEGIN " +
    "INSERT INTO audit (action, item_id, amount) VALUES ('ins', NEW.id, NEW.amount); END",
  // Minnow trigger bodies take bare NEW/OLD references or constants, not expressions over
  // them, so the update audit records the post-image rather than the delta.
  "CREATE TRIGGER items_upd AFTER UPDATE ON items BEGIN " +
    "INSERT INTO audit (action, item_id, amount) VALUES ('upd', NEW.id, NEW.amount); END",
  "CREATE TRIGGER items_del AFTER DELETE ON items BEGIN " +
    "INSERT INTO audit (action, item_id, amount) VALUES ('del', OLD.id, OLD.amount); END",
];

async function minnowFixture(): Promise<MinnowDatabase> {
  // autoCompact stays on (the shipped default) so this harness exercises write scopes with
  // background compaction publishing underneath them, which is how it caught the spurious
  // version conflict that write() now rebases over.
  const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 16 });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string", nullable: true },
      { name: "amount", type: "number" },
      { name: "active", type: "boolean" },
      { name: "label", type: "string" },
    ],
  });
  await database.createTable({
    name: "audit",
    columns: [
      { name: "action", type: "string" },
      { name: "item_id", type: "number", nullable: true },
      { name: "amount", type: "number", nullable: true },
    ],
  });
  for (const trigger of TRIGGERS) await database.execute(trigger);
  return database;
}

function sqliteFixture(): DatabaseSync {
  const database = new DatabaseSync(":memory:");
  database.exec(
    `CREATE TABLE items ("id" INTEGER PRIMARY KEY, "region" TEXT, "amount" REAL, "active" INTEGER, "label" TEXT)`,
  );
  database.exec(`CREATE TABLE audit ("action" TEXT, "item_id" REAL, "amount" REAL)`);
  for (const trigger of TRIGGERS) database.exec(trigger);
  return database;
}

/**
 * TRIGGERS, translated: PostgreSQL spells a row trigger as a plpgsql function plus a
 * `FOR EACH ROW EXECUTE FUNCTION` declaration, but the body — one audit INSERT reading NEW/OLD
 * — is the same, so the audit trail stays comparable across all three engines.
 */
const PGLITE_TRIGGERS = [
  `CREATE FUNCTION items_ins_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ` +
    `INSERT INTO audit (action, item_id, amount) VALUES ('ins', NEW.id, NEW.amount); RETURN NULL; END $$`,
  `CREATE TRIGGER items_ins AFTER INSERT ON items FOR EACH ROW EXECUTE FUNCTION items_ins_audit()`,
  `CREATE FUNCTION items_upd_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ` +
    `INSERT INTO audit (action, item_id, amount) VALUES ('upd', NEW.id, NEW.amount); RETURN NULL; END $$`,
  `CREATE TRIGGER items_upd AFTER UPDATE ON items FOR EACH ROW EXECUTE FUNCTION items_upd_audit()`,
  `CREATE FUNCTION items_del_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ` +
    `INSERT INTO audit (action, item_id, amount) VALUES ('del', OLD.id, OLD.amount); RETURN NULL; END $$`,
  `CREATE TRIGGER items_del AFTER DELETE ON items FOR EACH ROW EXECUTE FUNCTION items_del_audit()`,
];

interface PgliteOracle {
  run(
    sql: string,
    params?: QueryValue[],
  ): Promise<{ rows: Array<Record<string, unknown>>; columns: string[]; count: number }>;
  exec(sql: string): Promise<void>;
  close(): Promise<void>;
}

async function pgliteFixture(): Promise<PgliteOracle> {
  const { PGlite } = await import("@electric-sql/pglite");
  const database = await PGlite.create();
  await database.exec(
    `CREATE TABLE items ("id" INTEGER PRIMARY KEY, "region" TEXT, "amount" DOUBLE PRECISION, "active" BOOLEAN, "label" TEXT)`,
  );
  await database.exec(
    `CREATE TABLE audit ("action" TEXT, "item_id" DOUBLE PRECISION, "amount" DOUBLE PRECISION)`,
  );
  for (const statement of PGLITE_TRIGGERS) await database.exec(statement);
  // int8 and numeric arrive as numbers so counts and numeric math compare directly; the fixture
  // has no datetime columns, so no timestamp parser is needed.
  const parsers = {
    20: (value: string) => Number(value),
    1700: (value: string) => Number(value),
  };
  return {
    run: async (sql, params) => {
      const result = await database.query(positionalToNumbered(sql), (params ?? []) as unknown[], {
        parsers,
      });
      return {
        rows: result.rows as Array<Record<string, unknown>>,
        columns: result.fields.map(({ name }) => name),
        count: result.affectedRows ?? 0,
      };
    },
    exec: async (sql) => {
      await database.exec(sql);
    },
    close: () => database.close(),
  };
}

// --- Script steps -------------------------------------------------------------------------------

/** One SQL statement applied verbatim to both engines. */
interface SqlStep {
  kind: "sql";
  sql: string;
  params?: QueryValue[];
}

/** One atomic write scope: batch ops on Minnow, the equivalent statements in one SQLite tx. */
interface ScopeStep {
  kind: "scope";
  /** Updates applied by key, then inserted rows, then deletions by key — one commit. */
  updates: Array<{ id: number; amount: number }>;
  inserts: Array<{ id: number; region: string | null; amount: number; label: string }>;
  deletes: number[];
  /** When set, the scope also stages an update to this missing key and must abort whole. */
  poisonKey?: number;
  /**
   * Keys that do not exist, mixed into the scope's delete batch. Deleting them removes
   * nothing and must fire no trigger — the batch API is the only way to reach this path.
   */
  absentDeletes?: number[];
}

/**
 * One logical change spelled differently for each engine — MERGE against the statements SQLite
 * has. Only the resulting state and audit trail are compared, since the two spellings report
 * their affected rows differently.
 */
interface PairStep {
  kind: "pair";
  minnow: SqlStep | { sql: string; params?: QueryValue[] };
  sqlite: Array<{ sql: string; params?: QueryValue[] }>;
}

type Step = SqlStep | ScopeStep | PairStep;

interface ScriptState {
  nextId: number;
  rng: () => number;
}

type StepTemplate = (state: ScriptState) => Step;

function freshRow(state: ScriptState): {
  id: number;
  region: string | null;
  amount: number;
  label: string;
} {
  const { rng } = state;
  const id = state.nextId;
  state.nextId += 1;
  return {
    id,
    region: rng() < 0.15 ? null : pick(rng, REGIONS),
    amount: quarter(rng),
    label: pick(rng, LABELS),
  };
}

function rowValues(row: ReturnType<typeof freshRow>, rng: () => number): string {
  const region = row.region === null ? "NULL" : `'${row.region}'`;
  const active = rng() < 0.5 ? "TRUE" : "FALSE";
  return `(${String(row.id)}, ${region}, ${String(row.amount)}, ${active}, '${row.label}')`;
}

const stepTemplates: StepTemplate[] = [
  // Plain multi-row insert with fresh keys.
  (state) => {
    const count = 1 + Math.floor(state.rng() * 3);
    const rows = Array.from({ length: count }, () => freshRow(state));
    return {
      kind: "sql",
      sql: `INSERT INTO items (id, region, amount, active, label) VALUES ${rows
        .map((row) => rowValues(row, state.rng))
        .join(", ")}`,
    };
  },
  // Insert colliding with an existing key: DO NOTHING keeps the old row on both engines.
  (state) => {
    const existing = 1 + Math.floor(state.rng() * Math.max(1, state.nextId - 1));
    const fresh = freshRow(state);
    const collide = { ...freshRow(state), id: existing };
    return {
      kind: "sql",
      sql:
        `INSERT INTO items (id, region, amount, active, label) VALUES ` +
        `${rowValues(collide, state.rng)}, ${rowValues(fresh, state.rng)} ` +
        `ON CONFLICT (id) DO NOTHING`,
    };
  },
  // Upsert: DO UPDATE overwrites a subset of non-key columns from EXCLUDED.
  (state) => {
    const existing = 1 + Math.floor(state.rng() * Math.max(1, state.nextId - 1));
    const collide = { ...freshRow(state), id: existing };
    return {
      kind: "sql",
      sql:
        `INSERT INTO items (id, region, amount, active, label) VALUES ${rowValues(collide, state.rng)} ` +
        `ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount, label = EXCLUDED.label`,
    };
  },
  // The same upsert listing every non-key column. Minnow routes this shape differently from
  // the subset form, so trigger firing must stay identical across the two.
  (state) => {
    const existing = 1 + Math.floor(state.rng() * Math.max(1, state.nextId - 1));
    const collide = { ...freshRow(state), id: existing };
    return {
      kind: "sql",
      sql:
        `INSERT INTO items (id, region, amount, active, label) VALUES ${rowValues(collide, state.rng)} ` +
        `ON CONFLICT (id) DO UPDATE SET region = EXCLUDED.region, amount = EXCLUDED.amount, ` +
        `active = EXCLUDED.active, label = EXCLUDED.label`,
    };
  },
  // Arithmetic update over a region predicate.
  (state) => ({
    kind: "sql",
    sql: `UPDATE items SET amount = amount + ? WHERE region = ?`,
    params: [quarter(state.rng) - 50, pick(state.rng, REGIONS)],
  }),
  // PostgreSQL predicates on the write path: a row comparison chooses the rows to update, and
  // the standard string functions build the new value. Both engines have to agree on which
  // rows matched as well as on what they became.
  (state) => ({
    // RTRIM rather than TRIM(TRAILING …): SQLite has no side keyword, and the point here is
    // the write path, which the read harness's TRIM cases already cover for the syntax.
    kind: "sql",
    sql: `UPDATE items SET label = RTRIM(label, '+') WHERE id >= ? AND id < ? RETURNING id, label`,
    params: [1 + Math.floor(state.rng() * state.nextId), state.nextId + 5],
  }),
  (state) => ({
    kind: "sql",
    sql: `DELETE FROM items WHERE (region, active) IN ((?, TRUE), (?, FALSE)) AND amount < ? RETURNING id`,
    params: [pick(state.rng, REGIONS), pick(state.rng, REGIONS), quarter(state.rng)],
  }),
  (state) => ({
    kind: "sql",
    sql: `UPDATE items SET amount = amount + ? WHERE (amount, id) > (?, ?) RETURNING id, amount`,
    params: [quarter(state.rng) / 4, quarter(state.rng), Math.floor(state.rng() * state.nextId)],
  }),
  // F312: the same rows a MERGE touches, written as the statements SQLite has. The two engines
  // have to agree on which rows matched, what they became, and which triggers fired — MERGE is
  // only worth having if it is exactly the sum of those parts.
  (state) => {
    const existing = 1 + Math.floor(state.rng() * Math.max(1, state.nextId - 1));
    const fresh = state.nextId + 50_000 + Math.floor(state.rng() * 1_000);
    const amount = quarter(state.rng);
    const label = pick(state.rng, LABELS);
    const source =
      `SELECT ${String(existing)} AS id, ${String(amount)} AS amount ` +
      `UNION ALL SELECT ${String(fresh)} AS id, ${String(amount)} AS amount`;
    return {
      kind: "pair",
      minnow: {
        sql:
          `MERGE INTO items t USING (${source}) s ON t.id = s.id ` +
          `WHEN MATCHED THEN UPDATE SET amount = s.amount, label = '${label}' ` +
          `WHEN NOT MATCHED THEN INSERT (id, region, amount, active, label) ` +
          `VALUES (s.id, 'west', s.amount, TRUE, '${label}')`,
      },
      sqlite: [
        {
          sql: `UPDATE items SET amount = ${String(amount)}, label = '${label}' WHERE id IN (${String(existing)}, ${String(fresh)})`,
        },
        {
          sql:
            `INSERT INTO items (id, region, amount, active, label) ` +
            `SELECT s.id, 'west', s.amount, 1, '${label}' FROM (${source}) s ` +
            `WHERE s.id NOT IN (SELECT id FROM items)`,
        },
      ],
    };
  },
  // Update over a key range, RETURNING the affected rows.
  (state) => {
    const low = 1 + Math.floor(state.rng() * state.nextId);
    return {
      kind: "sql",
      sql: `UPDATE items SET label = label || '+', active = ${state.rng() < 0.5 ? "TRUE" : "FALSE"} WHERE id >= ? AND id < ? RETURNING id, label, active`,
      params: [low, low + 1 + Math.floor(state.rng() * 20)],
    };
  },
  // Update through a modulo predicate (touches rows across every block).
  (state) => ({
    kind: "sql",
    sql: `UPDATE items SET amount = amount - ? WHERE id % ? = 0 AND amount > ?`,
    params: [quarter(state.rng) / 2, 3 + Math.floor(state.rng() * 5), quarter(state.rng)],
  }),
  // An empty RETURNING result still has a schema. Row-only comparison cannot see a renamed or
  // reordered projection here, so keep one deterministically empty case in every generated round.
  () => ({
    kind: "sql",
    sql: `UPDATE items SET label = label WHERE id = -1 RETURNING label, id`,
  }),
  // Bounded delete, RETURNING what fell.
  (state) => ({
    kind: "sql",
    sql: `DELETE FROM items WHERE id % ? = ? AND amount < ? RETURNING id, amount`,
    params: [7 + Math.floor(state.rng() * 6), Math.floor(state.rng() * 7), quarter(state.rng)],
  }),
  // INSERT ... SELECT: copy a slice of rows to guaranteed-fresh ids.
  (state) => {
    const base = state.nextId + 100_000;
    const residue = Math.floor(state.rng() * 9);
    const step: SqlStep = {
      kind: "sql",
      sql:
        `INSERT INTO items (id, region, amount, active, label) ` +
        `SELECT id + ${String(base)} AS id, region, amount, active, label FROM items ` +
        `WHERE id % 9 = ${String(residue)} AND id < ${String(base)}`,
    };
    state.nextId = base + state.nextId;
    return step;
  },
  // Atomic write scope: transfer-shaped updates plus an insert and a delete in one commit.
  (state) => {
    const { rng } = state;
    const updates = Array.from({ length: 2 }, () => ({
      id: 1 + Math.floor(rng() * state.nextId),
      amount: quarter(rng),
    }));
    const inserts = [freshRow(state)];
    const deletes = [1 + Math.floor(rng() * state.nextId)];
    return { kind: "scope", updates, inserts, deletes };
  },
  // Poisoned write scope: one staged update targets a missing key, everything must vanish.
  (state) => ({
    kind: "scope",
    updates: [{ id: 1 + Math.floor(state.rng() * state.nextId), amount: quarter(state.rng) }],
    inserts: [freshRow(state)],
    deletes: [],
    poisonKey: 9_999_999,
  }),
  // A delete batch mixing real keys with keys that were never there.
  (state) => ({
    kind: "scope",
    updates: [],
    inserts: [],
    deletes: [1 + Math.floor(state.rng() * state.nextId)],
    absentDeletes: [8_000_001 + Math.floor(state.rng() * 1000), 8_500_001],
  }),
];

function buildScript(): Step[] {
  const state: ScriptState = {
    nextId: 1,
    rng: mulberry32(seedFor("sql-mutation-conformance", 0xd1ffe4)),
  };
  const script: Step[] = [];
  // Seed rows so the first predicates have something to chew on.
  for (let index = 0; index < 8; index += 1) {
    const template = stepTemplates[0];
    if (template) script.push(template(state));
  }
  for (let round = 0; round < 6; round += 1) {
    for (const template of stepTemplates) script.push(template(state));
  }
  return script;
}

// --- Comparison ---------------------------------------------------------------------------------

function normalize(value: unknown): unknown {
  if (value === true) return 1;
  if (value === false) return 0;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") {
    if (Object.is(value, -0)) return 0;
    return Number(value.toFixed(9));
  }
  return value;
}

/** Renders an unknown thrown value for a failure message. */
function describeError(error: unknown): string {
  return error instanceof Error ? error.message : JSON.stringify(error);
}

function rowKey(row: Record<string, unknown>): string {
  return JSON.stringify(
    Object.keys(row)
      .sort()
      .map((key) => [key, normalize(row[key])]),
  );
}

function keys(rows: ReadonlyArray<Record<string, unknown>>, ordered: boolean): string[] {
  const mapped = rows.map(rowKey);
  return ordered ? mapped : [...mapped].sort();
}

function diffSummary(label: string, oracle: string, left: string[], right: string[]): string {
  const firstDiff = left.findIndex((key, index) => key !== right[index]);
  const at = firstDiff === -1 ? Math.min(left.length, right.length) : firstDiff;
  return [
    `${label}: ${String(left.length)} vs ${String(right.length)} rows`,
    `  first difference at row ${String(at)}:`,
    `    minnow: ${left[at] ?? "(missing)"}`,
    `    ${oracle}: ${right[at] ?? "(missing)"}`,
  ].join("\n");
}

function sqliteParams(params: QueryValue[] | undefined): Array<string | number | null> {
  return (params ?? []).map((value) => {
    if (value === null) return null;
    if (value === true) return 1;
    if (value === false) return 0;
    if (value instanceof Date) return value.toISOString();
    return value;
  });
}

// --- The harness --------------------------------------------------------------------------------

describe("DML conformance against SQLite and PGlite", () => {
  it("agrees on state, triggers, outcomes, and row counts across a seeded mutation script", async () => {
    const script = buildScript();
    const minnow = await minnowFixture();
    const sqlite = sqliteFixture();
    const pglite = await pgliteFixture();
    const failures: string[] = [];
    let pgliteCompared = 0;

    const compareState = async (label: string): Promise<void> => {
      const stateSql = `SELECT id, region, amount, active, label FROM items ORDER BY id`;
      const minnowState = keys((await minnow.query(stateSql)).rows, true);
      const sqliteState = keys(sqlite.prepare(stateSql).all(), true);
      if (minnowState.join("\n") !== sqliteState.join("\n")) {
        failures.push(
          `${label}\n${diffSummary("table state", "sqlite", minnowState, sqliteState)}`,
        );
      }
      const pgliteState = keys((await pglite.run(stateSql)).rows, true);
      if (minnowState.join("\n") !== pgliteState.join("\n")) {
        failures.push(
          `${label}\n${diffSummary("table state", "pglite", minnowState, pgliteState)}`,
        );
      }
      const auditSql = `SELECT action, item_id, amount FROM audit`;
      const minnowAudit = keys((await minnow.query(auditSql)).rows, false);
      const sqliteAudit = keys(sqlite.prepare(auditSql).all(), false);
      if (minnowAudit.join("\n") !== sqliteAudit.join("\n")) {
        failures.push(
          `${label}\n${diffSummary("audit trail", "sqlite", minnowAudit, sqliteAudit)}`,
        );
      }
      const pgliteAudit = keys((await pglite.run(auditSql)).rows, false);
      if (minnowAudit.join("\n") !== pgliteAudit.join("\n")) {
        failures.push(
          `${label}\n${diffSummary("audit trail", "pglite", minnowAudit, pgliteAudit)}`,
        );
      }
    };

    const runSql = async (step: SqlStep, label: string): Promise<void> => {
      const returning = step.sql.includes("RETURNING");
      let minnowError: unknown;
      let minnowCount: number | undefined;
      let minnowReturned: string[] | undefined;
      let minnowReturnedColumns: string[] | undefined;
      try {
        const result = await minnow.execute(step.sql, step.params);
        if (result.kind === "insert" || result.kind === "update" || result.kind === "delete") {
          minnowCount = result.rowCount;
          if (returning) {
            minnowReturned = keys(result.returnedRows ?? [], false);
            minnowReturnedColumns = result.returnedColumns ?? [];
          }
        }
      } catch (error) {
        minnowError = error;
      }
      let sqliteError: unknown;
      let sqliteCount: number | undefined;
      let sqliteReturned: string[] | undefined;
      let sqliteReturnedColumns: string[] | undefined;
      try {
        const prepared = sqlite.prepare(step.sql);
        if (returning) {
          const rows = prepared.all(...sqliteParams(step.params));
          sqliteReturned = keys(rows, false);
          sqliteReturnedColumns = prepared.columns().map(({ name }) => name);
          sqliteCount = rows.length;
        } else {
          sqliteCount = Number(prepared.run(...sqliteParams(step.params)).changes);
        }
      } catch (error) {
        sqliteError = error;
      }
      let pgliteError: unknown;
      let pgliteCount: number | undefined;
      let pgliteReturned: string[] | undefined;
      let pgliteReturnedColumns: string[] | undefined;
      try {
        const result = await pglite.run(step.sql, step.params);
        pgliteCount = returning ? result.rows.length : result.count;
        if (returning) {
          pgliteReturned = keys(result.rows, false);
          pgliteReturnedColumns = result.columns;
        }
      } catch (error) {
        pgliteError = error;
      }
      pgliteCompared += 1;
      const compareOracle = (
        oracle: string,
        oracleError: unknown,
        oracleCount: number | undefined,
        oracleReturned: string[] | undefined,
        oracleReturnedColumns: string[] | undefined,
      ): void => {
        if ((minnowError === undefined) !== (oracleError === undefined)) {
          failures.push(
            `${label}\n  outcome diverged: minnow ${
              minnowError === undefined ? "succeeded" : `threw: ${describeError(minnowError)}`
            }; ${oracle} ${
              oracleError === undefined ? "succeeded" : `threw: ${describeError(oracleError)}`
            }`,
          );
          return;
        }
        if (minnowError !== undefined) return; // Both rejected; compareState confirms no damage.
        if (minnowCount !== undefined && oracleCount !== undefined && minnowCount !== oracleCount) {
          failures.push(
            `${label}\n  affected rows diverged: minnow ${String(minnowCount)}, ${oracle} ${String(oracleCount)}`,
          );
        }
        if (
          minnowReturned !== undefined &&
          oracleReturned !== undefined &&
          minnowReturned.join("\n") !== oracleReturned.join("\n")
        ) {
          failures.push(
            `${label}\n${diffSummary("RETURNING rows", oracle, minnowReturned, oracleReturned)}`,
          );
        }
        if (
          minnowReturnedColumns !== undefined &&
          oracleReturnedColumns !== undefined &&
          minnowReturnedColumns.join(",") !== oracleReturnedColumns.join(",")
        ) {
          failures.push(
            `${label}\n  RETURNING column order/names diverged:\n` +
              `    minnow: ${minnowReturnedColumns.join(", ")}\n` +
              `    ${oracle}: ${oracleReturnedColumns.join(", ")}`,
          );
        }
      };
      compareOracle("sqlite", sqliteError, sqliteCount, sqliteReturned, sqliteReturnedColumns);
      compareOracle("pglite", pgliteError, pgliteCount, pgliteReturned, pgliteReturnedColumns);
    };

    const runPair = async (step: PairStep, label: string): Promise<void> => {
      let minnowError: unknown;
      try {
        await minnow.execute(step.minnow.sql, step.minnow.params);
      } catch (error) {
        minnowError = error;
      }
      let sqliteError: unknown;
      try {
        sqlite.exec("BEGIN");
        for (const statement of step.sqlite) {
          sqlite.prepare(statement.sql).run(...sqliteParams(statement.params));
        }
        sqlite.exec("COMMIT");
      } catch (error) {
        sqlite.exec("ROLLBACK");
        sqliteError = error;
      }
      // PGlite speaks the Minnow spelling (MERGE) directly, so it runs verbatim. Affected-row
      // reporting still differs between MERGE and its UPDATE + INSERT expansion, so a PairStep
      // compares outcomes here and leaves the row-level evidence to compareState.
      let pgliteError: unknown;
      try {
        await pglite.run(step.minnow.sql, step.minnow.params);
      } catch (error) {
        pgliteError = error;
      }
      pgliteCompared += 1;
      for (const [oracle, oracleError] of [
        ["sqlite", sqliteError],
        ["pglite", pgliteError],
      ] as const) {
        if ((minnowError === undefined) !== (oracleError === undefined)) {
          failures.push(
            `${label}\n  outcome diverged: minnow ${
              minnowError === undefined ? "succeeded" : `threw: ${describeError(minnowError)}`
            }; ${oracle} ${
              oracleError === undefined ? "succeeded" : `threw: ${describeError(oracleError)}`
            }`,
          );
        }
      }
    };

    const runScope = async (step: ScopeStep, label: string): Promise<void> => {
      // Resolve staged updates/deletes against current state so both engines see identical ops:
      // Minnow's updateBatch requires existing keys, so filter to rows that exist right now.
      const present = new Set(
        ((await minnow.query(`SELECT id FROM items`)).rows as Array<{ id: number }>).map(
          (row) => row.id,
        ),
      );
      const updates = step.updates.filter((update) => present.has(update.id));
      const deletes = step.deletes.filter(
        (id) => present.has(id) && !updates.some((update) => update.id === id),
      );
      let minnowError: unknown;
      try {
        await minnow.write(async (tx) => {
          for (const update of updates) {
            await tx.updateBatch("items", {
              keys: [update.id],
              changes: { amount: [update.amount] },
            });
          }
          if (step.inserts.length > 0) {
            await tx.insertBatch("items", {
              columns: {
                id: step.inserts.map((row) => row.id),
                region: step.inserts.map((row) => row.region),
                amount: step.inserts.map((row) => row.amount),
                active: step.inserts.map(() => true),
                label: step.inserts.map((row) => row.label),
              },
            });
          }
          const deleteKeys = [...deletes, ...(step.absentDeletes ?? [])];
          if (deleteKeys.length > 0) await tx.deleteBatch("items", { keys: deleteKeys });
          if (step.poisonKey !== undefined) {
            await tx.updateBatch("items", { keys: [step.poisonKey], changes: { amount: [0] } });
          }
        });
      } catch (error) {
        minnowError = error;
      }
      if (step.poisonKey !== undefined && minnowError === undefined) {
        failures.push(`${label}\n  poisoned scope was expected to abort but committed`);
        return;
      }
      if (step.poisonKey === undefined && minnowError !== undefined) {
        failures.push(`${label}\n  scope unexpectedly aborted: ${describeError(minnowError)}`);
        return;
      }
      if (minnowError !== undefined) return; // Aborted scope: the oracles apply nothing either.
      sqlite.exec("BEGIN");
      try {
        for (const update of updates) {
          sqlite.prepare(`UPDATE items SET amount = ? WHERE id = ?`).run(update.amount, update.id);
        }
        for (const row of step.inserts) {
          sqlite
            .prepare(`INSERT INTO items (id, region, amount, active, label) VALUES (?, ?, ?, 1, ?)`)
            .run(row.id, row.region, row.amount, row.label);
        }
        for (const id of deletes) sqlite.prepare(`DELETE FROM items WHERE id = ?`).run(id);
        sqlite.exec("COMMIT");
      } catch (error) {
        sqlite.exec("ROLLBACK");
        failures.push(`${label}\n  sqlite mirror transaction failed: ${describeError(error)}`);
      }
      try {
        await pglite.exec("BEGIN");
        for (const update of updates) {
          await pglite.run(`UPDATE items SET amount = ? WHERE id = ?`, [update.amount, update.id]);
        }
        for (const row of step.inserts) {
          await pglite.run(
            `INSERT INTO items (id, region, amount, active, label) VALUES (?, ?, ?, TRUE, ?)`,
            [row.id, row.region, row.amount, row.label],
          );
        }
        for (const id of deletes) await pglite.run(`DELETE FROM items WHERE id = ?`, [id]);
        await pglite.exec("COMMIT");
      } catch (error) {
        await pglite.exec("ROLLBACK");
        failures.push(`${label}\n  pglite mirror transaction failed: ${describeError(error)}`);
      }
      pgliteCompared += 1;
    };

    try {
      for (const [index, step] of script.entries()) {
        const label =
          step.kind === "sql"
            ? `#${String(index)} ${step.sql} :: ${JSON.stringify(step.params ?? [])}`
            : step.kind === "pair"
              ? `#${String(index)} ${step.minnow.sql}`
              : `#${String(index)} write-scope ${JSON.stringify({
                  updates: step.updates,
                  inserts: step.inserts.map((row) => row.id),
                  deletes: step.deletes,
                  poisonKey: step.poisonKey,
                })}`;
        if (step.kind === "sql") await runSql(step, label);
        else if (step.kind === "pair") await runPair(step, label);
        else await runScope(step, label);
        await compareState(label);
        if (failures.length >= 10) break; // Divergence cascades; stop at a useful sample.
      }
    } finally {
      sqlite.close();
      await pglite.close();
    }
    expect(script.length).toBeGreaterThan(60);
    if (failures.length > 0) {
      expect.fail(
        `${String(failures.length)} DML conformance divergences:\n\n` +
          failures.slice(0, 10).join("\n\n"),
      );
    }
    // The PGlite leg is only worth something while it actually runs: every SQL statement, MERGE
    // pair, and committed write scope must have been diffed against PostgreSQL as well. The floor
    // sits after the failure report so a real divergence is read before a coverage shortfall.
    expect(pgliteCompared).toBeGreaterThan(60);
  }, 180_000);
});
