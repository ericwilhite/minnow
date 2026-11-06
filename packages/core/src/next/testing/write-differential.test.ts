/**
 * The generated differential for the write path (review B3-p2a-1): seeded random workloads of the
 * statements the write path supports today, run on the write path, PostgreSQL (PGlite: the law) and
 * the current engine (`MinnowDatabase`) side by side. After every step the write path must agree
 * with PostgreSQL on each table's rows, on which statements failed and why (unique / foreign key),
 * and on what the indexes return; and the current engine must agree with PostgreSQL too, unless the
 * difference is a registered intended divergence (ruling R21, `divergences.ts`).
 *
 * The current engine runs each step inside a transaction and commits only when its outcome and
 * rows match PostgreSQL's. Otherwise it rolls back, the difference must match a registered
 * divergence, and the harness moves the engine's clock past the key-forwarding window (the only
 * entry so far) and runs the step again, which must then match exactly. So the comparison with the
 * current engine continues through every step of every seed.
 *
 * Statements: multi-row INSERT (duplicate keys, in the batch and against stored rows; NULLs in a
 * UNIQUE column), UPDATE by primary key (of a UNIQUE and an indexed column, and of the key itself
 * when no child references it), DELETE by primary key (refused while children reference it), child
 * INSERT / DELETE under a RESTRICT foreign key, explicit transactions that commit or roll back,
 * and reopen / crash of the write path. Small memtables put flushes, publishes and replay in the
 * middle of every run.
 *
 * The current engine always runs a step inside BEGIN … COMMIT / ROLLBACK (so it can be undone when
 * it differs); its autocommit behaviour for single statements is not compared. The write path and
 * PostgreSQL run single statements as autocommit.
 *
 * Limits: the parent rekey with children (PostgreSQL's default refuses it, the current engine
 * cascades it; the write path's rekey-follow lands in increment 2), ON DELETE CASCADE / SET NULL,
 * and statements the write path does not take yet (ALTER TABLE, staged large transactions) are not
 * generated.
 */
import { describe, expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import {
  ForeignKeyViolationError,
  UniqueViolationError,
  type Statement,
} from "../access/modify.js";
import type { Snapshot } from "../access/snapshot.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import type { WritePathImpl } from "../write/engine.js";
import {
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rules as rulesFor,
  rowsOf,
  tableInput,
  TEXT,
  texts,
  type Value,
} from "../write/test-support.js";
import {
  intendedDivergence,
  type DifferingRow,
  type Divergence,
  type StatementFacts,
} from "./divergences.js";
import { DEFAULT_KEY_FORWARD_RETENTION_MS } from "../../engine/key-forwards.js";

const SEEDS = seedsFor("next-write-differential", [0xd1ff, 7, 4242, 90210]);
const STEPS = 160;
const FREE = { allocate: () => true, free: () => undefined };

type Outcome = "ok" | "unique" | "foreignKey";

function classifyNext(error: unknown): Outcome {
  if (error instanceof UniqueViolationError) return "unique";
  if (error instanceof ForeignKeyViolationError) return "foreignKey";
  throw error;
}

/** A SQL engine the write path is compared with: PostgreSQL (the law) or the current engine. */
interface Oracle {
  readonly name: string;
  execute(text: string): Promise<void>;
  rows(text: string): Promise<Array<Record<string, unknown>>>;
  /** The constraint a failed statement broke; anything else is a harness error. */
  classify(error: unknown): Outcome;
  close(): Promise<void>;
}

async function postgres(): Promise<Oracle> {
  const pg = await PGlite.create();
  return {
    name: "PostgreSQL",
    execute: async (text) => {
      await pg.exec(text);
    },
    rows: async (text) => (await pg.query<Record<string, unknown>>(text)).rows,
    classify: (error) => {
      const code = (error as { code?: unknown }).code;
      if (code === "23505") return "unique";
      if (code === "23503") return "foreignKey";
      throw error;
    },
    close: () => pg.close(),
  };
}

/** The current engine, on a clock the harness moves (`now`). */
function currentEngine(clock: { ms: number }): Oracle {
  const db = new MinnowDatabase(new MemoryBlockStore(), {
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
    now: () => new Date(clock.ms),
  });
  return {
    name: "current engine",
    execute: async (text) => {
      await db.execute(text);
    },
    rows: async (text) => (await db.query(text)).rows,
    classify: (error) => {
      if (error instanceof Error && error.constructor.name === "UniqueConstraintError")
        return "unique";
      if (error instanceof Error && error.message.includes("FOREIGN KEY")) return "foreignKey";
      throw error;
    },
    close: () => db.close(),
  };
}

/** Runs one generated step on an oracle: its statements in a transaction when there are several. */
async function runOn(
  oracle: Oracle,
  sqls: readonly string[],
  rollback: boolean,
): Promise<{ outcome: Outcome; message: string | null; statement: number | null }> {
  const transaction = sqls.length > 1;
  if (transaction) await oracle.execute("BEGIN");
  for (const [at, text] of sqls.entries())
    try {
      await oracle.execute(text);
    } catch (error) {
      if (transaction) await oracle.execute("ROLLBACK");
      return { outcome: oracle.classify(error), message: errorText(error), statement: at };
    }
  if (transaction) await oracle.execute(rollback ? "ROLLBACK" : "COMMIT");
  return { outcome: "ok", message: null, statement: null };
}

/** An error's message, with PostgreSQL's DETAIL appended (`message: detail`) when it has one. */
function errorText(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  const detail = (error as { detail?: unknown }).detail;
  return typeof detail === "string" && detail !== "" ? `${message}: ${detail}` : message;
}

const sql = (value: number | string | null) =>
  value === null ? "NULL" : typeof value === "number" ? String(value) : `'${value}'`;

/** One generated operation: its SQL for the oracles and its write-path form. */
interface Op {
  readonly sql: string;
  /** What it does, for the divergence registry (divergences.ts `StatementFacts`, without sql). */
  readonly facts: Omit<StatementFacts, "sql">;
  readonly run: (statement: Statement, ids: Ids, step: StepLog) => Promise<unknown>;
}

/** What a step's write-path run did that the divergence facts need. */
interface StepLog {
  /** Keys of t a rekey moved a row away from, and where each moved. */
  readonly moved: Array<readonly [from: number, to: number]>;
}

interface Ids {
  readonly t: number;
  readonly c: number;
  readonly u: number;
  readonly v: number;
  readonly vIndex: number;
}

async function locate(
  view: Snapshot,
  table: number,
  index: number,
  key: number,
): Promise<number | null> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints([key]), direction: "asc", nulls: "first" }],
    { length: 1, selection: null },
    keys,
  );
  const out = { probes: new Int32Array(4), locators: new Float64Array(4), count: 0 };
  await view.index(table, index).probe(keys, out);
  keys.close();
  return out.count === 0 ? null : (out.locators[0] ?? null);
}

async function probeAll(
  view: Snapshot,
  table: number,
  index: number,
  values: readonly number[],
): Promise<number> {
  const keys = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(values), direction: "asc", nulls: "first" }],
    { length: values.length, selection: null },
    keys,
  );
  const out = { probes: new Int32Array(4096), locators: new Float64Array(4096), count: 0 };
  const result = await view.index(table, index).probe(keys, out);
  keys.close();
  expect(result.done).toBe(true);
  return out.count;
}

/** An oracle's rows of a table, as value arrays ordered by id. */
async function oracleRows(oracle: Oracle, table: string, columns: string): Promise<Value[][]> {
  const rows = await oracle.rows(`SELECT ${columns} FROM ${table} ORDER BY id`);
  return rows.map((row) => columns.split(",").map((name) => (row[name.trim()] ?? null) as Value));
}

const byId = (rows: Value[][]) => [...rows].sort((a, b) => Number(a[0]) - Number(b[0]));

class Workload {
  readonly #random: () => number;
  /** The oracle's committed state, mirrored for generation only (the oracle itself decides). */
  constructor(seed: number) {
    this.#random = mulberry32(seed);
  }

  int(below: number): number {
    return Math.floor(this.#random() * below);
  }

  chance(p: number): boolean {
    return this.#random() < p;
  }

  maybe(below: number, nulls = 0.25): number | null {
    return this.chance(nulls) ? null : this.int(below);
  }

  text(): string | null {
    if (this.chance(0.2)) return null;
    const length = this.int(6);
    return Array.from({ length }, () => "abcxyz"[this.int(6)]).join("");
  }

  /** One operation over the current parent ids and their child counts. */
  op(parents: readonly number[], children: ReadonlyMap<number, number>): Op {
    const pick = () =>
      parents.length > 0 && this.chance(0.8)
        ? (parents[this.int(parents.length)] ?? 0)
        : this.int(80);
    const kind = this.int(100);
    if (kind < 35) {
      const count = 1 + this.int(5);
      const rows = Array.from({ length: count }, () => ({
        id: this.chance(0.15) ? pick() : this.int(80),
        u: this.maybe(120, 0.4),
        v: this.maybe(10),
        s: this.text(),
      }));
      return {
        facts: {
          kind: "insert",
          table: "t",
          newKeys: rows.map((row) => row.id),
          matchedKeys: [],
          references: [],
        },
        sql: `INSERT INTO t VALUES ${rows.map((row) => `(${sql(row.id)}, ${sql(row.u)}, ${sql(row.v)}, ${sql(row.s)})`).join(", ")}`,
        run: (statement, ids) =>
          Promise.resolve(
            statement
              .table(ids.t)
              .insert(
                batch(
                  ints(rows.map((row) => row.id)),
                  ints(rows.map((row) => row.u)),
                  ints(rows.map((row) => row.v)),
                  texts(rows.map((row) => row.s)),
                ),
                null,
              ),
          ),
      };
    }
    if (kind < 55) {
      const id = pick();
      const u = this.maybe(120, 0.3);
      const v = this.maybe(10);
      return {
        facts: { kind: "update", table: "t", newKeys: [], matchedKeys: [id], references: [] },
        sql: `UPDATE t SET u = ${sql(u)}, v = ${sql(v)} WHERE id = ${String(id)}`,
        run: async (statement, ids) => {
          const at = await locate(statement.view, ids.t, primaryOf(statement, ids.t), id);
          if (at === null) return;
          await statement
            .table(ids.t, { assigned: [ids.u, ids.v] })
            .update(batch(locators([at], ids.t), ints([u]), ints([v])), null);
        },
      };
    }
    if (kind < 62) {
      // A rekey, only of a parent no child references (see Limits).
      const free = parents.filter((id) => (children.get(id) ?? 0) === 0);
      const id = free.length > 0 ? (free[this.int(free.length)] ?? 0) : this.int(80) + 1000;
      const next = this.chance(0.3) ? pick() : this.int(80);
      if ((children.get(next) ?? 0) > 0 || (children.get(id) ?? 0) > 0)
        return this.op(parents, children);
      return {
        facts: { kind: "rekey", table: "t", newKeys: [next], matchedKeys: [id], references: [] },
        sql: `UPDATE t SET id = ${String(next)} WHERE id = ${String(id)}`,
        run: async (statement, ids, step) => {
          const def = statement.catalog.table(ids.t);
          const at = await locate(statement.view, ids.t, primaryOf(statement, ids.t), id);
          if (at === null) return;
          await statement
            .table(ids.t, { assigned: [def?.columns[0]?.id ?? 0] })
            .update(batch(locators([at], ids.t), ints([next])), null);
          if (next !== id) step.moved.push([id, next]);
        },
      };
    }
    if (kind < 77) {
      const id = pick();
      return {
        facts: { kind: "delete", table: "t", newKeys: [], matchedKeys: [id], references: [] },
        sql: `DELETE FROM t WHERE id = ${String(id)}`,
        run: async (statement, ids) => {
          const at = await locate(statement.view, ids.t, primaryOf(statement, ids.t), id);
          if (at === null) return;
          await statement.table(ids.t).delete(batch(locators([at], ids.t)), null);
        },
      };
    }
    if (kind < 92) {
      const id = this.int(60);
      const pid = this.chance(0.15) ? null : pick();
      return {
        facts: {
          kind: "insert",
          table: "c",
          newKeys: [id],
          matchedKeys: [],
          references: pid === null ? [] : [{ table: "t", value: pid }],
        },
        sql: `INSERT INTO c VALUES (${String(id)}, ${sql(pid)})`,
        run: (statement, ids) =>
          Promise.resolve(statement.table(ids.c).insert(batch(ints([id]), ints([pid])), null)),
      };
    }
    const id = this.int(60);
    return {
      facts: { kind: "delete", table: "c", newKeys: [], matchedKeys: [id], references: [] },
      sql: `DELETE FROM c WHERE id = ${String(id)}`,
      run: async (statement, ids) => {
        const at = await locate(statement.view, ids.c, primaryOf(statement, ids.c), id);
        if (at === null) return;
        await statement.table(ids.c).delete(batch(locators([at], ids.c)), null);
      },
    };
  }
}

function primaryOf(statement: Statement, table: number): number {
  return statement.catalog.table(table)?.primaryKey ?? 0;
}

const SCHEMA = [
  "CREATE TABLE t(id INTEGER PRIMARY KEY, u INTEGER UNIQUE, v INTEGER, s TEXT)",
  "CREATE INDEX t_v ON t(v)",
  "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES t(id))",
];

async function setUp(path: WritePathImpl): Promise<Ids> {
  let t = 0;
  let c = 0;
  await transaction(path, false, async (statement) => {
    t =
      (
        await statement.changeCatalog({
          kind: "createTable",
          table: tableInput(
            "t",
            [column("id", INT, false), column("u", INT), column("v", INT), column("s", TEXT)],
            {
              unique: [{ name: "t_u", columns: [1] }],
            },
          ),
        })
      ).id ?? 0;
  });
  const def = path.catalog.table(t);
  const v = def?.columns[2]?.id ?? 0;
  await transaction(path, false, (statement) =>
    statement.changeCatalog({
      kind: "createIndex",
      index: { tableId: t, name: "t_v", columnIds: [v], unique: false, role: "secondary" },
    }),
  );
  await transaction(path, false, async (statement) => {
    c =
      (
        await statement.changeCatalog({
          kind: "createTable",
          table: tableInput("c", [column("id", INT, false), column("pid", INT)], {
            foreignKeys: [
              {
                name: "fk",
                columns: [1],
                parentTableId: t,
                parentIndexId: "primary",
                onDelete: "restrict",
                enforced: true,
              },
            ],
          }),
        })
      ).id ?? 0;
  });
  const vIndex = path.catalog.table(t)?.indexes.find((index) => index.name === "t_v")?.id ?? 0;
  return { t, c, u: def?.columns[1]?.id ?? 0, v, vIndex };
}

/**
 * Runs statements in one write-path transaction that commits (or, with `rollback`, rolls back);
 * the first failure rolls the whole of it back, as the oracles' ROLLBACK does.
 */
async function transaction(
  path: WritePathImpl,
  rollback: boolean,
  ...works: Array<(statement: Statement) => unknown>
): Promise<Outcome> {
  const tx = await path.begin();
  for (const work of works) {
    const statement = tx.statement({ rules: rulesFor });
    try {
      await work(statement);
      await statement.end();
    } catch (error) {
      statement.rollback();
      tx.rollback();
      return classifyNext(error);
    }
  }
  if (rollback) {
    tx.rollback();
    return "ok";
  }
  const receipt = await tx.commit(null);
  await receipt.durable;
  return "ok";
}

/** The write path's rows and index answers, checked against each oracle's rows. */
async function compare(
  path: WritePathImpl,
  oracles: readonly Oracle[],
  ids: Ids,
  workload: Workload,
  step: string,
): Promise<number[]> {
  const view = path.pin();
  try {
    const parents = byId(await rowsOf(view, ids.t));
    const children = byId(await rowsOf(view, ids.c));
    for (const oracle of oracles) {
      expect(parents, `t after ${step} (${oracle.name})`).toEqual(
        await oracleRows(oracle, "t", "id, u, v, s"),
      );
      expect(children, `c after ${step} (${oracle.name})`).toEqual(
        await oracleRows(oracle, "c", "id, pid"),
      );
    }
    // The indexes agree with the rows: the primary key, the UNIQUE column, the secondary index.
    const def = path.catalog.table(ids.t);
    const keys = parents.map((row) => Number(row[0]));
    if (keys.length > 0)
      expect(
        await probeAll(view, ids.t, def?.primaryKey ?? 0, keys),
        `pk probe after ${step}`,
      ).toBe(keys.length);
    const uIndex =
      def?.indexes.find((index) => index.unique && index.id !== def.primaryKey)?.id ?? 0;
    const us = Array.from({ length: 6 }, () => workload.int(120));
    const expectedU = us.filter((u) => parents.some((row) => row[1] === u)).length;
    expect(await probeAll(view, ids.t, uIndex, us), `unique probe after ${step}`).toBe(expectedU);
    const vs = [...new Set(Array.from({ length: 4 }, () => workload.int(10)))];
    const expectedV = parents.filter(
      (row) => vs.includes(Number(row[2])) && row[2] !== null,
    ).length;
    expect(await probeAll(view, ids.t, ids.vIndex, vs), `secondary probe after ${step}`).toBe(
      expectedV,
    );
    return keys;
  } finally {
    view.release();
  }
}

/** Children per parent id, from the write path's own (just compared) rows. */
async function childCounts(path: WritePathImpl, ids: Ids): Promise<Map<number, number>> {
  const view = path.pin();
  const counts = new Map<number, number>();
  for (const row of await rowsOf(view, ids.c))
    if (row[1] !== null) counts.set(Number(row[1]), (counts.get(Number(row[1])) ?? 0) + 1);
  view.release();
  return counts;
}

interface Observed {
  readonly step: string;
  readonly divergence: Divergence;
  readonly current: string | null;
}

/**
 * Runs a step on the current engine inside a transaction; commits only when its outcome, and (for
 * a committed step) its rows, are PostgreSQL's. Returns whether it matched, and its error.
 */
async function stepOnCurrent(
  current: Oracle,
  sqls: readonly string[],
  rollback: boolean,
  expected: { readonly outcome: Outcome },
  rows: { readonly t: Value[][]; readonly c: Value[][] },
): Promise<{
  readonly same: boolean;
  readonly outcome: Outcome;
  readonly error: string | null;
  readonly statement: number | null;
  readonly rowsDiffer: boolean;
  readonly differingRows: DifferingRow[];
}> {
  await current.execute("BEGIN");
  let outcome: Outcome = "ok";
  let error: string | null = null;
  let statement: number | null = null;
  for (const [at, text] of sqls.entries())
    try {
      await current.execute(text);
    } catch (caught) {
      outcome = current.classify(caught);
      error = errorText(caught);
      statement = at;
      break;
    }
  let same = outcome === expected.outcome;
  let rowsDiffer = false;
  const differingRows: DifferingRow[] = [];
  if (same && outcome === "ok" && !rollback) {
    const t = await oracleRows(current, "t", "id, u, v, s");
    const c = await oracleRows(current, "c", "id, pid");
    differingRows.push(...differing("t", t, rows.t, null), ...differing("c", c, rows.c, 1));
    rowsDiffer = differingRows.length > 0;
    same = !rowsDiffer;
  }
  await current.execute(same && outcome === "ok" && !rollback ? "COMMIT" : "ROLLBACK");
  return { same, outcome, error, statement, rowsDiffer, differingRows };
}

/**
 * Rows of `table` (value arrays, the key first) that differ between two sides, by key; `parent` is
 * the column holding a reference to t, whose values on either side the row reports.
 */
function differing(
  table: string,
  left: readonly Value[][],
  right: readonly Value[][],
  parent: number | null,
): DifferingRow[] {
  const byKey = (rows: readonly Value[][]) =>
    new Map(rows.map((row) => [Number(row[0]), row] as const));
  const a = byKey(left);
  const b = byKey(right);
  const out: DifferingRow[] = [];
  for (const key of new Set([...a.keys(), ...b.keys()])) {
    const x = a.get(key);
    const y = b.get(key);
    if (JSON.stringify(x) === JSON.stringify(y)) continue;
    const references =
      parent === null
        ? []
        : [x?.[parent], y?.[parent]]
            .filter((value): value is number => typeof value === "number")
            .map((value) => ({ table: "t", value }));
    out.push({ table, key, references });
  }
  return out;
}

describe("write path vs PostgreSQL and the current engine (generated)", () => {
  const observed: Observed[] = [];
  let compared = 0;
  for (const seed of SEEDS)
    it(`seed ${String(seed)}`, async () => {
      const workload = new Workload(seed);
      const harness = new Harness("strict", "sim", 16);
      let path = await harness.open();
      const pg = await postgres();
      const clock = { ms: Date.UTC(2026, 0, 1) };
      const current = currentEngine(clock);
      for (const text of SCHEMA) {
        await pg.execute(text);
        await current.execute(text);
      }
      const ids = await setUp(path);
      let parents: number[] = [];
      /** Keys of t a committed rekey moved away from → when (the current engine's clock). */
      const rekeyedAway = new Map<number, number>();
      /** Where each of those keys moved. */
      const movedTo = new Map<number, number>();
      const outcomes = new Map<Outcome, number>();
      try {
        for (let step = 0; step < STEPS; step++) {
          const oracles = [pg, current];
          const roll = workload.int(100);
          if (roll < 4) {
            path = workload.chance(0.5) ? await harness.reopen() : await harness.crash();
            parents = await compare(path, oracles, ids, workload, `reopen at ${String(step)}`);
            continue;
          }
          const count = roll < 20 ? 2 + workload.int(2) : 1;
          const children = await childCounts(path, ids);
          const ops = Array.from({ length: count }, () => workload.op(parents, children));
          const rollback = count > 1 && workload.chance(0.3);
          const sqls = ops.map((op) => op.sql);
          const label = `step ${String(step)}: ${sqls.join("; ")}${rollback ? " (rolled back)" : ""}`;
          const expected = await runOn(pg, sqls, rollback);
          const log: StepLog = { moved: [] };
          const actual = await transaction(
            path,
            rollback,
            ...ops.map((op) => (statement: Statement) => op.run(statement, ids, log)),
          );
          expect(actual, `${label} (PostgreSQL: ${String(expected.message)})`).toBe(
            expected.outcome,
          );
          outcomes.set(actual, (outcomes.get(actual) ?? 0) + 1);
          const rows = {
            t: await oracleRows(pg, "t", "id, u, v, s"),
            c: await oracleRows(pg, "c", "id, pid"),
          };
          // The current engine: equal to PostgreSQL, or an intended divergence (R21).
          let result = await stepOnCurrent(current, sqls, rollback, expected, rows);
          if (!result.same) {
            const divergence = intendedDivergence({
              statements: ops.map((op) => ({ sql: op.sql, ...op.facts })),
              oracle: {
                outcome: result.outcome,
                message: result.error,
                statement: result.statement,
              },
              authority: expected,
              rowsDiffer: result.rowsDiffer,
              differingRows: result.differingRows,
              facts: {
                nowMs: clock.ms,
                primaryKeys: new Map([
                  ["t", "id"],
                  ["c", "id"],
                ]),
                rekeyedAway: new Map([["t", rekeyedAway]]),
                movedTo: new Map([["t", movedTo]]),
              },
            });
            expect(
              divergence?.id,
              `the current engine differs from PostgreSQL at ${label}: ${String(result.error)} vs ${String(expected.message)} (unregistered)`,
            ).toBeDefined();
            if (divergence !== null)
              observed.push({
                step: `seed ${String(seed)} ${label}`,
                divergence,
                current: result.error,
              });
            // Past the forward window the current engine must match PostgreSQL exactly.
            clock.ms += DEFAULT_KEY_FORWARD_RETENTION_MS + 1;
            rekeyedAway.clear();
            movedTo.clear();
            result = await stepOnCurrent(current, sqls, rollback, expected, rows);
            expect(
              result.same,
              `${label}: still differs once forwards expired (${String(result.error)})`,
            ).toBe(true);
          }
          compared += 1;
          if (expected.outcome === "ok" && !rollback)
            for (const [from, to] of log.moved) {
              rekeyedAway.set(from, clock.ms);
              movedTo.set(from, to);
            }
          parents = await compare(path, oracles, ids, workload, label);
        }
        await path.quiesce();
        path = await harness.reopen();
        await compare(path, [pg, current], ids, workload, "the final reopen");
        await harness.space.audit();
        // The workload reached every outcome, so the comparison is not vacuous.
        expect(outcomes.get("ok") ?? 0).toBeGreaterThan(STEPS / 4);
        expect(outcomes.get("unique") ?? 0).toBeGreaterThan(0);
        expect(outcomes.get("foreignKey") ?? 0).toBeGreaterThan(0);
      } finally {
        await harness.close();
        await pg.close();
        await current.close();
      }
    }, 180_000);

  it("compared every step with the current engine, and met the registered divergence", () => {
    if (process.env.MINNOW_SEED !== undefined) return;
    console.info(
      `intended divergences (R21):\n${observed.map((o) => `  ${o.divergence.id} at ${o.step}: ${String(o.current)}`).join("\n")}\ncompared with the current engine: ${String(compared)} steps`,
    );
    expect(observed.length).toBeGreaterThan(0);
    expect(new Set(observed.map((o) => o.divergence.id))).toEqual(new Set(["key-forwarding"]));
  });
});
