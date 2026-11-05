/**
 * The generated differential (review B3-p2a-1): seeded random workloads of the statements the
 * write path supports today, run against the write path, PostgreSQL (PGlite: the law) and the
 * current engine (`MinnowDatabase`) side by side. After every step the write path must agree with
 * PostgreSQL on each table's rows, on which statements failed and why (unique / foreign key), and
 * on what the indexes return; and with the current engine on all of it until the current engine
 * first disagrees with PostgreSQL. That divergence is recorded, must be a known current-engine
 * defect (`KNOWN_ORACLE_DEFECTS`), and ends the comparison with the current engine for the seed
 * (its state differs from then on).
 *
 * Statements: multi-row INSERT (duplicate keys, in the batch and against stored rows; NULLs in a
 * UNIQUE column), UPDATE by primary key (of a UNIQUE and an indexed column, and of the key itself
 * when no child references it), DELETE by primary key (refused while children reference it), child
 * INSERT / DELETE under a RESTRICT foreign key, explicit transactions that commit or roll back,
 * and reopen / crash of the write path. Small memtables put flushes, publishes and replay in the
 * middle of every run.
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
import type { WritePathImpl } from "./engine.js";
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
} from "./test-support.js";

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

function currentEngine(): Oracle {
  const db = new MinnowDatabase(new MemoryBlockStore(), {
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
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

/**
 * Current-engine results PostgreSQL contradicts, found by this suite. The write path follows
 * PostgreSQL; the comparison with the current engine ends at the first one.
 */
const KNOWN_ORACLE_DEFECTS: ReadonlyArray<(message: string, history: History) => boolean> = [
  // After `UPDATE t SET id = 15 WHERE id = 51`, the current engine still refuses a new row with
  // id 51 ("Duplicate value for t.id: 51"); PostgreSQL inserts it.
  (message, history) => {
    const match = /^Duplicate value for t\.id: (\d+)$/u.exec(message);
    return match !== null && history.rekeyedAway.has(Number(match[1]));
  },
];

/** What PostgreSQL has accepted so far, for recognising a known current-engine defect. */
interface History {
  readonly rekeyedAway: Set<number>;
}

/** Runs one generated step on an oracle: its statements in a transaction when there are several. */
async function runOn(
  oracle: Oracle,
  sqls: readonly string[],
  rollback: boolean,
): Promise<{ outcome: Outcome; message: string | null }> {
  const transaction = sqls.length > 1;
  if (transaction) await oracle.execute("BEGIN");
  for (const text of sqls)
    try {
      await oracle.execute(text);
    } catch (error) {
      if (transaction) await oracle.execute("ROLLBACK");
      return {
        outcome: oracle.classify(error),
        message: error instanceof Error ? error.message : String(error),
      };
    }
  if (transaction) await oracle.execute(rollback ? "ROLLBACK" : "COMMIT");
  return { outcome: "ok", message: null };
}

const sql = (value: number | string | null) =>
  value === null ? "NULL" : typeof value === "number" ? String(value) : `'${value}'`;

/** One generated operation: its SQL for the oracle and its write-path form. */
interface Op {
  readonly sql: string;
  readonly run: (statement: Statement, ids: Ids) => Promise<unknown>;
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
        sql: `UPDATE t SET id = ${String(next)} WHERE id = ${String(id)}`,
        run: async (statement, ids) => {
          const def = statement.catalog.table(ids.t);
          const at = await locate(statement.view, ids.t, primaryOf(statement, ids.t), id);
          if (at === null) return;
          await statement
            .table(ids.t, { assigned: [def?.columns[0]?.id ?? 0] })
            .update(batch(locators([at], ids.t), ints([next])), null);
        },
      };
    }
    if (kind < 77) {
      const id = pick();
      return {
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
        sql: `INSERT INTO c VALUES (${String(id)}, ${sql(pid)})`,
        run: (statement, ids) =>
          Promise.resolve(statement.table(ids.c).insert(batch(ints([id]), ints([pid])), null)),
      };
    }
    const id = this.int(60);
    return {
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

interface Divergence {
  readonly step: string;
  readonly current: string | null;
  readonly postgres: string | null;
}

describe("write path vs PostgreSQL and the current engine (generated)", () => {
  const divergences: Divergence[] = [];
  let lockstep = 0;
  for (const seed of SEEDS)
    it(`seed ${String(seed)}`, async () => {
      const workload = new Workload(seed);
      const harness = new Harness("strict", "sim", 16);
      let path = await harness.open();
      const pg = await postgres();
      let current: Oracle | null = currentEngine();
      for (const text of SCHEMA) {
        await pg.execute(text);
        await current.execute(text);
      }
      const ids = await setUp(path);
      let parents: number[] = [];
      const history: History = { rekeyedAway: new Set() };
      const outcomes = new Map<Outcome, number>();
      try {
        for (let step = 0; step < STEPS; step++) {
          const oracles = current === null ? [pg] : [pg, current];
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
          if (expected.outcome === "ok" && !rollback)
            for (const text of sqls) {
              const rekey = /^UPDATE t SET id = \d+ WHERE id = (\d+)$/u.exec(text);
              if (rekey !== null) history.rekeyedAway.add(Number(rekey[1]));
            }
          if (current !== null) {
            const other = await runOn(current, sqls, rollback);
            if (other.outcome !== expected.outcome) {
              // The current engine contradicts PostgreSQL: a known defect, or a new finding.
              divergences.push({
                step: `seed ${String(seed)} ${label}`,
                current: other.message,
                postgres: expected.message,
              });
              expect(
                KNOWN_ORACLE_DEFECTS.some((defect) => defect(other.message ?? "", history)),
                `the current engine diverges from PostgreSQL at ${label}: ${String(other.message)} vs ${String(expected.message)}`,
              ).toBe(true);
              await current.close();
              current = null;
            } else lockstep += 1;
          }
          const actual = await transaction(
            path,
            rollback,
            ...ops.map((op) => (statement: Statement) => op.run(statement, ids)),
          );
          expect(actual, `${label} (PostgreSQL: ${String(expected.message)})`).toBe(
            expected.outcome,
          );
          outcomes.set(actual, (outcomes.get(actual) ?? 0) + 1);
          parents = await compare(
            path,
            current === null ? [pg] : [pg, current],
            ids,
            workload,
            label,
          );
        }
        await path.quiesce();
        path = await harness.reopen();
        await compare(
          path,
          current === null ? [pg] : [pg, current],
          ids,
          workload,
          "the final reopen",
        );
        await harness.space.audit();
        // The workload reached every outcome, so the comparison is not vacuous.
        expect(outcomes.get("ok") ?? 0).toBeGreaterThan(STEPS / 4);
        expect(outcomes.get("unique") ?? 0).toBeGreaterThan(0);
        expect(outcomes.get("foreignKey") ?? 0).toBeGreaterThan(0);
      } finally {
        await harness.close();
        await pg.close();
        await current?.close();
      }
    }, 120_000);

  it("compared most steps with the current engine too", () => {
    if (process.env.MINNOW_SEED !== undefined) return;
    if (divergences.length > 0)
      console.info(
        `current-engine divergences from PostgreSQL:\n${divergences.map((d) => `  ${d.step}: ${String(d.current)}`).join("\n")}`,
      );
    console.info(
      `lockstep steps with the current engine: ${String(lockstep)} of ${String(SEEDS.length * STEPS)}`,
    );
    expect(lockstep).toBeGreaterThan((SEEDS.length * STEPS) / 4);
  });
});
