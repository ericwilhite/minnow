/**
 * Constraint semantics against PostgreSQL. Each scenario's statements run on Minnow and on
 * PGlite; every statement's outcome (accepted or refused) and every table's final rows must
 * agree. The scenarios are the ones the seeded DML harness does not script: UNIQUE with NULLs
 * (column and composite index), CHECK with a NULL result, NOT NULL with defaults, generated
 * columns, FK cascade chains that meet RESTRICT, SET NULL, composite keys with a NULL component,
 * self-referential cascades, statement atomicity, upsert arbiters, cascaded deletes firing child
 * triggers, and BEGIN … ROLLBACK around foreign keys. A scenario tagged `divergence` documents a
 * difference Minnow chooses or does not support; its statements must still match that tag.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

interface Scenario {
  readonly name: string;
  readonly setup: readonly string[];
  readonly pgSetup?: readonly string[];
  readonly steps: readonly string[];
  readonly tables: readonly string[];
  /** A documented difference from PostgreSQL: the scenario must differ, and only there. */
  readonly divergence?: string;
}

const SCENARIOS: Scenario[] = [
  {
    name: "unique-nulls",
    setup: [
      "CREATE TABLE u (id INTEGER PRIMARY KEY, a TEXT UNIQUE, b INTEGER, c INTEGER)",
      "CREATE UNIQUE INDEX ubc ON u(b, c)",
    ],
    steps: [
      "INSERT INTO u VALUES (1, NULL, 1, NULL)",
      "INSERT INTO u VALUES (2, NULL, 1, NULL)",
      "INSERT INTO u VALUES (3, 'x', 2, 2)",
      "INSERT INTO u VALUES (4, 'x', 3, 3)",
      "INSERT INTO u VALUES (5, 'y', 2, 2)",
      "INSERT INTO u VALUES (6, 'z', NULL, 2)",
      "INSERT INTO u VALUES (7, 'w', NULL, 2)",
      "UPDATE u SET a = 'x' WHERE id = 2",
      "UPDATE u SET a = NULL WHERE id = 3",
      "INSERT INTO u VALUES (8, 'x', 9, 9)",
      "UPDATE u SET b = 1, c = NULL WHERE id = 8",
      "UPDATE u SET c = 2 WHERE id = 6",
    ],
    tables: ["u"],
  },
  {
    name: "check-null",
    setup: [
      "CREATE TABLE k (id INTEGER PRIMARY KEY, q INTEGER CHECK (q > 0), r INTEGER, CHECK (q + r > 0))",
    ],
    steps: [
      "INSERT INTO k VALUES (1, NULL, NULL)",
      "INSERT INTO k VALUES (2, 0, 5)",
      "INSERT INTO k VALUES (3, 5, NULL)",
      "INSERT INTO k VALUES (4, 1, -5)",
      "UPDATE k SET q = -1 WHERE id = 3",
      "UPDATE k SET q = 2 WHERE id = 3",
      "UPDATE k SET r = -2 WHERE id = 3",
      "INSERT INTO k VALUES (3, 9, 9) ON CONFLICT (id) DO UPDATE SET q = -9",
      "INSERT INTO k VALUES (3, 9, 9) ON CONFLICT (id) DO UPDATE SET q = 9, r = 1",
      "UPDATE k SET q = NULL WHERE id = 3",
    ],
    tables: ["k"],
  },
  {
    name: "not-null-default",
    setup: [
      "CREATE TABLE d (id INTEGER PRIMARY KEY, s TEXT NOT NULL DEFAULT 'x', n INTEGER DEFAULT 5, m INTEGER NOT NULL, e INTEGER NOT NULL DEFAULT (1 + 1))",
    ],
    steps: [
      "INSERT INTO d (id, m) VALUES (1, 1)",
      "INSERT INTO d (id, s, m) VALUES (2, NULL, 1)",
      "INSERT INTO d (id, s, n, m, e) VALUES (3, DEFAULT, DEFAULT, 2, DEFAULT)",
      "INSERT INTO d (id, m) VALUES (4, NULL)",
      "UPDATE d SET s = DEFAULT, n = NULL WHERE id = 1",
      "UPDATE d SET m = NULL WHERE id = 1",
      "UPDATE d SET s = NULL WHERE id = 3",
      "INSERT INTO d VALUES (5, 'a', 1, 1, 1)",
      "INSERT INTO d (id, m, n) VALUES (6, 1, NULL)",
      "INSERT INTO d DEFAULT VALUES",
    ],
    tables: ["d"],
  },
  {
    name: "generated",
    setup: [
      "CREATE TABLE g (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    ],
    steps: [
      "INSERT INTO g (id, a) VALUES (1, 5)",
      "INSERT INTO g (id, a, b) VALUES (2, 1, 99)",
      "UPDATE g SET a = 7 WHERE id = 1",
      "UPDATE g SET b = 3 WHERE id = 1",
      "INSERT INTO g (id, a) VALUES (3, NULL)",
      "INSERT INTO g (id, a) VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a",
      "INSERT INTO g (id, a, b) VALUES (4, 1, DEFAULT)",
      "UPDATE g SET b = DEFAULT WHERE id = 1",
      "UPDATE g SET b = DEFAULT",
      "UPDATE g SET a = 8, b = DEFAULT WHERE id = 4",
    ],
    tables: ["g"],
  },
  {
    name: "fk-cascade-chain-restrict",
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY, l TEXT)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE)",
      "CREATE TABLE gc (id INTEGER PRIMARY KEY, cid INTEGER REFERENCES c(id) ON DELETE CASCADE)",
      "CREATE TABLE r (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
    ],
    steps: [
      "INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')",
      "INSERT INTO c VALUES (1, 1), (2, 1), (3, 2), (4, 3)",
      "INSERT INTO gc VALUES (1, 1), (2, 3), (3, 4)",
      "INSERT INTO c VALUES (9, 99)",
      "DELETE FROM p WHERE id = 1",
      "INSERT INTO r VALUES (1, 2)",
      "DELETE FROM p WHERE id = 2",
      "DELETE FROM p WHERE id IN (2, 3)",
      "DELETE FROM r",
      "DELETE FROM p WHERE id = 2",
      "UPDATE c SET pid = 77 WHERE id = 4",
      "UPDATE c SET pid = NULL WHERE id = 4",
      "DELETE FROM p",
    ],
    tables: ["p", "c", "gc", "r"],
  },
  {
    name: "fk-set-null",
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE SET NULL, w INTEGER)",
    ],
    steps: [
      "INSERT INTO p VALUES (1), (2)",
      "INSERT INTO c VALUES (1, 1, 1), (2, 1, 2), (3, 2, 3), (4, NULL, 4)",
      "DELETE FROM p WHERE id = 1",
      "DELETE FROM p",
    ],
    tables: ["p", "c"],
  },
  {
    name: "fk-composite-partial-null",
    setup: [
      "CREATE TABLE p (a INTEGER, b INTEGER, PRIMARY KEY (a, b))",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES p(a, b) ON DELETE CASCADE)",
    ],
    steps: [
      "INSERT INTO p VALUES (1, 1), (1, 2)",
      "INSERT INTO c VALUES (1, NULL, NULL)",
      "INSERT INTO c VALUES (2, 1, NULL)",
      "INSERT INTO c VALUES (3, 9, 9)",
      "INSERT INTO c VALUES (4, 1, 1)",
      "INSERT INTO c VALUES (5, 1, 2)",
      "DELETE FROM p WHERE a = 1 AND b = 1",
      "UPDATE c SET b = 9 WHERE id = 5",
      "UPDATE c SET a = NULL WHERE id = 5",
    ],
    tables: ["p", "c"],
  },
  {
    name: "self-ref-cascade",
    setup: [
      "CREATE TABLE tree (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES tree(id) ON DELETE CASCADE)",
    ],
    steps: [
      "INSERT INTO tree VALUES (1, NULL)",
      "INSERT INTO tree VALUES (2, 1), (4, 1)",
      "INSERT INTO tree VALUES (3, 2)",
      "INSERT INTO tree VALUES (5, 42)",
      "DELETE FROM tree WHERE id = 2",
      "INSERT INTO tree VALUES (6, 4)",
      "DELETE FROM tree WHERE id = 1",
    ],
    tables: ["tree"],
  },
  {
    name: "atomic-multirow",
    setup: [
      "CREATE TABLE u2 (id INTEGER PRIMARY KEY, a TEXT UNIQUE)",
      "CREATE TABLE k2 (id INTEGER PRIMARY KEY, q INTEGER CHECK (q > 0))",
      "CREATE TABLE src (id INTEGER PRIMARY KEY, q INTEGER)",
    ],
    steps: [
      "INSERT INTO u2 VALUES (1, 'a'), (2, 'b'), (3, 'a')",
      "INSERT INTO u2 VALUES (1, 'a'), (2, 'b'), (1, 'c')",
      "INSERT INTO src VALUES (1, 1), (2, -1), (3, 3)",
      "INSERT INTO k2 SELECT * FROM src",
      "INSERT INTO k2 SELECT * FROM src WHERE q > 0",
      "UPDATE k2 SET q = q - 2",
      "UPDATE k2 SET q = q - 2 WHERE id = 3",
    ],
    tables: ["u2", "k2"],
  },
  {
    name: "upsert-arbiters",
    setup: ["CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE, v INTEGER)"],
    steps: [
      "INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2)",
      "INSERT INTO t VALUES (1, 'b', 9) ON CONFLICT (id) DO UPDATE SET code = EXCLUDED.code",
      "INSERT INTO t VALUES (9, 'a', 5) ON CONFLICT (code) DO UPDATE SET v = EXCLUDED.v",
      "INSERT INTO t VALUES (9, 'c', 7) ON CONFLICT (code) DO NOTHING",
      "INSERT INTO t VALUES (1, 'zz', 0) ON CONFLICT (code) DO NOTHING",
      "INSERT INTO t VALUES (1, 'zz', 0) ON CONFLICT DO NOTHING",
      "INSERT INTO t VALUES (2, 'q', 50) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE t.v < EXCLUDED.v",
      "INSERT INTO t VALUES (2, 'q', 10) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE t.v < EXCLUDED.v",
    ],
    tables: ["t"],
  },
  {
    name: "upsert-rekeys-target",
    setup: ["CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE, v INTEGER)"],
    steps: [
      "INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2)",
      "INSERT INTO t VALUES (2, 'q', 60) ON CONFLICT (id) DO UPDATE SET id = 77",
    ],
    tables: ["t"],
    divergence: "ON CONFLICT DO UPDATE cannot reassign the conflict key; PostgreSQL moves the row",
  },
  {
    name: "unique-update",
    setup: ["CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, w INTEGER)"],
    steps: [
      "INSERT INTO t VALUES (1, 1, 0), (2, 2, 0)",
      "UPDATE t SET u = 3 WHERE id = 1",
      "UPDATE t SET u = 2 WHERE id = 1",
      "UPDATE t SET u = NULL",
      "INSERT INTO t VALUES (3, NULL, 0)",
      "UPDATE t SET u = 5 WHERE w = 0",
    ],
    tables: ["t"],
  },
  {
    name: "fk-to-unique-nonpk",
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pcode TEXT REFERENCES p(code) ON DELETE CASCADE)",
    ],
    steps: [
      "INSERT INTO p VALUES (1, 'a'), (2, 'b')",
      "INSERT INTO c VALUES (1, 'a'), (2, 'zz')",
      "INSERT INTO c VALUES (1, 'a'), (2, 'b')",
      "DELETE FROM p WHERE id = 1",
    ],
    tables: ["p", "c"],
    divergence: "a FOREIGN KEY must reference the parent's primary key",
  },
  {
    name: "fk-parent-rekey",
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
    ],
    steps: [
      "INSERT INTO p VALUES (1)",
      "INSERT INTO c VALUES (1, 1)",
      "UPDATE p SET id = 5 WHERE id = 1",
    ],
    tables: ["p", "c"],
    divergence: "a rekey moves referencing rows with it; PostgreSQL's NO ACTION refuses",
  },
  {
    name: "set-null-on-not-null",
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER NOT NULL REFERENCES p(id) ON DELETE SET NULL)",
    ],
    steps: [
      "INSERT INTO p VALUES (1)",
      "INSERT INTO c VALUES (1, 1)",
      "DELETE FROM p WHERE id = 1",
    ],
    tables: ["p", "c"],
    divergence:
      "ON DELETE SET NULL on a NOT NULL column is refused at DDL; PostgreSQL fails the delete",
  },
  {
    name: "cascade-fires-child-trigger",
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE)",
      "CREATE TABLE audit (cid INTEGER)",
      "CREATE TRIGGER c_del AFTER DELETE ON c BEGIN INSERT INTO audit (cid) VALUES (OLD.id); END",
    ],
    pgSetup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id) ON DELETE CASCADE)",
      "CREATE TABLE audit (cid INTEGER)",
      "CREATE FUNCTION c_del_f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit (cid) VALUES (OLD.id); RETURN OLD; END $$",
      "CREATE TRIGGER c_del AFTER DELETE ON c FOR EACH ROW EXECUTE FUNCTION c_del_f()",
    ],
    steps: [
      "INSERT INTO p VALUES (1), (2)",
      "INSERT INTO c VALUES (1, 1), (2, 1), (3, 2)",
      "DELETE FROM p WHERE id = 1",
      "DELETE FROM c WHERE id = 3",
    ],
    tables: ["p", "c", "audit"],
  },
  {
    name: "tx-fk-rollback",
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
    ],
    steps: [
      "BEGIN",
      "INSERT INTO p VALUES (1)",
      "INSERT INTO c VALUES (1, 1)",
      "ROLLBACK",
      "BEGIN",
      "INSERT INTO p VALUES (2)",
      "INSERT INTO c VALUES (2, 2)",
      "COMMIT",
      "BEGIN",
      "INSERT INTO c VALUES (3, 99)",
      "ROLLBACK",
      "BEGIN",
      "DELETE FROM c WHERE id = 2",
      "DELETE FROM p WHERE id = 2",
      "COMMIT",
    ],
    tables: ["p", "c"],
  },
  {
    name: "check-two-columns-update",
    setup: [
      "CREATE TABLE b (id INTEGER PRIMARY KEY, lo INTEGER, hi INTEGER, CONSTRAINT ordered CHECK (lo <= hi))",
    ],
    steps: [
      "INSERT INTO b VALUES (1, 1, 5)",
      "UPDATE b SET lo = 9 WHERE id = 1",
      "UPDATE b SET lo = 9, hi = 10 WHERE id = 1",
      "UPDATE b SET hi = NULL WHERE id = 1",
      "UPDATE b SET lo = 100 WHERE id = 1",
    ],
    tables: ["b"],
  },
  {
    name: "delete-parent-and-children-in-statement",
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY, g INTEGER)",
      "CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))",
    ],
    steps: [
      "INSERT INTO p VALUES (1, 1), (2, 1), (3, 2)",
      "INSERT INTO c VALUES (1, 1), (2, 2)",
      "DELETE FROM p WHERE g = 1",
      "DELETE FROM p WHERE g = 2",
      "UPDATE c SET pid = 3 WHERE id = 1",
    ],
    tables: ["p", "c"],
  },
];

type Rows = Array<Record<string, unknown>>;
interface Run {
  readonly setup: string[];
  readonly steps: string[];
  readonly tables: Record<string, string>;
}

const canon = (rows: Rows): string =>
  JSON.stringify(
    rows
      .map((row) =>
        Object.fromEntries(
          Object.keys(row)
            .sort()
            .map((key) => [key, typeof row[key] === "bigint" ? Number(row[key]) : row[key]]),
        ),
      )
      .sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1)),
  );
const outcome = (p: Promise<unknown>): Promise<string> =>
  p.then(
    () => "ok",
    (error: unknown) => `ERR ${String(error).slice(0, 100)}`,
  );

async function runMinnow(s: Scenario): Promise<Run> {
  const db = new MinnowDatabase(new MemoryBlockStore());
  const setup: string[] = [];
  const steps: string[] = [];
  const tables: Record<string, string> = {};
  try {
    for (const sql of s.setup) setup.push(await outcome(db.execute(sql)));
    for (const sql of s.steps) steps.push(await outcome(db.execute(sql)));
    for (const t of s.tables) {
      tables[t] = await db.query(`SELECT * FROM ${t}`, { memoize: false }).then(
        (x) => canon(x.rows as Rows),
        (error: unknown) => `ERR ${String(error).slice(0, 80)}`,
      );
    }
  } finally {
    await db.close();
  }
  return { setup, steps, tables };
}

async function runPostgres(s: Scenario): Promise<Run> {
  const { PGlite } = await import("@electric-sql/pglite");
  const pg = await PGlite.create();
  const setup: string[] = [];
  const steps: string[] = [];
  const tables: Record<string, string> = {};
  try {
    for (const sql of s.pgSetup ?? s.setup) setup.push(await outcome(pg.exec(sql)));
    for (const sql of s.steps) steps.push(await outcome(pg.exec(sql)));
    for (const t of s.tables) {
      tables[t] = await pg.query(`SELECT * FROM ${t}`).then(
        (x) => canon(x.rows as Rows),
        (error: unknown) => `ERR ${String(error).slice(0, 80)}`,
      );
    }
  } finally {
    await pg.close();
  }
  return { setup, steps, tables };
}

function differences(s: Scenario, m: Run, p: Run): string[] {
  const diffs: string[] = [];
  const ok = (x: string | undefined): boolean => x === "ok";
  s.setup.forEach((sql, i) => {
    if (ok(m.setup[i]) !== ok(p.setup[i])) {
      diffs.push(`SETUP ${sql}\n  minnow=${m.setup[i] ?? ""}\n  pg=${p.setup[i] ?? ""}`);
    }
  });
  s.steps.forEach((sql, i) => {
    if (ok(m.steps[i]) !== ok(p.steps[i])) {
      diffs.push(`STEP ${sql}\n  minnow=${m.steps[i] ?? ""}\n  pg=${p.steps[i] ?? ""}`);
    }
  });
  for (const t of s.tables) {
    if (m.tables[t] !== p.tables[t]) {
      diffs.push(`TABLE ${t}\n  minnow=${m.tables[t] ?? ""}\n  pg=${p.tables[t] ?? ""}`);
    }
  }
  return diffs;
}

describe("constraint semantics agree with PostgreSQL", () => {
  for (const scenario of SCENARIOS) {
    it(scenario.name, async () => {
      const [minnow, postgres] = await Promise.all([runMinnow(scenario), runPostgres(scenario)]);
      const diffs = differences(scenario, minnow, postgres);
      if (scenario.divergence === undefined) {
        expect(diffs, `${scenario.name}\n${diffs.join("\n")}`).toEqual([]);
      } else {
        // The divergence is deliberate and documented; this pins that it is still the whole story.
        expect(diffs.length, scenario.divergence).toBeGreaterThan(0);
      }
    });
  }
});

describe("UPDATE … SET generated = DEFAULT", () => {
  it("updates every matched row as PostgreSQL does, recomputing the generated value", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.execute(
        "CREATE TABLE g (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
      );
      await db.execute("CREATE TABLE touched (gid INTEGER)");
      await db.execute(
        "CREATE TRIGGER g_upd AFTER UPDATE ON g BEGIN INSERT INTO touched (gid) VALUES (NEW.id); END",
      );
      await db.execute("INSERT INTO g (id, a) VALUES (1, 5), (2, 6)");
      const one = await db.execute("UPDATE g SET b = DEFAULT WHERE id = 1");
      expect(one.kind).toBe("update");
      expect(one.kind === "update" ? one.rowCount : -1).toBe(1);
      const all = await db.execute("UPDATE g SET b = DEFAULT");
      expect(all.kind === "update" ? all.rowCount : -1).toBe(2);
      const none = await db.execute("UPDATE g SET b = DEFAULT WHERE id = 99");
      expect(none.kind === "update" ? none.rowCount : -1).toBe(0);
      expect((await db.query("SELECT id, a, b FROM g ORDER BY id")).rows).toEqual([
        { id: 1, a: 5, b: 10 },
        { id: 2, a: 6, b: 12 },
      ]);
      // The rows were updated, so the update trigger saw each of them.
      expect((await db.query("SELECT gid FROM touched ORDER BY gid")).rows).toEqual([
        { gid: 1 },
        { gid: 1 },
        { gid: 2 },
      ]);
      const returning = await db.execute("UPDATE g SET b = DEFAULT WHERE id = 2 RETURNING id, b");
      expect(returning.kind === "update" ? returning.returnedRows : undefined).toEqual([
        { id: 2, b: 12 },
      ]);
    } finally {
      await db.close();
    }
  });

  it("works when the table holds only its key and generated columns", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.execute(
        "CREATE TABLE h (id INTEGER PRIMARY KEY, twice INTEGER GENERATED ALWAYS AS (id * 2) STORED)",
      );
      await db.execute("INSERT INTO h (id) VALUES (1), (2)");
      const result = await db.execute("UPDATE h SET twice = DEFAULT WHERE id = 2");
      expect(result.kind === "update" ? result.rowCount : -1).toBe(1);
      expect((await db.query("SELECT id, twice FROM h ORDER BY id")).rows).toEqual([
        { id: 1, twice: 2 },
        { id: 2, twice: 4 },
      ]);
    } finally {
      await db.close();
    }
  });
});
