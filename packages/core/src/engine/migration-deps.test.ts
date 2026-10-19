/**
 * What a migration may not leave dangling, and what it must survive. Renames and drops through
 * `migrate()` and `ALTER TABLE` are refused while a CHECK, a generated column, a foreign key, a
 * view or a trigger names the column; a migration interrupted at any catalog write leaves a
 * catalog the next run completes; two connections migrating at once converge; and a declared
 * view is proven against the schema the migration produces before any step applies, so it may
 * read a column the same call adds, and an invalid one changes nothing.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { column, schema, table, view } from "./schema.js";

/** Throws from the Nth catalog-mutating store call, where migrations commit their steps. */
function catalogFaultStore(inner: BlockStore, failAt: () => number): BlockStore {
  let calls = 0;
  return new Proxy(inner, {
    get(target, property) {
      const value: unknown = Reflect.get(target, property, target);
      if (typeof value !== "function") return value;
      if (
        typeof property === "string" &&
        /Table|View|Index/i.test(property) &&
        !/^(get|list|read|find|count)/.test(property)
      ) {
        return (...args: unknown[]) => {
          calls += 1;
          if (calls === failAt())
            throw new Error(`injected fault in ${property} #${String(calls)}`);
          return (value as (...a: unknown[]) => unknown).apply(target, args);
        };
      }
      return (value as (...a: unknown[]) => unknown).bind(target);
    },
  });
}

const outcome = (p: Promise<unknown>): Promise<string> =>
  p.then(
    () => "ok",
    (error: unknown) => `ERR ${String(error).slice(0, 160)}`,
  );

describe("renames through migrate() refuse dependent catalog objects", () => {
  it("a column a CHECK names, whether the CHECK text is updated or not", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      const v1 = table(
        "t",
        { id: column.integer().unique(), qty: column.integer() },
        { checks: [{ name: "pos", sql: "qty > 0" }] },
      );
      await db.migrate(schema([v1]));
      await db.execute("INSERT INTO t VALUES (1, 5)");
      const stale = table(
        "t",
        { id: column.integer().unique(), quantity: column.integer().renamedFrom("qty") },
        { checks: [{ name: "pos", sql: "qty > 0" }] },
      );
      await expect(db.migrate(schema([stale]))).rejects.toThrow(/CHECK pos/);
      const updated = table(
        "t",
        { id: column.integer().unique(), quantity: column.integer().renamedFrom("qty") },
        { checks: [{ name: "pos", sql: "quantity > 0" }] },
      );
      await expect(db.migrate(schema([updated]))).rejects.toThrow(/CHECK pos prevents renaming/);
      expect((await db.query("SELECT * FROM t")).columns).toEqual(["id", "qty"]);
      await expect(db.execute("INSERT INTO t VALUES (2, -1)")).rejects.toThrow(/CHECK pos/);
    } finally {
      await db.close();
    }
  });

  it("a column a generated sibling reads, a child FK column, and a column a view selects", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      const p = table("p", { id: column.integer().unique(), label: column.string() });
      const t1 = table("t", {
        id: column.integer().unique(),
        a: column.integer(),
        b: column.integer().generatedSql("a * 2"),
        pid: column.integer().references("p", "id", { onDelete: "cascade" }),
        qty: column.integer(),
      });
      const v = view("v", {
        sql: "SELECT id, qty FROM t",
        columns: { id: column.integer(), qty: column.integer() },
      });
      await db.migrate(schema([p, t1], { views: [v] }));
      const renamedA = table("t", {
        id: column.integer().unique(),
        x: column.integer().renamedFrom("a"),
        b: column.integer().generatedSql("a * 2"),
        pid: column.integer().references("p", "id", { onDelete: "cascade" }),
        qty: column.integer(),
      });
      await expect(db.migrate(schema([p, renamedA], { views: [v] }))).rejects.toThrow(
        /Generated column t\.b/,
      );
      const renamedPid = table("t", {
        id: column.integer().unique(),
        a: column.integer(),
        b: column.integer().generatedSql("a * 2"),
        parent: column.integer().renamedFrom("pid").references("p", "id", { onDelete: "cascade" }),
        qty: column.integer(),
      });
      await expect(db.migrate(schema([p, renamedPid], { views: [v] }))).rejects.toThrow(
        /FOREIGN KEY .* prevents renaming/,
      );
      const renamedQty = table("t", {
        id: column.integer().unique(),
        a: column.integer(),
        b: column.integer().generatedSql("a * 2"),
        pid: column.integer().references("p", "id", { onDelete: "cascade" }),
        quantity: column.integer().renamedFrom("qty"),
      });
      await expect(db.migrate(schema([p, renamedQty], { views: [v] }))).rejects.toThrow(
        /view v depends on its current schema/,
      );
      expect((await db.query("SELECT * FROM t")).columns).toEqual(["id", "a", "b", "pid", "qty"]);
    } finally {
      await db.close();
    }
  });
});

describe("ALTER TABLE and DROP refuse dependent catalog objects", () => {
  async function fixture(): Promise<MinnowDatabase> {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, label TEXT)");
    await db.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL CHECK (qty > 0), pid INTEGER REFERENCES p(id), twice INTEGER GENERATED ALWAYS AS (qty * 2) STORED, note TEXT, spare TEXT)",
    );
    await db.execute("CREATE VIEW v AS SELECT id, note FROM t");
    await db.execute("CREATE TABLE audit (id INTEGER, n TEXT)");
    await db.execute(
      "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO audit (id, n) VALUES (NEW.id, NEW.spare); END",
    );
    return db;
  }

  it("RENAME COLUMN", async () => {
    const db = await fixture();
    try {
      await expect(db.execute("ALTER TABLE t RENAME COLUMN qty TO quantity")).rejects.toThrow(
        /CHECK/,
      );
      await expect(db.execute("ALTER TABLE t RENAME COLUMN pid TO parent")).rejects.toThrow(
        /FOREIGN KEY/,
      );
      await expect(db.execute("ALTER TABLE t RENAME COLUMN spare TO memo")).rejects.toThrow(
        /trigger tr/,
      );
      await expect(db.execute("ALTER TABLE t RENAME COLUMN note TO memo")).rejects.toThrow(
        /view v/,
      );
      await expect(db.execute("ALTER TABLE p RENAME COLUMN id TO pk")).rejects.toThrow(
        /FOREIGN KEY/,
      );
      await db.execute("ALTER TABLE p RENAME COLUMN label TO name");
      expect((await db.query("SELECT * FROM p")).columns).toEqual(["id", "name"]);
    } finally {
      await db.close();
    }
  });

  it("DROP COLUMN and DROP TABLE", async () => {
    const db = await fixture();
    try {
      await expect(db.execute("ALTER TABLE t DROP COLUMN qty")).rejects.toThrow(/CHECK/);
      await expect(db.execute("ALTER TABLE t DROP COLUMN pid")).rejects.toThrow(/FOREIGN KEY/);
      await expect(db.execute("ALTER TABLE t DROP COLUMN note")).rejects.toThrow(/view v/);
      await expect(db.execute("ALTER TABLE t DROP COLUMN spare")).rejects.toThrow(/trigger tr/);
      await db.execute("ALTER TABLE t DROP COLUMN twice");
      await expect(db.execute("DROP TABLE p")).rejects.toThrow(/foreign key/);
      await expect(db.execute("DROP TABLE t")).rejects.toThrow(/view v/);
      await expect(db.execute("DROP TABLE audit")).rejects.toThrow(/trigger tr/);
    } finally {
      await db.close();
    }
  });

  it("migrate() dropping a column a CHECK, generated column, FK or view names", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      const p = table("p", { id: column.integer().unique(), label: column.string() });
      const full = table(
        "t",
        {
          id: column.integer().unique(),
          qty: column.integer(),
          pid: column.integer().references("p", "id"),
          twice: column.integer().generatedSql("qty * 2"),
          note: column.string().nullable(),
        },
        { checks: [{ name: "pos", sql: "qty > 0" }] },
      );
      const vw = view("v", {
        sql: "SELECT id, note FROM t",
        columns: { id: column.integer(), note: column.string().nullable() },
      });
      await db.migrate(schema([p, full], { views: [vw] }));
      const withoutQty = table(
        "t",
        {
          id: column.integer().unique(),
          pid: column.integer().references("p", "id"),
          twice: column.integer().generatedSql("qty * 2"),
          note: column.string().nullable(),
        },
        { checks: [{ name: "pos", sql: "qty > 0" }] },
      );
      const withoutNote = table(
        "t",
        {
          id: column.integer().unique(),
          qty: column.integer(),
          pid: column.integer().references("p", "id"),
          twice: column.integer().generatedSql("qty * 2"),
        },
        { checks: [{ name: "pos", sql: "qty > 0" }] },
      );
      const withoutPid = table(
        "t",
        {
          id: column.integer().unique(),
          qty: column.integer(),
          twice: column.integer().generatedSql("qty * 2"),
          note: column.string().nullable(),
        },
        { checks: [{ name: "pos", sql: "qty > 0" }] },
      );
      const destructive = { allowDestructive: true };
      await expect(
        db.migrate(schema([p, withoutQty], { views: [vw] }), destructive),
      ).rejects.toThrow();
      await expect(
        db.migrate(schema([p, withoutNote], { views: [vw] }), destructive),
      ).rejects.toThrow(/view v/);
      await expect(
        db.migrate(schema([p, withoutPid], { views: [vw] }), destructive),
      ).rejects.toThrow(/FOREIGN KEY/);
      expect((await db.query("SELECT * FROM t")).columns).toEqual([
        "id",
        "qty",
        "pid",
        "twice",
        "note",
      ]);
    } finally {
      await db.close();
    }
  });
});

describe("views in a migration", () => {
  const a1 = table("a", { id: column.integer().unique(), x: column.integer() });
  const a2 = table("a", {
    id: column.integer().unique(),
    x: column.integer(),
    y: column.integer().nullable(),
  });
  const fresh = table("n", { id: column.integer().unique() });

  it("may read a column and a table the same migrate() adds", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.migrate(schema([a1]));
      await db.execute("INSERT INTO a VALUES (1, 10)");
      const overNew = view("ay", {
        sql: "SELECT a.id AS id, a.y AS y, n.id AS nid FROM a LEFT JOIN n ON n.id = a.id",
        columns: {
          id: column.integer(),
          y: column.integer().nullable(),
          nid: column.integer().nullable(),
        },
      });
      const result = await db.migrate(schema([a2, fresh], { views: [overNew] }));
      expect(result.createdTables).toEqual(["n"]);
      expect(result.alteredTables).toEqual(["a"]);
      expect(result.replacedViews).toEqual(["ay"]);
      expect((await db.query("SELECT * FROM ay")).rows).toEqual([{ id: 1, y: null, nid: null }]);
      // Idempotent: nothing is left to do.
      expect((await db.migrate(schema([a2, fresh], { views: [overNew] }))).steps).toEqual([]);
    } finally {
      await db.close();
    }
  });

  it("an invalid view fails the migration before any step applies, naming the view", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.migrate(schema([a1]));
      const broken = view("ay", {
        sql: "SELECT id, nope FROM a",
        columns: { id: column.integer(), nope: column.integer().nullable() },
      });
      await expect(db.migrate(schema([a2, fresh], { views: [broken] }))).rejects.toThrow(
        /View ay cannot be created: .*nope/,
      );
      // No table created, no column added, no view.
      expect((await db.query("SELECT * FROM a")).columns).toEqual(["id", "x"]);
      await expect(db.query("SELECT * FROM n")).rejects.toThrow(/Unknown table/);
      await expect(db.query("SELECT * FROM ay")).rejects.toThrow(/Unknown table/);
      // A declaration that lies about the columns fails the same way.
      const lying = view("ay", {
        sql: "SELECT id, y FROM a",
        columns: { id: column.integer(), y: column.string().nullable() },
      });
      await expect(db.migrate(schema([a2, fresh], { views: [lying] }))).rejects.toThrow(
        /View ay declares/,
      );
      expect((await db.query("SELECT * FROM a")).columns).toEqual(["id", "x"]);
      await expect(db.query("SELECT * FROM n")).rejects.toThrow(/Unknown table/);
    } finally {
      await db.close();
    }
  });

  it("a view that reads only existing columns is redefined before the alterations", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      const withNote = table("a", {
        id: column.integer().unique(),
        x: column.integer(),
        note: column.string().nullable(),
      });
      const readsNote = view("vx", {
        sql: "SELECT id, note FROM a",
        columns: { id: column.integer(), note: column.string().nullable() },
      });
      await db.migrate(schema([withNote], { views: [readsNote] }));
      // Dropping `note` is possible only because the view is redefined first to stop reading
      // it: a view that infers against the current catalog keeps its place before the
      // alteration pass.
      const readsX = view("vx", {
        sql: "SELECT id, x FROM a",
        columns: { id: column.integer(), x: column.integer() },
      });
      const result = await db.migrate(schema([a1], { views: [readsX] }), {
        allowDestructive: true,
      });
      expect(result.replacedViews).toEqual(["vx"]);
      expect(result.alteredTables).toEqual(["a"]);
      expect((await db.query("SELECT * FROM a")).columns).toEqual(["id", "x"]);
      expect((await db.query("SELECT * FROM vx")).columns).toEqual(["id", "x"]);
    } finally {
      await db.close();
    }
  });
});

describe("interrupted and concurrent migrations", () => {
  it("a fault at any catalog write leaves a catalog the next run completes", async () => {
    const a1 = table("a", { id: column.integer().unique(), x: column.integer() });
    const b1 = table("b", {
      id: column.integer().unique(),
      aid: column.integer().references("a", "id"),
    });
    const a2 = table("a", {
      id: column.integer().unique(),
      x: column.integer(),
      y: column.integer().nullable(),
    });
    const b2 = table("b", {
      id: column.integer().unique(),
      aid: column.integer().references("a", "id"),
      z: column.string().backfill("none"),
    });
    const c2 = table("c", {
      id: column.integer().unique(),
      bid: column.integer().references("b", "id"),
    });
    const vw = view("ab", {
      sql: "SELECT a.id AS id, b.z AS z FROM a JOIN b ON b.aid = a.id",
      columns: { id: column.integer(), z: column.string() },
    });
    let interrupted = 0;
    for (let n = 1; n <= 8; n += 1) {
      let armed = Infinity;
      const db = new MinnowDatabase(catalogFaultStore(new MemoryBlockStore(), () => armed));
      try {
        await db.migrate(schema([a1, b1]));
        await db.execute("INSERT INTO a VALUES (1, 1)");
        await db.execute("INSERT INTO b VALUES (1, 1)");
        armed = n;
        const first = await outcome(db.migrate(schema([a2, b2, c2], { views: [vw] })));
        armed = Infinity;
        if (first.startsWith("ERR")) {
          interrupted += 1;
          expect(first).toMatch(/injected fault/);
        }
        await db.migrate(schema([a2, b2, c2], { views: [vw] }));
        expect((await db.migrate(schema([a2, b2, c2], { views: [vw] }))).steps).toEqual([]);
        expect((await db.query("SELECT * FROM ab")).rows).toEqual([{ id: 1, z: "none" }]);
        expect((await db.query("SELECT * FROM a")).columns).toEqual(["id", "x", "y"]);
        expect((await db.query("SELECT * FROM c")).columns).toEqual(["id", "bid"]);
      } finally {
        await db.close();
      }
    }
    expect(interrupted).toBeGreaterThan(2);
  });

  it("two connections migrating the same schema, then racing divergent DDL, converge", async () => {
    const store = new MemoryBlockStore();
    const a = new MinnowDatabase(store);
    const b = new MinnowDatabase(store);
    try {
      const t = table("t", { id: column.integer().unique(), x: column.integer() });
      const u = table("u", {
        id: column.integer().unique(),
        tid: column.integer().references("t", "id"),
      });
      await Promise.all([a.migrate(schema([t, u])), b.migrate(schema([t, u]))]);
      const t2 = table("t", {
        id: column.integer().unique(),
        x: column.integer(),
        y: column.string().nullable(),
      });
      await Promise.all([a.migrate(schema([t2, u])), b.migrate(schema([t2, u]))]);
      expect((await a.query("SELECT * FROM t")).columns).toEqual(["id", "x", "y"]);
      await a.execute("INSERT INTO t VALUES (1, 1, NULL)");
      await Promise.all([
        outcome(a.execute("ALTER TABLE t ADD COLUMN z INTEGER DEFAULT 3")),
        outcome(b.execute("ALTER TABLE t RENAME COLUMN x TO w")),
        outcome(a.execute("CREATE INDEX ix ON t(x)")),
        outcome(b.execute("ALTER TABLE t DROP COLUMN x")),
        outcome(a.execute("INSERT INTO t (id, x) VALUES (2, 2)")),
      ]);
      const viaA = await a.query("SELECT * FROM t ORDER BY id", { memoize: false });
      const viaB = await b.query("SELECT * FROM t ORDER BY id", { memoize: false });
      expect(viaA.columns).toEqual(viaB.columns);
      expect(viaA.rows).toEqual(viaB.rows);
    } finally {
      await a.close();
      await b.close();
    }
  });
});
