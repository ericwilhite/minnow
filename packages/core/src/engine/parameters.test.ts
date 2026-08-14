import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { bindPlanParameters, compileQuery, compileStatement } from "./query.js";

async function seeded(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  await database.createTable({
    name: "people",
    uniqueKey: "name",
    columns: [
      { name: "name", type: "string" },
      { name: "score", type: "number" },
      { name: "joined", type: "datetime", nullable: true },
    ],
  });
  await database.insertBatch("people", [
    { name: "Ada", score: 10, joined: new Date("2026-01-02T03:04:05.000Z") },
    { name: "Grace", score: 25, joined: new Date("2026-02-01T00:00:00.000Z") },
    { name: "Katherine", score: 30, joined: null },
  ]);
  return database;
}

describe("parameter compilation", () => {
  it("counts positional placeholders in document order", () => {
    const plan = compileQuery("SELECT name FROM people WHERE score > ? AND name != ?");
    expect(plan.parameterCount).toBe(2);
  });

  it("counts numbered placeholders by their highest number and allows reuse", () => {
    const plan = compileQuery("SELECT name FROM people WHERE score > $2 AND score < $2 + $1");
    expect(plan.parameterCount).toBe(2);
  });

  it("rejects mixing ? and $n in one statement", () => {
    expect(() => compileQuery("SELECT name FROM people WHERE score > ? AND score < $2")).toThrow(
      "not both",
    );
    expect(() => compileQuery("SELECT name FROM people WHERE score > $1 AND score < ?")).toThrow(
      "not both",
    );
  });

  it("rejects $0 and a bare $", () => {
    expect(() => compileQuery("SELECT name FROM people WHERE score > $0")).toThrow("$1");
    expect(() => compileQuery("SELECT name FROM people WHERE score > $")).toThrow(
      "parameter number",
    );
  });

  it("binds copy-on-write: the cached plan keeps its placeholders", () => {
    const plan = compileQuery("SELECT name FROM people WHERE score > ?");
    const bound = bindPlanParameters(plan, [20]);
    expect(bound).not.toBe(plan);
    expect(bound.parameterCount).toBeUndefined();
    expect(plan.parameterCount).toBe(1);
    expect(JSON.stringify(plan)).toContain('"parameter"');
    expect(JSON.stringify(bound)).not.toContain('"parameter"');
  });

  it("validates the parameter count in both directions", () => {
    const plan = compileQuery("SELECT name FROM people WHERE score > ?");
    expect(() => bindPlanParameters(plan, [])).toThrow("takes 1 parameter, got 0");
    expect(() => bindPlanParameters(plan, [1, 2])).toThrow("takes 1 parameter, got 2");
    const bare = compileQuery("SELECT name FROM people");
    expect(() => bindPlanParameters(bare, [1])).toThrow("takes 0 parameters, got 1");
  });

  it("validates parameter values", () => {
    const plan = compileQuery("SELECT name FROM people WHERE score > ?");
    expect(() => bindPlanParameters(plan, [Number.NaN])).toThrow("finite");
    expect(() => bindPlanParameters(plan, [new Date("nope")])).toThrow("valid date");
    expect(() => bindPlanParameters(plan, [{ nested: true } as unknown as string])).toThrow(
      "null, boolean, number, string, or Date",
    );
  });

  it("compiles placeholders in mutation statements", () => {
    expect(
      compileStatement("INSERT INTO people (name, score) VALUES (?, ?), (?, ?)").parameterCount,
    ).toBe(4);
    expect(compileStatement("UPDATE people SET score = $1 WHERE name = $2").parameterCount).toBe(2);
    expect(compileStatement("DELETE FROM people WHERE name = ?").parameterCount).toBe(1);
  });

  it("rejects placeholders nested inside INSERT value expressions", () => {
    expect(() => compileStatement("INSERT INTO people (score) VALUES (? + 1)")).toThrow(
      "bare ? placeholder",
    );
  });
});

describe("parameter execution", () => {
  it("filters through positional parameters", async () => {
    const database = await seeded();
    const result = await database.query("SELECT name FROM people WHERE score >= ? ORDER BY name", {
      params: [25],
    });
    expect(result.rows).toEqual([{ name: "Grace" }, { name: "Katherine" }]);
  });

  it("reuses one cached plan across different bindings", async () => {
    const database = await seeded();
    const sql = "SELECT name FROM people WHERE score >= $1 ORDER BY name";
    const high = await database.query(sql, { params: [30] });
    const low = await database.query(sql, { params: [10] });
    expect(high.rows).toEqual([{ name: "Katherine" }]);
    expect(low.rows.length).toBe(3);
  });

  it("binds parameters in IN lists, expressions, and subqueries", async () => {
    const database = await seeded();
    const inList = await database.query(
      "SELECT name FROM people WHERE name IN (?, ?) ORDER BY name",
      { params: ["Ada", "Grace"] },
    );
    expect(inList.rows).toEqual([{ name: "Ada" }, { name: "Grace" }]);
    const arithmetic = await database.query(
      "SELECT score + ? AS bumped FROM people WHERE name = ?",
      { params: [5, "Ada"] },
    );
    expect(arithmetic.rows).toEqual([{ bumped: 15 }]);
    const subquery = await database.query(
      "SELECT name FROM people WHERE score = (SELECT MAX(score) FROM people WHERE score < ?)",
      { params: [30] },
    );
    expect(subquery.rows).toEqual([{ name: "Grace" }]);
  });

  it("binds datetime parameters with full time precision", async () => {
    const database = await seeded();
    const result = await database.query("SELECT name FROM people WHERE joined = ?", {
      params: [new Date("2026-01-02T03:04:05.000Z")],
    });
    expect(result.rows).toEqual([{ name: "Ada" }]);
  });

  it("requires parameters when the statement has placeholders", async () => {
    const database = await seeded();
    await expect(database.query("SELECT name FROM people WHERE score > ?")).rejects.toThrow(
      "takes 1 parameter, got 0",
    );
  });

  it("binds positional parameters through query()", async () => {
    const database = await seeded();
    const result = await database.query("SELECT name FROM people WHERE score >= ? ORDER BY name", {
      params: [25],
    });
    expect(result.rows).toEqual([{ name: "Grace" }, { name: "Katherine" }]);
  });

  it("executes parameterized mutations end to end", async () => {
    const database = await seeded();
    const inserted = await database.execute(
      "INSERT INTO people (name, score, joined) VALUES (?, ?, ?)",
      ["Linus", 25, null],
    );
    expect(inserted).toMatchObject({ kind: "insert", rowCount: 1 });

    const updated = await database.execute("UPDATE people SET score = $1 WHERE name = $2", [
      40,
      "Ada",
    ]);
    expect(updated).toMatchObject({ kind: "update", rowCount: 1 });
    const after = await database.query("SELECT score FROM people WHERE name = ?", {
      params: ["Ada"],
    });
    expect(after.rows).toEqual([{ score: 40 }]);

    const deleted = await database.execute("DELETE FROM people WHERE name = ?", ["Linus"]);
    expect(deleted).toMatchObject({ kind: "delete", rowCount: 1 });
    const names = await database.query("SELECT COUNT(*) AS people FROM people");
    expect(names.rows).toEqual([{ people: 3 }]);
  });

  it("routes a parameterized SELECT through execute", async () => {
    const database = await seeded();
    const result = await database.execute("SELECT name FROM people WHERE score > ?", [20]);
    expect(result.kind).toBe("rows");
    if (result.kind === "rows") expect(result.result.rows.length).toBe(2);
  });

  it("refuses to run a statement with unbound placeholders", async () => {
    const database = await seeded();
    const statement = compileStatement("INSERT INTO people (name, score) VALUES (?, ?)");
    await expect(database.runStatement(statement)).rejects.toThrow("unbound placeholders");
    await expect(database.execute("UPDATE people SET score = 1 WHERE name = ?")).rejects.toThrow(
      "takes 1 parameter, got 0",
    );
  });
});
