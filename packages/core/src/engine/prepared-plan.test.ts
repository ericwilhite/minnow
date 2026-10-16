/**
 * Prepared read plans and literal shapes: a statement's catalog-rewritten template is built once
 * per structural catalog epoch and re-bound per execution, and literal statements that differ only
 * in lifted literals share one. Every test compares against the ordinary path (the hooks turned
 * off), pins that the prepared path actually served, and covers one way a template could bind a
 * value differently from the ordinary binder. What invalidates a template is pinned in
 * cache-invalidation.test.ts with the engine's other caches.
 */
import { afterEach, describe, expect, it } from "vitest";
import { normalizeSqlLiterals } from "./literal-normalization.js";
import { pointReadTestHooks } from "./point-read.js";
import { preparedPlanTestHooks } from "./prepared-plan.js";
import { both, seeded } from "./prepared-test-helpers.js";
import { planCloneStats } from "./query.js";

afterEach(() => {
  preparedPlanTestHooks.disabled = false;
  preparedPlanTestHooks.eager = false;
  pointReadTestHooks.disabled = false;
});

describe("prepared read plans", () => {
  it("re-binds a template without a catalog pass or a plan copy", async () => {
    const database = await seeded();
    const sql = "SELECT COUNT(*) AS n, MIN(name) AS low FROM items WHERE grp = ? AND id > ?";
    const run = (grp: number) =>
      database.query(sql, { params: [grp, 3], memoize: false }).then((result) => result.rows);
    await run(0);
    await run(1); // builds the template
    const rewrites = preparedPlanTestHooks.catalogRewrites;
    const clones = planCloneStats.clones;
    const built = preparedPlanTestHooks.built;
    const served = preparedPlanTestHooks.served;
    expect(await run(2)).toEqual([{ n: 9, low: "n0" }]);
    expect(await run(3)).toEqual([{ n: 10, low: "n0" }]);
    expect(preparedPlanTestHooks.served - served).toBe(2);
    expect(preparedPlanTestHooks.built - built).toBe(0);
    expect(preparedPlanTestHooks.catalogRewrites - rewrites).toBe(0);
    expect(planCloneStats.clones - clones).toBe(0);
    await database.close();
  });

  it("shares one shape across literal statements and keeps each literal's own answer", async () => {
    const database = await seeded();
    const rewrites = preparedPlanTestHooks.catalogRewrites;
    for (let grp = 0; grp < 4; grp += 1) {
      const { served } = await both(
        database,
        `SELECT id, name FROM items WHERE grp = ${String(grp)} AND name <> 'n3' ORDER BY id`,
      );
      expect(served).toBe(true);
    }
    // One template for the shape, built on its first prepared run; the ordinary runs rewrite.
    expect(preparedPlanTestHooks.catalogRewrites - rewrites).toBe(5);
    // An integer and a string never share a shape: '2' is read as the INTEGER column's type.
    await both(database, "SELECT id FROM items WHERE grp = '2' ORDER BY id");
    expect(normalizeSqlLiterals("SELECT id FROM items WHERE grp = '2'")?.kinds).toBe("s");
    expect(normalizeSqlLiterals("SELECT id FROM items WHERE grp = 2")?.kinds).toBe("i");
    // Ordinals, LIMIT, LIKE patterns, typed literals, casts, and decimals keep their spelling.
    for (const sql of [
      "SELECT grp FROM items GROUP BY 1 ORDER BY 1 LIMIT 2",
      "SELECT id FROM items WHERE name LIKE 'n1%'",
      "SELECT id FROM items WHERE day = DATE '2026-01-02'",
      "SELECT id FROM items WHERE grp = '1'::int",
      "SELECT id FROM items WHERE price > 10.5",
    ]) {
      expect(normalizeSqlLiterals(sql), sql).toBeUndefined();
      await both(database, sql);
    }
    await database.close();
  });

  it("replays domain coercion and declines what only the ordinary binder can coerce", async () => {
    const database = await seeded();
    // DATE and NUMERIC domains normalize the bound value as they would a literal.
    for (const day of ["2026-01-02", "2026-01-05"]) {
      expect(
        (
          await both(database, "SELECT id FROM items WHERE day = ? AND grp = ? ORDER BY id", [
            day,
            1,
          ])
        ).served,
      ).toBe(true);
    }
    expect(
      (await both(database, "SELECT id FROM items WHERE price = ? ORDER BY id", ["12.25"])).prepared
        .rows,
    ).toEqual([{ id: 13 }]);
    // Text read as an INTEGER column's type is the ordinary binder's coercion: declined, same answer.
    const declined = preparedPlanTestHooks.declined;
    expect(
      (
        await both(database, "SELECT id FROM items WHERE grp = ? AND id < ? ORDER BY id", [
          "2",
          "12",
        ])
      ).prepared.rows,
    ).toEqual([{ id: 3 }, { id: 7 }, { id: 11 }]);
    expect(preparedPlanTestHooks.declined).toBeGreaterThan(declined);
    // A number opposite TEXT is a type error on both paths, reported by the ordinary binder's
    // check: the template declines it up front rather than failing and being re-run.
    const retried = preparedPlanTestHooks.retried;
    for (const disabled of [true, false]) {
      preparedPlanTestHooks.disabled = disabled;
      preparedPlanTestHooks.eager = !disabled;
      await expect(
        database.query("SELECT id FROM items WHERE name = ? AND grp = 1", {
          params: [5],
          memoize: false,
        }),
      ).rejects.toThrow("comparable SQL types");
    }
    expect(preparedPlanTestHooks.retried).toBe(retried);
    await database.close();
  });

  it("keeps integer division typed by the bound value", async () => {
    const database = await seeded();
    const sql = "SELECT id, grp / ? AS q FROM items WHERE id = 4 OR id = 8 ORDER BY id";
    const whole = await both(database, sql, [2]);
    expect(whole.prepared.rows).toEqual([
      { id: 4, q: 1 },
      { id: 8, q: 1 },
    ]);
    expect(whole.served).toBe(false);
    expect((await both(database, sql, [2.5])).prepared.rows).toEqual([
      { id: 4, q: 1.2 },
      { id: 8, q: 1.2 },
    ]);
    await database.close();
  });

  it("binds ANY arrays of any length and LIMIT placeholders into one template", async () => {
    const database = await seeded();
    pointReadTestHooks.disabled = true;
    const sql = "SELECT id FROM items WHERE grp = ANY($1) AND id < $2 ORDER BY id LIMIT $3";
    for (const [members, limit] of [
      [[1], 3],
      [[1, 2, 3], 5],
      [[], 2],
      [[0, null], 4],
    ] as const) {
      expect((await both(database, sql, [members as never, 20, limit])).served).toBe(true);
    }
    // A NULL array compares to NULL, and a scalar is refused, exactly as the ordinary binder does.
    expect((await both(database, sql, [null, 20, 4])).prepared.rows).toEqual([]);
    await expect(database.query(sql, { params: [5, 20, 4], memoize: false })).rejects.toThrow(
      "must be an array for ANY or ALL",
    );
    await database.close();
  });
});

describe("key IN lists as point reads", () => {
  it("serves each member as a key lookup, deduplicated, in key order", async () => {
    const database = await seeded();
    const served = pointReadTestHooks.served;
    for (const [sql, params] of [
      ["SELECT id, name FROM items WHERE id IN (?, ?, ?, ?)", [9, 3, 9, 77]],
      ["SELECT id, name FROM items WHERE id IN (12, 2, NULL)", undefined],
      ["SELECT COUNT(*) AS n FROM items WHERE id IN (?, ?, ?)", [1, 2, 99]],
      ["SELECT * FROM items WHERE id = ANY($1)", [[40, 1, 20]]],
      ["SELECT id FROM items WHERE grp = 1 AND id IN (1, 2, 5, 6)", undefined],
    ] as const) {
      await both(database, sql, params);
    }
    expect(pointReadTestHooks.served - served).toBeGreaterThanOrEqual(5);
    expect((await database.query("SELECT id FROM items WHERE id IN (9, 3, 9, 77)")).rows).toEqual([
      { id: 3 },
      { id: 9 },
    ]);
    // Members of another type than the key fall back to the ordinary executor, which coerces.
    expect((await database.query("SELECT id FROM items WHERE id IN ('4', '5')")).rows).toEqual([
      { id: 4 },
      { id: 5 },
    ]);
    // Rows changed by later writes are read through their deltas, member by member.
    await database.execute("UPDATE items SET name = 'moved' WHERE id = 3");
    await database.execute("DELETE FROM items WHERE id = 9");
    expect(
      (await database.query("SELECT id, name FROM items WHERE id IN (3, 9, 10)")).rows,
    ).toEqual([
      { id: 3, name: "moved" },
      { id: 10, name: "n2" },
    ]);
    await database.close();
  });
});
