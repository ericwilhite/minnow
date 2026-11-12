import { OutputArena } from "./vectors.js";
import { compileExpr } from "./compile.js";
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { it, expect } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, LIKE_REACH } from "../../../testing/reach.js";
import { evaluate, texts, dictionary, TEXT, BOOL } from "./test-support.js";
import type { ScalarExpr } from "../../ir/ir.js";
import type { Batch, Vector } from "../../vector/vector.js";

it.each(seedsFor("next-like", [0xb781, 0xb782]))(
  "PG LIKE/ILIKE wildcards, escape, Unicode and strict NULLs seed%i",
  async (seed) => {
    const pg = new PGlite();
    const reach = createReach("LIKE PG", LIKE_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc
            .array(
              fc.constantFrom(
                "a",
                "A",
                "b",
                "%",
                "_",
                "\\",
                "Ä",
                "ä",
                "汉",
                "😀",
                "İ",
                "i",
                "Σ",
                "σ",
                "ς",
                "\n",
                "ſ",
                "S",
              ),
              { maxLength: 12 },
            )
            .map((a) => a.join("")),
          fc
            .array(
              fc.constantFrom(
                "a",
                "A",
                "%",
                "_",
                "\\",
                "Ä",
                "ä",
                "汉",
                "😀",
                "İ",
                "i",
                "Σ",
                "σ",
                "ς",
                "\n",
              ),
              { maxLength: 9 },
            )
            .map((a) => a.join("")),
          fc.constantFrom("like", "ilike"),
          fc.constantFrom(null, "", "\\", "!", "汉", "%", "_", "xx"),
          fc.boolean(),
          fc.constantFrom(
            "flat",
            "constant",
            "dictionary",
            "selected",
            "null-arg",
            "null-pattern",
            "C",
            "POSIX",
          ),
          async (input, pattern, mode, escape, negated, form) => {
            reach.beginHistory();
            const value = form === "null-arg" ? null : input,
              pat = form === "null-pattern" ? null : pattern;
            const column = (index: number): ScalarExpr => ({ op: "column", index, type: TEXT });
            const expr: ScalarExpr = {
              op: "like",
              mode,
              negated,
              arg: column(0),
              pattern: column(1),
              escape,
              collation: form === "C" || form === "POSIX" ? form : null,
              type: BOOL,
            };
            const vector = (s: string | null): Vector =>
              form === "constant"
                ? { form: "constant", type: TEXT, length: 2, value: texts([s]) }
                : form === "dictionary"
                  ? dictionary(texts([s]), [0, 0])
                  : texts([s, s]);
            const batch: Batch = {
              length: 2,
              columns: [vector(value), vector(pat)],
              selection: form === "selected" ? { rows: Uint16Array.of(1), count: 1 } : null,
            };
            let actual: unknown, error: unknown;
            try {
              actual = evaluate(expr, batch);
            } catch (e) {
              error = e;
            }
            const coll = expr.collation ? ` COLLATE "${expr.collation}"` : "";
            const sql = `SELECT $1::text${coll} ${negated ? "NOT " : ""}${mode.toUpperCase()} $2::text${escape === null ? "" : " ESCAPE $3::text"} AS x`;
            try {
              const q = await pg.query<{ x: boolean | null }>(
                sql,
                escape === null ? [value, pat] : [value, pat, escape],
              );
              expect(error).toBeUndefined();
              expect(actual).toEqual(Array(form === "selected" ? 1 : 2).fill(q.rows[0]?.x));
              reach.reach("values");
            } catch (e) {
              if (!(e && typeof e === "object" && "code" in e)) throw e;
              expect(error).toMatchObject({ sqlState: (e as { code: string }).code });
              reach.reach("escape error");
            }
            reach.reach(mode);
            reach.reach(form);
            if (negated) reach.reach("negated");
            if (input.includes("😀")) reach.reach("unicode scalar");
            if (escape === "") reach.reach("no escape");
            if (escape === "汉") reach.reach("unicode escape");
          },
        ),
        {
          seed,
          numRuns: 160,
          examples: [
            ["Ä", "ä", "ilike", null, false, "flat"],
            ["Ä", "ä", "ilike", null, false, "C"],
            ["Ä", "ä", "ilike", null, false, "POSIX"],
            ["😀", "_", "like", null, false, "dictionary"],
            ["abc", "a%", "like", null, true, "constant"],
            ["a_", "a!_", "like", "!", false, "selected"],
            ["x", "x", "like", "xx", false, "flat"],
            ["x", "x", "like", null, false, "null-arg"],
            ["x", "x", "like", "xx", false, "null-pattern"],
            ["a%", "a汉%", "like", "汉", false, "flat"],
            ["a\\", "a\\", "like", "", false, "flat"],
            ["a", "%_\\", "like", null, false, "flat"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  60000,
);

it("pins every C.UTF-8 simple case mapping independently of host Unicode", async () => {
  const pg = new PGlite();
  try {
    await pg.waitReady;
    const { likeLower } = await import("./like-case.js");
    const q = await pg.query<{ g: number; l: string }>(
      "SELECT g,lower(chr(g)) l FROM generate_series(1,1114111) g WHERE (g<55296 OR g>57343) AND lower(chr(g))<>chr(g)",
    );
    const map = new Map(q.rows.map((r) => [r.g, r.l.codePointAt(0)]));
    for (let c = 1; c <= 1114111; c++) {
      if (c >= 55296 && c <= 57343) continue;
      if (likeLower(c, true) !== (map.get(c) ?? c))
        throw new Error(`PG simple lowercase mismatch at ${String(c)}`);
      if (likeLower(c, false) !== (c >= 65 && c <= 90 ? c + 32 : c))
        throw new Error(`C lowercase mismatch at ${String(c)}`);
    }
  } finally {
    await pg.close();
  }
}, 30000);

it("reuses charged bool output and never evaluates unselected or NULL pattern errors", () => {
  const expr: ScalarExpr = {
    op: "like",
    mode: "like",
    negated: false,
    arg: { op: "column", index: 0, type: TEXT },
    pattern: { op: "column", index: 1, type: TEXT },
    escape: null,
    type: BOOL,
  };
  const selected: Batch = {
    columns: [texts(["ab", "a"]), texts(["a\\", "a"])],
    length: 2,
    selection: { rows: Uint16Array.of(1), count: 1 },
  };
  let charged = 0;
  const arena = new OutputArena(
    {
      allocate(n) {
        charged += n;
        return true;
      },
      free(n) {
        charged -= n;
      },
    },
    2,
  );
  const compiled = compileExpr(expr, { arena, input: [TEXT, TEXT], params: { slots: [] } });
  const before = charged;
  expect(before).toBeGreaterThan(0);
  const first = compiled.evaluate(selected, selected.selection);
  const long = {
    ...selected,
    columns: [texts(["x".repeat(50000), "a".repeat(50000)]), texts(["%", "%"])],
    selection: null,
  };
  expect(compiled.evaluate(long, null)).toBe(first);
  expect(charged).toBe(before);
  arena.close();
  expect(charged).toBe(0);
  expect(evaluate(expr, selected)).toEqual([true]);
  expect(
    evaluate(expr, {
      ...selected,
      columns: [texts([null, "a"]), texts(["a\\", null])],
      selection: null,
    }),
  ).toEqual([null, null]);
});
