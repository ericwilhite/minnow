/** Batch boundaries cannot change PostgreSQL's generated/NOT NULL/CHECK error or identity. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROW_RULE_ORDER_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { compileTableRules } from "../exec/rules.js";
import { ColumnStore } from "../exec/column-store.js";
import { bindParameter } from "../exec/parameters.js";
import { oracleRows } from "./differential-adapter.js";
import type { TableDef } from "../access/catalog.js";
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
const failures = ["check", "generated", "null", "names", "success"] as const;
const errorOf = (e: unknown, table?: TableDef) => {
  const x = e as {
    sqlState?: string;
    code?: string;
    checkId?: number;
    columnId?: number;
    constraint?: string;
    column?: string;
  };
  return {
    state: x.sqlState ?? x.code,
    constraint: x.constraint ?? table?.checks.find((c) => c.id === x.checkId)?.name,
    column: x.column ?? table?.columns.find((c) => c.id === x.columnId)?.name,
  };
};
for (const seed of seedsFor("next-row-rules-order", [0xb7c0, 0xb7c1]))
  it(`keeps generated, NOT NULL and named CHECK order across batches seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("row rule error order", ROW_RULE_ORDER_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...failures),
          fc.integer({ min: 1, max: 9 }),
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          fc.constantFrom("a", "Case", "a.b", "é", "\u{10000}"),
          async (kind, value, prefix, reverse, selected, name) => {
            reach.beginHistory();
            const n = quote(name),
              z = quote(value % 3 === 0 ? "\ue000" : value % 3 === 1 ? "Z" : name + "z");
            const checks = [`CONSTRAINT ${z} CHECK(a>0)`, `CONSTRAINT ${n} CHECK(a>1)`];
            if (reverse) checks.reverse();
            const gs = [
              "g INT GENERATED ALWAYS AS (100/a) STORED",
              "h INT GENERATED ALWAYS AS (b+1) STORED",
            ];
            if (reverse) gs.reverse();
            const tail =
              kind === "names"
                ? checks.join(",")
                : kind === "generated"
                  ? gs.join(",")
                  : `g INT GENERATED ALWAYS AS (${kind === "null" ? "CASE WHEN a<0 THEN NULL ELSE 100/b END" : "100/b"}) STORED${kind === "null" ? " NOT NULL" : ""},CONSTRAINT ${n} CHECK(a>0)`;
            const ddl = `CREATE TABLE t(a INT,b INT,${tail})`;
            const roots: number[][] = Array.from({ length: prefix }, () => [value + 2, 1]);
            roots.push(
              ...(kind === "generated"
                ? [
                    [value % 2, 2147483647],
                    [0, 0],
                  ]
                : kind === "names"
                  ? [[0, 1]]
                  : kind === "success"
                    ? [
                        [value, 1],
                        [value + 1, 2],
                      ]
                    : [
                        [-value, 1],
                        [1, 0],
                      ]),
            );
            // An excluded poisonous row verifies that precise replay honors physical selection.
            const rows = selected ? [[0, 0], ...roots] : roots;
            await pg.exec("DROP TABLE IF EXISTS t");
            await pg.exec(ddl);
            let expected: ReturnType<typeof errorOf> | null = null;
            try {
              await pg.exec(
                `INSERT INTO t(a,b) VALUES ${roots.map((r) => `(${r.join(",")})`).join(",")}`,
              );
            } catch (e) {
              expected = errorOf(e);
            }
            if (kind === "success") expect(expected).toBeNull();
            else
              expect(expected?.state).toBe(
                kind === "generated"
                  ? reverse || value % 2
                    ? "22003"
                    : "22012"
                  : kind === "null"
                    ? "23502"
                    : "23514",
              );
            for (const width of [1, 2, 8]) {
              const core = await new StorageCore().open();
              let grant: import("../memory/governor.js").Grant | undefined;
              try {
                await core.execute(ddl);
                const catalog = core.harness.path.catalog,
                  table = catalog.tableByName("t");
                if (!table) throw new Error("missing table");
                grant = await core.harness.governor.admit({
                  label: "rules/order",
                  minimumBytes: 65536,
                  spillable: true,
                });
                const charge = grant;
                const bound = compileTableRules(
                  table,
                  catalog.version,
                  core.lowerer.lowerTableRules(table, catalog),
                  { charge: grant, capacity: 8 },
                );
                const tx = await core.harness.path.begin(),
                  statement = tx.statement({ rules: () => bound });
                let actual: ReturnType<typeof errorOf> | null = null;
                try {
                  for (let start = 0; start < rows.length; start += width) {
                    const part = rows.slice(start, start + width),
                      store = new ColumnStore(
                        table.columns.map((c) => c.type),
                        grant,
                        { chunkRows: 8 },
                      );
                    try {
                      for (const row of part) {
                        const before = grant.allocated,
                          vals = table.columns.map((c, i) =>
                            bindParameter(c.type, row[i] ?? null, i, charge),
                          ),
                          bytes = grant.allocated - before;
                        try {
                          expect(store.append(vals, Uint16Array.of(0), 0, 1)).toBe(1);
                        } finally {
                          grant.free(bytes);
                        }
                      }
                      const active = part
                        .map((_, i) => i)
                        .filter((i) => !selected || start + i !== 0);
                      const batch = {
                        length: part.length,
                        columns: store.vectors().map((v) => ({ ...v })),
                        selection: selected
                          ? { rows: Uint16Array.from(active), count: active.length }
                          : null,
                      };
                      bound.generate(
                        batch,
                        table.columns.map((c) => c.id),
                      );
                      await statement.table(table.id).insert(batch, null);
                    } finally {
                      store.close();
                    }
                  }
                  await statement.end();
                  await (
                    await tx.commit(null)
                  ).durable;
                } catch (e) {
                  actual = errorOf(e, table);
                  statement.rollback();
                  tx.rollback();
                } finally {
                  bound.close();
                }
                expect(actual, `${kind}/${String(width)}/${name}`).toEqual(expected);
                const result = await core.execute("SELECT a,b FROM t ORDER BY a,b");
                if (!("batches" in result)) throw new Error("missing query");
                expect(oracleRows(result)).toEqual(
                  (await pg.query("SELECT a,b FROM t ORDER BY a,b")).rows,
                );
                expect(grant.allocated).toBe(0);
                grant.close();
                grant = undefined;
                reach.reach(
                  width === 1 ? "single row" : width === 2 ? "split batch" : "whole batch",
                );
              } finally {
                grant?.close();
                await core.close();
              }
            }
            reach.reach(kind);
            reach.reach(reverse ? "reverse declaration" : "forward declaration");
            reach.reach(selected ? "selection" : "dense");
            reach.reach(prefix ? "valid prefix" : "first row failure");
            reach.reach(name === "a" ? "plain name" : "quoted name");
            if (name === "\u{10000}" && value % 3 === 0) reach.reach("UTF8 names");
            await pg.exec("DROP TABLE t");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ["check", 1, 0, false, false, "a"],
            ["generated", 1, 0, false, false, "a"],
            ["generated", 2, 0, false, false, "a"],
            ["generated", 2, 0, true, false, "a"],
            ["null", 1, 1, true, true, "Case"],
            ["names", 1, 0, false, false, "a.b"],
            ["success", 2, 2, true, false, "é"],
            ["names", 1, 0, true, true, "\u{10000}"],
            ["names", 3, 0, true, false, "\u{10000}"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
