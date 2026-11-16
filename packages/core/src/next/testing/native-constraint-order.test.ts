/** PostgreSQL checks each generated row, then columns, then named CHECKs, independent of batches. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_CONSTRAINT_ORDER_REACH } from "../../testing/reach.js";
import type { TableDef } from "../access/catalog.js";
import { ColumnStore } from "../exec/column-store.js";
import { bindParameter } from "../exec/parameters.js";
import { compileTableRules } from "../exec/rules.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const kinds = ["check-null", "null-check", "error-null", "columns", "success"] as const;
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
const identity = (error: unknown, table?: TableDef) => {
  const e = error as {
    code?: string;
    sqlState?: string;
    checkId?: number;
    columnId?: number;
    constraint?: string;
    column?: string;
  };
  return {
    state: e.sqlState ?? e.code,
    constraint: e.constraint ?? table?.checks.find((c) => c.id === e.checkId)?.name,
    column: e.column ?? table?.columns.find((c) => c.id === e.columnId)?.name,
  };
};

for (const seed of seedsFor("next-native-constraint-order", [0xb60b, 0xb60c]))
  it(`keeps native constraint order across widths seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native constraint scheduling", NATIVE_CONSTRAINT_ORDER_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...kinds),
          fc.integer({ min: 1, max: 9 }),
          fc.integer({ min: 0, max: 4 }),
          fc.boolean(),
          fc.boolean(),
          fc.constantFrom(["Z", "a"], ["\ue000", "\u{10000}"]),
          async (kind, value, prefix, selected, reverse, names) => {
            reach.beginHistory();
            const checks = names.map((name) => `CONSTRAINT ${quote(name)} CHECK(a>0)`);
            if (reverse) checks.reverse();
            const ddl =
              kind === "columns"
                ? "CREATE TABLE t(a INT NOT NULL,b INT NOT NULL)"
                : `CREATE TABLE t(a INT,b INT,g INT GENERATED ALWAYS AS (CASE WHEN a=0 THEN NULL ELSE a END) STORED NOT NULL,${kind === "error-null" ? `CONSTRAINT ${quote(names[0])} CHECK(100/(a+1)>0)` : checks.join(",")})`;
            const roots: Array<Array<number | null>> = Array.from({ length: prefix }, () => [
              value + 2,
              value + 2,
            ]);
            roots.push(
              ...(kind === "columns"
                ? [
                    [value, null],
                    [null, value],
                  ]
                : kind === "null-check"
                  ? [
                      [0, 1],
                      [-value, 1],
                    ]
                  : kind === "success"
                    ? [
                        [value + 2, 1],
                        [value + 3, 1],
                      ]
                    : [
                        [kind === "error-null" ? -1 : -value, 1],
                        [0, 1],
                      ]),
            );
            await pg.exec("DROP TABLE IF EXISTS t");
            await pg.exec(ddl);
            let expected: ReturnType<typeof identity> | null = null;
            try {
              await pg.exec(
                `INSERT INTO t(a,b) VALUES ${roots.map((r) => `(${r.map((v) => (v === null ? "NULL" : String(v))).join(",")})`).join(",")}`,
              );
            } catch (error) {
              expected = identity(error);
            }
            expect(expected?.state ?? null).toBe(
              kind === "success"
                ? null
                : kind === "error-null"
                  ? "22012"
                  : kind === "check-null"
                    ? "23514"
                    : "23502",
            );
            const rows = selected ? [[null, null], ...roots] : roots;
            for (const legacy of [false, true])
              for (const width of [1, 2, 8]) {
                const core = await new StorageCore().open();
                let grant: import("../memory/governor.js").Grant | undefined;
                try {
                  await core.execute(ddl);
                  const catalog = core.harness.path.catalog,
                    table = catalog.tableByName("t");
                  if (!table) throw new Error("Missing table");
                  grant = await core.harness.governor.admit({
                    label: "constraint/order",
                    minimumBytes: 65536,
                    spillable: true,
                  });
                  const charge = grant,
                    bound = compileTableRules(
                      table,
                      catalog.version,
                      core.lowerer.lowerTableRules(table, catalog),
                      { charge, capacity: 8 },
                    ),
                    seen: number[] = [],
                    rules = legacy
                      ? {
                          check: (
                            batch: import("../vector/vector.js").Batch,
                            changed: readonly number[] | null,
                          ) => {
                            seen.push(batch.selection?.count ?? batch.length);
                            return bound.check(batch, changed);
                          },
                          generate: bound.generate.bind(bound),
                        }
                      : bound,
                    tx = await core.harness.path.begin(),
                    statement = tx.statement({ rules: () => rules });
                  let actual: ReturnType<typeof identity> | null = null;
                  try {
                    for (let start = 0; start < rows.length; start += width) {
                      const part = rows.slice(start, start + width),
                        store = new ColumnStore(
                          table.columns.map((c) => c.type),
                          charge,
                          { chunkRows: 8 },
                        );
                      try {
                        for (const row of part) {
                          const before = charge.allocated,
                            values = table.columns.map((c, i) =>
                              bindParameter(c.type, row[i] ?? null, i, charge),
                            ),
                            bytes = charge.allocated - before;
                          try {
                            expect(store.append(values, Uint16Array.of(0), 0, 1)).toBe(1);
                          } finally {
                            charge.free(bytes);
                          }
                        }
                        const active = part
                            .map((_, i) => i)
                            .filter((i) => !selected || start + i !== 0),
                          batch = {
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
                  } catch (error) {
                    actual = identity(error, table);
                    statement.rollback();
                    tx.rollback();
                  } finally {
                    bound.close();
                  }
                  expect(actual, `${kind}/width${String(width)}/legacy${String(legacy)}`).toEqual(
                    expected,
                  );
                  const result = await core.execute("SELECT a,b FROM t ORDER BY a,b");
                  if (!("batches" in result)) throw new Error("Missing rows");
                  expect(oracleRows(result)).toEqual(
                    (await pg.query("SELECT a,b FROM t ORDER BY a,b")).rows,
                  );
                  if (legacy && kind === "success" && width === 8) {
                    expect(seen.some((n) => n > 1)).toBe(true);
                    reach.reach("vectorized success");
                  }
                  expect(charge.allocated).toBe(0);
                  grant.close();
                  grant = undefined;
                  reach.reach(legacy ? "native fallback" : "complete validator");
                  reach.reach(width === 1 ? "width1" : width === 2 ? "width2" : "width8");
                } finally {
                  grant?.close();
                  await core.close();
                }
                expect(core.harness.governor.usedBytes).toBe(0);
              }
            reach.reach(kind);
            reach.reach(selected ? "selection" : "dense");
            reach.reach(prefix ? "valid prefix" : "first row");
            reach.reach(reverse ? "reverse CHECK declarations" : "forward CHECK declarations");
            reach.reach(names[0] === "Z" ? "ASCII names" : "UTF8 names");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            ["check-null", 1, 0, false, false, ["Z", "a"]],
            ["null-check", 2, 1, true, true, ["\ue000", "\u{10000}"]],
            ["error-null", 3, 0, false, false, ["Z", "a"]],
            ["columns", 4, 0, false, false, ["Z", "a"]],
            ["columns", 2, 2, true, true, ["Z", "a"]],
            ["success", 5, 2, false, false, ["\ue000", "\u{10000}"]],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
