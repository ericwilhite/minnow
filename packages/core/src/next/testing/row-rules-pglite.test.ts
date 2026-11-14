/** Shared execution rules, with PostgreSQL as authority and real charged kernel buffers. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROW_RULES_REACH } from "../../testing/reach.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { ints } from "../write/test-support.js";
import { compileTableRules } from "../exec/rules.js";

import { ColumnStore } from "../exec/column-store.js";
import { bindParameter } from "../exec/parameters.js";
import { Governor } from "../memory/governor.js";
import { newDictionaryId, type Batch, type Vector } from "../vector/vector.js";
import { bigAt } from "../exec/kernels/numeric.js";
import { flatOf, isValidSlot, slotIndex } from "../exec/kernels/vectors.js";
const close = (r: { close?: () => void }): void => {
  r.close?.();
};
const need = <T>(v: T | undefined): T => {
  if (v === undefined) throw new Error("missing test value");
  return v;
};
const outcome = async (work: () => unknown) => {
  try {
    return { value: await work(), state: null };
  } catch (error) {
    if (error && typeof error === "object") {
      if ("sqlState" in error) return { state: error.sqlState };
      if ("code" in error) return { state: error.code };
    }
    throw error;
  }
};
const read = (v: Vector, row: number): unknown => {
  if (!isValidSlot(v, row)) return null;
  const data = flatOf(v).data,
    i = slotIndex(v, row);
  if (data.layout === "varlen")
    return new TextDecoder().decode(data.bytes.subarray(data.offsets[i], data.offsets[i + 1]));
  if (data.layout === "int64") return bigAt(data.values, i).toString();
  if (data.layout === "i32") return data.values[i];
  throw new Error("unsupported test output");
};
for (const seed of seedsFor("next-row-rules-pglite", [0xb7b0, 0xb7b1]))
  it(`checks and generates in SQL row/constraint order seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("shared row rules", ROW_RULES_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.array(
            fc.tuple(
              fc.option(fc.integer({ min: -5, max: 5 }), { nil: null }),
              fc.option(fc.integer({ min: -4, max: 4 }), { nil: null }),
            ),
            { minLength: 1, maxLength: 8 },
          ),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (rows, selected, changeOnlyA, dictionary) => {
            reach.beginHistory();
            const next = new LoweredDatabase();
            const sql =
              "CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT,g BIGINT GENERATED ALWAYS AS (a::bigint+b) STORED,label TEXT GENERATED ALWAYS AS (CASE WHEN a IS NULL THEN 'nil' WHEN a=0 THEN 'zero' ELSE 'ok' END) STORED,CONSTRAINT ck_g CHECK(g>=0),CONSTRAINT ck_label CHECK(label<>'zero'))";
            await pg.exec("DROP TABLE IF EXISTS t");
            next.execute(sql);
            await pg.exec(sql);
            const table = next.catalog.view.tableByName("t");
            if (!table) throw new Error("missing rules table");
            const governor = new Governor(16 * 1024 * 1024);
            const grant = await governor.admit({
              label: "test/rules",
              minimumBytes: 65536,
              spillable: true,
            });
            const bound = compileTableRules(
              table,
              next.catalog.view.version,
              next.lowerer.lowerTableRules(table, next.catalog.view),
              { charge: grant, capacity: 16 },
            );
            const input = new ColumnStore(
              table.columns.map((c) => c.type),
              grant,
              { chunkRows: 16 },
            );
            let dictionaryBytes = 0;
            try {
              for (let i = 0; i < rows.length; i++) {
                const row = rows[i];
                if (!row) throw new Error("missing test row");
                const before = grant.allocated;
                const values = [i + 1, row[0], row[1], 999n, "sentinel"].map((v, j) =>
                  bindParameter(need(table.columns[j]).type, v, j, grant),
                );
                const bytes = grant.allocated - before;
                try {
                  expect(input.append(values, Uint16Array.of(0), 0, 1)).toBe(1);
                } finally {
                  grant.free(bytes);
                }
              }
              const columns: Vector[] = input.vectors().map((v) => ({ ...v }));
              if (dictionary) {
                const values = [...new Set(rows.flatMap(([a]) => (a === null ? [] : [a])))];
                if (values.length === 0) values.push(0);
                dictionaryBytes = 4 * values.length + 4 * rows.length;
                if (!grant.allocate(dictionaryBytes))
                  throw new Error("dictionary fixture not admitted");
                const original = need(columns[1]);
                columns[1] = {
                  form: "dictionary",
                  type: original.type,
                  length: rows.length,
                  dictionary: ints(values),
                  dictionaryId: newDictionaryId(),
                  codes: Int32Array.from(rows, ([a]) => (a === null ? 0 : values.indexOf(a))),
                  validity: input.vectors()[1]?.validity ?? null,
                };
                reach.reach("dictionary");
              } else reach.reach("flat");
              const active = Array.from({ length: rows.length }, (_, i) => i).filter(
                (i) => !selected || i % 2 === 0,
              );
              const batch: Batch = {
                length: rows.length,
                columns,
                selection: selected
                  ? { rows: Uint16Array.from(active), count: active.length }
                  : null,
              };
              const previous = read(need(input.vectors()[3]), 0);
              const generatedIds = table.columns.map((c) => c.id);
              bound.generate(batch, generatedIds);
              expect(read(need(input.vectors()[3]), 0)).toBe(previous);
              for (let i = 0; i < rows.length; i++)
                if (!active.includes(i)) {
                  expect(read(need(columns[3]), i)).toBe("999");
                  expect(read(need(columns[4]), i)).toBe("sentinel");
                  reach.reach("inactive preserved");
                }
              const authority = await outcome(() =>
                pg.exec(
                  `INSERT INTO t(id,a,b) VALUES ${active.map((i) => `(${String(i + 1)},${String(rows[i]?.[0] ?? "NULL")},${String(rows[i]?.[1] ?? "NULL")})`).join(",")}`,
                ),
              );
              const actual = bound.check(batch, changeOnlyA ? [need(table.columns[1]).id] : null);
              expect(actual === null ? null : "23514").toBe(authority.state);
              if (actual) reach.reach("CHECK false");
              else {
                const expected = (await pg.query("SELECT g::text,label FROM t ORDER BY id")).rows;
                expect(
                  active.map((i) => ({
                    g: read(need(columns[3]), i),
                    label: read(need(columns[4]), i),
                  })),
                ).toEqual(expected);
                reach.reach("generated parity");
              }
              if (rows.some(([a, b]) => a === null || b === null)) reach.reach("NULL passes");
              if (changeOnlyA) reach.reach("generated CHECK dependency");
              reach.reach("charged buffers");
              expect(grant.allocated).toBeGreaterThan(0);
              reach.reach(selected ? "selection" : "dense");
              reach.reach("OLD unchanged");
            } finally {
              grant.free(dictionaryBytes);
              input.close();
              close(bound);
              close(bound);
              expect(grant.allocated).toBe(0);
              grant.close();
              await pg.exec("DROP TABLE t");
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [[[null, null]], false, true, true],
            [
              [
                [null, 1],
                [2, 1],
                [-2, 0],
              ],
              true,
              true,
              false,
            ],
            [
              [
                [1, 2],
                [0, 0],
              ],
              false,
              false,
              true,
            ],
          ],
        },
      );
      // CHECK must stop at the first failing row, before a later row's expression error.
      const n = new LoweredDatabase();
      n.execute("CREATE TABLE e(a INT,b INT,CHECK(a>0),CHECK(100/b>0))");
      const t = n.catalog.view.tableByName("e");
      if (!t) throw new Error("missing error table");
      const gov = new Governor(4 * 1024 * 1024),
        g = await gov.admit({
          label: "errors",
          minimumBytes: 4096,
          spillable: true,
        });
      const r = compileTableRules(
        t,
        n.catalog.view.version,
        n.lowerer.lowerTableRules(t, n.catalog.view),
        { charge: g, capacity: 2 },
      );
      try {
        for (const first of [-1, 1]) {
          if (!g.allocate(16)) throw new Error("test input not admitted");
          const a = ints([first, 1]),
            b = ints([first < 0 ? 1 : 0, 0]);
          await pg.exec("CREATE TABLE e(a INT,b INT,CHECK(a>0),CHECK(100/b>0))");
          const pgResult = await outcome(() =>
            pg.exec(`INSERT INTO e VALUES(${String(first)},${first < 0 ? "1" : "0"}),(1,0)`),
          );
          const own = await outcome(() => {
            const f = r.check(
              {
                length: 2,
                columns: [a, b],
                selection: null,
              },
              null,
            );
            if (f) return "23514";
            return null;
          });
          expect(own.state ?? own.value).toBe(pgResult.state);
          reach.reach(first < 0 ? "row error precedence" : "expression error");
          g.free(16);
          await pg.exec("DROP TABLE e");
        }
      } finally {
        close(r);
        g.close();
      }
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
