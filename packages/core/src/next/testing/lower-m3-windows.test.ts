/** M3 window SQL through the real executor, not the IR reference interpreter. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import {
  compareEngines,
  type EngineUnderTest,
  type Operation,
} from "../../testing/differential-harness.js";
import { createReach, LOWER_M3_WINDOWS_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { queryAuthorityValue } from "./divergences.js";
import { StorageCore } from "./storage-core.js";

const reach = createReach("next lower M3 windows", LOWER_M3_WINDOWS_REACH, { minimum: 2 });
for (const seed of seedsFor("next-lower-m3-windows", [0x7731, 0x7732]))
  it(`executes generated window frames against both engines, seed ${String(seed)}`, async () => {
    const core = await new StorageCore(32).open();
    const pg = new PGlite();
    await pg.waitReady;
    const current = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
    });
    const random = mulberry32(seed);
    reach.beginHistory();
    try {
      for (let history = 0; history < 4; history++) {
        const name = `w${String(history)}`;
        const ddl = `CREATE TABLE ${name}(id INTEGER PRIMARY KEY,p TEXT,k INTEGER,i INTEGER,v DOUBLE PRECISION)`;
        await core.execute(ddl);
        await pg.exec(ddl);
        await current.execute(ddl);
        const rows = Array.from(
          { length: history === 0 ? 0 : history === 1 ? 1 : 25 },
          (_, id) => ({
            id,
            p: id % 4 === 0 ? null : `p${String(id % 3)}`,
            k: id % 5 === 0 ? null : id % 4,
            i: id % 3 === 0 ? null : Math.floor(random() * 100) - 50,
            v: id % 3 === 0 ? null : (Math.floor(random() * 64) - 32) / 4,
          }),
        );
        if (rows.length) {
          await core.writeBatch("insertBatch", name, rows);
          await current.insertBatch(name, rows);
          const params: unknown[] = [];
          const tuples = rows.map((row) => {
            const start = params.length;
            params.push(row.id, row.p, row.k, row.i, row.v);
            return `(${[1, 2, 3, 4, 5].map((n) => `$${String(start + n)}`).join(",")})`;
          });
          await pg.query(`INSERT INTO ${name} VALUES ${tuples.join(",")}`, params);
        }
        await core.harness.path.recycleLog();
        await core.harness.path.quiesce();
        if (history === 3) {
          await core.reopen();
          reach.reach("reopened");
        }
        const cases: Array<[(typeof LOWER_M3_WINDOWS_REACH)[number], string]> = [
          [
            "literal window amounts",
            `LAG(i,0,99) OVER(PARTITION BY p ORDER BY id),LEAD(i,1,99) OVER(PARTITION BY p ORDER BY id),LAG(i,2) OVER(PARTITION BY p ORDER BY id) AS lag_two,NTH_VALUE(i,3) OVER(PARTITION BY p ORDER BY id),NTILE(3) OVER(PARTITION BY p ORDER BY id)`,
          ],
          ["row_number", `ROW_NUMBER() OVER(PARTITION BY p ORDER BY v DESC,id)`],
          ["rank", `RANK() OVER(PARTITION BY p ORDER BY k DESC)`],
          [
            "running sum",
            `SUM(v) OVER(PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`,
          ],
          [
            "lag lead",
            `LAG(i,2,-99) OVER(PARTITION BY p ORDER BY id),LEAD(i,1,99) OVER(PARTITION BY p ORDER BY id)`,
          ],
          [
            "moving avg",
            `AVG(v) OVER(PARTITION BY p ORDER BY id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)`,
          ],
          [
            "ranking",
            `DENSE_RANK() OVER(PARTITION BY p ORDER BY k),PERCENT_RANK() OVER(PARTITION BY p ORDER BY k),CUME_DIST() OVER(PARTITION BY p ORDER BY k),NTILE(3) OVER(PARTITION BY p ORDER BY id)`,
          ],
          [
            "value functions",
            `FIRST_VALUE(i) OVER(PARTITION BY p ORDER BY id),LAST_VALUE(i) OVER(PARTITION BY p ORDER BY id),NTH_VALUE(i,2) OVER(PARTITION BY p ORDER BY id)`,
          ],
          [
            "default peers",
            `SUM(i) OVER(PARTITION BY p ORDER BY k),COUNT(*) OVER(PARTITION BY p ORDER BY k)`,
          ],
          ["whole partition", `SUM(i) OVER(PARTITION BY p),COUNT(i) OVER(PARTITION BY p)`],
          [
            "integer average",
            `AVG(i) OVER(PARTITION BY p ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)`,
          ],
          ["text order", `RANK() OVER(ORDER BY p COLLATE "C")`],
          [
            "different sorts",
            `ROW_NUMBER() OVER(PARTITION BY p ORDER BY id),RANK() OVER(PARTITION BY p ORDER BY v DESC),LAG(i) OVER(PARTITION BY p ORDER BY id)`,
          ],
          ["named", `ROW_NUMBER() OVER w,COUNT(i) OVER w WINDOW w AS(PARTITION BY p ORDER BY id)`],
          [
            "min max",
            `MIN(i) OVER(PARTITION BY p ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING),MAX(v) OVER(PARTITION BY p ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING)`,
          ],
        ];
        const units = ["ROWS", "RANGE", "GROUPS"] as const;
        for (const unit of units)
          for (const exclude of ["NO OTHERS", "CURRENT ROW", "GROUP", "TIES"]) {
            const width = 1 + Math.floor(random() * 4);
            cases.push([
              unit,
              `COUNT(*) OVER(PARTITION BY p ORDER BY k DESC NULLS LAST${unit === "ROWS" ? ",id" : ""} ${unit} BETWEEN ${String(width)} PRECEDING AND 1 FOLLOWING EXCLUDE ${exclude}),SUM(i) OVER(PARTITION BY p ORDER BY k DESC NULLS LAST${unit === "ROWS" ? ",id" : ""} ${unit} BETWEEN ${String(width)} PRECEDING AND 1 FOLLOWING EXCLUDE ${exclude}),AVG(v) OVER(PARTITION BY p ORDER BY k DESC NULLS LAST${unit === "ROWS" ? ",id" : ""} ${unit} BETWEEN ${String(width)} PRECEDING AND 1 FOLLOWING EXCLUDE ${exclude})`,
            ]);
          }
        reach.reach("duplicate window inputs");
        const operations: Operation[] = cases.map(([, expr]) => ({
          kind: "sql",
          sql: expr.includes(" WINDOW ")
            ? `SELECT id,p,k,i,v,p AS p_copy,k AS k_copy,${expr.replace(" WINDOW ", ` FROM ${name} WINDOW `)} ORDER BY id`
            : `SELECT id,p,k,i,v,p AS p_copy,k AS k_copy,${expr} FROM ${name} ORDER BY id`,
          totalOrder: "primary key id",
        }));
        const candidate: EngineUnderTest = {
          run: async (operation) => {
            if (operation.kind !== "sql") throw new Error("query expected");
            const result = await core.execute(operation.sql);
            if (!("batches" in result)) throw new Error("rows expected");
            return {
              kind: "rows",
              columns: result.columns.map((c) => c.name),
              domains: result.columns.map(() => null),
              rows: oracleRows(result),
            };
          },
          close: () => Promise.resolve(),
        };
        const authority: EngineUnderTest = {
          run: async (operation) => {
            if (operation.kind !== "sql") throw new Error("query expected");
            const result = await pg.query<Record<string, unknown>>(operation.sql);
            // Representation metadata may inspect a successful next plan. A next preparation
            // failure must never be relabeled as a PostgreSQL error in the RED run.
            const plan = (() => {
              try {
                return core.prepare(operation.sql).plan;
              } catch {
                return null;
              }
            })();
            return {
              kind: "rows",
              columns: result.fields.map((f) => f.name),
              domains: result.fields.map(() => null),
              rows: result.rows.map((row) =>
                Object.fromEntries(
                  Object.entries(row).map(([key, value], index) => [
                    key,
                    plan === null
                      ? typeof value === "bigint"
                        ? Number(value)
                        : value
                      : queryAuthorityValue(
                          plan,
                          index,
                          typeof value === "bigint" ? Number(value) : value,
                        ),
                  ]),
                ),
              ),
            };
          },
          close: () => Promise.resolve(),
        };
        const shipped: EngineUnderTest = {
          run: async (operation) => {
            if (operation.kind !== "sql") throw new Error("query expected");
            const result = await current.query(operation.sql);
            return {
              kind: "rows",
              columns: result.columns,
              domains: result.columns.map(() => null),
              rows: result.rows,
            };
          },
          close: () => Promise.resolve(),
        };
        let checked = 0;
        await compareEngines(
          { name: `M3 windows ${String(seed)} history ${String(history)}`, operations },
          [authority, candidate, shipped],
          (outcome) => {
            if (!outcome.ok) throw new Error("matching refusals do not prove window execution");
            const entry = cases[checked++];
            if (entry) reach.reach(entry[0]);
          },
        );
        expect(checked).toBe(cases.length);
        reach.reach(history === 0 ? "empty" : history === 1 ? "singleton" : "NULL peers");
      }
      reach.reach("flushed");
      reach.reach("complete");
    } finally {
      await core.close();
      await pg.close();
      await current.close();
    }
  }, 180_000);
it("reaches every declared window SQL dimension", () => reach.assertReached());
