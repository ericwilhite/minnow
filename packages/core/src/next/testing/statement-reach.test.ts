/** Trigger/FK reach follows immutable IDs, event, assignment and remaining body budget. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STATEMENT_RULE_REACH } from "../../testing/reach.js";
import { statementRuleReach } from "../exec/statement-reach.js";
import { StatementRules } from "../exec/statement-rules.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { column, INT, tableInput } from "../write/test-support.js";
import { ChunkReader } from "../page/reader.js";
it.each(seedsFor("next-statement-rule-reach", [0xb5cd]))(
  "tracks event/budget diamonds and refuses uninstalled firing (seed %i)",
  async (seed) => {
    const reach = createReach("statement rule reach", STATEMENT_RULE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 7 }), fc.boolean(), async (delta, returning) => {
        reach.beginHistory();
        const core = await new StorageCore().open();
        try {
          for (const sql of [
            "CREATE TABLE p(id INTEGER PRIMARY KEY,v INTEGER)",
            "CREATE TABLE side(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id) ON DELETE CASCADE,v INTEGER CHECK(v>=0))",
            "CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id) ON DELETE SET NULL)",
            "CREATE TABLE sink(v INTEGER CHECK(v>=0))",
            "CREATE TABLE info_sink(v INTEGER CHECK(v>=0))",
            "CREATE TABLE tail(v INTEGER CHECK(v>=0))",
            "CREATE TABLE gp(id INTEGER PRIMARY KEY,base INTEGER,k INTEGER GENERATED ALWAYS AS(base+1) STORED UNIQUE)",
            "CREATE TABLE gc(pid INTEGER REFERENCES gp(k))",
            "CREATE TABLE driver(id INTEGER PRIMARY KEY,v INTEGER)",
            "INSERT INTO p VALUES(1,0)",
            "INSERT INTO side VALUES(1,1,1)",
            "INSERT INTO c VALUES(1,1)",
            "CREATE TRIGGER tc AFTER UPDATE ON c BEGIN UPDATE side SET pid=NEW.pid,v=COALESCE(NEW.pid,0) WHERE id=NEW.id;END",
            "CREATE TRIGGER ts AFTER UPDATE ON side BEGIN INSERT INTO sink(v) VALUES(NEW.v);END",
            "CREATE TRIGGER back AFTER INSERT ON sink BEGIN INSERT INTO tail(v) VALUES(NEW.v);UPDATE side SET v=NEW.v WHERE id=1;END",
            "CREATE TRIGGER tg AFTER UPDATE ON gc BEGIN INSERT INTO sink(v) VALUES(NEW.pid);END",
            "CREATE TRIGGER drive AFTER UPDATE ON driver BEGIN UPDATE gp SET base=NEW.v WHERE id=NEW.id;END",
          ])
            await core.execute(sql);
          const catalog = core.harness.path.catalog,
            parent = catalog.tableByName("p");
          if (!parent) throw new Error("p");
          if (parent.primaryKey === null) throw new Error("p primary key");
          await core.change({
            kind: "createTable",
            table: tableInput("info", [column("id", INT, false), column("pid", INT, true)], {
              foreignKeys: [
                {
                  name: "info_fk",
                  columns: [1],
                  parentTableId: parent.id,
                  parentIndexId: parent.primaryKey,
                  enforced: false,
                  onDelete: "restrict",
                },
              ],
            }),
          });
          await core.execute(
            "CREATE TRIGGER ti AFTER UPDATE ON info BEGIN INSERT INTO info_sink(v) VALUES(NEW.pid);END",
          );
          const withInfo = core.harness.path.catalog;
          expect(withInfo.tableByName("info")?.foreignKeys[0]?.supportIndexId).toBeNull();
          const root = (sql: string) => {
            const out = core.lowerer.lower(sql, core.context());
            if (out.kind !== "modify" || out.plan.root.op !== "modify") throw new Error("modify");
            return out.plan.root;
          };
          const names = (ids: readonly number[]) =>
            ids.map((id) => withInfo.table(id)?.name).sort();
          const update = root(`UPDATE p SET id=id+${String(delta)}`);
          expect(names(statementRuleReach(withInfo, parent.id, update, -1).tables)).toEqual([
            "c",
            "info",
            "p",
            "side",
          ]);
          expect(names(statementRuleReach(withInfo, parent.id, update, 0).tables)).toEqual([
            "c",
            "info",
            "info_sink",
            "p",
            "side",
            "sink",
          ]);
          expect(names(statementRuleReach(withInfo, parent.id, update, 1).tables)).toEqual([
            "c",
            "info",
            "info_sink",
            "p",
            "side",
            "sink",
            "tail",
          ]);
          expect(statementRuleReach(catalog, parent.id, update, 0).triggers).toBe(true);
          reach.reach("FK preserves budget");
          reach.reach("budget diamond");
          reach.reach("budget zero fires");
          reach.reach("negative budget writes without firing");
          reach.reach("informational update only");
          const lookedUp: number[] = [];
          const scope = new StatementRules(
            withInfo,
            parent.id,
            (id) => {
              lookedUp.push(id);
              return { check: () => null, generate: () => undefined };
            },
            update,
          );
          expect(scope.requiresTriggers).toBe(true);
          expect(scope.minimumBytes()).toBeGreaterThan(0);
          expect(names(lookedUp)).toEqual(["info_sink", "side", "sink", "tail"]);
          reach.reach("rule minimum includes body targets");
          await scope.close();
          const harmless = root("UPDATE p SET v=v+1");
          expect(statementRuleReach(catalog, parent.id, harmless).triggers).toBe(false);
          expect(
            names(statementRuleReach(catalog, parent.id, root("INSERT INTO p VALUES(2,2)")).tables),
          ).toEqual(["p"]);
          reach.reach("unrelated update");
          reach.reach("insert stops FK edge");
          expect(
            names(statementRuleReach(withInfo, parent.id, root("DELETE FROM p")).tables),
          ).toEqual(["c", "side", "sink"]);
          reach.reach("delete event differs");
          const generated = root(`UPDATE gp SET base=base+${String(delta)}`);
          expect(names(statementRuleReach(catalog, generated.tableId, generated).tables)).toEqual([
            "gc",
            "gp",
            "side",
            "sink",
            "tail",
          ]);
          expect(statementRuleReach(catalog, generated.tableId, generated).triggers).toBe(true);
          const bodyGenerated = root(`UPDATE driver SET v=v+${String(delta)}`);
          expect(
            names(statementRuleReach(catalog, bodyGenerated.tableId, bodyGenerated).tables),
          ).toEqual(["driver", "gc", "gp", "sink"]);
          reach.reach("generated key propagation");
          for (const sql of [
            `UPDATE p SET id=id+${String(delta)}${returning ? " RETURNING id" : ""}`,
            `DELETE FROM p${returning ? " RETURNING id" : ""}`,
          ])
            await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "0A000" });
          // Raw executor clients may omit StatementRules; their writes need the same guard.
          const raw = core.lowerer.lower(
            `UPDATE p SET id=id+${String(delta)}${returning ? " RETURNING id" : ""}`,
            core.context(),
          );
          if (raw.kind !== "modify") throw new Error("modify");
          const tx = await core.harness.path.begin();
          const statement = tx.statement({
            rules: () => ({ check: () => null, generate: () => undefined }),
          });
          try {
            const execution = core.executor.prepare(raw.plan).execute({
              snapshot: null,
              statement,
              parameters: [],
              statementTime: 0n,
              governor: core.harness.governor,
              spill: core.spill,
              queryId: 999999,
              reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
            });
            await expect(execution.executed).rejects.toMatchObject({ sqlState: "0A000" });
            await Promise.allSettled([execution.executed, execution.delivered]);
          } finally {
            tx.rollback();
          }
          reach.reach("raw executor guarded");
          reach.reach(returning ? "RETURNING guarded" : "plain guarded");
          await core.execute("UPDATE p SET v=v+1");
          const rows = await core.execute("SELECT * FROM p");
          if (rows.kind !== "rows" || !("batches" in rows)) throw new Error("rows");
          expect(oracleRows(rows)).toEqual([{ id: 1, v: 1 }]);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("guard before mutation");
        } finally {
          await core.close();
        }
        expect(core.harness.governor.usedBytes).toBe(0);
      }),
      {
        seed,
        numRuns: 6,
        examples: [
          [1, false],
          [2, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
