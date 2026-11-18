/** Native effect integration. Trigger coverage drives the real body runner with a literal
 * single-row native host in the SAME statement; the SQL firing provider belongs to B5b. */
import { afterAll, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, LIVE_EFFECT_INTEGRATION_REACH } from "../../testing/reach.js";
import { column as dslColumn, schema, table } from "../../engine/schema.js";
import { ChangeFeed, type CommittedChange } from "../access/changes.js";
import { StorageCore } from "../testing/storage-core.js";
import { migrateCatalog } from "../owner/catalog-migration.js";
import { TriggerBodyRunner } from "../exec/trigger-body.js";
import { exportSnapshot } from "./snapshot-capture.js";
import { loadSnapshot } from "./snapshot-load.js";
import { snapshotTestTarget } from "./snapshot-test-support.js";
import { Harness, batch, ints, rules } from "./test-support.js";
const turn = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const reach = createReach("next-live-effect-integration", LIVE_EFFECT_INTEGRATION_REACH);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-live-effect-integration", [8864, 8865])) {
  for (const scenario of [
    "RI",
    "trigger body",
    "partial statement",
    "migration",
    "snapshot load",
  ] as const) {
    it(`publishes accepted ${scenario} effects seed=${String(seed)}`, async () => {
      reach.beginHistory();
      const core = await new StorageCore(16).open();
      const random = mulberry32(seed),
        n = 1 + Math.floor(random() * 20);
      let held = 0;
      const budget = {
        allocate(bytes: number) {
          held += bytes;
          return true;
        },
        free(bytes: number) {
          held -= bytes;
          expect(held).toBeGreaterThanOrEqual(0);
        },
      };
      try {
        await core.execute("CREATE TABLE p(id INT PRIMARY KEY)");
        await core.execute(
          "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE)",
        );
        await core.execute("CREATE TABLE audit(id INT)");
        await core.execute(`INSERT INTO p VALUES(${String(n)})`);
        await core.execute(`INSERT INTO c VALUES(${String(n)},${String(n)})`);
        await turn();
        const path = core.harness.path,
          p = path.catalog.tableByName("p"),
          c = path.catalog.tableByName("c"),
          audit = path.catalog.tableByName("audit");
        if (!p || !c || !audit) throw new Error("Missing integration tables");
        const events: Array<{
          lsn: number;
          tables: number[];
          catalog: boolean;
          replacement: boolean;
          unknown: boolean;
        }> = [];
        const observe = (e: CommittedChange) =>
          events.push({
            lsn: e.lsn,
            tables: e.tables.slice(0, e.tableCount).sort((a, b) => a - b),
            catalog: e.catalog,
            replacement: e.replacement,
            unknown: e.unknown,
          });
        const stop = path.changes.observe(observe);
        if (scenario === "RI") {
          await core.execute(`DELETE FROM p WHERE id=${String(n)}`);
          await turn();
          expect(events).toHaveLength(1);
          expect(events[0]).toMatchObject({
            tables: [p.id, c.id].sort((a, b) => a - b),
            catalog: false,
            replacement: false,
            unknown: false,
          });
        } else if (scenario === "trigger body") {
          const declaration = core.lowerer.lower(
            `CREATE TRIGGER log AFTER INSERT ON p BEGIN INSERT INTO audit(id) VALUES(${String(n + 100)}); END`,
            core.context(),
          );
          if (declaration.kind !== "ddl" || declaration.change?.kind !== "createTrigger")
            throw new Error("Missing trigger");
          const body = declaration.change.trigger.statements[0];
          if (!body) throw new Error("Missing body");
          const lowered = core.lowerer.lower(body.sql, {
            ...core.context(),
            parameterTypes: body.bindings.map((b) => b.type),
          });
          if (lowered.kind !== "modify") throw new Error("Missing body plan");
          const runner = new TriggerBodyRunner(p, body, lowered.plan);
          const tx = await path.begin(),
            statement = tx.statement({ rules });
          try {
            await statement.table(p.id).insert(batch(ints([n + 100])), null);
            let applied = false;
            await runner.run(
              { rootId: 1, id: 1, frameId: 0 },
              { spoolId: 1, row: 0 },
              {
                statement,
                native: {
                  readRowBindings: async () => {
                    throw new Error("Literal body must not read images");
                  },
                },
                host: {
                  spoolBodyInput: async ({ command }) => ({ rootId: command.rootId, id: 1 }),
                  applyBodyInput: async () => {
                    await statement.table(audit.id).insert(batch(ints([n + 100])), null);
                    applied = true;
                  },
                  releaseBodyInput: async () => undefined,
                },
                hooks: { beforeRow: async () => undefined, afterRow: async () => undefined },
                charge: budget,
                work: { signal: new AbortController().signal },
              },
            );
            expect(applied).toBe(true);
            await statement.end();
            await (
              await tx.commit(null)
            ).durable;
          } catch (error) {
            statement.rollback();
            tx.rollback();
            throw error;
          }
          await turn();
          expect(events).toHaveLength(1);
          expect(events[0]).toMatchObject({
            tables: [p.id, audit.id].sort((a, b) => a - b),
            catalog: false,
            unknown: false,
          });
        } else if (scenario === "partial statement") {
          const tx = await path.begin();
          let st = tx.statement({ rules });
          await st.table(audit.id).insert(batch(ints([n])), null);
          await st.end();
          st = tx.statement({ rules });
          // First native call succeeds, then the statement fails after partial accepted writes.
          await st.table(p.id).insert(batch(ints([n + 100])), null);
          await expect(
            Promise.resolve().then(() => st.table(p.id).insert(batch(ints([null])), null)),
          ).rejects.toMatchObject({
            sqlState: "23502",
          });
          st.rollback();
          const receipt = await tx.commit(null);
          await receipt.durable;
          await turn();
          expect(events).toHaveLength(1);
          expect(events[0]).toMatchObject({
            lsn: receipt.lsn,
            tables: [audit.id],
            catalog: false,
            unknown: false,
          });
        } else if (scenario === "migration") {
          await migrateCatalog(
            path,
            core.lowerer,
            core.harness.governor,
            schema([
              table("managed", { id: dslColumn.integer(), value: dslColumn.integer().default(n) }),
            ]),
          );
          await turn();
          expect(events).toHaveLength(1);
          expect(events[0]).toMatchObject({ catalog: true, replacement: false });
        } else {
          const chunks: Uint8Array[] = [];
          await exportSnapshot(
            path,
            new Uint8Array(16),
            {
              write(bytes) {
                chunks.push(bytes.slice());
              },
            },
            budget,
          );
          const data = new Uint8Array(chunks.reduce((sum, b) => sum + b.length, 0));
          let at = 0;
          for (const chunk of chunks) {
            data.set(chunk, at);
            at += chunk.length;
          }
          // Offline replacement keeps an owner-session feed alive across its exclusive loader.
          const target = new Harness("strict", "memory");
          const feed = ChangeFeed.create(budget, path.changes.probe());
          const replacementStop = feed.observe(observe);
          const prior = feed.probe();
          try {
            await loadSnapshot(
              {
                byteLength: data.length,
                async readInto(offset, into) {
                  into.set(data.subarray(offset, offset + into.length));
                },
              },
              async () => ({ ...(await snapshotTestTarget(target, prior, false)), changes: feed }),
              target.governor,
              { replace: true },
            );
            await turn();
            expect(events).toHaveLength(1);
            expect(events[0]).toMatchObject({
              lsn: prior + 1,
              tables: [],
              catalog: true,
              replacement: true,
              unknown: true,
            });
            expect(feed.probe()).toBe(prior + 1);
          } finally {
            replacementStop();
            feed.close();
          }
          expect(target.governor.usedBytes).toBe(0);
        }
        stop();
        reach.reach(scenario);
      } finally {
        await core.close();
      }
      expect(held).toBe(0);
      expect(core.harness.governor.usedBytes).toBe(0);
      reach.reach("zero charges");
    });
  }
}
