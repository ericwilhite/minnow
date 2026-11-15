/** Accepted DML and FTS history share one undo boundary and atomic WAL body.
 * Authority: Statement.noteFtsMutation, PostgreSQL statement/savepoint rollback.
 * The inactive stage4 FTS journal is inspected independently of SQL row results.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_FTS_REACH, MODIFY_FTS_COALESCING_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { ftsDef } from "../access/fts-def.js";
import { StatementRules } from "../exec/statement-rules.js";
import { compileTableRules } from "../exec/rules.js";
import { ChunkReader } from "../page/reader.js";
import { walkFtsTree } from "../format/fts-storage.js";
import { StatementImpl, TransactionImpl } from "../write/transaction.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import {
  openFtsHarness,
  setupFts,
  ftsParts,
  ftsBudget,
  lastFtsCommit,
} from "./fts-transaction.test-support.js";

import { Harness, batch, texts, rules } from "../write/test-support.js";

let queryId = 1_000_000;
/** Real compiled MODIFY in a caller-controlled transaction; no alternate writer/model. */
async function run(core: StorageCore, tx: TransactionImpl, sql: string) {
  const lowered = core.lowerer.lower(sql, { ...core.context(), catalog: tx.catalog });
  if (lowered.kind !== "modify" || lowered.plan.root.op !== "modify") throw new Error("modify");
  const rowRules = new StatementRules(
    tx.catalog,
    lowered.plan.root.tableId,
    (id) => {
      const table = tx.catalog.table(id);
      if (!table) throw new Error("table");
      return compileTableRules(
        table,
        tx.catalog.version,
        core.lowerer.lowerTableRules(table, tx.catalog),
      );
    },
    lowered.plan.root,
  );
  const statement = tx.statement({ rules: (id) => rowRules.table(id), targets: rowRules.targets });
  const execution = core.executor.prepare(lowered.plan).execute({
    snapshot: null,
    statement,
    rowRules,
    parameters: [],
    statementTime: 0n,
    governor: core.harness.governor,
    spill: core.spill,
    queryId: ++queryId,
    reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
    sequences: null,
    sequenceSession: null,
    returning: "atExecuted",
  });
  const drain = (async () => {
    for (;;) {
      const frame = await execution.results.next();
      if (!frame) break;
      frame.release();
    }
  })();
  try {
    await Promise.all([execution.executed, drain, execution.delivered]);
  } catch (error) {
    execution.cancel(error);
    await Promise.allSettled([execution.executed, drain, execution.delivered]);
    statement.rollback();
    throw error;
  } finally {
    await rowRules.close();
  }
}
async function begin(core: StorageCore) {
  const tx = await core.harness.path.begin();
  if (!(tx instanceof TransactionImpl)) throw new Error("native transaction");
  return tx;
}
function funded(tx: TransactionImpl) {
  let count = 0;
  const reserve = tx.reserveFtsRowUndo.bind(tx),
    release = tx.releaseFtsRowUndo.bind(tx);
  const r = vi.spyOn(tx, "reserveFtsRowUndo").mockImplementation(() => {
    reserve();
    count++;
  });
  const f = vi.spyOn(tx, "releaseFtsRowUndo").mockImplementation(() => {
    release();
    count--;
  });
  return {
    check() {
      expect(count, "one reservation per retained funded rows entry").toBe(
        tx.undo.filter((e) => e.kind === "rows" && e.ftsKeyed !== undefined).length,
      );
    },
    close() {
      r.mockRestore();
      f.mockRestore();
    },
  };
}
async function documentIds(core: StorageCore) {
  const budget = ftsBudget(),
    ids: number[] = [];
  for (const part of (await ftsParts(core.harness)).entries) {
    if (part.kind !== "insert") continue;
    for await (const e of walkFtsTree(part.documents.ref, budget, core.harness.space, undefined, {
      kind: "ftsDocuments",
      bounds: part.documents,
    }))
      if (e.kind === "ftsDocuments" && e.value.kind === "leaf")
        for (const id of e.value.ids) ids.push(id);
  }
  expect(budget.held).toBe(0);
  return ids.sort((a, b) => a - b);
}
const forms = [
  "insert",
  "updateFrom",
  "deleteUsing",
  "upsertInsert",
  "upsertUpdate",
  "nothing",
  "whereFalse",
] as const;
const modes = ["commit", "rollback", "savepoint", "failure"] as const;
type Form = (typeof forms)[number];
function mutation(form: Form, n: number, fail: boolean) {
  switch (form) {
    case "insert":
      return `INSERT INTO docs VALUES (${String(n + 1)},'new one',1),(${String(n + 2)},'new two',${String(fail ? -1 : 2)}) RETURNING id`;
    case "updateFrom":
      return `UPDATE docs SET body='changed',n=${fail ? `CASE WHEN docs.id=${String(n - 1)} THEN -1 ELSE src.d END` : "src.d"} FROM src WHERE docs.id=src.link AND docs.id<${String(n)} RETURNING docs.id`;
    case "deleteUsing":
      return `DELETE FROM docs USING src WHERE docs.id=src.link AND docs.id<${String(n)} RETURNING ${fail ? `100/(docs.id-${String(n - 1)})` : "docs.id"}`;
    case "upsertInsert":
      return `INSERT INTO docs VALUES (${String(n + 1)},'new one',1),(${String(n + 2)},'new two',${String(fail ? -1 : 2)}) ON CONFLICT(id) DO UPDATE SET body=excluded.body,n=excluded.n RETURNING id`;
    case "upsertUpdate":
      return `INSERT INTO docs VALUES (1,'new one',1),(2,'new two',${String(fail ? -1 : 2)}) ON CONFLICT(id) DO UPDATE SET body=excluded.body,n=excluded.n RETURNING id`;
    case "nothing":
      return "INSERT INTO docs VALUES (1,'ignored',1) ON CONFLICT DO NOTHING RETURNING id";
    case "whereFalse":
      return "INSERT INTO docs VALUES (1,'ignored',1) ON CONFLICT(id) DO UPDATE SET body=excluded.body WHERE docs.n<0 RETURNING id";
  }
}
for (const seed of seedsFor("next-modify-fts", [0xb613, 0xb614]))
  it(`keeps native DML FTS invalidation atomic with rows ${String(seed)}`, async () => {
    const pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    const reach = createReach("native DML FTS", MODIFY_FTS_REACH);
    const reference = async (sql: string) => {
      await pg.exec(sql);
      await shipped.run({ kind: "sql", sql });
    };
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...forms),
          fc.constantFrom(...modes),
          fc.integer({ min: 3, max: 9 }),
          fc.boolean(),
          async (form, mode, n, definition) => {
            reach.beginHistory();
            const core = new StorageCore(256);
            let tx: TransactionImpl | undefined,
              docsId = 0;
            const notes: string[] = [];
            // Intentionally borrow the prototype method and invoke it with the same native owner.
            // eslint-disable-next-line @typescript-eslint/unbound-method
            const nativeNote = StatementImpl.prototype.noteFtsMutation;
            const note = vi
              .spyOn(StatementImpl.prototype, "noteFtsMutation")
              .mockImplementation(function (this: StatementImpl, id, kind) {
                if (id === docsId) {
                  expect(
                    this.tx.undo.some(
                      (e) =>
                        e.statement === this.ordinal &&
                        (e.kind === "rows" || e.kind === "memDelete" || e.kind === "groupDelete") &&
                        e.tableId === id,
                    ),
                    "notification follows an accepted native write",
                  ).toBe(true);
                  notes.push(kind);
                }
                nativeNote.call(this, id, kind);
              });
            try {
              await openFtsHarness(core.harness);
              await reference("DROP TABLE IF EXISTS src");
              await reference("DROP TABLE IF EXISTS docs");
              for (const sql of [
                "CREATE TABLE docs(id INT PRIMARY KEY,body TEXT,n INT NOT NULL,CONSTRAINT positive CHECK(n>0))",
                "CREATE TABLE src(link INT,d INT)",
                `INSERT INTO src VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},${String(i + 10)})`).join(",")}`,
              ]) {
                await core.execute(sql);
                await reference(sql);
              }
              const table = core.harness.path.catalog.tableByName("docs");
              if (!table) throw new Error("docs");
              docsId = table.id;
              if (definition) {
                const textColumn = table.columns[1];
                if (!textColumn) throw new Error("body column");
                const { id: _id, ...fts } = ftsDef(1, docsId, textColumn.id);
                void _id;
                await core.change({ kind: "createFts", fts });
              }
              const seedSql = `INSERT INTO docs VALUES ${Array.from({ length: n }, (_, i) => `(${String(i + 1)},'alpha ${String(i)}',${String(i + 1)})`).join(",")} RETURNING id`;
              tx = await begin(core);
              const initial = funded(tx);
              try {
                await run(core, tx, seedSql);
                initial.check();
                const entries = tx.undo.filter((e) => e.kind === "rows" && e.tableId === docsId);
                expect(entries).toHaveLength(1);
                expect(entries[0]).toMatchObject({ count: n, ftsKeyed: false });
                const receipt = await tx.commit(null);
                await receipt.durable;
              } finally {
                initial.close();
              }
              await reference(seedSql);
              expect(notes).toEqual([]);
              if (definition) expect(await documentIds(core)).toHaveLength(n);
              reach.reach("coalesced insert");
              const before = structuredClone(core.harness.path.state().root.fts);
              tx = await begin(core);
              const accounting = funded(tx),
                point = tx.savepoint();
              try {
                await reference("BEGIN");
                await reference("SAVEPOINT start_here");
                const noop = form === "nothing" || form === "whereFalse";
                const failure = mode === "failure" && !noop;
                const sql = mutation(form, n, failure);
                if (failure) {
                  await expect(run(core, tx, sql)).rejects.toMatchObject({
                    sqlState: form === "deleteUsing" ? "22012" : "23514",
                  });
                  await expect(pg.exec(sql)).rejects.toMatchObject({
                    code: form === "deleteUsing" ? "22012" : "23514",
                  });
                  if (form === "deleteUsing") {
                    // Shipped 0.x deliberately returns NULL for division by zero
                    // (query.ts integerDivide; published 0.19.0 changelog). Native
                    // follows PG22012; rollback to the same savepoint on both.
                    const old = await shipped.run({ kind: "sql", sql });
                    expect(old.counts?.rowCount).toBe(n - 1);
                    expect(old.rows?.some((row) => row.expression === null)).toBe(true);
                  } else await expect(shipped.run({ kind: "sql", sql })).rejects.toThrow();
                  reach.reach("late statement error");
                } else {
                  await run(core, tx, sql);
                  await reference(sql);
                  if (noop) expect(notes).toEqual([]);
                  else if (form === "updateFrom") expect(notes).toContain("update");
                  else if (form === "deleteUsing") expect(notes).toContain("delete");
                  else if (form !== "insert") expect(notes).toContain("upsert");
                }
                accounting.check();
                if (mode === "rollback") {
                  tx.rollback();
                  await reference("ROLLBACK");
                  expect(core.harness.path.state().root.fts).toEqual(before);
                } else {
                  if (mode === "savepoint" || mode === "failure") {
                    tx.rollbackTo(point);
                    accounting.check();
                    await reference("ROLLBACK TO SAVEPOINT start_here");
                    const survivor = "INSERT INTO docs VALUES (999,'survivor',999) RETURNING id";
                    await run(core, tx, survivor);
                    await reference(survivor);
                    reach.reach("survivor after rollback");
                  }
                  accounting.check();
                  const acceptedIds: number[] = [];
                  for (const e of tx.undo)
                    if (e.kind === "rows" && e.tableId === docsId)
                      for (let i = 0; i < e.count; i++)
                        acceptedIds.push(e.memtable.firstRowId + e.before + i);
                  const receipt = await tx.commit(null);
                  await receipt.durable;
                  await reference("COMMIT");
                  const keyed = mode === "commit" && !noop && form !== "insert";
                  const logged = await lastFtsCommit(core.harness);
                  expect(
                    logged.body.effects.some(
                      (e) => e.kind === "ftsMutation" && e.tableId === docsId,
                    ),
                    "keyed invalidation shares the accepted row commit",
                  ).toBe(keyed);
                  if (keyed) {
                    expect(logged.lastLsn).toBe(receipt.lsn);
                    expect(
                      (await ftsParts(core.harness)).entries.some(
                        (e) => e.kind === "keyed" && e.tableId === docsId,
                      ),
                    ).toBe(true);
                    reach.reach("same WAL body");
                  } else if (definition && !(mode === "commit" && noop)) {
                    expect(await documentIds(core)).toEqual(acceptedIds);
                  }
                }
                reach.reach("undo accounting");
              } finally {
                accounting.close();
              }
              const compare = async () => {
                const sql = "SELECT * FROM docs ORDER BY id",
                  actual = await core.execute(sql);
                if (!("batches" in actual)) throw new Error("rows");
                expect(oracleRows(actual)).toEqual((await pg.query(sql)).rows);
                expect(oracleRows(actual)).toEqual((await shipped.run({ kind: "sql", sql })).rows);
              };
              await compare();
              await core.harness.path.recycleLog();
              await core.reopen();
              await compare();
              reach.reach(form);
              reach.reach(mode);
              reach.reach(definition ? "definition" : "no definition");
              reach.reach("reopen");
              reach.reach("PG and shipped");
            } finally {
              note.mockRestore();
              await Promise.allSettled([
                pg.exec("ROLLBACK"),
                shipped.run({ kind: "sql", sql: "ROLLBACK" }),
              ]);
              tx?.rollback();
              await core.close();
              expect(core.harness.governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 20,
          examples: [
            ["upsertInsert", "commit", 3, true],
            ["upsertUpdate", "commit", 4, false],
            ["updateFrom", "commit", 5, true],
            ["deleteUsing", "commit", 4, false],
            ["insert", "failure", 4, true],
            ["deleteUsing", "failure", 4, true],
            ["upsertInsert", "savepoint", 5, true],
            ["updateFrom", "rollback", 5, false],
            ["nothing", "commit", 3, true],
            ["whereFalse", "commit", 3, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await shipped.close();
      await pg.close();
    }
  }, 120_000);

for (const seed of seedsFor("next-modify-fts-coalescing", [0xb615, 0xb616]))
  it(`retains a keyed flag and exactly one reservation across later appends ${String(seed)}`, async () => {
    const reach = createReach("FTS coalescing", MODIFY_FTS_COALESCING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 12 }),
        fc.nat(20),
        fc.boolean(),
        async (n, position, rollback) => {
          reach.beginHistory();
          const h = new Harness(),
            path = await setupFts(h);
          const tx = await path.begin();
          if (!(tx instanceof TransactionImpl)) throw new Error("native transaction");
          const accounting = funded(tx),
            point = tx.savepoint();
          try {
            const s = tx.statement({ rules }),
              writer = s.table(1),
              mark = position % (n - 1);
            for (let i = 0; i < n; i++) {
              await writer.insert(batch(texts([`alpha ${String(i)}`])), null);
              if (i === mark) s.noteFtsMutation(1, "upsert");
              accounting.check();
              const rows = tx.undo.filter((e) => e.kind === "rows");
              expect(rows).toHaveLength(1);
              expect(rows[0]).toMatchObject({ count: i + 1, ftsKeyed: i >= mark });
            }
            await s.end();
            if (rollback) {
              tx.rollbackTo(point);
              accounting.check();
              expect(tx.undo.filter((e) => e.kind === "rows")).toHaveLength(0);
              const next = tx.statement({ rules });
              await next.table(1).insert(batch(texts(["survivor"])), null);
              await next.end();
            }
            accounting.check();
            const receipt = await tx.commit(null);
            await receipt.durable;
            const logged = await lastFtsCommit(h);
            expect(logged.body.effects.some((e) => e.kind === "ftsMutation")).toBe(!rollback);
            reach.reach(rollback ? "rollback flag" : "commit flag");
            reach.reach(mark === 0 ? "first flag" : "later flag");
            reach.reach("ordinary append after flag");
            reach.reach("one funded entry");
          } finally {
            accounting.close();
            tx.rollback();
            await h.close();
            expect(h.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [3, 0, false],
          [6, 3, true],
        ],
      },
    );
    reach.assertReached();
  }, 60_000);
