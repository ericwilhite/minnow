/** Real native primary/end integration; synthetic final callback, no SQL trigger provider. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  NATIVE_PRIMARY_REACH,
  NATIVE_PRIMARY_OWNERSHIP_REACH,
  NATIVE_PRIMARY_LIFETIME_REACH,
  NATIVE_PRIMARY_ADMISSION_REACH,
  NATIVE_PRIMARY_OWNER_CLOSE_REACH,
  NATIVE_PRIMARY_END_FENCE_REACH,
  NATIVE_PRIMARY_CLOSE_FAULT_REACH,
  NATIVE_ACTION_UNIQUE_REACH,
  NATIVE_BOUNDARY_REFERENCE_REACH,
  NATIVE_PRIMARY_IMAGE_WIDTH_REACH,
  NATIVE_PRIMARY_FTS_REACH,
  NATIVE_COMMAND_UNDO_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { currentEngine } from "../../testing/differential-current.js";
import { compileTableRules } from "../exec/rules.js";
import type { BoundTableRules } from "../exec/row-rules.js";
import { createLowerer } from "../lower/create.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { NativeCommandChecks } from "./command-checks.js";
import { NativeCommandExecution, type NativeCommandExecutionScope } from "./command-execution.js";
import { DivisionByZeroError } from "../exec/errors.js";
import { RegistryCommands } from "./registry-commands.js";
import { RegistryDirectory } from "./registry-directory.js";
import { RegistryFragments } from "./registry-fragments.js";
import { RegistryImages } from "./registry-images.js";
import type { AcceptedCommandRow } from "./registry-commands.js";
import { WritePathImpl } from "../write/engine.js";
import { yieldTask } from "../task.js";
import type { CommandToken } from "../access/statement-command.js";
import { ftsDef } from "../access/fts-def.js";
import { ftsMutationScan } from "../write/fts-undo.js";
import { openFtsHarness, ftsParts, lastFtsCommit } from "./fts-transaction.test-support.js";
import { NATIVE_PRIMARY_BYTES } from "../write/primary-command.js";
import { StatementImpl } from "../write/transaction.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  TEXT,
  batch,
  ints,
  texts,
  rowsOf,
  locators,
} from "../write/test-support.js";
async function result(run: () => Promise<unknown>): Promise<string | null> {
  try {
    await run();
    return null;
  } catch (error) {
    if (typeof error === "object" && error !== null) {
      if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
      if ("code" in error && typeof error.code === "string") return error.code;
    }
    throw error;
  }
}
async function fixture(
  count: number,
  published: boolean,
  after: boolean,
  closeGate?: () => Promise<void>,
  finishGate?: () => Promise<void>,
  actionCount = 0,
  reference = false,
  fts = false,
  frames = 1,
) {
  const h = new Harness("strict", "memory", 256),
    path = fts ? await openFtsHarness(h) : await h.open();
  let id = 0,
    parent = 0,
    parentIndex = 0;
  await autocommit(path, async (s) => {
    if (reference) {
      parent =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("p", [column("id", INT, false)]),
          })
        ).id ?? 0;
      parentIndex = s.catalog.table(parent)?.primaryKey ?? 0;
    }
    id =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput(
            "t",
            [column("id", INT, false), column("n", INT, false), column("payload", TEXT)],
            {
              checks: [{ name: "positive", columns: [1], conditionSql: "n>0" }],
              foreignKeys: reference
                ? [
                    {
                      name: "t_n_fkey",
                      columns: [1],
                      parentTableId: parent,
                      parentIndexId: parentIndex,
                      onDelete: "noAction",
                      enforced: true,
                    },
                  ]
                : [],
            },
          ),
        })
      ).id ?? 0;
    if (fts) {
      const columnId = s.catalog.table(id)?.columns[2]?.id;
      if (!columnId) throw new Error("Missing FTS payload column");
      const { id: ignored, ...definition } = ftsDef(1, id, columnId);
      void ignored;
      await s.changeCatalog({ kind: "createFts", fts: definition });
    }
  });
  const seed = Array.from({ length: count }, (_, i) => i + 1);
  if (reference) await autocommit(path, (s) => s.table(parent).insert(batch(ints(seed)), null));
  await autocommit(path, (s) =>
    s.table(id).insert(batch(ints(seed), ints(seed), texts(seed.map(() => "seed"))), null),
  );
  if (published) {
    await path.recycleLog();
    await path.quiesce();
  }
  const seedPin = path.pin();
  let firstNative: number;
  try {
    firstNative = seedPin.internalTable(id).rowId(0);
  } finally {
    seedPin.release();
  }

  const root = await h.governor.admit({
      label: "native command root",
      minimumBytes: 2 * 1024 * 1024,
      spillable: false,
    }),
    range = await h.governor.admit({
      label: "native command shared cursor",
      minimumBytes: 32 * 1024 * 1024,
      spillable: false,
    });
  const tx = await path.begin(),
    startPoint = tx.savepoint();
  let bound: BoundTableRules | null = null;
  const statement = tx.statement({
    rules: (table) => {
      if (table !== id || !bound) throw new Error("Missing bound rules");
      return bound;
    },
  });
  if (!(statement instanceof StatementImpl)) throw new Error("Expected native statement");
  const def = statement.catalog.table(id);
  if (!def) throw new Error("Missing native table");
  const lowerer = createLowerer(),
    stopping = new AbortController(),
    signal = stopping.signal;
  bound = compileTableRules(
    def,
    statement.catalog.version,
    lowerer.lowerTableRules(def, statement.catalog),
    { charge: root, capacity: 1, signal },
  );
  const spill = new MemorySpillService(),
    directory = new RegistryDirectory(root, spill, 1, signal),
    fragments = new RegistryFragments(root, spill, 1, signal, () => undefined),
    images = new RegistryImages({
      rootId: 61,
      catalog: statement.catalog,
      directory,
      fragments,
      charge: root,
      signal,
      prepare: () => undefined,
      checkpoint: () => undefined,
    });
  const commands = new RegistryCommands({
      rootId: 61,
      frames,
      rowEvents: actionCount,
      directory,
      charge: root,
      signal,
      checkpoint: () => undefined,
      referentialName: (fk, event) => {
        if (actionCount === 0) throw new Error("No FK in primary fixture");
        return `RI_ConstraintTrigger_a_${String(fk)}_${event}`;
      },
    }),
    command = commands.beginCommand(1, 1);
  let leases = 0,
    finished = 0,
    closed = false;
  const checks = new NativeCommandChecks({
    statement,
    tables: reference ? [id, parent] : [id],
    images,
    charge: root,
    signal,
    prepareRange: async (plan, stop) => {
      expect(leases).toBe(0);
      stop.throwIfAborted();
      expect(plan.minimumBytes).toBeLessThanOrEqual(range.held);
      return range;
    },
  });
  let referentialWork: NativeCommandExecutionScope["runReferential"] | null = null;
  const execution = new NativeCommandExecution({
    frames,
    catalog: statement.catalog,
    commands,
    images,
    checks,
    charge: root,
    signal,
    abortRoot: (reason) => stopping.abort(reason),
    runReferential: async (entry, hooks, stop) => {
      if (!referentialWork) throw new Error("No FK action in primary fixture");
      await referentialWork(entry, hooks, stop);
    },
  });
  const retained = statement.table(id),
    retainedUpdate = statement.table(id, { assigned: [def.columns[1]?.id ?? -1] });
  const primary = statement.openNativeCommands({
    tables: [id],
    charge: root,
    signal,
    commandBudget: (token) => commands.budget(token),
    finishRoot: async (stop) => {
      finished++;
      if (finishGate) {
        await finishGate();
        return;
      }
      await execution.verifyCommandUnique(command, stop);
      if (after) throw Object.assign(new Error("later AFTER callback"), { sqlState: "22P02" });
      await execution.verifyCommandReferences(command, stop);
    },
    closeRoot: async () => {
      await closeGate?.();
      closed = true;
    },
  });
  const admitted = path.reserveCommandRows.bind(path),
    reserve = vi.spyOn(path, "reserveCommandRows").mockImplementation(async (...args) => {
      expect(leases, "image retained across writer preparation").toBe(0);
      return await admitted(...args);
    }),
    legacyAdmission = vi.spyOn(path, "admit").mockImplementation(() => {
      throw new Error("native primary readmitted under image lease");
    });
  const suspects = vi.spyOn(statement.suspects, "push").mockImplementation(() => {
      throw new Error("legacy uniqueness queue retained");
    }),
    children = vi.spyOn(statement.childChecks, "push").mockImplementation(() => {
      throw new Error("legacy reference queue retained");
    }),
    parents = vi.spyOn(statement.parentChecks, "push").mockImplementation(() => {
      throw new Error("legacy parent queue retained");
    });
  let disposed = false;
  const dispose = async () => {
    if (disposed) return;
    disposed = true;
    await primary.close();
    expect(closed).toBe(true);
    await execution.close();
    await commands.close();
    await checks.close();
    await images.close();
    await fragments.close();
    await directory.close();
    bound?.close();
    bound = null;
    expect(root.allocated).toBe(0);
    expect(range.allocated).toBe(0);
    root.close();
    range.close();
    expect(spill.bytesInUse).toBe(0);
  };
  return {
    h,
    path,
    tx,
    statement,
    id,
    primary,
    execution,
    setReferentialWork(work: NativeCommandExecutionScope["runReferential"]) {
      referentialWork = work;
    },
    signal,
    root,
    command,
    commands,
    retained,
    retainedUpdate,
    get finished() {
      return finished;
    },
    async write(
      event: "insert" | "update" | "delete",
      before: number | null,
      key: number,
      n: number | null,
      payload: string | null = "changed",
      owner: CommandToken = command,
    ) {
      const old =
          before === null ? null : batch(ints([before + 1]), ints([before + 1]), texts(["seed"])),
        next = event === "delete" ? null : batch(ints([key]), ints([n]), texts([payload]));
      const ref = await images.append(
          id,
          old ? { batch: old, row: 0 } : null,
          next ? { batch: next, row: 0 } : null,
        ),
        widths = [0, 0, 0];
      // Preserve the parent's constant-width fixture as a replay control. Generated widths
      // reject that old design through the real native byte-bound check, not a missing method.
      const sizing: Partial<Pick<RegistryImages, "describe">> = images;
      if (sizing.describe) await sizing.describe(ref, widths, signal);
      else widths[2] = next ? 7 : 0;
      const token = await primary.prepare(
        owner,
        id,
        event,
        before === null ? null : firstNative + before,
        widths,
      );
      const lease = await execution.readRowImages(ref, signal);
      leases++;
      let newRowId: number | null;
      try {
        newRowId = await primary.apply(token, lease.old, lease.new);
      } finally {
        lease.release();
        leases--;
      }
      const accepted: AcceptedCommandRow = {
        command: owner,
        tableId: id,
        event,
        images: ref,
        oldRowId: before === null ? null : firstNative + before,
        newRowId,
      };
      await commands.accept(accepted);
      return accepted;
    },
    async settle(error: string | null, savepoint = false) {
      await dispose();
      if (savepoint) tx.rollbackTo(startPoint);
      if (error) {
        statement.rollback();
        tx.rollback();
      } else {
        const receipt = await tx.commit(null);
        await receipt.durable;
      }
    },
    async continueAfterRollback() {
      // The cancelled execution's idle helper grants must retire before a new statement.
      await dispose();
      legacyAdmission.mockRestore();
      const laterGrant = await h.governor.admit({
        label: "continued row rules",
        minimumBytes: 1024 * 1024,
        spillable: false,
      });
      const table = tx.catalog.table(id);
      if (!table) throw new Error("Missing continued table");
      const laterRules = compileTableRules(
        table,
        tx.catalog.version,
        lowerer.lowerTableRules(table, tx.catalog),
        { charge: laterGrant, capacity: 1, signal: new AbortController().signal },
      );
      const later = tx.statement({ rules: () => laterRules });
      try {
        const count = await later
          .table(id)
          .insert(batch(ints([3]), ints([3]), texts(["continued"])), null);
        await later.end();
        return count;
      } catch (error) {
        later.rollback();
        throw error;
      } finally {
        laterRules.close();
        expect(laterGrant.allocated).toBe(0);
        laterGrant.close();
      }
    },
    async rows() {
      const pin = path.pin();
      try {
        return (await rowsOf(pin, id)).sort((a, b) => Number(a[0]) - Number(b[0]));
      } finally {
        pin.release();
      }
    },
    async close() {
      reserve.mockRestore();
      legacyAdmission.mockRestore();
      suspects.mockRestore();
      children.mockRestore();
      parents.mockRestore();
      await dispose();
      statement.rollback();
      tx.rollback();
      await h.close();
      expect(h.governor.usedBytes).toBe(0);
    },
  };
}
it.each(seedsFor("next-native-primary-image-width", [0xb60f, 0xb610]))(
  "uses stored metadata to prepay variable native rows seed %i",
  async (seed) => {
    const reach = createReach("native metadata width preflight", NATIVE_PRIMARY_IMAGE_WIDTH_REACH),
      pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 100000 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (width, insert, nullable, reject) => {
            reach.beginHistory();
            const f = await fixture(1, false, false),
              payload = nullable ? null : "雪".repeat(width),
              n = reject ? -1 : 2;
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS t; CREATE TABLE t(id INT PRIMARY KEY,n INT NOT NULL,payload TEXT,CONSTRAINT positive CHECK(n>0)); INSERT INTO t VALUES(1,1,'seed')",
              );
              const expected = await result(() =>
                pg.query(
                  insert
                    ? "INSERT INTO t VALUES(2,$1,$2)"
                    : "UPDATE t SET n=$1,payload=$2 WHERE id=1",
                  [n, payload],
                ),
              );
              const actual = await result(async () => {
                await f.write(
                  insert ? "insert" : "update",
                  insert ? null : 0,
                  insert ? 2 : 1,
                  n,
                  payload,
                );
                await f.statement.end(f.signal);
              });
              expect(actual).toBe(expected);
              await f.settle(actual);
              expect(await f.rows()).toEqual(
                (
                  await pg.query<{ id: number; n: number; payload: string | null }>(
                    "SELECT * FROM t ORDER BY id",
                  )
                ).rows.map((row) => [row.id, row.n, row.payload]),
              );
              reach.reach(insert ? "insert" : "update");
              reach.reach(nullable ? "NULL" : "non-NULL");
              reach.reach(width === 0 ? "empty" : width > 65536 ? "above fragment" : "small");
              reach.reach(reject ? "constraint rollback" : "accepted row");
              reach.reach("writer prepared before lease");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [0, true, false, false],
            [12, false, true, false],
            [99999, false, false, true],
            [100000, true, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
);
it.each(seedsFor("next-native-primary", [0xb5ed]))(
  "applies native command images atomically without legacy deferred work seed %i",
  async (seed) => {
    const reach = createReach("native command primary", NATIVE_PRIMARY_REACH),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("insert", "update", "delete", "rekey"),
          fc.constantFrom("none", "notNull", "check", "unique", "after"),
          fc.integer({ min: 2, max: 12 }),
          fc.boolean(),
          async (kind, requested, count, published) => {
            reach.beginHistory();
            const failure =
                kind === "delete" || kind === "rekey"
                  ? requested === "after"
                    ? "after"
                    : "none"
                  : requested,
              f = await fixture(count, published, failure === "after");
            try {
              for (const sql of [
                "DROP TABLE IF EXISTS t",
                "CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER NOT NULL,payload TEXT,CONSTRAINT positive CHECK(n>0))",
                `INSERT INTO t VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)},'seed')`).join(",")}`,
              ]) {
                await pg.exec(sql);
                await shipped.run({ kind: "sql", sql });
              }
              const rows: Array<{ before: number | null; id: number; n: number | null }> =
                Array.from({ length: count }, (_, i) => ({
                  before: kind === "insert" ? null : i,
                  id: kind === "rekey" ? count - i : kind === "insert" ? count + i + 1 : i + 1,
                  n: i + 2,
                }));
              const last = rows[count - 1];
              if (!last) throw new Error("Missing last generated row");
              if (failure === "notNull") last.n = null;
              if (failure === "check") last.n = -1;
              if (failure === "unique") for (const row of rows) row.id = 1;
              const expr =
                failure === "notNull"
                  ? `CASE WHEN id=${String(count)} THEN NULL ELSE n+1 END`
                  : failure === "check"
                    ? `CASE WHEN id=${String(count)} THEN -1 ELSE n+1 END`
                    : "n+1";
              const mutation =
                kind === "insert"
                  ? `INSERT INTO t VALUES ${rows.map((row) => `(${String(row.id)},${row.n === null ? "NULL" : String(row.n)},'changed')`).join(",")}`
                  : kind === "delete"
                    ? "DELETE FROM t"
                    : kind === "rekey"
                      ? `UPDATE t SET id=${String(count + 1)}-id,payload='changed',n=n+1`
                      : `UPDATE t SET ${failure === "unique" ? "id=1," : ""}n=${expr},payload='changed'`;
              if (kind === "rekey") {
                // Product authority: engine/rekeying.mdx permits swaps and access/modify.ts
                // checks uniqueness at statement end. Ordinary PG is an explicit refusal
                // control; a DEFERRABLE INITIALLY IMMEDIATE key supplies the same boundary.
                await pg.exec("BEGIN");
                expect(await result(() => pg.exec(mutation))).toBe("23505");
                await pg.exec("ROLLBACK");
                reach.reach("PG immediate swap refused");
                await pg.exec(
                  "ALTER TABLE t DROP CONSTRAINT t_pkey, ADD PRIMARY KEY(id) DEFERRABLE INITIALLY IMMEDIATE",
                );
                reach.reach("product swap deferred PG control");
              }
              await pg.exec("BEGIN");
              const expected = await result(async () => {
                await pg.exec(mutation);
                if (failure === "after") await pg.exec("SELECT CAST('bad' AS INTEGER)");
              });
              await pg.exec(expected ? "ROLLBACK" : "COMMIT");
              await shipped.run({ kind: "sql", sql: "BEGIN" });
              let shippedFailed = false;
              try {
                await shipped.run({ kind: "sql", sql: mutation });
                if (failure === "after")
                  await shipped.run({ kind: "sql", sql: "SELECT CAST('bad' AS INTEGER)" });
              } catch {
                shippedFailed = true;
              }
              await shipped.run({ kind: "sql", sql: shippedFailed ? "ROLLBACK" : "COMMIT" });
              const actual = await result(async () => {
                for (const row of rows)
                  await f.write(kind === "rekey" ? "update" : kind, row.before, row.id, row.n);
                await f.statement.end(f.signal);
              });
              expect(actual).toBe(expected);
              expect(shippedFailed).toBe(expected !== null);
              await f.settle(actual);
              const native = await f.rows(),
                reference = (
                  await pg.query<{ id: number; n: number; payload: string }>(
                    "SELECT * FROM t ORDER BY id",
                  )
                ).rows.map((row) => [row.id, row.n, row.payload]);
              expect(native).toEqual(reference);
              reach.reach(kind === "rekey" ? "key swap" : kind);
              reach.reach(published ? "published sources" : "resident sources");
              reach.reach("no admission with image lease");
              reach.reach("no legacy deferred queues");
              if (expected) {
                reach.reach("whole statement rollback");
                reach.reach(
                  expected === "23502"
                    ? "NOT NULL"
                    : expected === "23514"
                      ? "CHECK"
                      : expected === "23505"
                        ? "command uniqueness"
                        : "later callback failure",
                );
              } else {
                expect(f.finished).toBe(1);
                reach.reach("root end once");
              }
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ["insert", "none", 3, false],
            ["update", "none", 3, true],
            ["delete", "none", 3, false],
            ["rekey", "none", 3, true],
            ["insert", "notNull", 3, false],
            ["update", "check", 3, true],
            ["update", "unique", 3, false],
            ["insert", "after", 3, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await shipped.close();
    }
  },
  120000,
);

it.each(seedsFor("next-native-primary-ownership", [0xb5ee]))(
  "refuses legacy and retired primary continuations seed %i",
  async (seed) => {
    const reach = createReach("native primary ownership", NATIVE_PRIMARY_OWNERSHIP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(
          "insert",
          "update",
          "delete",
          "conflicts",
          "DDL",
          "prepared end",
          "foreign command",
          "retired command",
          "forged preparation",
        ),
        async (mode) => {
          reach.beginHistory();
          const f = await fixture(2, false, false);
          try {
            const proposed = batch(ints([3]), ints([3]), texts(["changed"]));
            if (mode === "insert")
              await expect(
                Promise.resolve().then(() => f.retained.insert(proposed, null)),
              ).rejects.toThrow("require their primary capability");
            else if (mode === "update")
              await expect(
                Promise.resolve().then(() =>
                  f.retainedUpdate.update(batch(locators([0], f.id), ints([3])), null),
                ),
              ).rejects.toThrow("require their primary capability");
            else if (mode === "delete")
              await expect(
                Promise.resolve().then(() => f.retained.delete(batch(locators([0], f.id)), null)),
              ).rejects.toThrow("require their primary capability");
            else if (mode === "conflicts")
              await expect(
                Promise.resolve().then(() =>
                  f.retained.conflicts(proposed, [], {
                    kinds: new Uint8Array(1),
                    locators: new Float64Array(1),
                    earlier: new Int32Array(1),
                    indexIds: new Int32Array(1),
                  }),
                ),
              ).rejects.toThrow("require their primary capability");
            else if (mode === "DDL")
              await expect(
                Promise.resolve().then(() =>
                  f.statement.changeCatalog({
                    kind: "createTable",
                    table: tableInput("unchecked", [column("id", INT, false)]),
                  }),
                ),
              ).rejects.toThrow("require their primary capability");
            else if (mode === "foreign command")
              await expect(
                f.primary.prepare(
                  { ...f.command, rootId: f.command.rootId + 1 },
                  f.id,
                  "insert",
                  null,
                  [0, 0, 7],
                ),
              ).rejects.toThrow("native command work");
            else {
              const token = await f.primary.prepare(f.command, f.id, "insert", null, [0, 0, 7]);
              if (mode === "prepared end")
                await expect(f.statement.end(f.signal)).rejects.toThrow("unfinished row");
              else if (mode === "forged preparation")
                await expect(f.primary.apply({ ...token }, null, proposed)).rejects.toThrow(
                  "foreign or stale row",
                );
              else {
                await f.commands.verify(f.command, "unique", async () => undefined, f.signal);
                await f.commands.verify(f.command, "references", async () => undefined, f.signal);
                f.commands.retireCommand(f.command);
                await expect(f.primary.apply(token, null, proposed)).rejects.toThrow(
                  "native command work",
                );
              }
            }
            reach.reach(mode);
            await f.primary.close();
            f.statement.rollback();
            f.tx.rollback();
            expect(await f.rows()).toEqual([
              [1, 1, "seed"],
              [2, 2, "seed"],
            ]);
            reach.reach("no unchecked mutation");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          ["insert"],
          ["update"],
          ["delete"],
          ["conflicts"],
          ["DDL"],
          ["prepared end"],
          ["foreign command"],
          ["retired command"],
          ["forged preparation"],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-primary-lifetime", [0xb5ef]))(
  "fences prepaid primary completion before close refund seed %i",
  async (seed) => {
    const reach = createReach("native primary lifetime", NATIVE_PRIMARY_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (close) => {
        reach.beginHistory();
        const f = await fixture(2, false, false);
        let entered!: () => void, resume!: () => void;
        const ready = new Promise<void>((r) => {
            entered = r;
          }),
          gate = new Promise<void>((r) => {
            resume = r;
          });
        const native = WritePathImpl.prototype.reserveCommandRows.bind(f.path);
        vi.spyOn(f.path, "reserveCommandRows").mockImplementation(async (...args) => {
          const credit = await native(...args);
          entered();
          await gate;
          return credit;
        });
        let closing: Promise<void> | null = null;
        try {
          const pending = f.primary.prepare(f.command, f.id, "insert", null, [0, 0, 7]).then(
            () => null,
            (error: unknown) => error,
          );
          await Promise.race([
            ready,
            pending.then((error) => {
              throw new Error("Expected suspended native command", { cause: error });
            }),
          ]);
          const paid = f.root.allocated;
          let done = false;
          if (close) {
            closing = f.primary.close().then(() => {
              done = true;
            });
          } else {
            await f.commands.verify(f.command, "unique", async () => undefined, f.signal);
            await f.commands.verify(f.command, "references", async () => undefined, f.signal);
            f.commands.retireCommand(f.command);
          }
          await yieldTask();
          const early = done,
            stillPaid = f.root.allocated === paid;
          resume();
          const failure = await pending;
          await closing;
          expect(failure).toBeInstanceOf(Error);
          expect(early).toBe(false);
          expect(stillPaid).toBe(true);
          if (close) {
            expect(f.root.allocated).toBe(paid - NATIVE_PRIMARY_BYTES);
            reach.reach("close waits native reservation");
          } else {
            await f.primary.close();
            reach.reach("retirement during reservation");
          }
          f.statement.rollback();
          f.tx.rollback();
          expect(await f.rows()).toEqual([
            [1, 1, "seed"],
            [2, 2, "seed"],
          ]);
          reach.reach("no mutation after await");
          reach.reach("prepared credit retired");
        } finally {
          resume();
          await closing;
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 6, examples: [[false], [true]] },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-primary-admission", [0xb5f0]))(
  "cancels actual native primary growth while competing credit stays held seed %i",
  async (seed) => {
    const reach = createReach("native primary admission", NATIVE_PRIMARY_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4 * 1024 * 1024, max: 6 * 1024 * 1024 }),
        async (width) => {
          reach.beginHistory();
          const f = await fixture(2, false, false),
            governor = f.h.governor;
          const spare = governor.capBytes - governor.reserveBytes - governor.usedBytes,
            holder = await governor.admit({
              label: "competing admitted execution",
              minimumBytes: spare,
              spillable: false,
            });
          let entered!: () => void;
          const ready = new Promise<void>((r) => {
            entered = r;
          });
          const original = governor.admitGroup.bind(governor);
          const admission = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
            if (options.label.startsWith("grow write path")) entered();
            return original(options);
          });
          let closing: Promise<void> | null = null;
          try {
            const pending = f.primary.prepare(f.command, f.id, "insert", null, [0, 0, width]).then(
              () => null,
              (error: unknown) => error,
            );
            await Promise.race([
              ready,
              pending.then((error) => {
                throw new Error("Expected native row growth to wait", { cause: error });
              }),
            ]);
            reach.reach("native growth queued");
            const cancellation = { done: false };
            closing = f.primary.close().then(() => {
              cancellation.done = true;
            });
            // The observed boundary is governor admission, with no command I/O outstanding.
            // Drain rejection continuations, then capture completion before releasing anyone.
            for (let turn = 0; turn < 8; turn++) await Promise.resolve();
            await yieldTask();
            const finishedBeforeCompetitorRelease = cancellation.done;
            if (!finishedBeforeCompetitorRelease) holder.close(); // Fence a broken design before asserting.
            expect(await pending).toBeInstanceOf(Error);
            await closing;
            expect(
              finishedBeforeCompetitorRelease,
              "command remained blocked on independent maintenance",
            ).toBe(true);
            reach.reach("cancel completes at admission boundary");
            expect(holder.held).toBe(spare);
            reach.reach("competing credit retained");
            reach.reach("operation cancellation");
            f.statement.rollback();
            expect(await f.continueAfterRollback()).toBe(1);
            reach.reach("same transaction continues");
            expect(holder.held).toBe(spare);
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            admission.mockRestore();
            await closing;
            holder.close();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-primary-owner-close", [0xb5f1]))(
  "native owner waits command callbacks before rollback and release seed %i",
  async (seed) => {
    const reach = createReach("native primary owner close", NATIVE_PRIMARY_OWNER_CLOSE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (published) => {
        reach.beginHistory();
        let entered!: () => void, resume!: () => void;
        const ready = new Promise<void>((r) => {
            entered = r;
          }),
          gate = new Promise<void>((r) => {
            resume = r;
          });
        const f = await fixture(2, published, false, async () => {
          entered();
          await gate;
        });
        let closing: Promise<void> | null = null;
        try {
          await f.write("insert", null, 3, 3);
          const own = f.statement.readOwn(),
            paid = f.root.allocated,
            undo = f.tx.undo.length;
          let done = false;
          closing = f.path.close().then(() => {
            done = true;
          });
          await ready;
          await yieldTask();
          const early = done,
            retained = f.root.allocated === paid,
            stillOpen = f.tx.state === "open" && f.tx.undo.length === undo && !own.released;
          expect(() => f.statement.rollback()).toThrow("rollback before command fence");
          resume();
          await closing;
          expect(early).toBe(false);
          expect(retained).toBe(true);
          expect(stillOpen).toBe(true);
          expect(undo).toBeGreaterThan(0);
          expect(f.tx.state).toBe("rolledBack");
          expect(own.released).toBe(true);
          expect(f.root.allocated).toBe(paid - NATIVE_PRIMARY_BYTES);
          reach.reach(published ? "published source" : "resident source");
          reach.reach("pending callback fenced");
          reach.reach("rollback waits callback");
          reach.reach("prefix pin retired by rollback");
          reach.reach("native primary charge retained");
        } finally {
          resume();
          await closing;
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 6, examples: [[false], [true]] },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-primary-end-fence", [0xb5f3]))(
  "cancellation fences an in-flight native end callback seed %i",
  async (seed) => {
    const reach = createReach("native primary end fence", NATIVE_PRIMARY_END_FENCE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (published) => {
        reach.beginHistory();
        let entered!: () => void, resume!: () => void;
        const ready = new Promise<void>((r) => {
            entered = r;
          }),
          gate = new Promise<void>((r) => {
            resume = r;
          });
        const f = await fixture(2, published, false, undefined, async () => {
          entered();
          await gate;
        });
        let closing: Promise<void> | null = null;
        try {
          await f.write("insert", null, 3, 3);
          const pending = f.statement.end().then(
            () => null,
            (error: unknown) => error,
          );
          await Promise.race([
            ready,
            pending.then((error) => {
              throw new Error("Expected suspended native command", { cause: error });
            }),
          ]);
          const paid = f.root.allocated;
          let done = false;
          closing = f.primary.close().then(() => {
            done = true;
          });
          await yieldTask();
          const early = done,
            retained = f.root.allocated === paid;
          expect(() => f.statement.rollback()).toThrow("rollback before command fence");
          resume();
          const failure = await pending;
          await closing;
          expect(early).toBe(false);
          expect(retained).toBe(true);
          expect(failure).toBeInstanceOf(Error);
          expect(f.root.allocated).toBe(paid - NATIVE_PRIMARY_BYTES);
          f.statement.rollback();
          f.tx.rollback();
          expect(await f.rows()).toEqual([
            [1, 1, "seed"],
            [2, 2, "seed"],
          ]);
          reach.reach(published ? "published source" : "resident source");
          reach.reach("end callback fenced");
          reach.reach("cancelled end cannot succeed");
          reach.reach("rollback after fence");
          reach.reach("charge retained until callback");
        } finally {
          resume();
          await closing;
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 6, examples: [[false], [true]] },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-primary-close-fault", [0xb5f4]))(
  "native cleanup completes when the root close callback fails seed %i",
  async (seed) => {
    const reach = createReach("native primary close fault", NATIVE_PRIMARY_CLOSE_FAULT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (delayed) => {
        reach.beginHistory();
        const h = new Harness("strict", "memory", 256),
          path = await h.open();
        let id = 0;
        await autocommit(path, async (s) => {
          id =
            (
              await s.changeCatalog({
                kind: "createTable",
                table: tableInput("t", [column("id", INT, false)]),
              })
            ).id ?? 0;
        });
        const root = await h.governor.admit({
            label: "faulted native command root",
            minimumBytes: 16384,
            spillable: false,
          }),
          tx = await path.begin(),
          statement = tx.statement({
            rules: () => {
              throw new Error("No row rules needed");
            },
          }),
          fault = new Error("root controller cleanup failed");
        if (!(statement instanceof StatementImpl)) throw new Error("Expected native statement");
        statement.openNativeCommands({
          tables: [id],
          charge: root,
          signal: new AbortController().signal,
          commandBudget: () => 1,
          finishRoot: async () => undefined,
          closeRoot: async () => {
            if (delayed) await yieldTask();
            throw fault;
          },
        });
        const pin = statement.readOwn();
        try {
          const failure = await path.close().then(
            () => null,
            (error: unknown) => error,
          );
          expect(failure).toBeInstanceOf(AggregateError);
          expect((failure as AggregateError).errors).toEqual([fault]);
          expect(tx.state).toBe("rolledBack");
          expect(pin.released).toBe(true);
          expect(root.allocated).toBe(0);
          reach.reach(delayed ? "async callback fault" : "immediate callback fault");
          reach.reach("native rollback despite fault");
          reach.reach("metadata refunded after fence");
          reach.reach("original callback fault retained");
        } finally {
          pin.release();
          root.close();
          await h.close().catch((error: unknown) => {
            expect(error).toBeInstanceOf(AggregateError);
          });
        }
        expect(h.governor.usedBytes).toBe(0);
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 4, examples: [[false], [true]] },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-action-unique", [0xb602]))(
  "checks newly accepted native child work before each action resolves seed %i",
  async (seed) => {
    const reach = createReach("native action uniqueness", NATIVE_ACTION_UNIQUE_REACH),
      pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 3 }),
          fc.boolean(),
          fc.constantFrom("entry", "id"),
          async (actions, published, route) => {
            reach.beginHistory();
            const count = actions + 1,
              f = await fixture(count, published, false, undefined, undefined, actions);
            const key = (i: number) => (i === actions - 1 ? (actions === 1 ? count + 1 : 0) : -i);
            const hooks = {
              beforeRow: async () => {
                throw new Error("Unexpected outer BEFORE drain");
              },
              afterRow: async () => {
                throw new Error("Unexpected outer AFTER drain");
              },
            };
            let after = 0,
              called = 0;
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS t; CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER NOT NULL,payload TEXT)",
              );
              await pg.exec(
                `INSERT INTO t VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)},'seed')`).join(",")}`,
              );
              await pg.exec("BEGIN");
              const expected = await result(async () => {
                await pg.exec(
                  `UPDATE t SET id=${String(count + 1)},n=${String(count + 1)},payload='changed' WHERE id=${String(count)}`,
                );
                for (let i = 0; i < actions; i++)
                  await pg.exec(
                    `UPDATE t SET id=${String(key(i))},payload='changed' WHERE id=${String(i + 1)}`,
                  );
                await pg.exec("SELECT 100/0");
              });
              await pg.exec("ROLLBACK");
              expect(expected).toBe("23505");
              const parent = await f.write("update", count - 1, count + 1, count + 1),
                ids: number[] = [];
              for (let i = 0; i < actions; i++)
                ids.push(
                  await f.commands.addReferential(f.command, parent.images, {
                    fkId: i + 1,
                    event: "parentUpdate",
                    kind: "setDefault",
                    targets: null,
                  }),
                );
              // The callback performs real native child writes. Descriptor discovery/FK lookup
              // is deliberately synthetic here; this is not a SQL FK scheduling claim.
              f.setReferentialWork(async (entry, _hooks, stop) => {
                stop.throwIfAborted();
                called++;
                const i = entry.fkId - 1;
                await f.write("update", i, key(i), i + 1);
              });
              const actual = await result(async () => {
                await f.execution.verifyCommandUnique(f.command, f.signal);
                for (const id of ids) {
                  if (route === "id") await f.execution.executeReferentialId(id, hooks, f.signal);
                  else
                    await f.execution.executeReferential(
                      await f.commands.resolveReferential(id),
                      hooks,
                      f.signal,
                    );
                }
                after++;
                throw new DivisionByZeroError();
              });
              expect(actual).toBe(expected);
              expect(called).toBe(actions);
              expect(after).toBe(0);
              await f.settle(actual);
              expect(await f.rows()).toEqual(
                Array.from({ length: count }, (_, i) => [i + 1, i + 1, "seed"]),
              );
              reach.reach(route);
              reach.reach(published ? "published" : "resident");
              reach.reach(
                actions === 1 ? "one action" : actions === 2 ? "two actions" : "three actions",
              );
              reach.reach("shared command token");
              reach.reach("native23505 before later22012");
              reach.reach("atomic rollback");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [1, false, "entry"],
            [2, true, "id"],
            [3, false, "id"],
            [3, true, "entry"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120000,
);

it.each(seedsFor("next-native-boundary-reference", [0xb606]))(
  "verifies native command child references before retiring the command seed %i",
  async (seed) => {
    const reach = createReach("native boundary references", NATIVE_BOUNDARY_REFERENCE_REACH),
      pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 2, max: 6 }),
          async (published, missing, count) => {
            reach.beginHistory();
            const f = await fixture(count, published, false, undefined, undefined, 0, true);
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS t; DROP TABLE IF EXISTS p; CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE t(id INTEGER PRIMARY KEY,n INTEGER NOT NULL REFERENCES p(id),payload TEXT)",
              );
              await pg.exec(
                `INSERT INTO p VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`).join(",")}`,
              );
              await pg.exec(
                `INSERT INTO t VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)},'seed')`).join(",")}`,
              );
              const parent = missing ? count + 100 : count;
              const expected = await result(() =>
                pg.exec(`INSERT INTO t VALUES(${String(count + 1)},${String(parent)},'changed')`),
              );
              const actual = await result(async () => {
                await f.write("insert", null, count + 1, parent);
                await f.statement.end(f.signal);
              });
              expect(actual).toBe(expected);
              expect(actual).toBe(missing ? "23503" : null);
              await f.settle(actual);
              const rows = (
                await pg.query<{ id: number; n: number; payload: string }>(
                  "SELECT * FROM t ORDER BY id",
                )
              ).rows;
              expect(await f.rows()).toEqual(rows.map((row) => [row.id, row.n, row.payload]));
              reach.reach(published ? "published" : "resident");
              reach.reach(missing ? "native missing parent23503" : "native parent exists");
              reach.reach("command end checkpoint");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [false, true, 2],
            [true, false, 6],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120000,
);

for (const seed of seedsFor("next-native-primary-fts", [0xb61b, 0xb61c]))
  it(`preserves FTS history through accepted native command primaries ${String(seed)}`, async () => {
    const reach = createReach("native primary FTS", NATIVE_PRIMARY_FTS_REACH),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    const reference = async (sql: string) => {
      await pg.exec(sql);
      await shipped.run({ kind: "sql", sql });
    };
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("insert", "update", "delete"),
          fc.constantFrom("commit", "rollback", "savepoint"),
          fc.integer({ min: 1, max: 9 }),
          fc.boolean(),
          async (event, mode, count, published) => {
            reach.beginHistory();
            const f = await fixture(count, published, false, undefined, undefined, 0, false, true);
            const notes: string[] = [];
            const nativeNote = f.statement.noteFtsMutation.bind(f.statement);
            const spy = vi
              .spyOn(f.statement, "noteFtsMutation")
              .mockImplementation((table, kind) => {
                expect(table).toBe(f.id);
                notes.push(kind);
                nativeNote(table, kind);
              });
            try {
              await reference("DROP TABLE IF EXISTS t");
              await reference(
                "CREATE TABLE t(id INT PRIMARY KEY,n INT NOT NULL,payload TEXT,CONSTRAINT positive CHECK(n>0))",
              );
              await reference(
                `INSERT INTO t VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)},'seed')`).join(",")}`,
              );
              await reference("BEGIN");
              await reference("SAVEPOINT s");
              const before = structuredClone(f.path.state().root.fts);
              const last = f.tx.baseLsn;
              const key = event === "insert" ? count + 1 : 1;
              await f.write(event, event === "insert" ? null : 0, key, count + 100, "after");
              expect(notes, "accepted primary notifies shared FTS history").toEqual(
                event === "insert" ? [] : [event],
              );
              expect(
                [...ftsMutationScan(f.tx)].filter((e) => e !== null).map((e) => e.tableId),
                "native primary mutation participates in ordinary undo scan",
              ).toEqual(event === "insert" ? [] : [f.id]);
              await reference(
                event === "delete"
                  ? "DELETE FROM t WHERE id=1"
                  : event === "update"
                    ? `UPDATE t SET n=${String(count + 100)},payload='after' WHERE id=1`
                    : `INSERT INTO t VALUES(${String(key)},${String(count + 100)},'after')`,
              );
              await f.statement.end(f.signal);
              await f.settle(
                mode === "rollback" ? "requested rollback" : null,
                mode === "savepoint",
              );
              if (mode === "rollback") await reference("ROLLBACK");
              else {
                if (mode === "savepoint") await reference("ROLLBACK TO SAVEPOINT s");
                await reference("COMMIT");
              }
              if (mode === "commit") {
                const logged = await lastFtsCommit(f.h);
                expect(logged.lastLsn).toBeGreaterThan(last);
                expect(
                  logged.body.effects.filter((e) => e.kind === "ftsMutation" && e.tableId === f.id),
                ).toHaveLength(event === "insert" ? 0 : 1);
                if (event !== "insert")
                  expect(
                    (await ftsParts(f.h)).entries.some(
                      (e) => e.kind === "keyed" && e.tableId === f.id,
                    ),
                  ).toBe(true);
              } else {
                const pin = f.path.pin();
                try {
                  expect(pin.observedLsn).toBe(last);
                } finally {
                  pin.release();
                }
                expect(f.path.state().root.fts).toEqual(before);
              }
              const compare = async () => {
                const pin = f.h.path.pin();
                try {
                  const rows = (await rowsOf(pin, f.id)).sort(
                      (a, b) => Number(a[0]) - Number(b[0]),
                    ),
                    sql = "SELECT * FROM t ORDER BY id";
                  const expected = (await pg.query<{ id: number; n: number; payload: string }>(sql))
                    .rows;
                  expect(rows).toEqual(expected.map((row) => [row.id, row.n, row.payload]));
                  expect((await shipped.run({ kind: "sql", sql })).rows).toEqual(expected);
                } finally {
                  pin.release();
                }
              };
              await compare();
              await f.path.recycleLog();
              await f.h.reopen();
              await compare();
              reach.reach(event);
              reach.reach(mode);
              reach.reach(published ? "published" : "resident");
              reach.reach("undo and WAL");
              reach.reach("reopen");
            } finally {
              spy.mockRestore();
              await Promise.allSettled([
                pg.exec("ROLLBACK"),
                shipped.run({ kind: "sql", sql: "ROLLBACK" }),
              ]);
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 20,
          examples: [
            ["insert", "commit", 1, false],
            ["update", "commit", 3, true],
            ["delete", "commit", 4, false],
            ["insert", "rollback", 5, true],
            ["update", "rollback", 6, false],
            ["delete", "rollback", 7, true],
            ["insert", "savepoint", 3, false],
            ["update", "savepoint", 4, true],
            ["delete", "savepoint", 5, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await shipped.close();
      await pg.close();
    }
  }, 120000);

for (const seed of seedsFor("next-native-command-undo", [0xb61f, 0xb620]))
  it(`fences funded native append ranges by command identity ${String(seed)}`, async () => {
    const reach = createReach("native command undo", NATIVE_COMMAND_UNDO_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 6 }),
        fc.integer({ min: 1, max: 3 }),
        fc.constantFrom("commit", "rollback", "savepoint"),
        async (rounds, rows, mode) => {
          reach.beginHistory();
          const f = await fixture(1, false, false, undefined, undefined, 0, false, false, 3),
            runs: Array<{ command: number; count: number }> = [],
            expected: Array<readonly unknown[]> = [[1, 1, "seed"]];
          let reservations = 0,
            key = 100;
          const reserve = f.tx.reserveFtsRowUndo.bind(f.tx),
            release = f.tx.releaseFtsRowUndo.bind(f.tx),
            r = vi.spyOn(f.tx, "reserveFtsRowUndo").mockImplementation(() => {
              reserve();
              reservations++;
            }),
            d = vi.spyOn(f.tx, "releaseFtsRowUndo").mockImplementation(() => {
              release();
              reservations--;
            });
          const append = async (owner: CommandToken, n: number) => {
            for (let i = 0; i < n; i++) {
              await f.write("insert", null, key, 2, "command", owner);
              expected.push([key++, 2, "command"]);
              const last = runs.at(-1);
              if (last?.command === owner.id) last.count++;
              else runs.push({ command: owner.id, count: 1 });
              const actual = f.tx.undo.filter((e) => e.kind === "rows");
              expect(
                actual.map((e) => e.count),
                "one append undo range belongs to one native command",
              ).toEqual(runs.map((run) => run.count));
              expect(reservations, "one FTS reservation per retained command range").toBe(
                actual.length,
              );
              expect(actual.every((e) => e.ftsKeyed === false)).toBe(true);
            }
          };
          const finish = async (owner: CommandToken) => {
            await f.execution.verifyCommandUnique(owner, f.signal);
            await f.execution.verifyCommandReferences(owner, f.signal);
          };
          try {
            for (let round = 0; round < rounds; round++) {
              await append(f.command, 1);
              const body = f.commands.beginCommand(2, 0);
              await append(body, rows);
              const nested = f.commands.beginCommand(3, -1);
              await append(nested, rows);
              await finish(nested);
              await append(body, rows);
              await finish(body);
              const again = f.commands.beginCommand(2, 0);
              await append(again, rows);
              await finish(again);
              await append(f.command, 1);
            }
            await f.statement.end(f.signal);
            await f.settle(mode === "rollback" ? "requested rollback" : null, mode === "savepoint");
            expect(await f.rows()).toEqual(mode === "commit" ? expected : [[1, 1, "seed"]]);
            // Commit drops undo and refunds its aggregate slot budget directly;
            // rollback visits releaseFtsRowUndo per entry. The live-range assertions
            // above establish the aggregate's cardinality; owner close checks its ledger.
            if (mode !== "commit")
              expect(reservations, "rollback retires every command slot").toBe(0);
            expect(f.tx.undo).toHaveLength(0);
            reach.reach(mode);
            reach.reach("root/body/nested");
            reach.reach("reused frame slots");
            reach.reach("returned parent command");
            reach.reach("sequential bodies reuse frame");
            reach.reach("ordinary insert flags");
            if (rows > 1) reach.reach("coalesced same-command rows");
          } finally {
            r.mockRestore();
            d.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [2, 2, "commit"],
          [3, 3, "rollback"],
          [4, 1, "savepoint"],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
