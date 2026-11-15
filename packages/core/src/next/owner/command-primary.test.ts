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
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { currentEngine } from "../../testing/differential-current.js";
import { compileTableRules } from "../exec/rules.js";
import type { BoundTableRules } from "../exec/row-rules.js";
import { createLowerer } from "../lower/create.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { NativeCommandChecks } from "./command-checks.js";
import { RegistryCommands } from "./registry-commands.js";
import { RegistryDirectory } from "./registry-directory.js";
import { RegistryFragments } from "./registry-fragments.js";
import { RegistryImages } from "./registry-images.js";
import type { AcceptedCommandRow } from "./registry-commands.js";
import { WritePathImpl } from "../write/engine.js";
import { yieldTask } from "../task.js";
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
) {
  const h = new Harness("strict", "memory", 256),
    path = await h.open();
  let id = 0;
  await autocommit(path, async (s) => {
    id =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput(
            "t",
            [column("id", INT, false), column("n", INT, false), column("payload", TEXT)],
            { checks: [{ name: "positive", columns: [1], conditionSql: "n>0" }] },
          ),
        })
      ).id ?? 0;
  });
  const seed = Array.from({ length: count }, (_, i) => i + 1);
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
  const tx = await path.begin();
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
    signal = new AbortController().signal;
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
      frames: 1,
      rowEvents: 0,
      directory,
      charge: root,
      signal,
      checkpoint: () => undefined,
      referentialName: () => {
        throw new Error("No FK in primary fixture");
      },
    }),
    command = commands.beginCommand(1, 1);
  let leases = 0,
    finished = 0,
    closed = false;
  const checks = new NativeCommandChecks({
    statement,
    tables: [id],
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
      await commands.verify(command, "unique", (row, s) => checks.verifyUnique(row, s), stop);
      if (after) throw Object.assign(new Error("later AFTER callback"), { sqlState: "22P02" });
      await commands.verify(command, "references", async () => undefined, stop);
      commands.retireCommand(command);
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
    ) {
      const old =
          before === null ? null : batch(ints([before + 1]), ints([before + 1]), texts(["seed"])),
        next = event === "delete" ? null : batch(ints([key]), ints([n]), texts(["changed"]));
      const ref = await images.append(
          id,
          old ? { batch: old, row: 0 } : null,
          next ? { batch: next, row: 0 } : null,
        ),
        token = await primary.prepare(
          command,
          id,
          event,
          before === null ? null : firstNative + before,
          [0, 0, next ? 7 : 0],
        );
      const lease = await images.readRowImages(ref, signal);
      leases++;
      let newRowId: number | null;
      try {
        newRowId = await primary.apply(token, lease.old, lease.new);
      } finally {
        lease.release();
        leases--;
      }
      const accepted: AcceptedCommandRow = {
        command,
        tableId: id,
        event,
        images: ref,
        oldRowId: before === null ? null : firstNative + before,
        newRowId,
      };
      await commands.accept(accepted);
    },
    async settle(error: string | null) {
      await dispose();
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
