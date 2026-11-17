/** Only reachable live trigger bodies are prepared. Native signed depth is shipped's
 * documented budget, not PostgreSQL's unbounded trigger recursion. Explicit FK actions
 * propagate their actual mutation kinds; generated rekeys and informational follow count. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_COMMAND_REACH,
  NATIVE_COMMAND_FK_REACH,
  NATIVE_COMMAND_REACH_LIFETIME,
} from "../../testing/reach.js";
import { NativeCommandReach, nativeCommandReachBytes } from "../owner/command-reach.js";
import { StorageCore } from "./storage-core.js";
import { commandRootFixture } from "./command-root.test-support.js";
function sqlState(error: unknown): string {
  // Shipped exposes this depth error without SQLSTATE; compare the documented category.
  if (
    error instanceof Error &&
    error.message.startsWith("Trigger cascade depth exceeded at table:")
  )
    return "54001";
  if (typeof error === "object" && error !== null) {
    if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
    if ("code" in error && typeof error.code === "string") return error.code;
  }
  throw error;
}
for (const seed of seedsFor("next-native-command-reach", [0xb67f, 0xb680]))
  it(`prepares reachable signed frames and real bodies seed ${String(seed)}`, async () => {
    const reach = createReach("native prepared reach", NATIVE_COMMAND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 4 }),
        fc.integer({ min: 0, max: 3 }),
        fc.boolean(),
        async (depth, count, published) => {
          reach.beginHistory();
          const names = ["𐀀", "\uE000", "terminal", "unreachable"],
            setup = [
              "CREATE SEQUENCE s",
              "CREATE TABLE unused(id INT)",
              ...Array.from({ length: 5 }, (_, i) => `CREATE TABLE t${String(i)}(id INT)`),
              "CREATE TRIGGER dead AFTER INSERT ON unused BEGIN INSERT INTO t4 VALUES(nextval('s')); END",
            ];
          for (let i = 0; i < depth; i++)
            setup.push(
              `CREATE TRIGGER "${names[i] ?? "extra"}" AFTER INSERT ON t${String(i)} BEGIN INSERT INTO t${String(i + 1)} VALUES(${i < 2 ? "NEW.id" : "nextval('s')"}); END`,
            );
          const sql = count
              ? `INSERT INTO t0 VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`).join(",")}`
              : "INSERT INTO t0 SELECT id FROM t0 WHERE false",
            f = await commandRootFixture(setup, sql, published, 64 << 20, true),
            shipped = await currentEngine("memory");
          try {
            const native = f.nativeReach;
            if (!native) throw new Error("missing prepared reach");
            expect(native.frames).toBe(Math.min(3, depth + 1));
            expect(native.tables.map((id) => f.statement.catalog.table(id)?.name)).toEqual(
              Array.from({ length: Math.min(2, depth) + 1 }, (_, i) => `t${String(i)}`),
            );
            expect(native.triggers.map((t) => t.name)).toEqual(
              depth > 1 ? ["\uE000", "𐀀"] : depth === 1 ? ["𐀀"] : [],
            );
            expect(native.rowEvents).toBe(depth ? 1 : 0);
            for (const q of setup) await shipped.run({ kind: "sql", sql: q });
            let expected: string | null = null,
              actual: string | null = null;
            try {
              await shipped.run({ kind: "sql", sql });
            } catch (error) {
              expected = sqlState(error);
            }
            try {
              expect(await f.root.run({ slots: [] })).toBe(count);
              await f.root.end();
            } catch (error) {
              actual = sqlState(error);
            }
            expect(actual).toBe(expected);
            expect(actual).toBe(depth >= 3 && count > 0 ? "54001" : null);
            if (actual) await f.retire();
            else {
              const receipt = await f.tx.commit(null);
              await receipt.durable;
              await f.retire();
            }
            for (let i = 0; i < 5; i++) {
              const query = `SELECT CAST(count(*) AS INT) AS n FROM t${String(i)}`,
                old = await shipped.run({ kind: "sql", sql: query }),
                result = await f.core.execute(query);
              if (!("batches" in result)) throw new Error("rows");
              const { valueAt } = await import("../exec/kernels/test-support.js");
              expect(
                result.batches.flatMap((b) =>
                  Array.from({ length: b.length }, (_, r) => b.columns.map((v) => valueAt(v, r))),
                ),
              ).toEqual(old.rows?.map((r) => [r.n]));
            }
            reach.reach(depth === 0 ? "frames1" : depth === 1 ? "frames2" : "frames3");
            reach.reach(published ? "published" : "resident");
            if (!count) reach.reach("zero rows");
            if (depth >= 3 && count) reach.reach("terminal catalog error");
            reach.reach("unreachable volatile body excluded");
            if (depth > 1) reach.reach("UTF8 order");
            reach.reach("shipped data/error");
          } finally {
            await f.close();
            await shipped.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 20,
        examples: [
          [0, 1, false],
          [1, 1, true],
          [2, 3, false],
          [3, 1, true],
          [4, 0, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
for (const seed of seedsFor("next-native-command-fk-reach", [0xb681, 0xb682]))
  it(`follows exact FK mutation routes in charged catalog space seed ${String(seed)}`, async () => {
    const reach = createReach("native FK static reach", NATIVE_COMMAND_FK_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("follow", "cascade", "setNull", "setDefault", "restrict", "noAction"),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (mode, remove, key, generated, informational) => {
          reach.beginHistory();
          const info = informational && mode === "follow";
          const core = new StorageCore(256);
          await core.open();
          const stop = new AbortController();
          let prepared: NativeCommandReach | null = null,
            held = 0;
          try {
            const action =
              mode === "follow"
                ? ""
                : ` ON UPDATE ${mode === "setNull" ? "SET NULL" : mode === "setDefault" ? "SET DEFAULT" : mode === "noAction" ? "NO ACTION" : mode.toUpperCase()}`;
            const onDelete =
              mode === "follow"
                ? "CASCADE"
                : mode === "setNull"
                  ? "SET NULL"
                  : mode === "setDefault"
                    ? "SET DEFAULT"
                    : mode === "noAction"
                      ? "NO ACTION"
                      : mode.toUpperCase();
            await core.execute(
              `CREATE TABLE p(id INT PRIMARY KEY,v INT${generated ? ",g INT GENERATED ALWAYS AS(v+1) STORED UNIQUE" : ""})`,
            );
            await core.execute(
              `CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(${generated ? "g" : "id"})${info ? " NOT ENFORCED" : ` ON DELETE ${onDelete}${action}`})`,
            );
            await core.execute("CREATE TABLE log(id INT)");
            await core.execute(
              "CREATE TRIGGER child_insert AFTER INSERT ON c BEGIN INSERT INTO log VALUES(NEW.id); END",
            );
            await core.execute(
              "CREATE TRIGGER child_update AFTER UPDATE ON c BEGIN INSERT INTO log VALUES(NEW.id); END",
            );
            await core.execute(
              "CREATE TRIGGER child_delete AFTER DELETE ON c BEGIN INSERT INTO log VALUES(OLD.id); END",
            );
            const bodySql = remove
              ? "DELETE FROM p"
              : `UPDATE p SET ${key ? (generated ? "v" : "id") : generated ? "id" : "v"}=9`;
            // Stored assignments omit generated columns; lowered root assignments already
            // include them. Exercise the independent catalog/body reach path here.
            if (generated) {
              await core.execute("CREATE TABLE q(id INT PRIMARY KEY)");
              await core.execute(
                `CREATE TRIGGER root_enter AFTER INSERT ON q BEGIN ${bodySql}; END`,
              );
            }
            const catalog = core.harness.path.catalog,
              sql = generated ? "INSERT INTO q VALUES(1)" : bodySql,
              lowered = core.lowerer.lower(sql, { ...core.context(), catalog });
            if (lowered.kind !== "modify" || lowered.plan.root.op !== "modify")
              throw new Error("modify");
            const minimum = nativeCommandReachBytes(catalog),
              charge = {
                allocate: (n: number) => {
                  if (held + n > minimum) return false;
                  held += n;
                  return true;
                },
                free: (n: number) => {
                  held -= n;
                },
              };
            prepared = await NativeCommandReach.prepare(catalog, lowered.plan.root, charge, {
              signal: stop.signal,
            });
            expect(held).toBe(minimum);
            const moves = remove ? !info : key || generated,
              mutates = moves && ["follow", "cascade", "setNull", "setDefault"].includes(mode),
              event = remove && (mode === "cascade" || mode === "follow") ? "delete" : "update";
            expect(prepared.triggers.map((t) => t.name)).toEqual([
              ...(mutates ? [`child_${event}`] : []),
              ...(generated ? ["root_enter"] : []),
            ]);
            expect(prepared.tables.map((id) => catalog.table(id)?.name)).toEqual([
              ...(mutates ? ["p", "c", "log"] : ["p"]),
              ...(generated ? ["q"] : []),
            ]);
            expect(prepared.frames).toBe((mutates ? 2 : 1) + (generated ? 1 : 0));
            // Child UPDATE conservatively reserves its outgoing reference descriptor as well.
            expect(prepared.rowEvents).toBe(
              Math.max(
                generated ? 1 : 0,
                mutates && event === "update" && !info ? 2 : moves ? 1 : 0,
              ),
            );
            reach.reach(mode);
            reach.reach(remove ? "DELETE" : "UPDATE");
            if (generated && !remove) reach.reach("body generated parent key");
            if (info && !remove) reach.reach("informational follow");
            if (!remove && !generated && !key) reach.reach("non-key update pruned");
            reach.reach("charged fixed metadata");
          } finally {
            prepared?.close();
            expect(held).toBe(0);
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 48,
        examples: [
          ["follow", false, true, false, true],
          ["setDefault", true, true, false, false],
          ["cascade", false, true, true, false],
          ["restrict", false, false, false, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-native-command-reach-lifetime", [0xb683, 0xb684]))
  it(`retains prepaid preparation until its cancelled task unwinds seed ${String(seed)}`, async () => {
    const { TaskClock } = await import("../task.js");
    const reach = createReach("native preparation lifetime", NATIVE_COMMAND_REACH_LIFETIME);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (cancel) => {
        reach.beginHistory();
        const core = new StorageCore(256);
        await core.open();
        let held = 0;
        let result: NativeCommandReach | null = null;
        try {
          await core.execute("CREATE TABLE t(id INT PRIMARY KEY)");
          const catalog = core.harness.path.catalog,
            lowered = core.lowerer.lower("INSERT INTO t VALUES(1)", { ...core.context(), catalog });
          if (lowered.kind !== "modify" || lowered.plan.root.op !== "modify")
            throw new Error("modify");
          const minimum = nativeCommandReachBytes(catalog),
            stop = new AbortController(),
            charge = {
              allocate: (n: number) => {
                if (held + n > minimum) return false;
                held += n;
                return true;
              },
              free: (n: number) => {
                held -= n;
                expect(held).toBeGreaterThanOrEqual(0);
              },
            };
          let now = 0,
            enter: () => void = () => undefined,
            resume: () => void = () => undefined,
            done = false;
          const clock = new TaskClock(() => now),
            entered = new Promise<void>((r) => {
              enter = r;
            }),
            gate = new Promise<void>((r) => {
              resume = r;
            });
          now = 100;
          const pending = NativeCommandReach.prepare(catalog, lowered.plan.root, charge, {
            signal: stop.signal,
            clock,
            yieldTask: () => {
              enter();
              return gate;
            },
          }).finally(() => {
            done = true;
          });
          void pending.catch(() => undefined);
          await entered;
          await new Promise<void>((r) => setTimeout(r, 0));
          if (cancel) stop.abort(new Error("cancel prepared reach"));
          expect(done).toBe(false);
          expect(held).toBe(minimum);
          reach.reach("pause retains prepaid metadata");
          reach.reach(cancel ? "cancel" : "complete");
          resume();
          let failure: unknown;
          try {
            result = await pending;
          } catch (error) {
            failure = error;
          }
          if (cancel) expect(failure).toBeInstanceOf(Error);
          else {
            expect(failure).toBeUndefined();
            expect(result?.tables.length).toBe(1);
            result?.close();
            expect(() => result?.tables).toThrow(TypeError);
          }
        } finally {
          result?.close();
          expect(held).toBe(0);
          await core.close();
          expect(core.harness.governor.usedBytes).toBe(0);
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 8, examples: [[true], [false]] },
    );
    reach.assertReached();
  }, 120000);
