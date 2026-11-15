/** Component contract coverage. SQL provider integration remains separately gated. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  TRIGGER_CONTROLLER_REACH,
  TRIGGER_PREPARATION_REACH,
  TRIGGER_RI_UNIQUE_REACH,
} from "../../testing/reach.js";
import { fixture, quote } from "./trigger-controller.test-support.js";
for (const seed of seedsFor("next-trigger-controller", [0xb7da, 0xb7db]))
  it(`matches PG named RI and nested body ordering seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("named trigger controller", TRIGGER_CONTROLLER_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 17 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (count, update, unicode, twoBodies) => {
            reach.beginHistory();
            const event = update ? "update" : "delete",
              f = await fixture(event, unicode, twoBodies);
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS c,p,log,audit CASCADE; DROP SEQUENCE IF EXISTS event_seq; CREATE SEQUENCE event_seq; CREATE TABLE p(id int PRIMARY KEY); CREATE TABLE c(id int PRIMARY KEY,pid int REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE); CREATE TABLE log(label text,n int,ord bigint DEFAULT nextval('event_seq')); CREATE TABLE audit(label text,n int,ord bigint DEFAULT nextval('event_seq'));",
              );
              for (let i = 0; i < f.specs.length; i++) {
                const spec = f.specs[i];
                if (!spec) throw new Error("missing PG spec");
                const source = spec.table === "log" || spec.table === "audit" ? "NEW.n" : "OLD.id",
                  target = spec.table === "log" ? "audit" : "log";
                await pg.exec(
                  `CREATE OR REPLACE FUNCTION f${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO ${target}(label,n) VALUES ('${spec.label}',${source});${spec.double ? ` INSERT INTO audit(label,n) VALUES ('tail',${source});` : ""} RETURN ${spec.event === "delete" ? "OLD" : "NEW"}; END $$; CREATE TRIGGER ${quote(spec.name)} ${spec.timing} ${spec.event} ON ${spec.table} FOR EACH ROW EXECUTE FUNCTION f${String(i)}();`,
                );
              }
              await pg.exec(
                `INSERT INTO p SELECT n FROM generate_series(1,${String(count)})n; INSERT INTO c SELECT n+1000,n FROM generate_series(1,${String(count)})n; ${update ? "UPDATE p SET id=id+100" : "DELETE FROM p"};`,
              );
              for (let row = 1; row <= count; row++) await f.row(row);
              if (count * (unicode ? 5 : 3) >= 64) {
                expect(f.spill.bytesInUse).toBeGreaterThan(0);
                reach.reach("observed queue spill");
              }
              await f.runtime.finishRoot();
              const expected = (
                await pg.query<{ s: string }>(
                  "SELECT label||':'||n::text s FROM (SELECT * FROM log UNION ALL SELECT * FROM audit)q ORDER BY ord",
                )
              ).rows.map((r) => r.s);
              expect(f.trace, "PGlite name/frame order").toEqual(expected);
              expect(f.maxFrames()).toBe(3);
              expect(f.checkpoints[0]?.startsWith("unique:")).toBe(true);
              expect(f.checkpoints.at(-1)).toBe("refs:1");
              reach.reach(update ? "parent update and RI child" : "parent delete and RI child");
              const completedTrace = [...f.trace];
              await expect(f.row(999)).rejects.toMatchObject({ code: "MINNOW_TRIGGER_STATE" });
              expect(f.trace).toEqual(completedTrace);
              reach.reach("completed command refuses more primary rows");
              reach.reach("PG exact ordering");
              reach.reach("nested body before next statement");
              reach.reach("three live frames");
              reach.reach("competing grants");
              if (unicode) reach.reach("UTF8 names across surrogate boundary");
              if (count > 1) reach.reach("old parent queue before RI child AFTER");
            } finally {
              await f.close();
            }
            reach.reach("zero queues inputs pins and credit");
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            [17, false, true, true],
            [2, true, false, true],
            [1, false, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);

it("checks uniqueness before root AFTER and fences body failure/close", async () => {
  for (const fault of ["unique", "body", "pending close"] as const) {
    const f = await fixture("delete", false, false, fault);
    try {
      if (fault === "unique") {
        await f.row(1);
        const before = [...f.trace];
        await expect(f.runtime.finishRoot()).rejects.toBe(f.reason);
        expect(f.trace, "unique failure precedes root AFTER").toEqual(before);
        expect(f.checkpoints.at(-1)).toBe("unique:1");
      } else if (fault === "body") {
        await expect(f.row(1)).rejects.toBe(f.reason);
        expect(f.trace).toEqual([]);
      } else {
        const entered = f.waitInsideBody(),
          pending = f.row(1);
        await entered;
        const closing = f.runtime.close();
        await expect(pending).rejects.toMatchObject({ code: "MINNOW_TRIGGER_CLOSED" });
        await closing;
        expect(f.trace).toEqual([]);
      }
    } finally {
      await f.close();
    }
  }
}, 120_000);

it("refuses foreign commands and a third matching derived trigger before effects", async () => {
  for (const variant of ["origin", "budget", "depth"] as const) {
    const f = await fixture("delete", false, false, variant === "depth" ? "depth" : "none");
    try {
      if (variant === "depth") {
        await expect(f.row(1)).rejects.toMatchObject({
          code: "MINNOW_TRIGGER_DEPTH",
          sqlState: "54001",
        });
        expect(f.trace).toEqual(["pB:1", "echo:1"]);
        expect(f.maxFrames()).toBe(3);
      } else {
        const command = f.runtime.root;
        await expect(
          f.runtime.beforeRow(
            variant === "origin" ? { ...command, rootId: command.rootId + 1 } : command,
            1,
            "delete",
            { spoolId: 1, row: 1 },
            variant === "budget" ? 0 : 1,
          ),
        ).rejects.toMatchObject({ code: "MINNOW_TRIGGER_STATE", sqlState: "XX000" });
        expect(f.trace).toEqual([]);
      }
    } finally {
      await f.close();
    }
  }
}, 120_000);

for (const seed of seedsFor("next-trigger-preparation", [0xb7dc, 0xb7dd]))
  it(`refuses omitted live programs and detects unprepared terminal definitions seed ${String(seed)}`, async () => {
    const reach = createReach("trigger preparation completeness", TRIGGER_PREPARATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 5 }),
        fc.boolean(),
        fc.boolean(),
        async (omit, update, terminalBefore) => {
          reach.beginHistory();
          const f = await fixture(update ? "update" : "delete", false, false, "none", omit);
          try {
            const execute = (async () => {
              await f.row(1);
              await f.runtime.finishRoot();
            })();
            await expect(execute, "unprepared matching trigger must refuse").rejects.toMatchObject({
              code: "MINNOW_TRIGGER_STATE",
              sqlState: "XX000",
            });
            await expect(execute).rejects.toThrow("matching trigger was not prepared");
            reach.reach(
              omit === 0 || omit === 3 ? "BEFORE omission refused" : "AFTER omission refused",
            );
            if (omit === 3 || omit === 4) reach.reach("RI route omission refused");
            if (omit === 5) reach.reach("body route omission refused");
            reach.reach(update ? "update route" : "delete route");
          } finally {
            await f.close();
          }
          const terminal = await fixture(
            update ? "update" : "delete",
            false,
            false,
            "terminal not prepared",
            null,
            terminalBefore,
          );
          try {
            await expect(
              terminal.row(1),
              "unprepared terminal definition must still stop recursion",
            ).rejects.toMatchObject({ code: "MINNOW_TRIGGER_DEPTH", sqlState: "54001" });
            expect(terminal.trace).toEqual(terminalBefore ? ["pB:1"] : ["pB:1", "echo:1"]);
            expect(terminal.maxFrames()).toBe(3);
            reach.reach("terminal definition needs no compiled body");
            reach.reach(terminalBefore ? "terminal BEFORE" : "terminal AFTER");
          } finally {
            await terminal.close();
          }
          reach.reach("zero queues copies and readers");
        },
      ),
      {
        seed,
        numRuns: 10,
        examples: [
          [0, false, false],
          [1, true, true],
          [2, false, true],
          [3, true, false],
          [4, false, false],
          [5, true, true],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);

for (const seed of seedsFor("next-trigger-ri-unique-contract", [0xb7de, 0xb7df]))
  it(`checks each RI child write before later AFTER errors seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("incremental RI child uniqueness", TRIGGER_RI_UNIQUE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 3 }),
          fc.constantFrom("queue", "id", "entry"),
          async (count, route) => {
            reach.beginHistory();
            await pg.exec(`DROP TABLE IF EXISTS c,p CASCADE;
          CREATE TABLE p(id INT PRIMARY KEY);
          CREATE TABLE c(id INT PRIMARY KEY,pid INT DEFAULT 99 REFERENCES p(id) ON UPDATE SET DEFAULT,grp INT NOT NULL,UNIQUE(pid,grp));
          CREATE OR REPLACE FUNCTION later_after() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.id=${String(count)} THEN PERFORM 1/0; END IF; RETURN NEW; END $$;
          CREATE TRIGGER "zlast" AFTER UPDATE ON p FOR EACH ROW EXECUTE FUNCTION later_after();
          INSERT INTO p SELECT n FROM generate_series(1,${String(count)})n; INSERT INTO p VALUES(99);
          INSERT INTO c VALUES ${Array.from({ length: count }, (_, i) => `(${String(1001 + i)},${String(i + 1)},${String(i === count - 1 ? 1 : i + 1)})`).join(",")};
          ${count === 1 ? "INSERT INTO c VALUES(999,99,1);" : ""}`);
            let state: unknown = null;
            try {
              await pg.exec("UPDATE p SET id=id+10 WHERE id<99");
            } catch (error) {
              if (typeof error === "object" && error !== null && "code" in error)
                state = error.code;
              else throw error;
            }
            expect(state, "PG child uniqueness precedes later named body error").toBe("23505");
            const f = await fixture("update", false, false, "RI unique", null, false, count);
            try {
              const command = f.runtime.root;
              for (let row = 1; row <= count; row++) await f.row(row);
              const execute = async () => {
                if (route === "queue") await f.runtime.finishRoot();
                else {
                  await f.native.verifyCommandUnique(command, f.signal); // root passes at step zero
                  for (let row = 1; row <= count; row++) {
                    const entry = f.entries.get(row);
                    if (!entry) throw new Error("missing action");
                    if (route === "id")
                      await f.native.executeReferentialId(row, f.runtime, f.signal);
                    else await f.native.executeReferential(entry, f.runtime, f.signal);
                  }
                }
              };
              await expect(
                execute(),
                "RI child checkpoint must beat the competing AFTER error",
              ).rejects.toMatchObject({ sqlState: state });
              expect(
                f.rootUniqueSteps,
                "root and every new action checkpoint share one token",
              ).toEqual(Array.from({ length: count + 1 }, (_, i) => i));
              expect(f.riCommands).toEqual(Array.from({ length: count }, () => command));
              if (route !== "queue") {
                expect(
                  f.trace.some((s) => /^(p0|p1|cA):/.test(s)),
                  "RI method never drains outer AFTER",
                ).toBe(false);
                reach.reach("no outer AFTER drain");
              }
              reach.reach(route);
              reach.reach(count === 1 ? "first action conflict" : "successive actions one command");
              if (count === 3) reach.reach("two earlier action checkpoints pass");
              reach.reach("PG error priority");
            } finally {
              await f.close();
            }
            reach.reach("zero queues pins and credit");
          },
        ),
        {
          seed,
          numRuns: 13,
          examples: [
            [2, "queue"],
            [1, "queue"],
            [3, "queue"],
            [1, "id"],
            [2, "id"],
            [3, "id"],
            [1, "entry"],
            [2, "entry"],
            [3, "entry"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
