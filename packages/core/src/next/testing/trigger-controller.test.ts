/** PG sequencing authority around the real bounded controller/queue/binding/body components.
 * Native row/RI/source execution is an injected scalar fixture, not an enabled SQL provider. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  TRIGGER_CONTROLLER_REACH,
  TRIGGER_PREPARATION_REACH,
} from "../../testing/reach.js";
import type { TriggerDef } from "../access/trigger-def.js";
import type { CatalogView } from "../access/catalog.js";
import {
  ROW_IMAGE_UNREAD,
  type NativeCommandBoundary,
  type ReferentialEntry,
  type WriteEvent,
} from "../access/statement-command.js";
import {
  TriggerController,
  triggerControllerMinimumBytes,
  type PreparedRowTrigger,
} from "../exec/trigger-controller.js";
import { TriggerBodyRunner, type TriggerBodyHost } from "../exec/trigger-body.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ints } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { yieldTask } from "../task.js";

const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
const names = ["Afirst", "zlast", "\ue000", "\u{10000}"];
type Fault = "none" | "unique" | "body" | "pending close" | "depth" | "terminal not prepared";
async function fixture(
  event: "delete" | "update",
  unicode: boolean,
  twoBodies: boolean,
  fault: Fault = "none",
  omit: number | null = null,
  terminalBefore = false,
) {
  const core = await new StorageCore(16).open();
  for (const sql of [
    "CREATE TABLE p(id INT PRIMARY KEY)",
    "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
    "CREATE TABLE log(label TEXT,n INT)",
    "CREATE TABLE audit(label TEXT,n INT)",
  ])
    await core.execute(sql);
  const base = core.harness.path.catalog,
    programs: PreparedRowTrigger[] = [],
    definitions: TriggerDef[] = [],
    specs: Array<{
      name: string;
      table: string;
      timing: "before" | "after";
      event: WriteEvent;
      label: string;
      double: boolean;
    }> = [
      { name: "parentBefore", table: "p", timing: "before", event, label: "pB", double: twoBodies },
      ...names.slice(0, unicode ? 4 : 2).map((name, i) => ({
        name,
        table: "p",
        timing: "after" as const,
        event,
        label: `p${String(i)}`,
        double: twoBodies,
      })),
      { name: "childBefore", table: "c", timing: "before", event, label: "cB", double: false },
      { name: "childAfter", table: "c", timing: "after", event, label: "cA", double: twoBodies },
      {
        name: "echo",
        table: "log",
        timing: "after",
        event: "insert",
        label: "echo",
        double: false,
      },
    ];
  if (fault === "depth" || fault === "terminal not prepared")
    specs.push({
      name: "deep",
      table: "audit",
      timing: terminalBefore ? "before" : "after",
      event: "insert",
      label: "deep",
      double: false,
    });
  for (const spec of specs) {
    const source = spec.table === "log" || spec.table === "audit" ? "NEW.n" : "OLD.id",
      target = spec.table === "log" ? "audit" : "log",
      sql = `CREATE TRIGGER ${quote(spec.name)} ${spec.timing} ${spec.event} ON ${spec.table} BEGIN INSERT INTO ${target}(label,n) VALUES ('${spec.label}',${source});${spec.double ? ` INSERT INTO audit(label,n) VALUES ('tail',${source});` : ""} END`,
      declaration = core.lowerer.lower(sql, core.context());
    if (declaration.kind !== "ddl" || declaration.change?.kind !== "createTrigger")
      throw new Error("missing trigger definition");
    const definition = { ...declaration.change.trigger, id: definitions.length + 1 },
      owner = base.table(definition.tableId);
    if (!owner) throw new Error("missing owner");
    definitions.push(definition);
    // Native reach stops at spent budget. Its matching definition is retained in catalog,
    // but its unreachable body must not be lowered/prepared merely to report depth.
    if (omit === definition.id - 1 || (fault === "terminal not prepared" && spec.name === "deep"))
      continue;
    const bodies = definition.statements.map((body) => {
      const lowered = core.lowerer.lower(body.sql, {
        ...core.context(),
        parameterTypes: body.bindings.map((b) => b.type),
      });
      if (lowered.kind !== "modify") throw new Error("missing body modify");
      return new TriggerBodyRunner(owner, body, lowered.plan);
    });
    programs.push({ definition, bodies });
  }
  // UTF8 byte comparison is deliberately independent of the controller's scalar comparator.
  const encoder = new TextEncoder();
  programs.sort((a, b) => {
    const x = encoder.encode(a.definition.name),
      y = encoder.encode(b.definition.name);
    for (let i = 0; i < Math.min(x.length, y.length); i++)
      if (x[i] !== y[i]) return (x[i] ?? 0) - (y[i] ?? 0);
    return x.length - y.length || a.definition.id - b.definition.id;
  });
  const catalog: CatalogView = {
      version: base.version,
      tables: base.tables,
      sequences: base.sequences,
      views: base.views,
      types: base.types,
      fts: base.fts,
      triggers: definitions,
      table: (id) => base.table(id),
      tableByName: (name) => base.tableByName(name),
      type: (id) => base.type(id),
      typeByName: (name) => base.typeByName(name),
      sequence: (id) => base.sequence(id),
    },
    tx = await core.harness.path.begin(),
    statement = tx.statement({
      rules: () => {
        throw new Error("fixture does not write native rows");
      },
    }),
    governor = new Governor(8 * 1024 * 1024),
    minimum = triggerControllerMinimumBytes(3, 8),
    grant = await governor.admit({
      label: "controller root",
      minimumBytes: minimum + 256 * 1024,
      spillable: true,
    }),
    charge = new Charge(grant),
    pool = new Pool(charge, minimum + 256 * 1024),
    signal = new AbortController(),
    spill = new MemorySpillService(),
    files = new SpillFiles(spill, 1, charge),
    pages = new PageWriter(pool, 16384),
    io = new SpillIo(pool, files, pages, { grant, queryId: 1, signal: signal.signal }, () =>
      signal.signal.throwIfAborted(),
    ),
    reason = new Error(`injected ${fault}`),
    trace: string[] = [],
    checkpoints: string[] = [],
    budgets = new Map<number, number>(),
    inputs = new Map<number, { label: string; n: number; target: number }>(),
    entries = new Map<number, ReferentialEntry>();
  let serial = 0,
    nextInput = 0,
    maxFrames = 0,
    controller: TriggerController | null = null,
    insideBody: (() => void) | null = null;
  const p = base.tableByName("p"),
    c = base.tableByName("c"),
    log = base.tableByName("log"),
    audit = base.tableByName("audit");
  if (!p || !c || !log || !audit) throw new Error("missing fixture tables");
  const native: NativeCommandBoundary = {
    beginCommand(frameId, budget) {
      const command = { rootId: 9, id: ++serial, frameId };
      budgets.set(command.id, budget);
      return command;
    },
    readRowImages: async () => {
      throw new Error("full image helper not needed");
    },
    readRowBindings: async (ref, projection, abort) => {
      abort.throwIfAborted();
      const owner = base.table(ref.spoolId);
      if (!owner) throw new Error("missing image owner");
      const image = (ids: readonly number[]) =>
        owner.columns.map((col) => (ids.includes(col.id) ? ints([ref.row]) : ROW_IMAGE_UNREAD));
      return {
        tableId: owner.id,
        old: image(projection.old),
        new: image(projection.new),
        release: () => undefined,
      };
    },
    rowReferentialEntries: async function* () {
      /* entries already passed by this fixture */
    },
    verifyCommandUnique: async (command) => {
      checkpoints.push(`unique:${String(command.id)}`);
      if (fault === "unique" && command.frameId === 1) throw reason;
    },
    verifyCommandReferences: async (command) => {
      checkpoints.push(`refs:${String(command.id)}`);
    },
    executeReferential: async () => {
      throw new Error("queue must resolve by scalar ID");
    },
    executeReferentialId: async (id, hooks, abort) => {
      abort.throwIfAborted();
      const entry = entries.get(id);
      if (!entry) throw new Error("missing scalar RI entry");
      const command = { rootId: 9, id: entry.commandId, frameId: entry.frameId },
        images = { spoolId: c.id, row: entry.images.row + 1000 };
      await hooks.beforeRow(command, c.id, event, images, entry.triggerBudget);
      await hooks.afterRow(command, c.id, event, images, [], entry.triggerBudget);
    },
  };
  const host: TriggerBodyHost = {
    spoolBodyInput: async (request) => {
      const value = request.parameters.slots[0],
        label = /'([A-Za-z0-9]+)'/.exec(request.body.sql)?.[1];
      if (value?.form !== "flat" || value.data.layout !== "i32" || !label)
        throw new Error("missing typed fixture parameter/label");
      const id = ++nextInput;
      inputs.set(id, { label, n: value.data.values[0] ?? 0, target: request.body.targetTableId });
      return { rootId: request.command.rootId, id };
    },
    applyBodyInput: async (request) => {
      maxFrames = Math.max(maxFrames, controller?.retainedFrames ?? 0);
      if (!checkpoints.includes(`unique:${String(request.command.id)}`)) {
        // Source apply is before this command's unique check, by contract.
        const record = inputs.get(request.input.id);
        if (!record) throw new Error("retired input");
        insideBody?.();
        if (fault === "body") throw reason;
        if (fault === "pending close") {
          await new Promise<void>((resolve) =>
            request.signal.addEventListener(
              "abort",
              () => {
                void yieldTask().then(resolve);
              },
              { once: true },
            ),
          );
          request.signal.throwIfAborted();
        }
        if (record.target === log.id || record.target === audit.id) {
          const budget = budgets.get(request.command.id);
          if (budget === undefined) throw new Error("missing command budget");
          const image = { spoolId: record.target, row: record.n };
          await request.hooks.beforeRow(request.command, record.target, "insert", image, budget);
          trace.push(`${record.label}:${String(record.n)}`);
          await request.hooks.afterRow(request.command, record.target, "insert", image, [], budget);
        }
      } else throw new Error("body verified before primary");
    },
    releaseBodyInput: async (_command, input) => {
      inputs.delete(input.id);
    },
  };
  controller = await TriggerController.create({
    statement: { catalog, readOwn: () => statement.readOwn() },
    native,
    body: { host, charge: pool, work: { signal: signal.signal } },
    programs,
    io,
    spill,
    frameCount: 3,
    rowEvents: 8,
  });
  const runtime = controller,
    competing = await governor.admit({
      label: "all spare credit",
      minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
      spillable: false,
    });
  expect(competing.allocate(competing.held)).toBe(true);
  let closing: Promise<void> | null = null;
  return {
    runtime,
    trace,
    checkpoints,
    reason,
    spill,
    pool,
    grant,
    core,
    specs,
    maxFrames: () => maxFrames,
    waitInsideBody: () =>
      new Promise<void>((resolve) => {
        insideBody = resolve;
      }),
    async row(n: number) {
      const command = runtime.root,
        images = { spoolId: p.id, row: n };
      await runtime.beforeRow(command, p.id, event, images, 1);
      const entry: ReferentialEntry = {
        id: n,
        commandId: command.id,
        frameId: command.frameId,
        fkId: c.foreignKeys[0]?.id ?? 0,
        event: event === "delete" ? "parentDelete" : "parentUpdate",
        kind: "cascade",
        sortName: "RI_ConstraintTrigger_a_1",
        images,
        targets: null,
        triggerBudget: 1,
      };
      entries.set(n, entry);
      await runtime.afterRow(command, p.id, event, images, [entry], 1);
    },
    close: () =>
      (closing ??= (async () => {
        await runtime.close();
        const frames = runtime.retainedFrames,
          pendingInputs = inputs.size,
          readers = core.harness.path.shared.readers.count;
        statement.rollback();
        tx.rollback();
        await files.closeAll();
        pages.close();
        pool.close();
        grant.close();
        competing.close();
        expect(spill.bytesInUse).toBe(0);
        expect(governor.usedBytes).toBe(0);
        expect(core.harness.path.shared.readers.count).toBe(0);
        await core.close();
        expect(frames).toBe(0);
        expect(pendingInputs, "no retained fixture inputs").toBe(0);
        expect(readers, "no prefix reader registrations").toBe(0);
      })()),
  };
}
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
