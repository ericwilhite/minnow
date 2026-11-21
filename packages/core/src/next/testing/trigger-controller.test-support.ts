/** Scalar controller adapter only; not the native SQL provider. Shared by contract and PG ordering campaigns. */
import { expect } from "vitest";
import type { TriggerDef } from "../access/trigger-def.js";
import type { CatalogView } from "../access/catalog.js";
import {
  ROW_IMAGE_UNREAD,
  type CommandToken,
  type NativeCommandBoundary,
  type ReferentialEntry,
  type WriteEvent,
} from "../access/statement-command.js";
import {
  TriggerController,
  triggerControllerMinimumBytes,
  type PreparedRowTrigger,
} from "../exec/trigger-controller.js";
import { ExecutionError } from "../exec/errors.js";
import { TriggerBodyRunner, type TriggerBodyHost } from "../exec/trigger-body.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ints } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { yieldTask } from "../task.js";

export const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
const names = ["Afirst", "zlast", "\ue000", "\u{10000}"];
type Fault =
  "none" | "unique" | "body" | "pending close" | "depth" | "terminal not prepared" | "RI unique";
export async function fixture(
  event: "delete" | "update",
  unicode: boolean,
  twoBodies: boolean,
  fault: Fault = "none",
  omit: number | null = null,
  terminalBefore = false,
  conflictAfter = 2,
  options: {
    afterNames?: readonly string[];
    beforeNames?: readonly string[];
    reverseCreation?: boolean;
  } = {},
) {
  const core = await new StorageCore(16).open();
  for (const sql of [
    "CREATE TABLE p(id INT PRIMARY KEY)",
    fault === "RI unique"
      ? "CREATE TABLE c(id INT PRIMARY KEY,pid INT DEFAULT 99 REFERENCES p(id) ON DELETE CASCADE ON UPDATE SET DEFAULT,grp INT NOT NULL,UNIQUE(pid,grp))"
      : "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
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
      ...(options.beforeNames ?? ["parentBefore"]).map((name, i) => ({
        name,
        table: "p",
        timing: "before" as const,
        event,
        label: i === 0 ? "pB" : `pB${String(i)}`,
        double: twoBodies,
      })),
      ...(options.afterNames ?? names.slice(0, unicode ? 4 : 2)).map((name, i) => ({
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
  if (options.reverseCreation) specs.reverse();
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
    uniqueError = new ExecutionError("child unique conflict", "MINNOW_TEST_CHILD_UNIQUE", "23505"),
    afterError = new ExecutionError("later AFTER body division", "MINNOW_TEST_AFTER", "22012"),
    rootUniqueSteps: number[] = [],
    riCommands: CommandToken[] = [],
    trace: string[] = [],
    checkpoints: string[] = [],
    budgets = new Map<number, number>(),
    inputs = new Map<number, { label: string; n: number; target: number }>(),
    entries = new Map<number, ReferentialEntry>();
  let riWrites = 0,
    serial = 0,
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
      if (command.frameId === 1) {
        rootUniqueSteps.push(riWrites);
        if (fault === "RI unique" && riWrites >= conflictAfter) throw uniqueError;
      }
    },
    verifyCommandReferences: async (command) => {
      checkpoints.push(`refs:${String(command.id)}`);
    },
    executeReferential: async (entry, hooks, abort) => {
      abort.throwIfAborted();
      const command = { rootId: 9, id: entry.commandId, frameId: entry.frameId },
        images = { spoolId: c.id, row: entry.images.row + 1000 };
      riCommands.push(command);
      await hooks.beforeRow(command, c.id, event, images, entry.triggerBudget);
      riWrites++;
      await hooks.afterRow(command, c.id, event, images, [], entry.triggerBudget);
      // Same command token can gain new suspects after root/earlier action checkpoints.
      // Check only newly introduced uniqueness; this does not drain outer AFTER events.
      await native.verifyCommandUnique(command, abort);
    },
    executeReferentialId: async (id, hooks, abort) => {
      abort.throwIfAborted();
      const entry = entries.get(id);
      if (!entry) throw new Error("missing scalar RI entry");
      await native.executeReferential(entry, hooks, abort);
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
        if (fault === "RI unique" && record.label === "p1" && record.n === conflictAfter)
          throw afterError;
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
    native,
    signal: signal.signal,
    entries,
    rootUniqueSteps,
    riCommands,
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
        kind: fault === "RI unique" ? "setDefault" : "cascade",
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
