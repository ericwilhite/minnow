/** Actual native registry/writer/action/controller assembly. No SQL firing is enabled. */
import type { WriteTransaction } from "../access/write-path.js";
import { NativeBodyHost } from "../owner/body-host.js";
import { NativeBodyPrograms, nativeBodyProgramMinimumBytes } from "../owner/body-programs.js";
import type { BoundParameters } from "../exec/executor.js";
import type { NativeRowHooks } from "../access/statement-command.js";
import { expect } from "vitest";
import { TriggerBodyRunner } from "../exec/trigger-body.js";
import { ExecutionError } from "../exec/errors.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { StorageCore } from "./storage-core.js";
import { StatementImpl } from "../write/transaction.js";
import { releaseIdle } from "../write/budget.js";

import { bindTableRules } from "../exec/row-rules.js";
import { compileTableRules } from "../exec/rules.js";
import { clockParameter } from "../exec/parameters.js";
import { TriggerController } from "../exec/trigger-controller.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
import { ChunkReader } from "../page/reader.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { Batch } from "../vector/vector.js";
import { RegistryDirectory } from "../owner/registry-directory.js";
import { RegistryFragments } from "../owner/registry-fragments.js";
import { RegistryImages } from "../owner/registry-images.js";
import { RegistryCommands } from "../owner/registry-commands.js";
import { RegistryTargets } from "../owner/registry-targets.js";
import { RegistryFollowing } from "../owner/registry-following.js";
import { NativePrefixImages, nativePrefixSourceBytes } from "../owner/prefix-images.js";
import { NativeChildInputs } from "../owner/child-inputs.js";
import { NativeCommandChecks } from "../owner/command-checks.js";
import { NativeRows } from "../owner/native-rows.js";
import { NativeDefaults } from "../owner/native-defaults.js";
import { NativeReferentialChecks } from "../owner/referential-checks.js";
import { NativeReferentialMutations } from "../owner/referential-mutations.js";
import { NativeCommandExecution } from "../owner/command-execution.js";

export async function referentialFixture(
  setup: readonly string[],
  published = false,
  failAfter: number | null = null,
  capBytes = 256 * 1024 * 1024,
  nativeRootSql: string | null = null,
  options: { frameCount?: number; prior?: (tx: WriteTransaction) => Promise<void> } = {},
) {
  const core = new StorageCore(256);
  Object.defineProperty(core.harness, "governor", { value: new Governor(capBytes) });
  await core.open();
  for (const sql of setup) await core.execute(sql);
  if (failAfter !== null) {
    await core.execute("CREATE TABLE error_sink(v INT)");
    await core.execute(
      `CREATE TRIGGER "Z_after" AFTER UPDATE ON p BEGIN INSERT INTO error_sink VALUES(CASE WHEN OLD.id=${String(failAfter)} THEN 1/0 ELSE NULL END); END`,
    );
  }
  const frames = options.frameCount ?? (nativeRootSql === null ? (failAfter === null ? 1 : 2) : 3);
  const path = core.harness.path,
    governor = core.harness.governor;
  if (published) {
    await path.recycleLog();
    await path.quiesce();
  }
  const tx = await path.begin(),
    stop = new AbortController(),
    signal = stop.signal;
  let sourceMinimum = 0;
  for (const table of tx.catalog.tables)
    sourceMinimum = Math.max(sourceMinimum, nativePrefixSourceBytes(table));
  const grants = await governor.admitGroup({
      label: "complete native RI root",
      signal,
      grants: [
        { label: "native root metadata", minimumBytes: 4 * 1024 * 1024, spillable: true },
        { label: "native shared source step", minimumBytes: sourceMinimum, spillable: true },
      ],
    }),
    charge = grants[0],
    source = grants[1];
  if (!charge || !source) throw new Error("Incomplete native root admission");
  async function ensure(grant: Grant, bytes: number, stop: AbortSignal) {
    stop.throwIfAborted();
    signal.throwIfAborted();
    if (grant.loaned) throw new Error("native step entered with outstanding loan");
    if (grant.held - grant.allocated < bytes) {
      if (!grant.makeRoom || !(await grant.makeRoom(bytes, stop)))
        throw new Error("Missing root step capability");
      stop.throwIfAborted();
      signal.throwIfAborted();
    }
  }
  await options.prior?.(tx);
  const catalog = tx.catalog,
    tables = catalog.tables.map((table) => {
      const expr = core.lowerer.lowerTableRules(table, catalog),
        rules = compileTableRules(table, catalog.version, expr);
      return { table, expr, rules: bindTableRules(rules, charge, { capacity: 1, signal }) };
    }),
    statement = tx.statement({
      signal,
      rules: (id) => {
        const binding = tables.find((binding) => binding.table.id === id);
        if (!binding) throw new Error("missing native rules");
        return binding.rules;
      },
    });
  if (!(statement instanceof StatementImpl)) throw new Error("missing native statement");
  const preparedTriggers = catalog.triggers.map((definition) => {
    const owner = catalog.table(definition.tableId);
    if (!owner) throw new Error("Missing fixture trigger owner");
    return {
      definition,
      bodies: definition.statements.map((body) => {
        const lowered = core.lowerer.lower(body.sql, {
          ...core.context(),
          parameterTypes: body.bindings.map((binding) => binding.type),
        });
        if (lowered.kind !== "modify") throw new Error("Missing fixture body modify");
        return new TriggerBodyRunner(owner, body, lowered.plan);
      }),
    };
  });
  preparedTriggers.sort((a, b) => {
    const left = new TextEncoder().encode(a.definition.name),
      right = new TextEncoder().encode(b.definition.name);
    for (let i = 0; i < Math.min(left.length, right.length); i++)
      if (left[i] !== right[i]) return (left[i] ?? 0) - (right[i] ?? 0);
    return left.length - right.length || a.definition.id - b.definition.id;
  });
  const rootLowered =
    nativeRootSql === null ? null : core.lowerer.lower(nativeRootSql, core.context());
  if (rootLowered && rootLowered.kind !== "modify") throw new Error("Missing native root modify");
  const rootPlan = rootLowered?.kind === "modify" ? rootLowered.plan : null;
  const nativePlans = rootPlan
    ? [
        { body: null, plan: rootPlan },
        ...preparedTriggers.flatMap((trigger) =>
          trigger.bodies.map((body) => ({ body: body.body, plan: body.plan })),
        ),
      ]
    : null;
  if (nativePlans) {
    releaseIdle(source);
    await ensure(charge, nativeBodyProgramMinimumBytes(nativePlans), signal);
  }
  const bodyPrograms = nativePlans ? new NativeBodyPrograms(catalog, nativePlans, charge) : null;
  let bodyHost: NativeBodyHost | null = null;
  const checkpoint = async () => {
      signal.throwIfAborted();
    },
    spill = new MemorySpillService(),
    directory = new RegistryDirectory(charge, spill, 1, signal),
    fragments = new RegistryFragments(charge, spill, 1, signal, checkpoint),
    images = new RegistryImages({
      rootId: 39,
      catalog: bodyPrograms ?? catalog,
      directory,
      fragments,
      charge,
      signal,
      checkpoint,
      prepare: (bytes, stop) => ensure(charge, bytes, stop),
    }),
    commands = new RegistryCommands({
      rootId: 39,
      frames,
      rowEvents: 16,
      directory,
      charge,
      signal,
      checkpoint,
      referentialName: (id, kind) =>
        `RI_ConstraintTrigger_${kind.startsWith("child") ? "c" : "a"}_${String(id)}`,
    }),
    targets = new RegistryTargets({ rootId: 39, directory, charge, signal, checkpoint }),
    reads = {
      open: (
        pages: Parameters<typeof ChunkReader.open>[0],
        chunk: Parameters<typeof ChunkReader.open>[1],
        options: Parameters<typeof ChunkReader.open>[2],
      ) => ChunkReader.open(pages, chunk, options),
    },
    checks = new NativeCommandChecks({
      statement,
      tables: tables.map(({ table }) => table.id),
      images,
      charge,
      signal,
      prepareRange: async (plan, stop) => {
        await ensure(source, plan.minimumBytes, stop);
        return source;
      },
      scan: {
        reads,
        prepare: async (_table, bytes, stop) => {
          await ensure(source, bytes, stop);
          return source;
        },
      },
    }),
    following = new RegistryFollowing({
      rootId: 39,
      catalog,
      commands,
      directory,
      targets,
      checks,
      charge,
      signal,
    }),
    prefix = new NativePrefixImages({
      rootId: 39,
      statement,
      tables: tables.map(({ table }) => table.id),
      images,
      reads,
      charge,
      signal,
      checkpoint,
      abortRoot: (reason) => stop.abort(reason),
      prepareSource: async (_table, bytes, stop) => {
        await ensure(source, bytes, stop);
        return source;
      },
    }),
    inputs = new NativeChildInputs({
      rootId: 39,
      statement,
      targets,
      images: prefix,
      directory,
      charge,
      signal,
      checkpoint,
      abortRoot: (reason) => stop.abort(reason),
    }),
    defaults = new NativeDefaults({
      catalog,
      sources: tables.flatMap(({ table, expr }) =>
        table.columns
          .filter((c) => c.generatedSql === null)
          .map((column) => ({
            tableId: table.id,
            columnId: column.id,
            expr: expr.defaults?.find((expr) => expr.columnId === column.id)?.expr ?? null,
          })),
      ),
      clock: clockParameter({ kind: "timestamp", zoned: true }, 0n),
      charge,
      signal,
      prepare: (bytes, stop) => ensure(charge, bytes, stop),
    }),
    restrictions = new NativeReferentialChecks({
      catalog,
      commands,
      targets,
      checks,
      signal,
      charge,
    }),
    releaseIdleStep = () => {
      releaseIdle(source);
      releaseIdle(charge);
    };
  const primary = statement.openNativeCommands({
      tables: tables.map(({ table }) => table.id),
      charge,
      signal,
      commandBudget: (command) => commands.budget(command),
      finishRoot: async () => {
        try {
          await controller.finishRoot();
        } finally {
          releaseIdleStep();
        }
      },
      closeRoot: async () => {
        if (bodyHost) stop.abort(new Error("Native root closing"));
        await controller.close();
        await bodyHost?.close();
        await execution.close();
      },
    }),
    rows = new NativeRows({
      statement,
      primary,
      commands,
      images,
      targets,
      tables,
      frames,
      rowEvents: 16,
      charge,
      signal,
      checks,
      releaseIdleStep,
      prepareRules: async () => {
        await ensure(charge, 256 * 1024, signal);
      },
    }),
    actions = new NativeReferentialMutations({
      statement,
      catalog,
      commands,
      checks,
      restrictions,
      images,
      targets,
      following,
      inputs,
      rows,
      defaults,
      frames,
      charge,
      signal,
      checkpoint,
      releaseIdleStep,
    }),
    execution = new NativeCommandExecution({
      catalog,
      commands,
      images,
      checks,
      following,
      frames,
      charge,
      signal,
      abortRoot: (reason) => stop.abort(reason),
      runReferential: (entry, hooks, signal) => actions.run(entry, hooks, signal),
    }),
    pool = new Pool(new Charge(charge), 1024 * 1024),
    files = new SpillFiles(spill, 2, charge),
    pages = new PageWriter(pool, 16384),
    io = new SpillIo(pool, files, pages, { grant: charge, queryId: 2, signal }, () =>
      signal.throwIfAborted(),
    );
  if (bodyPrograms)
    bodyHost = new NativeBodyHost({
      rootId: 39,
      frames,
      programs: bodyPrograms,
      statement,
      commands,
      images,
      directory,
      prefix,
      rows,
      targets,
      charge,
      signal,
      abortRoot: (reason) => stop.abort(reason),
      checkpoint,
      releaseIdleStep,
      query: {
        governor,
        source,
        metadata: charge,
        signal,
        prepare: (bytes, stop) => ensure(source, bytes, stop),
        prepareMetadata: (bytes, stop) => ensure(charge, bytes, stop),
      },
      execution: {
        spill,
        queryId: 2,
        statementTime: 0n,
        reads,
        sequences: null,
        sequenceSession: null,
        returning: "atExecuted",
      },
    });
  const controller = await TriggerController.create({
    statement,
    native: execution,
    programs: preparedTriggers,
    io,
    spill,
    frameCount: frames,
    rowEvents: 16,
    body: {
      charge: pool,
      work: { signal },
      host: bodyHost ?? {
        // Deliberately an error-order adapter, not the compiled body provider. Every
        // native RI write/check above is real; user bodies are still disabled in SQL.
        spoolBodyInput: async (request) => {
          const id = request.parameters.slots[0];
          if (!id || failAfter === null) throw new Error("Unexpected RI fixture body");
          if (valueAt(id, 0) === failAfter)
            throw new ExecutionError("later AFTER division", "MINNOW_TEST_DIVISION", "22012");
          return { rootId: request.command.rootId, id: request.command.id };
        },
        applyBodyInput: async () => undefined,
        releaseBodyInput: async () => undefined,
      },
    },
  });
  let retiring: Promise<void> | null = null;
  const retire = () =>
    (retiring ??= (async () => {
      await statement.closeNativeCommands();
      await bodyPrograms?.close();
      actions.close();
      rows.close();
      restrictions.close();
      defaults.close();
      await inputs.close();
      await prefix.close();
      for (const binding of tables) binding.rules.close();
      await targets.close();
      await fragments.close();
      await directory.close();
      await files.closeAll();
      pages.close();
      pool.close();
      expect(charge.allocated).toBe(0);
      expect(source.allocated).toBe(0);
      expect(source.loaned).toBe(0);
      charge.close();
      source.close();
      statement.rollback();
      tx.rollback();
      expect(spill.bytesInUse).toBe(0);
    })());
  return {
    core,
    spill,
    path,
    statement,
    tx,
    stop,
    images,
    commands,
    checks,
    rows,
    prefix,
    controller,
    execution,
    charge,
    source,
    actions,
    defaults,
    bodyHost,
    rootPlan,
    async nativeRoot(
      parameters: BoundParameters = { slots: [] },
      hooks: NativeRowHooks = controller,
    ) {
      if (!bodyHost || !rootPlan) throw new Error("Missing native compiled root");
      const pin = statement.readOwn();
      let input: Awaited<ReturnType<typeof bodyHost.spoolRootInput>> | null = null;
      try {
        input = await bodyHost.spoolRootInput({
          command: controller.root,
          plan: rootPlan,
          inputView: pin,
          parameters,
          signal,
        });
        await bodyHost.applyRootInput({
          command: controller.root,
          plan: rootPlan,
          inputView: pin,
          input,
          hooks,
          signal,
        });
      } finally {
        if (input) await bodyHost.releaseBodyInput(controller.root, input);
        pin.release();
      }
    },
    async capture(tableName: string, indexes: readonly number[]) {
      const table = catalog.tableByName(tableName);
      if (!table) throw new Error("missing native table");
      const pin = statement.readOwn();
      let ids: number[];
      try {
        const view = pin.internalTable(table.id);
        ids = indexes.map((i) => view.rowId(i));
      } finally {
        pin.release();
      }
      const refs = [];
      for (const id of ids) refs.push({ id, image: await prefix.capture(table.id, id, signal) });
      return { tableId: table.id, refs };
    },
    async update(
      tableId: number,
      oldId: number,
      input: Batch,
      oldImage: Parameters<typeof images.derive>[0],
    ) {
      const table = catalog.table(tableId);
      if (!table) throw new Error("missing native table");
      const patches = table.columns.map((column, i) => ({
          columnId: column.id,
          value: input.columns[i] ?? null,
        })),
        proposed = await images.derive(oldImage, "old", patches, signal);
      return rows.write(controller.root, tableId, "update", proposed, oldId, controller, signal);
    },
    async rollback() {
      await statement.closeNativeCommands();
      statement.rollback();
      tx.rollback();
    },
    async end() {
      await statement.end();
      const receipt = await tx.commit(null);
      await receipt.durable;
    },
    retire,
    async close() {
      await retire();
      await core.close();
      expect(governor.usedBytes).toBe(0);
    },
  };
}
