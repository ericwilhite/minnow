/** Native graph assembly inside an ALREADY admitted root. No governor admission, root
 * commit or SQL enablement occurs here. Caller owns grants, bound rules and preparation. */
import type { CatalogView } from "../access/catalog.js";
import type { MaybePromise } from "../access/key-index.js";
import type { BoundParameters } from "../exec/executor.js";
import type { FlatVector } from "../vector/vector.js";
import type { Grant, MemoryGovernor } from "../memory/governor.js";
import type { RowImageRef } from "../access/statement-command.js";
import type { StatementImpl } from "../write/transaction.js";
import type { NativePrimaryAdapter } from "../write/primary-command.js";
import { NATIVE_PRIMARY_BYTES } from "../write/primary-command.js";
import {
  TriggerController,
  triggerControllerMinimumBytes,
  type PreparedRowTrigger,
} from "../exec/trigger-controller.js";
import type { TriggerBodyInput } from "../exec/trigger-body.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
import { RegistryDirectory, REGISTRY_DIRECTORY_BYTES } from "./registry-directory.js";
import { RegistryFragments, REGISTRY_FRAGMENT_BYTES } from "./registry-fragments.js";
import { RegistryImages, REGISTRY_IMAGE_BYTES } from "./registry-images.js";
import { RegistryCommands, registryCommandBytes } from "./registry-commands.js";
import { RegistryTargets, REGISTRY_TARGET_BYTES } from "./registry-targets.js";
import { RegistryFollowing, registryFollowingBytes } from "./registry-following.js";
import { NativePrefixImages, nativePrefixImageBytes } from "./prefix-images.js";
import { NativeChildInputs, NATIVE_CHILD_INPUT_BYTES } from "./child-inputs.js";
import {
  NativeDefaults,
  nativeDefaultMinimumBytes,
  type NativeDefaultSource,
} from "./native-defaults.js";
import { NativeCommandChecks, commandCheckMinimumBytes } from "./command-checks.js";
import { NativeRows, nativeRowMinimumBytes, type NativeRowScope } from "./native-rows.js";
import { NativeReferentialChecks, NATIVE_REFERENTIAL_CHECK_BYTES } from "./referential-checks.js";
import {
  NativeReferentialMutations,
  nativeReferentialMinimumBytes,
} from "./referential-mutations.js";
import { NativeCommandExecution, nativeCommandExecutionBytes } from "./command-execution.js";
import { NativeFollowUnique, nativeFollowUniqueBytes } from "./follow-unique.js";
import {
  nativeBodyProgramMinimumBytes,
  NativeBodyPrograms,
  type NativeBodyPlan,
} from "./body-programs.js";
import { NativeBodyHost, nativeBodyHostBytes, type NativeBodyHostScope } from "./body-host.js";
export interface NativeRootFunding {
  readonly metadata: Grant;
  readonly source: Grant;
  /** Complete root funding ports. Called only at the existing source/image/rule boundaries;
   * they must transfer/reclaim or use prepaid progress, never admit while holding a producer. */
  prepareMetadata(bytes: number, signal: AbortSignal): Promise<void>;
  prepareSource(bytes: number, signal: AbortSignal): Promise<void>;
  prepareRules: NativeRowScope["prepareRules"];
  releaseIdleStep(): void;
}
export interface NativeRootPreparation {
  readonly rootId: number;
  readonly catalog: CatalogView;
  readonly frames: number;
  readonly rowEvents: number;
  readonly root: NativeBodyPlan["plan"];
  /** Immutable, UTF8-name/ID ordered and statically reachable preparation. */
  readonly triggers: readonly PreparedRowTrigger[];
  /** Sorted stable IDs; caller funds this immutable reach array. */
  readonly tableIds: readonly number[];
  readonly defaults: readonly NativeDefaultSource[];
  readonly clock: FlatVector;
}
export interface NativeCommandRootScope extends NativeRootPreparation {
  readonly statement: StatementImpl;
  /** Exactly these bound modules must be returned by statement.rules(id). */
  readonly tables: NativeRowScope["tables"];
  readonly funding: NativeRootFunding;
  readonly governor: MemoryGovernor;
  readonly execution: NativeBodyHostScope["execution"];
  readonly signal: AbortSignal;
  checkpoint(): MaybePromise<void>;
}
function invalid(message: string): never {
  throw new TypeError(`Invalid native root: ${message}`);
}
function baseBytes(scope: NativeRootPreparation) {
  let bodies = 1;
  for (const trigger of scope.triggers) bodies += trigger.bodies.length;
  return 8192 + 512 * (scope.tableIds.length + scope.triggers.length + bodies);
}
function poolBytes(scope: NativeRootPreparation) {
  let bindings = 0;
  for (const trigger of scope.triggers)
    for (const body of trigger.bodies) bindings = Math.max(bindings, body.minimumBytes);
  return triggerControllerMinimumBytes(scope.frames, scope.rowEvents) + bindings + 4 * 65536;
}
function plans(scope: NativeRootPreparation): NativeBodyPlan[] {
  const entries: NativeBodyPlan[] = [{ body: null, plan: scope.root }];
  for (const trigger of scope.triggers)
    for (const runner of trigger.bodies) entries.push({ body: runner.body, plan: runner.plan });
  return entries;
}
/** THIS assembler's retained setup allocation only. Caller also funds its bound-rule and
 * immutable preparation retention, complete source/image/binding/writer progress and spill
 * IO according to the root admission contract. This is NOT a complete execution minimum. */
export function nativeCommandRootSetupBytes(scope: NativeRootPreparation): number {
  const catalog = scope.catalog,
    ids = scope.tableIds;
  let columns = 0;
  for (const table of catalog.tables) columns = Math.max(columns, table.columns.length);
  const programBytes = nativeBodyProgramMinimumBytes(plans(scope));
  const bytes =
    baseBytes(scope) +
    REGISTRY_DIRECTORY_BYTES +
    REGISTRY_FRAGMENT_BYTES +
    REGISTRY_IMAGE_BYTES +
    registryCommandBytes(scope.frames) +
    REGISTRY_TARGET_BYTES +
    registryFollowingBytes(catalog, scope.frames) +
    nativePrefixImageBytes(columns) +
    NATIVE_CHILD_INPUT_BYTES +
    nativeDefaultMinimumBytes(scope.defaults, scope.clock) +
    commandCheckMinimumBytes(catalog, ids) +
    NATIVE_REFERENTIAL_CHECK_BYTES +
    NATIVE_PRIMARY_BYTES +
    nativeRowMinimumBytes(columns, scope.frames, scope.rowEvents) +
    nativeReferentialMinimumBytes(catalog, scope.frames) +
    nativeCommandExecutionBytes(scope.frames) +
    nativeFollowUniqueBytes(scope.frames) +
    programBytes +
    nativeBodyHostBytes(columns, scope.frames) +
    poolBytes(scope);
  if (!Number.isSafeInteger(bytes)) invalid("setup too large");
  return bytes;
}
export class NativeCommandRoot {
  #scope: NativeCommandRootScope | null;
  #stop: AbortController | null = null;
  #signal: AbortSignal | null = null;
  readonly #base: number;
  #closers: Array<() => MaybePromise<void>> = [];
  #primary: NativePrimaryAdapter | null = null;
  #controller: TriggerController | null = null;
  #host: NativeBodyHost | null = null;
  #execution: NativeCommandExecution | null = null;
  #running: Promise<number> | null = null;
  #ending: Promise<void> | null = null;
  #images: RegistryImages | null = null;
  #ran = false;
  #stopping = false;
  #shutdown: Promise<void> | null = null;
  #closing: Promise<void> | null = null;
  private constructor(scope: NativeCommandRootScope, bytes: number) {
    this.#scope = scope;
    this.#base = bytes;
    const stop = new AbortController();
    this.#stop = stop;
    this.#signal = AbortSignal.any([scope.signal, stop.signal]);
  }
  static async create(scope: NativeCommandRootScope): Promise<NativeCommandRoot> {
    scope.signal.throwIfAborted();
    const bytes = baseBytes(scope);
    if (!Number.isSafeInteger(bytes) || !scope.funding.metadata.allocate(bytes))
      invalid("setup was not admitted");
    let root: NativeCommandRoot | null = null;
    try {
      root = new NativeCommandRoot(scope, bytes);
      await root.#install();
      return root;
    } catch (error) {
      if (root) {
        try {
          await root.close();
        } catch (cleanup) {
          throw new AggregateError([error, cleanup], "Native root setup failed", {
            cause: cleanup,
          });
        }
      } else scope.funding.metadata.free(bytes);
      throw error;
    }
  }
  private get scope() {
    return this.#scope ?? invalid("closed");
  }
  private get signal() {
    return this.#signal ?? invalid("closed");
  }
  #abort(reason: unknown) {
    this.#stop?.abort(reason);
  }
  #check() {
    this.#signal?.throwIfAborted();
    if (this.#stopping) invalid("stopped");
    if (!this.#signal) invalid("closed");
  }
  #own<T>(part: T, close: (part: T) => MaybePromise<void>): T {
    this.#closers.push(() => close(part));
    return part;
  }
  async #install() {
    this.#check();
    const scope = this.scope,
      catalog = scope.statement.catalog,
      charge = scope.funding.metadata,
      signal = this.signal,
      root = scope.root.root;
    if (
      root.op !== "modify" ||
      scope.catalog !== catalog ||
      scope.root.catalogVersion !== catalog.version
    )
      invalid("stale modification");
    for (const binding of scope.tables)
      if (
        catalog.table(binding.table.id)?.schemaVersion !== binding.table.schemaVersion ||
        scope.statement.rules(binding.table.id) !== binding.rules
      )
        invalid("foreign rule binding");
    const ids = scope.tableIds;
    if (ids.length !== scope.tables.length) invalid("table reach length");
    let previous = 0;
    for (const [i, id] of ids.entries()) {
      if (!Number.isSafeInteger(id) || id <= previous || scope.tables[i]?.table.id !== id)
        invalid("table reach order");
      previous = id;
    }
    if (!ids.includes(root.tableId)) invalid("missing root rules");
    const directory = this.#own(
        new RegistryDirectory(charge, scope.execution.spill, scope.execution.queryId, signal),
        (v) => v.close(),
      ),
      fragments = this.#own(
        new RegistryFragments(charge, scope.execution.spill, scope.execution.queryId, signal, () =>
          this.#checkpoint(),
        ),
        (v) => v.close(),
      ),
      programs = this.#own(new NativeBodyPrograms(catalog, plans(scope), charge), (v) => v.close()),
      images = this.#own(
        new RegistryImages({
          rootId: scope.rootId,
          catalog: programs,
          directory,
          fragments,
          charge,
          signal,
          checkpoint: () => this.#checkpoint(),
          prepare: (n, s) => this.scope.funding.prepareMetadata(n, s),
        }),
        (v) => v.close(),
      ),
      commands = this.#own(
        new RegistryCommands({
          rootId: scope.rootId,
          frames: scope.frames,
          rowEvents: scope.rowEvents,
          directory,
          charge,
          signal,
          checkpoint: () => this.#checkpoint(),
          referentialName: (id, kind) =>
            `RI_ConstraintTrigger_${kind.startsWith("child") ? "c" : "a"}_${String(id)}`,
        }),
        (v) => v.close(),
      ),
      targets = this.#own(
        new RegistryTargets({
          rootId: scope.rootId,
          directory,
          charge,
          signal,
          checkpoint: () => this.#checkpoint(),
        }),
        (v) => v.close(),
      ),
      checks = this.#own(
        new NativeCommandChecks({
          statement: scope.statement,
          tables: ids,
          images,
          charge,
          signal,
          prepareRange: async (plan, s) => {
            await this.scope.funding.prepareSource(plan.minimumBytes, s);
            return this.scope.funding.source;
          },
          scan: {
            reads: scope.execution.reads,
            visit: (tableId, rowId, columns, signal, visitor) =>
              prefix.visit(tableId, rowId, columns, signal, visitor),
            prepare: async (_t, n, s) => {
              await this.scope.funding.prepareSource(n, s);
              return this.scope.funding.source;
            },
          },
        }),
        (v) => v.close(),
      ),
      following = this.#own(
        new RegistryFollowing({
          rootId: scope.rootId,
          frames: scope.frames,
          catalog,
          commands,
          directory,
          targets,
          checks,
          charge,
          signal,
        }),
        (v) => v.close(),
      ),
      prefix = this.#own(
        new NativePrefixImages({
          rootId: scope.rootId,
          statement: scope.statement,
          tables: ids,
          images,
          reads: scope.execution.reads,
          charge,
          signal,
          checkpoint: () => this.#checkpoint(),
          abortRoot: (r) => this.#abort(r),
          prepareSource: async (_t, n, s) => {
            await this.scope.funding.prepareSource(n, s);
            return this.scope.funding.source;
          },
        }),
        (v) => v.close(),
      ),
      inputs = this.#own(
        new NativeChildInputs({
          rootId: scope.rootId,
          statement: scope.statement,
          targets,
          images: prefix,
          directory,
          charge,
          signal,
          checkpoint: () => this.#checkpoint(),
          abortRoot: (r) => this.#abort(r),
        }),
        (v) => v.close(),
      ),
      defaults = this.#own(
        new NativeDefaults({
          catalog,
          sources: scope.defaults,
          clock: scope.clock,
          charge,
          signal,
          prepare: (n, s) => this.scope.funding.prepareMetadata(n, s),
        }),
        (v) => v.close(),
      ),
      restrictions = this.#own(
        new NativeReferentialChecks({ catalog, commands, targets, checks, charge, signal }),
        (v) => v.close(),
      );
    this.#images = images;
    const primary = scope.statement.openNativeCommands({
      tables: ids,
      charge,
      signal,
      commandBudget: (c) => commands.budget(c),
      finishRoot: async () => {
        if (this.#running) invalid("unfinished root input");
        try {
          await (this.#controller ?? invalid("missing controller")).finishRoot();
        } finally {
          this.scope.funding.releaseIdleStep();
        }
      },
      closeRoot: () => this.#shutdownCommands(),
    });
    this.#primary = this.#own(primary, (v) => v.close());
    const rows = this.#own(
        new NativeRows({
          statement: scope.statement,
          primary,
          commands,
          images,
          targets,
          tables: scope.tables,
          frames: scope.frames,
          rowEvents: scope.rowEvents,
          charge,
          signal,
          checks,
          releaseIdleStep: () => this.scope.funding.releaseIdleStep(),
          prepareRules: (table, images, phase, signal) =>
            this.scope.funding.prepareRules(table, images, phase, signal),
        }),
        (v) => v.close(),
      ),
      actions = this.#own(
        new NativeReferentialMutations({
          statement: scope.statement,
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
          frames: scope.frames,
          charge,
          signal,
          checkpoint: () => this.#checkpoint(),
          releaseIdleStep: () => this.scope.funding.releaseIdleStep(),
        }),
        (v) => v.close(),
      ),
      followUnique = this.#own(
        new NativeFollowUnique({
          rootId: scope.rootId,
          frames: scope.frames,
          catalog,
          directory,
          following,
          targets,
          checks,
          charge,
          signal,
          checkpoint: () => this.#checkpoint(),
        }),
        (v) => v.close(),
      ),
      execution = this.#own(
        new NativeCommandExecution({
          catalog,
          commands,
          images,
          checks,
          following,
          followUnique,
          frames: scope.frames,
          charge,
          signal,
          abortRoot: (r) => this.#abort(r),
          runReferential: (e, h, s) => actions.run(e, h, s),
        }),
        (v) => v.close(),
      );
    this.#execution = execution;
    const pool = this.#own(new Pool(new Charge(charge), poolBytes(scope)), (v) => v.close()),
      files = this.#own(
        new SpillFiles(scope.execution.spill, scope.execution.queryId, charge),
        (v) => v.closeAll(),
      ),
      pages = this.#own(new PageWriter(pool, 16384), (v) => v.close()),
      io = new SpillIo(
        pool,
        files,
        pages,
        { grant: charge, queryId: scope.execution.queryId, signal },
        () => this.signal.throwIfAborted(),
      );
    this.#host = this.#own(
      new NativeBodyHost({
        rootId: scope.rootId,
        frames: scope.frames,
        programs,
        statement: scope.statement,
        commands,
        images,
        directory,
        prefix,
        rows,
        targets,
        charge,
        signal,
        abortRoot: (r) => this.#abort(r),
        checkpoint: () => this.#checkpoint(),
        releaseIdleStep: () => this.scope.funding.releaseIdleStep(),
        query: {
          governor: scope.governor,
          metadata: charge,
          source: scope.funding.source,
          signal,
          prepare: (n, s) => this.scope.funding.prepareSource(n, s),
          prepareMetadata: (n, s) => this.scope.funding.prepareMetadata(n, s),
        },
        execution: scope.execution,
      }),
      (v) => v.close(),
    );
    this.#controller = this.#own(
      await TriggerController.create({
        statement: scope.statement,
        native: execution,
        programs: scope.triggers,
        io,
        spill: scope.execution.spill,
        frameCount: scope.frames,
        rowEvents: scope.rowEvents,
        body: { host: this.#host, charge: pool, work: { signal } },
      }),
      (v) => v.close(),
    );
    this.#check();
  }
  async #checkpoint() {
    this.signal.throwIfAborted();
    await this.scope.checkpoint();
    this.signal.throwIfAborted();
  }
  run(parameters: BoundParameters): Promise<number> {
    this.#check();
    if (this.#running || this.#ran) invalid("root input already started");
    this.#ran = true;
    const scope = this.scope,
      host = this.#host ?? invalid("missing host"),
      controller = this.#controller ?? invalid("missing controller");
    const work = Promise.resolve().then(async () => {
      this.#check();
      const pin = scope.statement.readOwn();
      let input: TriggerBodyInput | null = null;
      try {
        input = await host.spoolRootInput({
          command: controller.root,
          plan: scope.root,
          parameters,
          inputView: pin,
          signal: this.signal,
        });
        this.#check();
        const count = await host.applyRootInput({
          command: controller.root,
          plan: scope.root,
          input,
          inputView: pin,
          hooks: controller,
          signal: this.signal,
        });
        this.#check();
        return count;
      } finally {
        try {
          if (input && !this.#stopping) await host.releaseBodyInput(controller.root, input);
        } finally {
          pin.release();
        }
      }
    });
    const pending = work
      .catch((reason: unknown) => {
        this.#abort(reason);
        throw reason;
      })
      .finally(() => {
        if (this.#running === pending) this.#running = null;
      });
    this.#running = pending;
    return pending;
  }
  /** Root-owned metadata inspection for rule/progress funding; no payload lease escapes. */
  describeImage(ref: RowImageRef, widths: number[], signal: AbortSignal) {
    this.#check();
    return (this.#images ?? invalid("missing images")).describe(
      ref,
      widths,
      AbortSignal.any([signal, this.signal]),
    );
  }
  /** Sole ending entrypoint for this graph. Bodies never call it; caller commits separately. */
  end(): Promise<void> {
    this.#check();
    if (!this.#ran || this.#running || this.#ending) invalid("unfinished root input/end");
    const work = Promise.resolve().then(async () => {
      this.#check();
      await this.scope.statement.end(this.signal);
      this.#check();
    });
    const pending = work
      .catch((reason: unknown) => {
        this.#abort(reason);
        throw reason;
      })
      .finally(() => {
        if (this.#ending === pending) this.#ending = null;
      });
    this.#ending = pending;
    return pending;
  }
  #shutdownCommands(): Promise<void> {
    return (this.#shutdown ??= (async () => {
      this.#stopping = true;
      this.#abort(new Error("Native root closing"));
      const errors: unknown[] = [];
      for (const close of [
        () => this.#controller?.close(),
        () => this.#host?.close(),
        () => this.#execution?.close(),
      ]) {
        try {
          await close();
        } catch (e) {
          errors.push(e);
        }
      }
      if (errors.length) throw new AggregateError(errors, "Native command shutdown failed");
    })());
  }
  close(): Promise<void> {
    return (this.#closing ??= (async () => {
      this.#stopping = true;
      this.#abort(new Error("Native root closed"));
      const errors: unknown[] = [];
      if (this.#primary) {
        try {
          await this.scope.statement.closeNativeCommands();
        } catch (e) {
          errors.push(e);
        }
      } else {
        try {
          await this.#shutdownCommands();
        } catch (e) {
          errors.push(e);
        }
      }
      await this.#running?.catch(() => undefined);
      await this.#ending?.catch(() => undefined);
      for (let close = this.#closers.pop(); close; close = this.#closers.pop()) {
        try {
          await close();
        } catch (e) {
          errors.push(e);
        }
      }
      this.#primary = this.#controller = this.#host = this.#execution = this.#images = null;
      const scope = this.scope;
      this.#scope = null;
      this.#stop = null;
      this.#signal = null;
      scope.funding.metadata.free(this.#base);
      if (errors.length) throw new AggregateError(errors, "Native root cleanup failed");
    })());
  }
}
