/**
 * The composition root's engine: `OwnerEngine` and `EngineSession` (engine.ts) over the planner
 * port (planner.ts, B6), the executor (exec/executor.ts, B4) and the write path
 * (access/write-path.ts, B3). The protocol server (server.ts) drives it; nothing here knows about
 * ports, frames on the wire or the ledger's routing.
 *
 * ## What a statement does, by kind and session state
 * | Statement         | Autocommit (session idle)                                  | Inside a transaction                         |
 * | ----------------- | ---------------------------------------------------------- | -------------------------------------------- |
 * | query             | pin a read snapshot, execute, release after `executed`      | read-write: a statement view; read-only: the transaction's pin |
 * | modify            | begin (writer slot) → statement → execute (`atCommit`) → `executed` → `commit()` → durable (strict) → `acknowledgeCommit()` | statement → execute (`atExecuted`) |
 * | ddl               | begin → statement → `changeCatalog` → `end` → follow-up → commit | statement → `changeCatalog` → `end`   |
 * | BEGIN / COMMIT / ROLLBACK / SAVEPOINT … | session transaction state (below)    |                                              |
 * | SHOW / SET        | a one-row result / nothing                                  | same                                         |
 *
 * ## Session transaction state (PostgreSQL's)
 * - `idle` → BEGIN → `open`. A read-write BEGIN takes the writer slot at once (ruling R11.2);
 *   BEGIN READ ONLY pins one snapshot for the whole transaction.
 * - A failing statement inside `open` → `failed` (25P02 for every later statement) until ROLLBACK,
 *   or ROLLBACK TO a savepoint, which reopens it. COMMIT of a failed transaction rolls back and
 *   reports `ROLLBACK`, as PostgreSQL does.
 * - The idle timeout (`expire`) rolls back → `timedOut`: every statement fails with
 *   `TransactionIdleTimeoutError` until ROLLBACK (succeeds) or COMMIT (fails, commits nothing);
 *   either ends it.
 *
 * ## The commit boundary (ruling R9)
 * `Run.reachedCommit` turns true just before `WriteTransaction.commit()` is called. Before it, the
 * run's signal aborts it (the execution cancels, the transaction rolls back); from it on the
 * signal is ignored.
 *
 * ## The synchronous point read (ruling R6)
 * `runNow` serves a `pointRead` query in an idle session (or a read-only transaction) inside the
 * call: pin, bind, `execute`; when B4 reports `state === "executed"` on return and the delivery
 * rule needs no wait, it polls the frames, releases the pin and returns them. Otherwise the
 * started execution continues as an ordinary `Run`: nothing is executed twice.
 */
import type { CatalogView } from "../access/catalog.js";
import type { Statement, TableRules } from "../access/modify.js";
import type { Snapshot } from "../access/snapshot.js";
import type { Savepoint, WritePath, WriteTransaction } from "../access/write-path.js";
import {
  EXECUTOR_COSTS,
  type Execution,
  type ExecutionSummary,
  type Executor,
  type PreparedPlan,
  type SequenceSession,
} from "../exec/executor.js";
import { createExecutor } from "../exec/create.js";
import { compileTableRules } from "../exec/rules.js";
import { ResultChannel, type ResultFrame, type ResultStream } from "../exec/result.js";
import type { ChunkReads } from "../exec/scan.js";
import type { ExecutorCosts } from "../ir/costs.js";
import type { OutputColumn, PhysicalPlan } from "../ir/ir.js";
import {
  bindParameterized,
  type ParameterizedPlan,
  type NotParameterizable,
} from "../lower/parameterize.js";
import { statisticsDrifted, type PlanningStatistics } from "../lower/statistics.js";
import type { MemoryGovernor } from "../memory/governor.js";
import { ReturnPool } from "../exec/return-pool.js";
import type { SpillService } from "../spill/contract.js";
import { PG_EPOCH_UNIX_MICROS } from "../temporal.js";
import type { TransactionModes } from "../lower/lowerer.js";
import { RuleCache } from "./rule-cache.js";
import { PlanCache } from "./plan-cache.js";
import type {
  EngineSession,
  OwnerEngine,
  PlannedStatement,
  Run,
  RunContext,
  RunOutcome,
  SyncRun,
} from "./engine.js";
import {
  InFailedTransactionError,
  NoActiveTransactionError,
  SavepointNotFoundError,
  ReadOnlyTransactionError,
  SequenceStateError,
  TransactionIdleTimeoutError,
  TransactionModeError,
} from "./errors.js";
import type { WireInputColumn } from "./frame.js";
import type { OwnerLedger } from "./outcomes.js";
import type {
  LoweredModify,
  LoweredStatement,
  StatementPlanner,
  PlannerContext,
} from "./planner.js";
import type { TransactionState, WireColumnMeta, WireValue } from "./protocol.js";

export interface ComposeOptions {
  /** Lowering (lower/create.ts `createLowerer()`), or a test double of its shape. */
  readonly planner: StatementPlanner;
  /**
   * The planning statistics lowering costs plans with (lower/statistics.ts), read synchronously
   * per table; the write path provides them (ruling R17.7). Default: none, so lowering plans with
   * its defaults.
   */
  readonly statistics?: PlanningStatistics;
  /** The executor's per-row costs the cost model reads; default the executor's own. */
  readonly costs?: ExecutorCosts;
  /** Real operator executor by default; protocol fixtures may inject a synthetic executor. */
  readonly executor?: Executor;
  readonly writePath: WritePath;
  readonly governor: MemoryGovernor;
  readonly spill: SpillService;
  readonly reads: ChunkReads;
  /**
   * The executor's row rules for a table at a catalog version. Default: exec `compileTableRules`
   * over the planner's `lowerTableRules`, cached by catalog identity (rule-cache.ts); the
   * composition passes them to every write statement.
   */
  readonly rules?: (catalog: CatalogView, tableId: number) => TableRules;
  /** Device clock in ms (the statement clock and the ledger's). */
  readonly now?: () => number;
  /** Shared templates, refusal markers and full-text plans retained across catalogs; default 512. */
  readonly planCacheSize?: number;
  /** Maximum retained plan/key/marker bytes; default 4 MiB. Zero disables retention. */
  readonly planCacheBytes?: number;
  /** The idle-transaction timeout the server applies (reported in errors). */
  readonly transactionIdleTimeoutMs?: number;
}

type AppliedTransactionModes = {
  readonly [K in keyof TransactionModes]: NonNullable<TransactionModes[K]>;
};
const DEFAULT_TRANSACTION_MODES: AppliedTransactionModes = {
  readOnly: false,
  isolation: "repeatable read",
  deferrable: false,
};
type CachedPlan =
  | PlannedStatement
  | NotParameterizable
  | {
      readonly kind: "parameterized";
      readonly template: ParameterizedPlan;
      readonly planned: PlannedStatement;
    };
function boundParameters(
  statement: PlannedStatement,
  params: readonly WireValue[],
): readonly WireValue[] {
  const binding = statement.parameterization;
  // The public binder appends only number/bigint/string literals to these wire values.
  return binding === undefined
    ? params
    : (bindParameterized(binding.template, binding.text, params) as WireValue[]);
}

const toOutputColumns = (columns: readonly WireColumnMeta[]): OutputColumn[] =>
  columns.map((column) => ({ name: column.name, type: column.type, nullable: column.nullable }));

/** A finished stream with no frames: statements without rows. */
function emptyStream(): ResultStream {
  const channel = new ResultChannel([]);
  channel.end();
  return channel;
}

/** One frame of text values, for SHOW. */
function oneTextRow(column: WireColumnMeta, value: string): ResultStream {
  const channel = new ResultChannel(toOutputColumns([column]));
  const bytes = new TextEncoder().encode(value);
  const payload = {
    rowCount: 1,
    dictionaries: [],
    columns: [
      {
        form: "flat" as const,
        length: 1,
        validity: null,
        data: { layout: "varlen" as const, bytes, offsets: Int32Array.of(0, bytes.length) },
      },
    ],
  };
  channel.offer({ payload, requiresLsn: 0, free: () => undefined });
  channel.end();
  return channel;
}

/**
 * A result stream available before its execution exists (a write waits for the writer slot
 * first): it forwards to the execution's stream once attached.
 */
class DeferredStream implements ResultStream {
  readonly columns: readonly OutputColumn[];
  #target: ResultStream | null = null;
  #failure: unknown = null;
  #failed = false;
  #cancelled: { reason: unknown } | null = null;
  #waiting: Array<() => void> = [];

  constructor(columns: readonly OutputColumn[]) {
    this.columns = columns;
  }

  /** Attaches the stream once; later calls are ignored. */
  attach(target: ResultStream): void {
    if (this.#target !== null || this.#failed) return;
    this.#target = target;
    if (this.#cancelled !== null) target.cancel(this.#cancelled.reason);
    this.#wake();
  }

  fail(error: unknown): void {
    if (this.#target !== null) return;
    this.#failed = true;
    this.#failure = error;
    this.#wake();
  }

  #wake(): void {
    for (const wake of this.#waiting.splice(0)) wake();
  }

  poll(): ResultFrame | null | undefined {
    if (this.#target !== null) return this.#target.poll();
    if (this.#failed) throw this.#failure;
    return undefined;
  }

  async next(): Promise<ResultFrame | null> {
    while (this.#target === null && !this.#failed)
      await new Promise<void>((resolve) => this.#waiting.push(resolve));
    if (this.#target === null) throw this.#failure;
    return this.#target.next();
  }

  cancel(reason?: unknown): void {
    if (this.#target !== null) this.#target.cancel(reason);
    else this.#cancelled ??= { reason };
  }
}

class Sequences implements SequenceSession {
  readonly #current = new Map<number, bigint>();
  #last: bigint | null = null;

  current(sequenceId: number): bigint {
    const value = this.#current.get(sequenceId);
    if (value === undefined) throw new SequenceStateError("currval", sequenceId);
    return value;
  }

  last(): bigint {
    if (this.#last === null) throw new SequenceStateError("lastval", null);
    return this.#last;
  }

  record(sequenceId: number, value: bigint): void {
    this.#current.set(sequenceId, value);
    this.#last = value;
  }
}

/** µs since 2000-01-01 (temporal.ts) for a device time in ms. */
const statementTimeOf = (ms: number): bigint =>
  BigInt(Math.trunc(ms)) * 1000n - PG_EPOCH_UNIX_MICROS;

function countsOf(
  summary: ExecutionSummary,
  lowered: LoweredModify,
  catalog: CatalogView,
): Record<string, number> {
  const table = lowered.writes[0];
  const name = table === undefined ? "rows" : (catalog.table(table)?.name ?? String(table));
  return { [name]: summary.rowCount };
}

export function composeEngine(options: ComposeOptions): OwnerEngine {
  return new ComposedEngine(options);
}

class ComposedEngine implements OwnerEngine {
  createReturnPool(capBytes: number): ReturnPool {
    return new ReturnPool(this.#options.governor, capBytes);
  }
  readonly #options: ComposeOptions & { readonly executor: Executor };
  readonly #now: () => number;
  readonly #plans: PlanCache<CachedPlan>;
  readonly #sessionPlans = new Set<PlanCache<PlannedStatement>>();
  readonly #statistics: PlanningStatistics;
  /** Compiled table rules for the committed catalog (rule-cache.ts). */
  readonly #rules: RuleCache;
  readonly #prepared = new WeakMap<PhysicalPlan, PreparedPlan>();
  readonly #commitListeners = new Set<(tables: readonly number[]) => void>();
  #queryId = 0;
  #sessionId = 0;

  constructor(options: ComposeOptions) {
    this.#options = { ...options, executor: options.executor ?? createExecutor() };
    this.#now = options.now ?? Date.now;
    this.#statistics = options.statistics ?? { table: () => null };
    this.#rules = new RuleCache(options.governor);
    this.#plans = new PlanCache(options.governor, options.planCacheSize, options.planCacheBytes);
  }

  /**
   * A table's row rules at `catalog`: the composition's override, or lowering's rule expressions
   * compiled by the executor. Cached by catalog identity (rule-cache.ts): the committed catalog's
   * here, a transaction's own schema in `local` (the session's), anything else not at all.
   */
  rules(catalog: CatalogView, tableId: number, local: RuleCache | null = null): TableRules {
    const override = this.#options.rules;
    if (override !== undefined) return override(catalog, tableId);
    const compile = () => {
      const table = catalog.table(tableId);
      if (table === undefined)
        throw new TypeError(`Table ${String(tableId)} is not in the catalog`);
      const expressions = this.#options.planner.lowerTableRules(table, catalog);
      return { rules: compileTableRules(table, catalog.version, expressions), expressions };
    };
    if (catalog === this.#options.writePath.catalog)
      return this.#rules.get(catalog, tableId, compile, true);
    if (local !== null) return local.get(catalog, tableId, compile);
    return compile().rules;
  }

  /** The committed catalog's rule cache (tests). */
  get ruleCache(): RuleCache {
    return this.#rules;
  }

  get durability() {
    return this.#options.writePath.durability;
  }

  get catalogVersion(): number {
    return this.#options.writePath.catalog.version;
  }

  get ledger(): OwnerLedger {
    return this.#options.writePath.ledger;
  }

  get writePath(): WritePath {
    return this.#options.writePath;
  }

  get options(): ComposeOptions {
    return this.#options;
  }

  now(): number {
    return this.#now();
  }

  nextQueryId(): number {
    return ++this.#queryId;
  }

  nextSessionId(): number {
    return ++this.#sessionId;
  }

  #context(catalog: CatalogView): PlannerContext {
    return { catalog, statistics: this.#statistics, costs: this.#options.costs ?? EXECUTOR_COSTS };
  }

  /** Lowers without caching: plans against uncommitted schema state (session-private). */
  lower(text: string, catalog: CatalogView): PlannedStatement {
    return toPlanned(this.#options.planner.lower(text, this.#context(catalog)));
  }

  #drifted(statement: LoweredStatement): boolean {
    return (
      "basis" in statement &&
      statement.basis.length > 0 &&
      statisticsDrifted(statement.basis, this.#statistics)
    );
  }

  /** Public lowering results/markers use normalized keys; raw constants use full text only. */
  plan(text: string, catalog: CatalogView = this.#options.writePath.catalog): PlannedStatement {
    const context = this.#context(catalog);
    const prefix = `${String(catalog.version)}\u0000${JSON.stringify(context.parameterTypes ?? [])}\u0000`;
    const parameterized = this.#options.planner.parameterize(text);
    if (parameterized !== null) {
      const key = `${prefix}normalized\u0000${parameterized.key}`;
      let cached = this.#plans.get(key);
      if (cached?.kind === "parameterized" && this.#drifted(cached.template.statement)) {
        this.#plans.delete(key);
        cached = undefined;
      }
      if (
        cached === undefined ||
        (cached.kind !== "parameterized" && cached.kind !== "notParameterizable")
      ) {
        const result = this.#options.planner.lowerParameterized(parameterized, context);
        cached =
          result.kind === "notParameterizable"
            ? result
            : {
                kind: "parameterized",
                template: result,
                planned: toPlanned(result.statement),
              };
        this.#plans.set(key, cached);
      }
      if (cached.kind === "parameterized")
        return {
          ...cached.planned,
          text,
          parameterization: { template: cached.template, text: parameterized },
        };
    }
    const key = `${prefix}text\u0000${text}`;
    const cached = this.#plans.get(key);
    if (
      cached !== undefined &&
      cached.kind !== "parameterized" &&
      cached.kind !== "notParameterizable" &&
      !this.#drifted(cached.lowered)
    )
      return cached;
    if (cached !== undefined) this.#plans.delete(key);
    const planned = toPlanned(this.#options.planner.lower(text, context));
    this.#plans.set(key, planned);
    return planned;
  }

  prepare(plan: PhysicalPlan): PreparedPlan {
    let prepared = this.#prepared.get(plan);
    if (prepared === undefined) {
      prepared = this.#options.executor.prepare(plan);
      this.#prepared.set(plan, prepared);
    }
    return prepared;
  }

  durable(lsn: number): Promise<void> | null {
    if (this.#options.writePath.durability === "relaxed") return null;
    return this.#options.writePath.durable(lsn);
  }

  openSession(clientId: string, generation: number, sessionId?: string): EngineSession {
    return new ComposedSession(
      this,
      generation,
      sessionId ?? `${clientId}:${String(this.nextSessionId())}`,
    );
  }

  privatePlans(): PlanCache<PlannedStatement> {
    const cache = new PlanCache<PlannedStatement>(
      this.#options.governor,
      this.#options.planCacheSize,
      this.#options.planCacheBytes,
    );
    this.#sessionPlans.add(cache);
    return cache;
  }

  closePrivatePlans(cache: PlanCache<PlannedStatement>): void {
    cache.close();
    this.#sessionPlans.delete(cache);
  }

  readSet(statement: PlannedStatement): readonly number[] {
    const lowered = statement.lowered;
    return lowered.kind === "query" || lowered.kind === "modify" ? lowered.reads : [];
  }

  onCommit(listener: (tables: readonly number[]) => void): () => void {
    this.#commitListeners.add(listener);
    return () => this.#commitListeners.delete(listener);
  }

  committed(tables: readonly number[]): void {
    for (const listener of this.#commitListeners) listener(tables);
  }

  admin(op: string): Promise<unknown> {
    if (op === "catalog")
      return Promise.resolve(structuredClone(this.#options.writePath.catalog.tables));
    if (op === "storageStats") return Promise.resolve({ ...this.#options.writePath.stats() });
    return Promise.reject(new TypeError(`Admin operation ${op} is not available yet`));
  }

  flushPending(): Promise<void> {
    return this.#options.writePath.flush();
  }

  async close(): Promise<void> {
    try {
      await this.#options.writePath.close();
    } finally {
      // The committed catalog's rules go with the engine, whether or not the write path closed.
      this.#rules.close();
      this.#plans.close();
      for (const cache of this.#sessionPlans) cache.close();
      this.#sessionPlans.clear();
    }
  }
}

function controlOf(lowered: LoweredStatement): PlannedStatement["control"] {
  if (lowered.kind !== "transactionControl") return null;
  if (lowered.action === "begin" || lowered.action === "commit" || lowered.action === "rollback")
    return lowered.action;
  return "other";
}

function toPlanned(lowered: LoweredStatement): PlannedStatement {
  const columns =
    lowered.kind === "query" || lowered.kind === "modify"
      ? lowered.columns
      : lowered.kind === "utility" && lowered.show !== null
        ? [lowered.show.column]
        : [];
  return {
    text: lowered.text,
    kind: lowered.kind,
    columns,
    parameters: lowered.parameters,
    controlsTransaction: lowered.controlsTransaction,
    control: controlOf(lowered),
    catalogVersion: lowered.catalogVersion,
    lowered,
    pointRead: lowered.kind === "query" && lowered.pointRead,
  };
}

/** A `Run` whose `reachedCommit` the composition sets as it crosses the boundary. */
interface MutableRun extends Run {
  done: Promise<RunOutcome>;
  reachedCommit: boolean;
}

function makeRun(results: ResultStream, commits: boolean): MutableRun {
  return {
    results,
    done: Promise.resolve({ command: "", rowCount: null, counts: null, lsn: null }),
    reachedCommit: false,
    commits,
  };
}

function failedRun(error: unknown): Run {
  const done = Promise.reject(error instanceof Error ? error : new Error(String(error)));
  done.catch(() => undefined);
  return { results: emptyStream(), done, reachedCommit: false, commits: false };
}

class ComposedSession implements EngineSession {
  readonly ledgerSession;
  readonly sequences = new Sequences();
  readonly #engine: ComposedEngine;
  #state: TransactionState = "idle";
  #modes: AppliedTransactionModes = { ...DEFAULT_TRANSACTION_MODES };
  #startedStatement = false;
  #tx: WriteTransaction | null = null;
  #readPin: Snapshot | null = null;
  #startedAtMs: number | null = null;
  #lastCommitLsn: number | null = null;
  /**
   * Savepoints, oldest first. Names may repeat: the newest shadows older ones, and RELEASE or
   * ROLLBACK TO name the newest (PostgreSQL). A read-only transaction's entries carry no write
   * path savepoint. Each keeps the transaction's counts at that point.
   */
  #savepoints: Array<{
    readonly name: string;
    readonly savepoint: Savepoint | null;
    readonly modes: AppliedTransactionModes;
    readonly counts: Readonly<Record<string, number>>;
  }> = [];
  /**
   * Plans against this transaction's own schema changes (uncommitted DDL): private to the
   * session, dropped when the transaction ends or rolls back to a savepoint.
   */
  #privatePlans: PlanCache<PlannedStatement> | null = null;
  /** Rules against this transaction's own schema changes, on the same lifetime as its plans. */
  #txRules: RuleCache | null = null;
  /** Affected rows per table of the open transaction, for its COMMIT's frame and outcome. */
  #txCounts: Record<string, number> = {};

  constructor(engine: ComposedEngine, generation: number, sessionId: string) {
    this.#engine = engine;
    this.ledgerSession = { sessionId, generation };
  }

  get transaction(): TransactionState {
    return this.#state;
  }

  get readOnly(): boolean {
    return this.#modes.readOnly;
  }

  get lastCommitLsn(): number | null {
    return this.#lastCommitLsn;
  }

  /** The catalog this session's statements see. */
  #catalog(): CatalogView {
    return this.#tx?.catalog ?? this.#readPin?.catalog ?? this.#writePath().catalog;
  }

  get catalogVersion(): number {
    return this.#catalog().version;
  }

  plan(text: string): PlannedStatement {
    const catalog = this.#catalog();
    // A transaction whose DDL moved its catalog past the committed one plans privately: another
    // transaction could reach the same version number with different contents.
    if (this.#tx === null || catalog.version === this.#writePath().catalog.version)
      return this.#engine.plan(text, catalog);
    const cache = (this.#privatePlans ??= this.#engine.privatePlans());
    const cached = cache.get(text);
    if (cached?.catalogVersion === catalog.version) return cached;
    if (cached !== undefined) cache.delete(text);
    const planned = this.#engine.lower(text, catalog);
    cache.set(text, planned);
    return planned;
  }

  // ---- helpers ----

  #writePath(): WritePath {
    return this.#engine.writePath;
  }

  #statementTime(): bigint {
    return statementTimeOf(this.#startedAtMs ?? this.#engine.now());
  }

  /** The read-only transaction's pin. */
  #transactionPin(): Snapshot {
    if (this.#readPin === null) throw new TypeError("No read-only transaction is open");
    return this.#readPin;
  }

  #pinOptions() {
    return this.#lastCommitLsn === null ? {} : { atLeastLsn: this.#lastCommitLsn };
  }

  #rules(catalog: () => CatalogView) {
    return (tableId: number) => this.#engine.rules(catalog(), tableId, this.#privateRules());
  }

  /** Rules compiled against this transaction's own schema: dropped with it (rule-cache.ts). */
  #privateRules(): RuleCache {
    this.#txRules ??= new RuleCache(this.#engine.options.governor);
    return this.#txRules;
  }

  #execute(
    plan: PhysicalPlan,
    params: readonly WireValue[],
    context: RunContext,
    target:
      | { readonly snapshot: Snapshot; readonly statement: null }
      | { readonly snapshot: null; readonly statement: Statement },
    returning: "atExecuted" | "atCommit",
  ): Execution {
    const prepared = this.#engine.prepare(plan);
    const options = this.#engine.options;
    return prepared.execute({
      parameters: params,
      statementTime: this.#statementTime(),
      reads: options.reads,
      governor: options.governor,
      spill: options.spill,
      queryId: this.#engine.nextQueryId(),
      sequences: options.writePath.sequences,
      sequenceSession: this.sequences,
      signal: context.signal,
      resultWindowBytes: context.windowBytes,
      resultFrameRows: context.frameRows,
      ...(context.outputBuffers ? { outputBuffers: context.outputBuffers } : {}),
      returning,
      ...target,
    });
  }

  /** Ends the open transaction's bookkeeping (after commit or rollback). */
  #endTransaction(): void {
    this.#tx = null;
    this.#readPin?.release();
    this.#readPin = null;
    this.#modes = { ...DEFAULT_TRANSACTION_MODES };
    this.#startedStatement = false;
    this.#startedAtMs = null;
    this.#savepoints = [];
    this.#privatePlans?.clear();
    this.#txRules?.clear();
    this.#txCounts = {};
  }

  #guard(statement: PlannedStatement): Error | null {
    const lowered = statement.lowered;
    const ends =
      lowered.kind === "transactionControl" &&
      (lowered.action === "rollback" ||
        lowered.action === "commit" ||
        (lowered.action === "rollbackTo" && this.#state === "failed"));
    if (this.#state === "failed" && !ends) return new InFailedTransactionError();
    if (this.#state === "timedOut" && !ends)
      return new TransactionIdleTimeoutError(this.#engine.options.transactionIdleTimeoutMs ?? 0);
    if (this.#modes.readOnly && (lowered.kind === "modify" || lowered.kind === "ddl"))
      return new ReadOnlyTransactionError(lowered.command);
    return null;
  }

  /** Inside an open transaction a failure aborts it (PostgreSQL 25P02 afterwards). */
  #abortOnFailure<T>(promise: Promise<T>): Promise<T> {
    return promise.catch((error: unknown) => {
      if (this.#state === "open") this.#state = "failed";
      throw error;
    });
  }

  // ---- EngineSession ----

  runNow(
    statement: PlannedStatement,
    params: readonly WireValue[],
    context: RunContext,
  ): SyncRun | { readonly continued: Run } | null {
    const lowered = statement.lowered;
    if (lowered.kind !== "query" || !lowered.pointRead) return null;
    const inReadOnly = this.#state === "open" && this.#modes.readOnly;
    if (this.#state !== "idle" && !inReadOnly) return null;
    if (inReadOnly) this.#startedStatement = true;
    const pin = inReadOnly ? this.#transactionPin() : this.#writePath().pin(this.#pinOptions());
    const releasePin = () => {
      if (!inReadOnly) pin.release();
    };
    let execution: Execution;
    try {
      execution = this.#execute(
        lowered.plan,
        boundParameters(statement, params),
        context,
        { snapshot: pin, statement: null },
        "atExecuted",
      );
    } catch (error) {
      releasePin();
      throw error;
    }
    const summary = execution.summary;
    if (
      execution.state === "executed" &&
      summary !== null &&
      this.#engine.durable(summary.requiresLsn) === null
    ) {
      const frames: ResultFrame[] = [];
      for (let frame = execution.results.poll(); frame !== null; frame = execution.results.poll()) {
        if (frame === undefined) throw new TypeError("An executed query left a frame pending");
        frames.push(frame);
      }
      releasePin();
      return {
        frames,
        columns: lowered.columns,
        outcome: { command: lowered.command, rowCount: summary.rowCount, counts: null, lsn: null },
      };
    }
    const run = makeRun(execution.results, false);
    const done = execution.executed.then((result) => ({
      command: lowered.command,
      rowCount: result.rowCount,
      counts: null,
      lsn: null,
    }));
    run.done = done.finally(releasePin);
    return { continued: run };
  }

  run(statement: PlannedStatement, params: readonly WireValue[], context: RunContext): Run {
    const run = this.#run(statement, params, context);
    // PostgreSQL: any error inside an open transaction aborts it (25P02 until it ends), whatever
    // raised it: a refusal, a missing savepoint, the statement itself.
    const done = this.#abortOnFailure(run.done);
    done.catch(() => undefined);
    return {
      results: run.results,
      done,
      commits: run.commits,
      // Read live: the composition sets it as the run crosses the commit boundary.
      get reachedCommit() {
        return run.reachedCommit;
      },
    };
  }

  #run(statement: PlannedStatement, params: readonly WireValue[], context: RunContext): Run {
    const refused = this.#guard(statement);
    if (refused !== null) return failedRun(refused);
    const lowered = statement.lowered;
    if (
      this.#state === "open" &&
      (lowered.kind === "query" || lowered.kind === "modify" || lowered.kind === "ddl")
    )
      this.#startedStatement = true;
    switch (lowered.kind) {
      case "query":
        return this.#query(lowered, boundParameters(statement, params), context);
      case "modify":
        return this.#modify(lowered, boundParameters(statement, params), context);
      case "ddl":
        return this.#ddl(lowered, context);
      case "transactionControl":
        return this.#control(lowered, context);
      case "utility":
        return {
          results:
            lowered.show === null
              ? emptyStream()
              : oneTextRow(
                  lowered.show.column,
                  lowered.show.column.name === "transaction_isolation"
                    ? this.#modes.isolation
                    : lowered.show.value,
                ),
          done: Promise.resolve({
            command: lowered.command,
            rowCount: lowered.show === null ? null : 1,
            counts: null,
            lsn: null,
          }),
          reachedCommit: false,
          commits: false,
        };
    }
  }

  #query(
    lowered: Extract<LoweredStatement, { kind: "query" }>,
    params: readonly WireValue[],
    context: RunContext,
  ): Run {
    if (this.#state === "open" && this.#tx !== null) {
      const tx = this.#tx;
      const statement = tx.statement({
        rules: this.#rules(() => statement.catalog),
        signal: context.signal,
      });
      let execution: Execution;
      try {
        execution = this.#execute(
          lowered.plan,
          params,
          context,
          { snapshot: statement.view, statement: null },
          "atExecuted",
        );
      } catch (error) {
        statement.rollback();
        this.#state = "failed";
        return failedRun(error);
      }
      const done = execution.executed.then(
        async (summary) => {
          await statement.end();
          return { command: lowered.command, rowCount: summary.rowCount, counts: null, lsn: null };
        },
        (error: unknown) => {
          statement.rollback();
          throw error;
        },
      );
      return {
        results: execution.results,
        done: this.#abortOnFailure(done),
        reachedCommit: false,
        commits: false,
      };
    }
    const inReadOnly = this.#state === "open" && this.#modes.readOnly;
    const pin = inReadOnly ? this.#transactionPin() : this.#writePath().pin(this.#pinOptions());
    const releasePin = () => {
      if (!inReadOnly) pin.release();
    };
    let execution: Execution;
    try {
      execution = this.#execute(
        lowered.plan,
        params,
        context,
        { snapshot: pin, statement: null },
        "atExecuted",
      );
    } catch (error) {
      releasePin();
      if (inReadOnly) this.#state = "failed";
      return failedRun(error);
    }
    const done = execution.executed
      .then((summary) => ({
        command: lowered.command,
        rowCount: summary.rowCount,
        counts: null,
        lsn: null,
      }))
      .finally(releasePin);
    return {
      results: execution.results,
      done: inReadOnly ? this.#abortOnFailure(done) : done,
      reachedCommit: false,
      commits: false,
    };
  }

  #modify(lowered: LoweredModify, params: readonly WireValue[], context: RunContext): Run {
    if (this.#state === "open" && this.#tx !== null) {
      const tx = this.#tx;
      const statement = tx.statement({
        rules: this.#rules(() => statement.catalog),
        signal: context.signal,
      });
      let execution: Execution;
      try {
        execution = this.#execute(
          lowered.plan,
          params,
          context,
          { snapshot: null, statement },
          "atExecuted",
        );
      } catch (error) {
        statement.rollback();
        this.#state = "failed";
        return failedRun(error);
      }
      const done = execution.executed.then((summary) => {
        const counts = countsOf(summary, lowered, statement.catalog);
        for (const [table, rows] of Object.entries(counts))
          this.#txCounts[table] = (this.#txCounts[table] ?? 0) + rows;
        return { command: lowered.command, rowCount: summary.rowCount, counts, lsn: null };
      });
      return {
        results: execution.results,
        done: this.#abortOnFailure(done),
        reachedCommit: false,
        commits: false,
      };
    }
    return this.#autocommit(
      lowered.columns,
      (tx, stream) => {
        const statement = tx.statement({
          rules: this.#rules(() => statement.catalog),
          signal: context.signal,
        });
        let execution: Execution;
        try {
          execution = this.#execute(
            lowered.plan,
            params,
            context,
            { snapshot: null, statement },
            "atCommit",
          );
        } catch (error) {
          statement.rollback();
          throw error;
        }
        stream.attach(execution.results);
        return {
          executed: execution.executed.then((summary) => ({
            command: lowered.command,
            rowCount: summary.rowCount,
            counts: countsOf(summary, lowered, statement.catalog),
            tables: lowered.writes,
          })),
          acknowledge: () => {
            execution.acknowledgeCommit();
          },
          abort: (reason: unknown) => {
            execution.cancel(reason);
          },
        };
      },
      context,
    );
  }

  /**
   * An autocommit write: writer slot, the statement, then the commit boundary. `body` starts the
   * statement on the transaction and reports its completion; `acknowledge` lets RETURNING flow.
   */
  #autocommit(
    columns: readonly WireColumnMeta[],
    body: (
      tx: WriteTransaction,
      stream: DeferredStream,
    ) => {
      readonly executed: Promise<{
        command: string;
        rowCount: number | null;
        counts: Record<string, number> | null;
        tables: readonly number[];
      }>;
      readonly acknowledge: () => void;
      readonly abort: (reason: unknown) => void;
    },
    context: RunContext,
  ): Run {
    const stream = new DeferredStream(toOutputColumns(columns));
    const run = makeRun(stream, true);
    const writePath = this.#writePath();
    run.done = (async (): Promise<RunOutcome> => {
      let tx: WriteTransaction;
      try {
        tx = await writePath.begin({ signal: context.signal });
      } catch (error) {
        stream.fail(error);
        throw error;
      }
      let started: ReturnType<typeof body> | null = null;
      try {
        started = body(tx, stream);
        const executed = await started.executed;
        context.signal.throwIfAborted();
        run.reachedCommit = true;
        const receipt = await tx.commit(
          context.request === null ? null : { ...context.request, counts: executed.counts ?? {} },
        );
        if (writePath.durability === "strict") await receipt.durable;
        started.acknowledge();
        // A body without rows (DDL) never attached a stream: the result is empty.
        stream.attach(emptyStream());
        if (receipt.wrote) {
          this.#lastCommitLsn = receipt.lsn;
          this.#engine.committed(executed.tables);
        }
        return {
          command: executed.command,
          rowCount: executed.rowCount,
          counts: executed.counts,
          lsn: receipt.wrote ? receipt.lsn : null,
        };
      } catch (error) {
        // The run's stream ends with the error either way: a spool behind a failed commit (or a
        // failed durability wait past the boundary) is discarded, never delivered.
        started?.abort(error);
        if (!run.reachedCommit) tx.rollback();
        stream.fail(error);
        throw error;
      }
    })();
    run.done.catch(() => undefined);
    return run;
  }

  #ddl(lowered: Extract<LoweredStatement, { kind: "ddl" }>, context: RunContext): Run {
    const apply = async (tx: WriteTransaction): Promise<void> => {
      const statement = tx.statement({
        rules: this.#rules(() => statement.catalog),
        signal: context.signal,
      });
      try {
        if (lowered.change !== null) await statement.changeCatalog(lowered.change);
        await statement.end();
      } catch (error) {
        statement.rollback();
        throw error;
      }
      if (lowered.followUp !== null) {
        const plannedFollowUp = this.plan(lowered.followUp);
        const followUp = plannedFollowUp.lowered;
        if (followUp.kind !== "modify") throw new TypeError("A DDL follow-up must be a write");
        const next = tx.statement({
          rules: this.#rules(() => next.catalog),
          signal: context.signal,
        });
        const execution = this.#execute(
          followUp.plan,
          boundParameters(plannedFollowUp, []),
          context,
          { snapshot: null, statement: next },
          "atExecuted",
        );
        execution.results.cancel();
        await execution.executed;
      }
    };
    const outcome = { command: lowered.command, rowCount: null, counts: null, lsn: null };
    if (this.#state === "open" && this.#tx !== null) {
      const done = this.#abortOnFailure(apply(this.#tx).then(() => outcome));
      return { results: emptyStream(), done, reachedCommit: false, commits: false };
    }
    return this.#autocommit(
      [],
      (tx) => ({
        executed: apply(tx).then(() => ({ ...outcome, tables: [] })),
        acknowledge: () => undefined,
        abort: () => undefined,
      }),
      context,
    );
  }

  #control(
    lowered: Extract<LoweredStatement, { kind: "transactionControl" }>,
    context: RunContext,
  ): Run {
    const done = (command: string): Run => ({
      results: emptyStream(),
      done: Promise.resolve({ command, rowCount: null, counts: null, lsn: null }),
      reachedCommit: false,
      commits: false,
    });
    switch (lowered.action) {
      case "begin": {
        if (this.#state !== "idle") return done(lowered.command); // PostgreSQL: a warning only
        const begun = this.begin(lowered.modes.readOnly ?? false, context.signal).then(() => {
          this.#modes = {
            readOnly: this.#modes.readOnly,
            isolation: lowered.modes.isolation ?? DEFAULT_TRANSACTION_MODES.isolation,
            deferrable: lowered.modes.deferrable ?? DEFAULT_TRANSACTION_MODES.deferrable,
          };
          return {
            command: lowered.command,
            rowCount: null,
            counts: null,
            lsn: null,
          };
        });
        return { results: emptyStream(), done: begun, reachedCommit: false, commits: false };
      }
      case "commit":
        return this.commit(context);
      case "rollback":
        this.rollback();
        return done("ROLLBACK");
      case "savepoint": {
        if (this.#state === "idle") return failedRun(new NoActiveTransactionError(lowered.command));
        this.#savepoints.push({
          name: lowered.savepoint ?? "",
          savepoint: this.#tx?.savepoint() ?? null,
          modes: { ...this.#modes },
          counts: { ...this.#txCounts },
        });
        return done(lowered.command);
      }
      case "rollbackTo":
      case "release": {
        if (this.#state === "idle") return failedRun(new NoActiveTransactionError(lowered.command));
        const name = lowered.savepoint ?? "";
        let index = this.#savepoints.length - 1;
        while (index >= 0 && this.#savepoints[index]?.name !== name) index--;
        const entry = this.#savepoints[index];
        if (entry === undefined) return failedRun(new SavepointNotFoundError(name));
        if (lowered.action === "release") {
          // Releases it and every savepoint after it.
          if (entry.savepoint !== null) this.#tx?.release(entry.savepoint);
          this.#savepoints.length = index;
        } else {
          // Undoes everything after it; it stays, later ones go; counts as they were then.
          if (entry.savepoint !== null) this.#tx?.rollbackTo(entry.savepoint);
          this.#savepoints.length = index + 1;
          this.#txCounts = { ...entry.counts };
          this.#privatePlans?.clear();
          this.#txRules?.clear();
          if (this.#state === "failed") this.#state = "open";
          const restored = this.#applyModes(entry.modes, context.signal).then(() => ({
            command: lowered.command,
            rowCount: null,
            counts: null,
            lsn: null,
          }));
          return { results: emptyStream(), done: restored, reachedCommit: false, commits: false };
        }
        return done(lowered.command);
      }
      case "setTransaction": {
        // Outside BEGIN this has no effect (PostgreSQL warns; this wire has no notices).
        if (this.#state === "idle") return done(lowered.command);
        if (this.#startedStatement) return failedRun(new TransactionModeError());
        const changed = this.#applyModes(lowered.modes, context.signal).then(() => ({
          command: lowered.command,
          rowCount: null,
          counts: null,
          lsn: null,
        }));
        return { results: emptyStream(), done: changed, reachedCommit: false, commits: false };
      }
    }
  }

  /** Switch resources before publishing modes, including restoration after ROLLBACK TO. */
  async #applyModes(modes: TransactionModes, signal: AbortSignal): Promise<void> {
    const readOnly = modes.readOnly ?? this.#modes.readOnly;
    if (readOnly !== this.#modes.readOnly) {
      if (readOnly) {
        const pin = this.#writePath().pin(this.#pinOptions());
        this.#tx?.rollback();
        this.#tx = null;
        this.#readPin = pin;
        this.#savepoints = this.#savepoints.map((entry) => ({ ...entry, savepoint: null }));
      } else {
        const tx = await this.#writePath().begin({ signal });
        this.#tx = tx;
        this.#readPin?.release();
        this.#readPin = null;
        this.#savepoints = this.#savepoints.map((entry) => ({
          ...entry,
          savepoint: tx.savepoint(),
        }));
      }
    }
    this.#modes = {
      readOnly,
      isolation: modes.isolation ?? this.#modes.isolation,
      // Without SERIALIZABLE (refused by lowering), DEFERRABLE has no execution effect.
      deferrable: modes.deferrable ?? this.#modes.deferrable,
    };
  }

  write(
    input: {
      readonly table: string;
      readonly op: "insert" | "upsert" | "update" | "delete";
      readonly names: readonly string[];
      readonly columns: readonly WireInputColumn[];
      readonly rowCount: number;
      readonly ignoreConflicts: boolean;
      readonly returning: readonly string[] | null;
    },
    context: RunContext,
  ): Run {
    void input;
    void context;
    return failedRun(
      new TypeError(
        "Batch writes need the planner's lowerWrite and the executor's input relation (IR request 3); not wired yet",
      ),
    );
  }

  async begin(readOnly: boolean, signal: AbortSignal): Promise<void> {
    if (this.#state !== "idle")
      throw new TypeError("A transaction is already open in this session");
    if (readOnly) {
      this.#readPin = this.#writePath().pin(this.#pinOptions());
    } else {
      this.#tx = await this.#writePath().begin({ signal });
    }
    this.#modes = { ...DEFAULT_TRANSACTION_MODES, readOnly };
    this.#startedStatement = false;
    this.#startedAtMs = this.#engine.now();
    this.#state = "open";
  }

  commit(context: RunContext): Run {
    const state = this.#state;
    if (state === "idle")
      return {
        results: emptyStream(),
        done: Promise.resolve({ command: "COMMIT", rowCount: null, counts: null, lsn: null }),
        reachedCommit: false,
        commits: false,
      };
    if (state === "timedOut") {
      this.#state = "idle";
      return failedRun(
        new TransactionIdleTimeoutError(this.#engine.options.transactionIdleTimeoutMs ?? 0),
      );
    }
    if (state === "failed") {
      this.rollback();
      return {
        results: emptyStream(),
        done: Promise.resolve({ command: "ROLLBACK", rowCount: null, counts: null, lsn: null }),
        reachedCommit: false,
        commits: false,
      };
    }
    const tx = this.#tx;
    if (tx === null) {
      this.#endTransaction();
      this.#state = "idle";
      return {
        results: emptyStream(),
        done: Promise.resolve({ command: "COMMIT", rowCount: null, counts: null, lsn: null }),
        reachedCommit: false,
        commits: false,
      };
    }
    const run = makeRun(emptyStream(), true);
    const writePath = this.#writePath();
    run.reachedCommit = true;
    run.done = (async (): Promise<RunOutcome> => {
      try {
        const counts = this.#txCounts;
        const receipt = await tx.commit(
          context.request === null ? null : { ...context.request, counts },
        );
        if (writePath.durability === "strict") await receipt.durable;
        if (receipt.wrote) this.#lastCommitLsn = receipt.lsn;
        return {
          command: "COMMIT",
          rowCount: null,
          counts,
          lsn: receipt.wrote ? receipt.lsn : null,
        };
      } finally {
        this.#endTransaction();
        this.#state = "idle";
      }
    })();
    run.done.catch(() => undefined);
    return run;
  }

  rollback(): void {
    this.#tx?.rollback();
    this.#endTransaction();
    this.#state = "idle";
  }

  expire(): void {
    if (this.#state !== "open" && this.#state !== "failed") return;
    this.#tx?.rollback();
    this.#endTransaction();
    this.#state = "timedOut";
  }

  fail(): void {
    if (this.#state === "open") this.#state = "failed";
  }

  close(): void {
    try {
      this.rollback();
    } finally {
      if (this.#privatePlans !== null) this.#engine.closePrivatePlans(this.#privatePlans);
      this.#txRules?.close();
      this.#txRules = null;
    }
  }
}
