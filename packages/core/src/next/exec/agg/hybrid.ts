/** PostgreSQL-style hybrid hash aggregation. A pass owns complete groups. Once it
 * freezes, resident groups keep transitioning; new groups spill in source order.
 * Law: REL_17_STABLE nodeAgg.c, "Spilling To Disk" (lines 196-226).
 */
import type { MaybePromise } from "../../access/key-index.js";
import type { Grant } from "../../memory/governor.js";
import type { Rel } from "../../ir/ir.js";
import type { SpillService } from "../../spill/contract.js";
import { BATCH_ROWS, type Batch, type Selection } from "../../vector/vector.js";
import type { CompileContext } from "../kernels/compile.js";
import { OutputArena, IDENTITY } from "../kernels/vectors.js";
import {
  afterYield,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type PushResult,
} from "../pipeline.js";
import { Slice, runSliced, type SlicedJob } from "../slices.js";
import { readValue } from "../../page/read-bounds.js";
import { Charge, Pool, SpillIo } from "../spill-pool.js";
import { PageWriter, SpillFiles } from "../spill-pages.js";
import { AggregateOperator, type AggregateOptions } from "./aggregate.js";
import { SortGrants } from "./sort-grants.js";
import { HashWorkspace, HashHeadroomError } from "./hash-workspace.js";
import { GroupCheckpoint } from "./checkpoint.js";
import { ParameterCheckpoint } from "../parameter-checkpoint.js";
import { ColumnStore } from "../column-store.js";
import { FrozenGroups } from "./frozen-groups.js";
import { HashPartitions } from "./hash-partitions.js";
import { TapeDirectory, HashTape } from "./hash-tape.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;
export interface HybridContext {
  readonly grant: Grant;
  readonly extras: readonly Grant[];
  readonly hashMinimum: number;
  readonly ioMinimum: number;
  readonly spill: SpillService;
  readonly queryId: number;
  readonly signal?: AbortSignal;
  /** The recipe retires the producer graph before admitting this continuation. */
  readonly handoff?: (phase: HybridPhase) => MaybePromise<void>;
  readonly resume?: HybridPhase;
  /** Only COUNT and float transitions over bounded fixed-width keys; no variable state. */
  readonly fixed?: boolean;
  /** Generated pressure fixtures force collapse at a smaller directory size. */
  readonly pendingPasses?: number;
  readonly progressBytes?: number;
}

const PASS_SLOTS = 64;
const PASS_METADATA_BYTES = PASS_SLOTS * 512;

interface Pass {
  readonly run: HashTape;
  readonly level: number;
}
/** Only native files and immutable sizing metadata survive between admissions. No
 * input vector, old compile arena or native grant is retained by this record. */
export interface HybridPhase {
  readonly files: SpillFiles;
  readonly directory: TapeDirectory;
  readonly checkpoint: GroupCheckpoint;
  readonly parameters: ParameterCheckpoint;
  readonly suffix: HashTape;
  readonly partitions: readonly HashTape[] | null;
  readonly pending: Pass[];
  readonly output: HashTape | null;
  readonly resident: number;
  readonly level: number;
  readonly frozen: boolean;
  readonly minimum: number;
  readonly progressBytes: number;
  readonly profile: HybridAggregateOperator["profile"];
}
export async function closeHybridPhase(phase: HybridPhase): Promise<void> {
  await phase.files.closeAll();
}

export class HybridAggregateOperator implements Operator {
  readonly #grants: SortGrants;
  readonly #hashGrant: Grant;
  readonly #ioPool: Pool;
  readonly #files: SpillFiles;
  readonly #pages: PageWriter;
  readonly #io: SpillIo;
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  readonly #residentRows: Uint16Array;
  readonly #newRows: Uint16Array;
  readonly #ids: Int32Array;
  readonly #controller = new AbortController();
  readonly #parentAbort: () => void;
  #workspace: HashWorkspace;
  #arena: OutputArena;
  #hash: AggregateOperator;
  #frozen: FrozenGroups | null = null;
  #resident = 0;
  #parts: HashPartitions | null = null;
  #level = 0;
  #directory: TapeDirectory | null = null;
  #pending: Promise<void> | null = null;
  #cleanup: Promise<void> | null = null;
  #finishWork: Promise<void> | null = null;
  #finished: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  #checkpoint: GroupCheckpoint | null = null;
  #suffix: HashTape | null = null;
  #output: HashTape | null = null;
  #staging: ColumnStore | null = null;
  #phaseMinimum = 0;
  #evicted = false;
  #transferred = false;
  #restore: Promise<void> | null = null;
  #savedParameters: ParameterCheckpoint | null = null;
  readonly #stack: Pass[] = [];
  readonly profile = {
    spilledBytes: 0,
    passes: 0,
    evictions: 0,
    maxLevel: 0,
    residentRows: 0,
    spilledRows: 0,
    collapsed: 0,
  };
  constructor(
    readonly rel: AggregateRel,
    readonly context: CompileContext,
    readonly shared: HybridContext,
    readonly downstream: Operator,
    readonly options: AggregateOptions = {},
  ) {
    this.#grants = new SortGrants(shared.grant, shared.extras);
    this.#hashGrant = this.#grants.fork(shared.hashMinimum, 0);
    const ioGrant = this.#grants.fork(shared.ioMinimum, 1);
    this.#ioPool = new Pool(new Charge(ioGrant), shared.ioMinimum);
    this.#files =
      shared.resume?.files ??
      new SpillFiles(shared.spill, shared.queryId, {
        allocate: () => false,
        free: () => {
          throw new Error("Unbuffered tape freed a batching buffer");
        },
      });
    this.#pages = new PageWriter(this.#ioPool);
    this.#io = new SpillIo(
      this.#ioPool,
      this.#files,
      this.#pages,
      { grant: ioGrant, queryId: shared.queryId, signal: this.#controller.signal },
      () => this.#check(),
      () => this.#pace(),
    );
    if (!this.#ioPool.allocate(8 * BATCH_ROWS + PASS_METADATA_BYTES))
      throw new Error("Hybrid row routing was not admitted");
    this.#residentRows = new Uint16Array(BATCH_ROWS);
    this.#newRows = new Uint16Array(BATCH_ROWS);
    this.#ids = new Int32Array(BATCH_ROWS);
    this.#workspace = new HashWorkspace(this.#hashGrant, shared.hashMinimum, () =>
      this.#grants.stateCapacity(this.#hashGrant),
    );
    this.#arena = new OutputArena(this.#workspace, BATCH_ROWS);
    try {
      this.#hash = this.#makeHash();
    } catch (error) {
      this.#arena.close();
      this.#workspace.close();
      this.#pages.close();
      this.#ioPool.free(8 * BATCH_ROWS + PASS_METADATA_BYTES);
      this.#ioPool.close();
      this.#grants.close();
      throw error;
    }
    if (shared.resume) {
      this.#directory = shared.resume.directory;
      this.#directory.attach(this.#io, this.#ioPool);
      this.#output = shared.resume.output;
      this.#level = shared.resume.level;
      this.#stack.push(...shared.resume.pending);
      Object.assign(this.profile, shared.resume.profile);
      this.#restore = this.#resume(shared.resume);
    }
    this.#parentAbort = () => this.abort(shared.signal?.reason);
    shared.signal?.addEventListener("abort", this.#parentAbort, { once: true });
    if (shared.signal?.aborted) this.#parentAbort();
  }
  #makeHash(): AggregateOperator {
    const sink: Operator = {
      push: (batch) => {
        if (this.#output === null) return this.downstream.push(batch);
        this.#pending = this.#write(this.#output, batch);
        return "blocked";
      },
      ready: () =>
        this.#output === null ? this.downstream.ready() : (this.#pending ?? Promise.resolve()),
      finish: () => undefined,
      abort: () => undefined,
    };
    return new AggregateOperator(
      this.rel,
      { ...this.context, arena: this.#arena },
      this.#workspace,
      sink,
      {
        ...this.options,
        signal: this.#controller.signal,
        ...(this.shared.fixed ? { outputCharge: this.#ioPool } : {}),
        typedLayout: false,
        packedAverages: false,
        packedIntegers: false,
      },
    );
  }
  #dir(): TapeDirectory {
    return (this.#directory ??= new TapeDirectory(this.#io, this.#ioPool));
  }
  async #write(tape: HashTape, batch: Batch): Promise<void> {
    this.#staging ??= new ColumnStore(
      batch.columns.map((c) => c.type),
      this.#ioPool,
      {
        chunkRows: 256,
        targetBytes: 32768,
        varlenBytes: 1024,
      },
    );
    await this.#io.stage(
      this.#staging,
      tape,
      { run: 0, partition: 0 },
      batch.columns,
      batch.selection?.rows ?? null,
      0,
      batch.selection?.count ?? batch.length,
    );
    await this.#io.flush(this.#staging, tape, { run: 0, partition: 0 });
    // Checkpoint input and completed output can have different schemas.
    this.#staging.close();
    this.#staging = null;
  }
  async #evict(batch: Batch | null, minimum: number): Promise<void> {
    if (!this.shared.handoff) throw new Error("Variable hash state requires a phase handoff");
    this.#checkpoint = new GroupCheckpoint(this.#dir(), this.rel.groupBy.length);
    await this.#checkpoint.save(
      this.#hash,
      this.#resident || (this.rel.groupBy.length === 0 ? 1 : 0),
      this.#ioPool,
    );
    this.profile.evictions++;
    this.#phaseMinimum = Math.max(this.shared.hashMinimum + 4096, minimum);
    this.#suffix = new HashTape(this.#dir());
    this.#evicted = true;
    this.#frozen?.close();
    this.#frozen = null;
    this.#hash.abort(undefined);
    this.#arena.close();
    this.#workspace.close();
    if (batch) await this.#write(this.#suffix, batch);
  }
  async #resume(phase: HybridPhase): Promise<void> {
    this.#resident = phase.resident;
    const continuation = { started: false };
    try {
      await phase.checkpoint.resume(this.#hash, async () => {
        continuation.started = true;
        if (phase.frozen && this.#resident > 0)
          await this.#freeze(this.#resident, phase.partitions ?? undefined);
        for await (const batch of phase.suffix.read(this.context.input)) {
          const work = runSliced(this.#push(batch), this.#slice, () => this.#check());
          if (work instanceof Promise) await work;
        }
      });
    } catch (error) {
      if (continuation.started || !(error instanceof HashHeadroomError)) throw error;
      // Restoration has applied no continuation when an underestimated layout fails.
      // The immutable prefix can be re-read after retiring this incomplete workspace.
      if (this.#evicted) throw error;
      this.#checkpoint = phase.checkpoint;
      this.#suffix = phase.suffix;
      // Admit the actual denied allocation, rather than doubling an already large
      // minimum into an artificial over-cap refusal. No suffix transition has run.
      this.#phaseMinimum = Math.max(this.shared.hashMinimum + 1, error.requested);
      this.#evicted = true;
      this.#savedParameters = phase.parameters;
      this.#parts?.close();
      this.#parts = null;
      this.#hash.abort(undefined);
      this.#arena.close();
      this.#workspace.close();
      return;
    }
    await phase.checkpoint.close();
    await phase.suffix.release();
    await phase.parameters.close();
  }
  #pace(): Promise<void> | null {
    this.#check();
    return this.#slice.due()
      ? yieldTask().then(() => {
          afterYield(this.#slice.clock);
          this.#check();
        })
      : null;
  }
  #hasEvicted(): boolean {
    return this.#evicted;
  }
  #check(): void {
    if (this.#aborted) throw this.#reason;
    this.shared.signal?.throwIfAborted();
  }
  push(batch: Batch): PushResult {
    this.#check();
    const work = runSliced(this.#push(batch), this.#slice, () => this.#check());
    if (work instanceof Promise) {
      this.#pending = work;
      return "blocked";
    }
    return "ready";
  }
  ready(): Promise<void> {
    return this.#restore ?? this.#pending ?? Promise.resolve();
  }
  *#push(batch: Batch): SlicedJob<void> {
    this.#check();
    if ((batch.selection?.count ?? batch.length) === 0) return;
    if (this.#evicted) {
      if (!this.#suffix) throw new Error("Missing evicted input tape");
      yield* readValue(this.#write(this.#suffix, batch));
      return;
    }
    if (this.#frozen !== null) {
      yield* readValue(this.#routeFrozen(batch));
      return;
    }
    const previous = this.#resident;
    const keys = this.#workspace.used + this.#hash.keyGrowthBytes(batch);
    if (!(yield* readValue(this.#workspace.prepare(keys, this.#controller.signal)))) {
      if (previous === 0) {
        yield* this.#split(batch);
        return;
      }
      yield* readValue(this.#freeze(previous));
      yield* readValue(this.#routeFrozen(batch));
      return;
    }
    const ids = this.#hash.assignGroups(batch),
      groups = this.#hash.groupCount;
    const state =
      this.#workspace.used +
      this.#hash.stateGrowthBytes(batch, groups, this.shared.fixed ? 0 : 4096);
    if (!(yield* readValue(this.#workspace.prepare(state, this.#controller.signal)))) {
      if (this.rel.groupBy.length === 0) {
        yield* readValue(this.#evict(batch, state));
        return;
      }
      if (previous === 0) {
        this.#resetEmpty();
        yield* this.#split(batch);
        return;
      }
      yield* readValue(this.#freeze(previous));
      yield* readValue(this.#routeFrozen(batch));
      return;
    }
    this.#hash.pushPrepared(batch, ids, groups);
    this.#resident = groups;
    this.profile.residentRows += batch.selection?.count ?? batch.length;
  }
  *#split(batch: Batch): SlicedJob<void> {
    const count = batch.selection?.count ?? batch.length;
    if (count <= 1) {
      yield* readValue(
        this.#evict(
          batch,
          this.#workspace.used +
            this.#hash.keyGrowthBytes(batch) +
            this.#hash.stateGrowthBytes(batch, this.#hash.groupCount + 1),
        ),
      );
      return;
    }
    const rows = batch.selection?.rows ?? IDENTITY,
      middle = Math.floor(count / 2);
    yield* this.#push({ ...batch, selection: { rows: rows.subarray(0, middle), count: middle } });
    yield* this.#push({
      ...batch,
      selection: { rows: rows.subarray(middle, count), count: count - middle },
    });
  }
  #resetEmpty(): void {
    if (this.#resident !== 0) throw new Error("Cannot retry transitioned groups");
    this.#hash.abort(undefined);
    this.#arena.close();
    this.#workspace.close();
    this.#workspace = new HashWorkspace(
      this.#hashGrant,
      this.#grants.stateCapacity(this.#hashGrant),
      () => this.#grants.stateCapacity(this.#hashGrant),
    );
    this.#arena = new OutputArena(this.#workspace, BATCH_ROWS);
    this.#hash = this.#makeHash();
  }
  async #freeze(groups: number, runs?: readonly HashTape[]): Promise<void> {
    const table = this.#hash.groupTable;
    if (!table) throw new Error("Variable scalar state needs eviction, not new-group spilling");
    // A freeze stops admission permanently for this pass, including key IDs assigned
    // in preparation but never transitioned. They are not resident groups.
    this.#frozen = new FrozenGroups(this.#ioPool, groups);
    this.#frozen.build(table, () => this.#check());
    this.#hash.sealGroups(groups);
    this.#output ??= new HashTape(this.#dir());
    this.#parts = new HashPartitions(
      this.context.input,
      this.rel.groupBy,
      this.#ioPool,
      this.#io,
      this.#level,
      this.#dir(),
      runs,
    );
  }
  async #routeFrozen(batch: Batch): Promise<void> {
    const frozen = this.#frozen,
      parts = this.#parts;
    if (!frozen || !parts) throw new Error("No frozen pass");
    const grant = this.#io.context.grant;
    await this.#ioPool.prepareStep(
      this.#grants.stepCapacity(grant),
      grant,
      this.#controller.signal,
    );
    const unpin = this.#ioPool.pinStep();
    try {
      frozen.encode(
        batch,
        this.rel.groupBy.map((c) => {
          const v = batch.columns[c];
          if (!v) throw new Error("Missing key");
          return v;
        }),
      );
      let resident = 0,
        spilled = 0;
      const count = batch.selection?.count ?? batch.length;
      for (let k = 0; k < count; k++) {
        const row = batch.selection?.rows[k] ?? k,
          id = frozen.find(k);
        if (id < 0) this.#newRows[spilled++] = row;
        else {
          this.#ids[row] = id;
          this.#residentRows[resident++] = row;
        }
      }
      // IDs and row ordinals now own the result; no encoded lookup key survives
      // into partition-key resize or row staging on this same I/O account.
      frozen.releaseBorrow();
      if (resident > 0) {
        const selection: Selection = { rows: this.#residentRows, count: resident },
          selected = { ...batch, selection };
        if (
          !(await this.#workspace.prepare(
            this.#workspace.used +
              this.#hash.stateGrowthBytes(selected, this.#resident, this.shared.fixed ? 0 : 4096),
            this.#controller.signal,
          ))
        ) {
          await this.#evict(
            batch,
            this.#workspace.used +
              this.#hash.stateGrowthBytes(selected, this.#resident, this.shared.fixed ? 0 : 4096),
          );
          return;
        }
        this.#hash.pushPrepared(selected, this.#ids, this.#resident);
        this.profile.residentRows += resident;
      }
      if (spilled > 0) {
        await parts.write({ ...batch, selection: { rows: this.#newRows, count: spilled } });
        this.profile.spilledRows += spilled;
        this.profile.spilledBytes = this.#files.written;
      }
    } finally {
      frozen.releaseBorrow();
      unpin();
    }
  }
  async #queue(runs: readonly HashTape[], level: number): Promise<void> {
    const slots = this.shared.pendingPasses ?? PASS_SLOTS;
    if (!Number.isSafeInteger(slots) || slots < 1 || slots > PASS_SLOTS)
      throw new Error("Invalid hash job directory size");
    if (runs.length + this.#stack.length <= slots) {
      for (const run of runs) this.#stack.push({ run, level });
      return;
    }
    // Bound the resident job directory even for adversarial hash collisions. Each
    // group is in exactly one child, so concatenating children preserves its order.
    // At least this pass's resident groups have finished; repeated collapsed passes
    // therefore remove groups instead of retrying the same state indefinitely.
    this.profile.collapsed++;
    const merged = new HashTape(this.#dir());
    for (const run of runs) {
      for await (const batch of run.read(this.context.input)) await this.#write(merged, batch);
      await run.release();
    }
    this.#stack.push({ run: merged, level });
  }
  async #endPass(): Promise<HashTape[]> {
    const peak = this.#workspace.used + this.#hash.outputGrowthBytes();
    if (!this.shared.fixed && !(await this.#workspace.prepare(peak, this.#controller.signal))) {
      await this.#evict(null, peak);
      return [];
    }
    await this.#hash.finish();
    this.#frozen?.close();
    this.#frozen = null;
    this.#hash.abort(undefined);
    this.#arena.close();
    this.#workspace.close();
    const runs = this.#parts?.runs.filter((run) => run.rows > 0) ?? [];
    this.#parts?.close();
    this.#parts = null;
    return runs;
  }
  #newPass(level: number): void {
    this.#level = level;
    this.profile.maxLevel = Math.max(this.profile.maxLevel, level);
    this.#resident = 0;
    this.#workspace = new HashWorkspace(
      this.#hashGrant,
      this.#grants.stateCapacity(this.#hashGrant),
      () => this.#grants.stateCapacity(this.#hashGrant),
    );
    this.#arena = new OutputArena(this.#workspace, BATCH_ROWS);
    this.#hash = this.#makeHash();
    this.profile.passes++;
  }
  finish(): Promise<void> {
    if (this.#finished !== null) return this.#finished;
    this.#finishWork = this.#finishPasses();
    return (this.#finished = this.#finishWork.then(
      () => this.#close(),
      async (error: unknown) => {
        this.abort(error);
        await this.#close().catch(() => undefined);
        throw error;
      },
    ));
  }
  async #finishPasses(): Promise<void> {
    await this.#restore;
    this.#restore = null;
    await this.#pending;
    if (!this.#evicted) {
      const runs = await this.#endPass();
      await this.#queue(runs, this.#level + 1);
    }
    while (!this.#evicted && this.#stack.length > 0) {
      this.#check();
      const item = this.#stack.pop();
      if (!item) break;
      this.#newPass(item.level);
      for await (const batch of item.run.read(this.context.input)) {
        if (this.push(batch) === "blocked") await this.ready();
      }
      if (!this.#hasEvicted()) {
        const children = await this.#endPass();
        await this.#queue(children, item.level + 1);
      }
      await item.run.release();
    }
    if (this.#evicted) {
      if (!this.#checkpoint || !this.#suffix || !this.shared.handoff)
        throw new Error("Incomplete aggregate checkpoint");
      const parameters = this.#savedParameters ?? new ParameterCheckpoint(this.#dir());
      if (!this.#savedParameters) await parameters.save(this.context.params.slots);
      this.profile.spilledBytes = this.#files.written;
      const phase: HybridPhase = {
        files: this.#files,
        directory: this.#dir(),
        checkpoint: this.#checkpoint,
        parameters,
        suffix: this.#suffix,
        partitions: this.#parts?.runs ?? this.shared.resume?.partitions ?? null,
        pending: this.#stack.splice(0),
        output: this.#output,
        resident: this.#resident || (this.rel.groupBy.length === 0 ? 1 : 0),
        level: this.#level,
        frozen: this.#parts !== null || (this.shared.resume?.frozen ?? false),
        minimum: this.#phaseMinimum,
        progressBytes: this.shared.progressBytes ?? 0,
        profile: { ...this.profile },
      };
      await this.shared.handoff(phase);
      this.#transferred = true;
      return;
    }
    if (this.#output) {
      const outputTypes = [
        ...this.rel.groupBy.map((c) => {
          const type = this.context.input[c];
          if (!type) throw new Error("Missing aggregate output key type");
          return type;
        }),
        ...this.rel.aggregates.map((c) => c.type),
      ];
      for await (const batch of this.#output.read(outputTypes))
        if (this.downstream.push(batch) === "blocked") await this.downstream.ready();
    }
    this.profile.spilledBytes = this.#files.written;
    await this.downstream.finish();
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    this.#controller.abort(reason);
    this.downstream.abort(reason);
    void this.#close().catch(() => undefined);
  }
  #close(): Promise<void> {
    return (this.#cleanup ??= (async () => {
      // Cancellation first drains every operation borrowing pages or routing buffers.
      // In particular finish() can be reading a tape without a pending push().
      await this.#finishWork?.catch(() => undefined);
      await this.#pending?.catch(() => undefined);
      await this.#restore?.catch(() => undefined);
      const errors: unknown[] = [];
      try {
        if (!this.#transferred) await this.#files.closeAll();
      } catch (error) {
        errors.push(error);
      }
      const clean = (action: () => void) => {
        try {
          action();
        } catch (error) {
          errors.push(error);
        }
      };
      clean(() => this.#frozen?.close());
      clean(() => this.#parts?.close());
      clean(() => this.#staging?.close());
      clean(() => this.#hash.abort(this.#reason));
      clean(() => this.#arena.close());
      clean(() => this.#workspace.close());
      clean(() => this.#directory?.detach());
      clean(() => this.#pages.close());
      clean(() => this.#ioPool.free(8 * BATCH_ROWS + PASS_METADATA_BYTES));
      clean(() => this.#ioPool.close());
      clean(() => this.#grants.close());
      this.shared.signal?.removeEventListener("abort", this.#parentAbort);
      if (errors.length) throw new AggregateError(errors, "Hybrid aggregate cleanup failed");
    })());
  }
  get closed(): Promise<void> {
    return this.#cleanup ?? Promise.resolve();
  }
}
