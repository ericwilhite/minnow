/** Pressure-only stable restoration. Saved fields/membership precede suffix rows;
 * one group is resident, no partial aggregate result is ever combined. */
import { MAX_BLOB_BYTES } from "../../access/space.js";
import type { Rel } from "../../ir/ir.js";
import type { Grant } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import { BATCH_ROWS, type Batch, type FlatVector, type Vector } from "../../vector/vector.js";
import { ColumnStore } from "../column-store.js";
import type { CompileContext } from "../kernels/compile.js";
import { nullConstant, rowsIn, countIn, slotOf } from "../kernels/vectors.js";
import { afterYield, yieldTask, YIELD_INTERVAL_MS, type Operator } from "../pipeline.js";
import { Slice } from "../slices.js";
import { keysEqual, type SortColumn } from "../sort-keys.js";
import { SortOperator, sortMinimumBytes } from "../sort.js";
import { PageWriter, decodePage } from "../spill-pages.js";
import { Charge, Pool, SpillIo } from "../spill-pool.js";
import { simpleAggregateType } from "./parameter-copies.js";
import type { HybridPhase } from "./hybrid.js";
import { ExternalCall, externalCallMinimum, externalCallDistinct } from "./external-call.js";
import { SortGrants } from "./sort-grants.js";
import { StateReader } from "./state-row.js";
import type { SpillService } from "../../spill/contract.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;
const NUMBER: PhysicalType = { kind: "float64" },
  BYTES: PhysicalType = { kind: "bytes" };
const ownMinimum = (input: readonly PhysicalType[]) => (1 << 20) + 2048 * input.length;
const records = (input: readonly PhysicalType[]) => [...input, NUMBER, NUMBER, NUMBER, BYTES];
const valueTypes = (rel: AggregateRel, call: number): readonly PhysicalType[] => {
  const arg = rel.aggregates[call]?.args[0];
  if (!arg) throw new Error("Missing saved membership argument");
  return [arg.type];
};
const asc = (column: number): SortColumn => ({
  column,
  direction: "asc",
  nulls: "last",
  collation: null,
});
export function restoredMinimum(rel: AggregateRel, input: readonly PhysicalType[]): number {
  return (
    ownMinimum(input) +
    sortMinimumBytes(records(input)) +
    rel.aggregates.reduce(
      (n, call) =>
        n +
        externalCallMinimum(call) +
        (externalCallDistinct(call) && call.args[0]
          ? 2 * sortMinimumBytes([call.args[0].type, NUMBER])
          : 0),
      0,
    )
  );
}
export function restoredLoans(rel: AggregateRel): number {
  return 2 + rel.aggregates.reduce((n, call) => n + 1 + 2 * Number(externalCallDistinct(call)), 0);
}
interface RestoreContext {
  readonly sharedStep?: boolean;
  readonly grant: Grant;
  readonly loans: readonly Grant[];
  readonly spill: SpillService;
  readonly queryId: number;
  readonly signal: AbortSignal;
}
export class RestoredAggregateOperator {
  readonly grants: SortGrants;
  readonly #own: Grant;
  readonly #pool: Pool;
  readonly #pages: PageWriter;
  readonly #io: SpillIo;
  readonly #keys: ColumnStore;
  readonly #keyTypes: readonly PhysicalType[];
  readonly #keyOrder: readonly SortColumn[];
  readonly #sort: SortOperator;
  readonly #calls: ExternalCall[] = [];
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  readonly #nulls: readonly Vector[];
  readonly #tag: Float64Array;
  readonly #call: Float64Array;
  readonly #ordinal: Float64Array;
  readonly #metadata: readonly Vector[];
  #payload: FlatVector;
  readonly #offsets: Int32Array;
  readonly #sharedStep: boolean;
  #started = false;
  #nextOrdinal = 0;
  #memberOrdinal = 0;
  #pending: Promise<void> | null = null;
  #cleanup: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  readonly profile = { spilledBytes: 0, spillPasses: 0, groups: 0 };
  constructor(
    readonly rel: AggregateRel,
    readonly context: CompileContext,
    readonly shared: RestoreContext,
    readonly phase: HybridPhase,
    readonly downstream: Operator,
  ) {
    this.#sharedStep =
      context.input.every(simpleAggregateType) &&
      !rel.aggregates.some(
        (call) =>
          call.args[0] &&
          ["text", "bytes", "json", "array", "enum", "decimal"].includes(call.args[0].type.kind),
      );
    this.grants = new SortGrants(shared.grant, shared.loans);
    this.#own = this.grants.fork(ownMinimum(context.input), 0);
    this.#pool = new Pool(new Charge(this.#own), ownMinimum(context.input));
    // Numeric decoder/encoder and range-check temporaries are charged before use.
    if (!this.#pool.allocate(384 * 1024 + 512 * context.input.length + 8 * BATCH_ROWS + 4096))
      throw new Error("Restoration scratch was not admitted");
    this.#pages = new PageWriter(this.#pool, 16 * 1024);
    this.#io = new SpillIo(
      this.#pool,
      phase.files,
      this.#pages,
      { grant: this.#own, queryId: shared.queryId, signal: shared.signal },
      () => this.check(),
      () => this.#pause(),
    );
    this.#keyTypes = rel.groupBy.map((c) => {
      const type = context.input[c];
      if (!type) throw new Error("Missing restoration key");
      return type;
    });
    this.#keyOrder = this.#keyTypes.map((_, c) => asc(c));
    this.#keys = new ColumnStore(this.#keyTypes, this.#pool, {
      chunkRows: 1,
      targetBytes: Infinity,
    });
    this.#nulls = context.input.map((type) => nullConstant(type));
    this.#tag = new Float64Array(1);
    this.#call = new Float64Array(1);
    this.#ordinal = new Float64Array(BATCH_ROWS);
    this.#metadata = [this.#tag, this.#call].map((values): Vector => ({
      form: "constant",
      type: NUMBER,
      length: BATCH_ROWS,
      value: {
        form: "flat",
        type: NUMBER,
        length: 1,
        validity: null,
        data: { layout: "f64", values },
      },
    }));
    this.#offsets = new Int32Array(2);
    this.#payload = {
      form: "flat",
      type: BYTES,
      length: 1,
      validity: null,
      data: { layout: "varlen", offsets: this.#offsets, bytes: new Uint8Array(0) },
    };
    let slot = 2;
    for (let i = 0; i < rel.aggregates.length; i++) {
      const call = rel.aggregates[i],
        saved = phase.checkpoint.calls[i];
      if (!call || !saved) throw new Error("Missing restoration call metadata");
      this.#calls.push(new ExternalCall(call, this, slot, saved));
      slot += 1 + 2 * Number(externalCallDistinct(call));
    }
    const sorted: Operator = {
      push: (batch) => {
        this.#pending = this.#groups(batch);
        return "blocked";
      },
      ready: () => this.#pending ?? Promise.resolve(),
      finish: async () => {
        if (this.#started) await this.#emit(false);
        await downstream.finish();
      },
      abort: (reason) => this.abort(reason),
    };
    this.#sort = new SortOperator(
      {
        types: records(context.input),
        keys: [...rel.groupBy, context.input.length, context.input.length + 2].map(asc),
        limit: null,
        offset: 0,
        withTies: false,
      },
      this.sortContext(this.grants.fork(sortMinimumBytes(records(context.input)), 1)),
      sorted,
    );
  }
  check(): void {
    if (this.#aborted) throw this.#reason;
    this.shared.signal.throwIfAborted();
  }
  #pause(): Promise<void> | null {
    this.check();
    return this.#slice.due()
      ? yieldTask().then(() => {
          afterYield(this.#slice.clock);
          this.check();
        })
      : null;
  }
  async pace(): Promise<void> {
    const pending = this.#pause();
    if (pending) await pending;
  }
  sortContext(grant: Grant) {
    return {
      ...this.shared,
      grant,
      // Wide pooled steps store raw records: a normalized byte-key sidecar would
      // be an additional payload. Narrow keys fit the normal minimum/floor.
      valueComparison:
        this.shared.sharedStep === true &&
        this.grants.stepCapacity(this.#own) - ownMinimum(this.context.input) > MAX_BLOB_BYTES,
    };
  }
  async #room(): Promise<void> {
    await this.#pool.prepareStep(
      this.grants.stepCapacity(this.#own),
      this.#own,
      this.shared.signal,
    );
  }
  async #push(
    columns: readonly Vector[],
    length: number,
    selection: Batch["selection"],
  ): Promise<void> {
    const ordinal: FlatVector = {
      form: "flat",
      type: NUMBER,
      length,
      validity: null,
      data: { layout: "f64", values: this.#ordinal },
    };
    if (
      this.#sort.push({
        columns: [
          ...columns,
          ...this.#metadata,
          ordinal,
          { form: "constant", type: BYTES, length, value: this.#payload },
        ],
        length,
        selection,
      }) === "blocked"
    )
      await this.#sort.ready();
  }
  /** The old page is released inside next(). Pin the COMPLETE read step before
   * that await, through the yielded borrow, so release/settle cannot leave only
   * a raw-page loan for the following decode or record. */
  async *#read<T>(source: AsyncIterable<T>): AsyncGenerator<T> {
    const input = source[Symbol.asyncIterator]();
    try {
      for (;;) {
        await this.#room();
        const unpin = this.#pool.pinStep();
        try {
          const next = await input.next();
          if (next.done) return;
          yield next.value;
        } finally {
          unpin();
        }
      }
    } finally {
      await input.return?.();
    }
  }
  async #feed(): Promise<void> {
    await this.#room();
    this.phase.directory.attach(this.#io, this.#pool);
    try {
      for await (const record of this.#read(this.phase.checkpoint.records())) {
        await this.pace();
        await this.#room();
        const columns = [...this.#nulls];
        for (let i = 0; i < this.rel.groupBy.length; i++) {
          const input = this.rel.groupBy[i],
            value = record.columns[i];
          if (input === undefined || !value) throw new Error("Missing saved group key");
          columns[input] = value;
        }
        const payload = record.columns.slice(this.rel.groupBy.length);
        let encoded = this.#pages.encode(
          { queryId: this.shared.queryId, run: 0, partition: 0, sequence: 0 },
          payload,
          1,
        );
        if (!encoded) {
          await this.#room();
          encoded = this.#pages.encode(
            { queryId: this.shared.queryId, run: 0, partition: 0, sequence: 0 },
            payload,
            1,
          );
        }
        if (!encoded) throw new Error("Saved fields exceeded restoration step");
        this.#payload = {
          ...this.#payload,
          data: { layout: "varlen", offsets: this.#offsets, bytes: encoded },
        };
        this.#offsets[1] = encoded.length;
        this.#tag[0] = record.kind === "state" ? 0 : 1;
        this.#call[0] = record.call;
        this.#ordinal[0] = record.kind === "state" ? record.call : this.#memberOrdinal++;
        await this.#push(columns, 1, null);
        this.#payload = {
          ...this.#payload,
          data: { layout: "varlen", offsets: this.#offsets, bytes: new Uint8Array(0) },
        };
        this.#pages.shrinkIf(true);
        this.#pool.settle();
      }
      this.#tag[0] = 2;
      this.#call[0] = -1;
      this.#offsets[1] = 0;
      // Earlier new-group rows precede the eviction suffix for those same groups.
      const runs = [
        ...(this.phase.partitions ?? []),
        this.phase.suffix,
        ...this.phase.pending.map((p) => p.run),
      ];
      for (const run of runs)
        for await (const batch of this.#read(run.read(this.context.input))) {
          await this.pace();
          const rows = rowsIn(batch.selection),
            count = countIn(batch.selection, batch.length);
          for (let k = 0; k < count; k++) this.#ordinal[rows[k] ?? 0] = this.#nextOrdinal++;
          if (!Number.isSafeInteger(this.#nextOrdinal))
            throw new Error("Restoration ordinal overflow");
          await this.#push(batch.columns, batch.length, batch.selection);
        }
      if (this.phase.output) {
        const output = [...this.#keyTypes, ...this.rel.aggregates.map((a) => a.type)];
        for await (const batch of this.#read(this.phase.output.read(output))) {
          this.check();
          if (this.downstream.push(batch) === "blocked") await this.downstream.ready();
        }
      }
    } finally {
      this.phase.directory.detach();
    }
    this.profile.spilledBytes = this.phase.files.written;
    await this.phase.files.closeAll();
  }
  async #capture(keys: readonly Vector[], row: number): Promise<void> {
    this.#keys.reset(true);
    this.#pool.settle();
    const need = this.#keys.appendNeed(keys, row);
    if (!this.#sharedStep) await this.#room();
    else
      await this.#pool.need(
        need + this.#keys.retainedBytes + 65536,
        this.#own,
        this.shared.signal,
        true,
      );
    if (this.#pool.withStep(() => this.#keys.append(keys, null, row, 1)) !== 1)
      throw new Error("Restored key exceeded its admitted step");
    if (this.#sharedStep) this.#pool.trimStep();
  }
  #number(vector: Vector | undefined, row: number): number {
    if (!vector) throw new Error("Missing restoration record tag");
    const [flat, slot] = slotOf(vector, row);
    if (flat.data.layout !== "f64") throw new Error("Invalid restoration tag");
    return flat.data.values[slot] ?? NaN;
  }
  async #groups(batch: Batch): Promise<void> {
    const keyColumns = this.rel.groupBy.map((c) => {
      const value = batch.columns[c];
      if (!value) throw new Error("Missing sorted group key");
      return value;
    });
    const n = this.context.input.length;
    for (let row = 0; row < batch.length; row++) {
      await this.pace();
      if (
        this.#started &&
        !keysEqual(this.#keyTypes, this.#keyOrder, this.#keys.vectors(), 0, keyColumns, row)
      )
        await this.#emit(true);
      if (!this.#started) {
        await this.#capture(keyColumns, row);
        this.#started = true;
      }
      const tag = this.#number(batch.columns[n], row),
        call = this.#number(batch.columns[n + 1], row);
      if (tag === 2) {
        const ordinal = batch.columns[n + 2];
        if (!ordinal) throw new Error("Missing suffix ordinal");
        for (const transition of this.#calls) await transition.add(batch, row, 1, ordinal);
        continue;
      }
      if (tag !== 0 && tag !== 1) throw new Error("Invalid saved record kind");
      if (call < 0) continue; // GROUP BY / DISTINCT with no transition calls.
      const payload = batch.columns[n + 3];
      if (!payload) throw new Error("Missing saved fields");
      const [flat, slot] = slotOf(payload, row);
      if (flat.data.layout !== "varlen") throw new Error("Invalid saved fields");
      const from = flat.data.offsets[slot] ?? 0,
        to = flat.data.offsets[slot + 1] ?? from;
      const bytes = flat.data.bytes.subarray(from, to);
      if (!this.#sharedStep) await this.#room();
      const types = tag === 0 ? this.phase.checkpoint.stateTypes(call) : valueTypes(this.rel, call);
      let decoded = decodePage(bytes, types, this.#pool);
      if ("needed" in decoded) {
        await this.#pool.need(decoded.needed, this.#own, this.shared.signal, true);
        decoded = decodePage(bytes, types, this.#pool);
      }
      if ("needed" in decoded) throw new Error("Saved fields exceeded decode admission");
      try {
        if (tag === 0)
          for (let i = 0; i < this.#calls.length; i++) {
            if (this.phase.checkpoint.calls[i]?.owner !== call) continue;
            const transition = this.#calls[i];
            if (!transition) throw new Error("Missing saved transition");
            const reader = new StateReader(decoded.columns, 0);
            await transition.restoreState(reader, bytes.length);
            if (reader.consumed !== decoded.columns.length)
              throw new Error("Saved state field count changed");
          }
        else {
          const transition = this.#calls[call],
            value = decoded.columns[0];
          if (!transition || !value) throw new Error("Missing saved membership");
          await transition.restoreMember(value);
        }
      } finally {
        this.#pool.free(decoded.charged);
        if (this.#sharedStep) this.#pool.trimStep();
      }
    }
  }
  async #emit(restart: boolean): Promise<void> {
    this.check();
    const columns: Vector[] = this.rel.groupBy.length ? [...this.#keys.vectors()] : [];
    for (const transition of this.#calls) columns.push(await transition.emit());
    if (this.downstream.push({ length: 1, columns, selection: null }) === "blocked")
      await this.downstream.ready();
    this.profile.groups++;
    this.check();
    if (restart) for (const transition of this.#calls) transition.restart();
    this.#started = false;
  }
  push(): never {
    throw new Error("Restoration consumes its sealed input, not a reopened producer");
  }
  ready(): Promise<void> {
    return this.#pending ?? Promise.resolve();
  }
  async finish(): Promise<void> {
    try {
      await this.#feed();
      this.#pool.trimStep();
      await this.#sort.finish();
      this.profile.spilledBytes += this.#sort.profile.spilledBytes;
      this.profile.spillPasses = this.#sort.profile.mergePasses + 1;
    } catch (error) {
      this.abort(error);
      throw error;
    } finally {
      await this.#close();
    }
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    this.grants.holdFrees = true;
    this.#sort.abort(reason);
    for (const transition of this.#calls) transition.abort(reason);
    this.downstream.abort(reason);
  }
  #close(): Promise<void> {
    return (this.#cleanup ??= (async () => {
      await this.#sort.closed.catch(() => undefined);
      await this.#pending?.catch(() => undefined);
      const results = await Promise.allSettled(this.#calls.map((c) => c.close()));
      this.#keys.close();
      this.#pages.close();
      this.#pool.free(384 * 1024 + 512 * this.context.input.length + 8 * BATCH_ROWS + 4096);
      this.#pool.close();
      this.grants.close();
      const errors = results
        .filter((r): r is PromiseRejectedResult => r.status === "rejected")
        .map((r) => r.reason as unknown);
      if (errors.length) throw new AggregateError(errors, "Restoration cleanup failed");
    })());
  }
  get closed(): Promise<void> {
    return this.#cleanup ?? Promise.resolve();
  }
}
