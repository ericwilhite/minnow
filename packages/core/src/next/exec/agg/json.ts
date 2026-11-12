import { JSON_VALUE_KINDS } from "../kernels/json-value.js";
import { JsonOrdinaryTransition, ordinaryJsonCall } from "./json-ordinary.js";
/** File-backed JSON transitions share the ordinary sort, spill and phase admission protocol. */
import type { Rel, AggregateCall } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { Grant } from "../../memory/governor.js";
import { MemoryAdmissionError } from "../../memory/governor.js";
import { BATCH_ROWS, type Batch, type Vector, type FlatVector } from "../../vector/vector.js";
import type { CompileContext } from "../kernels/compile.js";
import type { JsonMemory } from "../kernels/json.js";
import {
  afterYield,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type PushResult,
  type Transfer,
} from "../pipeline.js";
import { Slice, type SlicedJob } from "../slices.js";
import { Charge, Pool, SpillIo } from "../spill-pool.js";
import { PageWriter, SpillFiles } from "../spill-pages.js";
import { ColumnStore } from "../column-store.js";
import { SortOperator, sortMinimumBytes, type SortContext } from "../sort.js";
import { keysEqual, type SortColumn } from "../sort-keys.js";
import { JsonRecords } from "../json-records.js";
import type { JsonPhaseContext, JsonPhase } from "../json-phase.js";
import { ParameterCheckpoint } from "../parameter-checkpoint.js";
import { TapeDirectory } from "./hash-tape.js";
import { SortGrants } from "./sort-grants.js";
import {
  JsonArrayTransition,
  jsonArraySortTypes,
  type JsonTransition,
  type JsonAggregateHost,
} from "./json-values.js";
import { JsonObjectTransition, jsonObjectMinimum } from "./json-object.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;
const INT8: PhysicalType = { kind: "int64" };
const ownMinimum = (rel: AggregateRel) => (2 << 20) + rel.aggregates.length * (256 << 10);
export const isJsonAggregate = (call: AggregateCall): boolean =>
  ["json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg"].includes(call.fn);
export function jsonAggregateMinimum(rel: AggregateRel, input: readonly PhysicalType[]): number {
  // Sorted order does not imply byte equality: JSONB 1 and 1.0 are one PG group.
  // Keep structural keys outside this transition until semantic boundary equality is available.
  for (const index of rel.groupBy) {
    const type = input[index];
    if (type?.kind === "json" && !type.binary)
      throw new ExecutionError(
        "could not identify an equality operator for type json",
        "MINNOW_JSON_GROUP_KEY",
        "42883",
      );
    if (type?.kind === "json" || type?.kind === "array")
      throw new UnsupportedPlanError("JSONB/array aggregate grouping equality", {
        construct: "aggregate",
      });
  }
  for (const call of rel.aggregates) {
    if (!isJsonAggregate(call)) ordinaryJsonCall(call);
    else if (
      call.args.some((arg) => arg.op !== "column" || !JSON_VALUE_KINDS.has(arg.type.kind)) ||
      call.orderBy.some((key) => key.expr.op !== "column") ||
      (call.filter !== null && call.filter.op !== "column")
    )
      throw new UnsupportedPlanError("JSON aggregate input type or unprojected argument", {
        construct: "aggregate",
      });
  }
  if (rel.groupingSets !== null)
    throw new UnsupportedPlanError("mixed JSON aggregate transitions or grouping sets", {
      construct: "aggregate",
    });
  return (
    ownMinimum(rel) +
    (rel.groupBy.length ? sortMinimumBytes([...input, INT8]) : 0) +
    rel.aggregates.reduce(
      (n, c) =>
        n +
        (!isJsonAggregate(c)
          ? 0
          : c.fn.includes("object")
            ? jsonObjectMinimum(c)
            : c.distinct || c.orderBy.length
              ? sortMinimumBytes(jsonArraySortTypes(c))
              : 0),
      0,
    )
  );
}
export const jsonAggregateLoans = (rel: AggregateRel): number =>
  1 +
  (rel.groupBy.length ? 1 : 0) +
  rel.aggregates.reduce(
    (n, c) =>
      n +
      (!isJsonAggregate(c)
        ? 0
        : c.fn.includes("object")
          ? (c.orderBy.length ? 1 : 0) + (c.fn === "jsonb_object_agg" || c.json?.unique ? 1 : 0)
          : c.distinct || c.orderBy.length
            ? 1
            : 0),
    0,
  );

export class JsonAggregateOperator implements Operator, JsonAggregateHost {
  readonly pool: Pool;
  readonly io: SpillIo;
  readonly slice = new Slice(YIELD_INTERVAL_MS);
  readonly memory: JsonMemory;
  readonly #controller = new AbortController();
  readonly #grants: SortGrants;
  readonly #ownGrant: Grant;
  readonly #files: SpillFiles;
  readonly #pages: PageWriter;
  readonly #directory: TapeDirectory;
  readonly #prefix: JsonRecords;
  readonly #suffix: JsonRecords;
  readonly #keys: ColumnStore;
  readonly #keyTypes: readonly PhysicalType[];
  readonly #keyOrder: readonly SortColumn[];
  readonly #rows: Uint16Array;
  readonly #ordinal: FlatVector & {
    data: { layout: "int64"; values: { repr: "int64"; values: BigInt64Array } };
  };
  readonly #transitions: JsonTransition[] = [];
  readonly #sort: SortOperator | null;
  readonly #abortParent: () => void;
  #started = false;
  #sequence = 0n;
  #pending: Promise<void> | null = null;
  #sortedPending: Promise<void> | null = null;
  #finishWork: Promise<void> | null = null;
  #finished: Promise<void> | null = null;
  #cleanup: Promise<void> | null = null;
  #transferred = false;
  #aborted = false;
  #reason: unknown;
  #written = 0;
  #maxValue = 0;
  #rowError: ExecutionError | null = null;
  #transitionError: unknown;
  constructor(
    readonly rel: AggregateRel,
    readonly context: CompileContext,
    readonly shared: JsonPhaseContext & { readonly loans: readonly Grant[] },
    readonly downstream: Operator,
  ) {
    this.#grants = new SortGrants(shared.grant, shared.loans);
    this.#ownGrant = this.#grants.fork(ownMinimum(rel), 0);
    this.pool = new Pool(new Charge(this.#ownGrant), ownMinimum(rel));
    this.#files = new SpillFiles(shared.spill, shared.queryId, {
      allocate: () => false,
      free: () => {
        throw new Error("Unbuffered JSON aggregate cache");
      },
    });
    this.#pages = new PageWriter(this.pool, 16384);
    this.io = new SpillIo(
      this.pool,
      this.#files,
      this.#pages,
      { grant: this.#ownGrant, queryId: shared.queryId, signal: this.#controller.signal },
      () => this.check(),
      () => this.pace(),
    );
    this.#directory = new TapeDirectory(this.io, this.pool);
    this.#prefix = new JsonRecords(
      this.io,
      shared.spill,
      rel.output.map((c) => c.type),
    );
    this.#suffix = new JsonRecords(this.io, shared.spill, context.input);
    this.#keyTypes = rel.groupBy.map((i) => {
      const t = context.input[i];
      if (!t) throw new Error("Missing group key");
      return t;
    });
    this.#keyOrder = this.#keyTypes.map((_, column) => ({
      column,
      direction: "asc",
      nulls: "last",
      collation: null,
    }));
    this.#keys = new ColumnStore(this.#keyTypes, this.pool, {
      chunkRows: 1,
      targetBytes: Infinity,
    });
    if (!this.pool.allocate(14 * BATCH_ROWS + 4096))
      throw new Error("JSON grouping metadata not admitted");
    this.#rows = new Uint16Array(BATCH_ROWS);
    this.#ordinal = {
      form: "flat",
      type: INT8,
      length: BATCH_ROWS,
      validity: null,
      data: { layout: "int64", values: { repr: "int64", values: new BigInt64Array(BATCH_ROWS) } },
    };
    this.memory = {
      take: (bytes) => this.#takeMemory(bytes),
      free: (bytes) => this.pool.free(bytes),
    };
    let loanSlot = rel.groupBy.length ? 2 : 1;
    for (const call of rel.aggregates) {
      if (!isJsonAggregate(call)) this.#transitions.push(new JsonOrdinaryTransition(call, this));
      else if (call.fn.includes("object"))
        this.#transitions.push(
          new JsonObjectTransition(
            call,
            this,
            call.orderBy.length ? loanSlot++ : 0,
            call.fn === "jsonb_object_agg" || call.json?.unique ? loanSlot++ : 0,
          ),
        );
      else
        this.#transitions.push(
          new JsonArrayTransition(
            call,
            this,
            call.distinct || call.orderBy.length ? loanSlot++ : 0,
          ),
        );
    }
    const groupTypes = [...context.input, INT8];
    const sink: Operator = {
      push: (batch) => {
        this.#sortedPending = this.#groups(batch);
        return "blocked";
      },
      ready: () => this.#sortedPending ?? Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    this.#sort = rel.groupBy.length
      ? new SortOperator(
          {
            types: groupTypes,
            keys: [
              ...rel.groupBy.map((column) => ({
                column,
                direction: "asc" as const,
                nulls: "last" as const,
                collation: null,
              })),
              { column: inputLength(context), direction: "asc", nulls: "last", collation: null },
            ],
            limit: null,
            offset: 0,
            withTies: false,
          },
          this.sortContext(this.forkSort(sortMinimumBytes(groupTypes), 1)),
          sink,
        )
      : null;
    this.#abortParent = () => this.abort(shared.signal.reason);
    shared.signal.addEventListener("abort", this.#abortParent, { once: true });
    shared.dispose(() => this.#close());
    if (shared.signal.aborted) this.#abortParent();
  }
  *#takeMemory(bytes: number): SlicedJob<void> {
    if (!this.pool.allocate(bytes)) {
      yield this.pool.need(bytes, this.#ownGrant, this.#controller.signal);
      if (!this.pool.allocate(bytes)) throw new Error("JSON serializer scratch not admitted");
    }
  }
  forkSort(minimum: number, slot: number): Grant {
    return this.#grants.fork(minimum, slot);
  }
  resetSort(grant: Grant): void {
    this.#grants.reset(grant);
  }
  sortContext(grant: Grant): SortContext {
    return {
      grant,
      spill: this.shared.spill,
      queryId: this.shared.queryId,
      signal: this.#controller.signal,
    };
  }
  check(): void {
    if (this.#aborted) throw this.#reason;
    this.shared.signal.throwIfAborted();
  }
  async pace(): Promise<void> {
    this.check();
    if (this.slice.due()) {
      await yieldTask();
      afterYield(this.slice.clock);
      this.check();
    }
  }
  async capture(store: ColumnStore, vectors: readonly Vector[], row: number): Promise<void> {
    store.reset(true);
    this.pool.settle();
    const n = store.appendNeed(vectors, row);
    if (this.pool.allocate(n)) this.pool.free(n);
    else await this.pool.need(n, this.#ownGrant, this.#controller.signal);
    this.check();
    if (store.append(vectors, null, row, 1) !== 1)
      throw new Error("JSON group capture was not admitted");
  }
  checkValueLength(length: number): void {
    this.#maxValue = Math.max(this.#maxValue, length);
    if (2 * length > this.shared.capacity)
      throw new MemoryAdmissionError("JSON aggregate value", 2 * length, this.shared.capacity);
  }
  spilled(bytes: number): void {
    this.#written += bytes;
  }
  get stopped(): boolean {
    return this.#rowError !== null || this.shared.stopped?.() === true;
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    this.#pending = this.#input(batch)
      .catch((error: unknown) => {
        if (!this.#defer(error)) throw error;
      })
      .finally(() => transfer?.release());
    return "blocked";
  }
  ready(): Promise<void> {
    return this.#pending ?? Promise.resolve();
  }
  async #input(batch: Batch): Promise<void> {
    this.check();
    const n = batch.selection?.count ?? batch.length;
    for (let at = 0; at < n; at++) {
      const row = batch.selection?.rows[at] ?? at;
      this.#rows[at] = row;
      this.#ordinal.data.values.values[row] = this.#sequence++;
    }
    const augmented = { ...batch, columns: [...batch.columns, this.#ordinal] };
    if (this.#sort) {
      if (this.#sort.push(augmented) === "blocked") await this.#sort.ready();
    } else await this.#groups(augmented);
  }
  #defer(error: unknown): boolean {
    if (!(error instanceof ExecutionError) || !error.sqlState.startsWith("22")) return false;
    this.#transitionError = error;
    this.#rowError = new ExecutionError(error.message, error.code, error.sqlState);
    for (const t of this.#transitions) t.abort(error);
    return true;
  }
  async #groups(batch: Batch): Promise<void> {
    if (this.#rowError) return;
    try {
      await this.#groupRows(batch);
    } catch (error) {
      if (!this.#defer(error)) throw error;
    }
  }
  async #groupRows(batch: Batch): Promise<void> {
    const n = batch.selection?.count ?? batch.length,
      ids = this.#sort ? null : batch.selection?.rows;
    const rows = this.#rows,
      keys = this.rel.groupBy.map((i) => {
        const v = batch.columns[i];
        if (!v) throw new Error("Missing JSON group key");
        return v;
      });
    const ordinal = batch.columns[inputLength(this.context)];
    if (!ordinal) throw new Error("Missing JSON ordinal");
    for (let at = 0; at < n;) {
      await this.pace();
      const row = ids?.[at] ?? at;
      if (
        this.#started &&
        !keysEqual(this.#keyTypes, this.#keyOrder, this.#keys.vectors(), 0, keys, row)
      )
        await this.#emit(true);
      if (!this.#started) {
        if (keys.length) await this.capture(this.#keys, keys, row);
        this.#started = true;
      }
      let end = at + 1;
      while (
        end < n &&
        keysEqual(this.#keyTypes, this.#keyOrder, keys, row, keys, ids?.[end] ?? end)
      ) {
        end++;
        if ((end & 1023) === 0) await this.pace();
      }
      for (let i = at; i < end; i++) rows[i - at] = ids?.[i] ?? i;
      for (const transition of this.#transitions)
        await transition.add(batch, rows, 0, end - at, ordinal);
      at = end;
    }
  }
  async #emit(restart: boolean): Promise<void> {
    const fields: Array<Vector | import("../json-records.js").StreamedJsonField> = [
      ...this.#keys.vectors(),
    ];
    for (const t of this.#transitions) fields.push(await t.result());
    // Each transition shares one I/O lane: serialize the completed fields in column order.
    await this.#prefix.writeStreamed(fields);
    if (restart) for (const t of this.#transitions) await t.restart();
    this.#started = false;
  }
  finish(): Promise<void> {
    if (this.#finished) return this.#finished;
    this.#finishWork = this.#finish();
    return (this.#finished = this.#finishWork.then(
      () => this.#close(),
      async (error: unknown) => {
        this.abort(error);
        await this.#close().catch(() => undefined);
        throw error;
      },
    ));
  }
  async #finish(): Promise<void> {
    await this.#pending;
    this.check();
    if (this.#sort) {
      await this.#sort.finish();
      await this.#sort.closed;
    }
    if (!this.#rowError && (this.#started || !this.rel.groupBy.length)) {
      try {
        await this.#emit(false);
      } catch (error) {
        if (!this.#defer(error)) throw error;
      }
    }
    await this.#prefix.seal();
    await this.#suffix.seal();
    const parameters = new ParameterCheckpoint(this.#directory);
    await parameters.save(this.context.params.slots);
    this.#directory.detach();
    const saved: JsonPhase = {
      emit: true,
      ownedDelivery: true,
      error: this.#rowError,
      prefix: this.#prefix,
      suffix: this.#suffix,
      parameters,
      files: this.#files,
      directory: this.#directory,
      capacity: 0,
      retainedBytes: this.#prefix.retainedBytes,
      progressBytes: this.shared.progressBytes,
      writtenAtHandoff:
        this.#files.written + this.#prefix.files.written + this.#suffix.files.written,
      aggregateValueBytes: this.#maxValue,
    };
    await this.shared.handoff(saved);
    this.#transferred = true;
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    this.#grants.holdFrees = true;
    this.#controller.abort(reason);
    this.#sort?.abort(reason);
    for (const t of this.#transitions) t.abort(reason);
    this.downstream.abort(reason);
    void this.#close().catch(() => undefined);
  }
  #close(): Promise<void> {
    return (this.#cleanup ??= (async () => {
      await this.#pending?.catch(() => undefined);
      await this.#finishWork?.catch(() => undefined);
      await this.#sortedPending?.catch(() => undefined);
      const results = await Promise.allSettled([
        ...(this.#sort ? [this.#sort.closed] : []),
        ...this.#transitions.map((t) => t.close()),
        ...(this.#transferred
          ? []
          : [this.#prefix.close(), this.#suffix.close(), this.#files.closeAll()]),
      ]);
      this.#keys.close();
      this.#directory.detach();
      this.#pages.close();
      this.pool.free(14 * BATCH_ROWS + 4096);
      this.pool.close();
      this.#grants.close();
      this.shared.signal.removeEventListener("abort", this.#abortParent);
      const failures = results.filter(
        (r): r is PromiseRejectedResult =>
          r.status === "rejected" &&
          r.reason !== this.#reason &&
          r.reason !== this.#transitionError,
      );
      if (failures.length)
        throw new AggregateError(
          failures.map((r) => r.reason as unknown),
          "JSON aggregate cleanup failed",
        );
    })());
  }
  get closed(): Promise<void> {
    return this.#cleanup ?? Promise.resolve();
  }
  get spilledBytes(): number {
    return (
      this.#written +
      (this.#sort?.profile.spilledBytes ?? 0) +
      this.#files.written +
      this.#prefix.files.written +
      this.#suffix.files.written
    );
  }
  get spillPasses(): number {
    return this.spilledBytes ? 1 : 0;
  }
}
function inputLength(context: CompileContext): number {
  return context.input.length;
}
