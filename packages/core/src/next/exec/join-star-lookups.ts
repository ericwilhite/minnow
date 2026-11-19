/** C -> facts -> A -> streaming B preserves native source demand. C survivors
 * retain charged query IDs/payload once; a bounded ID pass links A/B buckets.
 * Each B prefix visits A descending/facts ascending before another source read.
 */
import type { MaybePromise } from "../access/key-index.js";
import type { ByteCharge } from "../vector/varlen.js";
import { BATCH_ROWS, type Batch, type FlatVector, type Vector } from "../vector/vector.js";
import { Charged } from "./agg/groups.js";
import { StarBuckets } from "./join-star-buckets.js";
import type { StarLookupShape, StarLookupBound } from "./join-star-shape.js";
import { nullConstant } from "./column-store.js";
import { IDENTITY, isValidSlot, slotOf } from "./kernels/vectors.js";
import {
  clockFor,
  afterYield,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type PushResult,
  type TaskClock,
} from "./pipeline.js";
import type { SlicedJob } from "./slices.js";
const I32 = { kind: "int32", bits: 32 } as const,
  F64 = { kind: "float64" } as const;
const EMPTY_I32 = new Int32Array(0),
  EMPTY_F64 = new Float64Array(0),
  EMPTY_U32 = new Uint32Array(0);

export class StarLookups {
  readonly #memory: Charged;
  readonly #buckets: StarBuckets;
  #maps: readonly [Int32Array, Int32Array, Int32Array];
  #aGroups: Int32Array;
  #factA: Int32Array;
  #factB: Int32Array;
  #keys: readonly [Int32Array, Int32Array, Int32Array];
  #values: Float64Array;
  #validity: readonly [Uint32Array, Uint32Array, Uint32Array, Uint32Array];
  readonly #columns: Vector[];
  readonly #clock: TaskClock;
  #a = 0;
  #emitted = 0;
  #cDone = false;
  #factsDone = false;
  #aDone = false;
  #linked = false;
  #bDone = false;
  #closing = false;
  #closed = false;
  #aborted = false;
  #reason: unknown;
  #job: SlicedJob<void> | null = null;
  #advancing = false;
  #pending: Promise<void> | null = null;
  #retirement: Promise<void> | undefined;
  #retire: (() => void) | undefined;
  constructor(
    readonly shape: StarLookupShape,
    readonly bound: StarLookupBound,
    charge: ByteCharge,
    readonly signal: AbortSignal,
  ) {
    this.#memory = new Charged(charge);
    this.#clock = clockFor();
    let buckets: StarBuckets | undefined;
    try {
      // Fixed ports, vector wrappers, job and retirement metadata are prepaid.
      this.#memory.take(128 << 10);
      this.#maps = [
        this.#memory.make(Int32Array, bound.domains[0]),
        this.#memory.make(Int32Array, bound.domains[1]),
        this.#memory.make(Int32Array, bound.domains[2]),
      ];
      this.#aGroups = this.#memory.make(Int32Array, bound.a);
      this.#factA = this.#memory.make(Int32Array, bound.facts);
      this.#factB = this.#memory.make(Int32Array, bound.facts);
      this.#keys = [
        this.#memory.make(Int32Array, BATCH_ROWS),
        this.#memory.make(Int32Array, BATCH_ROWS),
        this.#memory.make(Int32Array, BATCH_ROWS),
      ];
      this.#values = this.#memory.make(Float64Array, BATCH_ROWS);
      this.#validity = [
        this.#memory.make(Uint32Array, BATCH_ROWS / 32),
        this.#memory.make(Uint32Array, BATCH_ROWS / 32),
        this.#memory.make(Uint32Array, BATCH_ROWS / 32),
        this.#memory.make(Uint32Array, BATCH_ROWS / 32),
      ];
      this.#columns = shape.input.output.map((c) => nullConstant(c.type, BATCH_ROWS));
      buckets = new StarBuckets(bound.facts, bound.a, bound.b, charge);
      this.#buckets = buckets;
    } catch (error) {
      buckets?.close();
      this.#memory.close();
      throw error;
    }
  }
  get matchedRows(): number {
    return this.#emitted;
  }
  get closed(): MaybePromise<void> {
    return (
      this.#retirement ??
      this.#pending?.then(
        () => undefined,
        () => undefined,
      )
    );
  }
  #check(): void {
    if (this.#aborted) throw this.#reason;
    this.signal.throwIfAborted();
    if (this.#closing) throw new Error("Star lookups closed");
  }
  #code(vector: Vector | undefined, row: number): number | null {
    if (!vector) throw new Error("Missing encoded star column");
    if (!isValidSlot(vector, row)) return null;
    if (vector.form === "flat" && vector.data.layout === "i32") return vector.data.values[row] ?? 0;
    const [flat, at] = slotOf(vector, row);
    if (flat.data.layout !== "i32") throw new Error("Star lookup lacks query IDs");
    return flat.data.values[at] ?? 0;
  }
  #port(push: (batch: Batch) => void, finish: () => void): Operator {
    return {
      push: (batch) => {
        this.#check();
        push(batch);
        return this.#clock.due(YIELD_INTERVAL_MS) ? "blocked" : "ready";
      },
      ready: async () => {
        this.#check();
        await yieldTask();
        afterYield(this.#clock);
        this.#check();
      },
      finish: () => {
        this.#check();
        finish();
      },
      abort: (reason) => this.abort(reason),
    };
  }
  dimension(axis: 0 | 2): Operator {
    const dim = axis === 0 ? this.shape.a : this.shape.c,
      map = this.#maps[axis];
    return this.#port(
      (batch) => {
        if (axis === 2 ? this.#cDone || this.#factsDone : !this.#factsDone || this.#aDone)
          throw new Error("Star dimension outside native source phase");
        const active = batch.selection?.rows ?? IDENTITY,
          n = batch.selection?.count ?? batch.length;
        for (let i = 0; i < n; i++) {
          const row = active[i] ?? 0,
            key = this.#code(batch.columns[dim.key], row),
            group = this.#code(batch.columns[dim.group], row);
          if (key === null || key < 0 || key >= map.length || map[key] !== 0)
            throw new Error("Star dimension violated its PK/domain certificate");
          if (axis === 0) {
            if (this.#a >= this.bound.a) throw new RangeError("Star A rows exceed certificate");
            map[key] = this.#a + 1;
            this.#aGroups[this.#a++] = group ?? -1;
          } else map[key] = group === null ? 1 : group + 2;
        }
      },
      () => {
        if (axis === 0) this.#aDone = true;
        else this.#cDone = true;
      },
    );
  }
  fact(): Operator {
    return this.#port(
      (batch) => {
        if (!this.#cDone || this.#factsDone || this.#aDone)
          throw new Error("Star facts outside native source phase");
        const active = batch.selection?.rows ?? IDENTITY,
          n = batch.selection?.count ?? batch.length,
          [ak, bk, ck] = this.shape.factKeys,
          ka = batch.columns[ak],
          kb = batch.columns[bk],
          kc = batch.columns[ck];
        if (
          ka?.form !== "flat" ||
          ka.data.layout !== "i32" ||
          kb?.form !== "flat" ||
          kb.data.layout !== "i32" ||
          kc?.form !== "flat" ||
          kc.data.layout !== "i32"
        )
          throw new Error("Star facts lack encoded query IDs");
        const av = ka.data.values,
          bv = kb.data.values,
          cv = kc.data.values,
          amask = ka.validity,
          bmask = kb.validity,
          cmask = kc.validity,
          [amap, bmap, cmap] = this.#maps,
          vector = batch.columns[this.shape.factValue];
        if (
          av.length < batch.length ||
          bv.length < batch.length ||
          cv.length < batch.length ||
          !vector
        )
          throw new Error("Incomplete star fact batch");
        let payload: Float64Array | null = null,
          payloadMask: Uint32Array | null = null;
        if (vector.form === "flat" && vector.data.layout === "f64") {
          payload = vector.data.values;
          payloadMask = vector.validity;
        }
        for (let i = 0; i < n; i++) {
          const row = active[i] ?? 0;
          if (cmask !== null && ((cmask[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
          const cc = cv[row] ?? 0;
          if (cc < 0 || cc >= cmap.length)
            throw new RangeError("Star query ID exceeds its exact domain");
          const c = cmap[cc] ?? 0;
          if (c === 0) continue;
          if (
            (amask !== null && ((amask[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) ||
            (bmask !== null && ((bmask[row >>> 5] ?? 0) & (1 << (row & 31))) === 0)
          )
            continue;
          const a = av[row] ?? 0,
            b = bv[row] ?? 0;
          if (a < 0 || a >= amap.length || b < 0 || b >= bmap.length)
            throw new RangeError("Star query ID exceeds its exact domain");
          let value: number | null = null;
          if (payload !== null) {
            if (payloadMask === null || ((payloadMask[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0)
              value = payload[row] ?? 0;
          } else if (isValidSlot(vector, row)) {
            const [flat, at] = slotOf(vector, row);
            if (flat.data.layout !== "f64") throw new Error("Star SUM lacks float8 payload");
            value = flat.data.values[at] ?? 0;
          }
          const record = this.#buckets.stage(c - 2, value);
          this.#factA[record] = a;
          this.#factB[record] = b;
        }
      },
      () => {
        this.#factsDone = true;
      },
    );
  }
  /** Only row IDs are revisited after A becomes available; no payload gather. */
  linkFacts(): MaybePromise<void> {
    this.#check();
    if (!this.#aDone || !this.#factsDone || this.#linked)
      throw new Error("Star linking outside native source phase");
    return this.#start(this.#linkJob()) === "blocked" ? this.ready() : undefined;
  }
  *#linkJob(): SlicedJob<void> {
    const amap = this.#maps[0];
    for (let row = 0; row < this.#buckets.rows; row++) {
      const a = (amap[this.#factA[row] ?? 0] ?? 0) - 1;
      if (a >= 0) this.#buckets.link(row, a, this.#factB[row] ?? 0);
      if ((row & 8191) === 8191 && this.#clock.due(YIELD_INTERVAL_MS)) yield;
    }
    this.#linked = true;
  }
  /** Consume a B prefix completely before the scanner may request another item. */
  probe(downstream: Operator): Operator {
    this.#check();
    if (!this.#linked || this.#bDone) throw new Error("Star B outside native source phase");
    return {
      push: (batch) => {
        this.#check();
        return this.#start(this.#probeJob(batch, downstream));
      },
      ready: () => this.ready(),
      finish: () => {
        this.#check();
        if (this.#pending !== null) throw new Error("Star B finished before readiness");
        this.#bDone = true;
        return this.replay(downstream);
      },
      abort: (reason) => {
        try {
          downstream.abort(reason);
        } finally {
          this.abort(reason);
        }
      },
    };
  }
  *#probeJob(batch: Batch, downstream: Operator): SlicedJob<void> {
    const active = batch.selection?.rows ?? IDENTITY,
      n = batch.selection?.count ?? batch.length,
      dim = this.shape.b,
      map = this.#maps[1];
    for (let k = 0; k < n; k++) {
      this.#check();
      const row = active[k] ?? 0,
        b = this.#code(batch.columns[dim.key], row),
        group = this.#code(batch.columns[dim.group], row);
      if (b === null || b < 0 || b >= map.length)
        throw new RangeError("Star B key exceeds its exact domain");
      this.#buckets.beginB(this.#a, b);
      for (;;) {
        const step = this.#buckets.read(this.#keys[2], this.#keys[0], this.#keys[1]);
        if (step.length > 0) {
          this.#output(step.length, group ?? -1);
          this.#emitted += step.length;
          if (
            downstream.push({ length: step.length, columns: this.#columns, selection: null }) ===
            "blocked"
          )
            yield downstream.ready();
          this.#check();
        }
        if (step.done) break;
        if (this.#clock.due(YIELD_INTERVAL_MS)) yield;
      }
      if ((k & 255) === 255 && this.#clock.due(YIELD_INTERVAL_MS)) yield;
    }
  }
  #output(n: number, bGroup: number): void {
    const [ao, bo, co] = this.#keys,
      [am, bm, cm, vm] = this.#validity;
    let anull = false,
      bnull = false,
      cnull = false,
      vnull = false;
    for (const validity of this.#validity) validity.fill(0);
    for (let i = 0; i < n; i++) {
      const record = co[i] ?? 0,
        a = this.#aGroups[ao[i] ?? 0] ?? -1,
        c = this.#buckets.groupCodes[record] ?? -1,
        bit = 1 << (i & 31),
        word = i >>> 5;
      ao[i] = Math.max(0, a);
      bo[i] = Math.max(0, bGroup);
      co[i] = Math.max(0, c);
      if (a < 0) anull = true;
      else am[word] = (am[word] ?? 0) | bit;
      if (bGroup < 0) bnull = true;
      else bm[word] = (bm[word] ?? 0) | bit;
      if (c < 0) cnull = true;
      else cm[word] = (cm[word] ?? 0) | bit;
      this.#values[i] = this.#buckets.values[record] ?? 0;
      if (this.#buckets.isNull(record)) vnull = true;
      else vm[word] = (vm[word] ?? 0) | bit;
    }
    for (let axis = 0; axis < 3; axis++) {
      const index = this.shape.groups[axis],
        values = this.#keys[axis],
        validity = this.#validity[axis];
      if (index === undefined || !values || !validity) throw new Error("Missing star group output");
      const nullable = axis === 0 ? anull : axis === 1 ? bnull : cnull;
      this.#columns[index] = {
        form: "flat",
        type: I32,
        length: n,
        validity: nullable ? validity : null,
        data: { layout: "i32", values },
      };
    }
    const value: FlatVector = {
      form: "flat",
      type: F64,
      length: n,
      validity: vnull ? vm : null,
      data: { layout: "f64", values: this.#values },
    };
    this.#columns[this.shape.value] = value;
  }
  /** B EOF precedes aggregate finalization, including an empty result. */
  async replay(downstream: Operator): Promise<void> {
    this.#check();
    if (!this.#bDone) throw new Error("Star finalization before B EOF");
    this.close(); // Every B prefix has ended its borrow; finalization needs no fact state.
    await downstream.finish();
  }
  #step(): IteratorResult<Promise<void> | void, void> {
    const job = this.#job;
    if (job === null) throw new Error("Missing star job");
    this.#advancing = true;
    try {
      return job.next();
    } finally {
      this.#advancing = false;
      if (this.#closing && this.#pending === null) this.#release();
    }
  }
  #start(job: SlicedJob<void>): PushResult {
    this.#check();
    if (this.#job !== null) throw new Error("Star input before readiness");
    this.#job = job;
    let step: IteratorResult<Promise<void> | void, void>;
    try {
      step = this.#step();
    } catch (error) {
      this.#job = null;
      throw error;
    }
    if (step.done === true) {
      this.#job = null;
      return "ready";
    }
    this.#pending = this.#resume(step);
    return "blocked";
  }
  async #resume(first: IteratorResult<Promise<void> | void, void>): Promise<void> {
    let step = first;
    try {
      while (step.done !== true) {
        if (step.value === undefined) {
          await yieldTask();
          afterYield(this.#clock);
        } else await step.value;
        this.#check();
        step = this.#step();
      }
    } finally {
      this.#job = null;
      this.#pending = null;
      if (this.#closing) this.#release();
    }
  }
  ready(): Promise<void> {
    if (this.#pending !== null) return this.#pending;
    try {
      this.#check();
      return Promise.resolve();
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- preserve the exact caller-supplied abort reason
      return Promise.reject(error);
    }
  }
  abort(reason: unknown): void {
    if (!this.#aborted) {
      this.#aborted = true;
      this.#reason = reason;
    }
    this.close();
  }
  close(): void {
    if (this.#closing) return;
    this.#closing = true;
    if (this.#job !== null && !this.#advancing) {
      try {
        this.#job.return();
      } catch {
        /* Abort keeps its original reason. */
      }
      this.#job = null;
    }
    if (this.#pending !== null || this.#advancing) {
      this.#retirement = new Promise((resolve) => {
        this.#retire = resolve;
      });
    } else this.#release();
  }
  #release(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#columns.length = 0;
    this.#buckets.close();
    this.#maps = [EMPTY_I32, EMPTY_I32, EMPTY_I32];
    this.#aGroups = EMPTY_I32;
    this.#factA = EMPTY_I32;
    this.#factB = EMPTY_I32;
    this.#keys = [EMPTY_I32, EMPTY_I32, EMPTY_I32];
    this.#values = EMPTY_F64;
    this.#validity = [EMPTY_U32, EMPTY_U32, EMPTY_U32, EMPTY_U32];
    this.#memory.close();
    this.#retire?.();
  }
}
