/** Pressure handoff only: spool already-admitted parameter vectors, never caller values.
 * Varlen payloads use bounded fragments; re-admission charges the complete owned copy
 * before restoring it. Initial parameter size refusal remains in parameters.ts.
 */
import type { FlatVector, VectorData } from "../vector/vector.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { Grant } from "../memory/governor.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { PageWriter, type SpillFiles } from "./spill-pages.js";
import type { PhysicalType } from "../types.js";
import { HashTape, type TapeDirectory } from "./agg/hash-tape.js";
import { flatBytes } from "./agg/values.js";
import { Slice } from "./slices.js";
import { afterYield, yieldTask, YIELD_INTERVAL_MS } from "./pipeline.js";
import type { ParameterBinding } from "./parameters.js";
const BYTES: PhysicalType = { kind: "bytes" };
const FRAGMENT = 65536;
interface Slot {
  readonly type: PhysicalType;
  readonly bytes: number | null;
  readonly retained: number;
  readonly null: boolean;
  readonly tape: HashTape;
}
export class ParameterCheckpoint {
  readonly #slots: Slot[] = [];
  #saved = false;
  #bytes = 0;
  #largestSlot = 0;
  readonly #files: SpillFiles;
  readonly #queryId: number;
  constructor(readonly directory: TapeDirectory) {
    this.#files = directory.io.files;
    this.#queryId = directory.io.context.queryId;
  }
  async save(owned: readonly FlatVector[]): Promise<void> {
    if (this.#saved) throw new Error("Parameters already checkpointed");
    this.#saved = true;
    const io = this.directory.io;
    if (!io.pool.allocate(1024)) throw new Error("Parameter checkpoint scratch not admitted");
    try {
      const offsets = new Int32Array(2);
      for (const slot of owned) {
        io.checkAborted();
        if (slot.length !== 1) throw new Error("Parameter slot must contain one value");
        const data = slot.data,
          tape = new HashTape(this.directory),
          missing = slot.validity !== null && ((slot.validity[0] ?? 0) & 1) === 0;
        if (data.layout === "varlen") {
          const from = data.offsets[0] ?? 0,
            to = data.offsets[1] ?? from;
          this.#slots.push({
            type: slot.type,
            bytes: missing ? 0 : to - from,
            retained: 512 + (missing ? 0 : to - from),
            null: missing,
            tape,
          });
          if (!missing)
            for (let at = from; at < to; at += FRAGMENT) {
              const end = Math.min(to, at + FRAGMENT);
              offsets[1] = end - at;
              await this.#write(tape, {
                form: "flat",
                type: BYTES,
                length: 1,
                validity: null,
                data: { layout: "varlen", bytes: data.bytes.subarray(at, end), offsets },
              });
            }
        } else {
          this.#slots.push({
            type: slot.type,
            bytes: null,
            retained: 512 + flatBytes(slot),
            null: missing,
            tape,
          });
          await this.#write(tape, slot);
        }
        const retained = this.#slots[this.#slots.length - 1]?.retained ?? 0;
        this.#bytes += retained;
        this.#largestSlot = Math.max(this.#largestSlot, retained);
      }
    } finally {
      io.pool.free(1024);
    }
  }
  async #write(tape: HashTape, column: FlatVector): Promise<void> {
    const io = this.directory.io;
    io.checkAborted();
    const pause = io.pace?.();
    if (pause) await pause;
    let page = io.pages.encode(
      { queryId: io.context.queryId, run: 0, partition: 0, sequence: tape.pageCount },
      [column],
      1,
    );
    if (!page) {
      await io.need(io.pages.growthFor([column], 1));
      page = io.pages.encode(
        { queryId: io.context.queryId, run: 0, partition: 0, sequence: tape.pageCount },
        [column],
        1,
      );
    }
    if (!page || !tape.write) throw new Error("Parameter checkpoint step was not admitted");
    await tape.write(page, 1);
  }
  async restore(charge: ByteCharge): Promise<{ slots: FlatVector[]; close: () => void }> {
    if (!this.#saved) throw new Error("Parameters not checkpointed");
    const slots: FlatVector[] = [];
    let bytes = 0,
      closed = false;
    const close = () => {
      if (closed) return;
      closed = true;
      slots.length = 0;
      charge.free(bytes);
      bytes = 0;
    };
    try {
      for (const slot of this.#slots) {
        this.directory.io.checkAborted();
        if (!charge.allocate(slot.retained))
          throw new Error("Owned parameter restore was not admitted");
        bytes += slot.retained;
        if (slot.bytes !== null) {
          const data = new Uint8Array(slot.bytes);
          let at = 0;
          for await (const batch of slot.tape.read([BYTES])) {
            const part = batch.columns[0];
            if (part?.data.layout !== "varlen") throw new Error("Invalid parameter fragment");
            const from = part.data.offsets[0] ?? 0,
              to = part.data.offsets[1] ?? from;
            if (at + to - from > data.length)
              throw new Error("Parameter checkpoint length overflow");
            data.set(part.data.bytes.subarray(from, to), at);
            at += to - from;
          }
          if (at !== data.length) throw new Error("Truncated parameter checkpoint");
          slots.push({
            form: "flat",
            type: slot.type,
            length: 1,
            validity: slot.null ? Uint32Array.of(0) : null,
            data: { layout: "varlen", bytes: data, offsets: Int32Array.of(0, data.length) },
          });
        } else {
          let value: FlatVector | undefined;
          for await (const batch of slot.tape.read([slot.type])) {
            const column = batch.columns[0];
            if (value || batch.length !== 1 || column === undefined)
              throw new Error("Invalid parameter checkpoint cardinality");
            if (512 + flatBytes(column) > slot.retained)
              throw new Error("Parameter checkpoint exceeded its admitted shape");
            value = {
              ...column,
              validity: column.validity?.slice() ?? null,
              data: copy(column.data),
            };
          }
          if (!value) throw new Error("Truncated parameter checkpoint");
          slots.push(value);
        }
      }
      return { slots, close };
    } catch (error) {
      close();
      throw error;
    }
  }
  /** Metadata only: no caller buffer or old grant survives in this binding. Each bind
   * restores under the new execution's parameter account, whose close owns its lifetime. */
  binding(): ParameterBinding {
    if (!this.#saved) throw new Error("Parameters not checkpointed");
    return {
      bytes: this.#bytes,
      logicalBytes: this.#bytes,
      largestSlot: this.#largestSlot,
      slotBytes: this.#slots.map((slot) => slot.retained),
      bind: async (charge) => ({ slots: (await this.restore(charge)).slots }),
    };
  }
  /** Restoring parameters precedes operator construction. Its temporary I/O lane
   * is part of the SAME atomic phase admission and leaves before any operator runs. */
  phaseBinding(signal: AbortSignal): ParameterBinding {
    const ioBytes = 2 << 20;
    const binding = this.binding();
    if (this.#slots.length === 0) return binding;
    return {
      bytes: binding.bytes + ioBytes,
      logicalBytes: this.#bytes,
      largestSlot: binding.largestSlot,
      ...(binding.slotBytes === undefined ? {} : { slotBytes: binding.slotBytes }),
      bind: async (charge) => {
        const native = charge as Grant;
        if (typeof native.borrow !== "function")
          throw new Error("Phase parameters require a native grant");
        const pool = new Pool(new Charge(native), ioBytes),
          pages = new PageWriter(pool);
        const slice = new Slice(YIELD_INTERVAL_MS);
        const io = new SpillIo(
          pool,
          this.#files,
          pages,
          { grant: native, queryId: this.#queryId, signal },
          () => signal.throwIfAborted(),
          () =>
            slice.due()
              ? yieldTask().then(() => {
                  afterYield(slice.clock);
                  signal.throwIfAborted();
                })
              : null,
        );
        let attached = false;
        try {
          this.directory.attach(io, pool);
          attached = true;
          return { slots: (await this.restore(charge)).slots };
        } finally {
          if (attached) this.directory.detach();
          pages.close();
          pool.close();
        }
      },
    };
  }
  async close(): Promise<void> {
    const results = await Promise.allSettled(this.#slots.map((slot) => slot.tape.release()));
    this.#slots.length = 0;
    const errors = results
      .filter((r): r is PromiseRejectedResult => r.status === "rejected")
      .map((r) => r.reason as unknown);
    if (errors.length) throw new AggregateError(errors, "Parameter checkpoint cleanup failed");
  }
}

/** Copy one scalar's native buffers after charging its complete stored shape. Wide
 * coefficients are immutable BigInts; adopting them preserves their exact value and
 * charges their new lifetime independently of the decoded spill-page lifetime. */
function copy(data: VectorData): VectorData {
  switch (data.layout) {
    case "bool":
      return { ...data, values: data.values.slice() };
    case "i32":
      return { ...data, values: data.values.slice() };
    case "f32":
      return { ...data, values: data.values.slice() };
    case "f64":
      return { ...data, values: data.values.slice() };
    case "int64":
      return {
        ...data,
        values:
          data.values.repr === "safe"
            ? { repr: "safe", values: data.values.values.slice() }
            : { repr: "int64", values: data.values.values.slice() },
      };
    case "fixed16":
      return { ...data, bytes: data.bytes.slice() };
    case "varlen":
      throw new Error("Variable parameter must use bounded fragments");
    case "interval":
      return {
        ...data,
        months: data.months.slice(),
        days: data.days.slice(),
        micros:
          data.micros.repr === "safe"
            ? { repr: "safe", values: data.micros.values.slice() }
            : { repr: "int64", values: data.micros.values.slice() },
      };
    case "decimal": {
      const c = data.coefficients;
      return {
        ...data,
        scales: data.scales?.slice() ?? null,
        specials: data.specials?.slice() ?? null,
        coefficients:
          c.repr === "safe"
            ? { repr: "safe", values: c.values.slice() }
            : c.repr === "int64"
              ? { repr: "int64", values: c.values.slice() }
              : c.repr === "int128"
                ? { repr: "int128", limbs: c.limbs.slice() }
                : { repr: "wide", values: c.values.slice() },
      };
    }
  }
}
