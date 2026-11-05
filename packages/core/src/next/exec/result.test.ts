/**
 * The result stream's credit window (exec/result.ts) under the pipeline driver: ordering, end,
 * backpressure, demand-driven early frames, the synchronous `poll`, the result grant's lifetime
 * (`delivered`), errors after frames, consumer cancellation, one frame larger than the window,
 * and the wire-ready payload checks.
 */
import { describe, expect, it } from "vitest";
import type { OutputColumn } from "../ir/ir.js";
import { Governor, type Grant } from "../memory/governor.js";
import { allocateFlat, type Batch, type FlatVector } from "../vector/vector.js";
import { QueryCancelledError } from "./errors.js";
import { drive, type BatchSource, type Operator, type PushResult } from "./pipeline.js";
import {
  FrameBufferError,
  ResultChannel,
  frameByteLength,
  frameTransferList,
  type FrameOffer,
  type ResultFrame,
  type UnnumberedPayload,
  type WireFlat,
} from "./result.js";

const INT = { kind: "int32", bits: 32 } as const;
const COLUMNS: readonly OutputColumn[] = [{ name: "n", type: INT, nullable: false }];
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

function int32Flat(values: readonly number[]): WireFlat {
  return {
    form: "flat",
    length: values.length,
    validity: null,
    data: { layout: "i32", values: Int32Array.from(values) },
  };
}

function offerOf(
  values: readonly number[],
  onFree: () => void = () => undefined,
  requiresLsn = 0,
): FrameOffer {
  return {
    payload: { rowCount: values.length, columns: [int32Flat(values)], dictionaries: [] },
    requiresLsn,
    free: onFree,
  };
}

function valuesOf(frame: ResultFrame): number[] {
  const column = frame.payload.columns[0];
  if (column?.form !== "flat" || column.data.layout !== "i32") throw new TypeError("int32");
  return [...column.data.values];
}

/** A source of `count` int32 batches of `rows` rows, reusing one buffer (borrowed). */
class CountingSource implements BatchSource {
  readonly #vector: FlatVector = allocateFlat(INT);
  #next = 0;
  constructor(
    readonly count: number,
    readonly rows: number,
  ) {}
  next(): Promise<{ batch: Batch } | null> {
    if (this.#next === this.count) return Promise.resolve(null);
    const base = this.#next++ * this.rows;
    const values = this.#vector.data.layout === "i32" ? this.#vector.data.values : null;
    for (let i = 0; i < this.rows; i++) if (values !== null) values[i] = base + i;
    this.#vector.length = this.rows;
    return Promise.resolve({
      batch: { length: this.rows, columns: [this.#vector], selection: null },
    });
  }
  abort(): void {
    // Nothing to release: the buffer is reused, never handed over.
  }
}

/**
 * A toy result sink: copies batches into an exactly sized frame charged to its grant, sealing at
 * `targetRows` or early when the consumer is waiting (`demand`). The real sink (Phase 2) does the
 * same over every vector form, with stream dictionaries.
 */
class ToySink implements Operator {
  #pending: number[] = [];
  constructor(
    readonly channel: ResultChannel,
    readonly grant: Grant,
    readonly targetRows: number,
  ) {}
  push(batch: Batch): PushResult {
    const column = batch.columns[0] as FlatVector;
    if (column.data.layout !== "i32") throw new TypeError("int32");
    this.#pending.push(...column.data.values.subarray(0, batch.length));
    if (this.#pending.length < this.targetRows && !this.channel.demand) return "ready";
    return this.#seal();
  }
  #seal(): PushResult {
    const values = this.#pending;
    this.#pending = [];
    const bytes = values.length * 4;
    if (!this.grant.allocate(bytes)) throw new Error("toy sink: grant refused");
    return this.channel.offer(offerOf(values, () => this.grant.free(bytes)));
  }
  ready(): Promise<void> {
    return this.channel.ready();
  }
  async finish(): Promise<void> {
    if (this.#pending.length > 0 && this.#seal() === "blocked") await this.channel.ready();
    this.channel.end();
  }
  abort(reason: unknown): void {
    this.channel.fail(reason);
  }
}

async function collect(channel: ResultChannel): Promise<number[][]> {
  const frames: number[][] = [];
  for (let frame = await channel.next(); frame !== null; frame = await channel.next()) {
    frames.push(valuesOf(frame));
    frame.release();
  }
  return frames;
}

describe("ResultChannel", () => {
  it("delivers frames in order with sequence numbers, then null", async () => {
    const channel = new ResultChannel(COLUMNS);
    channel.offer(offerOf([1, 2], undefined, 7));
    channel.offer(offerOf([3]));
    channel.end();
    const first = await channel.next();
    const second = await channel.next();
    expect([first?.payload.sequence, second?.payload.sequence]).toEqual([0, 1]);
    expect(first === null ? [] : valuesOf(first)).toEqual([1, 2]);
    expect(first?.requiresLsn).toBe(7);
    expect(await channel.next()).toBeNull();
    expect(await channel.next()).toBeNull();
  });

  it("payloads are plain data: structured-clone safe, release outside", () => {
    const channel = new ResultChannel(COLUMNS);
    channel.offer(offerOf([1, 2, 3]));
    const frame = channel.poll();
    if (frame === null || frame === undefined) throw new Error("frame expected");
    const cloned = structuredClone(frame.payload, { transfer: [...frame.transfer] });
    expect(cloned.rowCount).toBe(3);
    expect(frame.transfer[0]?.byteLength).toBe(0); // detached: the bytes moved
  });

  it("polls synchronously: a frame, undefined while running, null at the end", () => {
    const channel = new ResultChannel(COLUMNS);
    expect(channel.poll()).toBeUndefined();
    channel.offer(offerOf([5]));
    expect(channel.poll()?.payload.rowCount).toBe(1);
    channel.end();
    expect(channel.poll()).toBeNull();
  });

  it("pauses the pipeline at the window and resumes on release, never holding more", async () => {
    const governor = new Governor(64 << 20);
    const grant = await governor.admit({ label: "sink", minimumBytes: 64 << 10, spillable: false });
    // 1,000-row frames of 4 KB; a 10 KB window holds at most 3 unreleased frames.
    const channel = new ResultChannel(COLUMNS, { windowBytes: 10_000 });
    const sink = new ToySink(channel, grant, 1000);
    const run = drive(new CountingSource(40, 250), sink);
    await tick();
    expect(channel.outstandingBytes).toBe(12_000);
    let peak = 0;
    let rows = 0;
    for (let frame = await channel.next(); frame !== null; frame = await channel.next()) {
      peak = Math.max(peak, channel.outstandingBytes, grant.allocated);
      rows += frame.payload.rowCount;
      await tick();
      frame.release();
    }
    await run;
    await channel.delivered;
    expect(rows).toBe(10_000);
    expect(peak).toBeLessThan(10_000 + 4_000);
    expect(grant.allocated).toBe(0);
    expect(channel.outstandingBytes).toBe(0);
    grant.close();
  });

  it("seals a partial frame when the consumer is waiting (first rows leave early)", async () => {
    const governor = new Governor(64 << 20);
    const grant = await governor.admit({ label: "sink", minimumBytes: 64 << 10, spillable: false });
    const channel = new ResultChannel(COLUMNS);
    const first = channel.next();
    expect(channel.demand).toBe(true);
    const run = drive(new CountingSource(8, 100), new ToySink(channel, grant, 10_000));
    const head = await first;
    expect(head?.payload.rowCount).toBe(100);
    head?.release();
    expect(channel.demand).toBe(false);
    await run;
    // The rest coalesced into one frame: nobody was waiting while they were produced.
    expect((await collect(channel)).map((frame) => frame.length)).toEqual([700]);
    grant.close();
  });

  it("settles delivered only after the end and every release", async () => {
    const channel = new ResultChannel(COLUMNS);
    let delivered = false;
    void channel.delivered.then(() => (delivered = true));
    channel.offer(offerOf([1]));
    const frame = await channel.next();
    channel.end();
    await tick();
    expect(delivered).toBe(false); // the handed-out frame still holds the result grant
    frame?.release();
    await tick();
    expect(delivered).toBe(true);
  });

  it("delivers queued frames before an execution error, then rejects delivered", async () => {
    const channel = new ResultChannel(COLUMNS);
    channel.offer(offerOf([1]));
    channel.fail(new RangeError("division by zero"));
    const frame = await channel.next();
    expect(frame?.payload.rowCount).toBe(1);
    await expect(channel.next()).rejects.toThrow("division by zero");
    expect(() => channel.poll()).toThrow("division by zero");
    frame?.release();
    await expect(channel.delivered).rejects.toThrow("division by zero");
  });

  it("rejects a waiting consumer when the execution fails", async () => {
    const channel = new ResultChannel(COLUMNS);
    const waiting = channel.next();
    channel.fail(new RangeError("overflow"));
    await expect(waiting).rejects.toThrow("overflow");
  });

  it("cancels: frees queued frames, rejects the producer, closes at once, onCancel once", async () => {
    const reasons: unknown[] = [];
    let freed = 0;
    const channel = new ResultChannel(COLUMNS, {
      windowBytes: 4,
      onCancel: (reason) => reasons.push(reason),
    });
    channel.offer(offerOf([1], () => (freed += 1)));
    const handedOut = channel.poll();
    expect(channel.offer(offerOf([2], () => (freed += 1)))).toBe("blocked");
    const producer = channel.ready();
    channel.cancel("closed by client");
    channel.cancel("again");
    await expect(producer).rejects.toBeInstanceOf(QueryCancelledError);
    await expect(channel.next()).rejects.toBeInstanceOf(QueryCancelledError);
    await expect(channel.delivered).rejects.toBeInstanceOf(QueryCancelledError);
    expect(reasons).toEqual(["closed by client"]);
    expect(freed).toBe(1); // the queued frame; the handed-out one is still the consumer's
    handedOut?.release();
    handedOut?.release();
    expect(freed).toBe(2);
    expect(channel.outstandingBytes).toBe(0);
    // A producer that offers after the cancel has its frame freed at once.
    expect(channel.offer(offerOf([3], () => (freed += 1)))).toBe("blocked");
    expect(freed).toBe(3);
  });

  it("cancelling a finished stream frees its queue without calling onCancel", async () => {
    let cancelled = false;
    let freed = 0;
    const channel = new ResultChannel(COLUMNS, { onCancel: () => (cancelled = true) });
    channel.offer(offerOf([1], () => (freed += 1)));
    channel.end();
    channel.cancel();
    expect([cancelled, freed]).toEqual([false, 1]);
    await expect(channel.next()).rejects.toBeInstanceOf(QueryCancelledError);
  });

  it("passes one frame larger than the window without deadlock", async () => {
    const channel = new ResultChannel(COLUMNS, { windowBytes: 16 });
    expect(channel.offer(offerOf(Array.from({ length: 100 }, (_, i) => i)))).toBe("blocked");
    const producer = channel.ready();
    const frame = await channel.next();
    expect(frame?.payload.rowCount).toBe(100);
    frame?.release();
    await producer;
    channel.end();
    expect(await channel.next()).toBeNull();
  });

  it("refuses a second next(), a poll while one waits, and an offer after end", async () => {
    const channel = new ResultChannel(COLUMNS);
    const first = channel.next();
    await expect(channel.next()).rejects.toThrow(TypeError);
    expect(() => channel.poll()).toThrow(TypeError);
    channel.end();
    expect(await first).toBeNull();
    expect(() => channel.offer(offerOf([1]))).toThrow(TypeError);
  });
});

describe("frame payloads", () => {
  it("lists each buffer once for transfer and counts bytes, dictionary deltas included", () => {
    const shared = new Uint32Array([0b101]);
    const payload: UnnumberedPayload = {
      rowCount: 3,
      columns: [
        {
          form: "flat",
          length: 3,
          validity: shared,
          data: { layout: "f64", values: new Float64Array(3) },
        },
        {
          form: "dictionary",
          length: 3,
          codes: new Int32Array(3),
          validity: shared,
          dictionary: 0,
        },
        { form: "constant", length: 3, value: int32Flat([7]) },
        {
          form: "flat",
          length: 1,
          validity: null,
          data: {
            layout: "decimal",
            scale: 2,
            scales: null,
            coefficients: { repr: "wide", values: [10n ** 40n] },
            specials: null,
          },
        },
      ],
      dictionaries: [
        {
          id: 0,
          from: 0,
          values: {
            form: "flat",
            length: 2,
            validity: null,
            data: {
              layout: "varlen",
              bytes: new Uint8Array(6),
              offsets: Int32Array.from([0, 3, 6]),
            },
          },
        },
      ],
    };
    const buffers = frameTransferList(payload);
    expect(buffers).toHaveLength(6);
    expect(new Set(buffers).size).toBe(6);
    // 24 + 4 (validity once) + 12 + 6 + 12 + 4, plus the BigInt (16 + 8 × 3 digits).
    expect(frameByteLength(payload)).toBe(62 + 40);
  });

  it("refuses a view that does not cover its whole buffer", () => {
    const big = new Int32Array(16);
    const payload: UnnumberedPayload = {
      rowCount: 4,
      columns: [
        {
          form: "flat",
          length: 4,
          validity: null,
          data: { layout: "i32", values: big.subarray(0, 4) },
        },
      ],
      dictionaries: [],
    };
    expect(() => frameTransferList(payload)).toThrow(FrameBufferError);
  });
});
