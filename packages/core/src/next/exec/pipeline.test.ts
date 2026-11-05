/**
 * The push protocol under the four situations review A1-1 named (hash-aggregate spill mid-push,
 * external-sort output, join fan-out, slow result consumer), plus retain-without-copy credits and
 * cancellation. Toy operators keep the logic visible; real operators follow the same shape.
 */
import { describe, expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { SpillFile, SpillPageRef } from "../spill/contract.js";
import { allocateFlat, type Batch, type FlatVector } from "../vector/vector.js";
import {
  drive,
  pushAndWait,
  type BatchSource,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";

const INT = { kind: "int32", bits: 32 } as const;
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

function values(batch: Batch): Int32Array {
  const column = batch.columns[0] as FlatVector;
  if (column.data.layout !== "i32") throw new TypeError("int32 expected");
  return column.data.values.subarray(0, batch.length);
}

function batchOf(vector: FlatVector, length: number): Batch {
  vector.length = length;
  return { length, columns: [vector], selection: null };
}

/**
 * A borrowing source with ONE reusable buffer: each `next()` overwrites it, so an operator that
 * read a batch after its borrow ended would see garbage (-1s) and produce wrong results.
 */
class RecyclingSource implements BatchSource {
  readonly #buffer = allocateFlat(INT, 64);
  #position = 0;
  aborted = false;
  constructor(readonly input: readonly number[]) {}
  async next() {
    // Recycle at once, before any await: the strictest producer the borrow rule allows.
    const data = this.#buffer.data;
    if (data.layout !== "i32") throw new TypeError("int32");
    data.values.fill(-1);
    const rows = this.input.slice(this.#position, this.#position + 64);
    this.#position += rows.length;
    data.values.set(rows);
    await tick();
    return rows.length === 0 ? null : { batch: batchOf(this.#buffer, rows.length) };
  }
  abort() {
    this.aborted = true;
  }
}

/** Collects every value; optionally blocks after each push until a timer says it is ready. */
class Collect implements Operator {
  readonly seen: number[] = [];
  inFlight = 0;
  maxInFlight = 0;
  #release: (() => void) | null = null;
  #ready: Promise<void> = Promise.resolve();
  #reject: ((reason: unknown) => void) | null = null;
  finished = false;
  constructor(readonly window = Infinity) {}
  push(batch: Batch): PushResult {
    this.seen.push(...values(batch));
    if (this.window === Infinity) return "ready";
    // A result sink: the copy above is the frame on the wire; credit returns when acknowledged.
    this.inFlight += 1;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    setTimeout(() => {
      this.inFlight -= 1;
      this.#release?.();
    }, 1);
    if (this.inFlight < this.window) return "ready";
    this.#ready = new Promise((resolve, reject) => {
      this.#release = resolve;
      this.#reject = reject;
    });
    return "blocked";
  }
  ready() {
    return this.#ready;
  }
  async finish() {
    this.finished = true;
  }
  abort(reason: unknown) {
    this.#reject?.(reason);
  }
}

/** Hash aggregate (COUNT per key) that spills a partition mid-batch when its grant refuses growth. */
class SpillingCount implements Operator {
  readonly #counts = new Map<number, number>();
  readonly #runs: SpillPageRef[] = [];
  #pending: Promise<void> = Promise.resolve();
  spills = 0;
  constructor(
    readonly grant: Grant,
    readonly file: SpillFile,
    readonly downstream: Operator,
  ) {}
  #consume(rows: Int32Array, from: number): PushResult {
    for (let row = from; row < rows.length; row++) {
      const key = rows[row] ?? 0;
      if (!this.#counts.has(key) && !this.grant.tryGrow(64)) {
        // Grant refused mid-batch: spill, then resume from `row` — the batch stays borrowed.
        this.#pending = this.#spill().then(() => {
          if (this.#consume(rows, row) === "blocked") return this.#pending;
        });
        return "blocked";
      }
      this.#counts.set(key, (this.#counts.get(key) ?? 0) + 1);
    }
    return "ready";
  }
  async #spill() {
    this.spills += 1;
    const page = new TextEncoder().encode(JSON.stringify([...this.#counts]));
    this.#runs.push(await this.file.append(page));
    this.grant.release(64 * this.#counts.size);
    this.#counts.clear();
  }
  push(batch: Batch): PushResult {
    return this.#consume(values(batch), 0);
  }
  ready() {
    return this.#pending;
  }
  async finish() {
    for (const ref of this.#runs) {
      const run = JSON.parse(new TextDecoder().decode(await this.file.read(ref))) as Array<
        [number, number]
      >;
      for (const [key, count] of run) this.#counts.set(key, (this.#counts.get(key) ?? 0) + count);
    }
    const out = allocateFlat(INT, 64);
    const keys = [...this.#counts]
      .sort(([a], [b]) => a - b)
      .flatMap(([key, count]) => [key, count]);
    for (let start = 0; start < keys.length; start += 64) {
      const data = out.data;
      if (data.layout === "i32") data.values.set(keys.slice(start, start + 64));
      await pushAndWait(this.downstream, batchOf(out, Math.min(64, keys.length - start)));
    }
    await this.downstream.finish();
    await this.file.close();
    this.grant.close();
  }
  abort(reason: unknown) {
    this.downstream.abort(reason);
    void this.file.close();
    this.grant.close();
  }
}

/** External sort: runs of ≤ 50 values spilled, merged in `finish` into a blocking downstream. */
class ExternalSort implements Operator {
  readonly #run: number[] = [];
  readonly #runs: SpillPageRef[] = [];
  #pending: Promise<void> = Promise.resolve();
  constructor(
    readonly file: SpillFile,
    readonly downstream: Operator,
  ) {}
  push(batch: Batch): PushResult {
    this.#run.push(...values(batch)); // copied: the batch can be recycled
    if (this.#run.length < 50) return "ready";
    const run = this.#run.splice(0).sort((a, b) => a - b);
    this.#pending = this.file.append(new TextEncoder().encode(JSON.stringify(run))).then((ref) => {
      this.#runs.push(ref);
    });
    return "blocked";
  }
  ready() {
    return this.#pending;
  }
  async finish() {
    const runs = [this.#run.sort((a, b) => a - b)];
    for (const ref of this.#runs)
      runs.push(JSON.parse(new TextDecoder().decode(await this.file.read(ref))) as number[]);
    const merged = runs.flat().sort((a, b) => a - b);
    const out = allocateFlat(INT, 16);
    for (let start = 0; start < merged.length; start += 16) {
      if (out.data.layout === "i32") out.data.values.set(merged.slice(start, start + 16));
      await pushAndWait(this.downstream, batchOf(out, Math.min(16, merged.length - start)));
    }
    await this.downstream.finish();
    await this.file.close();
  }
  abort(reason: unknown) {
    this.downstream.abort(reason);
    void this.file.close();
  }
}

/** Join fan-out: each input row v matches v % 5 + 1 build rows; output batches of 8 rows. */
class FanOut implements Operator {
  readonly #out = allocateFlat(INT, 8);
  #filled = 0;
  #pending: Promise<void> = Promise.resolve();
  constructor(readonly downstream: Operator) {}
  #emit(rows: Int32Array, row: number, match: number): PushResult {
    for (; row < rows.length; row++, match = 0) {
      const value = rows[row] ?? 0;
      for (; match < (value % 5) + 1; match++) {
        if (this.#out.data.layout === "i32") this.#out.data.values[this.#filled++] = value;
        if (this.#filled < 8) continue;
        this.#filled = 0;
        const next = match + 1;
        if (this.downstream.push(batchOf(this.#out, 8)) === "blocked") {
          // Downstream full mid-input: resume this input row at the next match once it drains.
          this.#pending = this.downstream.ready().then(() => {
            if (this.#emit(rows, row, next) === "blocked") return this.#pending;
          });
          return "blocked";
        }
      }
    }
    return "ready";
  }
  push(batch: Batch): PushResult {
    return this.#emit(values(batch), 0, 0);
  }
  ready() {
    return this.#pending;
  }
  async finish() {
    if (this.#filled > 0) await pushAndWait(this.downstream, batchOf(this.#out, this.#filled));
    await this.downstream.finish();
  }
  abort(reason: unknown) {
    this.downstream.abort(reason);
  }
}

describe("pipeline push protocol", () => {
  const input = Array.from({ length: 1000 }, (_, i) => (i * 7919) % 300);

  it("hash aggregate spills mid-push and resumes the borrowed batch exactly", async () => {
    const governor = new Governor(16 << 20);
    const grant = await governor.admit({ label: "agg", minimumBytes: 0, spillable: true });
    const spill = new MemorySpillService();
    // Let the aggregate hold only ~100 groups so it must spill several times mid-batch.
    const filler = await governor.admit({
      label: "pressure",
      minimumBytes: governor.capBytes - governor.reserveBytes - 6400,
      spillable: false,
    });
    const sink = new Collect();
    const agg = new SpillingCount(grant, await spill.create(1), sink);
    await drive(new RecyclingSource(input), agg);
    expect(agg.spills).toBeGreaterThan(1);
    const expected = new Map<number, number>();
    for (const value of input) expected.set(value, (expected.get(value) ?? 0) + 1);
    const got = new Map<number, number>();
    for (let i = 0; i < sink.seen.length; i += 2)
      got.set(sink.seen[i] ?? -1, sink.seen[i + 1] ?? -1);
    expect(got).toEqual(expected);
    filler.close();
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
  });

  it("external sort spills runs and streams its output into a slow consumer", async () => {
    const spill = new MemorySpillService();
    const consumer = new Collect(2);
    await drive(new RecyclingSource(input), new ExternalSort(await spill.create(2), consumer));
    expect(consumer.seen).toEqual([...input].sort((a, b) => a - b));
    expect(consumer.maxInFlight).toBeLessThanOrEqual(2);
    expect(consumer.finished).toBe(true);
    expect(spill.bytesInUse).toBe(0);
  });

  it("join fan-out pauses mid-row on a full consumer and loses nothing", async () => {
    const consumer = new Collect(1);
    await drive(new RecyclingSource(input), new FanOut(consumer));
    expect(consumer.seen).toEqual(
      input.flatMap((value) => Array<number>((value % 5) + 1).fill(value)),
    );
    expect(consumer.maxInFlight).toBe(1);
  });

  it("bounds retained batches by the producer's credits (retain without copy)", async () => {
    const credits = 3;
    let free = credits;
    let outstanding = 0;
    let maxOutstanding = 0;
    let waiter: (() => void) | null = null;
    let produced = 0;
    const source: BatchSource = {
      async next() {
        if (produced === 40) return null;
        while (free === 0) await new Promise<void>((resolve) => (waiter = resolve));
        free -= 1;
        outstanding += 1;
        maxOutstanding = Math.max(maxOutstanding, outstanding);
        const vector = allocateFlat(INT, 1);
        if (vector.data.layout === "i32") vector.data.values[0] = produced++;
        const transfer: Transfer = {
          release() {
            free += 1;
            outstanding -= 1;
            waiter?.();
          },
        };
        return { batch: batchOf(vector, 1), transfer };
      },
      abort: () => undefined,
    };
    const kept: Array<{ value: number; transfer: Transfer }> = [];
    const seen: number[] = [];
    const retainer: Operator = {
      push(batch, transfer) {
        if (transfer === undefined) throw new TypeError("transfer expected");
        kept.push({ value: values(batch)[0] ?? -1, transfer });
        // Keeps the two newest batches (a window function's frame), releasing older ones.
        while (kept.length > 2) {
          const oldest = kept.shift();
          if (oldest !== undefined) {
            seen.push(oldest.value);
            oldest.transfer.release();
          }
        }
        return "ready";
      },
      ready: () => Promise.resolve(),
      async finish() {
        for (const entry of kept.splice(0)) {
          seen.push(entry.value);
          entry.transfer.release();
        }
      },
      abort: () => undefined,
    };
    await drive(source, retainer);
    expect(seen).toEqual(Array.from({ length: 40 }, (_, i) => i));
    expect(maxOutstanding).toBeLessThanOrEqual(credits);
    expect(outstanding).toBe(0);
  });

  it("cancellation while blocked rejects, and releases grants, spill files and buffers", async () => {
    const governor = new Governor(16 << 20);
    const grant = await governor.admit({ label: "agg", minimumBytes: 1024, spillable: true });
    const spill = new MemorySpillService();
    const stuck: Operator = {
      push: () => "blocked",
      ready: () => new Promise<void>(() => undefined),
      finish: () => Promise.resolve(),
      abort: () => undefined,
    };
    const agg = new SpillingCount(grant, await spill.create(3), stuck);
    const source = new RecyclingSource(input);
    const controller = new AbortController();
    // The aggregate's output is stuck, so the drive blocks in finish(); cancel it there.
    const running = drive(source, agg, controller.signal);
    setTimeout(() => controller.abort(new Error("cancelled by the client")), 20);
    await expect(running).rejects.toThrow("cancelled by the client");
    expect(source.aborted).toBe(true);
    await tick();
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  });
});
