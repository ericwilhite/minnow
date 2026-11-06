/**
 * Review B1-p2d finding 4 on IndexedDB byte spaces (idb-bytes.ts, indexeddb.ts): the in-memory
 * record index is blocked (insert and delete O(log n + block)) and charged to a memory
 * registration, and small log appends of one batch share records.
 */
import { describe, expect, it } from "vitest";
import { expectBytes } from "./space-test-support.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { ByteStore, CHARGE_STEP, INDEX_BLOCK, IntervalIndex, type Interval } from "./idb-bytes.js";
import { LOG_COALESCE_BYTES, createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

const SEEDS = seedsFor("next-idb-index", [0x1d4, 31]);

const filled = (length: number, byte: number) => new Uint8Array(length).fill(byte);

describe("the in-memory index (finding 4)", () => {
  it.each(SEEDS)("matches a naive model through growth, churn and shrinking (seed %i)", (seed) => {
    const random = mulberry32(seed);
    const index = new IntervalIndex();
    // The model: sorted starts and their ends.
    const starts: number[] = [];
    const ends = new Map<number, number>();
    const position = (x: number) => {
      let low = 0;
      let high = starts.length;
      while (low < high) {
        const middle = (low + high) >>> 1;
        if ((starts[middle] ?? 0) < x) low = middle + 1;
        else high = middle;
      }
      return low;
    };
    const modelOverlapping = (from: number, to: number): Interval[] => {
      const out: Interval[] = [];
      for (let at = Math.max(0, position(from) - 1); at < starts.length; at++) {
        const start = starts[at] ?? 0;
        const end = ends.get(start) ?? 0;
        if (start >= to) break;
        if (end > from) out.push({ start, end });
      }
      return out;
    };
    const SLOTS = 40_000;
    // Growth (inserts only), churn, then shrinking: blocks split, empty and merge.
    const phases = [
      { steps: 12_000, insert: 1 },
      { steps: 8_000, insert: 0.5 },
      { steps: 6_000, insert: 0.1 },
    ];
    let step = 0;
    for (const phase of phases) {
      for (let done = 0; done < phase.steps; done++, step++) {
        const at = Math.floor(random() * SLOTS) * 8;
        if (random() < phase.insert) {
          const end = at + 1 + Math.floor(random() * 7);
          if (modelOverlapping(at, end).length > 0) continue;
          index.insert(at, end);
          starts.splice(position(at), 0, at);
          ends.set(at, end);
        } else {
          const last = at + Math.floor(random() * (random() < 0.05 ? 20_000 : 600));
          const removed = index.removeStarts(at, last);
          const low = position(at);
          const high = position(last + 1);
          const expected = starts
            .slice(low, high)
            .map((start) => ({ start, end: ends.get(start) ?? 0 }));
          expect(removed, `step ${String(step)}`).toEqual(expected);
          for (const start of starts.splice(low, high - low)) ends.delete(start);
        }
        if (step % 1000 === 0) {
          index.check();
          expect(index.count).toBe(starts.length);
          const from = Math.floor(random() * SLOTS * 8);
          const to = from + Math.floor(random() * 40_000);
          expect(index.overlapping(from, to)).toEqual(modelOverlapping(from, to));
        }
      }
      index.check();
      expect(index.count).toBe(starts.length);
      expect(index.overlapping(0, Infinity)).toEqual(modelOverlapping(0, Infinity));
      // Growth only splits full blocks, so blocks stay at least half full.
      if (phase.insert === 1) {
        expect(index.count).toBeGreaterThan(8 * INDEX_BLOCK);
        expect(index.blocks).toBeLessThanOrEqual(Math.ceil(index.count / (INDEX_BLOCK / 2)) + 1);
      }
    }
  });

  it("charges its size to the memory registration, counts what is refused, and releases at close", () => {
    let charged = 0;
    let cap = 3 * CHARGE_STEP;
    const memory = {
      charge(bytes: number) {
        if (charged + bytes > cap) return false;
        charged += bytes;
        return true;
      },
      release(bytes: number) {
        charged -= bytes;
      },
    };
    const index = new IntervalIndex(memory);
    for (let at = 0; at < 20_000; at++) index.insert(at * 8, at * 8 + 4);
    const wanted = Math.ceil(index.bytes / CHARGE_STEP) * CHARGE_STEP;
    expect(wanted).toBeGreaterThan(cap);
    expect(index.charge.charged + index.charge.overdraft).toBe(wanted);
    expect(index.charge.overdraft).toBeGreaterThan(0);
    expect(charged).toBe(index.charge.charged);
    // Room appears: the next change charges the overdraft.
    cap = Infinity;
    index.insert(1_000_000, 1_000_001);
    expect(index.charge.overdraft).toBe(0);
    expect(charged).toBe(Math.ceil(index.bytes / CHARGE_STEP) * CHARGE_STEP);
    // Shrinking returns granules.
    index.removeStarts(0, 70_000);
    expect(charged).toBe(Math.ceil(index.bytes / CHARGE_STEP) * CHARGE_STEP);
    index.release();
    expect(charged).toBe(0);
  });

  it("the backend charges both spaces' indexes and releases them at close", async () => {
    let charged = 0;
    const memory = {
      charge(bytes: number) {
        charged += bytes;
        return true;
      },
      release(bytes: number) {
        charged -= bytes;
      },
    };
    const db = simIndexedDb(new SimDisk());
    const backend = createIndexedDbBackend({
      database: () => db,
      name: "db",
      locks: new SimLocks(),
      memory,
    });
    const session = await backend.open(await backend.acquire());
    await session.writeData(0, filled(4096, 1));
    await session.appendLog(filled(10, 2));
    await session.flushLog();
    expect(charged).toBe(2 * CHARGE_STEP);
    await session.close();
    expect(charged).toBe(0);
  });

  it("joins small log appends of one batch into shared records", async () => {
    const db = simIndexedDb(new SimDisk());
    const backend = createIndexedDbBackend({
      database: () => db,
      name: "db",
      locks: new SimLocks(),
    });
    const session = await backend.open(await backend.acquire());
    const expected = new Uint8Array(1000 * 200 + 100_000);
    let length = 0;
    for (let at = 0; at < 1000; at++) {
      const bytes = filled(200, at & 0xff);
      expect(await session.appendLog(bytes)).toBe(length);
      bytes.fill(0); // the caller may reuse its buffer once the call resolves
      expected.fill(at & 0xff, length, length + 200);
      length += 200;
    }
    // A large append goes alone; the small ones after it start a new run.
    const large = filled(100_000, 9);
    await session.appendLog(large);
    expected.set(large, length);
    length += large.length;
    await session.flushLog();
    const log = await ByteStore.load(db, "log");
    // 200,000 bytes in ≤ 64 KiB runs: 4 records (and 1 for the large append).
    expect(log.records).toBe(Math.ceil(200_000 / LOG_COALESCE_BYTES) + 1);
    expectBytes(await session.readLog(0, length), expected.subarray(0, length), "log");
    await session.close();
  });
});
