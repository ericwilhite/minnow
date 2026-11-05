import { describe, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { packLocator } from "../vector/locator.js";
import { allocateValidity, isValid, setValid } from "../vector/validity.js";
import { VarlenBuilder, decodeUtf8 } from "../vector/varlen.js";
import { allocateFlat, type FlatVector } from "../vector/vector.js";
import {
  MemoryRowFetcher,
  SnapshotReleasedError,
  type FetchRequest,
  type SnapshotPin,
} from "./fetch.js";

const ID = 1;
const NAME = 2;
/** Two segments of 3,000 rows: id = segment × 10,000 + position, name = "n<id>" (null every 7th). */
const segments = [0, 1].map(
  (segment) =>
    new Map<number, Array<number | string | null>>([
      [ID, Array.from({ length: 3000 }, (_, p) => segment * 10_000 + p)],
      [
        NAME,
        Array.from({ length: 3000 }, (_, p) =>
          p % 7 === 0 ? null : `n${String(segment * 10_000 + p)}`,
        ),
      ],
    ]),
);

const pin = (): SnapshotPin => {
  let released = false;
  return {
    rootSequence: 1,
    get released() {
      return released;
    },
    release() {
      released = true;
    },
  };
};

function locators(entries: ReadonlyArray<readonly [number, number] | null>): FlatVector {
  const vector = allocateFlat({ kind: "locator", tableId: 1 }, entries.length);
  vector.validity = allocateValidity(entries.length);
  entries.forEach((entry, row) => {
    if (vector.data.layout === "f64" && entry !== null)
      vector.data.values[row] = packLocator(...entry);
    setValid(vector.validity ?? allocateValidity(1), row, entry !== null);
  });
  vector.length = entries.length;
  return vector;
}

async function setup(capacity: number, targetBytes = 1 << 20) {
  const governor = new Governor(16 << 20);
  const grant = await governor.admit({
    label: "fetch",
    minimumBytes: 256 * 1024,
    spillable: false,
  });
  const ids = allocateFlat({ kind: "int32", bits: 32 }, capacity);
  ids.validity = allocateValidity(capacity);
  const names = allocateFlat({ kind: "text" }, capacity);
  names.validity = allocateValidity(capacity);
  const builder = new VarlenBuilder(capacity, grant, targetBytes, 256);
  return { governor, grant, ids, names, builder };
}

function read(ids: FlatVector, names: FlatVector, builder: VarlenBuilder, count: number) {
  const data = builder.data();
  return Array.from({ length: count }, (_, k) => [
    isValid(ids.validity, k) && ids.data.layout === "i32" ? ids.data.values[k] : null,
    isValid(names.validity, k) ? decodeUtf8(data, k) : null,
  ]);
}

describe("row fetcher contract (review A1-2 finding 8)", () => {
  it("fills dense slots for selected, repeated and null-extended locators, reading each page once", async () => {
    const { grant, ids, names, builder } = await setup(16);
    const fetcher = new MemoryRowFetcher(segments);
    const entries = [[1, 2999], null, [0, 5], [1, 2999], [0, 1], [0, 2048], [0, 7]] as const;
    const request: FetchRequest = {
      snapshot: pin(),
      tableId: 1,
      locators: locators(entries),
      length: entries.length,
      selection: { rows: Uint16Array.from([0, 1, 3, 4, 5, 6]), count: 6 },
      from: 0,
      columns: [ID, NAME],
      output: [ids, names],
      varlen: [null, builder],
      grant,
    };
    expect(await fetcher.fetch(request)).toEqual({ done: true, filled: 6 });
    expect(read(ids, names, builder, 6)).toEqual([
      [12_999, "n12999"],
      [null, null],
      [12_999, "n12999"],
      [1, "n1"],
      [2048, "n2048"],
      [7, null],
    ]);
    // Pages touched: (1, 2), (0, 0), (0, 2) — each read once although (1, 2999) repeats.
    expect(fetcher.pageReads).toBe(3);
    expect(grant.allocated).toBe(builder.retainedBytes);
  });

  it("stops at the batch byte target and resumes exactly where it stopped", async () => {
    const { grant, ids, names, builder } = await setup(64, 64);
    const fetcher = new MemoryRowFetcher(segments);
    const entries = Array.from({ length: 40 }, (_, i) => [0, i + 1] as const);
    const all: Array<Array<number | string | null | undefined>> = [];
    let from = 0;
    for (let round = 0; from < entries.length; round++) {
      builder.reset();
      const result = await fetcher.fetch({
        snapshot: pin(),
        tableId: 1,
        locators: locators(entries),
        length: entries.length,
        selection: null,
        from,
        columns: [ID, NAME],
        output: [ids, names],
        varlen: [null, builder],
        grant,
      });
      all.push(...read(ids, names, builder, result.filled));
      expect(builder.rows).toBe(result.filled);
      from += result.filled;
      if (!result.done) expect(result.reason).toBe("full");
      expect(round).toBeLessThan(40);
    }
    expect(all.map(([id]) => id)).toEqual(entries.map(([, p]) => p));
  });

  it("round-trips locators through a spill page and fetches the same rows", async () => {
    const { grant, ids, names, builder } = await setup(8);
    const entries = [[1, 10], null, [0, 3]] as const;
    const original = locators(entries);
    const spill = await new MemorySpillService().create(1);
    const page = new Uint8Array(8 * 3 + 4);
    if (original.data.layout === "f64")
      page.set(new Uint8Array(original.data.values.buffer, 0, 24));
    page.set(new Uint8Array((original.validity ?? allocateValidity(1)).buffer, 0, 4), 24);
    const back = await spill.read(await spill.append(page));
    const restored: FlatVector = {
      form: "flat",
      type: original.type,
      length: 3,
      validity: new Uint32Array(back.slice(24, 28).buffer),
      data: { layout: "f64", values: new Float64Array(back.slice(0, 24).buffer) },
    };
    const fetch = (vector: FlatVector) =>
      new MemoryRowFetcher(segments).fetch({
        snapshot: pin(),
        tableId: 1,
        locators: vector,
        length: 3,
        selection: null,
        from: 0,
        columns: [ID, NAME],
        output: [ids, names],
        varlen: [null, builder],
        grant,
      });
    await fetch(restored);
    const viaSpill = read(ids, names, builder, 3);
    builder.reset();
    await fetch(original);
    expect(viaSpill).toEqual(read(ids, names, builder, 3));
    expect(viaSpill[1]).toEqual([null, null]);
    await spill.close();
  });

  it("is cancellable mid-read under a small grant and refuses a released snapshot", async () => {
    const { governor, grant, ids, names, builder } = await setup(4096);
    const controller = new AbortController();
    const fetcher = new MemoryRowFetcher(segments, 64, async () => {
      await new Promise((resolve) => setTimeout(resolve, 1));
      if (fetcher.pageReads === 3) controller.abort(new Error("cancelled by the client"));
    });
    const entries = Array.from({ length: 3000 }, (_, i) => [i % 2, i] as const);
    const request: FetchRequest = {
      snapshot: pin(),
      tableId: 1,
      locators: locators(entries),
      length: entries.length,
      selection: null,
      from: 0,
      columns: [ID, NAME],
      output: [ids, names],
      varlen: [null, builder],
      grant,
      signal: controller.signal,
    };
    await expect(fetcher.fetch(request)).rejects.toThrow("cancelled by the client");
    expect(grant.allocated).toBe(builder.retainedBytes);
    const released = pin();
    released.release();
    await expect(
      fetcher.fetch({ ...request, snapshot: released, signal: new AbortController().signal }),
    ).rejects.toThrow(SnapshotReleasedError);
    builder.close();
    grant.close();
    expect(governor.usedBytes).toBe(0);
  });
});
