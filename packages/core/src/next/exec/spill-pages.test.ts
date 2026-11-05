/**
 * Spill pages (exec/spill-pages.ts): every `VectorData` variant round-trips exactly through the
 * spill contract's page layout, and a corrupt, truncated or mistyped page is refused, never read
 * as rows. Law: spill/contract.ts "Files and pages".
 */
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { SpillCorruptionError } from "../spill/contract.js";
import { MemorySpillFile, MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import { setInt128 } from "../vector/int128.js";
import type { FlatVector, VectorData } from "../vector/vector.js";
import { defined } from "./join-fixtures.js";
import { PageWriter, SpillFiles, SpilledRun, decodePage, pageHeader } from "./spill-pages.js";

const FREE = { allocate: () => true, free: () => undefined };
const SEEDS = seedsFor("next-spill-pages", [0x5b11, 7]);

function validity(n: number, random: () => number): Uint32Array | null {
  if (random() < 0.3) return null;
  const bits = new Uint32Array((n + 31) >>> 5);
  for (let i = 0; i < n; i++)
    if (random() < 0.8) bits[i >>> 5] = (bits[i >>> 5] ?? 0) | (1 << (i & 31));
  return bits;
}

/** One flat vector per variant, random contents. */
function variants(n: number, random: () => number): FlatVector[] {
  const int = (bound: number) => Math.floor(random() * bound) - bound / 2;
  const big = () => BigInt(int(2 ** 40)) * BigInt(int(2 ** 20) + 1);
  const make = (type: PhysicalType, data: VectorData): FlatVector => ({
    form: "flat",
    type,
    length: n,
    validity: validity(n, random),
    data,
  });
  const varlen = () => {
    const offsets = new Int32Array(n + 1);
    const prefix = 5;
    let total = prefix;
    offsets[0] = prefix;
    for (let i = 0; i < n; i++) {
      total += Math.floor(random() * 20);
      offsets[i + 1] = total;
    }
    const bytes = Uint8Array.from({ length: total }, () => Math.floor(random() * 256));
    return { layout: "varlen" as const, bytes, offsets };
  };
  const limbs = new Uint32Array(n * 4);
  for (let i = 0; i < n; i++) setInt128(limbs, i, big() * big());
  return [
    make(
      { kind: "bool" },
      { layout: "bool", values: Uint8Array.from({ length: n }, () => (random() < 0.5 ? 1 : 0)) },
    ),
    make(
      { kind: "int32", bits: 32 },
      { layout: "i32", values: Int32Array.from({ length: n }, () => int(2 ** 31)) },
    ),
    make(
      { kind: "date" },
      { layout: "i32", values: Int32Array.from({ length: n }, () => int(2 ** 20)) },
    ),
    make(
      { kind: "float32" },
      { layout: "f32", values: Float32Array.from({ length: n }, () => random() * 100) },
    ),
    make(
      { kind: "float64" },
      {
        layout: "f64",
        values: Float64Array.from({ length: n }, () => (random() < 0.1 ? Number.NaN : random())),
      },
    ),
    make(
      { kind: "locator", tableId: 3 },
      { layout: "f64", values: Float64Array.from({ length: n }, (_, i) => i * 65_536 + 7) },
    ),
    make(
      { kind: "int64" },
      {
        layout: "int64",
        values: { repr: "safe", values: Float64Array.from({ length: n }, () => int(2 ** 50)) },
      },
    ),
    make(
      { kind: "timestamp", zoned: true },
      {
        layout: "int64",
        values: { repr: "int64", values: BigInt64Array.from({ length: n }, big) },
      },
    ),
    make(
      { kind: "uuid" },
      {
        layout: "fixed16",
        bytes: Uint8Array.from({ length: n * 16 }, () => Math.floor(random() * 256)),
      },
    ),
    make(
      { kind: "interval" },
      {
        layout: "interval",
        months: Int32Array.from({ length: n }, () => int(1000)),
        days: Int32Array.from({ length: n }, () => int(1000)),
        micros: { repr: "int64", values: BigInt64Array.from({ length: n }, big) },
      },
    ),
    make({ kind: "text" }, varlen()),
    make({ kind: "json", binary: true }, varlen()),
    make(
      { kind: "decimal", precision: 12, scale: 2 },
      {
        layout: "decimal",
        scale: 2,
        scales: null,
        coefficients: {
          repr: "safe",
          values: Float64Array.from({ length: n }, () => int(2 ** 40)),
        },
        specials: null,
      },
    ),
    make(
      { kind: "decimal", precision: null, scale: null },
      {
        layout: "decimal",
        scale: null,
        scales: Uint16Array.from({ length: n }, () => Math.floor(random() * 30)),
        coefficients: { repr: "int128", limbs },
        specials: Uint8Array.from({ length: n }, () => Math.floor(random() * 4)),
      },
    ),
    make(
      { kind: "decimal", precision: null, scale: null },
      {
        layout: "decimal",
        scale: 4,
        scales: null,
        coefficients: {
          repr: "wide",
          values: Array.from({ length: n }, () => big() * big() * big() * big()),
        },
        specials: null,
      },
    ),
    make(
      { kind: "decimal", precision: 30, scale: 3 },
      {
        layout: "decimal",
        scale: 3,
        scales: null,
        coefficients: { repr: "int64", values: BigInt64Array.from({ length: n }, big) },
        specials: null,
      },
    ),
  ];
}

/** Valid slots of a vector as comparable values (garbage under nulls is not compared). */
function values(vector: FlatVector): unknown[] {
  const out: unknown[] = [];
  const data = vector.data;
  for (let i = 0; i < vector.length; i++) {
    if (vector.validity !== null && ((vector.validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0) {
      out.push("NULL");
      continue;
    }
    switch (data.layout) {
      case "bool":
      case "i32":
      case "f32":
      case "f64":
        out.push(Object.is(data.values[i], Number.NaN) ? "NaN" : data.values[i]);
        break;
      case "int64":
        out.push(String(data.values.values[i]));
        break;
      case "fixed16":
        out.push([...data.bytes.subarray(i * 16, i * 16 + 16)]);
        break;
      case "interval":
        out.push([data.months[i], data.days[i], String(data.micros.values[i])]);
        break;
      case "varlen":
        out.push([...data.bytes.subarray(data.offsets[i] ?? 0, data.offsets[i + 1] ?? 0)]);
        break;
      case "decimal": {
        const c = data.coefficients;
        const coefficient =
          c.repr === "int128" ? [...c.limbs.subarray(i * 4, i * 4 + 4)] : String(c.values[i]);
        out.push([c.repr, coefficient, data.scale ?? data.scales?.[i], data.specials?.[i] ?? 0]);
        break;
      }
    }
  }
  return out;
}

describe("spill pages", () => {
  for (const seed of SEEDS)
    it(`round-trips every vector variant exactly (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      for (const n of [0, 1, 31, 32, 33, 700]) {
        const columns = variants(n, random);
        const writer = new PageWriter(FREE, 64);
        const page = writer.encode({ queryId: 9, run: 2, partition: 5, sequence: 11 }, columns, n);
        if (page === null) throw new Error("refused");
        expect(page.length).toBe(PageWriter.encodedBytes(columns, n));
        // Through a spill file, so the checksum and copy are real.
        const service = new MemorySpillService();
        const files = new SpillFiles(service, 9, FREE);
        const run = new SpilledRun();
        await files.append(run, page, n);
        const ref = run.pages[0];
        if (ref === undefined || run.file === null) throw new Error("no page");
        const bytes = await run.file.read(ref);
        expect(pageHeader(bytes)).toMatchObject({
          queryId: 9,
          run: 2,
          partition: 5,
          sequence: 11,
          rows: n,
        });
        const decoded = decodePage(
          bytes,
          columns.map((c) => c.type),
        );
        expect(decoded.rows).toBe(n);
        decoded.columns.forEach((column, c) => {
          const original = columns[c];
          if (original === undefined) throw new Error("column");
          expect(column.data.layout).toBe(original.data.layout);
          if (original.data.layout === "int64" && column.data.layout === "int64")
            expect(column.data.values.repr).toBe(original.data.values.repr);
          expect(values(column)).toEqual(values(original));
        });
        await files.closeAll();
        expect(service.bytesInUse).toBe(0);
      }
    });

  it("refuses a flipped byte, a wrong schema and a truncated page", async () => {
    const random = mulberry32(3);
    const columns = variants(50, random);
    const types = columns.map((c) => c.type);
    const page = new PageWriter(FREE).encode(
      { queryId: 1, run: 0, partition: 0, sequence: 0 },
      columns,
      50,
    );
    if (page === null) throw new Error("refused");
    const file = (await new MemorySpillService().create(1)) as MemorySpillFile;
    const ref = await file.append(page);
    file.corrupt(ref, 200);
    await expect(file.read(ref)).rejects.toBeInstanceOf(SpillCorruptionError);
    const copy = page.slice();
    expect(() => decodePage(copy, types.slice(1))).toThrow(SpillCorruptionError);
    expect(() => decodePage(copy, [{ kind: "text" }, ...types.slice(1)])).toThrow(
      SpillCorruptionError,
    );
    expect(() => decodePage(copy.subarray(0, copy.length - 40), types)).toThrow(
      SpillCorruptionError,
    );
    const badMagic = copy.slice();
    badMagic[0] = 0;
    expect(() => decodePage(badMagic, types)).toThrow(SpillCorruptionError);
    // A varlen offset that runs backwards is corruption, not a short string.
    const textColumns = [defined(columns[10])];
    const textPage = new PageWriter(FREE).encode(
      { queryId: 1, run: 0, partition: 0, sequence: 0 },
      textColumns,
      50,
    );
    if (textPage === null) throw new Error("refused");
    const broken = textPage.slice();
    const offsetsAt = 32 + 16 + (textColumns[0]?.validity === null ? 0 : 8 + 8) + 8;
    broken[offsetsAt + 4 * 10] = 0xff;
    broken[offsetsAt + 4 * 10 + 1] = 0xff;
    expect(() => decodePage(broken, [{ kind: "text" }])).toThrow(SpillCorruptionError);
  });

  it("charges a growing page buffer and refuses when the charge does", () => {
    let held = 0;
    let limit = 1024 * 1024;
    const charge = {
      allocate: (bytes: number) => {
        if (held + bytes > limit) return false;
        held += bytes;
        return true;
      },
      free: (bytes: number) => {
        held -= bytes;
      },
    };
    const writer = new PageWriter(charge, 1024);
    const columns = variants(500, mulberry32(4));
    expect(
      writer.encode({ queryId: 1, run: 0, partition: 0, sequence: 0 }, columns, 500),
    ).not.toBeNull();
    expect(held).toBe(writer.retainedBytes);
    limit = held;
    expect(
      writer.encode(
        { queryId: 1, run: 0, partition: 0, sequence: 0 },
        variants(4000, mulberry32(5)),
        4000,
      ),
    ).toBeNull();
    writer.close();
    expect(held).toBe(0);
  });
});
