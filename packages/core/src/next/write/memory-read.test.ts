/** B4's bounded provider contract: one logical prefix survives publication without new outputs. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MEMORY_READ_BOUND_STATES } from "../../testing/reach.js";
import type {
  ChunkOutput,
  MemoryOutputNeed,
  MemoryReadContext,
  MemoryReadHint,
  MemoryReadRequest,
  MemorySegment,
  Snapshot,
} from "../access/snapshot.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type { PhysicalType } from "../types.js";
import { int128At, setInt128 } from "../vector/int128.js";
import { int64At } from "../vector/int64.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { allocateData, type FlatVector } from "../vector/vector.js";
import { memoryNeed, memoryOutputLayout, memoryValid } from "./memory-output.js";
import { LazyGroupRoot, type Memtable } from "./memtable.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  locators,
  rules,
  tableInput,
  texts,
} from "./test-support.js";
import { ChunkReader } from "../page/reader.js";
import { bigintBytes } from "../memory/governor.js";
import { beginTask, taskClock } from "../exec/pipeline.js";

const MiB = 1024 * 1024;
const reach = createReach("bounded memory reads", MEMORY_READ_BOUND_STATES);
class Budget implements ByteBudget {
  used = 0;
  peak = 0;
  constructor(readonly cap = 64 * MiB) {}
  allocate(n: number): boolean {
    expect(Number.isSafeInteger(n) && n >= 0).toBe(true);
    if (this.used + n > this.cap) return false;
    this.used += n;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(n: number): void {
    this.used -= n;
    expect(this.used).toBeGreaterThanOrEqual(0);
  }
  reset(): void {
    expect(this.used).toBe(0);
    this.peak = 0;
  }
}
/** Independent native ownership count: every backing, rather than the provider's byte formula. */
function buffersOf(value: unknown, out = new Set<ArrayBufferLike>()): Set<ArrayBufferLike> {
  if (ArrayBuffer.isView(value)) out.add(value.buffer);
  else if (value !== null && typeof value === "object")
    for (const child of Object.values(value)) buffersOf(child, out);
  return out;
}
function prepare(
  type: PhysicalType,
  need: MemoryOutputNeed,
  budget: Budget,
): { out: ChunkOutput; close(): void } {
  if (!budget.allocate(need.bytes)) throw new Error("fixture output does not fit");
  const layout = need.shape.layout;
  const data =
    layout === "int64"
      ? { layout, values: { repr: "int64" as const, values: new BigInt64Array(need.rows) } }
      : layout === "interval"
        ? {
            layout,
            months: new Int32Array(need.rows),
            days: new Int32Array(need.rows),
            micros: { repr: "int64" as const, values: new BigInt64Array(need.rows) },
          }
        : layout === "decimal"
          ? {
              layout,
              scale: null,
              scales: new Uint16Array(need.rows),
              specials: new Uint8Array(need.rows),
              coefficients: { repr: "int128" as const, limbs: new Uint32Array(need.rows * 4) },
            }
          : allocateData(type, need.rows, need.varlenBytes);
  const flat: FlatVector = {
    form: "flat",
    type,
    length: 0,
    validity: new Uint32Array(Math.ceil(need.rows / 32)),
    data,
  };
  const out = { flat, codes: new Int32Array(need.rows), varlen: null };
  expect([...buffersOf(out)].reduce((sum, buffer) => sum + buffer.byteLength, 0)).toBe(need.bytes);
  return { out, close: () => budget.free(need.bytes) };
}
function values(vector: FlatVector): unknown[] {
  return Array.from({ length: vector.length }, (_, k) => {
    if (!memoryValid(vector, k)) return null;
    const d = vector.data;
    switch (d.layout) {
      case "bool":
      case "i32":
      case "f32":
      case "f64":
        return d.values[k];
      case "int64":
        return int64At(d.values, k);
      case "decimal":
        return [
          (d.specials?.[k] ?? 0) !== 0
            ? 0n
            : d.coefficients.repr === "int128"
              ? int128At(d.coefficients.limbs, k)
              : d.coefficients.repr === "wide"
                ? d.coefficients.values[k]
                : int64At(d.coefficients, k),
          d.scale ?? d.scales?.[k],
          d.specials?.[k] ?? 0,
        ];
      case "interval":
        return [d.months[k], d.days[k], int64At(d.micros, k)];
      case "fixed16":
        return Array.from(d.bytes.subarray(k * 16, (k + 1) * 16));
      case "varlen":
        return Buffer.from(d.bytes.subarray(d.offsets[k], d.offsets[k + 1])).toString("hex");
    }
  });
}
const kinds = [
  "text",
  "bytes",
  "json",
  "array",
  "int64",
  "decimal",
  "interval",
  "float64",
  "uuid",
] as const;
type Kind = (typeof kinds)[number];
function input(kind: Kind, n: number, width: number, nullable: boolean, start = 0): FlatVector {
  const nulls = nullable ? Uint32Array.from({ length: Math.ceil(n / 32) }, () => 0xffffffff) : null;
  if (nulls !== null && n > 2) nulls[0] = (nulls[0] ?? 0) & ~4;
  if (kind === "text" || kind === "bytes" || kind === "json" || kind === "array") {
    if (kind === "array") {
      const payloads = Array.from({ length: n }, (_, k) =>
        encodeArrayValue(INT, { dimensions: [2], lowerBounds: [-1], elements: [k + start, null] }),
      );
      const size = payloads.reduce((s, b) => s + b.length, 0),
        bytes = new Uint8Array(size),
        offsets = new Int32Array(n + 1);
      let at = 0;
      for (const [k, b] of payloads.entries()) {
        bytes.set(b, at);
        at += b.length;
        offsets[k + 1] = at;
      }
      return {
        form: "flat",
        type: { kind: "array", element: INT },
        length: n,
        validity: nulls,
        data: { layout: "varlen", bytes, offsets },
      };
    }
    const vector = texts(
      Array.from({ length: n }, (_, k) =>
        nullable && k === 2
          ? null
          : kind === "json"
            ? JSON.stringify("x".repeat(width) + String(k + start))
            : "x".repeat(width) + String(k + start),
      ),
    );
    return { ...vector, type: kind === "json" ? { kind, binary: false } : { kind } };
  }
  const type: PhysicalType = kind === "decimal" ? { kind, precision: null, scale: null } : { kind };
  let data = allocateData(type, n);
  if (data.layout === "int64")
    data = {
      layout: "int64",
      values: {
        repr: "int64",
        values: BigInt64Array.from({ length: n }, (_, k) =>
          k === 0 ? 5n : 2n ** 60n + BigInt(k + start),
        ),
      },
    };
  if (data.layout === "decimal") {
    const limbs = new Uint32Array(n * 4);
    for (let k = 0; k < n; k++) setInt128(limbs, k, 10n ** 30n + BigInt(k + start));
    data = {
      ...data,
      coefficients: { repr: "int128", limbs },
      scales: Uint16Array.from({ length: n }, (_, k) => k % 4),
      specials: Uint8Array.from({ length: n }, (_, k) => (k === 1 ? 1 : 0)),
    };
  }
  if (data.layout === "interval")
    data = {
      ...data,
      months: Int32Array.from({ length: n }, (_, k) => k),
      days: Int32Array.from({ length: n }, (_, k) => -k),
      micros: {
        repr: "int64",
        values: BigInt64Array.from({ length: n }, (_, k) => 2n ** 60n + BigInt(k)),
      },
    };
  if (data.layout === "f64") data.values.set(Array.from({ length: n }, (_, k) => (k + start) / 10));
  if (data.layout === "fixed16")
    data.bytes.set(Uint8Array.from({ length: n * 16 }, (_, k) => (k + start) % 256));
  return { form: "flat", type, length: n, validity: nulls, data };
}
async function table(h: Harness, vector: FlatVector): Promise<number> {
  let id = 0;
  await autocommit(h.path, async (s) => {
    id =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", vector.type)]),
        })
      ).id ?? 0;
  });
  await autocommit(h.path, (s) =>
    s
      .table(id)
      .insert(batch(ints(Array.from({ length: vector.length }, (_, k) => k)), vector), null),
  );
  return id;
}
function memory(snapshot: Snapshot, id: number): { segment: MemorySegment; id: number } {
  const t = snapshot.table(id, { budget: new Budget() });
  if (t instanceof Promise) throw new Error("resident table fixture unexpectedly asynchronous");
  const segment = t.segments.find((s) => s.kind === "memory");
  if (segment?.kind !== "memory") throw new Error("missing memory segment");
  return { segment, id: t.columns[1]?.id ?? -1 };
}
/** Parent RED uses the old API itself, not a missing-method/setup error. */
async function hintFor(
  segment: MemorySegment,
  request: MemoryReadRequest,
  context: MemoryReadContext,
  type: PhysicalType,
): Promise<MemoryReadHint> {
  if (typeof segment.readHint === "function") return segment.readHint(request, context);
  const count = Math.min(request.maxRows, segment.rowCount - request.startRow);
  const shape = memoryOutputLayout(type),
    need = memoryNeed(request.columns[0] ?? -1, request.startRow, count, shape, 4 * MiB);
  return {
    request,
    rowCount: count,
    outputBytes: need.bytes,
    outputs: [need],
    residentBytes: 32 * MiB,
    scratchBytes: 4 * MiB,
    ioBytes: 4 * MiB,
    oversized: false,
    token: Object.freeze({}),
  };
}

function generated(name: string, work: (width: number) => Promise<void>): void {
  for (const seed of seedsFor("next-bounded-memory-phases", [0xb043]))
    it(`${name} (seed ${String(seed)})`, async () => {
      await fc.assert(fc.asyncProperty(fc.integer({ min: 1000, max: 4000 }), work), {
        seed,
        numRuns: 8,
      });
    }, 60000);
}

for (const seed of seedsFor("next-bounded-memory-read", [0xb04d, 2718, 90210]))
  it(`keeps logical prefixes and original bounds across publication (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          kind: fc.constantFrom(...kinds),
          n: fc.integer({ min: 4, max: 12 }),
          start: fc.nat(),
          rows: fc.integer({ min: 1, max: 4 }),
          target: fc.constantFrom(64, 256, 4096),
          nullable: fc.boolean(),
          deleted: fc.boolean(),
          large: fc.boolean(),
          mixed: fc.boolean(),
          publish: fc.boolean(),
          lazy: fc.boolean(),
          width: fc.integer({ min: 1, max: 300 }),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.kind);
          const large = s.large && (s.kind === "text" || s.kind === "bytes");
          const n = large ? 3 : s.n;
          if (large) reach.reach("multi blob");
          if (s.mixed) reach.reach("mixed columns");
          const vector = input(s.kind, n, large ? 2 * MiB + 17 : s.width, s.nullable),
            expected = values(vector),
            h = new Harness("strict", "memory", 32);
          await h.open();
          let snapshot: Snapshot | undefined;
          try {
            const t = await table(h, vector);
            if (s.deleted) {
              await autocommit(h.path, (st) => st.table(t).delete(batch(locators([0], t)), null));
              reach.reach("deletions");
            }
            snapshot = h.path.pin();
            const { segment, id } = memory(snapshot, t),
              budget = new Budget();
            expect((await segment.deletions())?.isDeleted(0) ?? false).toBe(s.deleted);
            let reads = 0;
            const pages: SpaceReader = {
              peek: () => null,
              read: async (refs, options) => {
                reads++;
                expect(refs.reduce((a, r) => a + r.length, 0)).toBeLessThanOrEqual(4 * MiB);
                expect(options.signal).toBeDefined();
                return h.space.read(refs, options);
              },
            };
            const ctx: MemoryReadContext = { budget, pages, signal: new AbortController().signal };
            const pk = snapshot.table(t, { budget });
            if (pk instanceof Promise) throw new Error("unexpected table wait");
            const pkId = pk.columns[0]?.id ?? -1;
            const request = {
              columns: s.mixed ? [id, pkId] : [id],
              chunk: 0,
              startRow: s.start % n,
              maxRows: s.rows,
              targetBytes: s.target,
            };
            const hint = await hintFor(segment, request, ctx, vector.type);
            expect(budget.used).toBe(0);
            expect(hint.outputs.reduce((sum, o) => sum + o.bytes, 0)).toBe(hint.outputBytes);
            expect(h.path.stats().borrowedChunkBytes).toBe(0);
            reach.reach("no sizing borrow");
            if (s.publish) {
              const suffix = input(s.kind, 32 - n, 5, true, n);
              await autocommit(h.path, (st) =>
                st
                  .table(t)
                  .insert(
                    batch(ints(Array.from({ length: suffix.length }, (_, k) => n + k)), suffix),
                    null,
                  ),
              );
              await autocommit(h.path, (st) =>
                st.table(t).insert(batch(ints([32]), input(s.kind, 1, 5, false, 32)), null),
              );
              await h.path.quiesce();
              expect(h.path.stats().repointPins).toBeGreaterThan(0);
              reach.reach("repointed");
              reach.reach("invisible suffix");
              if (s.lazy) {
                const m = (segment as unknown as { memtable: Memtable }).memtable,
                  published = m.published;
                if (published === null) throw new Error("missing publication");
                m.published = {
                  ...published,
                  groupRoot: new LazyGroupRoot(() => {
                    throw new Error("bounded read used the writer's lazy callback");
                  }),
                };
                reach.reach("lazy root");
              }
            } else reach.reach("resident");
            expect((await segment.deletions())?.isDeleted(0) ?? false).toBe(s.deleted);
            const later = await hintFor(segment, request, ctx, vector.type);
            expect(later.rowCount).toBe(hint.rowCount);
            for (const field of [
              "outputBytes",
              "residentBytes",
              "scratchBytes",
              "ioBytes",
            ] as const)
              expect(later[field]).toBeLessThanOrEqual(hint[field]);
            const required = hint.outputs[0];
            if (required === undefined) throw new Error("missing prepared shape");
            const output = prepare(vector.type, required, budget),
              original = output.out.flat.data,
              originalMask = output.out.flat.validity,
              originalBuffers = buffersOf(output.out);
            budget.peak = budget.used;
            const r = await segment.chunk(id, 0, output.out, request.startRow, {
              hint,
              context: ctx,
            });
            expect(r.rowCount).toBe(hint.rowCount);
            expect(r.firstRow).toBe(request.startRow);
            expect([...output.out.codes.subarray(0, r.rowCount)]).toEqual(
              Array.from({ length: r.rowCount }, (_, row) => row),
            );
            expect(r.reason).toBe(request.startRow + r.rowCount === n ? null : "full");
            expect(values(r.vector as FlatVector)).toEqual(
              expected.slice(request.startRow, request.startRow + hint.rowCount),
            );
            expect(output.out.flat.data).toBe(original);
            expect(output.out.flat.validity).toBe(originalMask);
            expect(buffersOf(output.out)).toEqual(originalBuffers);
            expect(budget.peak).toBeLessThanOrEqual(
              required.bytes + hint.residentBytes + hint.scratchBytes + hint.ioBytes,
            );
            expect(budget.used).toBe(required.bytes);
            if (s.mixed) {
              const pkNeed = hint.outputs[1];
              if (!pkNeed) throw new Error("missing second output");
              const second = prepare(INT, pkNeed, budget);
              const a = await segment.chunk(pkId, 0, second.out, request.startRow, {
                hint,
                context: ctx,
              });
              expect(a.rowCount).toBe(hint.rowCount);
              expect(values(a.vector as FlatVector)).toEqual(
                Array.from({ length: hint.rowCount }, (_, k) => request.startRow + k),
              );
              expect(budget.peak).toBeLessThanOrEqual(
                hint.outputBytes + hint.residentBytes + hint.scratchBytes + hint.ioBytes,
              );
              second.close();
            }
            output.close();
            expect(budget.used).toBe(0);
            if (s.publish) expect(reads).toBeGreaterThan(0);
            if (hint.rowCount < Math.min(s.rows, n - request.startRow)) reach.reach("byte prefix");
            if (hint.oversized) {
              expect(hint.rowCount).toBe(1);
              reach.reach("oversized row");
            } else reach.reach("row prefix");
          } finally {
            snapshot?.release();
            await h.close();
          }
        },
      ),
      { seed, numRuns: 36 },
    );
  }, 120000);

it.each([false, true])(
  "reports a native integer or first-null need without replacing buffers (published=%s)",
  async (publish) => {
    const h = new Harness("strict", "memory", 4),
      vector = input("int64", 3, 0, true);
    await h.open();
    let snapshot: Snapshot | undefined;
    try {
      const t = await table(h, vector);
      snapshot = h.path.pin();
      const { segment, id } = memory(snapshot, t);
      if (publish) {
        await autocommit(h.path, (s) =>
          s.table(t).insert(batch(ints([3]), input("int64", 1, 0, false)), null),
        );
        await autocommit(h.path, (s) =>
          s.table(t).insert(batch(ints([4]), input("int64", 1, 0, false)), null),
        );
        await h.path.quiesce();
      }
      for (const start of [0, 2]) {
        const budget = new Budget(),
          ctx = { budget, pages: h.space, signal: new AbortController().signal };
        const hint = await hintFor(
          segment,
          { columns: [id], chunk: 0, startRow: start, maxRows: 3, targetBytes: 4096 },
          ctx,
          vector.type,
        );
        expect(budget.allocate(36)).toBe(true); // three safe int64 values and three codes
        const flat: FlatVector = {
          form: "flat",
          type: vector.type,
          length: 0,
          validity: null,
          data: allocateData(vector.type, 3),
        };
        const data = flat.data,
          r = await segment.chunk(id, 0, { flat, codes: new Int32Array(3), varlen: null }, start, {
            hint,
            context: ctx,
          });
        expect(r.reason).toBe("need");
        expect(r.rowCount).toBe(start === 0 ? 1 : 0);
        expect(r.need?.row).toBe(start === 0 ? 1 : 2);
        expect(r.need?.shape.integer).toBe(start === 0 ? "int64" : "safe");
        expect(r.need?.shape.validity).toBe(start === 2);
        expect(flat.data).toBe(data);
        expect(flat.validity).toBeNull();
        expect(budget.used).toBe(36);
        budget.free(36);
        expect(budget.used).toBe(0);
      }
    } finally {
      snapshot?.release();
      await h.close();
    }
  },
);

generated(
  "streams a multi-blob value across a mid-scan publication without touching a later value",
  async (width) => {
    reach.beginHistory();
    reach.reach("multi blob");
    const h = new Harness("strict", "memory", 4),
      vector = input("bytes", 3, 2 * MiB + width, false);
    await h.open();
    let snapshot: Snapshot | undefined;
    try {
      const t = await table(h, vector);
      snapshot = h.path.pin();
      const { segment, id } = memory(snapshot, t);
      const budget = new Budget(),
        reads: number[] = [];
      const pages: SpaceReader = {
        peek: () => null,
        read: async (refs, options) => {
          reads.push(...refs.map((r) => r.length));
          return h.space.read(refs, options);
        },
      };
      const context = { budget, pages, signal: new AbortController().signal };
      const firstHint = await hintFor(
        segment,
        { columns: [id], chunk: 0, startRow: 0, maxRows: 1, targetBytes: MiB },
        context,
        vector.type,
      );
      const firstNeed = firstHint.outputs[0];
      if (!firstNeed) throw new Error("missing first prefix output");
      const first = prepare(vector.type, firstNeed, budget);
      try {
        budget.peak = budget.used;
        const prefix = await segment.chunk(id, 0, first.out, 0, { hint: firstHint, context });
        expect(prefix.firstRow).toBe(0);
        expect(prefix.rowCount).toBe(1);
        expect(first.out.codes[0]).toBe(0);
        expect(values(prefix.vector as FlatVector)).toEqual(values(vector).slice(0, 1));
        expect(budget.peak).toBeLessThanOrEqual(
          firstHint.outputBytes +
            firstHint.residentBytes +
            firstHint.scratchBytes +
            firstHint.ioBytes,
        );
        reach.reach("consumed before publication");
      } finally {
        first.close();
      }
      expect(budget.used).toBe(0);
      expect(h.path.stats().borrowedChunkBytes).toBe(0);
      const hint = await hintFor(
        segment,
        { columns: [id], chunk: 0, startRow: 1, maxRows: 2, targetBytes: MiB },
        context,
        vector.type,
      );
      expect(hint.rowCount).toBe(1);
      expect(hint.oversized).toBe(true);
      expect(hint.outputs[0]?.varlenBytes).toBe(2 * MiB + width + 1);
      await autocommit(h.path, (s) =>
        s.table(t).insert(batch(ints([3]), input("bytes", 1, 5, false)), null),
      );
      await autocommit(h.path, (s) =>
        s.table(t).insert(batch(ints([4]), input("bytes", 1, 5, false)), null),
      );
      await h.path.quiesce();
      reads.length = 0;
      expect(budget.allocate(13)).toBe(true); // one byte, two offsets and one code
      const short: ChunkOutput = {
        flat: {
          form: "flat",
          type: vector.type,
          length: 0,
          validity: null,
          data: allocateData(vector.type, 1, 1),
        },
        codes: new Int32Array(1),
        varlen: null,
      };
      const needed = await segment.chunk(id, 0, short, 1, { hint, context });
      expect(needed.reason).toBe("need");
      expect(needed.rowCount).toBe(0);
      expect(needed.need?.varlenBytes).toBe(2 * MiB + width + 1);
      expect(reads.every((n) => n < MiB)).toBe(true); // page/root/directory only
      expect(budget.used).toBe(13);
      budget.free(13);
      reads.length = 0;
      const required = hint.outputs[0];
      if (required === undefined) throw new Error("missing output");
      const output = prepare(vector.type, required, budget);
      budget.peak = budget.used;
      const r = await segment.chunk(id, 0, output.out, 1, { hint, context });
      expect(r.firstRow).toBe(1);
      expect(r.rowCount).toBe(1);
      expect(output.out.codes[0]).toBe(0);
      expect(values(r.vector as FlatVector)).toEqual(values(vector).slice(1, 2));
      expect(reads.filter((n) => n >= MiB).length).toBe(2); // exactly two full parts, then a short part
      expect(reads.reduce((a, n) => a + n, 0)).toBeLessThan(3 * MiB); // no second logical value
      expect(budget.peak).toBeLessThanOrEqual(
        hint.outputBytes + hint.residentBytes + hint.scratchBytes + hint.ioBytes,
      );
      reach.reach("resumed after publication");
      reach.reach("multi blob resumed");
      expect(budget.used).toBe(required.bytes);
      output.close();
    } finally {
      snapshot?.release();
      await h.close();
    }
  },
);

it("rejects foreign tokens and altered requests before I/O", async () => {
  const h = new Harness();
  await h.open();
  let a: Snapshot | undefined, b: Snapshot | undefined;
  try {
    const t = await table(h, input("text", 4, 10, false));
    a = h.path.pin();
    b = h.path.pin();
    const x = memory(a, t),
      y = memory(b, t),
      budget = new Budget();
    let reads = 0;
    const pages: SpaceReader = {
      peek: () => null,
      read: () => {
        reads++;
        throw new Error("unexpected I/O");
      },
    };
    const context = { budget, pages, signal: new AbortController().signal },
      request = { columns: [x.id], chunk: 0, startRow: 0, maxRows: 2, targetBytes: 1024 };
    const hint = await hintFor(x.segment, request, context, { kind: "text" }),
      required = hint.outputs[0];
    if (!required) throw new Error("output");
    const output = prepare({ kind: "text" }, required, budget);
    expect(() => y.segment.chunk(y.id, 0, output.out, 0, { hint, context })).toThrow(/hint/);
    expect(() => x.segment.chunk(x.id, 0, output.out, 1, { hint, context })).toThrow(/hint/);
    expect(() => x.segment.readHint({ ...request, maxRows: 0 }, context)).toThrow(RangeError);
    expect(reads).toBe(0);
    output.close();
  } finally {
    a?.release();
    b?.release();
    await h.close();
  }
});

generated("keeps a statement's hint through first staging publication", async (width) => {
  reach.beginHistory();
  const h = new Harness("strict", "memory", 8);
  await h.open();
  try {
    let t = 0;
    await autocommit(h.path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", { kind: "text" })]),
          })
        ).id ?? 0;
    });
    const tx = await h.path.begin(),
      first = tx.statement({ rules });
    await first
      .table(t)
      .insert(batch(ints([0, 1, 2, 3, 4, 5, 6, 7]), input("text", 8, width, false)), null);
    await first.end();
    const statement = tx.statement({ rules }),
      { segment, id } = memory(statement.view, t);
    const budget = new Budget(),
      context = { budget, pages: h.space, signal: new AbortController().signal };
    const hint = await hintFor(
      segment,
      { columns: [id], chunk: 0, startRow: 0, maxRows: 2, targetBytes: 1024 },
      context,
      { kind: "text" },
    );
    const required = hint.outputs[0];
    if (!required) throw new Error("output");
    const expected = values({ ...input("text", 4, width, false), length: hint.rowCount });
    for (let from = 8; from < 32; from += 8)
      await statement
        .table(t)
        .insert(
          batch(ints(Array.from({ length: 8 }, (_, k) => from + k)), input("text", 8, 20, false)),
          null,
        );
    expect(h.path.maintenance.stages).toBeGreaterThan(0);
    reach.reach("staged");
    const output = prepare({ kind: "text" }, required, budget),
      r = await segment.chunk(id, 0, output.out, 0, { hint, context });
    expect(values(r.vector as FlatVector)).toEqual(expected);
    expect(r.rowCount).toBe(hint.rowCount);
    output.close();
    expect(budget.used).toBe(0);
    statement.rollback();
    tx.rollback();
  } finally {
    await h.close();
  }
});

generated("reads a frozen memtable without waiting for its publication", async (width) => {
  reach.beginHistory();
  let entered!: () => void, unblock!: () => void;
  const arrival = new Promise<void>((r) => {
      entered = r;
    }),
    gate = new Promise<void>((r) => {
      unblock = r;
    });
  const h = new Harness("strict", "memory", 8, undefined, {
    beforeFlushPublish: async () => {
      entered();
      await gate;
    },
  });
  await h.open();
  let snapshot: Snapshot | undefined;
  try {
    const t = await table(h, input("text", 4, width, false));
    snapshot = h.path.pin();
    const { segment, id } = memory(snapshot, t);
    const budget = new Budget(),
      context = { budget, pages: h.space, signal: new AbortController().signal };
    const hint = await hintFor(
      segment,
      { columns: [id], chunk: 0, startRow: 0, maxRows: 2, targetBytes: 1024 },
      context,
      { kind: "text" },
    );
    await autocommit(h.path, (s) =>
      s.table(t).insert(batch(ints([4, 5, 6, 7]), input("text", 4, 1, false)), null),
    );
    await autocommit(h.path, (s) =>
      s.table(t).insert(batch(ints([8]), input("text", 1, 1, false)), null),
    );
    await arrival;
    reach.reach("frozen");
    const required = hint.outputs[0];
    if (!required) throw new Error("output");
    const output = prepare({ kind: "text" }, required, budget);
    const realClock = taskClock.now;
    let pending;
    try {
      taskClock.now = () => 0;
      beginTask();
      pending = segment.chunk(id, 0, output.out, 0, { hint, context });
    } finally {
      taskClock.now = realClock;
      beginTask();
    }
    expect(pending).not.toBeInstanceOf(Promise);
    expect((await pending).rowCount).toBe(hint.rowCount);
    output.close();
    expect(budget.used).toBe(0);
  } finally {
    unblock();
    snapshot?.release();
    await h.close();
  }
});

for (const seed of seedsFor("next-bounded-memory-cancel", [0xca11]))
  it(`drains late reads before returning private I/O on cancellation (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          phase: fc.constantFrom("chunk", "sizing"),
          cancel: fc.constantFrom("cancel", "release"),
        }),
        async ({ phase, cancel }) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", 4);
          await h.open();
          let snapshot: Snapshot | undefined;
          let unblock: () => void = () => undefined;
          try {
            const t = await table(h, input("text", 3, 30, false));
            snapshot = h.path.pin();
            const { segment, id } = memory(snapshot, t);
            const budget = new Budget(),
              stop = new AbortController(),
              base = { budget, pages: h.space, signal: stop.signal };
            const request = { columns: [id], chunk: 0, startRow: 0, maxRows: 2, targetBytes: 1024 };
            const hint = await hintFor(segment, request, base, { kind: "text" });
            await autocommit(h.path, (s) =>
              s.table(t).insert(batch(ints([3, 4]), input("text", 2, 1, false)), null),
            );
            await h.path.quiesce();
            let entered!: () => void;
            const arrived = new Promise<void>((r) => {
                entered = r;
              }),
              gate = new Promise<void>((r) => {
                unblock = r;
              });
            const pages: SpaceReader = {
              peek: () => null,
              read: async (refs, options) => {
                const lease = await h.space.read(refs, options);
                entered();
                await gate;
                return lease;
              },
            };
            const required = hint.outputs[0];
            if (!required) throw new Error("output");
            const output =
              phase === "chunk" ? prepare({ kind: "text" }, required, budget) : undefined;
            const context = { budget, pages, signal: stop.signal };
            const pending = Promise.resolve(
              output === undefined
                ? segment.readHint(request, context)
                : segment.chunk(id, 0, output.out, 0, { hint, context }),
            );
            let settled = false;
            const checked = pending.then(
              () => {
                settled = true;
                return undefined;
              },
              (error: unknown) => {
                settled = true;
                return error;
              },
            );
            await arrived;
            const cancellation = new Error("test cancellation");
            const pins = h.space.stats.pinCount;
            if (cancel === "cancel") stop.abort(cancellation);
            else {
              snapshot.release();
              expect(h.space.stats.pinCount).toBe(pins);
              reach.reach("retain pin during read");
            }
            await Promise.resolve();
            expect(settled).toBe(false);
            expect(budget.used).toBeGreaterThan(output === undefined ? 0 : required.bytes);
            unblock();
            const error = await checked;
            expect(error).toBeInstanceOf(Error);
            if (cancel === "cancel") expect(error).toBe(cancellation);
            else {
              expect((error as Error).name).toBe("SnapshotReleasedError");
              expect(h.space.stats.pinCount).toBeLessThan(pins);
              reach.reach("release pin after read");
            }
            expect(budget.used).toBe(output === undefined ? 0 : required.bytes);
            output?.close();
            expect(budget.used).toBe(0);
            reach.reach(`${cancel} ${phase}`);
          } finally {
            unblock();
            snapshot?.release();
            await h.close();
          }
        },
      ),
      { seed, numRuns: 20 },
    );
  }, 60000);

generated("checks the shared task clock during resident row copies", async (width) => {
  reach.beginHistory();
  const h = new Harness("strict", "memory", 128);
  await h.open();
  let snapshot: Snapshot | undefined;
  const previous = taskClock.now;
  let settled = false,
    turns = 0;
  try {
    const t = await table(h, input("text", 64, width, false));
    snapshot = h.path.pin();
    const { segment, id } = memory(snapshot, t);
    const budget = new Budget(),
      context = { budget, pages: h.space, signal: new AbortController().signal };
    const hint = await hintFor(
      segment,
      { columns: [id], chunk: 0, startRow: 0, maxRows: 64, targetBytes: MiB },
      context,
      { kind: "text" },
    );
    const required = hint.outputs[0];
    if (!required) throw new Error("output");
    const output = prepare({ kind: "text" }, required, budget);
    let now = 0;
    taskClock.now = () => (now += 3);
    beginTask();
    const tick = () => {
      if (!settled) {
        turns++;
        setImmediate(tick);
      }
    };
    setImmediate(tick);
    const r = await segment.chunk(id, 0, output.out, 0, { hint, context });
    settled = true;
    expect(r.rowCount).toBe(64);
    expect(turns).toBeGreaterThan(0);
    reach.reach("task clock");
    output.close();
  } finally {
    settled = true;
    taskClock.now = previous;
    beginTask();
    snapshot?.release();
    await h.close();
  }
});

generated("reads recovered staged groups through the caller's context", async (width) => {
  reach.beginHistory();
  const h = new Harness("strict", "sim", 8);
  await h.open();
  let snapshot: Snapshot | undefined;
  try {
    let t = 0;
    await autocommit(h.path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", { kind: "bytes" })]),
          })
        ).id ?? 0;
    });
    const tx = await h.path.begin();
    for (let from = 0; from < 32; from += 8) {
      const s = tx.statement({ rules });
      await s
        .table(t)
        .insert(
          batch(
            ints(Array.from({ length: 8 }, (_, k) => from + k)),
            input("bytes", 8, width, false, from),
          ),
          null,
        );
      await s.end();
    }
    expect(h.path.maintenance.stages).toBeGreaterThan(0);
    await (
      await tx.commit(null)
    ).durable;
    const recovered = await h.crash();
    snapshot = recovered.pin();
    const { segment, id } = memory(snapshot, t),
      budget = new Budget();
    const memtable = (segment as unknown as { memtable: Memtable }).memtable;
    expect(memtable.published?.groupRoot).toBeInstanceOf(LazyGroupRoot);
    reach.reach("recovered");
    let reads = 0;
    const context = {
      budget,
      signal: new AbortController().signal,
      pages: {
        peek: () => null,
        read: (refs, options) => {
          reads++;
          return h.space.read(refs, options);
        },
      } satisfies SpaceReader,
    };
    const hint = await segment.readHint(
      { columns: [id], chunk: 0, startRow: 1, maxRows: 3, targetBytes: 1024 },
      context,
    );
    const need = hint.outputs[0];
    if (!need) throw new Error("output");
    const output = prepare({ kind: "bytes" }, need, budget),
      r = await segment.chunk(id, 0, output.out, 1, { hint, context });
    expect(values(r.vector as FlatVector)).toEqual(
      values(input("bytes", 8, width, false)).slice(1, 1 + hint.rowCount),
    );
    expect(reads).toBeGreaterThan(0);
    output.close();
    expect(budget.used).toBe(0);
  } finally {
    snapshot?.release();
    await h.close();
  }
});

it.each([false, true])(
  "requires int128 before writing a decimal into an int64 output (published=%s)",
  async (publish) => {
    const h = new Harness("strict", "memory", 4);
    await h.open();
    let snapshot: Snapshot | undefined;
    try {
      const vector = input("decimal", 3, 0, false),
        t = await table(h, vector);
      snapshot = h.path.pin();
      const { segment, id } = memory(snapshot, t);
      const budget = new Budget(),
        context = { budget, pages: h.space, signal: new AbortController().signal };
      const hint = await hintFor(
        segment,
        { columns: [id], chunk: 0, startRow: 0, maxRows: 2, targetBytes: 1024 },
        context,
        vector.type,
      );
      if (publish) {
        await autocommit(h.path, (s) =>
          s.table(t).insert(batch(ints([3, 4]), input("decimal", 2, 0, false)), null),
        );
        await h.path.quiesce();
      }
      expect(budget.allocate(30)).toBe(true); // int64 coefficients, scales, specials and codes
      const out: ChunkOutput = {
        flat: {
          form: "flat",
          type: vector.type,
          length: 0,
          validity: null,
          data: {
            layout: "decimal",
            scale: null,
            scales: new Uint16Array(2),
            specials: new Uint8Array(2),
            coefficients: { repr: "int64", values: new BigInt64Array(2).fill(77n) },
          },
        },
        codes: new Int32Array(2),
        varlen: null,
      };
      const r = await segment.chunk(id, 0, out, 0, { hint, context });
      expect(r.reason).toBe("need");
      expect(r.rowCount).toBe(0);
      expect(r.need?.shape.integer).toBe("int128");
      expect(
        out.flat.data.layout === "decimal" &&
          out.flat.data.coefficients.repr === "int64" &&
          out.flat.data.coefficients.values[0],
      ).toBe(77n);
      expect(budget.used).toBe(30);
      budget.free(30);
      expect(budget.used).toBe(0);
    } finally {
      snapshot?.release();
      await h.close();
    }
  },
);

generated(
  "precharges a decoder's native alignment copy independently of reported peak",
  async (width) => {
    reach.beginHistory();
    const h = new Harness("strict", "memory", 64);
    await h.open();
    let snapshot: Snapshot | undefined;
    try {
      const vector = input("bytes", 64, width, false),
        t = await table(h, vector);
      snapshot = h.path.pin();
      const { segment, id } = memory(snapshot, t);
      const budget = new Budget(),
        base = { budget, pages: h.space, signal: new AbortController().signal };
      const hint = await segment.readHint(
        { columns: [id], chunk: 0, startRow: 0, maxRows: 1, targetBytes: MiB },
        base,
      );
      await autocommit(h.path, (s) =>
        s.table(t).insert(batch(ints([64]), input("bytes", 1, 1, false)), null),
      );
      await h.path.quiesce();
      const m = (segment as unknown as { memtable: Memtable }).memtable,
        root = m.published?.groupRoot;
      if (root === undefined || root instanceof LazyGroupRoot)
        throw new Error("expected decoded publication");
      const stored = root.columns.find((c) => c.columnId === id);
      if (!stored) throw new Error("missing column");
      const reader = await ChunkReader.open(h.space, stored, {
        budget: { allocate: () => true, free: () => undefined },
      });
      let ref;
      try {
        await reader.load([0]);
        const page = reader.directory.pages[0];
        if (!page) throw new Error("missing page");
        ref = {
          extent: stored.data.extent,
          offset: stored.data.offset + page.offset,
          length: page.length,
          checksum: page.checksum,
        };
      } finally {
        reader.close();
      }
      const source = h.space.peek(ref);
      if (!source) throw new Error("warm page missing");
      const misaligned = new Uint8Array(source.length + 1).subarray(1);
      misaligned.set(source);
      const pages: SpaceReader = {
        peek: (r, options) =>
          r.extent.offset === ref.extent.offset && r.offset === ref.offset
            ? misaligned
            : h.space.peek(r, options),
        read: (refs, options) => h.space.read(refs, options),
      };
      const need = hint.outputs[0];
      if (!need) throw new Error("output");
      const output = prepare(vector.type, need, budget);
      // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its observed source via .call below.
      const original = Uint8Array.prototype.slice;
      let nativeCopy = 0;
      const spy = vi.spyOn(Uint8Array.prototype, "slice").mockImplementation(function (
        this: Uint8Array,
        from,
        to,
      ) {
        if (this === misaligned) {
          nativeCopy += (to ?? this.length) - (from ?? 0);
          // Exclude the caller's output and the encoded input. Metadata alone cannot fund a
          // second full page: this assertion observes actual native demand BEFORE allocation.
          expect(budget.used - need.bytes - misaligned.byteLength).toBeGreaterThanOrEqual(
            nativeCopy,
          );
        }
        return original.call(this, from, to);
      });
      try {
        const r = await segment.chunk(id, 0, output.out, 0, { hint, context: { ...base, pages } });
        expect(r.rowCount).toBe(1);
        expect(nativeCopy).toBe(source.length);
        reach.reach("precharged native copy");
        expect(budget.peak).toBeLessThanOrEqual(
          need.bytes + hint.residentBytes + hint.scratchBytes + hint.ioBytes,
        );
      } finally {
        spy.mockRestore();
        output.close();
      }
      expect(budget.used).toBe(0);
    } finally {
      snapshot?.release();
      await h.close();
    }
  },
);

/** Stronger caller destinations own both their pointer array and each coefficient's capacity. */
function prepareDecimalDestination(
  type: PhysicalType,
  need: MemoryOutputNeed,
  budget: Budget,
  wide: boolean,
  coefficientRows = need.rows,
): { out: ChunkOutput; charge: number; close(): void } {
  if (!wide) return { ...prepare(type, need, budget), charge: need.bytes };
  // Native int128 uses 16B/slot. The caller instead reserves a pointer plus max 128-bit
  // BigInt (32B/slot), and the array header, before creating any destination storage.
  const charge = need.bytes - need.rows * 16 + coefficientRows * (8 + 32) + 64;
  expect(budget.allocate(charge)).toBe(true);
  const out: ChunkOutput = {
    flat: {
      form: "flat",
      type,
      length: 0,
      validity: new Uint32Array(Math.ceil(need.rows / 32)),
      data: {
        layout: "decimal",
        scale: null,
        scales: new Uint16Array(need.rows),
        specials: new Uint8Array(need.rows),
        coefficients: { repr: "wide", values: new Array<bigint>(coefficientRows).fill(0n) },
      },
    },
    codes: new Int32Array(need.rows),
    varlen: null,
  };
  return { out, charge, close: () => budget.free(charge) };
}
for (const seed of seedsFor("next-bounded-memory-destination", [0xb04e]))
  it(`fills stronger decimal destinations and resumes at capacity (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          n: fc.integer({ min: 4, max: 12 }),
          capacity: fc.integer({ min: 1, max: 3 }),
          publish: fc.boolean(),
          nullable: fc.boolean(),
          negative: fc.boolean(),
        }),
        async (s) => {
          const h = new Harness("strict", "memory", s.n);
          await h.open();
          let snapshot: Snapshot | undefined;
          const budget = new Budget();
          try {
            const vector = input("decimal", s.n, 0, s.nullable);
            if (vector.data.layout !== "decimal" || vector.data.coefficients.repr !== "int128")
              throw new Error("decimal source");
            for (let k = 0; k < s.n; k++)
              setInt128(
                vector.data.coefficients.limbs,
                k,
                (s.negative ? -1n : 1n) * (10n ** 30n + BigInt(k)),
              );
            const expected = values(vector),
              t = await table(h, vector);
            snapshot = h.path.pin();
            const { segment, id } = memory(snapshot, t),
              context = { budget, pages: h.space, signal: new AbortController().signal },
              request = { columns: [id], chunk: 0, startRow: 0, maxRows: s.n, targetBytes: 65536 },
              intervals: Array<{ maxRows: number; hint: MemoryReadHint }> = [];
            for (const maxRows of [1, s.n])
              intervals.push({
                maxRows,
                hint: await segment.readHint({ ...request, maxRows }, context),
              });
            if (s.publish) {
              await autocommit(h.path, (st) =>
                st
                  .table(t)
                  .insert(batch(ints([s.n, s.n + 1]), input("decimal", 2, 0, false)), null),
              );
              await h.path.quiesce();
            }
            // Size both intervals before publication; reuse the same hints after re-pointing.
            for (const { maxRows, hint: original } of intervals) {
              for (const wide of [false, true]) {
                reach.beginHistory();
                reach.reach(wide ? "wide decimal destination" : "native decimal destination");
                reach.reach(
                  s.publish ? "decimal destination repointed" : "decimal destination resident",
                );
                if (s.negative) reach.reach("negative destination coefficient");
                if (s.nullable) reach.reach("nullable decimal destination");
                const actual: unknown[] = [];
                for (let start = 0; start < s.n;) {
                  const hint =
                    start === 0
                      ? original
                      : await segment.readHint(
                          { ...request, startRow: start, maxRows: Math.min(maxRows, s.n - start) },
                          context,
                        );
                  const capacity = Math.min(hint.rowCount, s.capacity),
                    need = memoryNeed(
                      id,
                      start,
                      wide ? hint.rowCount : capacity,
                      memoryOutputLayout(vector.type),
                      0,
                    ),
                    prepared = prepareDecimalDestination(vector.type, need, budget, wide, capacity),
                    out = prepared.out,
                    data = out.flat.data,
                    native = [...buffersOf(out)];
                  try {
                    const result = await segment.chunk(id, 0, out, start, { hint, context });
                    expect(result.rowCount).toBe(capacity);
                    expect(result.firstRow).toBe(start);
                    expect(result.reason).toBe(
                      start + capacity === s.n ? null : capacity < hint.rowCount ? "need" : "full",
                    );
                    if (wide && maxRows === 1 && start === 0) {
                      // Review P2: an int128 coefficient fits a prepared one-slot wide output.
                      expect(result.reason).toBe("full");
                      expect(values(out.flat)).toEqual([expected[0]]);
                      reach.reach(
                        s.publish ? "single-slot wide repointed" : "single-slot wide resident",
                      );
                      if (!s.negative) {
                        expect(values(out.flat)[0]).toEqual([10n ** 30n, 0, 0]);
                        reach.reach("exact int128 to wide repro");
                      }
                    }
                    if (result.reason === "need") {
                      expect(result.need?.row).toBe(start + capacity);
                      reach.reach("decimal capacity continuation");
                    }
                    expect(out.flat.data).toBe(data);
                    expect([...buffersOf(out)]).toEqual(native);
                    expect(budget.used).toBe(prepared.charge);
                    if (data.layout === "decimal" && data.coefficients.repr === "wide") {
                      expect(data.coefficients.values.length).toBe(capacity);
                      const live = data.coefficients.values.reduce(
                          (sum, v) => sum + bigintBytes(v),
                          0,
                        ),
                        nativeBytes = native.reduce((sum, b) => sum + b.byteLength, 0);
                      expect(nativeBytes + capacity * 8 + live + 64).toBeLessThanOrEqual(
                        prepared.charge,
                      );
                      reach.reach("wide destination independently charged");
                    }
                    actual.push(...values(out.flat));
                    start += result.rowCount;
                  } finally {
                    prepared.close();
                  }
                }
                expect(actual).toEqual(expected);
                expect(budget.used).toBe(0);
              }
            }
          } finally {
            snapshot?.release();
            await h.close();
          }
          expect(budget.used).toBe(0);
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 20 },
    );
  }, 120000);

it("reaches all bounded provider dimensions", () => reach.assertReached());
