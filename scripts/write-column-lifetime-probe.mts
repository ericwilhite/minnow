/** Isolated --expose-gc witness: live chunks must not root borrowed source buffers. */
import { strict as assert } from "node:assert";
import { setImmediate as nextTask } from "node:timers/promises";
import {
  ColumnChunk,
  encodeColumn,
  MEMORY_CHUNK_ROWS,
} from "../packages/core/dist/next/write/columns.js";
import type { FlatVector, Vector, VectorData } from "../packages/core/dist/next/vector/vector.js";
import type { PhysicalType } from "../packages/core/dist/next/types.js";

export const FAMILIES = [
  "bool",
  "i32",
  "f32",
  "f64",
  "int64",
  "decimal",
  "varlen",
  "fixed16",
  "interval",
] as const;
export const FORMS = ["flat", "constant", "dictionary"] as const;
export const OUTCOMES = ["copied", "full", "denied"] as const;
export interface LifetimeCase {
  family: (typeof FAMILIES)[number];
  form: (typeof FORMS)[number];
  outcome: (typeof OUTCOMES)[number];
  rows: number;
  selected: boolean;
  nullable: boolean;
  offset: number;
}
function flat(s: LifetimeCase): FlatVector {
  const n = Math.max(s.rows, 8192),
    value = (k: number) => s.offset + k + 1;
  let type: PhysicalType, data: VectorData;
  switch (s.family) {
    case "bool":
      type = { kind: "bool" };
      data = { layout: "bool", values: Uint8Array.from({ length: n }, (_, k) => value(k) & 1) };
      break;
    case "i32":
      type = { kind: "int32", bits: 32 };
      data = { layout: "i32", values: Int32Array.from({ length: n }, (_, k) => value(k)) };
      break;
    case "f32":
      type = { kind: "float32" };
      data = { layout: "f32", values: Float32Array.from({ length: n }, (_, k) => value(k) / 4) };
      break;
    case "f64":
      type = { kind: "float64" };
      data = { layout: "f64", values: Float64Array.from({ length: n }, (_, k) => value(k) / 8) };
      break;
    case "int64":
      type = { kind: "int64" };
      data = {
        layout: "int64",
        values: {
          repr: "int64",
          values: BigInt64Array.from({ length: n }, (_, k) => (1n << 55n) + BigInt(value(k))),
        },
      };
      break;
    case "decimal":
      type = { kind: "decimal", precision: null, scale: 2 };
      data = {
        layout: "decimal",
        scale: 2,
        scales: null,
        specials: null,
        coefficients: {
          repr: "int64",
          values: BigInt64Array.from({ length: n }, (_, k) => (1n << 55n) + BigInt(value(k))),
        },
      };
      break;
    case "varlen":
      type = { kind: "bytes" };
      data = {
        layout: "varlen",
        bytes: Uint8Array.from({ length: n * 16 }, (_, k) => value(k) & 255),
        offsets: Int32Array.from({ length: n + 1 }, (_, k) => k * 16),
      };
      break;
    case "fixed16":
      type = { kind: "uuid" };
      data = {
        layout: "fixed16",
        bytes: Uint8Array.from({ length: n * 16 }, (_, k) => value(k) & 255),
      };
      break;
    case "interval":
      type = { kind: "interval" };
      data = {
        layout: "interval",
        months: Int32Array.from({ length: n }, (_, k) => value(k)),
        days: Int32Array.from({ length: n }, (_, k) => -value(k)),
        micros: {
          repr: "int64",
          values: BigInt64Array.from({ length: n }, (_, k) => (1n << 55n) + BigInt(value(k))),
        },
      };
      break;
  }
  const validity = s.nullable ? new Uint32Array(Math.ceil(s.rows / 32)).fill(0x55555555) : null;
  return { form: "flat", type, data, length: s.rows, validity };
}
function buffersOf(value: unknown, result = new Set<ArrayBufferLike>()): Set<ArrayBufferLike> {
  if (ArrayBuffer.isView(value)) result.add(value.buffer);
  else if (value !== null && typeof value === "object")
    for (const child of Object.values(value)) buffersOf(child, result);
  return result;
}
function makeBudget(state: { allowed: boolean; paid: number }) {
  return {
    allocate(bytes: number) {
      if (!state.allowed) return false;
      state.paid += bytes;
      return true;
    },
    free(bytes: number) {
      state.paid -= bytes;
      assert(state.paid >= 0);
    },
  };
}
function setup(s: LifetimeCase) {
  const state = { allowed: true, paid: 0 };
  const budget = makeBudget(state);
  const input = flat(s),
    chunk = new ColumnChunk(input.type, budget);
  // Fill a real chunk before the deliberate overfull call; this source is independently witnessed too.
  if (s.outcome === "full")
    chunk.append(
      {
        form: "constant",
        type: input.type,
        length: MEMORY_CHUNK_ROWS,
        value: { ...input, validity: null, length: 1 },
      },
      null,
      0,
      MEMORY_CHUNK_ROWS,
    );
  let vector: Vector =
    s.form === "flat"
      ? input
      : s.form === "constant"
        ? { form: "constant", type: input.type, length: s.rows, value: { ...input, length: 1 } }
        : {
            form: "dictionary",
            type: input.type,
            length: s.rows,
            dictionaryId: 1,
            dictionary: { ...input, validity: null },
            codes: Int32Array.from({ length: s.rows }, (_, k) => s.rows - 1 - k),
            validity: input.validity,
          };
  if (s.outcome === "denied") {
    // Widen int64-family storage while funded so nonconstant cases can copy a prefix before
    // the first null's denied bitmap allocation. The warmup has no remaining live rows.
    chunk.append(
      {
        form: "constant",
        type: input.type,
        length: 1,
        value: { ...input, validity: null, length: 1 },
      },
      null,
      0,
      1,
    );
    chunk.truncate(0);
    // A null forces the first validity allocation, after sourceSlot has borrowed the input.
    const validity = new Uint32Array(Math.ceil(s.rows / 32)).fill(0xffffffff);
    const bad = Math.min(s.rows - 1, s.selected ? 2 : 1);
    validity[bad >>> 5] = (validity[bad >>> 5] ?? 0) & ~(1 << (bad & 31));
    if (vector.form === "flat") vector = { ...vector, validity };
    else if (vector.form === "constant")
      vector = { ...vector, value: { ...vector.value, validity: new Uint32Array(1) } };
    else vector = { ...vector, validity };
    state.allowed = false;
  }
  const rows = s.selected
    ? Uint16Array.from({ length: Math.ceil(s.rows / 2) }, (_, k) => k * 2)
    : null;
  const count = rows?.length ?? s.rows;
  const sources = buffersOf(vector);
  for (const buffer of buffersOf(input)) sources.add(buffer);
  const witnesses = [...sources].map((buffer) => ({
    ref: new WeakRef(buffer),
    bytes: buffer.byteLength,
  }));
  if (s.outcome === "copied") chunk.append(vector, rows, 0, count);
  else
    assert.throws(
      () => chunk.append(vector, rows, 0, count),
      s.outcome === "full" ? /chunk is full/ : /memtable/i,
    );
  if (s.outcome === "denied")
    assert.equal(
      chunk.length,
      s.form !== "constant" && s.rows > 1 ? 1 : 0,
      "denied append copied the generated prefix",
    );
  state.allowed = true;
  const view = chunk.view(),
    expected = encodeColumn(view, 0, view.length);
  // The borrow ended: producers may recycle their source buffers immediately.
  for (const buffer of sources) new Uint8Array(buffer).fill(0);
  assert.deepEqual(
    encodeColumn(view, 0, view.length),
    expected,
    "chunk must own its copied payload",
  );
  assert.equal(chunk.chargedBytes, state.paid);
  return { chunk, view, expected, witnesses, state, s };
}

export async function probe(cases: LifetimeCase[]) {
  if (globalThis.gc === undefined) throw new Error("Run with --expose-gc");
  const live = cases.map(setup);
  // WeakRef construction keeps targets alive until this job ends. Do not deref between GCs.
  await nextTask();
  for (let k = 0; k < 3; k++) {
    globalThis.gc();
    await nextTask();
  }
  const retained = live.flatMap((item) =>
    item.witnesses
      .filter((w) => w.ref.deref() !== undefined)
      .map((w) => ({
        bytes: w.bytes,
        family: item.s.family,
        form: item.s.form,
        outcome: item.s.outcome,
      })),
  );
  const byExit = Object.fromEntries(
    OUTCOMES.map((outcome) => [
      outcome,
      retained
        .filter((item) => item.outcome === outcome)
        .reduce((sum, item) => sum + item.bytes, 0),
    ]),
  );
  assert.equal(
    retained.length,
    0,
    `copied input retained by live chunks: ${JSON.stringify(byExit)}`,
  );
  for (const item of live) {
    assert.deepEqual(
      encodeColumn(item.view, 0, item.view.length),
      item.expected,
      "retained owned views survive source collection",
    );
    item.chunk.truncate(0);
    item.chunk.releaseCharge();
    assert.equal(item.state.paid, 0);
  }
  return { cases: cases.length, retainedInputBytes: 0, retainedInputBuffers: 0 };
}
// Tests import only the case types; execution is always an isolated Node process.
if (process.argv[2] !== undefined)
  console.log(JSON.stringify(await probe(JSON.parse(process.argv[2]) as LifetimeCase[])));
