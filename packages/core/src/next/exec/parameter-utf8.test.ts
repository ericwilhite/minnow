/** WHATWG TextEncoder/USVString law: exact UTF8, never a truncated slot or a split pair.
 * Sizing and conversion may yield independently; reservation scratch is not payload. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PARAMETER_UTF8_REACH } from "../../testing/reach.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { Governor } from "../memory/governor.js";
import { prepareParameters } from "./parameters.js";
import type { ParameterValue } from "./executor.js";
import { Slice } from "./slices.js";

const labels = ["ok", "é", "中", "😀"];
const spec = fc.record({
  kind: fc.constantFrom("int", "bool", "text", "bytes", "enum"),
  nullable: fc.boolean(),
});
const history = fc.record({
  atom: fc.constantFrom("a", "Ж", "中", "😀"),
  boundary: fc.constantFrom(8190, 8191, 8192, 16383, 16384),
  marker: fc.constantFrom("pair", "high", "low", "mixed"),
  suffix: fc.integer({ min: 2, max: 257 }),
  slots: fc.array(spec, { minLength: 1, maxLength: 8 }),
  position: fc.nat({ max: 16 }),
  label: fc.integer({ min: 0, max: 3 }),
  yielding: fc.boolean(),
  partial: fc.boolean(),
});
const allSlots = [
  { kind: "int", nullable: false },
  { kind: "bool", nullable: false },
  { kind: "bytes", nullable: false },
  { kind: "enum", nullable: false },
  { kind: "text", nullable: false },
  { kind: "text", nullable: true },
];
const example = (
  atom: string,
  boundary: number,
  marker: string,
  position: number,
  partial: boolean,
  yielding: boolean,
) => ({
  atom,
  boundary,
  marker,
  suffix: 17,
  slots: allSlots,
  position,
  label: 3,
  partial,
  yielding,
});

for (const seed of seedsFor("next-parameter-utf8", [49511, 49512]))
  it(`binds long mixed UTF8 slots across yields and short writes (${String(seed)})`, async () => {
    const reach = createReach("parameter UTF8", PARAMETER_UTF8_REACH);
    const native = new TextEncoder();
    const nativeEncode = native.encodeInto.bind(native);
    await fc.assert(
      fc.asyncProperty(history, async (data) => {
        reach.beginHistory();
        const prefix =
          data.atom.repeat(Math.floor(data.boundary / data.atom.length)) +
          "a".repeat(data.boundary % data.atom.length);
        const marker =
          data.marker === "pair"
            ? "😀"
            : data.marker === "high"
              ? "\ud800Z"
              : data.marker === "low"
                ? "\udc00Z"
                : "Ж中😀\ud800Z\udc00";
        const long = prefix + marker + data.atom.repeat(data.suffix);
        expect(long.length).toBeGreaterThan(8192);
        const types: PhysicalType[] = [],
          values: ParameterValue[] = [];
        for (const [index, slot] of data.slots.entries()) {
          const type: PhysicalType =
            slot.kind === "int"
              ? { kind: "int32", bits: 32 }
              : slot.kind === "bool"
                ? { kind: "bool" }
                : slot.kind === "bytes"
                  ? { kind: "bytes" }
                  : slot.kind === "enum"
                    ? { kind: "enum", name: "mood", labels }
                    : { kind: "text" };
          types.push(type);
          values.push(
            slot.nullable
              ? null
              : slot.kind === "int"
                ? index + 17
                : slot.kind === "bool"
                  ? true
                  : slot.kind === "bytes"
                    ? Uint8Array.of(index, 17, 255)
                    : slot.kind === "enum"
                      ? (labels[data.label] ?? "ok")
                      : "é中😀",
          );
          if (slot.nullable) reach.reach("null slot");
          if (slot.kind === "enum") reach.reach("enum slot");
        }
        const position = data.position % (types.length + 1);
        types.splice(position, 0, { kind: "text" });
        values.splice(position, 0, long);
        reach.reach(
          position === 0
            ? "text first"
            : position === types.length - 1
              ? "text last"
              : "text middle",
        );
        const plan: PhysicalPlan = {
          root: { id: 1, op: "values", output: [], rows: [] },
          columns: [],
          parameters: types,
          placeholderCount: types.length,
          clockSlot: null,
          initPlans: [],
          catalogVersion: 0,
          volatile: false,
        };
        const governor = new Governor(8 << 20);
        let phase = "size",
          sizeYields = 0,
          bindYields = 0,
          shortWrites = 0;
        const due = vi.spyOn(Slice.prototype, "due").mockImplementation(() => {
          if (data.yielding) {
            if (phase === "size") sizeYields++;
            else bindYields++;
          }
          return data.yielding;
        });
        const encoder = vi
          .spyOn(TextEncoder.prototype, "encodeInto")
          .mockImplementation((source, target) => {
            // A deterministic valid prefix simulates Node26's short-write behavior on every runtime.
            const into = data.partial
              ? target.subarray(0, Math.max(4, Math.floor(target.length / 2)))
              : target;
            const result = nativeEncode(source, into);
            if (result.read < source.length && result.read > 0) shortWrites++;
            return result;
          });
        let grant: Awaited<ReturnType<Governor["admit"]>> | undefined;
        try {
          const binding = await prepareParameters(plan, values);
          phase = "bind";
          grant = await governor.admit({
            label: "UTF8 parameters",
            minimumBytes: binding.bytes,
            spillable: false,
          });
          const bound = await binding.bind(grant);
          expect(bound.slots).toHaveLength(types.length);
          expect(binding.slotBytes).toHaveLength(types.length);
          if (binding.logicalBytes !== undefined)
            expect(binding.logicalBytes).toBe(binding.slotBytes?.reduce((n, size) => n + size, 0));
          expect(binding.bytes).toBeGreaterThan(binding.logicalBytes ?? 0);
          for (let slot = 0; slot < types.length; slot++) {
            const vector = bound.slots[slot],
              value = values[slot],
              type = types[slot];
            if (!vector || !type) throw new Error("missing bound slot");
            if (value === null) {
              expect((vector.validity?.[0] ?? 1) & 1).toBe(0);
              continue;
            }
            expect(vector.validity).toBeNull();
            if (typeof value === "string") {
              expect(vector.data.layout).toBe("varlen");
              if (vector.data.layout !== "varlen") throw new Error("expected encoded slot");
              const expected = new TextEncoder().encode(value);
              expect(vector.data.offsets[1]).toBe(expected.length);
              expect(vector.data.bytes.length).toBe(expected.length);
              expect(vector.data.bytes.every((byte, index) => byte === expected[index])).toBe(true);
            } else if (value instanceof Uint8Array) {
              if (vector.data.layout !== "varlen") throw new Error("expected byte slot");
              expect(vector.data.bytes.length).toBe(value.length);
              expect(vector.data.bytes.every((byte, index) => byte === value[index])).toBe(true);
            } else if (vector.data.layout === "i32") expect(vector.data.values[0]).toBe(value);
            else if (vector.data.layout === "bool")
              expect(vector.data.values[0]).toBe(Number(value));
            else throw new Error("wrong mixed-slot type");
          }
          reach.reach(
            data.atom === "a"
              ? "one byte"
              : data.atom === "Ж"
                ? "two bytes"
                : data.atom === "中"
                  ? "three bytes"
                  : "four bytes",
          );
          if (data.marker === "high" || data.marker === "mixed") reach.reach("lone high");
          if (data.marker === "low" || data.marker === "mixed") reach.reach("lone low");
          if (data.marker === "pair")
            reach.reach(
              data.boundary === 8191
                ? "pair crosses 8192"
                : data.boundary === 8190
                  ? "pair before 8192"
                  : "pair after 8192",
            );
          if (data.yielding) {
            expect(sizeYields).toBeGreaterThan(0);
            expect(bindYields).toBeGreaterThan(0);
            reach.reach("sizing and binding yield");
          }
          if (data.partial) {
            expect(shortWrites).toBeGreaterThan(0);
            reach.reach("short write");
          }
          reach.reach("mixed slots");
        } finally {
          encoder.mockRestore();
          due.mockRestore();
          grant?.close();
          expect(governor.usedBytes).toBe(0);
        }
        reach.reach("zero retained");
      }),
      {
        seed,
        numRuns: 64,
        endOnFailure: true,
        examples: [
          [example("中", 16384, "pair", 0, false, true)],
          [example("a", 8192, "high", 6, true, false)],
          [example("😀", 8190, "low", 3, true, true)],
          [example("Ж", 16383, "pair", 3, false, true)],
          [example("Ж", 8191, "pair", 6, true, true)],
          [example("中", 8192, "pair", 0, true, true)],
        ],
      },
    );
    reach.assertReached();
  });

it("reserves one encoding slice while keeping per-slot UTF8 extents independent", async () => {
  const parameters: PhysicalType[] = [{ kind: "text" }, { kind: "text" }, { kind: "text" }];
  const plan: PhysicalPlan = {
    root: { id: 1, op: "values", output: [], rows: [] },
    columns: [],
    parameters,
    placeholderCount: 3,
    clockSlot: null,
    initPlans: [],
    catalogVersion: 0,
    volatile: false,
  };
  const binding = await prepareParameters(plan, ["中".repeat(9000), "Ж".repeat(10000), ""]);
  // The type contract's fixed512bytes covers each vector/metadata wrapper. UTF16
  // slice storage is transient and shared; it must not become part of any slot.
  expect(binding.slotBytes).toEqual([512 + 27000, 512 + 20000, 512]);
  expect(binding.logicalBytes).toBe(3 * 512 + 47000);
  expect(binding.bytes).toBe(3 * 512 + 47000 + 2 * 8192 + 24);
  const g = new Governor(8 << 20),
    grant = await g.admit({ label: "UTF8 scratch", minimumBytes: binding.bytes, spillable: false });
  try {
    const result = await binding.bind(grant);
    const empty = result.slots[2];
    expect(empty?.validity).toBeNull();
    expect(empty?.data).toMatchObject({
      layout: "varlen",
      bytes: new Uint8Array(0),
      offsets: Int32Array.of(0, 0),
    });
  } finally {
    grant.close();
    expect(g.usedBytes).toBe(0);
  }
});
