/** Independent scalar lane oracle across native write/read and portable export. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_PHYSICAL_REACH } from "../../testing/reach.js";
import { SnapshotKind, SnapshotStreamReader } from "../format/snapshot.js";
import { vectorOf, valueAt } from "../page/test-support.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { exportPinnedSnapshot } from "./snapshot-export.js";
import { decodeSnapshotColumnHeader } from "./snapshot-column.js";
import { Harness, autocommit, batch, column, INT, ints, tableInput } from "./test-support.js";

const budget = () => ({
  used: 0,
  allocate(n: number) {
    this.used += n;
    return true;
  },
  free(n: number) {
    this.used -= n;
    if (this.used < 0) throw new Error("double free");
  },
});
async function* catalog(): AsyncGenerator<Uint8Array> {
  yield new Uint8Array([1]);
}
async function* input(bytes: readonly Uint8Array[]): AsyncGenerator<Uint8Array> {
  yield* bytes;
}
function oracle(bytes: Uint8Array, vector: FlatVector): void {
  const count = decodeSnapshotColumnHeader(vector.type, bytes.subarray(0, 16)),
    v = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let at = 16;
  for (let row = 0; row < count; row++) {
    const expected = valueAt(vector, row);
    expect(bytes[at++]).toBe(expected === null ? 0 : 1);
    if (expected === null) continue;
    const d = vector.data;
    switch (d.layout) {
      case "bool":
        expect(v.getUint8(at) === 1).toBe(expected);
        at++;
        break;
      case "i32":
        expect(v.getInt32(at, true)).toBe(expected);
        at += 4;
        break;
      case "f32":
      case "f64": {
        const width = d.values.BYTES_PER_ELEMENT;
        expect(bytes.subarray(at, at + width)).toEqual(
          new Uint8Array(d.values.buffer, d.values.byteOffset + row * width, width),
        );
        at += width;
        break;
      }
      case "int64":
        expect(v.getBigInt64(at, true)).toBe(expected);
        at += 8;
        break;
      case "decimal": {
        const e = expected as { coefficient: bigint; scale?: number; special: number };
        if (e.special === 0)
          expect(
            BigInt.asIntN(128, v.getBigUint64(at, true) | (v.getBigUint64(at + 8, true) << 64n)),
          ).toBe(e.coefficient);
        expect(v.getUint16(at + 16, true)).toBe(e.scale ?? d.scale);
        expect(bytes[at + 18]).toBe(e.special);
        at += 19;
        break;
      }
      case "interval": {
        const e = expected as { months: number; days: number; micros: bigint };
        expect(v.getInt32(at, true)).toBe(e.months);
        expect(v.getInt32(at + 4, true)).toBe(e.days);
        expect(v.getBigInt64(at + 8, true)).toBe(e.micros);
        at += 16;
        break;
      }
      case "fixed16":
        expect(bytes.subarray(at, at + 16)).toEqual(expected);
        at += 16;
        break;
      case "varlen":
        throw new Error("separate streamed dictionary oracle");
    }
  }
  expect(at).toBe(bytes.length);
}
for (const seed of seedsFor("next-snapshot-physical", [0xb257, 0xb258]))
  it(`preserves every fixed physical layout and nulls seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot physical", SNAPSHOT_PHYSICAL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: -10000, max: 10000 }), async (salt) => {
        reach.beginHistory();
        const uuid = Uint8Array.from({ length: 16 }, (_, i) => (salt + i) & 255);
        const make = (
          type: PhysicalType,
          values: Parameters<typeof vectorOf>[1],
          tier?: "int64" | "int128",
        ) => vectorOf(type, values, tier === undefined ? {} : { tier });
        const vectors = [
          make({ kind: "bool" }, [true, null, false]),
          make(INT, [salt, null, -salt]),
          make({ kind: "float32" }, [-0, null, Math.fround(salt / 3)]),
          make({ kind: "float64" }, [-0, null, salt / 7]),
          make({ kind: "date" }, [salt, null, -salt]),
          make({ kind: "time" }, [123456789n, null, 86400000000n]),
          make({ kind: "timestamp", zoned: false }, [-(2n ** 60n), null, 2n ** 60n]),
          make({ kind: "timestamp", zoned: true }, [2n ** 60n, null, -123n]),
          make({ kind: "int64" }, [2n ** 60n + BigInt(salt), null, -(2n ** 61n)]),
          make({ kind: "interval" }, [
            { months: salt, days: -3, micros: 2n ** 60n },
            null,
            { months: 0, days: 7, micros: -99n },
          ]),
          make(
            { kind: "decimal", precision: 38, scale: null },
            [
              { coefficient: 2n ** 100n + BigInt(salt), special: 0, scale: 3 },
              null,
              { coefficient: -123n, special: 0, scale: 7 },
            ],
            "int128",
          ),
          make(
            { kind: "decimal", precision: null, scale: null },
            [
              { coefficient: 0n, special: 1, scale: 0 },
              { coefficient: 0n, special: 2, scale: 0 },
              { coefficient: 0n, special: 3, scale: 0 },
            ],
            "int128",
          ),
          make({ kind: "uuid" }, [uuid, null, uuid.slice().reverse()]),
        ];
        // Preserve payload/sign bits independently of JS Number loads, including NaN payloads. Native write ingestion may quiet signaling NaNs.
        for (const vector of vectors)
          if (vector.data.layout === "f64" || vector.data.layout === "f32") {
            const lane = vector.data.values,
              v = new DataView(lane.buffer);
            if (lane instanceof Float64Array) v.setBigUint64(16, 0x7ff8000000000042n, true);
            else v.setUint32(8, 0x7fc00042, true);
          }
        for (const persisted of [false, true]) {
          const h = new Harness("strict", "memory"),
            path = await h.open();
          try {
            await autocommit(path, (st) =>
              st.changeCatalog({
                kind: "createTable",
                table: tableInput("physical", [
                  column("id", INT, false),
                  ...vectors.map((v, i) => column(`v${String(i)}`, v.type)),
                ]),
              }),
            );
            await autocommit(path, (st) =>
              st.table(1).insert(batch(ints([1, 2, 3]), ...vectors), null),
            );
            if (persisted) await path.recycleLog();
            const bytes: Uint8Array[] = [],
              b = budget(),
              pin = path.pin(),
              table = pin.table(1, { budget: b });
            expect(table.segments[0]?.kind).toBe(persisted ? "group" : "memory");
            await exportPinnedSnapshot(
              pin,
              new Uint8Array(16),
              catalog(),
              {
                write: (chunk) => {
                  bytes.push(chunk.slice());
                },
              },
              b,
            );
            expect(b.used).toBe(0);
            const reader = await SnapshotStreamReader.open(input(bytes), b);
            let id = 0,
              parts: Uint8Array[] = [];
            for await (const frame of reader.frames()) {
              if (frame.kind === SnapshotKind.columnBegin) {
                id = new DataView(frame.payload.buffer, frame.payload.byteOffset).getUint32(
                  8,
                  true,
                );
                parts = [];
              }
              if (frame.kind === SnapshotKind.columnPart) parts.push(frame.payload.slice());
              if (frame.kind !== SnapshotKind.columnEnd || id === 1) continue;
              const body = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
              let offset = 0;
              for (const part of parts) {
                body.set(part, offset);
                offset += part.length;
              }
              const source = vectors[id - 2];
              if (source === undefined) throw new Error("unexpected column");
              oracle(body, source);
              reach.reach(source.data.layout as (typeof SNAPSHOT_PHYSICAL_REACH)[number]);
            }
            expect(b.used).toBe(0);
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        }
      }),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  }, 60_000);

for (const seed of seedsFor("next-snapshot-float-words", [0xb259, 0xb25a]))
  it(`encodes raw float words without a Number load seed ${String(seed)}`, async () => {
    const { snapshotFixedValue } = await import("./snapshot-column.js");
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 0x3fffff }), (payload) => {
        for (const type of [{ kind: "float32" }, { kind: "float64" }] as const) {
          const vector = vectorOf(type, [0]),
            data = vector.data;
          if (data.layout !== "f32" && data.layout !== "f64") throw new Error("bad float fixture");
          const source = data.values,
            view = new DataView(source.buffer);
          if (data.layout === "f32") view.setUint32(0, 0x7f800000 | payload, true);
          else view.setBigUint64(0, 0x7ff0000000000000n | BigInt(payload), true);
          const into = new Uint8Array(32),
            length = snapshotFixedValue(vector, 0, into);
          expect(into.subarray(1, length)).toEqual(new Uint8Array(source.buffer));
        }
      }),
      { seed, numRuns: 40 },
    );
  });
