/** Snapshot1 framing law: complete ordered, bounded, CRC-protected objects, never partial success. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_CODEC_REACH } from "../../testing/reach.js";
import { TaskClock } from "../task.js";
import type { ByteBudget } from "../storage/extents.js";
import {
  SnapshotStreamReader,
  SnapshotStreamWriter,
  SnapshotKind,
  snapshotCrc32,
  snapshotNumbers,
  encodeSnapshotHeader,
  decodeSnapshotHeader,
  encodeSnapshotFrame,
  decodeSnapshotFrame,
  snapshotFrameBytes,
  SNAPSHOT_PAYLOAD_BYTES,
} from "./snapshot.js";

class Budget implements ByteBudget {
  used = 0;
  peak = 0;
  constructor(readonly cap = 5 << 20) {}
  allocate(n: number): boolean {
    if (this.used + n > this.cap) return false;
    this.used += n;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(n: number): void {
    this.used -= n;
    if (this.used < 0) throw new Error("Snapshot budget double free");
  }
}
const numbers = (...values: number[]): Uint8Array => {
  const out = new Uint8Array(values.length * 8);
  snapshotNumbers(values, out);
  return out;
};
async function archive(rows: number, columns: number, data: Uint8Array): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [],
    budget = new Budget();
  const writer = await SnapshotStreamWriter.open(
    { id: Uint8Array.from({ length: 16 }, (_, i) => i), sourceLsn: 99 },
    {
      write: (bytes) => {
        chunks.push(bytes.slice());
      },
    },
    budget,
  );
  const catalog = new Uint8Array([7, 9]);
  await writer.frame(SnapshotKind.catalogBegin, 1, 0, new Uint8Array());
  await writer.frame(SnapshotKind.catalogPart, 1, 1, catalog);
  const end = numbers(catalog.length, 0);
  new DataView(end.buffer).setUint32(8, snapshotCrc32(catalog), true);
  await writer.frame(SnapshotKind.catalogEnd, 1, 2, end);
  if (rows !== 0 || columns !== 0) {
    await writer.frame(SnapshotKind.tableBegin, 2, 0, numbers(7, 1, columns));
    if (rows !== 0)
      for (let c = 1; c <= columns; c++) {
        await writer.frame(
          SnapshotKind.columnBegin,
          2 + c,
          0,
          numbers(7, c, 1, 0, rows, data.length),
        );
        await writer.frame(SnapshotKind.columnPart, 2 + c, 1, data);
        await writer.frame(SnapshotKind.columnEnd, 2 + c, 2, numbers(data.length, rows));
      }
    await writer.frame(SnapshotKind.tableEnd, 2, 1, numbers(rows));
  }
  await writer.finish();
  expect(budget.used).toBe(0);
  const bytes = new Uint8Array(chunks.reduce((n, chunk) => n + chunk.length, 0));
  let at = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, at);
    at += chunk.length;
  }
  return bytes;
}
async function* chunks(bytes: Uint8Array, size: number): AsyncGenerator<Uint8Array> {
  yield new Uint8Array();
  for (let at = 0; at < bytes.length; at += size) yield bytes.subarray(at, at + size);
}
async function read(bytes: Uint8Array, size: number, budget = new Budget()): Promise<number[]> {
  try {
    const reader = await SnapshotStreamReader.open(chunks(bytes, size), budget, {
        clock: new TaskClock(() => 0),
      }),
      kinds: number[] = [];
    for await (const frame of reader.frames()) kinds.push(frame.kind);
    return kinds;
  } finally {
    expect(budget.used).toBe(0);
  }
}
function crcReference(bytes: Uint8Array): number {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}
for (const seed of seedsFor("next-snapshot-codec", [0xb251, 0xb252]))
  it(`round trips and refuses corrupt or incomplete streams seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot codec", SNAPSHOT_CODEC_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.uint8Array({ minLength: 1, maxLength: 96 }),
        fc.integer({ min: 1, max: 200 }),
        async (data, split) => {
          reach.beginHistory();
          expect(snapshotCrc32(data)).toBe(crcReference(data));
          expect(snapshotCrc32(new TextEncoder().encode("123456789"))).toBe(0xcbf43926);
          const middle = data.length >>> 1;
          expect(
            snapshotCrc32(data.subarray(middle), snapshotCrc32(data.subarray(0, middle))),
          ).toBe(crcReference(data));
          for (const [rows, columns] of [
            [0, 0],
            [2, 0],
            [0, 2],
            [3, 2],
          ] as const) {
            const bytes = await archive(rows, columns, data);
            const kinds = await read(bytes, split);
            expect(kinds.at(-1)).toBe(SnapshotKind.footer);
            reach.reach(rows === 0 ? "empty tables" : "table rows");
            if (columns === 0) reach.reach("zero columns");
            if (columns > 1) reach.reach("multiple columns");
            reach.reach("split input");
            if (rows > 0 && columns > 0) {
              expect(new Set(kinds)).toEqual(new Set(Object.values(SnapshotKind)));
              reach.reach("all kinds");
              // Every cut, including inside both CRCs and padding, must refuse.
              for (let at = 0; at < bytes.length; at++)
                await expect(read(bytes.subarray(0, at), bytes.length)).rejects.toMatchObject({
                  code: "MINNOW_SNAPSHOT_FORMAT",
                });
              reach.reach("truncation");
              for (let at = 0; at < bytes.length; at++) {
                const bad = bytes.slice();
                bad[at] = (bad[at] ?? 0) ^ 1;
                await expect(read(bad, bytes.length)).rejects.toMatchObject({
                  code: "MINNOW_SNAPSHOT_FORMAT",
                });
              }
              reach.reach("corruption");
              // CRC-valid reordered object. Recompute both CRCs to reach the ordering law.
              const bad = bytes.slice(),
                v = new DataView(bad.buffer),
                start = 64;
              v.setUint32(start + 8, 1, true);
              v.setUint32(start + 36, snapshotCrc32(bad.subarray(start, start + 36)), true);
              v.setUint32(start + 40, snapshotCrc32(bad.subarray(start, start + 40)), true);
              await expect(read(bad, split)).rejects.toThrow(/ordinal/u);
              reach.reach("reorder");
              const trailing = new Uint8Array(bytes.length + 1);
              trailing.set(bytes);
              await expect(read(trailing, split)).rejects.toThrow(/Trailing/u);
              reach.reach("trailing");
              await expect(read(bytes, split, new Budget(1100))).rejects.toMatchObject({
                code: "MINNOW_SNAPSHOT_BUDGET",
              });
              reach.reach("quota");
              const budget = new Budget(),
                controller = new AbortController();
              const reader = await SnapshotStreamReader.open(chunks(bytes, split), budget, {
                signal: controller.signal,
              });
              const stream = reader.frames();
              await stream.next();
              controller.abort(new Error("cancel snapshot"));
              await expect(stream.next()).rejects.toThrow("cancel snapshot");
              expect(budget.used).toBe(0);
              reach.reach("cancel");
            }
          }
          const large = new Uint8Array(SNAPSHOT_PAYLOAD_BYTES);
          large.set(data);
          const frame = {
            kind: SnapshotKind.columnPart,
            ordinal: 99,
            objectId: 5,
            partIndex: 2,
            payload: large,
          };
          const encoded = new Uint8Array(snapshotFrameBytes(large.length));
          encodeSnapshotFrame(frame, encoded);
          expect(decodeSnapshotFrame(encoded).payload).toEqual(large);
          reach.reach("large part");
        },
      ),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  }, 60_000);

it("validates every frame kind and argument before mutating output; refuses legacy headers", () => {
  const output = new Uint8Array(64).fill(93),
    before = output.slice();
  expect(() => encodeSnapshotHeader({ id: new Uint8Array(15), sourceLsn: 0 }, output)).toThrow();
  expect(output).toEqual(before);
  const legacy = new Uint8Array(64);
  legacy.set(new TextEncoder().encode("MINSNAP1"));
  expect(() => decodeSnapshotHeader(legacy)).toThrow(
    expect.objectContaining({ code: "MINNOW_LEGACY_DATA" }),
  );
  for (const kind of Object.values(SnapshotKind)) {
    const payload = new Uint8Array([1, 2, 3]),
      into = new Uint8Array(snapshotFrameBytes(3));
    encodeSnapshotFrame(
      { kind, ordinal: Number.MAX_SAFE_INTEGER, objectId: 1, partIndex: 0xffffffff, payload },
      into,
    );
    expect(decodeSnapshotFrame(into)).toMatchObject({
      kind,
      ordinal: Number.MAX_SAFE_INTEGER,
      objectId: 1,
      partIndex: 0xffffffff,
      payload,
    });
    for (let at = 0; at < into.length; at++) {
      const bad = into.slice();
      bad[at] = (bad[at] ?? 0) ^ 1;
      expect(() => decodeSnapshotFrame(bad)).toThrow(
        expect.objectContaining({ code: "MINNOW_SNAPSHOT_FORMAT" }),
      );
    }
  }
});

it("keeps a pending sink charged through close and releases a reader closed while suspended", async () => {
  const budget = new Budget();
  let unblock!: () => void,
    block = false;
  const writer = await SnapshotStreamWriter.open(
    { id: new Uint8Array(16), sourceLsn: 0 },
    {
      write: () =>
        block
          ? new Promise<void>((resolve) => {
              unblock = resolve;
            })
          : undefined,
    },
    budget,
  );
  block = true;
  const pending = writer.frame(SnapshotKind.catalogBegin, 1, 0, new Uint8Array());
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  writer.close();
  expect(budget.used).toBeGreaterThan(1024);
  unblock();
  await pending;
  expect(budget.used).toBe(0);
  const bytes = await archive(1, 1, new Uint8Array([1]));
  const reader = await SnapshotStreamReader.open(chunks(bytes, 7), budget),
    stream = reader.frames();
  await stream.next();
  reader.close();
  expect(budget.used).toBe(0);
  await stream.return(undefined);
  expect(budget.used).toBe(0);
});
