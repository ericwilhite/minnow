import fc from "fast-check";
import { expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STORAGE_CANONICAL_REACH } from "../../testing/reach.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { ByteStore } from "../storage/idb-bytes.js";
import { encodeListPage, decodeListPage } from "../storage/extent-list.js";
import { NativeExtentList, NATIVE_LIST_WINDOW_BYTES } from "../storage/native-extent-list.js";
import { StoreCorruptionError } from "../storage/contract.js";
import {
  encodeFrame,
  encodeLogHeader,
  scanLog,
  scanLogIncremental,
  LogCorruptionError,
} from "../format/log-frame.js";
import { requireHeader } from "../durable-format.js";
import { xxHash32 } from "../hash/xxhash32.js";

for (const seed of seedsFor("next-storage-canonical", [0xb2d7, 0xb2d8]))
  it(`canonical storage refusals and unchanged writer bytes ${String(seed)}`, async () => {
    const reach = createReach("canonical storage", STORAGE_CANONICAL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 10 }),
        fc.integer({ min: 1, max: 7 }),
        fc.integer({ min: 0, max: 6 }),
        async (bit, padding, position) => {
          reach.beginHistory();
          const db = await openIndexedDb(new IDBFactory(), "canonical");
          try {
            await db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
              tx.store("data").put(0, new Uint8Array([7]).buffer);
              tx.store("data-index").put(0, {
                offset: 0,
                length: 1,
                [`extra${String(bit)}`]: padding,
              });
            });
            await expect(ByteStore.openLazy(db, "data")).rejects.toBeInstanceOf(
              StoreCorruptionError,
            );
            reach.reach("closed IDB fields");
          } finally {
            db.close();
          }
          const original = encodeFrame(
            { salt: 7, lsn: 1, partIndex: 0, partCount: 1 },
            new Uint8Array(8 - padding),
            7,
          );
          const incarnation = { salt: 7, firstLsn: 1, rootSequence: 1 };
          for (const kind of ["LSN high bits", "padding"] as const) {
            const bytes = original.bytes.slice(),
              v = new DataView(bytes.buffer);
            if (kind === "LSN high bits") v.setUint32(12, 2 ** (21 + bit), true);
            else bytes[28 + 8 - padding + (position % padding)] = 1 << (bit % 8);
            v.setUint32(bytes.length - 4, xxHash32(bytes, 7, 0, bytes.length - 4), true);
            if (kind === "LSN high bits")
              expect(() => requireHeader("logFrame", bytes, LogCorruptionError)).toThrow(
                LogCorruptionError,
              );
            const header = encodeLogHeader({
              salt: 7,
              generation: 1,
              ackEnd: bytes.length,
              ackChain: v.getUint32(bytes.length - 4, true),
              ackLsn: 1,
              rootSequence: 1,
            });
            expect(() => scanLog(bytes, [header, null], incarnation)).toThrow(LogCorruptionError);
            expect(scanLog(bytes, [null, null], incarnation).validEnd).toBe(0);
            const prefixHeader = encodeLogHeader({
              salt: 7,
              generation: 1,
              ackEnd: original.bytes.length,
              ackChain: original.chain,
              ackLsn: 1,
              rootSequence: 1,
            });
            const suffix = bytes.slice(),
              suffixView = new DataView(suffix.buffer);
            suffixView.setUint32(8, 2, true);
            suffixView.setUint32(
              suffix.length - 4,
              xxHash32(suffix, original.chain, 0, suffix.length - 4),
              true,
            );
            const joined = new Uint8Array(original.bytes.length + suffix.length);
            joined.set(original.bytes);
            joined.set(suffix, original.bytes.length);
            const prefix = scanLog(joined, [prefixHeader, null], incarnation);
            expect(prefix.validEnd).toBe(original.bytes.length);
            expect(prefix.frames).toHaveLength(1);
            let delivered = 0,
              charged = 0;
            await expect(
              scanLogIncremental(
                { length: bytes.length, read: async (a, n) => bytes.subarray(a, a + n) },
                [header, null],
                incarnation,
                {
                  part() {
                    delivered++;
                  },
                  commit() {
                    throw new Error("derived commit");
                  },
                  abandon() {
                    throw new Error("no frame should have been accepted");
                  },
                },
                {
                  allocate(n) {
                    charged += n;
                    return true;
                  },
                  free(n) {
                    charged -= n;
                  },
                },
              ),
            ).rejects.toBeInstanceOf(LogCorruptionError);
            expect(delivered).toBe(0);
            expect(charged).toBe(0);
            if (kind === "LSN high bits")
              reach.reach(
                `LSN bit ${String(21 + bit)}` as (typeof STORAGE_CANONICAL_REACH)[number],
              );
            if (kind === "padding") {
              if (position % padding === 0) reach.reach("padding first");
              if (position % padding === padding - 1) reach.reach("padding last");
              if (position % padding > 0 && position % padding < padding - 1)
                reach.reach("padding interior");
            }
            reach.reach(kind);
          }
          const decoded = scanLog(original.bytes, [null, null], incarnation).frames[0];
          if (decoded === undefined) throw new Error("Valid writer frame was refused");
          expect(
            encodeFrame({ salt: 7, lsn: 1, partIndex: 0, partCount: 1 }, decoded.body, 7).bytes,
          ).toEqual(original.bytes);
          reach.reach("frame byte identity");
          for (const kind of [
            "valid",
            "alignment",
            "aligned overlap",
            "safe aligned end",
            "physical end",
          ] as const) {
            const extents =
              kind === "alignment"
                ? [{ offset: padding, length: 1 }]
                : kind === "aligned overlap"
                  ? [
                      { offset: 0, length: 4097 },
                      { offset: 4096, length: 1 },
                    ]
                  : kind === "safe aligned end"
                    ? [{ offset: 2 ** 53 - 4096, length: 4095 }]
                    : kind === "physical end"
                      ? [{ offset: 8192, length: 1 }]
                      : [
                          { offset: 0, length: 4096 },
                          { offset: 4096, length: padding },
                        ];
            const bytes = encodeListPage(extents),
              limit =
                kind === "safe aligned end"
                  ? Number.MAX_SAFE_INTEGER
                  : kind === "valid"
                    ? 4096 + padding
                    : 8192;
            const first = extents[0];
            if (first === undefined) throw new Error("Empty generated extent list");
            const page = {
              blob: {
                extent: { offset: 0, length: bytes.length },
                offset: 0,
                length: bytes.length,
                checksum: xxHash32(bytes),
              },
              first: first.offset,
              count: extents.length,
            };
            const native = new NativeExtentList(
              [page],
              {
                async readRaw(at: number, target: Uint8Array) {
                  target.set(bytes.subarray(at, at + target.length));
                },
              } as never,
              new Uint8Array(NATIVE_LIST_WINDOW_BYTES),
              limit,
            );
            const read = async () => {
              const out = [];
              try {
                while (await native.next())
                  out.push({ offset: native.offset, length: native.length });
                return out;
              } finally {
                native.close();
              }
            };
            if (kind === "valid") {
              const values = decodeListPage(bytes, page, limit);
              expect(await read()).toEqual(values);
              expect(encodeListPage(values)).toEqual(bytes);
            } else {
              expect(() => decodeListPage(bytes, page, limit)).toThrow(StoreCorruptionError);
              await expect(read()).rejects.toBeInstanceOf(StoreCorruptionError);
            }
            reach.reach(kind);
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          ...Array.from(
            { length: 11 },
            (_, bit) => [bit, 1 + (bit % 7), bit % 7] as [number, number, number],
          ),
          [0, 7, 3] as [number, number, number],
        ],
      },
    );
    reach.assertReached();
  });
