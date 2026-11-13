/** Source reach is earned by observed native kinds/encodings, not by log flush calls. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_SOURCE_REACH } from "../../testing/reach.js";
import { SnapshotKind, SnapshotStreamReader } from "../format/snapshot.js";
import { PageEncoding } from "../format/manifest.js";
import { ChunkReader } from "../page/reader.js";
import { vectorOf } from "../page/test-support.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { exportPinnedSnapshot } from "./snapshot-export.js";
import { decodeSnapshotColumnHeader } from "./snapshot-column.js";
import { Harness, autocommit, batch, column, INT, ints, tableInput } from "./test-support.js";

for (const seed of seedsFor("next-snapshot-source", [0xb25f, 0xb260]))
  it(`checks published encodings and preserves array bytes seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot source", SNAPSHOT_SOURCE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: -1000, max: 1000 }), async (salt) => {
        reach.beginHistory();
        const element = INT,
          type = { kind: "array", element } as const;
        const a = encodeArrayValue(element, {
          dimensions: [3],
          lowerBounds: [-2],
          elements: [salt, null, -salt],
        });
        const values = [a, null, a, a];
        for (const persisted of [false, true]) {
          const h = new Harness("strict", "memory"),
            path = await h.open();
          const b = {
            used: 0,
            allocate(n: number) {
              this.used += n;
              return true;
            },
            free(n: number) {
              this.used -= n;
            },
          };
          try {
            await autocommit(path, (st) =>
              st.changeCatalog({
                kind: "createTable",
                table: tableInput("arrays", [column("constant", INT), column("a", type)], {
                  primaryKey: null,
                }),
              }),
            );
            await autocommit(path, (st) =>
              st
                .table(1)
                .insert(batch(ints([salt, salt, salt, salt]), vectorOf(type, values)), null),
            );
            if (persisted) await path.recycleLog();
            const pin = path.pin(),
              table = pin.table(1, { budget: b }),
              segment = table.segments[0];
            expect(segment?.kind).toBe(persisted ? "group" : "memory");
            if (segment?.kind === "group") {
              await segment.root();
              for (const [id, encoding] of [
                [1, PageEncoding.constant],
                [2, PageEncoding.dictionary],
              ]) {
                const chunk = segment.chunk(id ?? 0);
                if (chunk === null) throw new Error("missing published fixture chunk");
                const reader = await ChunkReader.open(pin.pages, chunk, { budget: b, once: true });
                try {
                  expect(reader.directory.pages[0]?.encoding).toBe(encoding);
                } finally {
                  reader.close();
                }
              }
              reach.reach("published constant page");
              reach.reach("published dictionary page");
            } else reach.reach("resident vector");
            const chunks: Uint8Array[] = [];
            await exportPinnedSnapshot(
              pin,
              new Uint8Array(16),
              (async function* () {
                yield new Uint8Array([1]);
              })(),
              {
                write(bytes) {
                  chunks.push(bytes.slice());
                },
              },
              b,
            );
            expect(b.used).toBe(0);
            const input = await SnapshotStreamReader.open(
              (async function* () {
                yield* chunks;
              })(),
              b,
            );
            let selected = false;
            for await (const frame of input.frames()) {
              if (frame.kind === SnapshotKind.columnBegin)
                selected =
                  new DataView(frame.payload.buffer, frame.payload.byteOffset).getUint32(
                    8,
                    true,
                  ) === 2;
              if (frame.kind !== SnapshotKind.columnPart || !selected) continue;
              const bytes = frame.payload,
                view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
              expect(decodeSnapshotColumnHeader(type, bytes.subarray(0, 16))).toBe(4);
              let at = 16;
              for (const value of values) {
                expect(bytes[at++]).toBe(value === null ? 0 : 1);
                if (value === null) continue;
                const length = view.getUint32(at, true);
                at += 4;
                expect(bytes.subarray(at, at + length)).toEqual(value);
                at += length;
                expect(bytes[at++]).toBe(2);
                expect(view.getUint32(at, true)).toBe(0);
                at += 4;
              }
              expect(at).toBe(bytes.length);
              reach.reach("array byte equality");
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
