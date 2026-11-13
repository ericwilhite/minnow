/** Known-size native flushes reserve actual rows, not a full 65,536-row group.
 * Authority: GroupWriter's estimate-only contract and storage/extents.ts progress/ownership.
 * Bound reservations BEFORE storage can turn a sparse multi-GiB offset into resident memory. */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WIDE_ROW_RECYCLE_STATES } from "../../testing/reach.js";
import { StorageCore } from "../testing/storage-core.js";
import { Governor, type Grant } from "../memory/governor.js";
import { GroupWriter } from "../page/writer.js";
import { ChunkReader } from "../page/reader.js";
import { defined, targetFor, valueAt, vectorOf } from "../page/test-support.js";
import type { FlatVector } from "../vector/vector.js";
import { need } from "./bytes.js";

const reach = createReach("native wide-row recycle progress", WIDE_ROW_RECYCLE_STATES);
afterAll(() => reach.assertReached());
// eslint-disable-next-line @typescript-eslint/unbound-method -- called with the original writer receiver.
const append = GroupWriter.prototype.append;
// eslint-disable-next-line @typescript-eslint/unbound-method -- called with the original writer receiver.
const finish = GroupWriter.prototype.finish;
const utf8 = new TextEncoder();
const json = { kind: "json", binary: true } as const;
const widths = [16383, 32767, 43690, 65520, 65537, 131072] as const;
const columnCounts = [1, 4, 6, 8] as const;
const widthStates = {
  16383: "width 16383",
  32767: "width 32767",
  43690: "width 43690",
  65520: "width 65520",
  65537: "width 65537",
  131072: "width 131072",
} as const;
const columnStates = {
  1: "JSONB columns 1",
  4: "JSONB columns 4",
  6: "JSONB columns 6",
  8: "JSONB columns 8",
} as const;

for (const seed of seedsFor("next-wide-row-recycle", [46912]))
  it(`bounds wide native flush work and retains exact bytes after reopen (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          width: fc.constantFrom(...widths),
          columns: fc.constantFrom(...columnCounts),
          rows: fc.integer({ min: 2, max: 3 }),
          bitmap: fc.boolean(),
        }),
        async ({ width, columns, rows, bitmap }) => {
          reach.beginHistory();
          const core = new StorageCore();
          Object.assign(core.harness, { governor: new Governor(64 << 20) });
          const names = Array.from({ length: columns }, (_, i) => "k" + String(i));
          let reservations = 0,
            totalCapacity = 0,
            appends = 0,
            finishes = 0;
          const vector = (type: FlatVector["type"], values: Uint8Array[]) => {
            const result = vectorOf(type, values);
            return bitmap ? { ...result, validity: Uint32Array.of((1 << rows) - 1) } : result;
          };
          const values = names.map((_, i) =>
            Array.from({ length: rows }, (_, row) => {
              const label =
                String(i) + "k".repeat(width) + (i === columns - 1 ? String(row % 3) : "same");
              return utf8.encode(
                row % 2
                  ? `{"s":${JSON.stringify(label)},"n":1.00}`
                  : `{"n":1,"s":${JSON.stringify(label)}}`,
              );
            }),
          );
          const payload = values.reduce(
            (sum, column) => sum + column.reduce((n, v) => n + v.length, 0),
            0,
          );
          // This guard rejects the old 3.15 GB request before allocating or zero-filling it.
          // It remains active through reopen and never clips or rewrites a reservation.
          core.harness.onSpace = (space) => {
            const begin = space.beginWrite.bind(space);
            space.beginWrite = (...args) => {
              const writer = begin(...args),
                open = writer.open.bind(writer);
              writer.open = (capacity) => {
                expect(capacity, "bounded native extent reservation").toBeLessThan(2 << 20);
                totalCapacity += capacity;
                reservations++;
                expect(reservations, "flush must make forward progress").toBeLessThan(128);
                return open(capacity);
              };
              return writer;
            };
          };
          const appendSpy = vi
            .spyOn(GroupWriter.prototype, "append")
            .mockImplementation(async function (this: GroupWriter, ...args) {
              appends++;
              const charge = this.memory.charge;
              expect(charge, "native encoder must own charged storage").not.toBeNull();
              const check = () => {
                expect(this.retainedBytes).toBeGreaterThan(0);
                expect((charge as Grant).allocated).toBeGreaterThanOrEqual(this.retainedBytes);
              };
              check();
              const taken = await append.apply(this, args);
              expect(taken, "every nonempty native append advances").toBe(args[1]);
              check();
              return taken;
            });
          const finishSpy = vi
            .spyOn(GroupWriter.prototype, "finish")
            .mockImplementation(async function (this: GroupWriter) {
              const written = await finish.call(this);
              finishes++;
              expect(this.retainedBytes, "finished encoder releases all scratch").toBe(0);
              return written;
            });
          try {
            await core.open();
            await core.createTable({
              name: "wide",
              columns: [
                { name: "n", type: "number" },
                { name: "p", type: "string" },
                ...names.map((name) => ({
                  name,
                  type: "string" as const,
                  nullable: true,
                  sqlDomain: { kind: "jsonb" as const },
                })),
              ],
            });
            const table = need(core.harness.path.catalog.tableByName("wide"));
            expect(
              await core.insertVectors(table, {
                length: rows,
                selection: null,
                columns: [
                  vectorOf(
                    { kind: "float64" },
                    Array.from({ length: rows }, (_, i) => i + 1),
                  ),
                  vector(
                    { kind: "text" },
                    Array.from({ length: rows }, (_, i) => utf8.encode(String(i + 1))),
                  ),
                  ...values.map((column) => vector(json, column)),
                ],
              }),
            ).toBe(rows);
            const started = performance.now();
            await core.harness.path.recycleLog();
            // recycleLog records background failures; quiesce is the public settlement check.
            await core.harness.path.quiesce();
            expect(
              performance.now() - started,
              "recycle deadline, not a performance target",
            ).toBeLessThan(10000);
            expect(appends).toBe(1);
            expect(finishes).toBe(1);
            expect(totalCapacity).toBeLessThan(payload * 4 + (1 << 20));
            expect(core.harness.governor.peakBytes).toBeLessThanOrEqual(64 << 20);
            reach.reach("bounded reservations");
            reach.reach("charged encoder");
            reach.reach("complete append");
            reach.reach(width < 65520 ? "inline wide values" : "large value boundary");
            reach.reach(columnStates[columns]);
            reach.reach(widthStates[width]);
            reach.reach(rows === 2 ? "two rows" : "three rows");
            reach.reach(bitmap ? "validity bitmap" : "no validity bitmap");
            // Read one native chunk at a time: no executor/query-admission dependency.
            const verify = async () => {
              const pin = core.harness.path.pin();
              const grant = await core.harness.governor.admit({
                label: "wide recycle reader",
                minimumBytes: 8 << 20,
                spillable: true,
              });
              try {
                const snapshot = pin.table(table.id, { budget: grant });
                expect(snapshot.segments.length).toBe(1);
                const segment = defined(snapshot.segments[0]);
                expect(segment.kind).toBe("group");
                if (segment.kind !== "group") throw Error("Recycle left an unflushed memtable");
                await segment.root();
                for (let i = 0; i < columns; i++) {
                  const chunk = need(segment.chunk(need(table.columns[i + 2]).id));
                  const reader = await ChunkReader.open(pin.pages, chunk, { budget: grant });
                  try {
                    await reader.load(
                      Array.from({ length: reader.directory.pages.length }, (_, p) => p),
                    );
                    await reader.loadValues(0, rows);
                    const target = targetFor(reader, rows, reader.varlenBytes(0, rows));
                    reader.decodeRange(0, rows, target, 0);
                    for (let row = 0; row < rows; row++) {
                      const actual = valueAt(target, row);
                      if (!(actual instanceof Uint8Array))
                        throw Error("Expected native JSON bytes");
                      expect(Buffer.compare(actual, defined(defined(values[i])[row]))).toBe(0);
                    }
                  } finally {
                    reader.close();
                  }
                }
              } finally {
                pin.release();
                grant.close();
              }
            };
            await verify();
            reach.reach("exact native readback");
            await core.reopen();
            await verify();
            reach.reach("exact reopened readback");
          } finally {
            appendSpy.mockRestore();
            finishSpy.mockRestore();
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          reach.reach("all charges released");
        },
      ),
      {
        seed,
        numRuns: 24,
        endOnFailure: true,
        examples: [
          [{ width: 43690, columns: 6, rows: 2, bitmap: true }],
          [{ width: 65537, columns: 8, rows: 3, bitmap: false }],
        ],
      },
    );
  }, 30000);
