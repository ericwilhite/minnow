/** A feasible durable load grants/stages, never refuses a whole oversized input estimate. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_BATCH_ADMISSION_STATES } from "../../testing/reach.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";

const MiB = 1024 * 1024;
const reach = createReach("oversized input admission", WRITE_BATCH_ADMISSION_STATES);
for (const backend of ["memory", "indexeddb"] as const)
  for (const seed of seedsFor("next-write-batch-admission", [0xb47c]))
    it(`stages inside one oversized ${backend} input (seed ${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            capMiB: fc.constantFrom(32, 48),
            rows: fc.constantFrom(12000, 16384),
            width: fc.constantFrom(32, 64),
          }),
          async (s) => {
            reach.beginHistory();
            reach.reach(backend);
            reach.reach("startup retained");
            const governor = new Governor(s.capMiB * MiB);
            const harness = new Harness("strict", "memory", 256, undefined, {
              // Isolate input admission: this real non-spillable fixture leaves too little
              // room for a merge minimum. low-cap.test.ts separately exercises merges.
              merges: { mode: "off" },
            });
            Object.assign(harness, { governor });
            if (backend === "indexeddb")
              Object.assign(harness, {
                backend: createIndexedDbBackend({
                  database: () => simIndexedDb(harness.disk),
                  name: "batch-admission",
                  locks: new SimLocks(),
                  memory: governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
                }),
              });
            let path = await harness.open();
            let pressure: Awaited<ReturnType<Governor["admit"]>> | null = null;
            let buffer: Uint8Array | null = null;
            try {
              let table = 0;
              await autocommit(path, async (statement) => {
                table =
                  (
                    await statement.changeCatalog({
                      kind: "createTable",
                      table: tableInput("t", [
                        column("id", INT, false),
                        ...Array.from({ length: 8 }, (_, k) => column(`v${String(k)}`, TEXT)),
                      ]),
                    })
                  ).id ?? 0;
              });
              await path.quiesce();
              const bytes =
                governor.capBytes - governor.reserveBytes - governor.usedBytes - 2 * MiB;
              pressure = await governor.admit({
                label: "other live working memory",
                minimumBytes: bytes,
                spillable: false,
              });
              expect(pressure.allocate(bytes)).toBe(true);
              buffer = new Uint8Array(bytes).fill(0x5a);
              reach.reach("other live working memory");
              const ids = Array.from({ length: s.rows }, (_, k) => k);
              const strings = ids.map((id) => `${String(id)}:${"x".repeat(s.width)}`);
              reach.reach("oversized input");
              await autocommit(path, (statement) =>
                statement
                  .table(table)
                  .insert(
                    batch(ints(ids), ...Array.from({ length: 8 }, () => texts(strings))),
                    null,
                  ),
              );
              await path.quiesce();
              expect(path.maintenance.stagedRows).toBeGreaterThan(0);
              reach.reach("staged within call");
              expect(path.failure).toBeNull();
              expect(path.maintenance.mergeFailures).toBe(0);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              expect(buffer[0]).toBe(0x5a);
              expect(buffer.at(-1)).toBe(0x5a);
              pressure.free(buffer.byteLength);
              buffer = null;
              pressure.close();
              pressure = null;
              path = await harness.reopen();
              const view = path.pin();
              try {
                const actual = await rowsOf(view, table);
                expect(actual.length).toBe(s.rows);
                for (let row = 0; row < s.rows; row++) {
                  expect(actual[row]?.[0]).toBe(row);
                  for (let col = 1; col <= 8; col++) expect(actual[row]?.[col]).toBe(strings[row]);
                }
              } finally {
                view.release();
              }
              reach.reach("durable reopen");
            } finally {
              if (buffer) pressure?.free(buffer.byteLength);
              pressure?.close();
              await harness.close();
            }
            expect(governor.usedBytes).toBe(0);
          },
        ),
        { seed, numRuns: 8 },
      );
    }, 120_000);
it("reaches bounded admission states", () => reach.assertReached());
