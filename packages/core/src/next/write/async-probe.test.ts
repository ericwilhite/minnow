/** MaybePromise is a completed result, not a request to retry until the cache answers synchronously. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_ASYNC_PROBE_STATES } from "../../testing/reach.js";
import { KeyBatch, encodeKeyBatch } from "../keys/key-encoding.js";
import type { ProbeResume } from "../access/key-index.js";
import { SimLocks } from "../storage/sim-disk.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { RunReader } from "./runs.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  locators,
  tableInput,
} from "./test-support.js";

const reach = createReach("asynchronous index probe continuation", WRITE_ASYNC_PROBE_STATES);
for (const seed of seedsFor("next-write-async-probe", [0xb3c6]))
  it(`consumes completed asynchronous reads without replay (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          rows: fc.integer({ min: 2, max: 12 }),
          capacity: fc.constantFrom(1, 2, 4),
          unique: fc.boolean(),
          metadata: fc.boolean(),
          backend: fc.constantFrom("memory", "indexeddb"),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.backend);
          reach.reach(s.unique ? "unique" : "nonunique");
          const h = new Harness("strict", "memory", 4);
          if (s.backend === "indexeddb")
            Object.assign(h, {
              backend: createIndexedDbBackend({
                database: () => simIndexedDb(h.disk),
                name: "async-probe",
                locks: new SimLocks(),
                memory: h.governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
              }),
            });
          const p = await h.open();
          let table = 0,
            index = 0;
          try {
            await autocommit(p, async (st) => {
              table =
                (
                  await st.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false), column("v", INT, false)]),
                  })
                ).id ?? 0;
            });
            if (s.unique) index = p.catalog.table(table)?.primaryKey ?? 0;
            else
              await autocommit(p, async (st) => {
                index =
                  (
                    await st.changeCatalog({
                      kind: "createIndex",
                      index: {
                        tableId: table,
                        name: "v",
                        columnIds: [p.catalog.table(table)?.columns[1]?.id ?? 0],
                        unique: false,
                        role: "secondary",
                      },
                    })
                  ).id ?? 0;
              });
            const ids = Array.from({ length: s.rows }, (_, k) => k),
              values = ids.map((k) => (s.unique ? k : k % 3));
            await autocommit(p, (st) =>
              st.table(table).insert(batch(ints(ids), ints(values)), null),
            );
            await p.recycleLog();
            await p.quiesce();
            expect(p.stats().levelZeroRuns).toBeGreaterThan(0);
            const old = p.pin();
            try {
              await autocommit(p, (st) =>
                st
                  .table(table)
                  .delete(batch(locators([old.internalTable(table).locator(1)], table)), null),
              );
              let reads = 0,
                metadataReads = 0;
              // A legal SpaceReader can decline every peek. Completion must not depend on caching.
              vi.spyOn(h.space, "peek").mockReturnValue(null);
              // eslint-disable-next-line @typescript-eslint/unbound-method -- explicitly called with the reader receiver below.
              const lookup = RunReader.prototype.lookup;
              vi.spyOn(RunReader.prototype, "lookup").mockImplementation(function (
                this: RunReader,
                ...args
              ) {
                if (++reads > 512) throw new Error("probe replayed a completed block read");
                return lookup.apply(this, args);
              });
              if (s.metadata) {
                const get = p.shared.runReader.bind(p.shared);
                vi.spyOn(p.shared, "runReader").mockImplementation((ref) => {
                  if (++metadataReads > 512)
                    throw new Error("probe replayed a completed metadata read");
                  return Promise.resolve(get(ref));
                });
                reach.reach("asynchronous metadata");
              }
              reach.reach("asynchronous blocks");
              for (const beforeDelete of [true, false]) {
                const view = beforeDelete ? old : p.pin(),
                  keys = new KeyBatch({ allocate: () => true, free: () => undefined });
                try {
                  const query = [0, 1, 0, 2, 99];
                  encodeKeyBatch(
                    "equality",
                    [{ vector: ints(query), direction: "asc", nulls: "first" }],
                    { length: query.length, selection: null },
                    keys,
                  );
                  const expected = query.flatMap((key, probe) =>
                    ids
                      .filter((id) => values[id] === key && (beforeDelete || id !== 0))
                      .map((id) => [probe, id + 1]),
                  );
                  const got: number[][] = [];
                  let resume: ProbeResume | undefined;
                  for (let calls = 0; ; calls++) {
                    expect(calls).toBeLessThan(expected.length + 2);
                    const out = {
                      probes: new Int32Array(s.capacity),
                      locators: new Float64Array(s.capacity),
                      count: 0,
                    };
                    const result = await view.index(table, index).probe(keys, out, resume);
                    for (let k = 0; k < out.count; k++)
                      got.push([
                        out.probes[k] ?? 0,
                        view.internalTable(table).rowId(out.locators[k] ?? 0),
                      ]);
                    if (result.done) break;
                    resume = result.resume;
                    reach.reach("output continuation");
                  }
                  expect(got).toEqual(expected);
                  reach.reach(beforeDelete ? "older pin" : "visible delete");
                } finally {
                  keys.close();
                  if (!beforeDelete) view.release();
                }
              }
              expect(reads).toBeGreaterThan(0);
            } finally {
              vi.restoreAllMocks();
              old.release();
            }
          } finally {
            vi.restoreAllMocks();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120_000);
it("reaches asynchronous probe states", () => reach.assertReached());
