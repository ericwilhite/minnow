/** Writer cardinality is private scratch, not an unfunded resident vector (R23/R25).
 * The independent logical root describes rows arithmetically, never by copying ownership.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITER_OWNED_PRODUCERS_REACH,
  WRITER_FOREIGN_OWNER_REACH,
  WRITER_OWNERSHIP_PRESSURE_REACH,
} from "../../testing/reach.js";
import { ExtentListKeeper } from "../storage/extent-list.js";
import { encodeRoot, openOwner, pressure } from "./retirement-test-support.js";

// Real write-path producers, rather than a synthetic ExtentWriter alone.
import { Governor, type Grant } from "../memory/governor.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  rules,
  tableInput,
  texts,
} from "../write/test-support.js";
import { setupFts, ftsParts } from "./fts-transaction.test-support.js";

for (const seed of seedsFor("next-writer-ownership-pressure", [0xb93f, 0xb940]))
  for (const count of [65, 4097])
    it(
      `bounds a native ${String(count)}-row writer seed ${String(seed)}`,
      async (context) => {
        const stop = new AbortController();
        let drain: (() => Promise<void>) | undefined;
        context.onTestFinished(async () => {
          stop.abort();
          await drain?.();
        });
        const reach = createReach("native writer pressure", WRITER_OWNERSHIP_PRESSURE_REACH);
        await fc.assert(
          fc.asyncProperty(fc.boolean(), async (publish) => {
            stop.signal.throwIfAborted();
            reach.beginHistory();
            const owner = await openOwner(),
              fixed = await pressure(owner, 128 << 10);
            const writer = owner.space.beginWrite("large single writer", {
              ownershipBudget: fixed.grant,
            });
            const keeper = new ExtentListKeeper([], {
              reader: owner.space,
              budget: owner.startup,
              pageExtents: 64,
            });
            let snapshot:
              Awaited<ReturnType<NonNullable<typeof writer.snapshotExtents>>> | undefined;
            let closing: Promise<void> | undefined;
            const close = () =>
              (closing ??= (async () => {
                try {
                  if (writer.state === "open") await writer.abort();
                } finally {
                  await snapshot?.release();
                  keeper.close();
                  fixed.close();
                  await owner.close();
                }
              })());
            drain = close;
            try {
              let first = -1;
              for (let i = 0; i < count; i++) {
                stop.signal.throwIfAborted();
                const output = await writer.write([Uint8Array.of(i & 255)]);
                if (i === 0) first = output.extent.offset;
                expect(output.extent.offset).toBe(first + i * 4096);
                expect(fixed.grant.allocated).toBeLessThanOrEqual(128 << 10);
              }
              // No API can accidentally clone the old unbounded vector. Length is scalar.
              expect(writer.extents.length).toBe(count);
              expect(() => writer.extents.slice()).toThrow(/inventory cursor/);
              reach.reach("bounded synchronous view");
              if (!writer.snapshotExtents) throw new Error("Missing bounded native ownership");
              snapshot = await writer.snapshotExtents({
                budget: owner.startup,
                signal: stop.signal,
              });
              expect(snapshot.count).toBe(count);
              const source = snapshot;
              const verify = async () => {
                const cursor = source.cursor({ budget: owner.startup, signal: stop.signal });
                try {
                  let i = 0;
                  while (await cursor.next()) {
                    expect(cursor.offset).toBe(first + i * 4096);
                    expect(cursor.length).toBe(1);
                    i++;
                  }
                  expect(i).toBe(count);
                } finally {
                  await cursor.close();
                }
              };
              await verify();
              reach.reach("exact immutable source");
              if (publish) {
                const delta = await keeper.prepare(1, {
                  added: writer.extents,
                  released: [],
                  writer,
                });
                const allocated = writer.bytes;
                await owner.space.publish(encodeRoot({ first, count, list: delta.list }), {
                  writers: [writer],
                  retired: delta.retire,
                  list: delta.list,
                  budget: owner.startup,
                });
                keeper.commit(1, 1);
                expect(writer.state).toBe("published");
                expect(writer.bytes).toBe(allocated);
                reach.reach("published source drained");
              } else {
                await writer.abort();
                expect(writer.state).toBe("aborted");
                expect(owner.space.stats.dataBytes).toBe(0);
                reach.reach("aborted source drained");
              }
              await verify();
              expect(owner.governor.peakBytes).toBeLessThanOrEqual(owner.governor.capBytes);
              reach.reach("all-credit competitor");
            } finally {
              await close();
            }
            reach.reach("zero ownership");
          }),
          { seed, numRuns: 2, examples: [[true], [false]] },
        );
        reach.assertReached();
      },
      process.env.CI ? 120_000 : 40_000,
    );

class FixedHarness extends Harness {
  override readonly governor = new Governor(64 << 20);
}
for (const seed of seedsFor("next-writer-owned-producers", [0xb941, 0xb942]))
  for (const fts of [false, true])
    it(
      `bounds ${fts ? "FTS" : "plain row"} writer ${String(seed)}`,
      async (context) => {
        const stop = new AbortController();
        let drain: (() => Promise<void>) | undefined;
        context.onTestFinished(async () => {
          stop.abort();
          await drain?.();
        });
        const reach = createReach("native writer producers", WRITER_OWNED_PRODUCERS_REACH);
        await fc.assert(
          fc.asyncProperty(fc.integer({ min: 0, max: 8 }), async (extra) => {
            stop.signal.throwIfAborted();
            reach.beginHistory();
            const h = new FixedHarness("strict", "memory", 32, undefined, {
              merges: { mode: "off" },
            });
            let competitor: Grant | undefined,
              largest = 0,
              seals = 0,
              armed = false,
              competed = false;
            let closing: Promise<void> | undefined;
            const labels: string[] = [];
            const close = () =>
              (closing ??= (async () => {
                competitor?.close();
                competitor = undefined;
                await h.close();
              })());
            drain = close;
            h.onSpace = (space) => {
              const publish = space.publish.bind(space);
              space.publish = async (...args) => {
                try {
                  return await publish(...args);
                } finally {
                  if (!fts) {
                    competitor?.close();
                    competitor = undefined;
                  }
                }
              };
              const begin = space.beginWrite.bind(space);
              space.beginWrite = (label, options) => {
                labels.push(label);
                const writer = begin(label, options),
                  open = writer.open.bind(writer);
                const pressure = async () => {
                  stop.signal.throwIfAborted();
                  if (
                    armed &&
                    !competed &&
                    (fts
                      ? label === "FTS preparation"
                      : label === "stage" || label.startsWith("flush t"))
                  ) {
                    competitor = await h.governor.admit({
                      label: "writer producer competing credit",
                      minimumBytes:
                        h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes,
                      spillable: false,
                      signal: stop.signal,
                    });
                    expect(competitor.allocate(competitor.held)).toBe(true);
                    competed = true;
                  }
                };
                writer.open = (capacity) => {
                  const builder = open(capacity),
                    append = builder.append.bind(builder),
                    seal = builder.seal.bind(builder);
                  builder.append = async (...args) => {
                    await pressure();
                    return append(...args);
                  };
                  builder.seal = async () => {
                    const result = await seal();
                    largest = Math.max(largest, writer.extents.length);
                    seals++;
                    if (writer.extents.length > 64)
                      expect(() => writer.extents.slice()).toThrow(/inventory cursor/);
                    return result;
                  };
                  return builder;
                };
                return writer;
              };
            };
            try {
              const path = fts ? await setupFts(h) : await h.open();
              const width = fts ? 1 : 96 + extra,
                count = fts ? 97 + extra : 1024 + 32 * extra;
              if (!fts)
                await autocommit(path, (s) =>
                  s.changeCatalog({
                    kind: "createTable",
                    table: tableInput(
                      "rows",
                      Array.from({ length: width }, (_, c) => column(`c${String(c)}`, INT, false)),
                      { primaryKey: null },
                    ),
                  }),
                );
              await path.quiesce();
              const tx = await path.begin({ signal: stop.signal }),
                s = tx.statement({ rules });
              const expected = Array.from({ length: count }, (_, r) =>
                fts
                  ? [`token${String(r)} shared`]
                  : Array.from(
                      { length: width },
                      (_, c) => (r * r * 17 + r * 31 + c * 997) % 10007,
                    ),
              );
              armed = true;
              await s
                .table(1)
                .insert(
                  fts
                    ? batch(texts(expected.map((row) => String(row[0]))))
                    : batch(
                        ...Array.from({ length: width }, (_, c) =>
                          ints(expected.map((row) => Number(row[c]))),
                        ),
                      ),
                  null,
                );
              await s.end();
              armed = true;
              const receipt = await tx.commit(null);
              await receipt.durable;
              if (!fts) {
                const tail = Array.from({ length: width }, (_, c) => c + 123456);
                await autocommit(path, (s) =>
                  s.table(1).insert(batch(...tail.map((value) => ints([value]))), null),
                );
                expected.push(tail);
                await path.quiesce();
              }
              expect(
                competed,
                JSON.stringify({ labels, largest, seals, maintenance: path.maintenance }),
              ).toBe(true);
              armed = false;
              competitor?.close();
              competitor = undefined;
              await path.quiesce();
              expect(largest).toBeGreaterThan(64);
              expect(seals).toBeGreaterThan(64);
              reach.reach("large single producer");
              reach.reach("competing publication credit");
              if (fts)
                expect((await ftsParts(h)).entries.filter((e) => e.kind === "insert")).toHaveLength(
                  count,
                );
              const pin = path.pin({});
              try {
                expect(await rowsOf(pin, 1)).toEqual(expected);
              } finally {
                pin.release();
              }
              const reopened = await h.reopen(),
                again = reopened.pin({});
              try {
                expect(await rowsOf(again, 1)).toEqual(expected);
              } finally {
                again.release();
              }
              reach.reach("exact committed and reopened rows");
            } finally {
              await close();
            }
            expect(h.governor.usedBytes).toBe(0);
            reach.reach("zero ownership");
          }),
          { seed, numRuns: 2, examples: [[0]] },
        );
        reach.assertReached();
      },
      process.env.CI ? 120_000 : 40_000,
    );

/** An importer's grant and the native target may belong to separate governors. */
for (const seed of seedsFor("next-writer-foreign-owner", [0xb943, 0xb944]))
  it(
    `keeps foreign creator credit separate seed ${String(seed)}`,
    async (context) => {
      const stop = new AbortController();
      let drain: (() => Promise<void>) | undefined;
      context.onTestFinished(async () => {
        stop.abort();
        await drain?.();
      });
      const reach = createReach("foreign writer funding", WRITER_FOREIGN_OWNER_REACH);
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 65, max: 129 }), async (count) => {
          reach.beginHistory();
          stop.signal.throwIfAborted();
          const owner = await openOwner(),
            foreign = new Governor(16 << 20),
            grant = await foreign.admit({
              label: "separate import phase",
              minimumBytes: 128 << 10,
              spillable: false,
            });
          const first = owner.space.beginWrite("target bank owner", { ownershipBudget: grant });
          let second: ReturnType<typeof owner.space.beginWrite> | undefined,
            snapshot: Awaited<ReturnType<NonNullable<typeof first.snapshotExtents>>> | undefined,
            closing: Promise<void> | undefined;
          const close = () =>
            (closing ??= (async () => {
              try {
                if (second?.state === "open") await second.abort();
              } finally {
                try {
                  if (first.state === "open") await first.abort();
                } finally {
                  await snapshot?.release();
                  grant.close();
                  await owner.close();
                }
              }
            })());
          drain = close;
          try {
            // The first owner fills the target bank; a second must use admitted target credit.
            second = owner.space.beginWrite("target structural owner", { ownershipBudget: grant });
            reach.reach("two live owners beyond bank");
            for (let i = 0; i < count; i++) await second.write([Uint8Array.of(i & 255)]);
            if (!second.snapshotExtents) throw new Error("Missing native inventory");
            snapshot = await second.snapshotExtents({ budget: grant, signal: stop.signal });
            expect(snapshot.count).toBe(count);
            expect(grant.allocated).toBe(0);
            grant.close();
            expect(foreign.usedBytes).toBe(0);
            reach.reach("creator grant closes before target");
            const cursor = snapshot.cursor({ budget: owner.startup, signal: stop.signal });
            try {
              let read = 0;
              while (await cursor.next()) {
                expect(cursor.length).toBe(1);
                read++;
              }
              expect(read).toBe(count);
            } finally {
              await cursor.close();
            }
            reach.reach("snapshot survives creator");
            await second.abort();
            await first.abort();
          } finally {
            await close();
          }
          expect(foreign.usedBytes).toBe(0);
          reach.reach("zero both governors");
        }),
        { seed, numRuns: 4, examples: [[65], [129]] },
      );
      reach.assertReached();
    },
    process.env.CI ? 120000 : 40000,
  );
