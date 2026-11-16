/** R4/pg_dump: a strict pin's sequence definition retains its emitted horizon until DROP is durable. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_SEQUENCE_DROP_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { exportSnapshot } from "./snapshot-capture.js";
import { loadSnapshot } from "./snapshot-load.js";
import { snapshotBlob } from "./snapshot-source.js";
import { snapshotTestTarget } from "./snapshot-test-support.js";
import { Harness, autocommit, rules } from "./test-support.js";

for (const seed of seedsFor("next-snapshot-sequence-drop", [0xb299, 0xb29a]))
  it(`preserves emitted values across a nondurable sequence DROP seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot sequence DROP", SNAPSHOT_SEQUENCE_DROP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 6 }),
        fc.integer({ min: 1, max: 20 }),
        fc.integer({ min: 1, max: 3 }),
        async (many, salt, step) => {
          reach.beginHistory();
          for (const direction of [1n, -1n])
            for (const count of [1, many]) {
              const source = new Harness("strict", "memory"),
                path = await source.open(),
                increment = BigInt(step) * direction;
              try {
                await autocommit(path, (statement) =>
                  statement.changeCatalog({
                    kind: "createSequence",
                    sequence: {
                      name: "s",
                      start: BigInt(salt) * direction,
                      increment,
                      min: -100000n,
                      max: 100000n,
                      cycle: false,
                      ownedBy: null,
                    },
                  }),
                );
                const emitted = { repr: "int64" as const, values: new BigInt64Array(count) };
                await path.sequences.next(1, count, emitted, 0);
                await path.flush();
                const last = emitted.values[count - 1];
                if (last === undefined) throw new Error("Missing emitted sequence fixture");
                const grant = await source.governor.admit({
                    label: "pending DROP export",
                    minimumBytes: 8 << 20,
                    spillable: false,
                  }),
                  drop = vi.spyOn(path.sequences, "drop");
                try {
                  const tx = await path.begin(),
                    statement = tx.statement({ rules });
                  await statement.changeCatalog({ kind: "dropSequence", sequenceId: 1 });
                  await statement.end();
                  const receipt = await tx.commit(null),
                    earlyDrops = drop.mock.calls.length,
                    parts: Uint8Array[] = [];
                  try {
                    expect(path.appliedLsn).toBeGreaterThan(path.durableLsn);
                    let pinned = false;
                    const exporting = exportSnapshot(
                      {
                        sequences: path.sequences,
                        pin() {
                          const pin = path.pin();
                          expect(pin.observedLsn).toBeLessThan(receipt.lsn);
                          expect(pin.catalog.sequences.map((def) => def.id)).toEqual([1]);
                          pinned = true;
                          return pin;
                        },
                      },
                      new Uint8Array(16),
                      { write: (bytes) => void parts.push(bytes.slice()) },
                      grant,
                    );
                    expect(pinned).toBe(true);
                    reach.reach("pending DROP in strict pin");
                    reach.reach("capture before await");
                    await exporting;
                    const governor = new Governor(64 << 20),
                      target = new Harness("strict", "memory");
                    Object.assign(target, { governor });
                    const startup = await governor.admit({
                      label: "sequence DROP load owner",
                      minimumBytes: 8 << 20,
                      spillable: true,
                    });
                    try {
                      await loadSnapshot(
                        snapshotBlob(new Blob(parts.map((part) => part.slice().buffer))),
                        () => snapshotTestTarget(target, 0, true, undefined, startup),
                        governor,
                      );
                    } finally {
                      startup.close();
                    }
                    try {
                      const loaded = await target.open(),
                        next = { repr: "int64" as const, values: new BigInt64Array(1) };
                      await loaded.sequences.next(1, 1, next, 0);
                      expect(direction * (next.values[0] ?? 0n)).toBeGreaterThan(direction * last);
                      reach.reach("ordinary load advances past emitted");
                      const used = next.values[0] ?? 0n,
                        reopened = await target.reopen();
                      await reopened.sequences.next(1, 1, next, 0);
                      expect(direction * (next.values[0] ?? 0n)).toBeGreaterThan(direction * used);
                      reach.reach("ordinary reopen advances");
                    } finally {
                      await target.close();
                    }
                    expect(governor.usedBytes).toBe(0);
                    expect(earlyDrops).toBe(0);
                    await receipt.durable;
                    expect(drop).toHaveBeenCalledExactlyOnceWith(1);
                    const after = path.pin();
                    try {
                      expect(after.catalog.sequences).toHaveLength(0);
                    } finally {
                      after.release();
                    }
                    reach.reach("reclaimed after durability");
                    reach.reach(direction > 0n ? "ascending" : "descending");
                    reach.reach(count === 1 ? "one emission" : "many emissions");
                  } finally {
                    await receipt.durable;
                  }
                  expect(grant.allocated).toBe(0);
                } finally {
                  drop.mockRestore();
                  grant.close();
                }
              } finally {
                await source.close();
              }
              expect(source.governor.usedBytes).toBe(0);
              reach.reach("zero charges");
            }
        },
      ),
      { seed, numRuns: 4, endOnFailure: true },
    );
    reach.assertReached();
  }, 60000);
