/** Portable scope excludes FTS definitions; replacement retires native FTS with the old
 * generation under R25's complete inventory and R8's two-root reclamation contract. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_FTS_RETIREMENT_REACH } from "../../testing/reach.js";
import { walkFtsJournal } from "../format/fts-journal.js";
import { decodeManifestRoot, encodeManifestRoot } from "../format/manifest-codec.js";
import { Governor } from "../memory/governor.js";
import { DATA_ALIGN, type DataRange } from "../storage/contract.js";
import { sortedListedExtents } from "../storage/extent-list.js";
import type { BlobRef } from "../storage/extents.js";
import { exportSnapshot } from "../write/snapshot-capture.js";
import { loadSnapshot } from "../write/snapshot-load.js";
import { snapshotBlob } from "../write/snapshot-source.js";
import { snapshotTestTarget } from "../write/snapshot-test-support.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
  texts,
} from "../write/test-support.js";
import { setupFts } from "./fts-transaction.test-support.js";

async function archive(salt: number): Promise<Blob> {
  const source = new Harness("strict", "memory"),
    path = await source.open(),
    grant = await source.governor.admit({
      label: "FTS replacement export",
      minimumBytes: 8 << 20,
      spillable: false,
    });
  try {
    await autocommit(path, (s) =>
      s.changeCatalog({ kind: "createTable", table: tableInput("loaded", [column("n", INT)]) }),
    );
    await autocommit(path, (s) => s.table(1).insert(batch(ints([salt])), null));
    await path.recycleLog();
    const parts: Array<Uint8Array<ArrayBuffer>> = [];
    await exportSnapshot(
      path,
      new Uint8Array(16).fill(salt),
      { write: (bytes) => void parts.push(bytes.slice()) },
      grant,
    );
    return new Blob(parts);
  } finally {
    grant.close();
    await source.close();
    expect(source.governor.usedBytes).toBe(0);
  }
}
for (const seed of seedsFor("next-snapshot-fts-retirement", [0xb2bb, 0xb2bc]))
  it(`replaces native FTS pages with canonical empty state and exact retirement seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot FTS retirement", SNAPSHOT_FTS_RETIREMENT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 3 }),
        fc.integer({ min: 1, max: 5 }),
        fc.integer({ min: 1, max: 31 }),
        async (many, rows, salt) => {
          reach.beginHistory();
          for (const rounds of [1, many]) {
            const h = new Harness(),
              releases: DataRange[] = [],
              source = await archive(salt);
            let closed = false;
            try {
              const path = await setupFts(h);
              for (let turn = 0; turn < rounds; turn++) {
                await autocommit(path, (s) =>
                  s
                    .table(1)
                    .insert(
                      batch(
                        texts(
                          Array.from(
                            { length: rows },
                            (_, i) => `alpha beta ${String(turn)} ${String(i)}`,
                          ),
                        ),
                      ),
                      null,
                    ),
                );
                await path.recycleLog();
              }
              // A dropped declaration leaves immutable history/payload until its owning
              // generation retires. The incoming portable catalog has no FTS definition.
              await autocommit(path, (s) => s.changeCatalog({ kind: "dropFts", ftsId: 1 }));
              await path.recycleLog();
              const lsn = path.appliedLsn;
              await h.close();
              closed = true;
              expect(h.governor.usedBytes).toBe(0);
              const open = h.backend.open.bind(h.backend);
              h.backend.open = async (...args) => {
                const session = await open(...args),
                  release = session.releaseData.bind(session);
                session.releaseData = async (ranges) => {
                  releases.push(...ranges.map((range) => ({ ...range })));
                  await release(ranges);
                };
                return session;
              };
              const probe = await h.governor.admit({
                  label: "FTS retirement proof",
                  minimumBytes: 8 << 20,
                  spillable: false,
                }),
                loader = new Governor(64 << 20),
                refs: BlobRef[] = [],
                extents = new Map<number, number>();
              const aligned = (length: number) => Math.ceil(length / DATA_ALIGN) * DATA_ALIGN;
              const freed = (offset: number, length: number) =>
                releases.reduce(
                  (sum, range) =>
                    sum +
                    Math.max(
                      0,
                      Math.min(offset + length, range.offset + range.length) -
                        Math.max(offset, range.offset),
                    ),
                  0,
                );
              try {
                await loadSnapshot(
                  snapshotBlob(source),
                  async () => {
                    const target = await snapshotTestTarget(h, lsn, false);
                    try {
                      const space = target.space;
                      const before = space.root;
                      if (before === null) throw new Error("Missing FTS target root");
                      const old = decodeManifestRoot(before, probe);
                      try {
                        expect(old.value.fts.checkpoint).not.toBeNull();
                        for (const [ref, checkpoint] of [
                          [old.value.fts.checkpoint, true],
                          [old.value.fts.staged, false],
                        ] as const) {
                          if (ref === null) continue;
                          for await (const event of walkFtsJournal(ref, checkpoint, probe, space)) {
                            refs.push(event.ref);
                            extents.set(event.ref.extent.offset, event.ref.extent.length);
                          }
                        }
                        expect(refs.length).toBeGreaterThan(2);
                        expect(extents.size).toBeGreaterThan(1);
                        const occurrences = new Map<number, number>();
                        for await (const extent of sortedListedExtents(
                          old.value.extentList,
                          space,
                          probe,
                        )) {
                          if (!extents.has(extent.offset)) continue;
                          expect(extent.length).toBe(extents.get(extent.offset));
                          occurrences.set(extent.offset, (occurrences.get(extent.offset) ?? 0) + 1);
                        }
                        for (const offset of extents.keys())
                          expect(occurrences.get(offset)).toBe(1);
                        reach.reach("old FTS inventory exactly once");
                      } finally {
                        probe.free(old.charge);
                      }
                      let published = false;
                      const publish = space.publish.bind(space),
                        close = target.close.bind(target);
                      space.publish = async (root, change) => {
                        expect(change.retireCurrentRoot).toBe(true);
                        expect(change.retired).toEqual([]);
                        const generation = await publish(root, change);
                        published = true;
                        return generation;
                      };
                      return {
                        ...target,
                        async close() {
                          try {
                            if (!published) return;
                            const bytes = space.root;
                            if (bytes === null) throw new Error("Missing imported root");
                            const loaded = decodeManifestRoot(bytes, probe);
                            try {
                              expect(loaded.value.fts).toEqual({ checkpoint: null, staged: null });
                              reach.reach("canonical empty FTS state");
                              for (const [offset, length] of extents)
                                expect(freed(offset, aligned(length))).toBe(0);
                              for (let at = 0; at < refs.length; at += 16) {
                                const batch = refs.slice(at, at + 16),
                                  lease = await space.read(batch, { budget: probe, retain: false });
                                try {
                                  expect(lease.bytes.map((bytes) => bytes.length)).toEqual(
                                    batch.map((ref) => ref.length),
                                  );
                                } finally {
                                  lease.release();
                                }
                              }
                              reach.reach("previous root FTS remains readable");
                              await space.audit();
                              // Advance the other root slot without allocating ANY new data. This
                              // isolates one retirement from legitimate later reuse of those addresses.
                              await publish(
                                encodeManifestRoot({
                                  ...loaded.value,
                                  sequence: loaded.value.sequence + 1,
                                }),
                                {
                                  writers: [],
                                  retired: [],
                                  list: loaded.value.extentList,
                                  budget: probe,
                                },
                              );
                              await space.audit();
                            } finally {
                              probe.free(loaded.charge);
                            }
                          } finally {
                            await close();
                          }
                        },
                      };
                    } catch (error) {
                      await target.close();
                      throw error;
                    }
                  },
                  loader,
                  { replace: true },
                );
                for (const [offset, length] of extents)
                  expect(freed(offset, aligned(length))).toBe(aligned(length));
                reach.reach("old FTS physical retirement exactly once");
                expect(loader.usedBytes).toBe(0);
              } finally {
                probe.close();
                h.backend.open = open;
              }
              expect(h.governor.usedBytes).toBe(0);
              const loaded = await h.open();
              closed = false;
              expect(loaded.state().root.fts).toEqual({ checkpoint: null, staged: null });
              let pin = loaded.pin();
              try {
                expect(await rowsOf(pin, 1)).toEqual([[salt]]);
                expect(pin.catalog.fts).toHaveLength(0);
              } finally {
                pin.release();
              }
              await autocommit(loaded, (s) => s.table(1).insert(batch(ints([salt + 1000])), null));
              await loaded.recycleLog();
              await h.reopen();
              pin = h.path.pin();
              try {
                expect(await rowsOf(pin, 1)).toEqual([[salt], [salt + 1000]]);
              } finally {
                pin.release();
              }
              await h.space.audit();
              reach.reach("ordinary reopen and continued write");
              reach.reach(rounds === 1 ? "one checkpoint" : "several checkpoints");
            } finally {
              if (!closed) await h.close();
            }
            expect(h.governor.usedBytes).toBe(0);
            reach.reach("zero charges");
          }
        },
      ),
      { seed, numRuns: 2, endOnFailure: true },
    );
    reach.assertReached();
  }, 120000);
