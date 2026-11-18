/** Portable scope excludes FTS definitions; replacement retires native FTS with the old
 * generation under R25's complete inventory and R8's two-root reclamation contract. */
import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { closedDiskFixture } from "./closed-disk-fixture.test-support.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  SNAPSHOT_FTS_RETIREMENT_REACH,
  SNAPSHOT_FTS_CUT_REACH,
} from "../../testing/reach.js";
import { DURABLE_FORMATS } from "../durable-format.js";
import { SimDisk } from "../storage/sim-disk.js";
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

/** Full physical-publication sweep with real native history; no synthetic FTS references. */
for (const seed of seedsFor("next-snapshot-fts-cuts", [0xb2bf, 0xb2c0]))
  describe(`snapshot FTS publication seed ${String(seed)}`, () => {
    const reach = createReach("snapshot FTS publication cuts", SNAPSHOT_FTS_CUT_REACH);
    afterAll(() => {
      reach.assertReached();
    });
    for (const staged of [false, true])
      it(`recovers and retries every FTS replacement publication cut seed ${String(seed)} ${staged ? "checkpoint and staged" : "checkpoint"}`, async () => {
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 1, max: 3 }),
            fc.integer({ min: 1, max: 31 }),
            async (rows, salt) => {
              reach.beginHistory();
              const source = await archive(salt);
              reach.reach(staged ? "checkpoint and staged target" : "checkpoint target");
              let preparations = 0;
              // Build the immutable target once per generated staged configuration. Every
              // cut forks ALL images and pending/queued writes with its own original RNG seed.
              const prepare = async () => {
                const disk = new SimDisk({ seed: salt * 131 }),
                  h = new Harness("strict", "sim", undefined, disk);
                let closed = false;
                try {
                  const path = await setupFts(h);
                  preparations++;
                  expect(preparations, "one native preparation per configuration").toBe(1);
                  const values = Array.from(
                    { length: rows },
                    (_, i) => `alpha beta ${String(salt)} ${String(i)}`,
                  );
                  await autocommit(path, (s) => s.table(1).insert(batch(texts(values)), null));
                  await path.recycleLog();
                  if (staged) {
                    const tail = `gamma delta ${String(salt)}`;
                    values.push(tail);
                    await autocommit(path, (s) => s.table(1).insert(batch(texts([tail])), null));
                  }
                  if (staged) expect(path.appliedLsn).toBeGreaterThan(path.state().root.globalLsn);
                  const oldFts = structuredClone(path.state().root.fts),
                    oldLsn = path.appliedLsn;
                  expect(oldFts.checkpoint).not.toBeNull();
                  expect(oldFts.staged === null).toBe(!staged);
                  const proof = await h.governor.admit({
                      label: "FTS cut payload proof",
                      minimumBytes: 8 << 20,
                      spillable: false,
                    }),
                    payloads: Array<{ ref: BlobRef; bytes: Uint8Array }> = [],
                    extents = new Map<number, number>();
                  try {
                    const payloadKinds = new Set<number>();
                    for (const [ref, checkpoint] of [
                      [oldFts.checkpoint, true],
                      [oldFts.staged, false],
                    ] as const) {
                      if (ref === null) continue;
                      for await (const event of walkFtsJournal(ref, checkpoint, proof, h.space)) {
                        const lease = await h.space.read([event.ref], {
                          budget: proof,
                          retain: false,
                        });
                        try {
                          const bytes = lease.bytes[0];
                          if (bytes === undefined) throw new Error("missing FTS payload");
                          payloads.push({ ref: event.ref, bytes: bytes.slice() });
                          payloadKinds.add(
                            new DataView(
                              bytes.buffer,
                              bytes.byteOffset,
                              bytes.byteLength,
                            ).getUint32(0, true),
                          );
                        } finally {
                          lease.release();
                        }
                        extents.set(event.ref.extent.offset, event.ref.extent.length);
                      }
                    }
                    for (const kind of [
                      DURABLE_FORMATS.ftsDocuments,
                      DURABLE_FORMATS.ftsTerms,
                      DURABLE_FORMATS.ftsPostings,
                    ])
                      expect(payloadKinds.has(kind.magic)).toBe(true);
                    reach.reach("document and term payloads");
                  } finally {
                    proof.close();
                  }

                  await h.close();
                  closed = true;
                  expect(h.governor.usedBytes).toBe(0);
                  return {
                    fork: closedDiskFixture(disk),
                    oldFts,
                    oldLsn,
                    values,
                    payloads,
                    extents,
                  };
                } finally {
                  if (!closed) await h.close();
                  expect(h.governor.usedBytes).toBe(0);
                }
              };
              const { fork, oldFts, oldLsn, values, payloads, extents } = await prepare();
              reach.reach("one native preparation per configuration");
              const run = async (cut: number | null): Promise<number> => {
                const options = {
                    seed: salt * 131 + (cut ?? 0),
                    crashAtStep: Number.MAX_SAFE_INTEGER,
                  },
                  disk = fork(options),
                  h = new Harness("strict", "sim", undefined, disk),
                  loader = new Governor(64 << 20),
                  releases: DataRange[] = [];
                let closed = true,
                  steps = 0;
                const open = h.backend.open.bind(h.backend);
                h.backend.open = async (...args) => {
                  const session = await open(...args),
                    release = session.releaseData.bind(session);
                  session.releaseData = async (ranges) => {
                    // Failed calls on crashed handles are attempts, not physical releases.
                    await release(ranges);
                    releases.push(...ranges.map((range) => ({ ...range })));
                  };
                  return session;
                };
                try {
                  const expectRetired = () => {
                    for (const [offset, length] of extents) {
                      const aligned = Math.ceil(length / DATA_ALIGN) * DATA_ALIGN;
                      const freed = releases.reduce(
                        (sum, range) =>
                          sum +
                          Math.max(
                            0,
                            Math.min(offset + aligned, range.offset + range.length) -
                              Math.max(offset, range.offset),
                          ),
                        0,
                      );
                      expect(freed, `cut=${String(cut)} FTSextent=${String(offset)}`).toBe(aligned);
                    }
                  };
                  const first = loadSnapshot(
                    snapshotBlob(source),
                    () =>
                      snapshotTestTarget(h, oldLsn, false, (space) => {
                        const publish = space.publish.bind(space);
                        space.publish = async (...args) => {
                          const start = disk.steps;
                          if (cut !== null) options.crashAtStep = start + cut;
                          const result = await publish(...args);
                          steps = disk.steps - start;
                          return result;
                        };
                      }),
                    loader,
                    { replace: true },
                  );
                  if (cut === null) await first;
                  else {
                    await expect(first).rejects.toThrow();
                    expect(disk.crashes).toBe(1);
                    reach.reach("one power loss per cut");
                  }
                  expect(loader.usedBytes).toBe(0);
                  expect(h.governor.usedBytes).toBe(0);
                  // The no-fault run measures the complete publication's physical steps.
                  // Successful FTS retirement already has its independent generator above.
                  if (cut === null) return steps;
                  const recovered = await h.open();
                  closed = false;
                  const pin = recovered.pin(),
                    check = await h.governor.admit({
                      label: "FTS recovered payload proof",
                      minimumBytes: 8 << 20,
                      spillable: false,
                    });
                  try {
                    const old = pin.catalog.tables[0]?.name === "docs";
                    expect(await rowsOf(pin, 1)).toEqual(
                      old ? values.map((value) => [value]) : [[salt]],
                    );
                    if (old) {
                      expect(recovered.state().root.fts).toEqual(oldFts);
                      for (const payload of payloads) {
                        const lease = await h.space.read([payload.ref], {
                          budget: check,
                          retain: false,
                        });
                        try {
                          expect(lease.bytes[0]).toEqual(payload.bytes);
                        } finally {
                          lease.release();
                        }
                      }
                      reach.reach("old target recovered");
                      reach.reach("old history and payload intact");
                    } else {
                      expect(recovered.state().root.fts).toEqual({
                        checkpoint: null,
                        staged: null,
                      });
                      reach.reach("complete replacement recovered");
                      reach.reach("canonical empty replacement");
                    }
                  } finally {
                    pin.release();
                    check.close();
                  }
                  await h.space.audit();
                  const retryLsn = recovered.appliedLsn;
                  await h.close();
                  closed = true;
                  await loadSnapshot(
                    snapshotBlob(source),
                    async () => {
                      const target = await snapshotTestTarget(h, retryLsn, false),
                        space = target.space,
                        close = target.close.bind(target),
                        publish = space.publish.bind(space);
                      let published = false;
                      space.publish = async (...args) => {
                        const generation = await publish(...args);
                        published = true;
                        return generation;
                      };
                      return {
                        ...target,
                        async close() {
                          try {
                            // Admission retries close an unpublished attempt too. Advancing
                            // that root would retire old pages before later allocations.
                            if (!published) return;
                            const bytes = space.root;
                            if (bytes === null) throw new Error("missing retry root");
                            const funding = await h.governor.admit({
                              label: "FTS cut drain",
                              minimumBytes: 8 << 20,
                              spillable: false,
                            });
                            try {
                              const root = decodeManifestRoot(bytes, funding);
                              try {
                                expect(root.value.fts).toEqual({ checkpoint: null, staged: null });
                                // No new data after retry: addresses cannot be legitimately reused
                                // while exact-once release coverage is being measured.
                                await publish(
                                  encodeManifestRoot({
                                    ...root.value,
                                    sequence: root.value.sequence + 1,
                                  }),
                                  {
                                    writers: [],
                                    retired: [],
                                    list: root.value.extentList,
                                    budget: funding,
                                  },
                                );
                                let auditFailure: Error | null = null;
                                try {
                                  await space.audit();
                                } catch (error) {
                                  auditFailure =
                                    error instanceof Error
                                      ? error
                                      : new Error("FTS retry audit failed", { cause: error });
                                }
                                // Audit drains the reclaimer before checking allocation coverage.
                                // Report FTS-specific loss first, even if the broad audit fails too.
                                expectRetired();
                                if (auditFailure !== null) throw auditFailure;
                              } finally {
                                funding.free(root.charge);
                              }
                            } finally {
                              funding.close();
                            }
                          } finally {
                            await close();
                          }
                        },
                      };
                    },
                    loader,
                    { replace: true },
                  );
                  reach.reach("retry after interruption");
                  expectRetired();
                  reach.reach("old FTS retirement exactly once");
                  expect(loader.usedBytes).toBe(0);
                  expect(h.governor.usedBytes).toBe(0);
                  const loaded = await h.open();
                  closed = false;
                  expect(loaded.state().root.fts).toEqual({ checkpoint: null, staged: null });
                  const final = loaded.pin();
                  try {
                    expect(await rowsOf(final, 1)).toEqual([[salt]]);
                  } finally {
                    final.release();
                  }
                  await h.space.audit();
                  reach.reach("ordinary reopen");
                  return steps;
                } finally {
                  try {
                    if (!closed) await h.close();
                    expect(h.governor.usedBytes).toBe(0);
                    expect(loader.usedBytes).toBe(0);
                  } finally {
                    fork.verify();
                    reach.reach("shared fixture fingerprint verified");
                  }
                }
              };
              const steps = await run(null);
              expect(steps).toBeGreaterThan(0);
              // Start after the root write/flush; still visit every step exactly once.
              for (let cut = steps - 1; cut >= 0; cut--) await run(cut);
              reach.reach("every physical publication step");
              reach.reach("zero charges");
            },
          ),
          { seed, numRuns: 2, endOnFailure: true },
        );
      }, 180000);
  });
