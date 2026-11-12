/** Governor rules1/4/7: completed merge pieces have funded publication ownership. */
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type { BlobRef, RunRef } from "../format/manifest.js";
import { Governor, OBJECT_OVERHEAD_BYTES, type Grant } from "../memory/governor.js";
import type { ExtentWriter } from "../storage/extents.js";
import { xxHash32 } from "../hash/xxhash32.js";
import {
  createReach,
  WRITE_MERGE_SUMMARY_STATES,
  WRITE_MERGE_SUMMARY_PRESSURE_STATES,
  WRITE_MERGE_KEY_LIMIT_STATES,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import * as merging from "./merge.js";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";
import { ByteReader } from "./bytes.js";
import { MetadataBudget } from "./budget.js";
import { DEFAULT_MERGE_POLICY, MERGE_FIXED_BYTES, MERGE_INPUT_BYTES, mergeRuns } from "./merge.js";
import { KeyDelta, type DeltaVisitor } from "./key-hash.js";
import { RunBuilder, RUN_BUFFER_OVERHEAD, type RunReader } from "./runs.js";

const reach = createReach("retained merge output", WRITE_MERGE_SUMMARY_STATES);
const MiB = 1024 * 1024;
const STAGE = 256 * 1024;
// RunRef, three BlobRefs, common ExtentRef and the retained array membership.
const DESCRIPTORS = 6 * OBJECT_OVERHEAD_BYTES;

async function fixture(cap: number, width: number, fan: number, rows: number) {
  const governor = new Governor(cap);
  const grant = await governor.admit({
    label: "merge working",
    minimumBytes: MERGE_FIXED_BYTES + fan * MERGE_INPUT_BYTES,
    spillable: false,
  });
  const publication = await governor.admit({
    label: "merge output owner",
    minimumBytes: 0,
    spillable: true,
  });
  const inputs = await governor.admit({
    label: "input fixture metadata",
    minimumBytes: 0,
    spillable: true,
  });
  expect(inputs.allocate(rows * 128 + fan * 192 + width + 128)).toBe(true);
  const lengthBytes = width < 128 ? 1 : 2;
  const template = new Uint8Array(4 + lengthBytes + width + 7),
    v = new DataView(template.buffer);
  v.setUint16(0, 1, true);
  v.setUint16(2, 4, true);
  template[4] = width < 128 ? width : (width & 127) | 128;
  if (lengthBytes === 2) template[5] = width >>> 7;
  const key = template.subarray(4 + lengthBytes, 4 + lengthBytes + width).fill(129);
  const ids = new WeakMap<BlobRef, number>();
  const readers = Array.from({ length: fan }, (_, f) => {
    const refs: BlobRef[] = [];
    for (let id = f + 1; id <= rows; id += fan) {
      const ref = {
        extent: { offset: 0, length: template.length * rows },
        offset: (id - 1) * template.length,
        length: template.length,
        checksum: 0,
      };
      ids.set(ref, id);
      refs.push(ref);
    }
    const ref = {
      level: 0,
      entryCount: refs.length,
      entries: refs[0],
      minKey: key,
      maxKey: key,
    } as RunRef;
    return { ref, blockRefs: () => refs } as unknown as RunReader;
  });
  let activeInputs = 0,
    staged = 0,
    seals = 0,
    injected: "none" | "descriptor" = "none";
  const space = {
    read: async (refs: readonly BlobRef[]) => {
      const size = refs.length * template.length;
      expect(grant.allocate(size), "input leases fit the working credit").toBe(true);
      activeInputs += size;
      const bytes = refs.map((ref) => {
        const b = template.slice();
        new DataView(b.buffer).setUint32(4 + lengthBytes + width, ids.get(ref) ?? 0, true);
        return b;
      });
      let live = true;
      return {
        bytes,
        release() {
          if (!live) return;
          live = false;
          for (const b of bytes) b.fill(238);
          activeInputs -= size;
          grant.free(size);
        },
      };
    },
  } as unknown as SpaceReader;
  const closes = new Set<() => void>();
  const abort = () => {
    for (const close of closes) close();
    closes.clear();
  };
  const writer = {
    open(capacity: number) {
      expect(grant.allocate(STAGE)).toBe(true);
      staged += STAGE;
      let stage: Uint8Array | null = new Uint8Array(STAGE),
        cursor = 0;
      const blobs: BlobRef[] = [],
        tail: Array<{ offset: number; length: number }> = [];
      const extent = { offset: seals * MiB, length: 0 };
      const close = () => {
        if (stage === null) return;
        stage = null;
        closes.delete(close);
        tail.length = 0;
        staged -= STAGE;
        grant.free(STAGE);
      };
      closes.add(close);
      return {
        capacity,
        length: 0,
        extend(n: number) {
          this.capacity += n;
          return true;
        },
        async append(parts: readonly Uint8Array[]) {
          if (stage === null) throw new Error("sealed fixture");
          const pending = [];
          for (const p of parts) {
            expect(p.length).toBeLessThanOrEqual(64 * 1024);
            const slot = (cursor++ % 3) * 64 * 1024;
            stage.set(p, slot);
            tail.push({ offset: slot, length: p.length });
            if (tail.length > 3) tail.shift();
            const ref = { extent, offset: this.length, length: p.length, checksum: xxHash32(p) };
            blobs.push(ref);
            pending.push(ref);
            this.length += p.length;
          }
          return pending;
        },
        async seal() {
          if (stage === null) throw new Error("sealed fixture");
          const parts = tail.map(
            (p) =>
              new ByteReader((stage ?? new Uint8Array()).subarray(p.offset, p.offset + p.length)),
          );
          const index = parts[0],
            fences = parts[1],
            directory = parts[2];
          if (!index || !fences || !directory) throw new Error("missing tails");
          const blocks = index.varuint();
          expect(fences.varuint()).toBe(blocks);
          for (let b = 0; b < blocks; b++) {
            expect(fences.varuint()).toBe(width);
            expect(fences.raw(width).every((v) => v === 129)).toBe(true);
          }
          fences.end();
          expect(directory.varuint()).toBe(256);
          expect(directory.varuint()).toBe(Math.ceil(blocks / 256));
          extent.length = this.length;
          seals++;
          close();
          if (injected === "descriptor" && seals === 3)
            return {
              extent,
              blobs: new Proxy(blobs, {
                get(t, p, r) {
                  if (p === String(t.length - 3)) {
                    expect(
                      publication.allocated,
                      "descriptor allocation is funded first",
                    ).toBeGreaterThan(0);
                    throw new Error("descriptor failure after output charge");
                  }
                  return Reflect.get(t, p, r) as unknown;
                },
              }),
            };
          return { extent, blobs };
        },
      };
    },
  } as unknown as ExtentWriter;
  return {
    governor,
    grant,
    publication,
    inputs,
    space,
    writer,
    readers,
    key,
    abort,
    get activeInputs() {
      return activeInputs;
    },
    get staged() {
      return staged;
    },
    get seals() {
      return seals;
    },
    set injected(value: typeof injected) {
      injected = value;
    },
    cleanup() {
      abort();
      expect(staged).toBe(0);
      expect(activeInputs).toBe(0);
      grant.close();
      publication.close();
      inputs.close();
      expect(governor.usedBytes).toBe(0);
    },
  };
}

for (const seed of seedsFor("next-write-merge-retained-summaries", [0xb5f2]))
  it(`funds hundreds or thousands of accumulated pieces (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(256, 512, 1024, 2048),
        fc.constantFrom(129, 1024, 2048),
        fc.oneof(fc.constantFrom(1, 32), fc.integer({ min: 1, max: 32 })),
        fc.constantFrom(1, 3),
        fc.boolean(),
        async (pieces, width, fan, splitEntries, fault) => {
          reach.beginHistory();
          const cap = (pieces === 2048 ? 24 : 16) * MiB;
          // The split count excludes the one lookahead entry held for put/tombstone cancellation.
          const rows = pieces * (splitEntries + 1);
          const f = await fixture(cap, width, fan, rows);
          reach.reach(fan === 1 ? "fan one" : fan === 32 ? "fan thirty two" : "intermediate fan");
          reach.reach(pieces >= 1024 ? "thousands of pieces" : "hundreds of pieces");
          if (width === 2048) reach.reach("exact two KiB keys");
          if (cap === 16 * MiB) reach.reach("small cap");
          const outputBudget: ByteBudget = {
            allocate(n) {
              expect(f.grant.allocated, "prior owner still funds keys before handoff").toBe(
                f.activeInputs + 2 * (width + RUN_BUFFER_OVERHEAD),
              );
              reach.reach("fund before refund");
              return f.publication.allocate(n);
            },
            free(n) {
              f.publication.free(n);
            },
          };
          const work = async () => {
            try {
              return await mergeRuns({
                space: f.space,
                writer: f.writer,
                grant: f.grant,
                readers: f.readers,
                outputBudget,
                policy: { ...DEFAULT_MERGE_POLICY, splitEntries },
                level: 1,
                bottom: true,
                stopped: () => false,
              });
            } catch (error) {
              f.abort();
              throw error;
            }
          };
          try {
            if (fault) {
              f.injected = "descriptor";
              await expect(work()).rejects.toThrow("descriptor failure after output charge");
              expect(f.grant.allocated).toBe(0);
              expect(f.publication.allocated).toBe(0);
              reach.reach("failure after funding");
              f.injected = "none";
              reach.reach("retry after failure");
            }
            const result = await work();
            expect(result.entriesOut).toBe(rows);
            expect(result.runs).toHaveLength(pieces);
            expect(f.grant.allocated, "bounded working account is empty after merge").toBe(0);
            expect(f.publication.allocated, "all still-live keys and descriptors stay funded").toBe(
              pieces * (2 * (width + RUN_BUFFER_OVERHEAD) + DESCRIPTORS),
            );
            for (const run of result.runs) {
              expect(run.minKey.every((v) => v === 129)).toBe(true);
              expect(run.maxKey.every((v) => v === 129)).toBe(true);
              expect(run.minKey.length).toBe(width);
              expect(run.maxKey.length).toBe(width);
            }
            reach.reach("retained output owned");
            result.release();
            result.release();
            expect(f.publication.allocated).toBe(0);
            reach.reach("output consumed");
            expect(f.governor.peakBytes).toBeLessThanOrEqual(cap);
          } finally {
            f.cleanup();
          }
        },
      ),
      { seed, numRuns: 30 },
    );
  }, 300_000);

const pressureReach = createReach(
  "merge publication pressure",
  WRITE_MERGE_SUMMARY_PRESSURE_STATES,
);
for (const seed of seedsFor("next-write-merge-output-pressure", [0xb5f3]))
  it(`unwinds before cancellable metadata waiting (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.constantFrom(1, 8, 32), async (cancel, fan) => {
        pressureReach.beginHistory();
        const f = await fixture(16 * MiB, 2048, fan, 512);
        const stop = new AbortController(),
          stopped = new Error("cancel merge metadata wait");
        const held: { grant: Grant | null; bytes: Uint8Array | null } = {
          grant: null,
          bytes: null,
        };
        let attempts = 0,
          pressured = false;
        let waiting!: () => void;
        const beganWaiting = new Promise<void>((resolve) => {
          waiting = resolve;
        });
        const nativeAllocate = f.publication.allocate.bind(f.publication);
        f.publication.allocate = (n) => {
          if (!pressured && f.seals === 3) {
            pressured = true;
            held.grant = f.governor.tryAdmit({
              label: "other pinned holder",
              minimumBytes: 0,
              spillable: true,
            });
            if (!held.grant) throw new Error("missing holder");
            const size = f.governor.capBytes - f.governor.reserveBytes - f.governor.usedBytes;
            expect(held.grant.allocate(size)).toBe(true);
            held.bytes = new Uint8Array(size).fill(7);
            pressureReach.reach("other pinned bytes retained");
          }
          return nativeAllocate(n);
        };
        const budget = new MetadataBudget(
          f.publication,
          () => 0,
          f.governor.capBytes,
          async (wanted) => {
            expect(f.grant.allocated, "no input or encoder buffers while waiting").toBe(0);
            expect(f.publication.allocated, "abandoned output returned before waiting").toBe(0);
            expect(f.staged).toBe(0);
            pressureReach.reach("unwound before wait");
            const credit = f.governor.admit({
              label: "publication growth",
              minimumBytes: Math.max(0, wanted - f.publication.held),
              spillable: false,
              signal: stop.signal,
            });
            waiting();
            const g = await credit;
            try {
              expect(f.governor.transferCredit(g, f.publication, g.held)).toBe(true);
            } finally {
              g.close();
            }
          },
        );
        const check = () => stop.signal.throwIfAborted();
        const result = Promise.resolve(
          budget.retry(async () => {
            attempts++;
            try {
              return await mergeRuns({
                space: f.space,
                writer: f.writer,
                grant: f.grant,
                readers: f.readers,
                outputBudget: budget,
                level: 1,
                bottom: true,
                policy: { ...DEFAULT_MERGE_POLICY, splitEntries: 1 },
                stopped: () => stop.signal.aborted,
              });
            } catch (error) {
              f.abort();
              throw error;
            }
          }, check),
        );
        // Attach rejection before driving cancellation, so no unhandled rejection can race it.
        const settled = result.then(
          (value) => ({ value, error: null }),
          (error: unknown) => ({ value: null, error }),
        );
        try {
          await beganWaiting;
          expect(held.bytes?.[0]).toBe(7);
          expect(held.bytes?.at(-1)).toBe(7);
          expect(attempts).toBe(1);
          pressureReach.reach("waiting at barrier");
          if (cancel) {
            stop.abort(stopped);
            const done = await settled;
            expect(done.error).toBe(stopped);
            pressureReach.reach("cancelled without retry");
          } else {
            held.grant?.close();
            held.grant = null;
            held.bytes = null;
            const done = await settled;
            if (done.error !== null)
              throw done.error instanceof Error
                ? done.error
                : new Error("metadata retry failed", { cause: done.error });
            if (!done.value) throw new Error("missing output");
            expect(attempts).toBe(2);
            expect(done.value.entriesOut).toBe(512);
            expect(done.value.runs).toHaveLength(256);
            expect(f.publication.allocated).toBeGreaterThan(0);
            done.value.release();
            pressureReach.reach("resumed same split policy");
          }
          expect(f.grant.allocated).toBe(0);
          expect(f.publication.allocated).toBe(0);
        } finally {
          stop.abort(stopped);
          await settled;
          held.grant?.close();
          held.bytes = null;
          f.cleanup();
        }
      }),
      { seed, numRuns: 12 },
    );
    pressureReach.assertReached();
  }, 120_000);

const limitReach = createReach("encoded index key limit", WRITE_MERGE_KEY_LIMIT_STATES);
for (const seed of seedsFor("next-write-encoded-key-limit", [0xb5f4]))
  it(`refuses oversized encoded keys as documented (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(fc.constant(4096), fc.integer({ min: 2049, max: 4096 })),
        fc.boolean(),
        async (width, bulk) => {
          limitReach.beginHistory();
          let paid = 0;
          const budget = {
            allocate(n: number) {
              paid += n;
              return true;
            },
            free(n: number) {
              paid -= n;
              expect(paid).toBeGreaterThanOrEqual(0);
            },
          };
          const delta = new KeyDelta(budget),
            builder = new RunBuilder(false, bulk, budget),
            key = new Uint8Array(width).fill(129);
          delta.add(key, 0, width, 1, 1, false);
          let refused = false;
          try {
            try {
              if (bulk) {
                const visit: DeltaVisitor = () => {
                  throw new Error("expected bulk slot");
                };
                visit.singles = (data, from, to, bound, keep) =>
                  builder.addSingleBatch(data, from, to, bound, keep);
                await delta.forEachSorted(1, visit);
              } else builder.addRange(key, 0, width, 1, false);
              builder.finish();
            } catch (error) {
              if (!(error instanceof RangeError)) throw error;
              expect(error.message).toBe("An index key is longer than 2 KiB");
              refused = true;
            }
            expect(refused, "oversized input cannot produce a run").toBe(true);
            limitReach.reach(bulk ? "bulk typed refusal" : "ordinary typed refusal");
            if (width === 4096) limitReach.reach("four KiB refusal");
          } finally {
            builder.close();
            delta.releaseCharge();
          }
          expect(paid).toBe(0);
          limitReach.reach("refusal cleanup");
        },
      ),
      { seed, numRuns: 30 },
    );
    limitReach.assertReached();
  });

afterAll(() => reach.assertReached());

it("the production merge uses the publication account and releases it at root adoption", async () => {
  const h = new Harness("strict", "memory", 8, undefined, {
    merges: { fanIn: 2, stallRuns: 4, splitEntries: 1 },
  });
  const original = merging.mergeRuns;
  let observed = 0,
    wrongOwner = false,
    retainedWorkingBytes = 0;
  const spy = vi.spyOn(merging, "mergeRuns").mockImplementation(async (input) => {
    wrongOwner ||=
      input.outputBudget === input.grant || !(input.outputBudget instanceof MetadataBudget);
    observed++;
    const out = await original(input);
    retainedWorkingBytes += input.grant.allocated;
    return out;
  });
  try {
    const path = await h.open();
    let t = 0;
    await autocommit(path, async (statement) => {
      t =
        (
          await statement.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    for (let from = 0; from < 64; from += 8)
      await autocommit(path, (statement) =>
        statement.table(t).insert(batch(ints(Array.from({ length: 8 }, (_, i) => from + i))), null),
      );
    await path.quiesce();
    expect(wrongOwner, "production pieces require publication metadata ownership").toBe(false);
    expect(observed).toBeGreaterThan(0);
    expect(retainedWorkingBytes, "production working buffers are returned before publication").toBe(
      0,
    );
    expect(path.maintenance.mergeFailures).toBe(0);
    expect(path.statistics(t)?.rowCount).toBe(64);
  } finally {
    spy.mockRestore();
    await h.close();
  }
});
