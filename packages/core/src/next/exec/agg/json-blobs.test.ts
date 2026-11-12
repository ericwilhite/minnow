/** Native blob contract: byte order, constant references and borrowed-IO lifetime. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, JSON_BLOB_REACH, JSON_BLOB_LIFETIME_REACH } from "../../../testing/reach.js";
import { seedsFor } from "../../../testing/seeds.js";
import { Governor } from "../../memory/governor.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { Charge, Pool, SpillIo } from "../spill-pool.js";
import { PageWriter, SpillFiles } from "../spill-pages.js";
import { Slice } from "../slices.js";
import { JsonBlobs } from "./json-blobs.js";
import { JsonStream } from "./json-stream.js";
import { yieldTask } from "../pipeline.js";

// Exact bytes, without Vitest boxing every element of a multi-page typed array under V8 coverage.
function equalBytes(actual: Uint8Array, expected: Uint8Array): void {
  expect(actual.length).toBe(expected.length);
  expect(
    actual.findIndex((byte, i) => byte !== expected[i]),
    "first differing wire byte",
  ).toBe(-1);
}

it.each(seedsFor("next-json-blob-chains", [0xb704, 0xb705]))(
  "preserves multi-page blob byte order under a fixed workspace (seed %i)",
  async (seed) => {
    const reach = createReach<string>("native JSON blob chains", JSON_BLOB_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 70000, max: 240000 }),
        fc.integer({ min: 1, max: 251 }),
        async (length, step) => {
          reach.beginHistory();
          const governor = new Governor(4 << 20),
            spill = new MemorySpillService();
          const grant = await governor.admit({
            label: "blob",
            minimumBytes: 2 << 20,
            spillable: false,
          });
          const pool = new Pool(new Charge(grant), 2 << 20),
            files = new SpillFiles(spill, 1, pool),
            pages = new PageWriter(pool, 16384);
          const signal = new AbortController().signal;
          const io = new SpillIo(pool, files, pages, { grant, queryId: 1, signal }, () =>
            signal.throwIfAborted(),
          );
          const log = new JsonBlobs(io, spill),
            writer = new JsonStream(log, new Slice(4), () => signal.throwIfAborted());
          const bytes = Uint8Array.from(
            { length },
            (_, i) => (Math.floor(i / 65536) + i * step) % 256,
          );
          try {
            await writer.run(writer.copy(bytes));
            const saved = await writer.finish();
            expect(saved.pages).toBeGreaterThan(1);
            reach.reach("multiple native pages");
            expect(spill.bytesInUse).toBeGreaterThan(length);
            const result = new Uint8Array(length);
            let at = 0;
            await log.read(saved, async (part) => {
              result.set(part, at);
              at += part.length;
            });
            equalBytes(result, bytes);
            reach.reach("forward byte order");
            expect(at).toBe(length);
          } finally {
            writer.close();
            await log.close();
            await files.closeAll();
            pages.close();
            pool.close();
            grant.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("zero resources");
        },
      ),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-json-blob-lifetime", [0xb712, 0xb713]))(
  "keeps files and borrowed pages until a suspended visitor settles (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON blob borrowed lifetime", JSON_BLOB_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 70000, max: 150000 }),
        async (fault, length) => {
          reach.beginHistory();
          const governor = new Governor(4 << 20),
            spill = new MemorySpillService(),
            grant = await governor.admit({
              label: "borrowed blob",
              minimumBytes: 2 << 20,
              spillable: false,
            });
          const pool = new Pool(new Charge(grant), 2 << 20),
            files = new SpillFiles(spill, 1, pool),
            pages = new PageWriter(pool, 16384),
            signal = new AbortController().signal;
          const io = new SpillIo(pool, files, pages, { grant, queryId: 1, signal }, () =>
              signal.throwIfAborted(),
            ),
            log = new JsonBlobs(io, spill),
            writer = new JsonStream(log, new Slice(4), () => signal.throwIfAborted());
          const bytes = Uint8Array.from({ length }, (_, i) => i % 251);
          let enter!: () => void, release!: () => void;
          const entered = new Promise<void>((resolve) => {
              enter = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              release = resolve;
            });
          let reading: Promise<void> | undefined, closing: Promise<void> | undefined;
          try {
            await writer.run(writer.copy(bytes));
            const saved = await writer.finish();
            writer.close();
            const failure = new Error("injected visitor failure");
            let seen = 0,
              first = true;
            reading = log.read(saved, async (part) => {
              if (first) {
                first = false;
                const snapshot = part.slice();
                enter();
                await gate;
                equalBytes(part, snapshot);
                if (fault) throw failure;
              }
              equalBytes(part, bytes.subarray(seen, seen + part.length));
              seen += part.length;
            });
            void reading.catch(() => undefined);
            await entered;
            reach.reach("pending visitor");
            let closed = false;
            closing = log.close().then(() => {
              closed = true;
            });
            await yieldTask();
            await yieldTask();
            expect(closed, "the complete visitor borrow must outlive file close").toBe(false);
            reach.reach("close waits");
            release();
            if (fault) {
              await expect(reading).rejects.toBe(failure);
              reach.reach("visitor fault");
            } else {
              await reading;
              expect(seen).toBe(length);
              reach.reach("visitor resumes");
            }
            await closing;
          } finally {
            release();
            await reading?.catch(() => undefined);
            await closing?.catch(() => undefined);
            writer.close();
            await log.close();
            await files.closeAll();
            pages.close();
            pool.close();
            grant.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("zero resources");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, 70000],
          [true, 70000],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
