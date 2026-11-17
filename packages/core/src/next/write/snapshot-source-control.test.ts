/** A source close retains its fixed control funding until started reclaim I/O settles. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_SOURCE_CONTROL_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { spoolSnapshotInput } from "./snapshot-source.js";

for (const seed of seedsFor("next-snapshot-source-control", [0xb2eb, 0xb2ec]))
  it(`source funding and repeated close span reclamation ${String(seed)}`, async () => {
    const reach = createReach("snapshot source control", SNAPSHOT_SOURCE_CONTROL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 1024 }), fc.boolean(), async (length, fail) => {
        reach.beginHistory();
        const governor = new Governor(4 << 20),
          disk = new Uint8Array(65536);
        let entered!: () => void, release!: () => void;
        const started = new Promise<void>((resolve) => {
          entered = resolve;
        });
        const waiting = new Promise<void>((resolve) => {
          release = resolve;
        });
        const failure = new Error("scratch release failed");
        const source = await spoolSnapshotInput(
          (async function* () {
            yield new Uint8Array(length).fill(7);
          })(),
          {
            offset: 0,
            capacity: disk.length,
            scratch: {
              write: (at, bytes) => {
                disk.set(bytes, at);
                return Promise.resolve();
              },
              readInto: (at, into) => {
                into.set(disk.subarray(at, at + into.length));
                return Promise.resolve();
              },
              truncate: () => Promise.resolve(),
              async release() {
                entered();
                await waiting;
                if (fail) throw failure;
              },
            },
          },
          governor,
          { maxChunkBytes: 1024 },
        );
        let secondSettled = false;
        const first = source.close().then(
          () => null,
          (error: unknown) => error,
        );
        await started;
        const second = source.close().then(
          () => {
            secondSettled = true;
            return null;
          },
          (error: unknown) => {
            secondSettled = true;
            return error;
          },
        );
        try {
          await Promise.resolve();
          expect(governor.usedBytes).toBe(1024);
          reach.reach("close holds control");
          expect(secondSettled).toBe(false);
          reach.reach("concurrent closes wait");
          await expect(
            source.readInto(0, new Uint8Array(1), new AbortController().signal),
          ).rejects.toThrow("Snapshot spool released");
          reach.reach("closed source refuses reads");
        } finally {
          release();
          await first;
          await second;
        }
        expect(await first).toBe(fail ? failure : null);
        expect(await second).toBe(fail ? failure : null);
        reach.reach(fail ? "release failure" : "release success");
        expect(governor.usedBytes).toBe(0);
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, false],
          [1, true],
        ],
      },
    );
    reach.assertReached();
  });
