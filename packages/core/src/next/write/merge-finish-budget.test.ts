/* eslint-disable no-restricted-imports -- compare the complete native fence blob without per-byte assertion objects. */
/** Governor rules 1/4/7: finishing must fit prepaid progress, including metadata ownership. */
import { Buffer } from "node:buffer";
import fc from "fast-check";
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_MERGE_FINISH_STATES } from "../../testing/reach.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import { RunEncoder, BLOCK_BYTES, PARTITION_BLOCKS, type RunSummary } from "./runs.js";
import { DEFAULT_MERGE_POLICY, MERGE_FIXED_BYTES, MERGE_INPUT_BYTES } from "./merge.js";

const reach = createReach("default-boundary encoder finish", WRITE_MERGE_FINISH_STATES);
for (const seed of seedsFor("next-write-merge-finish-budget", [0xb5f1]))
  it(`finishes default-sized metadata within admitted credit (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(1024, 1536, 2048),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 5 }),
        async (width, borrow, pressure, abort, extra) => {
          reach.beginHistory();
          reach.reach(borrow ? "borrowed keys" : "copied keys");
          const governor = new Governor(32 * 1024 * 1024);
          const grant = await governor.admit({
            label: "merge finish",
            minimumBytes: MERGE_FIXED_BYTES + 2 * MERGE_INPUT_BYTES,
            spillable: false,
          });
          // Native input leases and writer staging stay live through finish, as in mergeRuns.
          const buffers = [MERGE_INPUT_BYTES, MERGE_INPUT_BYTES, 256 * 1024].map((n) => {
            expect(grant.allocate(n)).toBe(true);
            return new Uint8Array(n).fill(7);
          });
          const ioBytes = buffers.reduce((sum, b) => sum + b.byteLength, 0);
          const key = (buffers[0] ?? new Uint8Array()).subarray(32, 32 + width).fill(129);
          const holder = pressure
            ? await governor.admit({
                label: "other execution minimum",
                minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
                spillable: false,
              })
            : null;
          if (pressure) reach.reach("other minimum held");
          // Long keys reach the real 896 KiB split boundary with a compact input. Three
          // independent encoder contexts reproduce the review's live private-grant footprint;
          // the separate two-reader probe retains its repeated nine-byte, 19M-row workload.
          const perBlock = Math.floor((BLOCK_BYTES - 2) / (width + 14));
          const rows =
            (Math.ceil(DEFAULT_MERGE_POLICY.splitBytes / (width + 2)) + 2 + extra) * perBlock;
          const encoders: RunEncoder[] = [];
          const live: number[] = [];
          const outputs: RunSummary[] = [];
          const tails: Uint8Array[][] = [[], [], []];
          let finishing = -1,
            beforeFinish = 0,
            maximumRequest = 0,
            denials = 0;
          const allocate = grant.allocate.bind(grant);
          grant.allocate = (n) => {
            if (finishing >= 0) maximumRequest = Math.max(maximumRequest, n);
            const ok = allocate(n);
            if (!ok) denials++;
            return ok;
          };
          const stopped = new Error("abort after metadata handoff");
          try {
            for (let c = 0; c < 3; c++) {
              const before = grant.allocated;
              const encoder: RunEncoder = new RunEncoder(
                (part) => {
                  if (finishing === c) {
                    if (part.length >= DEFAULT_MERGE_POLICY.splitBytes) {
                      expect(
                        grant.allocated,
                        "transferred fence backing stays charged until consumed",
                      ).toBeGreaterThanOrEqual(beforeFinish - 64 * 1024);
                      reach.reach("charged metadata handoff");
                      if (abort && c === 0) throw stopped;
                    }
                    // The oracle's stored bytes belong to the consumer, outside the encoder grant.
                    tails[c]?.push(Uint8Array.from(part));
                  }
                  encoder.releasePart(part); // synchronous sink has consumed this part
                },
                false,
                borrow,
                grant,
              );
              encoders.push(encoder);
              for (let row = 0; row < rows; row++) encoder.add(key, 0, key.length, row + 1, false);
              expect(encoder.indexBytes).toBeGreaterThan(DEFAULT_MERGE_POLICY.splitBytes);
              live.push(grant.allocated - before);
            }
            expect(
              grant.allocated,
              "a real default-boundary footprint, not a tiny split",
            ).toBeGreaterThan(3_500_000);
            expect(grant.allocated).toBeLessThanOrEqual(grant.held);
            reach.reach("default split boundary");
            reach.reach("near private allowance");
            for (let c = 0; c < encoders.length; c++) {
              const encoder = encoders[c];
              if (!encoder) throw new Error("missing encoder");
              beforeFinish = grant.allocated;
              finishing = c;
              try {
                const summary = encoder.finish();
                if (abort && c === 0) throw new Error("handoff abort did not fire");
                if (!summary) throw new Error("missing summary");
                outputs.push(summary);
                expect(summary.entryCount).toBe(rows);
                const parts = (tails[c] ?? []).slice(-3),
                  fence = parts[1];
                if (!fence) throw new Error("missing fence tail");
                const blocks = Math.ceil(rows / perBlock);
                const expected = new ByteWriter(fence.length);
                expected.varuint(blocks);
                for (let b = 0; b < blocks; b++) {
                  expected.varuint(width);
                  expected.raw(key);
                }
                expect(
                  Buffer.from(fence).equals(Buffer.from(expected.finish())),
                  "stored fence grammar is unchanged",
                ).toBe(true);
                const index = new ByteReader(parts[0] ?? new Uint8Array());
                expect(index.varuint()).toBe(blocks);
                const directory = new ByteReader(parts[2] ?? new Uint8Array());
                expect(directory.varuint()).toBe(PARTITION_BLOCKS);
                expect(directory.varuint()).toBe(Math.ceil(blocks / PARTITION_BLOCKS));
                summary.release();
                reach.reach("completed finish");
              } catch (error) {
                if (!(abort && c === 0)) throw error;
                expect(error).toBe(stopped);
                reach.reach("abort after transfer");
              } finally {
                encoder.close();
                finishing = -1;
              }
              expect(grant.allocated, "finish/abort returns only its own context").toBe(
                beforeFinish - (live[c] ?? 0),
              );
            }
            expect(maximumRequest, "finish needs no second cardinality-sized backing").toBeLessThan(
              64 * 1024,
            );
            expect(denials).toBe(0);
            expect(grant.allocated).toBe(ioBytes);
            expect(key.every((b) => b === 129)).toBe(true);
            reach.reach("no credit refusal");
          } finally {
            for (const summary of outputs) summary.release();
            for (const encoder of encoders) encoder.close();
            grant.free(ioBytes);
            grant.close();
            holder?.close();
          }
          expect(governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 60 },
    );
    reach.assertReached();
  }, 120_000);
