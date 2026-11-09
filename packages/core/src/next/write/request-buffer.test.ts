/** Large request metadata streams before stringification; fitting strings use owned UTF-8 space. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_REQUEST_BUFFER_STATES } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { SMALL_COMMIT_BYTES } from "./commit-buffers.js";
import { encodeBody } from "./body.js";
import { ByteWriter } from "./bytes.js";
import {
  Harness,
  INT,
  autocommit,
  batch,
  column,
  ints,
  tableInput,
  rules,
} from "./test-support.js";

const reach = createReach("request encoding before allocation", WRITE_REQUEST_BUFFER_STATES);
for (const seed of seedsFor("next-write-request-buffers", [0xb0d1]))
  it(`bounds request and sequence string buffers (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          size: fc.constantFrom(1, 300000),
          token: fc.constantFrom("x", "😀", '\\\u0000"', "\ud800"),
        }),
        async (s) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", undefined, undefined, {
            logRecycleBytes: 16 * 1024 * 1024,
          });
          Object.assign(h, { governor: new Governor(32 * 1024 * 1024) });
          let capture: Uint8Array | null = null;
          let at = 0;
          const open = h.backend.open.bind(h.backend);
          h.backend.open = async (lease) => {
            const session = await open(lease);
            const append = session.appendLog.bind(session);
            session.appendLog = async (bytes) => {
              if (capture !== null) {
                const length = new DataView(
                  bytes.buffer,
                  bytes.byteOffset,
                  bytes.byteLength,
                ).getUint32(16, true);
                capture.set(bytes.subarray(28, 28 + length), at);
                at += length;
              }
              return append(bytes);
            };
            return session;
          };
          const path = await h.open();
          let table = 0;
          let sequence = 0;
          try {
            await autocommit(path, async (st) => {
              table =
                (
                  await st.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false)]),
                  })
                ).id ?? 0;
              sequence =
                (
                  await st.changeCatalog({
                    kind: "createSequence",
                    sequence: {
                      name: "s",
                      start: 1n,
                      increment: 1n,
                      min: 1n,
                      max: 100n,
                      cycle: false,
                      ownedBy: null,
                    },
                  })
                ).id ?? 0;
            });
            await path.quiesce();
            const tx = await path.begin();
            const st = tx.statement({ rules });
            await st.table(table).insert(batch(ints([1])), null);
            await st.end();
            const text = s.token.repeat(s.size);
            const counts = { [`A${text}`]: 1, [`B${text}`]: 2 };
            const request = {
              clientId: new Uint8Array(16).fill(5),
              sequence: 1,
              sentAtMs: 1234,
              counts,
            };
            const expected = encodeBody({ request, effects: tx.effects(new ByteWriter()) });
            const large = expected.length > SMALL_COMMIT_BYTES;
            reach.reach(large ? "streamed metadata" : "single-buffer metadata");
            const stringify = JSON.stringify;
            const jsonSpy = vi
              .spyOn(JSON, "stringify")
              .mockImplementation((...args: Parameters<typeof JSON.stringify>) => {
                if (large && args[0] === counts)
                  throw new Error("Large request stringified before streaming");
                return Reflect.apply(stringify, JSON, args);
              });
            const utf8Spy = vi.spyOn(TextEncoder.prototype, "encode").mockImplementation(() => {
              throw new Error("Commit string materialized uncharged UTF-8 buffer");
            });
            try {
              const output = { repr: "safe" as const, values: new Float64Array(1) };
              const lsn = await path.sequences.next(sequence, 1, output, 0);
              await path.durable(lsn);
              expect(output.values[0]).toBe(1);
              reach.reach("sequence UTF-8");
              capture = new Uint8Array(expected.length);
              const receipt = await tx.commit(request);
              await receipt.durable;
              expect(at).toBe(expected.length);
              expect(Buffer.compare(capture, expected), "request bytes unchanged").toBe(0);
              reach.reach("no unowned string buffer");
            } finally {
              utf8Spy.mockRestore();
              jsonSpy.mockRestore();
              capture = null;
            }
            expect(h.governor.peakBytes).toBeLessThanOrEqual(h.governor.capBytes);
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 8 },
    );
  }, 120_000);
it("reaches both request encoders", () => reach.assertReached());
