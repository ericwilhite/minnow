/** The measured API path must decode real stored pages via SQL and preserve independent values. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import {
  createReach,
  OWNER_API_BENCH_REACH,
  OWNER_API_ENVELOPE_REACH,
} from "../../../packages/core/src/testing/reach.js";
import { MemoryExtentSpace } from "../../../packages/core/src/next/page/memory-extents.js";
import { defined, valueAt, writeGroup } from "../../../packages/core/src/next/page/test-support.js";
import { Governor } from "../../../packages/core/src/next/memory/governor.js";
import { ProtocolClient } from "../../../packages/core/src/next/owner/client.js";
import { payloadBytes } from "../../../packages/core/src/next/owner/frame.js";
import type { WireFlat } from "../../../packages/core/src/next/exec/result.js";
import { isValid } from "../../../packages/core/src/next/vector/validity.js";
import { carriedFrames, prepareOwnerExport } from "./owner-export.js";

for (const seed of seedsFor("owner-api-bench", [0xb2f2, 0xb2f3]))
  it(`measures the production owner API (${String(seed)})`, async () => {
    const reach = createReach("owner API bench", OWNER_API_BENCH_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 33, max: 129 }),
        fc.integer({ min: 1, max: 80 }),
        fc.boolean(),
        async (count, width, sparse) => {
          reach.beginHistory();
          const space = new MemoryExtentSpace(),
            governor = new Governor(32 * 1048576);
          const source: Array<readonly [number, string | null, bigint]> = Array.from(
            { length: count },
            (_, r) => [
              r,
              sparse && r % 3 === 0 ? null : `水${String(r % 4)}:${"x".repeat(width)}`,
              (1n << 60n) + BigInt(r),
            ],
          );
          const { group } = await writeGroup(
            [
              { type: { kind: "int32", bits: 32 }, values: source.map((r) => Number(r[0])) },
              {
                type: { kind: "text" },
                values: source.map((r) => (r[1] === null ? null : new TextEncoder().encode(r[1]))),
              },
              { type: { kind: "int64" }, values: source.map((r) => BigInt(defined(r[2]))) },
            ],
            { space },
          );
          const channels = new MessageChannel(),
            client = new ProtocolClient();
          const actor = prepareOwnerExport(space, [{ rows: count, chunks: group.columns }], {
            governor,
            port: channels.port1,
            memory: () => ({ governorHeld: governor.usedBytes }),
          });
          await client.connect(channels.port2);
          try {
            for (let pass = 0; pass < 2; pass++) {
              const completion = actor.arm(),
                stream = client.exportStream("SELECT * FROM t");
              completion.catch(() => undefined);
              const actual: unknown[][] = [];
              let frames = 0,
                bytes = 0;
              for await (const frame of stream.frames) {
                frames++;
                bytes += payloadBytes(frame.payload);
                for (let row = 0; row < frame.payload.rowCount; row++)
                  actual.push(
                    frame.payload.columns.map((wire, k) => {
                      let flat: WireFlat,
                        index = row;
                      if (wire.form === "dictionary") {
                        if (!isValid(wire.validity, row)) return null;
                        const entry = frame.dictionaries.locate(
                          wire.dictionary,
                          defined(wire.codes[row]),
                        );
                        flat = entry.entries;
                        index = entry.index;
                      } else if (wire.form === "constant") {
                        flat = wire.value;
                        index = 0;
                      } else flat = wire;
                      const decoded = valueAt(
                        { ...flat, type: defined(group.columns[k]).type },
                        index,
                      );
                      return decoded instanceof Uint8Array
                        ? new TextDecoder().decode(decoded)
                        : decoded;
                    }),
                  );
                frame.release();
              }
              await stream.done;
              const result = await completion;
              expect(actual).toEqual(source);
              expect(result.rows).toBe(count);
              expect(result.executorStats).toMatchObject({
                path: "production-owner-api",
                frames,
                wireBytes: bytes,
                outputRecycling: true,
              });
              expect(result.executorStats.profile.operators.some((op) => op.op === "project")).toBe(
                true,
              );
              reach.reach("real SQL identity projection");
              reach.reach("wire totals");
              reach.reach(pass === 0 ? "first export" : "same connection export");
            }
            reach.reach(sparse ? "sparse NULLs" : "all valid");
          } finally {
            client.close();
            await actor.close();
            channels.port2.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  }, 60_000);

for (const seed of seedsFor("owner-api-bench-envelopes", [0xb2f4, 0xb2f5]))
  it(`observes rows and terminal frames (${String(seed)})`, async () => {
    const reach = createReach("owner API frame envelopes", OWNER_API_ENVELOPE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: -100000, max: 100000 }), { minLength: 1, maxLength: 80 }),
        fc.boolean(),
        async (values, terminal) => {
          reach.beginHistory();
          const frames = [
            {
              sequence: 0,
              rowCount: values.length,
              dictionaries: [],
              columns: [
                {
                  form: "flat" as const,
                  length: values.length,
                  validity: null,
                  data: { layout: "i32" as const, values: Int32Array.from(values) },
                },
              ],
            },
          ];
          const common = { seq: 1, columns: null, frames };
          const message = terminal
            ? {
                ...common,
                t: "done" as const,
                summary: {
                  command: "SELECT",
                  rowCount: values.length,
                  counts: null,
                  lsn: null,
                  replayed: false,
                  closedEarly: false,
                  transaction: "idle" as const,
                  catalogVersion: 1,
                },
              }
            : { ...common, t: "rows" as const };
          const observed = carriedFrames(message).flatMap((frame) =>
            frame.columns.flatMap((column) =>
              column.form === "flat" && column.data.layout === "i32"
                ? Array.from(column.data.values)
                : [],
            ),
          );
          expect(observed).toEqual(values);
          reach.reach(terminal ? "done envelope" : "rows envelope");
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  });
