/** A retired parent may keep native files, never its previous grant or copied payload. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { SCRATCH_CONTROL_BYTES } from "../spill/scratch-ranges.js";
import { FileScratch } from "../storage/file-store.js";
import { MemoryFile } from "../storage/memory-store.js";
import type { FlatVector } from "../vector/vector.js";
import { DBL, BIG, TEXT } from "../exec/join-fixtures.js";
import { texts } from "../exec/kernels/test-support.js";
import { PageWriter, SpillFiles } from "../exec/spill-pages.js";
import { Charge, Pool, SpillIo } from "../exec/spill-pool.js";
import { RecordCheckpoint } from "../exec/record-checkpoint.js";

it.each(seedsFor("next-binary-record-checkpoint", [0xb4d1, 0xb4d2]))(
  "restores parent records after releasing every old native buffer (seed %i)",
  async (seed) => {
    const reach = createReach("binary record checkpoint", [
      "scratch",
      "memory",
      "retired",
      "float bits",
      "wide counts",
      "rows",
      "zero",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 70, max: 160 }),
        fc.bigInt({ min: 0n, max: (1n << 51n) - 1n }),
        async (rows, payload) => {
          reach.beginHistory();
          for (const scratch of [false, true]) {
            const governor = new Governor(8 << 20);
            const spillControl = scratch
              ? await governor.admit({
                  label: "shared scratch metadata",
                  minimumBytes: SCRATCH_CONTROL_BYTES,
                  spillable: false,
                })
              : null;
            const service = spillControl
              ? new ScratchSpillService(
                  new FileScratch(
                    new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
                    () => undefined,
                    new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
                  ),
                  { budget: spillControl },
                )
              : new MemorySpillService();
            const types = [DBL, BIG, TEXT];
            const grant = await governor.admit({
              label: "old parent",
              minimumBytes: 2 << 20,
              spillable: false,
            });
            const makeIo = (owner: typeof grant) => {
              const pool = new Pool(new Charge(owner), 2 << 20),
                pages = new PageWriter(pool),
                files = new SpillFiles(service, 1, {
                  allocate: () => false,
                  free: () => {
                    throw new Error("unbuffered");
                  },
                });
              return {
                pool,
                pages,
                files,
                io: new SpillIo(pool, files, pages, { grant: owner, queryId: 1 }, () => undefined),
              };
            };
            const old = makeIo(grant),
              checkpoint = new RecordCheckpoint(old.io, service, types),
              floats = new Float64Array(rows),
              bits = new BigUint64Array(floats.buffer),
              counts = new BigInt64Array(rows);
            for (let i = 0; i < rows; i++) {
              bits[i] =
                i % 3 === 0
                  ? 1n << 63n
                  : i % 3 === 1
                    ? 0x7ff8_0000_0000_0000n | payload
                    : 0xfff8_0000_0000_0000n | payload;
              counts[i] = (1n << 60n) + BigInt(i);
            }
            const words = Array.from(
                { length: rows },
                (_, i) => `q${String(i)}-${"λ".repeat(i % 31)}`,
              ),
              columns: FlatVector[] = [
                {
                  form: "flat",
                  type: DBL,
                  length: rows,
                  validity: null,
                  data: { layout: "f64", values: floats },
                },
                {
                  form: "flat",
                  type: BIG,
                  length: rows,
                  validity: null,
                  data: { layout: "int64", values: { repr: "int64", values: counts } },
                },
                texts(words),
              ];
            let next: ReturnType<typeof makeIo> | null = null,
              nextGrant: typeof grant | null = null;
            try {
              for (let i = 0; i < rows; i++) await checkpoint.write(columns, i);
              await checkpoint.seal();
              await old.files.closeAll();
              old.pages.close();
              old.pool.close();
              expect(grant.allocated).toBe(0);
              grant.close();
              // Retiring the parent frees its grant; the shared scratch owner stays live.
              expect(governor.usedBytes).toBe(spillControl?.held ?? 0);
              reach.reach("retired");
              nextGrant = await governor.admit({
                label: "restored parent",
                minimumBytes: 2 << 20,
                spillable: false,
              });
              next = makeIo(nextGrant);
              let at = 0;
              await checkpoint.read(next.io, async (saved, count) => {
                const f = saved[0],
                  n = saved[1],
                  t = saved[2];
                if (
                  f?.data.layout !== "f64" ||
                  n?.data.layout !== "int64" ||
                  n.data.values.repr !== "int64" ||
                  t?.data.layout !== "varlen"
                )
                  throw new Error("Wrong checkpoint layout");
                const actual = new BigUint64Array(
                  f.data.values.buffer,
                  f.data.values.byteOffset,
                  count,
                );
                for (let i = 0; i < count; i++, at++) {
                  expect(actual[i]).toBe(bits[at]);
                  expect(n.data.values.values[i]).toBe(counts[at]);
                  const from = t.data.offsets[i] ?? 0,
                    to = t.data.offsets[i + 1] ?? from;
                  expect(new TextDecoder().decode(t.data.bytes.subarray(from, to))).toBe(words[at]);
                }
              });
              expect(at).toBe(rows);
              expect(checkpoint.tape.pages).toHaveLength(0);
              expect(checkpoint.tape.retainedPageRefs).toBe(1);
              reach.reach("float bits");
              reach.reach("wide counts");
              reach.reach("rows");
              reach.reach(scratch ? "scratch" : "memory");
            } finally {
              await checkpoint.close();
              await old.files.closeAll();
              old.pages.close();
              old.pool.close();
              grant.close();
              if (next) {
                await next.files.closeAll();
                next.pages.close();
                next.pool.close();
              }
              nextGrant?.close();
              if (service instanceof ScratchSpillService) await service.close();
              spillControl?.close();
            }
            expect(governor.usedBytes).toBe(0);
            expect(service.bytesInUse).toBe(0);
            reach.reach("zero");
          }
        },
      ),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  },
);
