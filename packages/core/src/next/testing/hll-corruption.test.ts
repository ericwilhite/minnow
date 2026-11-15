/** PC4 is a first-read law even while HLL is not used for query planning. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, NATIVE_HLL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import * as snapshotCodec from "../format/snapshot.js";
import { DistinctSketch } from "../page/sketch.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { SimLocks } from "../storage/sim-disk.js";
import { exportPinnedSnapshot } from "../write/snapshot-export.js";
import { autocommit, batch, column, tableInput, INT, ints } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";

const image = (native: MemoryOpfs) =>
  ["root-0", "root-1", "log"].map((name) =>
    Buffer.from(native.readFileBytes(name) ?? []).toString("base64"),
  );
for (const operation of ["query", "export", "index rebuild"] as const)
  for (const seed of seedsFor("next-native-hll", [0xb2b5, 0xb2b6]))
    it(`refuses checksummed HLL bodies on native ${operation} seed ${String(seed)}`, async () => {
      const reach = createReach(`native HLL ${operation}`, NATIVE_HLL_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 512, max: 640 }),
          fc.integer({ min: 1, max: 127 }),
          fc.integer({ min: 24, max: 255 }),
          async (count, salt, rank) => {
            reach.beginHistory();
            for (const mutation of ["dense rank", "sparse duplicate", "sparse order"] as const) {
              const core = new StorageCore(),
                native = new MemoryOpfs();
              Object.assign(core.harness, {
                backend: createOpfsBackend({
                  name: "native-hll",
                  directory: native.root,
                  locks: new SimLocks(),
                }),
              });
              await core.open();
              try {
                await autocommit(core.harness.path, (s) =>
                  s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("v", INT)], { primaryKey: null }),
                  }),
                );
                const rows = mutation === "dense rank" ? count : 32 + (salt % 64);
                await autocommit(core.harness.path, (s) =>
                  s
                    .table(1)
                    .insert(
                      batch(ints(Array.from({ length: rows }, (_, row) => row + salt * 1000))),
                      null,
                    ),
                );
                // The returned fresh body is damaged BEFORE directory and BlobRef checksums.
                // eslint-disable-next-line @typescript-eslint/unbound-method -- Explicit .call(this).
                const original = DistinctSketch.prototype.serialize;
                let damaged = 0;
                const change = vi
                  .spyOn(DistinctSketch.prototype, "serialize")
                  .mockImplementation(function (this: DistinctSketch) {
                    const bytes = original.call(this);
                    expect(bytes[1]).toBe(mutation === "dense rank" ? 0 : 1);
                    if (mutation === "dense rank") bytes[4 + salt] = rank;
                    else {
                      expect((bytes.length - 4) / 3).toBeGreaterThan(1);
                      if (mutation === "sparse duplicate") bytes.copyWithin(7, 4, 6);
                      else {
                        const first = bytes.slice(4, 7);
                        bytes.copyWithin(4, 7, 10);
                        bytes.set(first, 7);
                      }
                    }
                    damaged++;
                    return bytes;
                  });
                try {
                  await core.harness.path.recycleLog();
                  await core.harness.path.quiesce();
                } finally {
                  change.mockRestore();
                }
                expect(damaged).toBeGreaterThan(0);
                const grant = await core.harness.governor.admit({
                    label: "native HLL",
                    minimumBytes: 8 << 20,
                    spillable: false,
                  }),
                  pin = core.harness.path.pin();
                try {
                  const segment = pin.table(1, { budget: grant }).segments[0];
                  expect(segment?.kind).toBe("group");
                  if (segment?.kind !== "group") throw new Error("HLL source was not stored");
                  await segment.root();
                  const ref = segment.chunk(1)?.distinctSketch;
                  if (!ref) throw new Error("Stored HLL reference missing");
                  // Direct raw read proves the independent HLL reference checksum is valid too;
                  // do not consume the directory before the operation under test.
                  const held = await pin.pages.read([ref], { budget: grant, retain: false });
                  try {
                    const bytes = held.bytes[0];
                    if (bytes === undefined) throw new Error("Stored HLL body missing");
                    expect(bytes[1]).toBe(mutation === "dense rank" ? 0 : 1);
                  } finally {
                    held.release();
                  }
                  pin.release();
                  reach.reach("real stored source");
                  reach.reach("checksums valid");
                  const before = image(native);
                  let failure: unknown;
                  try {
                    if (operation === "query") await core.execute("SELECT v FROM t");
                    else if (operation === "index rebuild")
                      await core.execute("CREATE INDEX ix ON t (v)");
                    else {
                      const exporting = core.harness.path.pin(),
                        footer = vi.spyOn(snapshotCodec, "encodeSnapshotFooter");
                      try {
                        await exportPinnedSnapshot(
                          exporting,
                          new Uint8Array(16),
                          (async function* () {
                            yield Uint8Array.of(1);
                          })(),
                          { write: () => undefined },
                          grant,
                        );
                      } finally {
                        expect(footer).not.toHaveBeenCalled();
                        footer.mockRestore();
                        expect(exporting.released).toBe(true);
                      }
                    }
                  } catch (error) {
                    failure = error;
                  }
                  expect(failure).toMatchObject({ code: "MINNOW_PAGE_CORRUPT" });
                  expect((failure as Error).message).not.toMatch(/checksum/iu);
                  expect(image(native)).toEqual(before);
                  reach.reach(mutation);
                  reach.reach("typed first-read refusal");
                  reach.reach("root and WAL unchanged");
                } finally {
                  pin.release();
                  expect(grant.allocated).toBe(0);
                  grant.close();
                }
                await core.harness.reopen();
                expect((await core.execute("SELECT 1")).kind).toBe("rows");
                reach.reach("reopenable");
              } finally {
                await core.close();
              }
              expect(core.harness.governor.usedBytes).toBe(0);
              reach.reach("charges released");
            }
          },
        ),
        { seed, numRuns: 4 },
      );
      reach.assertReached();
    }, 180_000);
