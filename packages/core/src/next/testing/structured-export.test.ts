/** PC2: native checksums do not make a structured varlen body trustworthy. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, STRUCTURED_EXPORT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import * as snapshotCodec from "../format/snapshot.js";
import { vectorOf } from "../page/test-support.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { SimLocks } from "../storage/sim-disk.js";
import type { PhysicalType } from "../types.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { exportPinnedSnapshot } from "../write/snapshot-export.js";
import { autocommit, batch, column, tableInput } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";

function image(native: MemoryOpfs): string[] {
  return ["root-0", "root-1", "log"].map((name) =>
    Buffer.from(native.readFileBytes(name) ?? []).toString("base64"),
  );
}
for (const seed of seedsFor("next-structured-export", [0xb2b3, 0xb2b4]))
  it(`withholds the footer on checksummed internal corruption seed ${String(seed)}`, async () => {
    const reach = createReach("structured native export", STRUCTURED_EXPORT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 255 }),
        fc.integer({ min: 1, max: 7 }),
        async (salt, bit) => {
          reach.beginHistory();
          for (const mode of [
            "version",
            "element tag",
            "dimensions",
            "flags",
            "element length",
            "decimal special",
          ] as const) {
            const element: PhysicalType =
                mode === "decimal special"
                  ? { kind: "decimal", precision: null, scale: null }
                  : { kind: "text" },
              type: PhysicalType = { kind: "array", element },
              bytes = encodeArrayValue(element, {
                dimensions: [1],
                lowerBounds: [1],
                elements: [
                  mode === "decimal special"
                    ? { coefficient: 0n, scale: 0, special: "NaN" }
                    : Uint8Array.of(salt),
                ],
              });
            if (mode === "version") bytes[0] = 2 + (salt % 254);
            else if (mode === "element tag") bytes[1] = 17 + (salt % 239);
            else if (mode === "dimensions") bytes[2] = 7 + (salt % 249);
            else if (mode === "flags") bytes[3] = 1 << bit;
            else if (mode === "element length")
              new DataView(bytes.buffer).setUint32(12, 0xffffffff, true);
            else bytes[12] = 4 + (salt % 252);
            const core = new StorageCore(),
              native = new MemoryOpfs();
            Object.assign(core.harness, {
              backend: createOpfsBackend({
                name: "structured-export",
                directory: native.root,
                locks: new SimLocks(),
              }),
            });
            try {
              await core.open();
              await autocommit(core.harness.path, (statement) =>
                statement.changeCatalog({
                  kind: "createTable",
                  table: tableInput("t", [column("v", type)], { primaryKey: null }),
                }),
              );
              // Intentional corruption at the trusted-vector seam, BEFORE native encoding and
              // checksums. The SQL writer is not asked to accept an invalid user array literal.
              await autocommit(core.harness.path, (statement) =>
                statement.table(1).insert(batch(vectorOf(type, [bytes])), null),
              );
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              const grant = await core.harness.governor.admit({
                  label: "structured export",
                  minimumBytes: 8 << 20,
                  spillable: false,
                }),
                pin = core.harness.path.pin();
              try {
                const table = pin.table(1, { budget: grant });
                expect(table.segments[0]?.kind).toBe("group");
                reach.reach("real stored source");
                pin.release();
                const before = image(native);
                await expect(core.execute("SELECT v::text FROM t")).rejects.toMatchObject({
                  code: "MINNOW_MALFORMED_ARRAY",
                });
                reach.reach("typed query refusal");
                const exporting = core.harness.path.pin(),
                  footer = vi.spyOn(snapshotCodec, "encodeSnapshotFooter");
                try {
                  await expect(
                    exportPinnedSnapshot(
                      exporting,
                      new Uint8Array(16),
                      (async function* () {
                        yield Uint8Array.of(1);
                      })(),
                      { write: () => undefined },
                      grant,
                    ),
                  ).rejects.toMatchObject({ code: "MINNOW_PAGE_CORRUPT" });
                  expect(footer).not.toHaveBeenCalled();
                  expect(exporting.released).toBe(true);
                } finally {
                  footer.mockRestore();
                }
                reach.reach(mode);
                reach.reach("typed export refusal");
                reach.reach("no footer");
                expect(image(native)).toEqual(before);
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
  }, 120_000);
