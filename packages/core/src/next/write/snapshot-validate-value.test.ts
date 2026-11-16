/** Loader parser ownership is observed before closing its admission grant. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, SNAPSHOT_VALIDATOR_LEASE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import type { ExtentReader } from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { encodeSnapshotColumnHeader } from "./snapshot-column.js";
import { SnapshotStagedColumn } from "./snapshot-stage.js";
import { validateSnapshotValue } from "./snapshot-validate-value.js";
import { column } from "./test-support.js";

for (const seed of seedsFor("next-snapshot-validator-lease", [0xb2bd, 0xb2be]))
  it(`returns parser leases before admission ends seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot validator leases", SNAPSHOT_VALIDATOR_LEASE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 3, max: 97 }), async (width) => {
        reach.beginHistory();
        const utf8 = new TextEncoder(),
          element: PhysicalType = { kind: "bool" };
        const good = encodeArrayValue(element, {
          dimensions: [2],
          lowerBounds: [1],
          elements: [true, null],
        });
        const bad = good.slice();
        bad[0] = 2;
        const examples: Array<{ type: PhysicalType; value: Uint8Array; valid: boolean }> = [
          { type: { kind: "array", element }, value: good, valid: true },
          { type: { kind: "array", element }, value: bad, valid: false },
          {
            type: { kind: "json", binary: true },
            value: utf8.encode('{"a":[true,null]}'),
            valid: true,
          },
          {
            type: { kind: "json", binary: true },
            value: utf8.encode('{"a":[true,]}'),
            valid: false,
          },
          { type: { kind: "text" }, value: utf8.encode("😀".repeat(width)), valid: true },
          { type: { kind: "text" }, value: Uint8Array.of(255), valid: false },
        ];
        for (const example of examples) {
          const governor = new Governor(16 << 20),
            grant = await governor.admit({
              label: "validator",
              minimumBytes: 8 << 20,
              spillable: false,
            });
          const space = new MemoryExtentSpace({ cache: false }),
            writer = space.beginWrite("staged");
          const data = new Uint8Array(26 + example.value.length);
          encodeSnapshotColumnHeader(example.type, 1, data.subarray(0, 16));
          data[16] = 1;
          new DataView(data.buffer).setUint32(17, example.value.length, true);
          data.set(example.value, 21);
          data[21 + example.value.length] = 2;
          const controller = new AbortController();
          const staged = new SnapshotStagedColumn(
            {
              id: 1,
              name: "v",
              type: example.type,
              declared: column("v", example.type).declared,
              nullable: true,
              defaultSql: null,
              generatedSql: null,
              identitySequence: null,
              missing: null,
            },
            1,
            data.length,
            writer,
            grant,
            { signal: controller.signal },
          );
          try {
            for (let at = 0; at < data.length; at += width)
              await staged.append(data.subarray(at, at + width));
            const result = staged.finish(space, new Uint8Array(32));
            if (example.valid) await result;
            else await expect(result).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_FORMAT" });
            // Closing the grant first would hide a leaked parser read charge.
            expect(space.leased).toBe(0);
            expect(grant.allocated).toBe(staged.account.used);
            reach.reach("leases returned before grant close");
            reach.reach(example.valid ? "fragmented accepted values" : "fragmented refused values");
            if (example.valid) {
              const before = grant.allocated,
                reads = space.reads;
              const occupy = grant.held - before - 131072 + 1;
              expect(grant.allocate(occupy)).toBe(true);
              try {
                await expect(
                  validateSnapshotValue(staged, space, 21, example.value.length),
                ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_BUDGET" });
                expect(space.reads).toBe(reads);
                expect(space.leased).toBe(0);
                expect(grant.allocated).toBe(before + occupy);
                reach.reach("parser funded before producer entry");
              } finally {
                grant.free(occupy);
              }
              const reason = new Error("cancel parser after read");
              const cancelled: ExtentReader = {
                peek: space.peek.bind(space),
                async read(refs, options) {
                  const lease = await space.read(refs, options);
                  controller.abort(reason);
                  return lease;
                },
              };
              await expect(
                validateSnapshotValue(staged, cancelled, 21, example.value.length),
              ).rejects.toBe(reason);
              expect(space.leased).toBe(0);
              expect(grant.allocated).toBe(before);
              reach.reach("cancel after lease acquisition");
            }
          } finally {
            await staged.discard();
            await writer.abort();
            grant.close();
          }
          expect(space.leased).toBe(0);
          expect(governor.usedBytes).toBe(0);
        }
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  });
