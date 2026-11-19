/** Empty row sets still retain canonical column metadata and must admit it before decode. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_STORE_METADATA_REACH } from "../../testing/reach.js";
import { MetadataClient } from "./live-store-result.test-support.js";
import { NativeLiveStore } from "../owner/live-store.js";

for (const seed of seedsFor("next-live-store-metadata", [8906, 8907]))
  it(`admits retained canonical headers seed=${String(seed)}`, async () => {
    const reach = createReach("receiving result metadata", LIVE_STORE_METADATA_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.constantFrom(64, 2048, 131072), async (type, width) => {
        reach.beginHistory();
        const client = new MetadataClient({
          columns: [
            {
              name: type ? "value" : "x".repeat(width),
              sqlType: type ? "x".repeat(width) : "integer",
              type: { kind: "int32", bits: 32 },
              nullable: false,
              render: null,
            },
          ],
          frames: [],
          summary: {
            command: "SELECT",
            rowCount: 0,
            counts: null,
            lsn: 1,
            replayed: false,
            closedEarly: false,
            transaction: "idle",
            catalogVersion: 1,
          },
        });
        const store = new NativeLiveStore(client, "SELECT value", [], { memoryBytes: 65536 });
        try {
          await store.refresh();
          if (width > 65536) {
            expect(store.getSnapshot()).toMatchObject({
              status: "error",
              rows: [],
              error: { code: "MINNOW_MEMORY_ADMISSION" },
            });
            expect(store.getSnapshot().result).toBeUndefined();
            reach.reach("refused before canonical decode");
          } else {
            expect(store.getSnapshot()).toMatchObject({ status: "ready", rows: [] });
            expect(
              store.getSnapshot().result?.columns[0]?.[type ? "sqlType" : "name"],
            ).toHaveLength(width);
            reach.reach("small metadata admitted");
          }
          expect(store.usedBytes).toBeLessThanOrEqual(65536);
          reach.reach(type ? "type metadata" : "column labels");
        } finally {
          await store.close();
          client.close();
        }
        expect(store.usedBytes).toBe(0);
        expect(NativeLiveStore.clientUsedBytes(client)).toBe(0);
        reach.reach("zero charge");
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, 64],
          [true, 131072],
          [false, 131072],
          [true, 2048],
        ],
      },
    );
    reach.assertReached();
  });
