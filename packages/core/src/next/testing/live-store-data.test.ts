/** Adapter data has stable plain fields; accessors/host objects cannot hide owned clone demand. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_STORE_DATA_REACH } from "../../testing/reach.js";
import { NativeLiveStore } from "../owner/live-store.js";
import { MetadataClient } from "./live-store-result.test-support.js";
for (const seed of seedsFor("next-live-store-data", [8908, 8909]))
  it(`admits only stable decoder data fields seed=${String(seed)}`, async () => {
    const reach = createReach("receiving decoder data", LIVE_STORE_DATA_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("data", "accessor", "map", "set", "shared"),
        fc.boolean(),
        fc.integer({ min: 1, max: 99 }),
        async (shape, keyed, id) => {
          reach.beginHistory();
          const client = new MetadataClient({
            columns: [
              {
                name: "id",
                sqlType: "integer",
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
          let reads = 0;
          const row: { id: number; payload?: unknown } = { id };
          if (shape === "accessor")
            Object.defineProperty(row, "payload", {
              enumerable: true,
              get() {
                return ++reads <= (keyed ? 2 : 1)
                  ? Uint8Array.of(7)
                  : new Uint8Array(new ArrayBuffer(4 * 1048576), 0, 1);
              },
            });
          else if (shape === "map")
            row.payload = new Map([["wide", new Uint8Array(new ArrayBuffer(4 * 1048576), 0, 1)]]);
          else if (shape === "set")
            row.payload = new Set([new Uint8Array(new ArrayBuffer(4 * 1048576), 0, 1)]);
          else if (shape === "shared") row.payload = new Uint8Array(new SharedArrayBuffer(8));
          else row.payload = Uint8Array.of(id);
          const store = new NativeLiveStore(client, "SELECT id", [], {
            memoryBytes: 65536,
            ...(keyed ? { key: "id" as const } : {}),
            decode: () => [row],
          });
          try {
            await store.refresh();
            if (shape === "data") {
              expect(store.getSnapshot()).toMatchObject({ status: "ready", rows: [row] });
              reach.reach("plain binary data admitted");
            } else {
              const rejected = store.getSnapshot();
              expect(rejected).toMatchObject({ status: "error", rows: [] });
              if (rejected.status !== "error" || !(rejected.error instanceof Error))
                throw new Error("Expected a cloned decoder error");
              expect(rejected.error.message).toMatch(/data properties|plain records|non-shared/);
              expect(store.getSnapshot().result).toBeUndefined();
              expect(reads).toBe(0);
              reach.reach("rejected before clone");
            }
            reach.reach(shape);
            reach.reach(keyed ? "keyed" : "unkeyed");
          } finally {
            await store.close();
            client.close();
          }
          expect(store.usedBytes).toBe(0);
          expect(NativeLiveStore.clientUsedBytes(client)).toBe(0);
          reach.reach("zero charge");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["data", false, 1],
          ["accessor", true, 2],
          ["map", false, 3],
          ["set", true, 4],
          ["shared", false, 5],
        ],
      },
    );
    reach.assertReached();
  });
