/** Mutable adapter rows must not change the independently retained canonical Result. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_STORE_CANONICAL_REACH } from "../../testing/reach.js";
import { NativeLiveStore } from "../owner/live-store.js";
import { MetadataClient } from "./live-store-result.test-support.js";
for (const seed of seedsFor("next-live-store-canonical", [8910, 8911]))
  it(`isolates canonical values from adapter mutation seed=${String(seed)}`, async () => {
    const reach = createReach("canonical receiving result", LIVE_STORE_CANONICAL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 99 }),
        async (binary, keyed, n) => {
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
              {
                name: "v",
                sqlType: binary ? "bytea" : "integer",
                type: binary ? { kind: "bytes" } : { kind: "int32", bits: 32 },
                nullable: false,
                render: null,
              },
            ],
            frames: [
              {
                sequence: 0,
                rowCount: 1,
                dictionaries: [],
                columns: [
                  {
                    form: "flat",
                    length: 1,
                    validity: null,
                    data: { layout: "i32", values: Int32Array.of(1) },
                  },
                  {
                    form: "flat",
                    length: 1,
                    validity: null,
                    data: binary
                      ? { layout: "varlen", bytes: Uint8Array.of(n), offsets: Int32Array.of(0, 1) }
                      : { layout: "i32", values: Int32Array.of(n) },
                  },
                ],
              },
            ],
            summary: {
              command: "SELECT",
              rowCount: 1,
              counts: null,
              lsn: 1,
              replayed: false,
              closedEarly: false,
              transaction: "idle",
              catalogVersion: 1,
            },
          });
          const store = new NativeLiveStore<{ id: number; v: number | Uint8Array }>(
            client,
            "SELECT id,v",
            [],
            {
              ...(keyed ? { key: "id" as const } : {}),
              decode: (result) => {
                const rows = result.rows();
                const first = rows[0];
                if (first === undefined) throw new Error("Missing adapter row");
                if (first.v instanceof Uint8Array) first.v[0] = n + 1;
                else first.v = n + 1;
                return rows;
              },
            },
          );
          try {
            await store.refresh();
            expect(store.getSnapshot().rows).toEqual([
              { id: 1, v: binary ? Uint8Array.of(n + 1) : n + 1 },
            ]);
            expect(store.getSnapshot().result?.rows()).toEqual([
              { id: 1, v: binary ? Uint8Array.of(n) : n },
            ]);
            expect(store.getSnapshot().result?.tuples()).toEqual([
              [1, binary ? Uint8Array.of(n) : n],
            ]);
            reach.reach(binary ? "binary mutation" : "scalar mutation");
            reach.reach(keyed ? "keyed" : "unkeyed");
            reach.reach("canonical preserved");
          } finally {
            await store.close();
            client.close();
          }
          expect(store.usedBytes).toBe(0);
          reach.reach("zero charge");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, false, 1],
          [true, true, 2],
          [false, true, 3],
          [true, false, 4],
        ],
      },
    );
    reach.assertReached();
  });
