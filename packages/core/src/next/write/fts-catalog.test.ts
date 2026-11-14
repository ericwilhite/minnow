/** MCAT21 is a portable declaration; native roots/leases never enter it.
 * Authority: access/catalog.ts stable IDs/version preconditions and the approved FTS contract.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_CATALOG_REACH, FTS_CATALOG_FEATURE_REACH } from "../../testing/reach.js";
import { ftsDef, type FtsDef } from "../access/fts-def.js";
import { DurableFormatError, CATALOG_FTS_FEATURE } from "../durable-format.js";
import { Catalog, decodeLogged, encodeLogged } from "./catalog.js";
import { column, tableInput, TEXT, Harness, autocommit } from "./test-support.js";
import { CatalogBuffers, writeCatalogStorage, catalogStorageParts } from "./catalog-storage.js";
import { decodeCatalogParts, decodeCatalogStream } from "./catalog-stream.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { ByteWriter } from "./bytes.js";
import { decodeCatalogRecord, encodeCatalogRecord } from "./catalog-records.js";
import { StoreCorruptionError } from "../storage/contract.js";

// Independent MCAT framing: field-id varuint, separate flags byte, length, payload.
function wire(kind: number, fields: ReadonlyArray<readonly [number, Uint8Array, number?]>) {
  const body = new ByteWriter();
  for (const [id, value, flags = 1] of fields) {
    body.varuint(id);
    body.u8(flags);
    body.blob(value);
  }
  const out = new ByteWriter();
  out.varuint(kind);
  out.varuint(1);
  out.blob(body.finish());
  return out.finish();
}
function golden(id = 1, config = wire(26, [[1, Uint8Array.of(1)]]), columnId = 1) {
  return wire(21, [
    [1, Uint8Array.of(id)],
    [2, Uint8Array.of(1)],
    [3, Uint8Array.of(columnId)],
    [4, Uint8Array.of(1)],
    [5, Uint8Array.of(2)],
    [6, Uint8Array.of(1)],
    [7, config],
  ]);
}
it("reads the independently authored FtsDef21 / config26 wire allocation", () => {
  expect(decodeCatalogRecord("fts", golden())).toEqual(ftsDef(1, 1, 1));
  const encoded = encodeCatalogRecord("fts", ftsDef(1, 1, 1));
  try {
    expect(encoded.bytes).toEqual(golden());
  } finally {
    encoded.close();
  }
});

function first<T>(values: readonly T[]): T {
  const value = values[0];
  if (value === undefined) throw new Error("missing generated catalog entry");
  return value;
}
function budget() {
  return {
    held: 0,
    allocate(n: number) {
      this.held += n;
      return true;
    },
    free(n: number) {
      this.held -= n;
      expect(this.held).toBeGreaterThanOrEqual(0);
    },
  };
}
for (const seed of seedsFor("next-fts-catalog", [0xb81b, 0xb81c]))
  it(`preserves FTS declarations through WAL/native/fragmented copies seed ${String(seed)}`, async () => {
    const reach = createReach("FTS catalog", FTS_CATALOG_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 1, max: 31 }),
        async (count, chunk) => {
          reach.beginHistory();
          let catalog = Catalog.empty().change(
            {
              kind: "createTable",
              table: tableInput(
                "docs",
                Array.from({ length: count }, (_, i) => column(`c${String(i)}`, TEXT, true)),
              ),
            },
            1,
          ).catalog;
          for (const col of first(catalog.tables).columns) {
            const { id: _id, ...input } = ftsDef(1, 1, col.id);
            void _id;
            const result = catalog.change({ kind: "createFts", fts: input }, catalog.version + 1);
            for (const change of result.logged) {
              const log = decodeLogged(encodeLogged(change));
              expect(catalog.apply(log, log.version).fts).toEqual(result.catalog.fts);
              expect(() =>
                catalog.withVersion(catalog.version + 1).apply(log, log.version),
              ).toThrow(StoreCorruptionError);
              reach.reach("stale WAL");
            }
            catalog = result.catalog;
          }
          reach.reach("WAL");
          expect(catalog.fts.length).toBe(count);
          expect(catalog.counters.nextFtsId).toBe(count + 1);
          const meta = decodeCatalogStream(catalog.encode()).wireMetadata as {
            requiredFeatures: number[];
          };
          expect(meta.requiredFeatures).toEqual([CATALOG_FTS_FEATURE]);
          const account = budget(),
            buffers = new CatalogBuffers(account);
          async function* parts() {
            const bytes = catalog.encode();
            for (let at = 0; at < bytes.length; at += chunk) yield bytes.subarray(at, at + chunk);
          }
          const split = Catalog.fromStored(await decodeCatalogParts(parts(), buffers), account);
          expect(split.encode()).toEqual(catalog.encode());
          reach.reach("fragmented stream");
          const space = new MemoryExtentSpace(),
            writer = space.beginWrite("fts catalog");
          try {
            const root = await writeCatalogStorage(writer, catalog, account);
            const loaded = Catalog.fromStored(
              await decodeCatalogParts(catalogStorageParts(root, space, account), buffers),
              account,
            );
            expect(loaded.encode()).toEqual(catalog.encode());
            reach.reach("native pages");
          } finally {
            await writer.abort();
            buffers.close();
          }
          const def = first(catalog.fts);
          const extended = wire(21, [
            [1, Uint8Array.of(def.id)],
            [2, Uint8Array.of(1)],
            [3, Uint8Array.of(def.columnId)],
            [4, Uint8Array.of(1)],
            [5, Uint8Array.of(2)],
            [6, Uint8Array.of(1)],
            [
              7,
              wire(26, [
                [1, Uint8Array.of(1)],
                [100, Uint8Array.of(0xff), 0],
              ]),
            ],
            [100, Uint8Array.of(9), 0],
          ]);
          const opaque = decodeCatalogRecord("fts", extended) as FtsDef;
          const updated = catalog.change(
            { kind: "upsertFts", fts: opaque },
            catalog.version + 1,
          ).catalog;
          const loaded = Catalog.decode(updated.withVersion(updated.version + 1).encode());
          const encoded = encodeCatalogRecord("fts", loaded.fts[0]);
          try {
            expect(encoded.bytes).toEqual(extended);
          } finally {
            encoded.close();
          }
          reach.reach("display extensions");
          reach.reach("portable definitions");
          const drop = catalog.change(
            { kind: "dropFts", ftsId: def.id },
            catalog.version + 1,
          ).catalog;
          expect(() => drop.change({ kind: "upsertFts", fts: def }, drop.version + 1)).toThrow(
            StoreCorruptionError,
          );
          const { id: _old, ...input } = def;
          void _old;
          const renewed = drop.change({ kind: "createFts", fts: input }, drop.version + 1).catalog;
          expect(renewed.fts.at(-1)?.id).toBe(count + 1);
          reach.reach("stable IDs");
          expect(
            renewed.change({ kind: "dropTable", tableId: 1 }, renewed.version + 1).catalog.fts,
          ).toEqual([]);
          reach.reach("drop table");
          expect(() =>
            catalog.change(
              { kind: "upsertFts", fts: { ...def, columnId: 999 } },
              catalog.version + 1,
            ),
          ).toThrow(StoreCorruptionError);
          reach.reach("invalid binding");
          expect(() =>
            catalog.change(
              { kind: "upsertFts", fts: { ...def, rendererVersion: 1 } },
              catalog.version + 1,
            ),
          ).toThrow(DurableFormatError);
          reach.reach("unknown semantics");
          expect(account.held).toBe(0);
          reach.reach("drained");
        },
      ),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });

for (const store of ["memory", "sim"] as const)
  it(`replays FTS definitions through the real ${store} write path and checkpoint`, async () => {
    const h = new Harness("strict", store);
    let path = await h.open();
    try {
      await autocommit(path, (s) =>
        s.changeCatalog({
          kind: "createTable",
          table: tableInput("docs", [column("body", TEXT, true)]),
        }),
      );
      const { id: _id, ...input } = ftsDef(1, 1, 1);
      void _id;
      await autocommit(path, (s) => s.changeCatalog({ kind: "createFts", fts: input }));
      const expected = path.catalog.encode();
      path = await h.reopen();
      expect(path.catalog.encode()).toEqual(expected);
      await path.recycleLog();
      path = await h.reopen();
      expect(path.catalog.encode()).toEqual(expected);
    } finally {
      await h.close();
    }
  });

for (const seed of seedsFor("next-fts-independent-stream", [0xb81f, 0xb820]))
  it(`accepts generated independent FTS streams seed ${String(seed)}`, () => {
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 4 }), (count) => {
        const table = Catalog.empty().change(
          {
            kind: "createTable",
            table: tableInput(
              "docs",
              Array.from({ length: count }, (_, i) => column(`c${String(i)}`, TEXT, true)),
            ),
          },
          1,
        ).catalog.tables[0];
        if (!table) throw new Error("missing generated table");
        const tableBytes = encodeCatalogRecord("table", table);
        try {
          const counters = wire(2, [
            [1, Uint8Array.of(2)],
            [2, Uint8Array.of(count + 1)],
            [3, Uint8Array.of(2)],
            [4, Uint8Array.of(1)],
            [5, Uint8Array.of(1)],
            [6, Uint8Array.of(1)],
            [7, Uint8Array.of(1)],
            [8, Uint8Array.of(count + 1)],
          ]);
          const sections = new ByteWriter();
          sections.varuint(5);
          for (const [kind, n] of [
            [3, 1],
            [12, 0],
            [14, 0],
            [15, 0],
            [21, count],
          ] as const)
            sections.raw(
              wire(22, [
                [1, Uint8Array.of(kind)],
                [2, Uint8Array.of(n)],
              ]),
            );
          const meta = wire(1, [
            [1, Uint8Array.of(1)],
            [2, counters],
            [3, Uint8Array.of(1, 1)],
            [4, sections.finish()],
          ]);
          const out = new ByteWriter();
          out.u32(0x5441434d);
          out.u16(1);
          out.u16(0);
          out.u32(16);
          out.u32(1);
          out.raw(meta);
          out.raw(tableBytes.bytes);
          for (let i = 1; i <= count; i++) out.raw(golden(i, undefined, i));
          const decoded = Catalog.decode(out.finish());
          expect(decoded.fts).toEqual(
            Array.from({ length: count }, (_, i) => ftsDef(i + 1, 1, i + 1)),
          );
          expect(decoded.counters.nextFtsId).toBe(count + 1);
        } finally {
          tableBytes.close();
        }
      }),
      { seed, numRuns: 40 },
    );
  });

for (const seed of seedsFor("next-fts-catalog-features", [0xb821, 0xb822]))
  it(`refuses duplicate/out-of-order feature tags and unknown features seed ${String(seed)}`, () => {
    const empty = Catalog.empty(),
      reach = createReach("FTS catalog features", FTS_CATALOG_FEATURE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 2, max: 20 }),
        fc.integer({ min: 2, max: Number.MAX_SAFE_INTEGER }),
        (copies, future) => {
          reach.beginHistory();
          for (const [name, features] of [
            ["duplicate supported", Array.from({ length: copies }, () => 1)],
            ["out of order", [future, 1]],
            ["future typed", [future]],
          ] as const) {
            const encoded = encodeCatalogRecord("catalog", {
              version: 0,
              counters: empty.counters,
              requiredFeatures: features,
              sections: [3, 12, 14, 15, 21].map((kind) => ({ kind, count: 0 })),
            });
            try {
              const out = new ByteWriter();
              out.u32(0x5441434d);
              out.u16(1);
              out.u16(0);
              out.u32(16);
              out.u32(1);
              out.raw(encoded.bytes);
              expect(() => Catalog.decode(out.finish())).toThrow(
                expect.objectContaining({
                  code: features.length > 1 ? "MINNOW_STORE_CORRUPT" : "MINNOW_CATALOG_UNSUPPORTED",
                }),
              );
              reach.reach(name);
            } finally {
              encoded.close();
            }
          }
        },
      ),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });
