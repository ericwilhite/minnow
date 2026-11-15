/** Source identity is semantic: coherent type/spelling, known enum and required field16. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_DEFAULT_WIRE_REACH, createReach } from "../../testing/reach.js";
import type { CatalogDefaultSource } from "../access/catalog-default-source.js";
import { defaultLiteralSql } from "../access/catalog-default-source.js";
import { Catalog } from "./catalog.js";
import { column, INT, TEXT, tableInput } from "./test-support.js";
import { decodeCatalogStream, encodeCatalogStream } from "./catalog-stream.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import { encodeCatalogRecord, decodeCatalogRecord } from "./catalog-records.js";
for (const seed of seedsFor("next-catalog-default-wire", [0xb7f2, 0xb7f3]))
  it(`refuses malformed source descriptors seed ${String(seed)}`, () => {
    const reach = createReach("default source wire", CATALOG_DEFAULT_WIRE_REACH);
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 9 }), fc.integer({ min: 1, max: 100 }), (mode, n) => {
        reach.beginHistory();
        const source: CatalogDefaultSource =
          mode === 4
            ? "datetime"
            : mode === 5
              ? "string"
              : mode === 6 || mode === 9
                ? "boolean"
                : "number";
        const type =
          source === "datetime"
            ? ({ kind: "timestamp", zoned: true } as const)
            : source === "string"
              ? TEXT
              : source === "boolean"
                ? ({ kind: "bool" } as const)
                : INT;
        const literal =
          source === "datetime"
            ? new Date(Date.UTC(2000, 0, 1) + n)
            : source === "string"
              ? `汉'${String(n)}`
              : source === "boolean"
                ? true
                : n;
        const catalog = Catalog.empty().change(
          {
            kind: "createTable",
            table: tableInput(
              "t",
              [
                {
                  ...column("v", type),
                  defaultSql: defaultLiteralSql(literal),
                  defaultSource: source,
                },
              ],
              { primaryKey: null },
            ),
          },
          1,
        ).catalog;
        const data = decodeCatalogStream(catalog.encode()),
          table = data.tables[0],
          stored = table?.columns[0];
        if (!table || !stored) throw new Error("missing default fixture");
        const bad = { ...stored } as unknown as Record<string, unknown>;
        const labels = [
          "unknown tag",
          "SQL absent",
          "source type mismatch",
          "nonfinite",
          "invalid datetime",
          "unclosed text",
          "noncanonical boolean",
          "generated default",
          "noncanonical number",
          "literal NULL",
        ] as const;
        if (mode === 0) bad.defaultSource = "unknown";
        if (mode === 1) bad.defaultSql = null;
        if (mode === 2) {
          bad.defaultSource = "string";
          bad.defaultSql = "'1'";
        }
        if (mode === 3) bad.defaultSql = "NaN";
        if (mode === 4) bad.defaultSql = "'not-a-date'";
        if (mode === 5) bad.defaultSql = "'unclosed";
        if (mode === 6) bad.defaultSql = "true";
        if (mode === 7) bad.generatedSql = "1";
        if (mode === 8) bad.defaultSql = `+${String(n)}`;
        if (mode === 9) bad.defaultSql = "NULL";
        expect(() =>
          Catalog.decode(
            encodeCatalogStream({
              ...data,
              tables: [{ ...table, columns: [bad] }],
            } as unknown as typeof data),
          ),
        ).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
        const label = labels[mode];
        if (!label) throw new Error("missing corruption label");
        reach.reach(label);
        const encoded = encodeCatalogRecord("column", stored);
        try {
          const input = new ByteReader(encoded.bytes),
            family = input.varuint(),
            version = input.varuint(),
            fields = new ByteReader(input.blob()),
            body = new ByteWriter();
          let found = false;
          while (!fields.done) {
            const id = fields.varuint(),
              flags = fields.u8(),
              bytes = fields.blob();
            body.varuint(id);
            body.u8(id === 16 ? 0 : flags);
            body.blob(bytes);
            if (id === 16) {
              expect(flags).toBe(1);
              found = true;
            }
          }
          expect(found).toBe(true);
          const changed = new ByteWriter();
          changed.varuint(family);
          changed.varuint(version);
          changed.blob(body.finish());
          expect(() => decodeCatalogRecord("column", changed.finish())).toThrow(
            expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
          );
          reach.reach("field16 required");
        } finally {
          encoded.close();
        }
      }),
      {
        seed,
        numRuns: 30,
        examples: Array.from({ length: 10 }, (_, mode) => [mode, mode + 1] as [number, number]),
      },
    );
    reach.assertReached();
  });
