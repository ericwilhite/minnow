/** SQL TEXT retains every UTF8 character. PostgreSQL byte/length output is independent
 * authority; the FTS document bound counts the complete stored/rendered string.
 */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_UTF8_BOM_REACH } from "../../testing/reach.js";
import { ftsValue } from "../exec/fts-value.js";
import { tokenize, MAX_INDEXED_STRING_CHARACTERS } from "../full-text.js";
import { decodeUtf8 } from "../vector/varlen.js";
import type { FlatVector } from "../vector/vector.js";
const encoder = new TextEncoder();
for (const seed of seedsFor("next-fts-utf8-bom", [0xb84d, 0xb84e]))
  it(`preserves PostgreSQL text bytes and exact FTS length at BOM boundaries ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("FTS UTF8 BOM", FTS_UTF8_BOM_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(fc.constantFrom(0, 1, 31, 16382, 16383, 16384), async (n) => {
          reach.beginHistory();
          for (const [position, text] of [
            ["leading", "\ufeff" + "a".repeat(n)],
            ["interior", "a\ufeff" + "a".repeat(n)],
            ["none", "a".repeat(n)],
          ] as const) {
            const bytes = encoder.encode(text),
              data = { layout: "varlen" as const, bytes, offsets: Int32Array.of(0, bytes.length) },
              vector: FlatVector = {
                form: "flat",
                type: { kind: "text" },
                length: 1,
                validity: null,
                data,
              };
            const rows = await pg.query<{ hex: string; n: number }>(
              "SELECT encode(convert_to($1::text,'UTF8'),'hex') AS hex, length($1::text) AS n",
              [text],
            );
            const native = decodeUtf8(data, 0),
              expected = rows.rows[0];
            if (!expected) throw new Error("Missing PG text oracle");
            expect(
              Buffer.from(encoder.encode(native)).toString("hex"),
              "native U+FEFF must survive as PostgreSQL TEXT bytes",
            ).toBe(expected.hex);
            expect(native.length).toBe(expected.n);
            expect(ftsValue(vector, 0)).toBe(text);
            if (text.length > MAX_INDEXED_STRING_CHARACTERS) {
              expect(() => tokenize(native)).toThrow(/16384 characters/);
              reach.reach("over bound");
            } else {
              expect(() => tokenize(native)).not.toThrow();
              reach.reach("within bound");
            }
            if (text.length === MAX_INDEXED_STRING_CHARACTERS) reach.reach("exact bound");
            reach.reach(position);
            if (!text.length) reach.reach("empty");
          }
        }),
        { seed, numRuns: 24 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 60_000);
