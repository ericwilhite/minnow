/** Lead ruling 2026-11-13: renderer2 uses PG TIME/ARRAY output, not original lexical spelling. */
import { FTS_RENDERING_AUTHORITY } from "./divergences.js";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, FTS_RENDER_REACH } from "../../testing/reach.js";
import { ftsValue, ftsTimeText } from "../exec/fts-value.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { FTS_RENDER_VERSION, ftsMatchTruth, tokenizeQuery } from "../full-text.js";
import * as shipped from "../../engine/fts.js";
import { timeDomainValue, arrayDomainValue, jsonDomainValue } from "../../engine/sql-domains.js";
import type { FlatVector } from "../vector/vector.js";
import type { PhysicalType } from "../types.js";

for (const seed of seedsFor("next-fts-render", [0xb7f7, 0xb7f8]))
  it(`pins PostgreSQL canonical text and deliberate v1 differences (${String(seed)})`, async () => {
    const pg = new PGlite(),
      random = mulberry32(seed),
      reach = createReach(`FTS renderer2 ${String(seed)}`, FTS_RENDER_REACH);
    try {
      await pg.waitReady;
      reach.beginHistory();
      expect(FTS_RENDER_VERSION).toBe(FTS_RENDERING_AUTHORITY.nativeRenderVersion);
      expect(shipped.FTS_RENDER_VERSION).toBe(FTS_RENDERING_AUTHORITY.shippedRenderVersion);
      reach.reach("versioned");
      for (const micros of [
        0,
        36_000_500_000,
        86_400_000_000,
        ...Array.from({ length: 48 }, () => Math.floor(random() * 86_400_000_000)),
      ]) {
        const text = ftsTimeText(BigInt(micros));
        expect(text).toBe(
          (await pg.query<{ t: string }>("SELECT $1::time::text t", [text])).rows[0]?.t,
        );
      }
      reach.reach("TIME canonical");
      for (const values of [
        ["alpha", "beta"],
        ["alpha\nbeta", null, "NULL", "", "a,b", "a\\b", '"x"'],
        [],
        ...Array.from({ length: 24 }, () =>
          Array.from({ length: Math.floor(random() * 8) }, () =>
            random() < 0.25 ? null : `v${String(Math.floor(random() * 10))}`,
          ),
        ),
      ]) {
        const bytes = encodeArrayValue(
          { kind: "text" },
          {
            dimensions: values.length ? [values.length] : [],
            lowerBounds: values.length ? [1] : [],
            elements: values.map((value) =>
              value === null ? null : new TextEncoder().encode(value),
            ),
          },
        );
        const type: PhysicalType = { kind: "array", element: { kind: "text" } };
        const vector: FlatVector = {
          form: "flat",
          type,
          length: 1,
          validity: null,
          data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
        };
        const expected = (await pg.query<{ t: string }>("SELECT $1::text[]::text t", [values]))
          .rows[0]?.t;
        expect(ftsValue(vector, 0)).toBe(expected);
      }
      for (const [binary, source, canonical] of [
        [false, '{"x":"\\u0061","n":1.50}', '{"x":"\\u0061","n":1.50}'],
        [true, '{"text":"\\u0061","x":1.50}', '{"x": 1.50, "text": "a"}'],
      ] as const) {
        const expected = (
          await pg.query<{ t: string }>(`SELECT $1::${binary ? "jsonb" : "json"}::text t`, [source])
        ).rows[0]?.t;
        expect(expected).toBe(canonical);
        const bytes = new TextEncoder().encode(canonical);
        const vector: FlatVector = {
          form: "flat",
          type: { kind: "json", binary },
          length: 1,
          validity: null,
          data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
        };
        expect(ftsValue(vector, 0)).toBe(expected);
        const term = binary ? "50" : "u0061";
        expect(ftsMatchTruth(tokenizeQuery(term), [ftsValue(vector, 0)])).toBe(true);
        expect(shipped.ftsMatchTruth(tokenizeQuery(term), [jsonDomainValue(source, binary)])).toBe(
          false,
        );
      }
      reach.reach("JSON SQL text");
      reach.reach("JSONB SQL text");
      reach.reach("ARRAY canonical");
      reach.reach("NULL elements and escaping");
      expect(shipped.ftsMatchTruth(tokenizeQuery("500"), [timeDomainValue("10:00:00.500")])).toBe(
        true,
      );
      expect(ftsMatchTruth(tokenizeQuery("500"), [ftsTimeText(36_000_500_000n)])).toBe(false);
      expect(ftsMatchTruth(tokenizeQuery("5"), [ftsTimeText(36_000_500_000n)])).toBe(true);
      expect(
        shipped.ftsMatchTruth(tokenizeQuery("nbeta"), [arrayDomainValue(["alpha\nbeta"])]),
      ).toBe(true);
      expect(ftsMatchTruth(tokenizeQuery("nbeta"), ['{"alpha\nbeta"}'])).toBe(false);
      reach.reach("shipped unchanged");
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 60000);
