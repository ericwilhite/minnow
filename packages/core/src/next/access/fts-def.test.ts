/** Approved native semantics are token1/render2/scorer1/config1; IDs are exact positiveu53. */
import { expect, it } from "vitest";
import fc from "fast-check";
import { createReach, FTS_DEFINITION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { DurableFormatError } from "../durable-format.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { ftsDef, validateFtsDef } from "./fts-def.js";

const id = fc.oneof(
  fc.integer({ min: 1, max: 1_000_000 }),
  fc.integer({ min: 0, max: 1024 }).map((n) => Number.MAX_SAFE_INTEGER - n),
);
for (const seed of seedsFor("next-fts-definition", [0xb815, 0xb816]))
  it(`keeps exact definitions and refuses unsupported semantics (${String(seed)})`, () => {
    const reach = createReach("FTS definitions", FTS_DEFINITION_REACH);
    fc.assert(
      fc.property(id, id, id, (index, table, column) => {
        reach.beginHistory();
        const expected = {
          id: index,
          tableId: table,
          columnId: column,
          tokenizerVersion: 1,
          rendererVersion: 2,
          scorerVersion: 1,
          config: { version: 1 },
        };
        const definition = ftsDef(index, table, column);
        expect(definition).toEqual(expected);
        expect(() => validateFtsDef(expected)).not.toThrow();
        reach.reach("exact definition");
        if (Math.max(index, table, column) > 2 ** 52) reach.reach("large IDs");
        for (const [field, value] of [
          ["tokenizerVersion", 2],
          ["rendererVersion", 1],
          ["scorerVersion", 2],
        ] as const) {
          expect(() => validateFtsDef({ ...expected, [field]: value })).toThrow(DurableFormatError);
          reach.reach(field);
        }
        expect(() => validateFtsDef({ ...expected, config: { version: 2 } })).toThrow(
          DurableFormatError,
        );
        reach.reach("config version");
        for (const invalid of [0, -0, -1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1])
          for (const field of ["id", "tableId", "columnId"])
            expect(() => validateFtsDef({ ...expected, [field]: invalid })).toThrow(
              StoreCorruptionError,
            );
        reach.reach("invalid IDs");
        for (const physical of ["root", "delta", "lease", "state", "coverage"])
          expect(() => validateFtsDef({ ...expected, [physical]: null })).toThrow(
            StoreCorruptionError,
          );
        reach.reach("physical state excluded");
        expect(() => validateFtsDef({ ...expected, config: { version: 1, tokens: 1 } })).toThrow(
          StoreCorruptionError,
        );
        expect(() => validateFtsDef({ ...expected, config: null })).toThrow(StoreCorruptionError);
        reach.reach("fixed config");
      }),
      { seed, numRuns: 200 },
    );
    reach.assertReached();
  });
