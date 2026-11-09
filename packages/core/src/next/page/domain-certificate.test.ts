/** ColumnStatistics promises exact integer bounds; unknown/prefix data is never a certificate. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Int32DomainCertificate } from "./domain-certificate.js";

it.each(seedsFor("next-int32-domain-certificate", [0xb4a1, 0xb4a2]))(
  "certifies only a complete known integer domain (seed %i)",
  (seed) => {
    const reach = createReach("int32 domain certificate", [
      "exact",
      "endpoints",
      "all-null",
      "unknown",
      "missing",
      "prefix",
      "unsafe-rows",
      "wrong-type",
    ]);
    fc.assert(
      fc.property(fc.array(fc.integer(), { minLength: 1, maxLength: 32 }), (input) => {
        reach.beginHistory();
        const values = [-2147483648, ...input, 2147483647];
        const chunk = {
          type: { kind: "int32", bits: 32 } as const,
          statistics: {
            min: Math.min(...values),
            max: Math.max(...values),
            nullCount: 0,
            dictionarySize: 1,
          },
        };
        const certificate = new Int32DomainCertificate(7, values.length, true);
        certificate.stored(values.length, chunk);
        expect(certificate.result()).toEqual({
          column: 7,
          low: -2147483648,
          high: 2147483647,
          rows: values.length,
        });
        reach.reach("exact");
        reach.reach("endpoints");
        const nullable = new Int32DomainCertificate(7, values.length + 1, true);
        nullable.stored(1, {
          ...chunk,
          statistics: { ...chunk.statistics, min: null, max: null, nullCount: 1 },
        });
        nullable.stored(values.length, chunk);
        expect(nullable.result()?.low).toBe(-2147483648);
        reach.reach("all-null");
        const unknown = new Int32DomainCertificate(7, values.length, true);
        unknown.stored(values.length, chunk);
        unknown.unknown();
        expect(
          unknown.result(),
          "unknown segments must invalidate the complete certificate",
        ).toBeUndefined();
        reach.reach("unknown");
        const missing = new Int32DomainCertificate(7, values.length, true);
        missing.stored(values.length, undefined);
        expect(missing.result()).toBeUndefined();
        reach.reach("missing");
        const prefix = new Int32DomainCertificate(7, values.length, false);
        prefix.stored(values.length, chunk);
        expect(prefix.result()).toBeUndefined();
        reach.reach("prefix");
        const unsafe = new Int32DomainCertificate(7, Number.MAX_SAFE_INTEGER + 1, true);
        unsafe.stored(values.length, chunk);
        expect(unsafe.result()).toBeUndefined();
        reach.reach("unsafe-rows");
        const wrong = new Int32DomainCertificate(7, values.length, true);
        wrong.stored(values.length, { ...chunk, type: { kind: "float64" } });
        expect(wrong.result()).toBeUndefined();
        reach.reach("wrong-type");
        const absent = new Int32DomainCertificate(7, values.length, true);
        absent.stored(values.length, { ...chunk, statistics: { ...chunk.statistics, min: null } });
        expect(absent.result()).toBeUndefined();
      }),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  },
);
