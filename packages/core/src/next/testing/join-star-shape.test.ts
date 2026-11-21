/** The lookup certificate must not discard any native equality or transition. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, B9_STAR_SHAPE_REACH, B9_STAR_BACKING_REACH } from "../../testing/reach.js";
import type { Rel } from "../ir/ir.js";
import { StorageCore } from "./storage-core.js";
import { starDictionaryShape } from "../exec/star-shape.js";
import { starLookupShape } from "../exec/join-star-shape.js";
import { StarLookups } from "../exec/join-star-lookups.js";
import { StarBuckets } from "../exec/join-star-buckets.js";
import { Governor } from "../memory/governor.js";
import { BATCH_ROWS } from "../vector/vector.js";
type Aggregate = Extract<Rel, { op: "aggregate" }>;
function aggregate(node: Rel): Aggregate {
  if (node.op === "aggregate") return node;
  if ("input" in node) return aggregate(node.input);
  throw new Error("Missing star aggregate");
}
function composite(node: Rel): Rel {
  if (node.op === "join")
    return {
      ...node,
      leftKeys: [...node.leftKeys, ...node.leftKeys],
      rightKeys: [...node.rightKeys, ...node.rightKeys],
    };
  if (node.op === "project") return { ...node, input: composite(node.input) };
  throw new Error("Missing star join");
}
it.each(seedsFor("b9-star-shape", [0xb9a27, 0xb9a28]))(
  "single-key, transition and join barriers (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      reach = createReach("B9 star shape", B9_STAR_SHAPE_REACH);
    try {
      await core.execute(
        "CREATE TABLE sf(id INTEGER PRIMARY KEY,a TEXT,b TEXT,c TEXT,v DOUBLE PRECISION)",
      );
      for (const name of ["sa", "sb", "sc"])
        await core.execute(
          `CREATE TABLE ${name}(k TEXT PRIMARY KEY,tier TEXT,label TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
        );
      const lowered = core.lowerer.lower(
        "SELECT a.tier AS tier_a,b.tier AS tier_b,c.label AS label_c,COUNT(*) AS n,SUM(f.v) AS s FROM sf f JOIN sa a ON a.k=f.a JOIN sb b ON b.k=f.b JOIN sc c ON c.k=f.c WHERE a.active AND b.weight>1 AND c.tier='t1' GROUP BY a.tier,b.tier,c.label ORDER BY s DESC,tier_a,tier_b,label_c LIMIT 50",
        core.context(),
      );
      if (lowered.kind !== "query") throw new Error("Missing query");
      const original = aggregate(lowered.plan.root),
        shape = starDictionaryShape(original, false);
      if (!shape) throw new Error("Missing dictionary recipe");
      expect(starLookupShape(shape)).not.toBeNull();
      const variants = [
        "composite keys",
        "AVG transition",
        "additional SUM",
        "ordered aggregate",
        "volatile plan",
      ] as const;
      await fc.assert(
        fc.asyncProperty(fc.constantFrom(...variants), async (variant) => {
          reach.beginHistory();
          let changed = original;
          if (variant === "composite keys")
            changed = { ...original, input: composite(original.input) };
          if (variant === "AVG transition")
            changed = {
              ...original,
              aggregates: original.aggregates.map((a) =>
                a.fn === "sum" ? { ...a, fn: "avg" } : a,
              ),
            };
          if (variant === "additional SUM")
            changed = {
              ...original,
              aggregates: [
                ...original.aggregates,
                ...original.aggregates.filter((a) => a.fn === "sum"),
              ],
            };
          if (variant === "ordered aggregate")
            changed = {
              ...original,
              aggregates: original.aggregates.map((a) =>
                a.fn === "sum" && a.args[0]
                  ? {
                      ...a,
                      orderBy: [
                        { expr: a.args[0], direction: "asc", nulls: "last", collation: null },
                      ],
                    }
                  : a,
              ),
            };
          const dictionary = starDictionaryShape(changed, variant === "volatile plan");
          expect(dictionary === null ? null : starLookupShape(dictionary)).toBeNull();
          reach.reach(variant);
        }),
        { seed, numRuns: 12, examples: variants.map((v) => [v]), endOnFailure: true },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  },
  60000,
);

it.each(seedsFor("b9-star-buffer-accounting", [0xb9f95, 0xb9f96]))(
  "independent constructor census covers staged key sidecars before allocation (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      reach = createReach("B9 star backing", B9_STAR_BACKING_REACH);
    try {
      await core.execute(
        "CREATE TABLE sf(id INTEGER PRIMARY KEY,a TEXT,b TEXT,c TEXT,v DOUBLE PRECISION)",
      );
      for (const name of ["sa", "sb", "sc"])
        await core.execute(
          `CREATE TABLE ${name}(k TEXT PRIMARY KEY,tier TEXT,label TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
        );
      const lowered = core.lowerer.lower(
        "SELECT a.tier AS tier_a,b.tier AS tier_b,c.label AS label_c,COUNT(*) AS n,SUM(f.v) AS s FROM sf f JOIN sa a ON a.k=f.a JOIN sb b ON b.k=f.b JOIN sc c ON c.k=f.c WHERE a.active AND b.weight>1 AND c.tier='t1' GROUP BY a.tier,b.tier,c.label ORDER BY s DESC,tier_a,tier_b,label_c LIMIT 50",
        core.context(),
      );
      if (lowered.kind !== "query") throw Error("No star query");
      const dictionary = starDictionaryShape(aggregate(lowered.plan.root), false),
        lookup = dictionary && starLookupShape(dictionary);
      if (!lookup?.joins[0]) throw Error("No actual star shape");
      const owner = lookup.joins[0];
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 4097, max: 65536 }),
          fc.integer({ min: 1, max: 16 }),
          async (facts, domain) => {
            reach.beginHistory();
            const buckets = StarBuckets.bytes(facts, domain, domain);
            if (buckets === null) throw Error("Small exact domain refused");
            const actualExpected =
                buckets -
                256 +
                8 * facts +
                4 * (4 * domain) +
                20 * BATCH_ROWS +
                4 * 4 * (BATCH_ROWS / 32),
              minimum = actualExpected + (192 << 10) + 256,
              governor = new Governor(8 << 20),
              grant = await governor.admit({
                label: "star constructor",
                minimumBytes: minimum,
                spillable: false,
              }),
              restores: Array<() => void> = [],
              seen = new WeakSet();
            let actual = 0,
              constructors = 0,
              state: StarLookups | undefined;
            try {
              for (const name of [
                "Int32Array",
                "Uint32Array",
                "Float64Array",
                "Uint8Array",
              ] as const) {
                const descriptor = Object.getOwnPropertyDescriptor(globalThis, name);
                if (!descriptor || typeof descriptor.value !== "function")
                  throw Error(`Missing ${name}`);
                const Type = descriptor.value as new (...args: unknown[]) => ArrayBufferView;
                Object.defineProperty(globalThis, name, {
                  ...descriptor,
                  value: new Proxy(Type, {
                    construct(target, args, newTarget) {
                      const array = Reflect.construct(target, args, newTarget) as ArrayBufferView;
                      if (!seen.has(array.buffer)) {
                        seen.add(array.buffer);
                        actual += array.buffer.byteLength;
                        constructors++;
                        expect(
                          actual,
                          "actual backing requires preceding owner charge",
                        ).toBeLessThanOrEqual(grant.allocated);
                      }
                      return array;
                    },
                  }),
                });
                restores.push(() => Object.defineProperty(globalThis, name, descriptor));
              }
              state = new StarLookups(
                lookup,
                {
                  facts,
                  a: domain,
                  b: domain,
                  domains: [domain, domain, domain],
                  bytes: minimum,
                  owner,
                  minimum,
                },
                grant,
                new AbortController().signal,
              );
              expect(constructors).toBeGreaterThanOrEqual(18);
              // Tiny constant-null vectors use the fixed owner envelope. The large
              // sidecars and buckets must be represented in the independent census.
              expect(actual).toBeGreaterThanOrEqual(actualExpected);
              expect(actual).toBeLessThan(actualExpected + (16 << 10));
              expect(grant.allocated).toBe(actualExpected + (128 << 10) + 256);
              reach.reach("actual backing independent of charges");
              reach.reach("A and B sidecars");
              reach.reach("8 MiB cap");
              if (facts > BATCH_ROWS) reach.reach("multiple fact batches");
              if (domain > 1) reach.reach("matrix domain");
            } finally {
              for (const restore of restores.reverse()) restore();
              state?.close();
              expect(grant.allocated).toBe(0);
              grant.close();
              expect(governor.usedBytes).toBe(0);
              reach.reach("all credit returned");
            }
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [65536, 16],
            [4097, 1],
          ],
          endOnFailure: true,
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
  },
  60000,
);
