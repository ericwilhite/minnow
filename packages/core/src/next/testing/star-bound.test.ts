import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import { StorageCore } from "./storage-core.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STAR_BOUND_REACH } from "../../testing/reach.js";
import type { PartitionBounds } from "../exec/agg/partition-probe.js";
import { starDictionaryShape, type StarDictionaryShape } from "../exec/star-shape.js";
import { starBound, terminalStar } from "../exec/star-bound.js";

let core: StorageCore, plan: PhysicalPlan, shape: StarDictionaryShape;
beforeAll(async () => {
  core = await new StorageCore().open();
  await core.execute("CREATE TABLE f(ka TEXT,kb TEXT,kc TEXT,v DOUBLE PRECISION)");
  for (const name of ["a", "b", "c"])
    await core.execute(`CREATE TABLE ${name}(k TEXT PRIMARY KEY,t TEXT,label TEXT)`);
  plan = core.prepare(`SELECT a.t,b.t,c.label,count(*),sum(f.v) FROM f
    JOIN a ON a.k=f.ka JOIN b ON b.k=f.kb JOIN c ON c.k=f.kc
    GROUP BY a.t,b.t,c.label ORDER BY a.t,b.t,c.label LIMIT 5`).plan;
  const visit = (node: Rel): void => {
    if (node.op === "aggregate") {
      const found = starDictionaryShape(node, false);
      if (!found) throw new Error("Missing bounded star shape");
      shape = found;
    }
    if ("input" in node) visit(node.input);
  };
  visit(plan.root);
}, 60000);
afterAll(async () => core.close());

it.each(seedsFor("next-star-bound", [0xb769, 0xb76a]))(
  "certifies exact PK domains or keeps the generic recipe (seed %i)",
  (seed) => {
    const reach = createReach("star complete certificate", STAR_BOUND_REACH);
    fc.assert(
      fc.property(
        fc.constantFrom("proved", "missing", "not-PK", "wide", "many", "volatile", "alias"),
        fc.integer({ min: 1, max: 8 }),
        fc.integer({ min: 1, max: 20000 }),
        (mode, domain, rows) => {
          reach.beginHistory();
          const proofs = new Map<number, PartitionBounds>();
          for (const scan of shape.scans) {
            const keys = scan.output.filter((c) => c.type.kind === "text").length;
            proofs.set(scan.id, {
              rows,
              domains: Array.from({ length: keys }, () => (mode === "many" ? 2048 : domain)),
              groups: domain ** keys,
              groupsPerPass: Number.MAX_SAFE_INTEGER,
              passes: 1,
              keyBytes: Array.from({ length: keys }, () =>
                mode === "wide" ? (1 << 20) + 1 : 32 * domain,
              ),
              dictionaryEntries: Array.from({ length: keys }, () => domain),
              payloadBytes: [],
            });
          }
          const first = shape.scans[0];
          if (!first) throw new Error("Missing first source");
          if (mode === "missing") proofs.delete(first.id);
          const catalog = core.harness.path.catalog;
          const result = starBound(
            shape,
            mode === "not-PK"
              ? {
                  table: (id) => {
                    const table = catalog.table(id);
                    return table ? { ...table, primaryKey: null } : undefined;
                  },
                }
              : catalog,
            proofs,
            new Map(),
          );
          if (mode === "proved" || mode === "volatile" || mode === "alias") {
            expect(result).not.toBeNull();
            expect(result?.joins.size).toBe(3);
            expect(result?.books).toBeGreaterThan(0);
            expect(result?.aggregate).toBeGreaterThan(0);
            reach.reach("complete resident bounds");
          } else {
            expect(result).toBeNull();
            reach.reach(mode);
          }
          let candidate = plan;
          if (mode === "volatile") candidate = { ...plan, volatile: true };
          if (mode === "alias") {
            const type = shape.aggregate.output[0]?.type;
            const output = shape.aggregate.output[0];
            if (!type || !output) throw new Error("Missing output type");
            candidate = {
              ...plan,
              root: {
                id: 9999,
                op: "project",
                input: plan.root,
                exprs: [
                  { op: "column", index: 0, type },
                  { op: "column", index: 0, type },
                ],
                output: [output, output],
              },
            };
          }
          expect(terminalStar(candidate, shape.aggregate.id)).toBe(
            mode !== "volatile" && mode !== "alias",
          );
          if (mode === "volatile" || mode === "alias") reach.reach(mode);
        },
      ),
      {
        seed,
        numRuns: 20,
        examples: [
          ["proved", 1, 1],
          ["missing", 2, 7],
          ["not-PK", 3, 257],
          ["wide", 4, 2049],
          ["many", 5, 20000],
          ["volatile", 6, 7],
          ["alias", 8, 7],
        ],
      },
    );
    reach.assertReached();
  },
);
