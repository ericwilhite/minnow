/** Column-only bindings reuse the shared validator and cannot replace expression rules. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, COLUMN_RULE_BINDING_REACH } from "../../testing/reach.js";
import { bindColumnRules, columnRuleMinimumBytes } from "../exec/row-rules.js";
import { StorageCore } from "./storage-core.js";
import { batch, ints } from "../write/test-support.js";
it.each(seedsFor("next-column-rule-binding", [0xb5fb]))(
  "funds complete column rules without hiding CHECK/generated IR seed %i",
  async (seed) => {
    const reach = createReach("column rule binding", COLUMN_RULE_BINDING_REACH),
      core = await new StorageCore().open();
    try {
      await fc.assert(
        fc.asyncProperty(fc.constantFrom("columns", "check", "generated"), async (kind) => {
          reach.beginHistory();
          await core.execute("DROP TABLE IF EXISTS column_rule");
          await core.execute(
            `CREATE TABLE column_rule(id INTEGER PRIMARY KEY,n INTEGER NOT NULL${kind === "check" ? " CHECK(n>0)" : ""}${kind === "generated" ? ",g INTEGER GENERATED ALWAYS AS(n+1) STORED" : ""})`,
          );
          const table = core.harness.path.catalog.tableByName("column_rule");
          if (!table) throw new Error("Missing column-rule table");
          if (kind !== "columns") {
            expect(() => columnRuleMinimumBytes(table, 1)).toThrow(
              "cannot replace lowered CHECK/generated",
            );
            const grant = await core.harness.governor.admit({
              label: "column rule rejection",
              minimumBytes: 4096,
              spillable: false,
            });
            try {
              expect(() => bindColumnRules(table, grant, { capacity: 1 })).toThrow(
                "cannot replace lowered CHECK/generated",
              );
              expect(grant.allocated).toBe(0);
            } finally {
              grant.close();
            }
            reach.reach(kind === "check" ? "CHECK refusal" : "generated refusal");
          } else {
            const minimum = columnRuleMinimumBytes(table, 1),
              grant = await core.harness.governor.admit({
                label: "column rule binding",
                minimumBytes: minimum,
                spillable: false,
              });
            const rules = bindColumnRules(table, grant, { capacity: 1 });
            try {
              expect(grant.allocated).toBeGreaterThan(0);
              expect(grant.allocated).toBeLessThanOrEqual(minimum);
              expect(() => rules.validate?.(batch(ints([1]), ints([null])), null)).toThrow(
                expect.objectContaining({ sqlState: "23502", columnId: table.columns[1]?.id }),
              );
              rules.validate?.(batch(ints([1]), ints([1])), null);
              reach.reach("minimum covers binding");
              reach.reach("complete NOT NULL validator");
            } finally {
              rules.close();
              expect(grant.allocated).toBe(0);
              grant.close();
            }
          }
          reach.reach("zero charge");
        }),
        { seed, numRuns: 8, examples: [["columns"], ["check"], ["generated"]] },
      );
      reach.assertReached();
    } finally {
      await core.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);
