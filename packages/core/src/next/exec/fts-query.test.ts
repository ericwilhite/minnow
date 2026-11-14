/** A literal/owned parameter query is immutable for one native execution. Its cache owns
 * bounded term strings, never one full decoded query per MATCH node. */
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, FTS_QUERY_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { tokenizeQuery } from "../full-text.js";
import { compileExpr } from "./kernels/compile.js";
import { UnsupportedPlanError } from "./errors.js";
import { FtsMatchNode, queryTerms } from "./kernels/fts.js";
import { OutputArena, type Compiled } from "./kernels/vectors.js";
import { FTS_WORKSPACE_BYTES } from "./fts-value.js";
import { TEXT, vectorOf, batchesOf } from "./join-fixtures.js";

for (const seed of seedsFor("next-fts-query", [0xb803, 0xb804]))
  it(`keeps constant query caches bounded and exact (${String(seed)})`, async () => {
    const random = mulberry32(seed),
      reach = createReach(`FTS query ${String(seed)}`, FTS_QUERY_REACH);
    for (const word of ["alpha", "a".repeat(31) + "𐐀b"]) {
      reach.beginHistory();
      const source = word + (random() < 0.5 ? "*" : "");
      const query = " ".repeat(16384 - source.length) + source;
      const terms = queryTerms(query);
      expect(terms).toEqual(tokenizeQuery(query));
      expect(terms.every((term) => term.term.length <= 32)).toBe(true);
      reach.reach("bounded terms");
      if (word !== "alpha") {
        expect(terms[0]?.term.charCodeAt(31)).toBe(0xd801);
        reach.reach("UTF16 truncation");
      }
      const governor = new Governor(32 << 20);
      const grant = await governor.admit({
        label: "fts-query",
        minimumBytes: FTS_WORKSPACE_BYTES + 12 * 9000,
        spillable: false,
      });
      const arena = new OutputArena(grant, 32);
      const queryVector = vectorOf(TEXT, [query]);
      try {
        for (let nodeIndex = 0; nodeIndex < 8; nodeIndex++) {
          let evaluations = 0;
          const arg: Compiled = {
            type: TEXT,
            evaluate: () => {
              evaluations++;
              return queryVector;
            },
          };
          const doc: Compiled = {
            type: TEXT,
            evaluate: (batch) => {
              const column = batch.columns[0];
              if (!column) throw Error("document");
              return column;
            },
          };
          const before = grant.allocated;
          const node = new FtsMatchNode([arg, doc], arena);
          // Separate from the shared tokenizer workspace: each node must fund retained terms.
          expect(grant.allocated - before).toBeGreaterThanOrEqual(8192);
          reach.reach("charged metadata");
          for (let round = 0; round < 2; round++)
            for (const batch of batchesOf([TEXT], [[word], ["beta"], [null]])) {
              const out = node.evaluate(batch, null);
              if (out.form !== "flat" || out.data.layout !== "bool") throw Error("MATCH output");
              expect([...out.data.values.subarray(0, 2)]).toEqual([1, 0]);
              expect((out.validity?.[0] ?? 0) & 7).toBe(3);
            }
          expect(evaluations).toBe(1);
          reach.reach("constant parsed once");
        }
        expect(() =>
          compileExpr(
            {
              op: "call",
              fn: "fts_match",
              type: { kind: "bool" },
              args: [
                { op: "column", index: 0, type: TEXT },
                { op: "column", index: 1, type: TEXT },
              ],
            },
            { arena, input: [TEXT, TEXT], params: { slots: [] } },
          ),
        ).toThrow(UnsupportedPlanError);
        reach.reach("static query source");
      } finally {
        arena.close();
        grant.close();
      }
      expect(governor.usedBytes).toBe(0);
      reach.reach("drained");
    }
    reach.assertReached();
  });
