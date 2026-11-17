/** Caller-sized lists preserve order and bindings without native argument expansion. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, NEXT_CALL_LIST_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { Expression } from "../../plan/model.js";
import { ParameterTypes } from "../lower/coerce.js";
import { lowerCondition } from "../lower/expressions.js";
import { textLiteral } from "../lower/literals.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import type { StoreSession } from "../storage/contract.js";
import { SimDisk } from "../storage/sim-disk.js";
import { trackReleases } from "./retirement-test-support.js";

const lengths = fc.oneof(
  fc.integer({ min: 0, max: 8192 }),
  fc.integer({ min: 150000, max: 220000 }),
);
const examples: Array<[number, boolean]> = [
  [0, false],
  [1, true],
  [180000, false],
];
const sizeState = (length: number) =>
  length === 0 ? "empty" : length > 131072 ? "large" : "ordinary";

for (const seed of seedsFor("next-non-exec-call-lists", [47411, 47412])) {
  it(`lowers caller-sized IN lists with first explicit collation seed ${String(seed)}`, () => {
    const reach = createReach("IN call lists", NEXT_CALL_LIST_REACH);
    const catalog = new MemoryCatalog().view;
    const typed = { expr: textLiteral("item"), nullable: false, big: false };
    fc.assert(
      fc.property(lengths, fc.boolean(), (length, leftCollation) => {
        reach.beginHistory();
        reach.reach(sizeState(length));
        const left: Expression = { kind: "literal", value: "left" };
        const item: Expression = { kind: "literal", value: "item" };
        const marker: Expression = { kind: "literal", value: "marker" };
        const items = Array<Expression>(length).fill(item);
        if (length > 0) items[length - 1] = marker;
        const result = lowerCondition(
          "IN",
          left,
          { kind: "list", items },
          undefined,
          {
            column: () => undefined,
            match: (expression) =>
              expression === left && leftCollation
                ? { ...typed, collation: "POSIX" }
                : expression === marker && !leftCollation
                  ? { ...typed, collation: "C" }
                  : typed,
          },
          {
            catalog,
            parameters: new ParameterTypes(0),
            clock: () => 0,
            volatile: false,
            sequences: false,
          },
        );
        if (result.expr.op !== "inList") throw new Error("Expected IN");
        expect(result.expr.list.length).toBe(length);
        expect(result.expr.collation).toBe(leftCollation ? "POSIX" : length === 0 ? null : "C");
        expect(result.expr.list.every((e) => e === typed.expr)).toBe(true);
        reach.reach("exact list");
      }),
      { seed, numRuns: 12, examples },
    );
    reach.assertReached();
  });

  it(`flushes caller-sized simulated pending queues in order seed ${String(seed)}`, () => {
    const reach = createReach("queued call lists", NEXT_CALL_LIST_REACH);
    fc.assert(
      fc.property(lengths, fc.boolean(), (length, alternate) => {
        reach.beginHistory();
        reach.reach(sizeState(length));
        const disk = new SimDisk({ seed, flush: "lying" });
        const file = disk.open("call-lists", true);
        const modulus = alternate ? 7 : 3;
        try {
          for (let i = 0; i < length; i++) file.truncate(i % modulus);
          const state = disk.files.get("call-lists");
          if (!state) throw new Error("Missing simulated file");
          const pending = state.pending;
          file.flush();
          expect(state.pending.length).toBe(0);
          expect(pending.length).toBe(length);
          expect(state.queued.length).toBe(length);
          expect(
            state.queued.findIndex((op, i) => op.kind !== "truncate" || op.size !== i % modulus),
          ).toBe(-1);
          reach.reach("exact list");
        } finally {
          file.close();
        }
      }),
      { seed, numRuns: 12, examples },
    );
    reach.assertReached();
  });

  it(`copies caller-sized release inventories including aliases seed ${String(seed)}`, async () => {
    const reach = createReach("release call lists", NEXT_CALL_LIST_REACH);
    await fc.assert(
      fc.asyncProperty(lengths, fc.boolean(), async (length, alias) => {
        reach.beginHistory();
        reach.reach(sizeState(length));
        let released = 0;
        const session = {
          releaseData: async (ranges) => {
            released += ranges.length;
          },
        } as StoreSession;
        const recorded = trackReleases(session);
        const input = Array.from({ length }, (_, i) => ({ offset: i * 8, length: (i % 7) + 1 }));
        await session.releaseData(input);
        if (alias) await session.releaseData(recorded);
        const count = alias ? 2 * length : length;
        expect(recorded.length).toBe(count);
        expect(released).toBe(length + (alias ? count : 0));
        expect(
          recorded.findIndex(
            (r, i) => r.offset !== (i % length) * 8 || r.length !== ((i % length) % 7) + 1,
          ),
        ).toBe(-1);
        if (input[0]) {
          input[0].length = 0;
          expect(recorded[0]?.length).toBe(1);
        }
        reach.reach("exact list");
      }),
      { seed, numRuns: 12, examples },
    );
    reach.assertReached();
  });
}
