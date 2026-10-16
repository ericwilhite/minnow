/**
 * The layered forwards mirror against a plain map: whatever sequence of commit windows builds a
 * generation — writes, re-targets, deletes, folds into a new base — it answers every lookup,
 * source query, expiry count, and diff exactly as a generation read whole from the table would.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { KEY_FORWARDS_RECORD_ID } from "../storage/types.js";
import {
  CommittedKeyForwards,
  KEY_FORWARDS_TABLE_ID,
  storageKeyToken,
  type KeyForward,
  type KeyForwardChange,
} from "./key-forwards.js";

const TABLES = ["a", "b"];

function forward(tableId: string, from: number, to: number, expiresAt: number): KeyForward {
  return { tableId, from, to, fromValues: [from], toValues: [to], expiresAt };
}

const rowKey = (tableId: string, from: number): string => `${tableId}:${String(from)}`;

const change = fc.record({
  table: fc.constantFrom(...TABLES),
  from: fc.integer({ min: 0, max: 40 }),
  // Targets overlap retired keys, so forwards chain: 3 → 7 and then 7 → 12.
  to: fc.integer({ min: 0, max: 45 }),
  expiresAt: fc.integer({ min: 0, max: 20 }),
  deleted: fc.boolean(),
});

describe("layered key forwards", () => {
  it("answers like a whole read after any sequence of commit windows", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.array(change, { maxLength: 30 }), { minLength: 1, maxLength: 40 }),
        fc.integer({ min: 0, max: 21 }),
        async (windows, now) => {
          const clock = (): number => now;
          const model = new Map<string, KeyForward>();
          let generation = await CommittedKeyForwards.load("g0", [], clock);
          const history = [generation];
          for (const [index, window] of windows.entries()) {
            // One change per row key per window, as a commit window's segments report it.
            const byRow = new Map<string, (typeof window)[number]>();
            for (const entry of window) byRow.set(rowKey(entry.table, entry.from), entry);
            const changes: KeyForwardChange[] = [];
            for (const entry of byRow.values()) {
              const written = entry.deleted
                ? null
                : forward(entry.table, entry.from, entry.to, entry.expiresAt);
              if (written === null) model.delete(rowKey(entry.table, entry.from));
              else model.set(rowKey(entry.table, entry.from), written);
              changes.push({
                tableId: entry.table,
                token: storageKeyToken(entry.from),
                forward: written,
              });
            }
            generation = await generation.next(`g${String(index + 1)}`, changes);
            history.push(generation);
          }
          const whole = await CommittedKeyForwards.load("whole", model.values(), clock);
          for (const view of [generation, whole]) {
            expect(
              new Set([...view.entries()].map((f) => rowKey(f.tableId, Number(f.from)))),
            ).toEqual(new Set(model.keys()));
            expect(view.expiredCount()).toBe(
              [...model.values()].filter((f) => f.expiresAt <= now).length,
            );
            for (const table of TABLES) {
              const liveStep = (from: number): KeyForward | undefined => {
                const stored = model.get(rowKey(table, from));
                return stored !== undefined && stored.expiresAt > now ? stored : undefined;
              };
              for (let from = 0; from <= 45; from += 1) {
                expect(view.stored(table, storageKeyToken(from))).toEqual(
                  model.get(rowKey(table, from)),
                );
                expect(view.hop(table, storageKeyToken(from))).toEqual(liveStep(from));
                // The chain, followed in the model: live steps until none, or a key seen twice.
                const first = liveStep(from);
                let expected: KeyForward | undefined = first;
                if (first !== undefined) {
                  const seen = new Set([from]);
                  let last = first;
                  let expiresAt = first.expiresAt;
                  for (;;) {
                    const at = Number(last.to);
                    const next = liveStep(at);
                    if (next === undefined || seen.has(at)) break;
                    seen.add(at);
                    last = next;
                    expiresAt = Math.min(expiresAt, next.expiresAt);
                  }
                  if (last !== first) {
                    expected = { ...first, to: last.to, toValues: last.toValues, expiresAt };
                  }
                }
                expect(view.get(table, storageKeyToken(from))).toEqual(expected);
              }
            }
          }
          // A diff walked through the layers names exactly the forwards that differ from the
          // older generation and are still stored.
          const prior = history[Math.max(0, history.length - 6)];
          if (prior !== undefined) {
            const written = generation.writtenSince(prior);
            if (written !== undefined) {
              const changed = [...generation.entries()].filter((f) => {
                const before = prior.stored(f.tableId, storageKeyToken(f.from));
                return before !== f;
              });
              expect(new Set(written)).toEqual(new Set(changed));
            }
          }
        },
      ),
      { numRuns: 200 },
    );
  });

  it("folds its layers into a new base and keeps its diff history bounded", async () => {
    let generation = await CommittedKeyForwards.load("g0", [], () => 0);
    const first = generation;
    for (let index = 0; index < 40; index += 1) {
      generation = await generation.next(`g${String(index + 1)}`, [
        {
          tableId: "a",
          token: storageKeyToken(index),
          forward: forward("a", index, 1000 + index, 1),
        },
      ]);
    }
    expect([...generation.entries()]).toHaveLength(40);
    expect(generation.get("a", storageKeyToken(3))?.to).toBe(1003);
    // Forty generations on, the first is past the walkable history: the caller compares whole.
    expect(generation.writtenSince(first)).toBeUndefined();
  });

  it("names the internal table by the ID every store reserves for it", () => {
    expect(KEY_FORWARDS_TABLE_ID).toBe(KEY_FORWARDS_RECORD_ID);
  });
});
