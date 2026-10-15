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
  to: fc.integer({ min: 100, max: 106 }),
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
              for (let from = 0; from <= 40; from += 1) {
                const stored = model.get(rowKey(table, from));
                const live = stored !== undefined && stored.expiresAt > now ? stored : undefined;
                expect(view.get(table, storageKeyToken(from))).toEqual(live);
                expect(view.stored(table, storageKeyToken(from))).toEqual(stored);
              }
              for (let to = 100; to <= 106; to += 1) {
                const expected = [...model.values()]
                  .filter((f) => f.tableId === table && f.to === to && f.expiresAt > now)
                  .map((f) => f.from)
                  .sort();
                const actual = view
                  .sourcesOf(table, storageKeyToken(to))
                  .map((f) => f.from)
                  .sort();
                expect(actual).toEqual(expected);
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
