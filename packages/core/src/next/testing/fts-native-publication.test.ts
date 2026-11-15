import { need } from "../write/bytes.js";
import {
  ftsBudget as budget,
  setupFts as setup,
  ftsParts as parts,
  lastFtsCommit,
} from "./fts-transaction.test-support.js";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { Harness, autocommit, texts, batch, rules, rowsOf } from "../write/test-support.js";
import { walkFtsTree } from "../format/fts-storage.js";

// Independent fixed token fixtures, not the candidate tokenizer or renderer.
const fixtures = [
  { value: null, tokens: [] },
  { value: "", tokens: [] },
  { value: "Alpha ALPHA beta", tokens: ["alpha", "alpha", "beta"] },
  { value: "ＦＯＯ 42", tokens: ["foo", "42"] },
  { value: "\ufeffa", tokens: ["a"] },
  { value: "\u00a0a", tokens: ["a"] },
] as const;
for (const seed of seedsFor("next-fts-native-publication", [0xb843, 0xb844]))
  it(`preserves exact native FTS documents/postings through row publication ${String(seed)}`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: 0, max: fixtures.length - 1 }), { minLength: 1, maxLength: 8 }),
        async (indices) => {
          const h = new Harness();
          const b = budget();
          try {
            const path = await setup(h);
            const values = indices.map((i) => fixtures[i]?.value ?? null);
            const lsn = await autocommit(path, (s) =>
              s.table(1).insert(batch(texts(values)), null),
            );
            const { entries } = await parts(h);
            expect(entries.every((e) => e.kind === "insert")).toBe(true);
            const actual = new Map<
              number,
              { length: number; renderable: boolean; terms: Map<string, number> }
            >();
            for (const part of entries) {
              if (part.kind !== "insert") continue;
              for await (const e of walkFtsTree(part.documents.ref, b, h.space, undefined, {
                kind: "ftsDocuments",
                bounds: part.documents,
              }))
                if (e.kind === "ftsDocuments" && e.value.kind === "leaf")
                  for (let i = 0; i < e.value.ids.length; i++)
                    actual.set(need(e.value.ids[i]), {
                      length: need(e.value.lengths[i]),
                      renderable: e.value.renderable[i] === 1,
                      terms: new Map(),
                    });
              if (part.terms)
                for await (const e of walkFtsTree(part.terms.ref, b, h.space, undefined, {
                  kind: "ftsTerms",
                  bounds: part.terms,
                }))
                  if (e.kind === "ftsTerms" && e.value.kind === "leaf")
                    for (const t of e.value.entries)
                      for await (const p of walkFtsTree(t.postings.ref, b, h.space, undefined, {
                        kind: "ftsPostings",
                        bounds: t.postings,
                      }))
                        if (p.kind === "ftsPostings" && p.value.kind === "leaf")
                          for (let i = 0; i < p.value.ids.length; i++)
                            actual
                              .get(need(p.value.ids[i]))
                              ?.terms.set(t.term, need(p.value.frequencies[i]));
            }
            expect(actual.size).toBe(values.length);
            for (let i = 0; i < indices.length; i++) {
              const f = fixtures[need(indices[i])];
              if (!f) throw new Error("Missing fixture");
              const expected = new Map<string, number>();
              for (const t of f.tokens) expected.set(t, (expected.get(t) ?? 0) + 1);
              expect(actual.get(i + 1)).toEqual({
                length: f.tokens.length,
                renderable: f.value !== null,
                terms: expected,
              });
            }
            expect(b.held).toBe(0);
            await path.flush();
            await path.recycleLog();
            expect(path.state().root.globalLsn).toBeGreaterThanOrEqual(lsn);
            const saved = await parts(h);
            expect(saved.lsn).toBe(lsn);
            expect(saved.entries).toEqual(entries);
            const pin = path.pin();
            try {
              expect(await rowsOf(pin, 1)).toEqual(values.map((v) => [v]));
            } finally {
              pin.release();
            }
          } finally {
            await h.close();
          }
        },
      ),
      { seed, numRuns: 12 },
    );
  }, 60_000);

it("keeps a legal row commit when the semantic capability is missing", async () => {
  const h = new Harness();
  try {
    const path = await setup(h, false);
    await autocommit(path, (s) => s.table(1).insert(batch(texts(["kept"])), null));
    expect((await parts(h)).entries).toMatchObject([{ kind: "invalidate", ftsId: 1 }]);
    const pin = path.pin();
    try {
      expect(await rowsOf(pin, 1)).toEqual([["kept"]]);
    } finally {
      pin.release();
    }
  } finally {
    await h.close();
  }
});

it("preserves a streamed row body and crosses the 64-entry journal boundary", async () => {
  const h = new Harness();
  try {
    const path = await setup(h);
    const tx = await path.begin();
    const s = tx.statement({ rules });
    const values = Array.from(
      { length: 129 },
      (_, i) => "a" + " ".repeat(8200) + (i % 2 ? "b" : "c"),
    );
    await s.table(1).insert(batch(texts(values)), null);
    await s.end();
    expect(values.reduce((n, s) => n + s.length, 0)).toBeGreaterThan(512 * 1024);
    const receipt = await tx.commit(null);
    await receipt.durable;
    const logged = await lastFtsCommit(h);
    expect(logged.parts).toBeGreaterThan(1);
    expect(logged.lastLsn).toBe(receipt.lsn);
    expect(logged.firstLsn).toBeLessThan(receipt.lsn);
    expect(logged.body.effects.filter((e) => e.kind === "fts")).toHaveLength(1);
    expect(logged.body.effects.some((e) => e.kind === "rows")).toBe(true);
    expect((await parts(h)).entries.filter((e) => e.kind === "insert")).toHaveLength(values.length);
    await path.recycleLog();
    expect((await parts(h)).lsn).toBe(receipt.lsn);
    await h.reopen();
    expect((await parts(h)).lsn).toBe(receipt.lsn);
  } finally {
    await h.close();
  }
}, 60_000);

it("invalidates under competing all-credit pressure without waiting for fresh credit", async () => {
  const h = new Harness();
  let competitor: import("../memory/governor.js").Grant | undefined;
  try {
    const path = await setup(h);
    const tx = await path.begin(),
      s = tx.statement({ rules });
    await s.table(1).insert(batch(texts(["kept under pressure"])), null);
    await s.end();
    competitor = await h.governor.admit({
      label: "FTS competing owner",
      minimumBytes: 0,
      spillable: true,
    });
    while (competitor.tryGrow(64 * 1024)) {
      /* Exhaust ordinary growth credit. */
    }
    expect(competitor.allocate(competitor.held)).toBe(true);
    const receipt = await tx.commit(null);
    await receipt.durable;
    expect((await parts(h)).entries).toMatchObject([{ kind: "invalidate", ftsId: 1 }]);
  } finally {
    if (competitor) {
      competitor.free(competitor.allocated);
      competitor.close();
    }
    await h.close();
  }
}, 30_000);
