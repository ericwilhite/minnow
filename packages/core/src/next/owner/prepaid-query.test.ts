/** Law: the body host shares one root admission; every query part remains charged and
 * no producer retains its minimum across a second admission wait. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PREPAID_QUERY_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { PrepaidQueryGovernor } from "./prepaid-query.js";
function grant(gov: Governor, label: string, bytes: number): Grant {
  const g = gov.tryAdmit({ label, minimumBytes: bytes, spillable: true });
  if (!g) throw new Error("fixture admission");
  return g;
}
for (const seed of seedsFor("next-prepaid-query", [0xb655, 0xb656]))
  it(`splits complete root credit without a second governor seed ${String(seed)}`, async () => {
    const reach = createReach("prepaid body query", PREPAID_QUERY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc
          .integer({ min: 1, max: 30 })
          .chain((length) =>
            fc.array(fc.integer({ min: 0, max: 65536 }), { minLength: length, maxLength: length }),
          ),
        fc.boolean(),
        async (sizes, keepRecipe) => {
          reach.beginHistory();
          const native = new Governor(16 << 20),
            metadata = grant(native, "root metadata", 1 << 20),
            source = grant(native, "root source", 0),
            stop = new AbortController();
          let preparations = 0;
          const query = new PrepaidQueryGovernor({
            governor: native,
            source,
            metadata,
            signal: stop.signal,
            prepare: async (bytes, signal) => {
              preparations++;
              expect(source.allocated).toBe(0);
              if (!(await source.makeRoom?.(bytes, signal))) throw new Error("source room");
            },
            prepareMetadata: async (bytes, signal) => {
              if (!(await metadata.makeRoom?.(bytes, signal))) throw new Error("metadata room");
            },
          });
          const recipe = keepRecipe
            ? await query.admit({ label: "resident recipe", minimumBytes: 4096, spillable: false })
            : null;
          try {
            if (recipe) {
              expect(recipe.allocate(4096)).toBe(true);
              reach.reach("resident metadata");
            } else reach.reach("no recipe");
            expect(
              query.tryAdmit({
                label: "synchronous group control",
                minimumBytes: 1,
                spillable: false,
              }),
            ).toBe(null);
            const parts = await query.admitGroup({
              label: "whole body input",
              includesProgress: true,
              grants: sizes.map((minimumBytes, i) => ({
                label: String(i),
                minimumBytes,
                spillable: true,
              })),
            });
            expect(source.allocated).toBe(sizes.reduce((a, b) => a + b, 0));
            expect(parts.length).toBe(sizes.length);
            for (const [i, part] of parts.entries()) {
              expect(part.allocate(sizes[i] ?? 0)).toBe(true);
              expect(part.allocate(1)).toBe(false);
            }
            const blocked = new AbortController();
            if (sizes.some((n) => n > 0))
              await expect(
                query.admitGroup({
                  label: "wrong nested admission",
                  grants: [{ label: "more", minimumBytes: 15 << 20, spillable: false }],
                  signal: blocked.signal,
                  includesProgress: true,
                }),
              ).rejects.toThrow("retire before admission");
            for (const [i, part] of parts.entries()) {
              part.free(sizes[i] ?? 0);
              part.close();
              part.close();
            }
            expect(source.allocated).toBe(0);
            expect(preparations).toBe(sizes.some((n) => n > 0) ? 1 : 0);
            const pair = await query.admitGroup({
                label: "next phase",
                grants: [
                  { label: "a", minimumBytes: 1024, spillable: true },
                  { label: "b", minimumBytes: 0, spillable: true },
                ],
                includesProgress: true,
              }),
              a = pair[0],
              b = pair[1];
            if (!a || !b) throw new Error("pair");
            expect(query.transferCredit(a, b, 256)).toBe(true);
            expect([a.held, b.held]).toEqual([768, 256]);
            const loan = await b.borrow(128);
            expect(b.allocate(128)).toBe(true);
            expect(() => loan.repay()).toThrow("still allocated");
            b.free(128);
            loan.repay();
            expect(a.tryGrow(512)).toBe(true);
            a.close();
            b.close();
            const cache = query.registerCache({ evictableBytes: 0, evict: () => 0 });
            expect(cache.charge(1024)).toBe(false);
            expect(cache.takeFrom?.(source, 1)).toBe(false);
            cache.unregister?.();
            cache.unregister?.();
            reach.reach(sizes.length > 15 ? "many parts" : "few parts");
            reach.reach("loan");
            reach.reach("idle transfer");
            reach.reach("no hold and wait");
          } finally {
            recipe?.close();
            await query.close();
            metadata.close();
            source.close();
          }
          expect(native.usedBytes).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });
for (const seed of seedsFor("next-prepaid-query-cancel", [0xb657, 0xb658]))
  it(`fences root admission cancellation seed ${String(seed)}`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 1, max: 256 }), async (close, extra) => {
        const native = new Governor(16 << 20),
          metadata = grant(native, "root metadata", 65536),
          source = grant(native, "root source", 0),
          competing = grant(native, "competing", 10 << 20),
          stop = new AbortController(),
          reason = new Error("stop query scope");
        let enter!: () => void;
        const entered = new Promise<void>((resolve) => {
          enter = resolve;
        });
        const query = new PrepaidQueryGovernor({
            governor: native,
            metadata,
            source,
            signal: stop.signal,
            prepareMetadata: async () => undefined,
            prepare: async (bytes, signal) => {
              enter();
              await source.makeRoom?.(bytes, signal);
            },
          }),
          pending = query.admitGroup({
            label: "blocked source",
            grants: [{ label: "source", minimumBytes: (8 << 20) + extra, spillable: false }],
            signal: stop.signal,
            includesProgress: true,
          });
        void pending.catch(() => undefined);
        await entered;
        if (close) await query.close();
        else stop.abort(reason);
        await expect(pending).rejects.toBeInstanceOf(Error);
        await query.close();
        source.close();
        metadata.close();
        competing.close();
        expect(native.usedBytes).toBe(0);
      }),
      { seed, numRuns: 16 },
    );
  });
