/** Replacement catalogs can reuse version numbers. Plans are scoped to the installed path. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_INSTALL_REACH } from "../../testing/reach.js";
import { harness } from "./owner-harness.js";
for (const seed of seedsFor("next-snapshot-install", [0xb2ef, 0xb2f0]))
  it(`same-version replacement never reuses old schema plans ${String(seed)}`, async () => {
    const reach = createReach("snapshot install", SNAPSHOT_INSTALL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(
          { before: "INT", beforeKind: "int32", after: "TEXT", afterKind: "text" },
          { before: "TEXT", beforeKind: "text", after: "INT", afterKind: "int32" },
          { before: "INT", beforeKind: "int32", after: "BIGINT", afterKind: "int64" },
        ),
        async (pair) => {
          reach.beginHistory();
          const old = harness(),
            replacement = harness();
          try {
            const a = await old.connect(),
              b = await replacement.connect();
            await a.query(`CREATE TABLE t (payload ${pair.before})`);
            await b.query(`CREATE TABLE t (payload ${pair.after})`);
            for (const h of [old, replacement]) {
              const stop = h.server.quiesce("closing");
              await stop.drained;
              stop.finish();
            }
            expect(old.engine.catalogVersion).toBe(replacement.engine.catalogVersion);
            const query = "SELECT payload FROM t";
            expect(old.engine.plan(query).columns[0]?.type.kind).toBe(pair.beforeKind);
            expect(old.governor.usedBytes).toBeGreaterThan(0);
            reach.reach("old cache funded");
            // No intervening governor admission can accidentally evict the warmed template.
            old.engine.installWritePath(replacement.writePath);
            expect(old.governor.usedBytes).toBe(0);
            reach.reach("old cache released");
            expect(old.engine.plan(query).columns[0]?.type.kind).toBe(pair.afterKind);
            reach.reach(pair.afterKind);
            reach.reach("same version");
          } finally {
            await old.engine.close();
            await replacement.engine.close();
            await old.writePath.close();
            expect(old.governor.usedBytes).toBe(0);
            expect(replacement.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [{ before: "INT", beforeKind: "int32", after: "TEXT", afterKind: "text" }],
          [{ before: "TEXT", beforeKind: "text", after: "INT", afterKind: "int32" }],
          [{ before: "INT", beforeKind: "int32", after: "BIGINT", afterKind: "int64" }],
        ],
      },
    );
    reach.assertReached();
  });
