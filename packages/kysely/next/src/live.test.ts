import fc from "fast-check";
import type { Result } from "../../../core/dist/next/owner/api.js";
import { CamelCasePlugin, Kysely } from "kysely";
import { expect, expectTypeOf, it } from "vitest";
import { MinnowDatabase } from "../../../core/dist/next/owner/database.js";
import { MemoryBackend } from "../../../core/dist/next/storage/memory-store.js";
import { SimLocks } from "../../../core/dist/next/storage/sim-disk.js";
import { seedsFor } from "../../../core/dist/testing/seeds.js";
import { createReach, NATIVE_KYSELY_LIVE_REACH } from "../../../core/dist/testing/reach.js";
import { NativeMinnowDialect, nativeLiveQuery } from "./index.js";

interface DB {
  personRecord: { id: number; firstName: string; amount: number };
}
for (const seed of seedsFor("next-native-kysely-live", [8894, 8895]))
  it(`delivers native Kysely plugins and keyed windows seed=${String(seed)}`, async () => {
    const reach = createReach("native Kysely live", NATIVE_KYSELY_LIVE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 50 }), async (n) => {
        reach.beginHistory();
        const native = await MinnowDatabase.open({
          backend: new MemoryBackend({ locks: new SimLocks() }),
          memoryLimitBytes: 64 << 20,
        });
        const db = new Kysely<DB>({
          dialect: new NativeMinnowDialect({
            driver: native,
            resultDecoding: { numeric: "number" },
          }),
          plugins: [new CamelCasePlugin()],
        });
        const live = nativeLiveQuery({ driver: native, db });
        try {
          await native.execute(
            "CREATE TABLE person_record(id INT PRIMARY KEY,first_name TEXT,amount NUMERIC)",
          );
          await native.execute(
            "INSERT INTO person_record VALUES(1,'before',12.3400),(2,'later',22.0000)",
          );
          const query = db.selectFrom("personRecord").selectAll().orderBy("id");
          const store = live.window(query, { key: "id", table: "person_record", limit: 1 });
          expectTypeOf(store.$inferKey).toEqualTypeOf<number>();
          const stop = store.subscribe(() => undefined);
          try {
            await store.refresh();
            expect(store.getSnapshot().rows).toEqual([
              { id: 1, firstName: "before", amount: 12.34 },
            ]);
            const first = store.getSnapshot();
            if (first.status !== "ready") throw new Error("Expected initial live result");
            expectTypeOf(first.result).toEqualTypeOf<Result | undefined>();
            expect(first.result?.rows()).toEqual([
              { id: 1, first_name: "before", amount: "12.3400" },
            ]);
            reach.reach("plugin SQL");
            reach.reach("plugin result");
            reach.reach("native decoding");
            reach.reach("window");
            await native.execute("UPDATE person_record SET first_name=$1 WHERE id=1", [String(n)]);
            const end = Date.now() + 10000;
            while (store.getSnapshot().rows[0]?.firstName !== String(n)) {
              const snapshot = store.getSnapshot();
              if (snapshot.status === "error") throw snapshot.error;
              if (Date.now() > end) throw new Error("Native Kysely update did not arrive");
              await new Promise((resolve) => setTimeout(resolve, 2));
            }
            const updated = store.getSnapshot();
            if (updated.status !== "ready") throw new Error("Expected a ready keyed update");
            expect(updated.changes).toEqual(
              expect.arrayContaining([expect.objectContaining({ type: "update" })]),
            );
            reach.reach("keyed update");
            await live.refresh();
            reach.reach("refresh");
            expect(() =>
              live.window(db.selectFrom("personRecord").selectAll(), { key: "id", limit: 1 }),
            ).toThrow(/ORDER BY/);
            expect(() => live.window(query, { key: "id", limit: 0 })).toThrow(/bounds/);
          } finally {
            stop();
            await store.close();
          }
          await live.close();
          expect(() => live(query)).toThrow(/closed/);
          expect((await native.query("SELECT 1 n")).rows()).toEqual([{ n: 1 }]);
          reach.reach("factory disposal");
        } finally {
          await live.close();
          await db.destroy();
          await native.close();
        }
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 120000);
