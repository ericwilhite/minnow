/** Complete indexed-query minima fit beside native storage; waits own no producer lease. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_INDEX_PRESSURE_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { Governor, type Grant } from "../memory/governor.js";
import { rules } from "../write/test-support.js";
import { QueryCancelledError } from "../exec/errors.js";
import { yieldTask } from "../task.js";
import { operators } from "./query-index.test-support.js";
for (const seed of seedsFor("next-query-index-pressure", [47805, 47806]))
  it(`funds wide keys, IN lists and cancellable whole minima at32MiB seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("query index pressure", QUERY_INDEX_PRESSURE_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(8, 2048, 13000),
          fc.boolean(),
          fc.boolean(),
          async (width, published, cancel) => {
            reach.beginHistory();
            const core = new StorageCore(8),
              governor = new Governor(32 << 20);
            Object.assign(core.harness, { governor });
            await core.open();
            const ctx = core.context.bind(core);
            core.context = () => ({
              ...ctx(),
              statistics: { table: () => ({ rowCount: 1000000, column: () => null }) },
            });
            let tx: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null,
              blocker: Grant | undefined,
              releasePreparation: (() => void) | undefined;
            const spies: Array<{ mockRestore(): void }> = [];
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS qpress; CREATE TABLE qpress(id INT PRIMARY KEY,k TEXT UNIQUE,v TEXT)",
              );
              await core.execute("CREATE TABLE qpress(id INT PRIMARY KEY,k TEXT UNIQUE,v TEXT)");
              const key = (i: number) => String(i).padStart(5, "0") + "x".repeat(width - 5);
              for (let i = 1; i <= 24; i++) {
                const params = [i, key(i), "payload:" + String(i) + ":" + "雪".repeat(2048)];
                await core.execute("INSERT INTO qpress VALUES($1,$2,$3)", params);
                await pg.query("INSERT INTO qpress VALUES($1,$2,$3)", params);
              }
              if (published) {
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
              }
              tx = await core.harness.path.begin();
              const statement = tx.statement({ rules });
              const boundary = await statement.prepareNativeStep(0, new AbortController().signal, {
                includesProgress: true,
              });
              const floor = boundary.protectedWriteBytes;
              releasePreparation = () => boundary.release();
              // The immutable preparation remains protected until the competing-read history ends.
              const originalTry = governor.tryAdmit.bind(governor),
                originalGroup = governor.admitGroup.bind(governor);
              let funded = false,
                blockNext = false,
                observedWait = false;
              const check = (label: string, total: number) => {
                if (label.endsWith("/fixed-buffers"))
                  expect(
                    total + floor,
                    "complete query+protected writer minimum",
                  ).toBeLessThanOrEqual(governor.capBytes);
              };
              const fast = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
                check(options.label, options.minimumBytes);
                if (
                  funded &&
                  options.minimumBytes > 0 &&
                  (/query#.*\/(?:keyLookup|indexRange|index-keys|sort)#/u.test(options.label) ||
                    options.label === "write index range")
                )
                  throw Error("late cursor/operator admission while holding producer");
                if (blockNext && options.label.endsWith("/fixed-buffers")) return null;
                const result = originalTry(options);
                if (options.label.endsWith("/fixed-buffers") && result) funded = true;
                return result;
              });
              spies.push(fast);
              const controller = new AbortController();
              const groups = vi
                .spyOn(governor, "admitGroup")
                .mockImplementation(async (options) => {
                  const total = options.grants.reduce((n, p) => n + p.minimumBytes, 0);
                  check(options.label, total);
                  const block = blockNext && options.label.endsWith("/fixed-buffers");
                  if (block) {
                    blockNext = false;
                    expect(governor.loanedBytes).toBe(0);
                    [blocker] = await originalGroup({
                      label: "competing retained value",
                      includesProgress: true,
                      grants: [
                        {
                          label: "competitor",
                          minimumBytes: governor.capBytes - governor.usedBytes,
                          spillable: false,
                        },
                      ],
                    });
                    if (!blocker) throw Error("Missing competing grant");
                    expect(blocker.allocate(blocker.held)).toBe(true);
                  }
                  const pending = originalGroup(options);
                  let settled = false;
                  void pending.then(
                    () => {
                      settled = true;
                    },
                    () => {
                      settled = true;
                    },
                  );
                  if (block) {
                    try {
                      await yieldTask();
                      await yieldTask();
                      expect(settled, "must wait for competing retained value").toBe(false);
                      observedWait = true;
                      reach.reach("observed wait");
                      if (cancel) {
                        controller.abort(new Error("cancel queued query"));
                        reach.reach("cancel");
                      } else reach.reach("grant after wait");
                    } finally {
                      blocker?.close();
                      blocker = undefined;
                    }
                  }
                  const grants = await pending;
                  if (options.label.endsWith("/fixed-buffers")) funded = true;
                  return grants;
                });
              spies.push(groups);
              const rows = async (
                sql: string,
                params: ReadonlyArray<number | string | null>,
                signal?: AbortSignal,
              ) => {
                funded = false;
                const result = await core.execute(
                  sql,
                  params,
                  signal ?? AbortSignal.timeout(30000),
                );
                if (!("batches" in result)) throw Error("Rows required");
                return oracleRows(result);
              };
              try {
                for (const [sql, params] of [
                  ["SELECT id,v FROM qpress WHERE k=$1", [key(7)]],
                  ["SELECT id FROM qpress WHERE k=$1", ["z".repeat(180000)]],
                  [
                    "SELECT id,v FROM qpress WHERE k>=$1 AND k<$2 ORDER BY id LIMIT 5",
                    [key(3), key(17)],
                  ],
                ] as const)
                  expect(await rows(sql, params)).toEqual((await pg.query(sql, [...params])).rows);
                reach.reach("point");
                reach.reach("range");
                reach.reach("oversized absent key");
                const join =
                  "SELECT a.id AS aid,b.id AS bid FROM qpress a JOIN qpress b ON a.id=b.id WHERE a.k IN($1,$2) AND b.k IN($1,$2) ORDER BY a.id";
                const lowered = core.lowerer.lower(join, core.context());
                if (lowered.kind !== "query") throw Error("Joined query required");
                expect(
                  operators(lowered.plan.root).filter((op) => op === "keyLookup"),
                ).toHaveLength(2);
                expect(await rows(join, [key(7), key(13)])).toEqual(
                  (await pg.query(join, [key(7), key(13)])).rows,
                );
                reach.reach("serial index workspaces");
                const params = Array.from({ length: 192 }, (_, i) =>
                  i % 13 === 0 ? null : key(1 + (i % 24)),
                );
                const sql = `SELECT id,v FROM qpress WHERE k IN(${params.map((_, i) => `$${String(i + 1)}`).join(",")})`;
                const bag = (items: readonly unknown[]) =>
                  [...items].sort(
                    (a, b) =>
                      Number((a as Record<string, unknown>).id) -
                      Number((b as Record<string, unknown>).id),
                  );
                expect(bag(await rows(sql, params))).toEqual(
                  bag((await pg.query(sql, params)).rows),
                );
                reach.reach("large IN");
                reach.reach("duplicates and NULL");
                reach.reach(
                  width === 13000 ? "near-limit key" : width === 2048 ? "medium key" : "short key",
                );
                reach.reach(published ? "stored" : "memory");
                reach.reach("no late admission");
                reach.reach("protected minimum");
                blockNext = true;
                const waiting = rows("SELECT id FROM qpress WHERE id=$1", [7], controller.signal);
                if (cancel) await expect(waiting).rejects.toBeInstanceOf(QueryCancelledError);
                else expect(await waiting).toEqual([{ id: 7 }]);
                expect(observedWait).toBe(true);
              } finally {
                boundary.release();
                for (const spy of spies.reverse()) spy.mockRestore();
                statement.rollback();
              }
            } finally {
              blocker?.close();
              releasePreparation?.();
              tx?.rollback();
              for (const spy of spies) spy.mockRestore();
              await core.close();
              expect(governor.usedBytes).toBe(0);
              expect(governor.loanedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero ownership");
            }
          },
        ),
        {
          seed,
          numRuns: 6,
          endOnFailure: true,
          examples: [
            [8, false, false],
            [2048, true, true],
            [13000, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 180000);
