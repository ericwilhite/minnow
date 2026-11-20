/** Production funding consumer, with the real graph/provider and no fixture slack reserve. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_COMMAND_FUNDED_REACH } from "../../testing/reach.js";
import { executeNativeCommand } from "../owner/native-command.js";
import { Governor, type Grant } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { orderingScript, orderingTrace } from "./trigger-ordering.test-support.js";

for (const seed of seedsFor("next-native-command-funded", [46974, 46975]))
  it(`executes funded native triggers and RI in PG order (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("funded native command", NATIVE_COMMAND_FUNDED_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (rows, update, published, pressure, cancel) => {
            reach.beginHistory();
            const history = {
                rows,
                event: update ? ("update" as const) : ("delete" as const),
                beforeNames: ["Z_before", "a_before"],
                afterNames: ["A_after", "z_after"],
                twoBodies: true,
                reverseCreation: published,
                published,
              },
              script = orderingScript(history, "accumulator"),
              core = new StorageCore(64),
              governor = new Governor(
                (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20,
              ),
              stop = new AbortController();
            Object.defineProperty(core.harness, "governor", { value: governor });
            await core.open();
            let tx: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null,
              foreign: Grant | null = null;
            let restoreSpy = () => undefined;
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p,log,audit,trace CASCADE");
              for (const text of [...script.postgresSetup, ...script.seed]) await pg.exec(text);
              for (const text of [...script.nativeSetup, ...script.seed]) await core.execute(text);
              if (published) {
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
              }
              tx = await core.harness.path.begin();
              const context = { ...core.context(), catalog: tx.catalog },
                lowered = core.lowerer.lower(script.mutation, context);
              if (lowered.kind !== "modify") throw new TypeError("Missing mutation plan");
              let entered: () => void = () => undefined;
              const waiting = new Promise<void>((resolve) => {
                entered = resolve;
              });
              if (pressure) {
                const parts = await governor.admitGroup({
                  label: "foreign result",
                  includesProgress: true,
                  grants: [
                    {
                      label: "foreign result",
                      minimumBytes: governor.capBytes - governor.usedBytes,
                      spillable: false,
                    },
                  ],
                  signal: stop.signal,
                });
                foreign = parts[0] ?? null;
                if (!foreign) throw new Error("Missing foreign result grant");
                expect(foreign.allocate(foreign.held)).toBe(true);
                const admit = governor.admitGroup.bind(governor);
                const spy = vi.spyOn(governor, "admitGroup").mockImplementation((request) => {
                  const promise = admit(request);
                  // The command's first admission that needs memory queues behind the foreign
                  // result. Since write admission (#9) that can be the writer growing its own
                  // minimum inside the root's first native step, before "native root open":
                  // either way the root holds no credit yet. Release or cancel from outside.
                  if (request.grants.some((grant) => grant.minimumBytes > 0)) entered();
                  return promise;
                });
                restoreSpy = () => {
                  spy.mockRestore();
                };
              }
              const pending = executeNativeCommand({
                transaction: tx,
                rootId: 1,
                plan: lowered.plan,
                parameters: [],
                planner: core.lowerer,
                context,
                governor,
                signal: stop.signal,
                execution: {
                  reads: {
                    open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
                  },
                  spill: core.spill,
                  queryId: 1,
                  statementTime: 0n,
                  sequences: null,
                  sequenceSession: null,
                  returning: "atExecuted",
                },
              });
              const outcome = pending.then(
                (value) => ({ value }),
                (error: unknown) => ({ error }),
              );
              if (pressure) {
                await waiting;
                if (cancel) stop.abort(new Error("cancel funded native root"));
                else {
                  foreign?.close();
                  foreign = null;
                }
              }
              const completed = await outcome;
              foreign?.close();
              foreign = null;
              if (pressure && cancel) {
                expect(completed).toHaveProperty("error", stop.signal.reason);
                reach.reach("pressure cancellation");
              } else {
                if ("error" in completed) throw completed.error;
                expect(completed.value.rowCount).toBe(rows);
                await pg.exec(script.mutation);
                reach.reach("exact root count");
                reach.reach(update ? "update RI" : "delete RI");
                if (pressure) reach.reach("pressure release");
              }
              const receipt = await tx.commit(null);
              await receipt.durable;
              tx = null;
              for (const query of [
                "SELECT * FROM p ORDER BY id",
                "SELECT * FROM c ORDER BY id",
                "SELECT label,n FROM log ORDER BY label,n",
                "SELECT label,n FROM audit ORDER BY label,n",
                script.traceSql,
              ]) {
                const actual = await core.execute(query);
                if (!("batches" in actual)) throw new Error("Missing rows");
                expect(oracleRows(actual), query).toEqual((await pg.query(query)).rows);
              }
              if (!(pressure && cancel)) {
                const trace = await core.execute(script.traceSql);
                if (!("batches" in trace)) throw new Error("Missing trace");
                expect(orderingTrace(script, oracleRows(trace) as Array<{ s: string }>)).toEqual(
                  orderingTrace(script, (await pg.query<{ s: string }>(script.traceSql)).rows),
                );
                reach.reach("nested bodies and ordered trace");
              }
              reach.reach(rows === 0 ? "empty root" : "nonempty root");
              reach.reach(published ? "published source" : "resident source");
            } finally {
              foreign?.close();
              restoreSpy();
              tx?.rollback();
              await core.close();
              expect(core.spill.bytesInUse).toBe(0);
              expect(governor.usedBytes).toBe(0);
            }
            reach.reach("zero retained ownership");
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            [0, false, false, false, false],
            [3, true, true, true, false],
            [2, false, true, true, true],
            [2, false, false, false, false],
            [1, true, false, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
