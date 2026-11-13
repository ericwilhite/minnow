/** End-to-end wide group keys cannot retain one loan while requesting their output step. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, JSON_GROUP_SQL_PRESSURE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});

it.each(seedsFor("next-json-group-key-sql-pressure", [0xb730, 0xb731]))(
  "spills wide SQL group representatives under competing credit (seed %i)",
  async (seed) => {
    const reach = createReach<string>("wide SQL JSON groups", JSON_GROUP_SQL_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2300000, max: 2400000 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (width, binary, delivery, expansion) => {
          reach.beginHistory();
          const text =
            binary && expansion ? "" : "x".repeat(binary ? Math.floor(width / 4) : width);
          const values = binary
            ? expansion
              ? ["1e80000", "10e79999"]
              : [`{"s":"${text}","n":1.0}`, `{"s":"${text}","n":1.00}`]
            : [text, text];
          // Exercise supported inputs: literal VALUES for the original TEXT
          // shape; stored rows for equivalent JSONB spellings. Parameterized
          // VALUES/UNION ALL and wide CASE admission are separate existing limits.
          const sql = binary
            ? "SELECT json_agg(n ORDER BY n)::text j FROM wide_json_group_keys GROUP BY doc::jsonb"
            : "SELECT json_agg(n)::text j FROM(VALUES(1),(2))t(n) GROUP BY $1::text";
          const bindings = binary ? [] : [text];
          const core = await new StorageCore().open(),
            governor = new Governor((binary ? 512 : 128) << 20);
          let blocker: Grant | undefined,
            blocked = false;
          let aggregateId: number | null = null;
          const admitted = governor.admitGroup.bind(governor);
          const controlled = new Proxy(governor, {
            get(target, property) {
              // The synchronous whole-minimum shortcut does not call admitGroup.
              // Force its equivalent async route so the competitor owns ordinary
              // credit DURING the producer, rather than only during final delivery.
              if (property === "tryAdmit")
                return (options: Parameters<Governor["tryAdmit"]>[0]) =>
                  !delivery && options.label.endsWith("/fixed-buffers")
                    ? null
                    : target.tryAdmit(options);
              if (property === "admitGroup")
                return async (options: Parameters<Governor["admitGroup"]>[0]) => {
                  // Occupy ordinary headroom only during the initial producer. The subsequent
                  // complete handoff admission must be allowed to reclaim that credit.
                  if (options.label.endsWith("/fixed-buffers") && blocker) {
                    blocker.close();
                    blocker = undefined;
                  }
                  const grants = await admitted(options);
                  const aggregate = options.grants.some(
                    (g) => /\/aggregate#\d+\/minimum$/u.test(g.label) && g.minimumBytes > 0,
                  );
                  const result =
                    options.grants.find((g) => g.label.endsWith("/result/minimum"))?.minimumBytes ??
                    0;
                  if (
                    !blocked &&
                    options.label.endsWith("/fixed-buffers") &&
                    (delivery
                      ? !aggregate &&
                        options.grants.some(
                          (g) =>
                            g.label.endsWith(`/project#${String(aggregateId)}/minimum`) &&
                            g.minimumBytes > 0,
                        ) &&
                        result > 0
                      : aggregate)
                  ) {
                    blocker = await governor.admit({
                      label: "competing group-key credit",
                      minimumBytes: 0,
                      spillable: true,
                    });
                    const remaining =
                      governor.capBytes - governor.reserveBytes - governor.usedBytes;
                    expect(remaining).toBeGreaterThan(0);
                    expect(blocker.allocate(remaining)).toBe(true);
                    blocked = true;
                  }
                  return grants;
                };
              const result: unknown = Reflect.get(target, property, target);
              return typeof result === "function"
                ? (result as (...args: unknown[]) => unknown).bind(target)
                : result;
            },
          });
          try {
            if (binary) {
              await core.execute("CREATE TABLE wide_json_group_keys(n INTEGER,doc TEXT)");
              await core.writeBatch(
                "insertBatch",
                "wide_json_group_keys",
                values.map((doc, i) => ({ n: i + 1, doc })),
              );
              await pg.exec(
                "DROP TABLE IF EXISTS wide_json_group_keys;CREATE TABLE wide_json_group_keys(n INTEGER,doc TEXT)",
              );
              for (const [i, doc] of values.entries())
                await pg.query("INSERT INTO wide_json_group_keys VALUES($1,$2)", [i + 1, doc]);
            }
            const expected = (await pg.query<{ j: string }>(sql, bindings)).rows;
            const lowered = core.lowerer.lower(sql, core.context());
            if (
              lowered.kind !== "query" ||
              lowered.plan.root.op !== "project" ||
              lowered.plan.root.input.op !== "aggregate"
            )
              throw new Error("Expected a single JSON aggregate below its result projection");
            aggregateId = lowered.plan.root.input.id;
            const prepared = core.prepare(sql),
              run = core.start(
                {
                  ...prepared,
                  execute: (request) => prepared.execute({ ...request, governor: controlled }),
                },
                bindings,
              );
            const actual: Array<{ j: string }> = [];
            try {
              for (;;) {
                const frame = await run.execution.results.next();
                if (!frame) break;
                try {
                  const c = frame.payload.columns[0];
                  if (c?.form !== "flat" || c.data.layout !== "varlen")
                    throw new Error("Expected aggregate text");
                  for (let n = 0; n < frame.payload.rowCount; n++)
                    actual.push({
                      j: new TextDecoder().decode(
                        c.data.bytes.subarray(c.data.offsets[n] ?? 0, c.data.offsets[n + 1] ?? 0),
                      ),
                    });
                } finally {
                  frame.release();
                }
              }
              const result = await run.execution.executed;
              await run.execution.delivered;
              expect(actual).toEqual(expected);
              expect(blocked).toBe(true);
              expect(
                result.profile.operators.reduce((n, op) => n + op.spilledBytes, 0),
              ).toBeGreaterThan(0);
              reach.reach("ordinary credit occupied");
              if (!delivery) reach.reach("async initial group admission");
              reach.reach(delivery ? "delivery credit occupied" : "producer credit occupied");
              reach.reach(binary ? "wide semantic JSONB key" : "wide retained TEXT key");
              if (binary && expansion) reach.reach("expanded JSONB key");
              reach.reach("exact PG aggregate");
              reach.reach("observed spill");
            } catch (error) {
              run.execution.cancel(error);
              await Promise.allSettled([run.execution.executed, run.execution.delivered]);
              throw error;
            } finally {
              run.release();
            }
          } finally {
            blocker?.close();
            await core.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("zero resources");
        },
      ),
      {
        seed,
        // Six required stage/type/expansion examples plus two randomized histories.
        numRuns: 8,
        examples: [
          [2300000, false, false, false],
          [2300000, true, false, false],
          [2300000, false, true, false],
          [2300000, true, true, false],
          [2300000, true, false, true],
          [2300000, true, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
