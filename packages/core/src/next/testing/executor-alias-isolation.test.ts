/** A transferred logical column is mutable client-owned data, independently of its aliases. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, SQL_ALIAS_ISOLATION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";

const reach = createReach("SQL alias isolation", SQL_ALIAS_ISOLATION_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-sql-alias-isolation", [0xb4f9, 0xb4fa]))
  it(`isolates client mutations after ordinary SQL and transfer (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          text: fc.boolean(),
          desc: fc.boolean(),
          aliases: fc.integer({ min: 2, max: 6 }),
          rows: fc.integer({ min: 1, max: 7 }),
        }),
        async ({ text, desc, aliases, rows }) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          try {
            const ddl = `CREATE TABLE t(id INTEGER PRIMARY KEY,v ${text ? "TEXT" : "INTEGER"})`;
            await core.execute(ddl);
            await pg.exec(`DROP TABLE IF EXISTS t;${ddl}`);
            const input = Array.from({ length: rows }, (_, i) => ({
              id: i + 1,
              v: i % 3 === 2 ? null : text ? `汉${String(i)}` : 7 + i,
            }));
            await core.writeBatch("insertBatch", "t", input);
            for (const row of input)
              await pg.query("INSERT INTO t VALUES ($1,$2)", [row.id, row.v]);
            const sql = `SELECT ${Array.from({ length: aliases }, (_, i) => `v AS a${String(i)}`).join(",")} FROM t ORDER BY a1 ${desc ? "DESC" : "ASC"},id`;
            const expected = (await pg.query<Record<string, unknown>>(sql)).rows;
            const run = core.start(core.prepare(sql));
            let count = 0;
            try {
              for (;;) {
                const frame = await run.execution.results.next();
                if (frame === null) break;
                try {
                  const received = structuredClone(frame.payload, {
                    transfer: [...frame.transfer],
                  });
                  const original = structuredClone(received.columns.slice(1));
                  const first = received.columns[0];
                  if (first?.form !== "flat") throw new Error("Expected flat SQL result");
                  if (first.data.layout === "i32") {
                    for (let i = 0; i < received.rowCount; i++)
                      expect(
                        first.validity === null ||
                          ((first.validity[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0
                          ? first.data.values[i]
                          : null,
                      ).toBe(expected[count + i]?.a0);
                    first.data.values.fill(999);
                  } else if (first.data.layout === "varlen") {
                    for (let i = 0; i < received.rowCount; i++) {
                      const valid =
                        first.validity === null ||
                        ((first.validity[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0;
                      const value = valid
                        ? new TextDecoder().decode(
                            first.data.bytes.subarray(
                              first.data.offsets[i],
                              first.data.offsets[i + 1],
                            ),
                          )
                        : null;
                      expect(value).toBe(expected[count + i]?.a0);
                    }
                    first.data.bytes.fill(0x61);
                    first.data.offsets.fill(0);
                  } else throw new Error("Unexpected alias layout");
                  first.validity?.fill(0);
                  expect(received.columns.slice(1)).toEqual(original);
                  count += received.rowCount;
                  reach.reach(text ? "text alias mutation" : "integer alias mutation");
                  reach.reach("structured transfer");
                } finally {
                  frame.release();
                }
              }
              await run.execution.executed;
              await run.execution.delivered;
              expect(count).toBe(expected.length);
              if (rows > 2) reach.reach("nullable aliases");
            } finally {
              run.execution.cancel(new Error("alias property cleanup"));
              await Promise.allSettled([run.execution.executed, run.execution.delivered]);
              run.release();
            }
          } finally {
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("all resources returned");
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [{ text: false, desc: false, aliases: 2, rows: 1 }],
          [{ text: true, desc: true, aliases: 6, rows: 7 }],
        ],
      },
    );
  }, 60000);
