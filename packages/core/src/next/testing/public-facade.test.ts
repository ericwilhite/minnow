/** The familiar facade delegates planning, transaction state and outcomes to the native owner. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_FACADE_REACH } from "../../testing/reach.js";
import type { SqlExecutor, Value } from "../owner/api.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { ProtocolClient } from "../owner/client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

for (const seed of seedsFor("next-public-facade", [0xb738, 0xb739]))
  it(`binds native SQL, isolated sessions, preparation and cursor ownership (${String(seed)})`, async () => {
    const reach = createReach("public facade", PUBLIC_FACADE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.bigInt({ min: -(1n << 60n), max: 1n << 60n }),
        fc.string({ maxLength: 32 }),
        fc.integer({ min: 1, max: 4 }),
        async (cache, number, text, count) => {
          reach.beginHistory();
          const owner = await publicOwner({ planCacheBytes: cache ? 4 << 20 : 0 });
          const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
          try {
            await db.ready();
            expect(db.status).toBe("ready");
            await expect(db.query("CREATE TABLE forbidden(id INT)")).rejects.toThrow(
              "use execute()",
            );
            await db.execute("CREATE TABLE t(id INT PRIMARY KEY, n BIGINT, v TEXT DEFAULT 'd')");
            await expect(db.query("INSERT INTO t VALUES(99,99,'bad')")).rejects.toThrow(
              "use execute()",
            );
            expect((await db.query("SELECT count(*) n FROM t")).values(0)).toEqual([0]);
            reach.reach("owner query-only guard");
            let held: SqlExecutor | undefined;
            const result = await db.transaction(async (tx) => {
              held = tx;
              await tx.execute("INSERT INTO t(id,n,v) VALUES(1,$1,$2)", [number, text]);
              expect((await tx.query("SELECT n,v FROM t")).tuples()).toEqual([[number, text]]);
              expect((await db.query("SELECT count(*) n FROM t")).values(0)).toEqual([0]);
              return number;
            });
            expect(result).toBe(number);
            if (held === undefined) throw new Error("Missing transaction scope");
            await expect(held.query("SELECT 1")).rejects.toThrow("Transaction is closed");
            reach.reach("transaction isolation and scope fence");
            await expect(
              db.transaction(async (tx) => {
                await tx.execute("INSERT INTO t(id,n) VALUES(2,$1)", [number]);
                throw new Error("rollback body");
              }),
            ).rejects.toThrow("rollback body");
            expect((await db.query("SELECT id FROM t")).values(0)).toEqual([1]);
            reach.reach("callback rollback");
            await db.transaction(async (tx) => {
              await tx.execute("SAVEPOINT s");
              await expect(tx.query("SELECT 1/0 FROM t")).rejects.toMatchObject({
                sqlState: "22012",
              });
              await tx.execute("ROLLBACK TO s");
              await tx.execute("INSERT INTO t(id,n) VALUES(3,$1)", [number]);
            });
            reach.reach("savepoint recovery");
            await expect(
              db.transaction(async (tx) => {
                await tx.execute("CREATE TABLE private_ddl(id INT PRIMARY KEY)");
                const handle = await tx.prepare("SELECT id FROM private_ddl");
                expect((await handle.query()).tuples()).toEqual([]);
                handle.close();
                await expect(tx.execute("COMMIT")).rejects.toThrow("callback transaction owns");
              }),
            ).rejects.toThrow(
              "A callback transaction owns its BEGIN, COMMIT and ROLLBACK boundary",
            );
            await db.execute("CREATE TABLE private_ddl(id INT PRIMARY KEY)");
            reach.reach("private preparation and owned transaction boundary");
            const tablePlan = await db.prepare("SELECT * FROM t ORDER BY id");
            await db.execute("ALTER TABLE t ADD COLUMN extra TEXT DEFAULT 'filled'");
            // PostgreSQL 18 plancache.c, observed in PGlite 18.3: a prepared statement whose
            // result columns changed is refused 0A000 on every execute; prepared again, it
            // returns the new columns.
            await expect(tablePlan.query()).rejects.toMatchObject({ sqlState: "0A000" });
            const staleCursor = tablePlan.cursor();
            await expect(staleCursor.columns).rejects.toMatchObject({ sqlState: "0A000" });
            await staleCursor.close();
            tablePlan.close();
            const refreshed = await db.prepare("SELECT * FROM t ORDER BY id");
            const fresh = await refreshed.query();
            expect(fresh.columns.map((column) => column.name)).toEqual(["id", "n", "v", "extra"]);
            expect(fresh.tuples()).toEqual([
              [1, number, text, "filled"],
              [3, number, "d", "filled"],
            ]);
            refreshed.close();
            // Fence the prior frame release/unprepare before isolating prepared-cache charge.
            await db.catalog();
            reach.reach("prepared catalog invalidation");
            owner.engine.plan("SELECT $1::bigint n, $2::text v");
            const beforePrepare = owner.core.harness.governor.usedBytes;
            const prepared = await db.prepare("SELECT $1::bigint n, $2::text v");
            if (cache) expect(owner.core.harness.governor.usedBytes).toBeGreaterThan(beforePrepare);
            else expect(owner.core.harness.governor.usedBytes).toBe(beforePrepare);
            expect(prepared.parameters).toEqual([{ kind: "int64" }, { kind: "text" }]);
            expect((await prepared.query([number, text])).tuples()).toEqual([[number, text]]);
            // A new connection has no prepared IDs: fallback SQL is planned by the SAME request.
            const channel = new MessageChannel();
            try {
              owner.server.accept(channel.port2);
              await owner.client.reconnect(channel.port1);
              expect((await prepared.query([number, text])).tuples()).toEqual([[number, text]]);
              prepared.close();
              prepared.close();
              await expect(prepared.query([number, text])).rejects.toThrow(
                "Prepared statement is closed",
              );
            } finally {
              channel.port1.close();
              channel.port2.close();
            }
            reach.reach(cache ? "charged bounded prepared cache" : "uncached prepared fallback");
            // Reconnect again to the original server before the independent cursor history.
            const cursorChannel = new MessageChannel();
            try {
              owner.server.accept(cursorChannel.port2);
              await owner.client.reconnect(cursorChannel.port1);
              const cursor = db.cursor(
                "SELECT i::bigint n FROM generate_series(1,$1) s(i)",
                [count + 3],
                { frameRows: 1, prefetchBytes: 1 },
              );
              expect(await cursor.columns).toMatchObject([{ name: "n", type: { kind: "int64" } }]);
              const heldResults: Value[][][] = [];
              for await (const chunk of cursor) {
                heldResults.push(chunk.tuples());
                break;
              }
              await cursor.close();
              expect(heldResults).toEqual([[[1n]]]);
              expect(owner.client.hasPins).toBe(false);
              reach.reach("cursor return retains Result and drains pin");
              const catalog = await db.catalog();
              expect(catalog).toBeDefined();
            } finally {
              cursorChannel.port1.close();
              cursorChannel.port2.close();
            }
          } finally {
            await db.close();
            await owner.close();
            expect(owner.core.harness.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [false, 0n, "", 1],
          [true, 9007199254740993n, "水", 3],
        ],
      },
    );
    reach.assertReached();
  }, 60000);

for (const seed of seedsFor("next-public-facade-queued-input", [0xb73a, 0xb73b]))
  it(`freezes queued inputs before connecting (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 10 }),
        fc.string({ maxLength: 20 }),
        async (id, text) => {
          const owner = await publicOwner();
          await owner.client.query("CREATE TABLE queued(id INT PRIMARY KEY, v TEXT)");
          const protocol = new ProtocolClient();
          let resolveReady: () => void = () => undefined;
          const ready = new Promise<void>((resolve) => {
            resolveReady = resolve;
          });
          const db = new MinnowDatabaseClient(protocol, { ready });
          const params = [id, text];
          const inserted = db.execute("INSERT INTO queued VALUES($1,$2)", params);
          params[0] = id + 100;
          params[1] = "changed";
          const channel = new MessageChannel();
          try {
            owner.server.accept(channel.port2);
            await protocol.connect(channel.port1);
            resolveReady();
            await inserted;
            expect((await db.query("SELECT * FROM queued")).tuples()).toEqual([[id, text]]);
          } finally {
            await db.close();
            channel.port1.close();
            channel.port2.close();
            await owner.close();
          }
        },
      ),
      { seed, numRuns: 8 },
    );
  }, 60000);
