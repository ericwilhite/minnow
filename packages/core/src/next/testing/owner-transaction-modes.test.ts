/** Typed Lowerer transaction modes; authority: its contract and PostgreSQL SET TRANSACTION. */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { OWNER_TRANSACTION_MODE_STATES, createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { TransactionModeError } from "../owner/errors.js";
import { cleanups, harness } from "./owner-harness.js";

async function subject() {
  const h = harness();
  const client = await h.connect();
  await client.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
  await client.query("INSERT INTO t VALUES (1)");
  return { ...h, client };
}
async function finish(h: Awaited<ReturnType<typeof subject>>) {
  h.server.stepDown("closing");
  await h.engine.close();
  for (const close of cleanups.splice(0).reverse()) close();
}

describe("owner applies typed transaction modes", () => {
  it("switches writer ownership before data work and keeps savepoints usable", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN READ WRITE");
      expect(h.writePath.writerBusy).toBe(true);
      await c.query("SAVEPOINT s");
      await c.query("SET TRANSACTION READ ONLY");
      expect(h.writePath.writerBusy).toBe(false);
      await c.query("SET TRANSACTION READ WRITE");
      expect(h.writePath.writerBusy).toBe(true);
      await c.query("INSERT INTO t VALUES (2)");
      await c.query("ROLLBACK TO s");
      expect(
        (await c.query("SELECT id FROM t")).rows().sort((a, b) => Number(a[0]) - Number(b[0])),
      ).toEqual([[1]]);
      await c.query("COMMIT");
      expect(h.writePath.writerBusy).toBe(false);
    } finally {
      await finish(h);
    }
  });

  it("rollback-to restores access modes and writer resources; release keeps changed modes", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN READ WRITE");
      await c.query("SAVEPOINT s");
      await c.query("SET TRANSACTION READ ONLY");
      await c.query("SELECT id FROM t WHERE id = 1");
      await c.query("ROLLBACK TO s");
      expect(h.writePath.writerBusy).toBe(true);
      await c.query("INSERT INTO t VALUES (2)");
      await c.query("COMMIT");
      await c.query("BEGIN READ ONLY");
      await c.query("SAVEPOINT s");
      await c.query("SET TRANSACTION READ WRITE");
      await c.query("INSERT INTO t VALUES (3)");
      await c.query("ROLLBACK TO s");
      expect(h.writePath.writerBusy).toBe(false);
      await expect(c.query("INSERT INTO t VALUES (4)")).rejects.toMatchObject({
        sqlState: "25006",
      });
      await c.query("ROLLBACK");
      expect(
        (await c.query("SELECT id FROM t")).rows().sort((a, b) => Number(a[0]) - Number(b[0])),
      ).toEqual([[1], [2]]);
      await c.query("BEGIN");
      await c.query("SAVEPOINT s");
      await c.query("SET TRANSACTION READ ONLY");
      await c.query("RELEASE s");
      expect(h.writePath.writerBusy).toBe(false);
      await expect(c.query("INSERT INTO t VALUES (5)")).rejects.toMatchObject({
        sqlState: "25006",
      });
      await c.query("ROLLBACK");
    } finally {
      await finish(h);
    }
  });

  it("a synchronous read freezes modes and rollback-to cannot reset that boundary", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN READ ONLY");
      await c.query("SAVEPOINT s");
      const before = h.server.stats.synchronous;
      expect((await c.query("SELECT id FROM t WHERE id = 1")).rows()).toEqual([[1]]);
      expect(h.server.stats.synchronous).toBe(before + 1);
      await expect(c.query("SET TRANSACTION READ WRITE")).rejects.toBeInstanceOf(
        TransactionModeError,
      );
      await expect(c.query("SELECT id FROM t")).rejects.toMatchObject({ sqlState: "25P02" });
      await c.query("ROLLBACK TO s");
      await expect(c.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")).rejects.toMatchObject(
        { sqlState: "25001" },
      );
      await c.query("ROLLBACK");
      await c.query("BEGIN");
      await c.query("SET TRANSACTION READ ONLY");
      expect(h.writePath.writerBusy).toBe(false);
      await expect(c.query("INSERT INTO t VALUES (2)")).rejects.toMatchObject({
        sqlState: "25006",
      });
      await c.query("ROLLBACK");
    } finally {
      await finish(h);
    }
  });

  it("applies isolation metadata per session and resets it at the transaction end", async () => {
    const h = await subject();
    const c = h.client;
    try {
      await c.query("BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY, DEFERRABLE");
      expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([["read committed"]]);
      expect((await c.query("SHOW transaction_isolation", [], { session: 1 })).rows()).toEqual([
        ["repeatable read"],
      ]);
      await c.query("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, NOT DEFERRABLE");
      expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([["read uncommitted"]]);
      await c.query("ROLLBACK");
      expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([["repeatable read"]]);
      // Outside a transaction PostgreSQL warns and changes nothing (the protocol has no notices).
      await c.query("SET TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED");
      expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([["repeatable read"]]);
      await c.query("INSERT INTO t VALUES (2)");
    } finally {
      await finish(h);
    }
  });
});

it.each(seedsFor("next-owner-transaction-modes", [0x7a2501]))(
  "generated begin/set/data/rollback histories (seed %i)",
  async (seed) => {
    const reach = createReach("next-owner-transaction-modes", OWNER_TRANSACTION_MODE_STATES, {
      minimum: 2,
    });
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          initialReadOnly: fc.boolean(),
          duplicateModes: fc.boolean(),
          targetReadOnly: fc.boolean(),
          isolation: fc.constantFrom("READ COMMITTED", "READ UNCOMMITTED", "REPEATABLE READ"),
          first: fc.constantFrom("none", "query", "modify", "ddl"),
          end: fc.constantFrom("COMMIT", "ROLLBACK"),
          deferrable: fc.boolean(),
          savepoint: fc.constantFrom("none", "rollback", "release"),
        }),
        async ({
          initialReadOnly,
          duplicateModes,
          targetReadOnly,
          isolation,
          first,
          end,
          deferrable,
          savepoint,
        }) => {
          const h = await subject();
          const c = h.client;
          const entered = new Set<string>();
          try {
            await c.query(
              `BEGIN ${duplicateModes ? `READ ${initialReadOnly ? "WRITE" : "ONLY"}, ` : ""}READ ${initialReadOnly ? "ONLY" : "WRITE"}`,
            );
            expect(h.writePath.writerBusy).toBe(!initialReadOnly);
            entered.add(initialReadOnly ? "read-only begin" : "read-write begin");
            if (duplicateModes)
              entered.add(initialReadOnly ? "last read-only wins" : "last read-write wins");
            const work =
              first === "query"
                ? "SELECT id FROM t WHERE id = 1"
                : first === "modify"
                  ? "INSERT INTO t VALUES (2)"
                  : "CREATE TABLE u (id INT PRIMARY KEY)";
            if (first !== "none") {
              if (initialReadOnly && first !== "query") {
                await expect(c.query(work)).rejects.toMatchObject({ sqlState: "25006" });
                entered.add("read-only refusal");
                await expect(c.query("SET TRANSACTION READ WRITE")).rejects.toMatchObject({
                  sqlState: "25P02",
                });
              } else {
                await c.query(work);
                entered.add(
                  first === "query"
                    ? "query boundary"
                    : first === "modify"
                      ? "modify boundary"
                      : "ddl boundary",
                );
                await expect(c.query("SET TRANSACTION READ WRITE")).rejects.toMatchObject({
                  sqlState: "25001",
                });
              }
            } else {
              if (savepoint !== "none") await c.query("SAVEPOINT modes");
              await c.query(
                `SET TRANSACTION READ ${targetReadOnly ? "ONLY" : "WRITE"}, ISOLATION LEVEL ${isolation}, ${deferrable ? "" : "NOT "}DEFERRABLE`,
              );
              expect(h.writePath.writerBusy).toBe(!targetReadOnly);
              expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([
                [isolation.toLowerCase()],
              ]);
              entered.add(targetReadOnly ? "set read-only" : "set read-write");
              entered.add("isolation metadata");
              entered.add(deferrable ? "deferrable" : "not deferrable");
              let effectiveReadOnly = targetReadOnly;
              if (savepoint === "rollback") {
                await c.query("ROLLBACK TO modes");
                effectiveReadOnly = initialReadOnly;
                expect((await c.query("SHOW transaction_isolation")).rows()).toEqual([
                  ["repeatable read"],
                ]);
                entered.add("rollback modes");
              } else if (savepoint === "release") {
                await c.query("RELEASE modes");
                entered.add("release modes");
              }
              expect(h.writePath.writerBusy).toBe(!effectiveReadOnly);
              if (effectiveReadOnly)
                await expect(c.query("INSERT INTO t VALUES (2)")).rejects.toMatchObject({
                  sqlState: "25006",
                });
              else await c.query("INSERT INTO t VALUES (2)");
            }
            await c.query(end);
            expect(h.writePath.writerBusy).toBe(false);
            entered.add(end === "COMMIT" ? "commit reset" : "rollback reset");
            await c.query("BEGIN");
            await c.query("SET TRANSACTION READ ONLY");
            expect(h.writePath.writerBusy).toBe(false);
            await c.query("ROLLBACK");
            reach.recordHistory(entered);
          } finally {
            await finish(h);
          }
        },
      ),
      { seed, numRuns: 120 },
    );
    reach.assertReached();
  },
  120_000,
);
