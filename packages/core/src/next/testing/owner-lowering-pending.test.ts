/** R21 pending integration: keep real SQL generators visible until B6 exposes reviewed metadata. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  createReach,
  OWNER_PENDING_LOCK_STATES,
  OWNER_PENDING_ORDER_STATES,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { PENDING_OWNER_CAPABILITIES } from "./divergences.js";
import { cleanups, harness } from "./owner-harness.js";

async function outcome(action: () => Promise<unknown>) {
  try {
    await action();
    return "ok";
  } catch (error) {
    const e = error as { sqlState?: string; code?: string };
    return e.sqlState ?? e.code;
  }
}
function pendingReason(id: (typeof PENDING_OWNER_CAPABILITIES)[number]["id"]): string {
  const entry = PENDING_OWNER_CAPABILITIES.find((candidate) => candidate.id === id);
  if (entry === undefined) throw new Error(`Missing R21 pending entry: ${id}`);
  return entry.reason;
}
const lockReason = pendingReason("B5-effective-row-locking");
const orderReason = pendingReason("B5-ordered-transaction-modes");

it.skip.each(seedsFor("next-owner-pending-locking", [0x10c6]))(
  `pending R21 effective row locking: ${lockReason} (seed %i)`,
  async (seed) => {
    const pg = new PGlite();
    const reach = createReach("next-owner-pending-locking", OWNER_PENDING_LOCK_STATES, {
      minimum: 2,
    });
    try {
      await pg.exec("CREATE TABLE t(id INT PRIMARY KEY); INSERT INTO t VALUES(1)");
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            strength: fc.constantFrom("UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"),
            kind: fc.constantFrom(
              "direct",
              "point",
              "empty",
              "nested",
              "nested-body",
              "view",
              "view-body",
              "cte-body",
              "cte-reference",
              "constants",
            ),
          }),
          async ({ strength, kind }) => {
            const h = harness();
            const c = await h.connect();
            try {
              await c.query("CREATE TABLE t(id INT PRIMARY KEY)");
              await c.query("INSERT INTO t VALUES(1)");
              await pg.exec("DROP VIEW IF EXISTS v");
              const view = `CREATE VIEW v AS SELECT * FROM t${kind === "view-body" ? ` FOR ${strength}` : ""}`;
              if (kind === "view" || kind === "view-body") {
                await pg.exec(view);
                await c.query(view);
              }
              const sql =
                kind === "direct"
                  ? `SELECT * FROM t FOR ${strength}`
                  : kind === "point"
                    ? `SELECT * FROM t WHERE id = 1 FOR ${strength}`
                    : kind === "empty"
                      ? `SELECT * FROM t WHERE false FOR ${strength}`
                      : kind === "nested"
                        ? `SELECT * FROM (SELECT * FROM t) q FOR ${strength}`
                        : kind === "nested-body"
                          ? `SELECT * FROM (SELECT * FROM t FOR ${strength}) q`
                          : kind === "view"
                            ? `SELECT * FROM v FOR ${strength}`
                            : kind === "view-body"
                              ? "SELECT * FROM v"
                              : kind === "cte-body"
                                ? `WITH x AS (SELECT * FROM t FOR ${strength}) SELECT * FROM x`
                                : kind === "cte-reference"
                                  ? `WITH x AS (SELECT * FROM t) SELECT * FROM x FOR ${strength}`
                                  : `SELECT * FROM (SELECT 1 AS n) q FOR ${strength}`;
              await pg.exec("BEGIN READ ONLY");
              await c.query("BEGIN READ ONLY");
              const expected = await outcome(() => pg.query(sql));
              expect(["ok", "25006"]).toContain(expected);
              expect(await outcome(() => c.query(sql)), sql).toBe(expected);
              reach.recordHistory([kind, strength]);
            } finally {
              await pg.exec("ROLLBACK");
              h.server.stepDown("closing");
              await h.engine.close();
              for (const close of cleanups.splice(0).reverse()) close();
            }
          },
        ),
        { seed, numRuns: 120 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120_000,
);

it.skip.each(seedsFor("next-owner-pending-ordered-modes", [0x0de2]))(
  `pending R21 ordered mode items: ${orderReason} (seed %i)`,
  async (seed) => {
    const pg = new PGlite();
    const reach = createReach("next-owner-pending-ordered-modes", OWNER_PENDING_ORDER_STATES, {
      minimum: 2,
    });
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            initialReadOnly: fc.boolean(),
            depth: fc.integer({ min: 0, max: 2 }),
            reverse: fc.boolean(),
            command: fc.constantFrom("BEGIN", "START TRANSACTION", "SET TRANSACTION"),
          }),
          async ({ initialReadOnly, depth, reverse, command }) => {
            const h = harness();
            const c = await h.connect();
            try {
              if (command === "SET TRANSACTION") {
                const begin = `BEGIN READ ${initialReadOnly ? "ONLY" : "WRITE"}`;
                await pg.exec(begin);
                await c.query(begin);
                for (let d = 0; d < depth; d++) {
                  await pg.exec(`SAVEPOINT s${String(d)}`);
                  await c.query(`SAVEPOINT s${String(d)}`);
                }
              }
              const sql = command + (reverse ? " READ WRITE, READ ONLY" : " READ ONLY, READ WRITE");
              const expected = await outcome(() => pg.query(sql));
              expect(["ok", "25001"]).toContain(expected);
              expect(await outcome(() => c.query(sql)), sql).toBe(expected);
              const entered = new Set<string>([
                command,
                reverse ? "readWrite then readOnly" : "readOnly then readWrite",
              ]);
              if (command === "SET TRANSACTION") {
                entered.add(
                  depth === 0
                    ? "root SET"
                    : depth === 1
                      ? "one savepoint SET"
                      : "nested savepoint SET",
                );
                entered.add(initialReadOnly ? "read-only SET entry" : "read-write SET entry");
                if (!initialReadOnly && depth > 0)
                  entered.add(
                    reverse
                      ? "readWrite then readOnly under writable savepoint"
                      : "readOnly then readWrite under writable savepoint",
                  );
              }
              reach.recordHistory(entered);
            } finally {
              await pg.exec("ROLLBACK");
              h.server.stepDown("closing");
              await h.engine.close();
              for (const close of cleanups.splice(0).reverse()) close();
            }
          },
        ),
        { seed, numRuns: 120 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120_000,
);
