/**
 * A long buffered write scope renews its lease per statement.
 *
 * Statements that fold into the write set stage nothing, and staging was the only inline
 * renewal. That left the transaction's `setInterval` heartbeat, which a loop of buffered
 * statements on the memory store starves — it yields only microtasks — so a scope longer than
 * the lease TTL reached COMMIT with an expired lease and published nothing. Buffering now
 * checks the lease on every statement. The TTL is shortened so the test runs in about a second;
 * the loop runs well past it without ever yielding a macrotask.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

const ROWS = 500;
const LEASE_MS = 400;

describe("scope lease renewal under a buffered statement loop", () => {
  it("a loop of buffered keyed updates longer than the lease TTL still commits", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore(), { transactionOwnerLeaseMs: LEASE_MS });
    await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
    await db.insertBatch(
      "items",
      Array.from({ length: ROWS }, (_, i) => ({ id: i, amount: 0 })),
    );
    const started = performance.now();
    let statements = 0;
    await db.write(async (tx) => {
      while (performance.now() - started < LEASE_MS * 2.5) {
        await tx.execute("UPDATE items SET amount = $1 WHERE id = $2", [
          statements,
          statements % ROWS,
        ]);
        statements += 1;
      }
    });
    expect(performance.now() - started).toBeGreaterThan(LEASE_MS);
    const committed = (await db.query("SELECT amount FROM items WHERE id = 0")).rows[0]?.amount;
    expect(committed).toBe(Math.floor((statements - 1) / ROWS) * ROWS);
    await db.close();
  });
});
