// SCRATCH PROBE — not committed.
import { it } from "vitest";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";

const SIZES = (process.env.SIZES ?? "100,1000,2000").split(",").map(Number);
const ENGINE = process.env.ENGINE ?? "next";
interface Db { execute(sql: string): Promise<unknown>; close(): Promise<void> }
async function open(): Promise<{ db: Db; close(): Promise<void> }> {
  if (ENGINE === "0x") {
    const db = new MinnowDatabase(new MemoryBlockStore());
    return { db: { execute: (sql) => db.execute(sql), close: () => db.close() }, close: () => Promise.resolve() };
  }
  const owner = await publicOwner({ capBytes: 64 << 20 });
  const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  return { db: { execute: (sql) => db.execute(sql), close: () => db.close() }, close: () => owner.close() };
}
it("throughput", async () => {
  for (const rows of SIZES) {
    const { db, close } = await open();
    try {
      await db.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
      await db.execute("CREATE TABLE mirror(id INT PRIMARY KEY, v TEXT)");
      await db.execute(`INSERT INTO mirror SELECT g, 'm' FROM generate_series(1, ${String(rows)}) g`);
      await db.execute(`INSERT INTO t SELECT g, 'x' || CAST(g AS TEXT) FROM generate_series(1, ${String(rows)}) g`);
      await db.execute("CREATE TRIGGER follow AFTER UPDATE ON t FOR EACH ROW BEGIN UPDATE mirror SET v = NEW.v WHERE id = NEW.id; END");
      const t0 = performance.now();
      await db.execute("UPDATE t SET v = 'y' || CAST(id * 7 AS TEXT) WHERE id % 2 = 0");
      const ms = performance.now() - t0;
      process.stderr.write(`TP ${ENGINE} ${String(rows)} ${ms.toFixed(0)}ms ${(ms / (rows / 2)).toFixed(3)}ms/row\n`);
    } finally {
      await db.close();
      await close();
    }
  }
}, 3_600_000);
