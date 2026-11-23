import { it } from "vitest";
import { appendFileSync } from "node:fs";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
const OUT = "/home/eric/.cache/c1-tmp/abort/size-hack.txt";
it("trigger update with and without whole-memtable sizing", async () => {
  for (const hack of [false, true])
    for (const rows of [100, 400, 1_000]) {
      (globalThis as { __sizeOne?: boolean }).__sizeOne = hack;
      const owner = await publicOwner({ capBytes: 64 << 20 });
      const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
      try {
        await db.execute("CREATE TABLE t(id INT PRIMARY KEY,v TEXT)");
        await db.execute("CREATE TABLE mirror(id INT PRIMARY KEY,v TEXT)");
        await db.execute("CREATE TABLE log(id INT,v TEXT)");
        await db.execute("CREATE TRIGGER copy AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES(NEW.id,NEW.v); END");
        await db.execute(`INSERT INTO mirror SELECT g, 'm' FROM generate_series(1, ${String(rows)}) g`);
        await db.execute("CREATE TRIGGER follow AFTER UPDATE ON t FOR EACH ROW BEGIN UPDATE mirror SET v=NEW.v WHERE id=NEW.id; END");
        await db.execute(`INSERT INTO t SELECT g, $1 || g::text FROM generate_series(1, ${String(rows)}) g`, ["xxxxxxxx"]);
        const t0 = performance.now();
        await db.execute("UPDATE t SET v = $1 || (id * 7)::text", ["yyyyyyyy"]);
        const ms = performance.now() - t0;
        appendFileSync(OUT, `${hack ? "size-one" : "whole   "} ${String(rows)} rows: ${ms.toFixed(0)} ms, ${(ms / rows).toFixed(2)} ms/row\n`);
      } finally {
        delete (globalThis as { __sizeOne?: boolean }).__sizeOne;
        await db.close();
        await owner.close();
      }
    }
}, 3_600_000);
