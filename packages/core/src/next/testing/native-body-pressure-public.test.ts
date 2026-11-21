/**
 * A native write whose nested AFTER bodies run point and range lookups completes beside a
 * competitor that holds all ordinary memory but 1 MiB, through the public path (production
 * funding: owner/native-command.ts). Its queries register caches with their prepaid governor
 * (owner/prepaid-query.ts), which declines every cache; a registration that charged root metadata
 * failed mid-statement with an untyped RangeError ("Native query cache metadata must be
 * admitted") once that metadata was fully used. Law (GOVERNOR-WAIT-POLICY): grant, cancellable
 * wait or spill, never an untyped error; results are exactly the statement's.
 */
import { expect, it } from "vitest";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

it("completes nested body lookups beside a competitor holding all but 1 MiB", async () => {
  const owner = await publicOwner({ capBytes: 64 << 20 });
  const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  const governor = owner.core.harness.governor;
  let competitor: Awaited<ReturnType<typeof governor.admit>> | null = null;
  try {
    for (const sql of [
      "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
      "CREATE TABLE audit(id INT PRIMARY KEY,v INT)",
      "CREATE TABLE sink(id INT PRIMARY KEY,n INT)",
      "CREATE TABLE trace(id INT PRIMARY KEY,v TEXT)",
      "INSERT INTO audit VALUES(1,0),(2,0)",
      "INSERT INTO trace VALUES(1,'')",
      `INSERT INTO p VALUES${Array.from({ length: 20 }, (_, i) => `(${String(i + 1)},0)`).join(",")}`,
      `INSERT INTO sink VALUES${Array.from({ length: 12 }, (_, i) => `(${String(i + 1)},0)`).join(",")}`,
      "CREATE TRIGGER p_after AFTER UPDATE ON p BEGIN UPDATE audit SET v=v+1 WHERE id=1; END",
      "CREATE TRIGGER audit_after AFTER UPDATE ON audit BEGIN UPDATE trace SET v=v||CAST(NEW.v AS TEXT)||';' WHERE id=1; UPDATE sink SET n=n+1 WHERE id BETWEEN 2 AND 7; END",
    ])
      await db.execute(sql);
    competitor = await governor.admit({
      label: "competitor",
      minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes - (1 << 20),
      spillable: false,
    });
    expect(competitor.allocate(competitor.held)).toBe(true);
    await db.execute("UPDATE p SET v=v+1");
    competitor.close();
    expect((await db.query("SELECT sum(v) AS s FROM p")).rows()).toEqual([{ s: 20 }]);
    expect((await db.query("SELECT v FROM audit WHERE id=1")).rows()).toEqual([{ v: 20 }]);
    expect((await db.query("SELECT sum(n) AS s FROM sink")).rows()).toEqual([{ s: 6 * 20 }]);
  } finally {
    competitor?.close();
    await db.close();
    await owner.close();
  }
  expect(governor.usedBytes).toBe(0);
}, 60_000);
