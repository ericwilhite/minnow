import { it } from "vitest";
import { appendFileSync } from "node:fs";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
const OUT = "/home/eric/.cache/c1-tmp/abort/stacks.txt";
it("abort composite sizes per site", async () => {
  for (const rows of [100]) {
    const owner = await publicOwner({ capBytes: 64 << 20 });
    const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
    const original = AbortSignal.any.bind(AbortSignal);
    const sites = new Map<string, { n: number; total: number; max: number }>();
    let on = false;
    try {
      await db.execute("CREATE TABLE t(id INT PRIMARY KEY,v TEXT)");
      await db.execute("CREATE TABLE mirror(id INT PRIMARY KEY,v TEXT)");
      await db.execute("CREATE TABLE log(id INT,v TEXT)");
      await db.execute("CREATE TRIGGER copy AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES(NEW.id,NEW.v); END");
      await db.execute(`INSERT INTO mirror SELECT g, 'm' FROM generate_series(1, ${String(rows)}) g`);
      await db.execute("CREATE TRIGGER follow AFTER UPDATE ON t FOR EACH ROW BEGIN UPDATE mirror SET v=NEW.v WHERE id=NEW.id; END");
      await db.execute(`INSERT INTO t SELECT g, $1 || g::text FROM generate_series(1, ${String(rows)}) g`, ["xxxxxxxx"]);
      AbortSignal.any = (signals: Iterable<AbortSignal>) => {
        const result = original(signals);
        if (on) {
          const key = Object.getOwnPropertySymbols(result).find((s) => s.description === "kSourceSignals");
          const size = key ? ((result as unknown as Record<symbol, Set<unknown>>)[key]?.size ?? 0) : -1;
          Error.stackTraceLimit = 40;
          const site = (new Error().stack ?? "").split("\n").slice(2, 30).filter((l) => !l.includes("node_modules") && !l.includes("node:internal")).map((l) => l.trim().replace(/^at /, "").replace(/\(.*\/next\//, "(")).join(" <- ");
          const s = sites.get(site) ?? { n: 0, total: 0, max: 0 };
          s.n++; s.total += size; s.max = Math.max(s.max, size);
          sites.set(site, s);
        }
        return result;
      };
      on = true;
      (globalThis as { __boundProbe?: unknown }).__boundProbe = (src: { rows: number | null; segments?: Uint32Array; selectedRow?: true; batchRows?: 1; tableId: number }, prefix: number) => {
        Error.stackTraceLimit = 40;
        const st = (new Error().stack ?? "").split("\n").slice(2, 9).filter((l) => !l.includes("node_modules")).map((l) => l.trim().replace(/^at /, "").replace(/\(.*\/next\//, "(").replace(/.*\/next\//, "")).join(" <- ");
        const key = `table ${String(src.tableId)} rows ${String(src.rows)} segments ${src.segments ? String(src.segments.length) : "all"} selected ${String(src.selectedRow)} batch ${String(src.batchRows)} prefix ${String(prefix)} :: ${st}`;
        const s = sites.get("B " + key) ?? { n: 0, total: 0, max: 0 }; s.n++; sites.set("B " + key, s);
      };
      const t0 = performance.now();
      await db.execute("UPDATE t SET v = $1 || (id * 7)::text", ["yyyyyyyy"]);
      on = false;
      delete (globalThis as { __boundProbe?: unknown }).__boundProbe;
      appendFileSync(OUT, `== ${String(rows)} rows update ${(performance.now() - t0).toFixed(0)} ms\n`);
      for (const [site, s] of [...sites].sort((a, b) => b[1].total - a[1].total))
        appendFileSync(OUT, `calls ${String(s.n)} mean ${(s.total / s.n).toFixed(1)} max ${String(s.max)} :: ${site}\n`);
    } finally {
      AbortSignal.any = original;
      await db.close();
      await owner.close();
    }
  }
}, 1_800_000);
