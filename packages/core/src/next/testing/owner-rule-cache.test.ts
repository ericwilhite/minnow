/**
 * owner/rule-cache.ts on its own: the bound, the governor charge, cooperative reclaim, and the
 * identity key through a transaction's cache (B5-p2c-review-1, findings 2 and 3).
 */
import { describe, expect, it } from "vitest";
import type { CatalogView } from "../access/catalog.js";
import type { TableRules } from "../access/modify.js";
import { EXECUTOR_COSTS } from "../exec/executor.js";
import { createLowerer } from "../lower/create.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import { Governor } from "../memory/governor.js";
import { DEFAULT_RULE_CACHE_ENTRIES, RuleCache } from "../owner/rule-cache.js";
import { harness } from "./owner-harness.js";

describe("RuleCache", () => {
  it("keeps each history's rules apart in a transaction's cache", () => {
    const { engine } = harness();
    const lowerer = createLowerer();
    const history = (sql: string) => {
      const catalog = new MemoryCatalog();
      const lowered = lowerer.lower(sql, {
        catalog: catalog.view,
        statistics: { table: () => null },
        costs: EXECUTOR_COSTS,
      });
      if (lowered.kind !== "ddl" || lowered.change === null) throw new Error("expected DDL");
      catalog.apply(lowered.change);
      return catalog.view;
    };
    const plain = history("CREATE TABLE t (id INT PRIMARY KEY)");
    const checked = history("CREATE TABLE t (id INT PRIMARY KEY CHECK (id > 0))");
    const table = plain.tables[0];
    if (table === undefined) throw new Error("expected a table");
    const tableId = table.id;
    const rules = engine as unknown as {
      rules(catalog: CatalogView, tableId: number, local?: RuleCache | null): TableRules;
    };
    const local = new RuleCache(null);
    const a = rules.rules(plain, tableId, local);
    expect(rules.rules(plain, tableId, local)).toBe(a);
    expect(rules.rules(checked, tableId, local)).not.toBe(a);
    local.clear();
    expect(rules.rules(plain, tableId, local)).not.toBe(a);
  });

  it("evicts least recently used entries past its bound, and returns their bytes", () => {
    const governor = new Governor(64 * 1024 * 1024);
    const cache = new RuleCache(governor, 3);
    const catalog = new MemoryCatalog().view;
    const rules = (id: number) => ({ check: () => null, generate: () => undefined, id });
    const expressions = { checks: [], generated: [], defaults: [] };
    for (let id = 1; id <= 5; id++)
      cache.get(catalog, id, () => ({ rules: rules(id), expressions }));
    expect(cache.size).toBe(3);
    expect(governor.usedBytes).toBe(cache.bytes);
    cache.close();
    expect(governor.usedBytes).toBe(0);
    expect(DEFAULT_RULE_CACHE_ENTRIES).toBe(512);
  });

  it("gives its bytes back when the governor asks (cooperative reclaim)", async () => {
    const governor = new Governor(8 * 1024 * 1024);
    const cache = new RuleCache(governor);
    const catalog = new MemoryCatalog().view;
    const expressions = { checks: [], generated: [], defaults: [] };
    for (let id = 1; id <= 20; id++)
      cache.get(catalog, id, () => ({
        rules: { check: () => null, generate: () => undefined },
        expressions,
      }));
    expect(cache.size).toBe(20);
    // A pipeline that needs nearly all of the cap: it is admitted only if the cache yields.
    const admitted = governor.admit({
      label: "big",
      minimumBytes: governor.capBytes - governor.reserveBytes,
      spillable: false,
    });
    const outcome = await Promise.race([
      admitted.then(() => "admitted"),
      new Promise<string>((resolve) => setTimeout(() => resolve("waiting"), 1_000)),
    ]);
    expect(outcome).toBe("admitted");
    expect(cache.size).toBeLessThan(20);
    (await admitted).close();
    cache.close();
  });
});
