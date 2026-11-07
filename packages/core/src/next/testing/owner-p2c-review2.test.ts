/**
 * The second review of R22 and the real-Lowerer swap (B5-p2c-review-2): its probes, kept as tests.
 *
 * 1. The rule cache is one of the governor's evictable caches (rule 6): it yields before an
 *    operator's growth is refused.
 * 2. Its size is walked, not serialized: a CHECK with a large int64 literal (a BigInt) compiles.
 * 3. `engine.close()` returns the committed rule cache's charge, even if the write path fails.
 * 4. The READ ONLY guard reads modes as the front end does (identifier words).
 */
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import type { CatalogView } from "../access/catalog.js";
import type { StatementOptions, TableRules } from "../access/modify.js";
import { EXECUTOR_COSTS } from "../exec/executor.js";
import { createLowerer } from "../lower/create.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import { Governor, OBJECT_OVERHEAD_BYTES, bigintBytes } from "../memory/governor.js";
import { RuleCache, retainedBytes, ruleBytes } from "../owner/rule-cache.js";
import { requestsReadOnly } from "../owner/sequence.js";
import { harness } from "./owner-harness.js";

const MiB = 1024 * 1024;

/** A catalog built by real lowering from one CREATE TABLE. */
function catalogOf(sql: string): CatalogView {
  const catalog = new MemoryCatalog();
  const lowered = createLowerer().lower(sql, {
    catalog: catalog.view,
    statistics: { table: () => null },
    costs: EXECUTOR_COSTS,
  });
  if (lowered.kind !== "ddl" || lowered.change === null) throw new Error("expected DDL");
  catalog.apply(lowered.change);
  return catalog.view;
}

const EMPTY = { checks: [], generated: [], defaults: [] };
const RULES: TableRules = { check: () => null, generate: () => undefined };

describe("finding 2 (review): the rule cache yields before an operator's growth is refused", () => {
  it("an operator at cap − reserve − entry grows by evicting the rule entry", () => {
    const governor = new Governor(8 * MiB);
    const cache = new RuleCache(governor);
    cache.get(new MemoryCatalog().view, 1, () => ({ rules: RULES, expressions: EMPTY }));
    expect(cache.size).toBe(1);
    const entry = cache.bytes;
    const operator = governor.tryAdmit({ label: "operator", minimumBytes: 0, spillable: true });
    if (operator === null) throw new Error("not admitted");
    expect(operator.allocate(governor.capBytes - governor.reserveBytes - entry)).toBe(true);
    // Only the cache's bytes are left below the admission limit: the cache must give them.
    expect(operator.allocate(1)).toBe(true);
    expect(cache.size).toBe(0);
    expect(cache.bytes).toBe(0);
    operator.close();
    cache.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("several rule caches and the governor's other caches are evicted oldest unused first", () => {
    const governor = new Governor(8 * MiB);
    // `newer` registers first, but `older` holds the entry used longest ago: recency, not
    // registration order, decides (EvictableCache.oldestUnusedAt).
    const newer = new RuleCache(governor);
    const older = new RuleCache(governor);
    const view = new MemoryCatalog().view;
    newer.get(view, 1, () => ({ rules: RULES, expressions: EMPTY }));
    older.get(view, 1, () => ({ rules: RULES, expressions: EMPTY }));
    newer.get(view, 1, () => ({ rules: RULES, expressions: EMPTY }));
    const operator = governor.tryAdmit({ label: "operator", minimumBytes: 0, spillable: true });
    if (operator === null) throw new Error("not admitted");
    expect(
      operator.allocate(governor.capBytes - governor.reserveBytes - older.bytes - newer.bytes),
    ).toBe(true);
    expect(operator.allocate(1)).toBe(true);
    expect([older.size, newer.size]).toEqual([0, 1]);
    operator.close();
    older.close();
    newer.close();
    expect(governor.usedBytes).toBe(0);
  });
});

describe("finding 1 (review): sizing walks the expressions, BigInts included", () => {
  it("compiles and charges a CHECK with a literal beyond 2^53 (the review's probe)", () => {
    const { engine, governor } = harness();
    const view = catalogOf("CREATE TABLE t (id BIGINT CHECK (id > 9007199254740993))");
    const table = view.tables[0];
    if (table === undefined) throw new Error("no table");
    const local = new RuleCache(governor);
    const rules = (
      engine as unknown as {
        rules(catalog: CatalogView, tableId: number, local?: RuleCache | null): TableRules;
      }
    ).rules(view, table.id, local);
    expect(rules).toBeDefined();
    expect(local.size).toBe(1);
    expect(local.bytes).toBeGreaterThan(0);
    local.close();
  });

  it("counts typed-array bytes once per buffer, BigInts and strings by size, never serializing", () => {
    const buffer = new BigInt64Array([9007199254740993n, 2n]);
    expect(retainedBytes(buffer)).toBe(OBJECT_OVERHEAD_BYTES + 16);
    expect(retainedBytes([buffer, buffer])).toBe(OBJECT_OVERHEAD_BYTES * 2 + 16);
    // Two views over one buffer: a header each, the buffer once.
    const other = new BigInt64Array(buffer.buffer, 8, 1);
    expect(retainedBytes([buffer, other])).toBe(OBJECT_OVERHEAD_BYTES * 3 + 16);
    expect(retainedBytes(10n ** 40n)).toBe(bigintBytes(10n ** 40n));
    expect(retainedBytes({ a: "abc" })).toBe(OBJECT_OVERHEAD_BYTES + 24 + 6);
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    expect(retainedBytes(cyclic)).toBe(OBJECT_OVERHEAD_BYTES);
    expect(ruleBytes(EMPTY)).toBe(OBJECT_OVERHEAD_BYTES * 5);
  });
});

/** The harness with every write statement's options recorded, to call `rules` as B3 will. */
function withRules() {
  const h = harness();
  const statements: Array<{ options: StatementOptions; catalog: CatalogView }> = [];
  h.writePath.onStatement = (options, catalog) => {
    statements.push({ options, catalog });
  };
  const compileLast = (name: string) => {
    const last = statements.at(-1);
    const table = last?.catalog.tableByName(name);
    if (last === undefined || table === undefined) throw new Error("no statement");
    return last.options.rules(table.id);
  };
  return { ...h, compileLast };
}

describe("finding 3 (review): engine.close() returns the committed rule cache's charge", () => {
  it("releases it on close", async () => {
    const { connect, engine, governor, compileLast } = withRules();
    const client = await connect();
    await client.query("CREATE TABLE t (id INT PRIMARY KEY CHECK (id > 0))");
    await client.query("INSERT INTO t VALUES (1)");
    const before = governor.usedBytes;
    compileLast("t");
    expect(governor.usedBytes).toBeGreaterThan(before);
    client.close();
    await engine.close();
    expect(governor.usedBytes).toBe(before);
  });

  it("releases it even when the write path fails to close", async () => {
    const { connect, engine, governor, writePath, compileLast } = withRules();
    const client = await connect();
    await client.query("CREATE TABLE t (id INT PRIMARY KEY CHECK (id > 0))");
    await client.query("INSERT INTO t VALUES (1)");
    const before = governor.usedBytes;
    compileLast("t");
    expect(governor.usedBytes).toBeGreaterThan(before);
    writePath.close = () => Promise.reject(new Error("disk gone"));
    client.close();
    await expect(engine.close()).rejects.toThrow("disk gone");
    expect(governor.usedBytes).toBe(before);
  });
});

describe("finding 4 (review): the READ ONLY guard reads modes as the front end does", () => {
  it("matches READ then ONLY among identifier words, punctuation ignored", () => {
    for (const text of [
      "BEGIN READ ONLY",
      "BEGIN READ, ONLY",
      "BEGIN READ (ONLY)",
      'BEGIN "read" "only"',
      "START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
      "BEGIN WORK READ /* x */ ONLY",
    ])
      expect(requestsReadOnly(text), text).toBe(true);
    for (const text of [
      "BEGIN",
      "BEGIN READ WRITE",
      "BEGIN /* READ ONLY */ READ WRITE",
      "BEGIN ISOLATION LEVEL READ COMMITTED",
      "SELECT 'READ ONLY'",
    ])
      expect(requestsReadOnly(text), text).toBe(false);
  });

  it("rejects malformed access modes before opening a transaction, as PostgreSQL", async () => {
    const pg = new PGlite();
    await pg.waitReady;
    try {
      const { connect, writePath } = harness();
      const client = await connect();
      await client.query("CREATE TABLE t (id INT PRIMARY KEY)");
      for (const text of ["BEGIN READ, ONLY", "BEGIN READ (ONLY)"]) {
        await expect(pg.exec(text)).rejects.toMatchObject({ code: "42601" });
        const error = await client.query(text, [], { session: 1 }).then(
          () => null,
          (failure: unknown) => failure,
        );
        expect(error).toBeInstanceOf(Error);
        const failure = error as Error & { sqlState?: string };
        // The shipped front end has a positional syntax-error class; newer lowering adds
        // SQLSTATE. Both represent PostgreSQL's syntax error, never unsupported mode (0A000).
        const state =
          failure.sqlState ??
          (failure.name === "SqlCompileError" && failure.message.startsWith("syntax error")
            ? "42601"
            : failure.name);
        expect(state, text).toBe("42601");
        expect(failure.message).toMatch(/^syntax error/u);
        expect(writePath.writerBusy).toBe(false);
      }
      expect(writePath.rows("t")).toEqual([]);
    } finally {
      await pg.close();
    }
  });
});
