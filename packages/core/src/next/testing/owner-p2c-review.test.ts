/**
 * The review of R22 and the real-Lowerer swap (B5-p2c-review-1): its probes, kept as tests.
 *
 * 1. SQL BEGIN READ ONLY must never open a writable transaction. Until lowering reports its
 *    access mode, the owner refuses it (0A000); once it does, a write in it fails with 25006.
 * 2. Table rules are cached by catalog identity, never by (table id, version): two histories at
 *    one version do not share rules, and a transaction's own rules die with it (rollback, rollback
 *    to a savepoint).
 * 3. The committed catalog's rule cache is bounded, charged to the governor, and drops the
 *    entries of committed catalogs no writer can reach.
 * 4. `close()` discards pending releases and their timer.
 */
import { describe, expect, it } from "vitest";
import type { CatalogView, TableDef } from "../access/catalog.js";
import type { StatementOptions, TableRules } from "../access/modify.js";
import { EXECUTOR_COSTS } from "../exec/executor.js";
import { createLowerer } from "../lower/create.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import { ProtocolClient } from "../owner/client.js";
import { PROTOCOL_VERSION, RELEASE_DELAY_MS } from "../owner/protocol.js";
import { harness, sleep } from "./owner-harness.js";

/** Whether lowering reports BEGIN READ ONLY's access mode yet (B6's fix). */
const LOWERING_READS_ACCESS_MODE = (() => {
  const lowered = createLowerer().lower("BEGIN READ ONLY", {
    catalog: new MemoryCatalog().view,
    statistics: { table: () => null },
    costs: EXECUTOR_COSTS,
  });
  return lowered.kind === "transactionControl" && lowered.readOnly;
})();

describe("finding 1: SQL BEGIN READ ONLY never opens a writable transaction", () => {
  for (const [session, text] of [
    [1, "BEGIN READ ONLY"],
    [0, "start transaction read only"],
    [1, "/* lead */ BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"],
  ] as const)
    it(`${JSON.stringify(text)} in session ${String(session)}`, async () => {
      const { connect, writePath } = harness();
      const client = await connect();
      await client.query("CREATE TABLE t (id INT PRIMARY KEY)");
      const begin = client.query(text, [], { session });
      if (LOWERING_READS_ACCESS_MODE) {
        // B6's fix landed: the transaction is read-only, and pins a snapshot, not the writer.
        await begin;
        await expect(
          client.query("INSERT INTO t VALUES (1)", [], { session }),
        ).rejects.toMatchObject({ sqlState: "25006" });
        expect(writePath.writerBusy).toBe(false);
        await client.query("ROLLBACK", [], { session });
      } else {
        await expect(begin).rejects.toMatchObject({
          name: "UnsupportedPlanError",
          sqlState: "0A000",
          message: expect.stringContaining("BEGIN READ ONLY isn't supported yet") as unknown,
        });
        // Refused before anything opened: no writer slot, no transaction.
        expect(writePath.writerBusy).toBe(false);
        const insert = await client.query("INSERT INTO t VALUES (1)", [], { session });
        expect(insert.summary.transaction).toBe("idle");
      }
      expect(writePath.rows("t").length).toBe(LOWERING_READS_ACCESS_MODE ? 0 : 1);
    });

  it("plain BEGIN, READ WRITE, and the protocol's read-only begin are unaffected", async () => {
    const { connect, writePath } = harness();
    const client = await connect();
    await client.query("CREATE TABLE t (id INT PRIMARY KEY)");
    for (const text of ["BEGIN", "BEGIN READ WRITE", "START TRANSACTION"]) {
      await client.query(text, [], { session: 1 });
      await client.query("ROLLBACK", [], { session: 1 });
    }
    await client.begin(1, true);
    await expect(
      client.query("INSERT INTO t VALUES (1)", [], { session: 1 }),
    ).rejects.toMatchObject({ sqlState: "25006" });
    expect(writePath.writerBusy).toBe(false);
  });
});

/** The harness, with `lowerTableRules` counted and every statement's options recorded. */
function spied() {
  const h = harness();
  const compiled: Array<{ readonly tableId: number; readonly checks: number }> = [];
  const lowerTableRules = h.planner.lowerTableRules.bind(h.planner);
  h.planner.lowerTableRules = (table: TableDef, catalog: CatalogView) => {
    compiled.push({ tableId: table.id, checks: table.checks.length });
    return lowerTableRules(table, catalog);
  };
  const statements: Array<{ readonly options: StatementOptions; readonly catalog: CatalogView }> =
    [];
  h.writePath.onStatement = (options, catalog) => {
    statements.push({ options, catalog });
  };
  /** The rules the write path would get for the last statement's table named `name`. */
  const rulesOfLast = (name: string): { rules: TableRules; table: TableDef; version: number } => {
    const last = statements.at(-1);
    if (last === undefined) throw new Error("no statement");
    const table = last.catalog.tableByName(name);
    if (table === undefined) throw new Error(`no table ${name}`);
    return { rules: last.options.rules(table.id), table, version: last.catalog.version };
  };
  return { ...h, compiled, rulesOfLast };
}

describe("finding 2: rules follow catalog identity, not (table id, version)", () => {
  it("two histories at one version and table id never share rules (the review's probe)", () => {
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
    const tableId = plain.tables[0]?.id ?? -1;
    expect([plain.version, tableId]).toEqual([checked.version, checked.tables[0]?.id]);
    const rules = engine as unknown as {
      rules(catalog: CatalogView, tableId: number): TableRules;
    };
    const a = rules.rules(plain, tableId);
    const b = rules.rules(checked, tableId);
    expect(b).not.toBe(a);
    // Neither is the committed catalog, and no transaction owns them: nothing was kept.
    expect(rules.rules(plain, tableId)).not.toBe(a);
  });

  it("a rolled-back transaction's rules are not reused by the next transaction's schema", async () => {
    const { connect, compiled, rulesOfLast, governor } = spied();
    const client = await connect();
    await client.query("BEGIN", [], { session: 1 });
    const baseline = governor.usedBytes;
    await client.query("CREATE TABLE u (id INT PRIMARY KEY CHECK (id > 0))", [], { session: 1 });
    await client.query("INSERT INTO u VALUES (1)", [], { session: 1 });
    const first = rulesOfLast("u");
    expect(governor.usedBytes).toBeGreaterThan(baseline);
    await client.query("ROLLBACK", [], { session: 1 });
    // The transaction's rules went with it.
    expect(governor.usedBytes).toBe(baseline);
    await client.query("BEGIN", [], { session: 1 });
    await client.query("CREATE TABLE u (id INT PRIMARY KEY)", [], { session: 1 });
    await client.query("INSERT INTO u VALUES (1)", [], { session: 1 });
    const second = rulesOfLast("u");
    // The collision the review found: same version, same table id, different definitions.
    expect([second.version, second.table.id]).toEqual([first.version, first.table.id]);
    expect(second.rules).not.toBe(first.rules);
    expect(compiled.map((entry) => entry.checks)).toEqual([1, 0]);
    await client.query("ROLLBACK", [], { session: 1 });
  });

  it("rolling back to a savepoint drops the rules compiled after it", async () => {
    const { connect, compiled, rulesOfLast, governor } = spied();
    const client = await connect();
    await client.query("BEGIN", [], { session: 1 });
    await client.query("SAVEPOINT a", [], { session: 1 });
    const baseline = governor.usedBytes;
    await client.query("CREATE TABLE u (id INT PRIMARY KEY CHECK (id > 0))", [], { session: 1 });
    await client.query("INSERT INTO u VALUES (1)", [], { session: 1 });
    const first = rulesOfLast("u");
    expect(governor.usedBytes).toBeGreaterThan(baseline);
    await client.query("ROLLBACK TO a", [], { session: 1 });
    expect(governor.usedBytes).toBe(baseline);
    await client.query("CREATE TABLE u (id INT PRIMARY KEY)", [], { session: 1 });
    await client.query("INSERT INTO u VALUES (1)", [], { session: 1 });
    const second = rulesOfLast("u");
    expect(second.rules).not.toBe(first.rules);
    expect(compiled.map((entry) => entry.checks)).toEqual([1, 0]);
    await client.query("ROLLBACK", [], { session: 1 });
  });

  it("the committed catalog's rules are shared across statements and sessions", async () => {
    const { connect, compiled, rulesOfLast } = spied();
    const client = await connect();
    await client.query("CREATE TABLE t (id INT PRIMARY KEY CHECK (id > 0))");
    await client.query("INSERT INTO t VALUES (1)");
    const first = rulesOfLast("t");
    await client.query("INSERT INTO t VALUES (2)", [], { session: 2 });
    expect(rulesOfLast("t").rules).toBe(first.rules);
    expect(compiled).toHaveLength(1);
  });
});

describe("finding 3: the rule cache is bounded, charged, and drops unreachable catalogs", () => {
  it("keeps only the committed catalog's entries as it moves on (600 versions)", async () => {
    const { connect, engine, governor, rulesOfLast } = spied();
    const client = await connect();
    await client.query("CREATE TABLE t (id INT PRIMARY KEY, n INT CHECK (n > 0))");
    const cache = (engine as unknown as { ruleCache?: { size: number; bytes: number } })
      .ruleCache ?? {
      size: Number.NaN,
      bytes: Number.NaN,
    };
    const before = governor.usedBytes;
    for (let version = 0; version < 600; version++) {
      await client.query(`CREATE SEQUENCE s${String(version)}`);
      await client.query("INSERT INTO t VALUES ($1, 1)", [version]);
      rulesOfLast("t");
    }
    // Charged to the governor at all (the unbounded map charged nothing)...
    expect(governor.usedBytes - before).toBeGreaterThan(0);
    // ...and one committed catalog reachable by writers: one entry.
    expect(cache.size).toBe(1);
    expect(cache.bytes).toBeGreaterThan(0);
    expect(governor.usedBytes - before).toBe(cache.bytes);
  });
});

describe("finding 4: close() discards pending releases", () => {
  it("clears the delayed-release timer, so nothing is posted after close", async () => {
    const sent: Array<Record<string, unknown>> = [];
    let closed = false;
    const port = {
      onmessage: null as ((event: { data: unknown }) => void) | null,
      close: () => {
        closed = true;
      },
      postMessage(message: Record<string, unknown>) {
        sent.push({ ...message, afterClose: closed });
        if (message.t === "hello")
          queueMicrotask(() =>
            port.onmessage?.({ data: { t: "welcome", ownerId: "o", protocol: PROTOCOL_VERSION } }),
          );
      },
    };
    const client = new ProtocolClient();
    await client.connect(port as unknown as MessagePort);
    const query = client.query("SELECT 1");
    const seq = sent.at(-1)?.seq as number;
    port.onmessage?.({
      data: {
        t: "done",
        seq,
        columns: [],
        frames: [
          {
            sequence: 0,
            rowCount: 1,
            dictionaries: [],
            columns: [
              {
                form: "flat",
                length: 1,
                validity: null,
                data: { layout: "i32", values: new Int32Array(1) },
              },
            ],
          },
        ],
        summary: {
          command: "SELECT",
          rowCount: 1,
          counts: null,
          lsn: null,
          replayed: false,
          closedEarly: false,
          transaction: "idle",
          catalogVersion: 1,
        },
      },
    });
    await query;
    client.close();
    await sleep(RELEASE_DELAY_MS + 20);
    expect(sent.filter((message) => message.afterClose === true)).toEqual([]);
    expect(sent.filter((message) => message.t === "release")).toEqual([]);
  });
});
