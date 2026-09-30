import { expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";
import {
  bindPlanParameters,
  compileQuery,
  DUAL_TABLE,
  dualTableRows,
  executeRowQuery,
} from "./query.js";
import { MinnowDatabaseClient } from "./client.js";
import { attachDatabaseWorker } from "./worker-host.js";
import { createBoundary } from "./client-audit-harness.js";
import {
  semanticCorpus,
  semanticOutcome,
  semanticFingerprint,
} from "../testing/semantic-corpus.js";
import { positionalToNumbered } from "../testing/oracle.js";

it("keeps values, domains and refusals aligned across folding, row/vector, memo and RPC paths", async () => {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCompact: false,
    autoCollect: false,
  });
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "memory" },
    databaseOptions: { autoCompact: false, autoCollect: false },
  });
  const postgres = new PGlite();
  try {
    const setup =
      "CREATE TABLE semantic_rows (id INTEGER); INSERT INTO semantic_rows VALUES (1),(2)";
    await database.execute("CREATE TABLE semantic_rows (id INTEGER)");
    await database.execute("INSERT INTO semantic_rows VALUES (1),(2)");
    await client.execute("CREATE TABLE semantic_rows (id INTEGER)");
    await client.execute("INSERT INTO semantic_rows VALUES (1),(2)");
    await postgres.exec(setup);
    const tables = new Map([
      ["semantic_rows", [{ id: 1 }, { id: 2 }]],
      [DUAL_TABLE, dualTableRows()],
    ]);
    const pairs = new Map<string, unknown>();
    let successes = 0;
    let refusals = 0;
    for (const testCase of semanticCorpus()) {
      const options = testCase.params === undefined ? {} : { params: testCase.params };
      const vector = await semanticOutcome(() =>
        database.query(testCase.sql, { ...options, memoize: false }),
      );
      for (const optimize of [true, false]) {
        const row = await semanticOutcome(() =>
          executeRowQuery(
            bindPlanParameters(compileQuery(testCase.sql, { optimize }), testCase.params),
            tables,
          ),
        );
        expect(row, `${testCase.sql}: row optimize=${String(optimize)}`).toEqual(vector);
      }
      expect(await semanticOutcome(() => database.query(testCase.sql, options))).toEqual(vector);
      expect(await semanticOutcome(() => database.query(testCase.sql, options))).toEqual(vector);
      expect(
        await semanticOutcome(() => client.query(testCase.sql, { ...options, memoize: false })),
      ).toEqual(vector);
      const oracle = await semanticOutcome(() =>
        postgres.query(positionalToNumbered(testCase.sql), testCase.params),
      );
      expect(oracle.ok, testCase.sql).toBe(vector.ok);
      if (vector.ok && oracle.ok) {
        successes += 1;
        expect(vector.result.rows, testCase.sql).toEqual(oracle.result.rows);
      } else refusals += 1;
      if (testCase.pair !== "membership") {
        const previous = pairs.get(testCase.pair);
        if (previous !== undefined)
          expect(vector, `literal vs bound: ${testCase.pair}`).toEqual(previous);
        else pairs.set(testCase.pair, vector);
      }
    }
    expect(successes).toBeGreaterThan(100);
    expect(refusals).toBeGreaterThan(0);
    expect((await client.query("SELECT 1 AS usable")).rows).toEqual([{ usable: 1 }]);
  } finally {
    await client.close();
    await database.close();
    await postgres.close();
  }
}, 60_000);

it("never normalizes away signed zero or exact numeric digits in worker comparisons", () => {
  expect(semanticFingerprint({ v: -0 })).not.toBe(semanticFingerprint({ v: 0 }));
  expect(semanticFingerprint({ v: NaN })).not.toBe(semanticFingerprint({ v: null }));
  expect(semanticFingerprint({ v: -0 })).not.toBe(semanticFingerprint({ v: { number: "-0" } }));
  expect(semanticFingerprint({ v: new Date(0) })).not.toBe(
    semanticFingerprint({ v: new Date(0).toISOString() }),
  );
  expect(semanticFingerprint({ v: "9007199254740993.0001" })).not.toBe(
    semanticFingerprint({ v: "9007199254740992.0001" }),
  );
});
