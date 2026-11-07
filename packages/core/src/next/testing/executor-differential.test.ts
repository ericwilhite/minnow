/** SQL -> lowering -> real executor -> frames, over real writes, flushes and reopen. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import {
  compareEngines,
  type EngineUnderTest,
  type Operation,
  type Workload,
} from "../../testing/differential-harness.js";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { composeEngine } from "../owner/compose.js";
import { ChunkReader } from "../page/reader.js";
import { nextEngine, oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const SEEDS = seedsFor("next-executor-sql", [0xe101, 0xe102]);
const REACH = [
  "empty",
  "nulls",
  "negative",
  "filtered",
  "groups",
  "distinct",
  "topN",
  "flushed",
  "reopened",
  "parameters",
  "multipleBatches",
  "checkpointedReopen",
  "logTailReopen",
  "zeroLimit",
] as const;

/** PostgreSQL shares no lowering, kernel, scan or aggregation code with the candidate. */
async function postgres(): Promise<EngineUnderTest> {
  const pg = new PGlite();
  await pg.waitReady;
  return {
    async run(operation) {
      if (operation.kind === "schema") {
        const schema = operation.schema;
        await pg.exec(
          `CREATE TABLE ${schema.name} (id DOUBLE PRECISION PRIMARY KEY, g DOUBLE PRECISION, v DOUBLE PRECISION, label TEXT)`,
        );
        return { kind: "schema" };
      }
      if (operation.kind === "insertBatch") {
        const params: unknown[] = [];
        const tuples = operation.rows.map((row) => {
          const start = params.length;
          params.push(row.id, row.g, row.v, row.label);
          return `($${String(start + 1)},$${String(start + 2)},$${String(start + 3)},$${String(start + 4)})`;
        });
        if (tuples.length > 0)
          await pg.query(`INSERT INTO ${operation.table} VALUES ${tuples.join(",")}`, params);
        return { kind: "insertBatch", counts: { rowCount: operation.rows.length } };
      }
      if (operation.kind === "reopen") return { kind: "reopen" };
      if (operation.kind !== "sql") throw new Error("unexpected oracle operation");
      const result = await pg.query<Record<string, unknown>>(operation.sql, [
        ...(operation.params ?? []),
      ]);
      // The adapter's oracle representation is a number for safe int8 (D2). NUMERIC stays text.
      const rows = result.rows.map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([key, value]) => {
            if (typeof value !== "bigint") return [key, value];
            if (value < BigInt(Number.MIN_SAFE_INTEGER) || value > BigInt(Number.MAX_SAFE_INTEGER))
              throw new Error("unsafe oracle integer");
            return [key, Number(value)];
          }),
        ),
      );
      return {
        kind: "rows",
        columns: result.fields.map((field) => field.name),
        rows,
        domains: result.fields.map(() => null),
      };
    },
    close: () => pg.close(),
  };
}

for (const seed of SEEDS)
  it(`runs generated SQL through storage and both independent engines, seed ${String(seed)}`, async () => {
    const reach = createReach("executor SQL", REACH);
    const random = mulberry32(seed);
    const operations: Operation[] = [];
    const inputFacts = new Map<string, { nulls: boolean; negative: boolean }>();
    for (let history = 0; history < 10; history++) {
      const table = `executor_${String(history)}`;
      const n = history === 0 ? 0 : history === 8 ? 2081 : 16 + Math.floor(random() * 32);
      const rows = Array.from({ length: n }, (_, id) => ({
        id,
        g: id % 7 === 0 ? null : Math.floor(random() * 4),
        v: id % 5 === 0 ? null : Math.floor(random() * 17) - 8,
        label: id % 3 === 0 ? null : `label-${String(id % 3)}`,
      }));
      operations.push({
        kind: "schema",
        schema: {
          name: table,
          uniqueKey: "id",
          columns: [
            { name: "id", type: "number" },
            { name: "g", type: "number", nullable: true },
            { name: "v", type: "number", nullable: true },
            { name: "label", type: "string", nullable: true },
          ],
        },
      });
      if (rows.length > 0) operations.push({ kind: "insertBatch", table, rows });
      inputFacts.set(table, {
        nulls: rows.some((row) => row.v === null),
        negative: rows.some((row) => row.v !== null && row.v < 0),
      });
      const queries = [
        `SELECT id, g, v, label FROM ${table}`,
        `SELECT id, v + 2 AS x FROM ${table} WHERE v >= 0 ORDER BY id LIMIT 5 OFFSET 1`,
        `SELECT COUNT(*) AS n, COUNT(v) AS nv, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi FROM ${table}`,
        `SELECT g, COUNT(*) AS n, SUM(v) AS s FROM ${table} GROUP BY g HAVING COUNT(*) > 1 ORDER BY g NULLS FIRST`,
        `SELECT COUNT(DISTINCT label) AS n FROM ${table}`,
        `SELECT id, v FROM ${table} WHERE v > $1 ORDER BY v DESC NULLS LAST, id LIMIT $2 OFFSET $3`,
        `SELECT label FROM ${table} WHERE label IS NOT NULL ORDER BY label, id LIMIT 7`,
        `SELECT id, v FROM ${table} LIMIT $1`,
      ];
      const queryOperations: Operation[] = queries.map((sql, index) => ({
        kind: "sql",
        sql,
        ...(index === 5 ? { params: [0, 3, 1] } : index === 7 ? { params: [0] } : {}),
        ...(index === 1 || index === 5 || index === 6
          ? { totalOrder: "id breaks all ties; label duplicates project equal values" }
          : {}),
      }));
      operations.push(
        ...queryOperations,
        { kind: "reopen" },
        ...queryOperations.map((operation) => ({ ...operation })),
      );
    }
    const workload: Workload = { name: `executor SQL ${String(seed)}`, operations };
    // Keep every generated table below automatic checkpoint pressure. The explicit alternating
    // checkpoint below then establishes both recovery paths independently of host/timer load.
    const core = await new StorageCore(8192).open();
    const candidate = await nextEngine(async () => core)("memory");
    const runCandidate = candidate.run.bind(candidate);
    let reopenNumber = 0,
      checkpointedReopen = false;
    candidate.run = async (operation) => {
      if (operation.kind === "reopen") checkpointedReopen = reopenNumber++ % 2 === 0;
      if (operation.kind === "reopen" && checkpointedReopen) {
        await core.harness.path.recycleLog();
        await core.harness.path.quiesce();
      }
      return runCandidate(operation);
    };
    const reference = await currentEngine("memory");
    const pg = await postgres();
    let checked = 0,
      currentTable = "",
      reopened = false;
    try {
      await compareEngines(workload, [pg, candidate, reference], (outcome) => {
        if (!outcome.ok) throw new Error("a matching error does not establish SQL execution");
        const operation = operations[checked++];
        if (operation?.kind === "schema") {
          reach.beginHistory();
          currentTable = operation.schema.name;
          reopened = false;
        }
        if (operation?.kind === "reopen") reopened = true;
        if (operation?.kind === "sql") {
          if (operation.sql.startsWith("SELECT id, g, v, label")) {
            if (outcome.rows.length === 0) reach.reach("empty");
            if (inputFacts.get(currentTable)?.nulls === true) reach.reach("nulls");
            if (inputFacts.get(currentTable)?.negative === true) reach.reach("negative");
          }
          if (operation.sql.includes("WHERE v >=")) reach.reach("filtered");
          if (operation.sql.includes("GROUP BY")) reach.reach("groups");
          if (operation.sql.includes("DISTINCT")) reach.reach("distinct");
          if (operation.sql.includes("LIMIT")) reach.reach("topN");
          if (operation.params !== undefined) reach.reach("parameters");
          if (operation.sql.endsWith("LIMIT $1") && outcome.rows.length === 0)
            reach.reach("zeroLimit");
          if (reopened) reach.reach("reopened");
          const scans =
            core.lastProfile?.operators.filter((profile) => profile.op === "scan") ?? [];
          if (scans.some((profile) => profile.pagesRead > 0)) {
            reach.reach("flushed");
            if (reopened && checkpointedReopen) reach.reach("checkpointedReopen");
          }
          if (scans.some((profile) => profile.batchesOut > 1)) reach.reach("multipleBatches");
          if (
            reopened &&
            !checkpointedReopen &&
            operation.sql.startsWith("SELECT id, g, v, label") &&
            outcome.rows.length > 0 &&
            scans.some((profile) => profile.rowsOut > 0) &&
            scans.every((profile) => profile.pagesRead === 0)
          )
            reach.reach("logTailReopen");
        }
      });
      expect(checked).toBe(operations.length);
      reach.assertReached();
    } finally {
      await pg.close();
      await reference.close();
      await candidate.close();
    }
  }, 60_000);

it("uses the real executor as owner composition's default", async () => {
  const core = await new StorageCore().open();
  try {
    await core.execute("CREATE TABLE owner_real (v INTEGER)");
    await core.writeBatch("insertBatch", "owner_real", [{ v: 3 }, { v: 7 }]);
    const engine = composeEngine({
      planner: core.lowerer,
      writePath: core.harness.path,
      governor: core.harness.governor,
      spill: core.spill,
      reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
    });
    const session = engine.openSession("real", 1);
    try {
      const run = session.run(engine.plan("SELECT v FROM owner_real ORDER BY v"), [], {
        signal: new AbortController().signal,
        request: null,
        windowBytes: 1024,
        frameRows: 1,
      });
      const frame = await run.results.next();
      expect(frame).not.toBeNull();
      const result = await core.execute("SELECT SUM(v) AS total FROM owner_real");
      if (!("batches" in result)) throw new Error("query did not return rows");
      expect(oracleRows(result)).toEqual([{ total: 10 }]);
      expect(frame?.payload.rowCount).toBe(1);
      frame?.release();
      const second = await run.results.next();
      expect(second?.payload.rowCount).toBe(1);
      second?.release();
      expect(await run.results.next()).toBeNull();
      expect((await run.done).rowCount).toBe(2);
    } finally {
      session.close();
    }
  } finally {
    await core.close();
  }
});

it("reports page reads and bound pruning from real flushed storage", async () => {
  const core = await new StorageCore(16).open();
  try {
    await core.execute("CREATE TABLE profile_real (v INTEGER)");
    await core.writeBatch(
      "insertBatch",
      "profile_real",
      Array.from({ length: 64 }, (_, v) => ({ v })),
    );
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
    for (const pruned of [false, true]) {
      const run = core.start(
        core.prepare(`SELECT SUM(v) FROM profile_real${pruned ? " WHERE v < 0" : ""}`),
      );
      try {
        for (;;) {
          const frame = await run.execution.results.next();
          if (frame === null) break;
          frame.release();
        }
        const summary = await run.execution.executed;
        await run.execution.delivered;
        const scan = summary.profile.operators.find((profile) => profile.op === "scan");
        if (pruned) {
          expect(scan?.pagesPruned).toBeGreaterThan(0);
          expect(scan?.pagesRead).toBe(0);
        } else {
          expect(scan?.pagesRead).toBeGreaterThan(0);
          expect(scan?.pagesPruned).toBe(0);
        }
      } finally {
        run.release();
      }
    }
  } finally {
    await core.close();
  }
});
