/** Complete aggregation minima wait atomically; admitted state never depends on another query's credit. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, AGGREGATE_PROGRESS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import type { Batch } from "../vector/vector.js";
import type { Execution, PreparedPlan } from "../exec/executor.js";
import { TypedLayout } from "../exec/agg/typed-layout.js";
import { ChunkReader } from "../page/reader.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const kinds = [
  "INTEGER",
  "INTEGER",
  "INTEGER",
  "INTEGER",
  "INTEGER",
  ...Array.from({ length: 7 }, () => "DOUBLE PRECISION"),
  "INTEGER",
  "INTEGER",
  "DOUBLE PRECISION",
  "DOUBLE PRECISION",
  "INTEGER",
  "INTEGER",
  "BIGINT",
];
const aggs = [
  "COUNT(*)",
  ...Array.from({ length: 5 }, (_, i) => `SUM(c${String(i)})`),
  ...Array.from({ length: 5 }, (_, i) => `AVG(c${String(i + 5)})`),
  "MIN(c10)",
  "MAX(c11)",
  "MIN(c12)",
  "MAX(c13)",
  "SUM(c14)",
  "SUM(c15)",
  "AVG(c16)",
  "COUNT(c17)",
  "MAX(c18)",
  "MIN(label)",
];
const sql = `SELECT k0,k1,k2,${aggs.map((fn, i) => `${fn} AS q${String(i)}`).join(",")} FROM t GROUP BY k0,k1,k2 ORDER BY k0,k1,k2`;
function canonical(rows: ReadonlyArray<Record<string, unknown>>): unknown[] {
  return rows.map((row) => [
    row.k0,
    row.k1,
    row.k2,
    ...aggs.map((_, i) => {
      const value = row[`q${String(i)}`];
      if (value === null || i === aggs.length - 1) return value;
      const bytes = new ArrayBuffer(8),
        view = new DataView(bytes);
      view.setFloat64(0, Number(value));
      return view.getBigUint64(0).toString(16);
    }),
  ]);
}
async function consume(prepared: PreparedPlan, execution: Execution) {
  const batches: Batch[] = [];
  for (;;) {
    const frame = await execution.results.next();
    if (!frame) break;
    try {
      const payload = structuredClone(frame.payload);
      batches.push({
        length: payload.rowCount,
        selection: null,
        columns: payload.columns.map((vector, i) => {
          const type = prepared.columns[i]?.type;
          if (vector.form !== "flat" || !type)
            throw new Error("Expected owned typed pressure output");
          return { ...vector, type };
        }),
      });
    } finally {
      frame.release();
    }
  }
  await execution.executed;
  await execution.delivered;
  return canonical(oracleRows({ columns: prepared.columns, batches }));
}
function deferred() {
  let resolve: () => void = () => {
    throw new Error("deferred not installed");
  };
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it.each(seedsFor("next-aggregation-native-progress", [0xb49b, 0xb49c]))(
  "waits without partial state, then grows and promotes under competing pressure (seed %i)",
  async (seed) => {
    const reach = createReach("aggregation memory progress", AGGREGATE_PROGRESS_REACH);
    let history = 0;
    const core = await new StorageCore(32768).open(),
      pg = new PGlite();
    await pg.waitReady;
    const packed = vi.spyOn(TypedLayout.prototype, "tryUpdate");
    try {
      const ddl = `CREATE TABLE t(k0 TEXT,k1 TEXT,k2 TEXT,${kinds.map((type, i) => `c${String(i)} ${type}`).join(",")},label TEXT)`;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 600, max: 900 }),
          fc.integer({ min: 4, max: 40 }),
          fc.boolean(),
          fc.boolean(),
          async (groups, width, stored, cancel) => {
            reach.beginHistory();
            const table = `t${String(history++)}`,
              query = sql.replace("FROM t ", `FROM ${table} `);
            const create = ddl.replace("CREATE TABLE t(", `CREATE TABLE ${table}(`);
            await core.execute(create);
            await pg.exec(create);
            const rows = Array.from({ length: 6144 }, (_, i) => {
              const g = i < 2048 ? 0 : (i - 2048) % groups;
              const row: Record<string, string | number | null> = {
                k0: `a${String(stored ? g : g % 5)}`,
                k1: `b${String(stored ? 0 : Math.floor(g / 5) % 10)}`,
                k2: `c${String(stored ? 0 : Math.floor(g / 50))}`,
                label: `${"z".repeat(width)}${String((i * 7) % 97)}`,
              };
              kinds.forEach((type, c) => {
                row[`c${String(c)}`] =
                  c === 5 && i >= 4096 && i % 3 === 0
                    ? null
                    : type === "DOUBLE PRECISION"
                      ? i % 3 === 0
                        ? 2 ** 30
                        : i % 3 === 1
                          ? -(2 ** 30)
                          : ((i + c) % 13) / 10
                      : (i * 17 + c) % 1009;
              });
              return row;
            });
            const names = ["k0", "k1", "k2", ...kinds.map((_, i) => `c${String(i)}`), "label"];
            if (stored) {
              await core.writeBatch("insertBatch", table, rows.slice(0, 2048));
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              await core.writeBatch("insertBatch", table, rows.slice(2048));
            } else await core.writeBatch("insertBatch", table, rows);
            for (let from = 0; from < rows.length; from += 128) {
              const chunk = rows.slice(from, from + 128);
              await pg.query(
                `INSERT INTO ${table} VALUES ${chunk.map((_, k) => `(${names.map((_, c) => `$${String(k * names.length + c + 1)}`).join(",")})`).join(",")}`,
                chunk.flatMap((r) => names.map((name) => r[name] ?? null)),
              );
            }
            if (stored) {
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              reach.reach("stored dictionary certificate");
            } else reach.reach("bounded memory certificate");
            const expected = canonical((await pg.query<Record<string, unknown>>(query)).rows),
              prepared = core.prepare(query);
            const keyColumn = core
              .context()
              .catalog.tableByName(table)
              ?.columns.find((column) => column.name === "k0")?.id;
            if (keyColumn === undefined) throw new Error("Missing pressure key column");
            const governor = new Governor(64 << 20),
              pinned = await governor.admit({
                label: "temporarily pinned competitor",
                minimumBytes: governor.capBytes - governor.reserveBytes - 65536,
                spillable: false,
              });
            const growth = await governor.admit({
              label: "competing growth",
              minimumBytes: 0,
              spillable: true,
            });
            let waitedLabel = "";
            const wait = deferred(),
              admitted = new Set<string>();
            const native = governor.admitGroup.bind(governor),
              fast = governor.tryAdmit.bind(governor);
            governor.tryAdmit = (options) => {
              const grant = fast(options);
              if (grant && options.label.endsWith("fixed-buffers")) admitted.add(options.label);
              return grant;
            };
            governor.admitGroup = (options) => {
              const total = options.grants.reduce((n, part) => n + part.minimumBytes, 0);
              const blocked = total > governor.capBytes - governor.usedBytes;
              const result = native(options);
              if (blocked && options.label.startsWith("query#") && waitedLabel === "") {
                waitedLabel = options.label;
                wait.resolve();
              }
              return result.then((parts) => {
                if (options.label.endsWith("fixed-buffers")) admitted.add(options.label);
                return parts;
              });
            };
            const snapshot = core.harness.path.pin(),
              executions: Execution[] = [],
              promises: Array<Promise<unknown>> = [];
            const reason = new Error("cancel aggregation credit wait");
            let headroomPinned = false;
            const dictionarySizes = new Set<number>();
            const before = packed.mock.calls.length;
            try {
              for (let i = 0; i < 2; i++) {
                const execution = prepared.execute({
                  snapshot,
                  statement: null,
                  parameters: [],
                  statementTime: 0n,
                  governor,
                  spill: core.spill,
                  queryId: i + 1,
                  reads: {
                    open: (pages, chunk, options) => {
                      if (!headroomPinned && admitted.size === (cancel ? 1 : 2)) {
                        const bytes = Math.max(
                          0,
                          governor.capBytes - governor.reserveBytes - governor.usedBytes,
                        );
                        expect(growth.allocate(bytes)).toBe(true);
                        headroomPinned = true;
                        reach.reach("no shared growth headroom");
                      }
                      const opened = ChunkReader.open(pages, chunk, options);
                      const observe = (reader: ChunkReader) => {
                        if (admitted.size !== 0 && chunk.columnId === keyColumn) {
                          const decode = reader.decodeCodes.bind(reader),
                            gather = reader.gatherCodes.bind(reader);
                          const note = () => {
                            if (reader.dictionary !== null)
                              dictionarySizes.add(reader.dictionary.length);
                          };
                          reader.decodeCodes = (...args) => {
                            note();
                            decode(...args);
                          };
                          reader.gatherCodes = (...args) => {
                            note();
                            gather(...args);
                          };
                        }
                        return reader;
                      };
                      return opened instanceof Promise ? opened.then(observe) : observe(opened);
                    },
                  },
                  sequences: null,
                  sequenceSession: null,
                  returning: "atExecuted",
                });
                executions.push(execution);
                promises.push(consume(prepared, execution).catch((error: unknown) => error));
              }
              await wait.promise;
              expect(executions.every((e) => e.state !== "executed")).toBe(true);
              reach.reach("temporary pinned holder waits");
              const cancelIndex = Number(/^query#(\d+)/.exec(waitedLabel)?.[1]) - 1;
              expect(cancelIndex).toBeGreaterThanOrEqual(0);
              expect(cancelIndex).toBeLessThan(2);
              if (cancel) {
                executions[cancelIndex]?.cancel(reason);
                expect(await promises[cancelIndex]).toMatchObject({
                  code: "MINNOW_QUERY_CANCELLED",
                  sqlState: "57014",
                });
                reach.reach("cancelled credit wait");
              }
              pinned.close();
              reach.reach("competing holder releases");
              const results = await Promise.all(promises);
              for (let i = 0; i < results.length; i++)
                if (!cancel || i !== cancelIndex) {
                  if (results[i] instanceof Error) throw results[i];
                  expect(results[i]).toEqual(expected);
                }
              const transitions = packed.mock.results.slice(before);
              expect(transitions.length).toBeGreaterThan(0);
              const prefix = transitions.some((r) => r.type === "return" && r.value);
              const fallback = transitions.some((r) => r.type === "return" && !r.value);
              expect(fallback).toBe(true);
              // Bounded memory decoding can retain an all-valid bitmap; the packed bank then
              // correctly uses ordinary transitions from the first batch. Stored page masks
              // certify the packed prefix, and the later NULL batch must promote it.
              if (stored) {
                expect(prefix).toBe(true);
                expect(dictionarySizes.has(1)).toBe(true);
                expect([...dictionarySizes].some((n) => n >= groups)).toBe(true);
                // The first stored group has one key class; the next codebook introduces
                // hundreds of classes. Both translation and tuple storage must resize.
                reach.reach("codebook translation growth");
                reach.reach("tuple growth");
              }
              if (prefix && fallback) reach.reach("numeric promotion");
              reach.reach("private resize");
              reach.reach("PG exact bits");
              reach.reach("concurrent prepared executions");
            } finally {
              pinned.close();
              growth.close();
              for (const e of executions) e.cancel(new Error("pressure cleanup"));
              await Promise.allSettled(promises);
              await Promise.allSettled(executions.flatMap((e) => [e.executed, e.delivered]));
              snapshot.release();
              expect(governor.usedBytes).toBe(0);
              reach.reach("all credit returned");
            }
          },
        ),
        {
          seed,
          numRuns: 3,
          examples: [
            [600, 8, true, false],
            [600, 8, false, true],
            [900, 16, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      packed.mockRestore();
      await pg.close();
      await core.close();
    }
  },
  120000,
);
