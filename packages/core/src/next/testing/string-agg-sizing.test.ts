/**
 * `string_agg`'s value: bounded by its rows where the plan bounds them, funded from what the
 * statement was admitted at preparation, and refused typed (53200) beyond it, never untyped.
 *
 * Found by #53's review (Astra) and P1's follow-up 2. At 32 MiB, beside another connection's held
 * 1.5 MB read, `INSERT INTO t SELECT 1, string_agg($1, ''), 'x' FROM generate_series(1, 3)` with
 * a 1 MB parameter was refused: "query#3/result result-owned buffers ... (3004096 bytes, 8791984
 * available)". The owned value (3 MB) was admitted at preparation (the result step), but the
 * modification's spool takes its rows from the same credit, so the value never fitted beside it.
 * A result-owned allocation beyond the admission with no frame out threw the untyped "Result-owned
 * allocation exceeds its admitted delivery minimum" (exec/result-sink.ts).
 *
 * Law: exec/input-size-plan.ts, "a bound is an upper bound on every value the expression
 * produces" (string_agg: at most one value and one separator per input row, PostgreSQL appends
 * the separator before every value but the first); next/memory/README.md, every memory refusal is
 * `MemoryAdmissionError` (53200); PostgreSQL statement atomicity, a refused statement writes
 * nothing.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { columnInputBytes } from "../exec/input-size-plan.js";
import { ResultChannel } from "../exec/result.js";
import { ResultSink } from "../exec/result-sink.js";
import { Governor } from "../memory/governor.js";
import { createReach, STRING_AGG_SIZING_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
type Reach = ReturnType<typeof createReach<(typeof STRING_AGG_SIZING_REACH)[number]>>;

const describe = (error: unknown) => {
  const e = error as { name?: string; sqlState?: string; message?: string };
  return `${String(e.name)} ${e.sqlState ?? ""} ${String(e.message)}`;
};

interface History {
  readonly capMiB: number;
  readonly parts: number;
  readonly width: number;
  readonly separator: "" | ",";
  readonly held: boolean;
  readonly form: "insert" | "select";
}

async function run(history: History, reach?: Reach): Promise<string> {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  Object.defineProperty(core.harness, "governor", { value: new Governor(history.capMiB * MiB) });
  await core.open();
  let end: (() => Promise<void>) | null = null;
  const context = JSON.stringify(history);
  try {
    await core.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT, w TEXT)");
    await core.execute("CREATE TABLE r (id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO r VALUES (1, $1)", ["b".repeat(1_500_000)]);
    if (history.held) {
      // Another connection's read holds its admission while this statement runs.
      const lowered = core.lowerer.lower("SELECT id, v FROM r", core.context());
      if (lowered.kind !== "query") throw new Error("query plan required");
      const other = core.start(core.executor.prepare(lowered.plan));
      await new Promise((resolve) => setTimeout(resolve, 50));
      // A finite holder: it ends on its own after 300 ms, whatever this statement does (a
      // statement may wait for it; it must never wait forever).
      const drained = new Promise<void>((resolve) => setTimeout(resolve, 300)).then(async () => {
        for (let frame = await other.execution.results.next(); frame !== null;) {
          frame.release();
          frame = await other.execution.results.next();
        }
        await other.execution.delivered;
        other.release();
      });
      end = () => drained;
    }
    const value = "a".repeat(history.width);
    const aggregate = `string_agg($1, '${history.separator}')`;
    const series = `generate_series(1, ${String(history.parts)})`;
    const sql =
      history.form === "insert"
        ? `INSERT INTO t SELECT 1, ${aggregate}, 'x' FROM ${series}`
        : `SELECT ${aggregate} FROM ${series}`;
    const expected = history.parts * history.width + (history.parts - 1) * history.separator.length;
    // The plan's bound covers the value it produces (input-size-plan.ts's law).
    const lowered = core.lowerer.lower(sql, core.context());
    if (lowered.kind === "query" || lowered.kind === "modify") {
      const root = lowered.plan.root;
      const input = root.op === "modify" ? root.input : root;
      const column = root.op === "modify" ? 1 : 0;
      const bound = columnInputBytes(input, column, {
        parameter: () => history.width,
        source: () => null,
      });
      expect(bound, `${context}: string_agg's bound`).not.toBeNull();
      expect(bound ?? 0, `${context}: string_agg's bound`).toBeGreaterThanOrEqual(expected);
      reach?.reach("bound checked");
    }
    const outcome = await core.execute(sql, [value]).then(
      (result) => result,
      (error: unknown) => error,
    );
    if (end !== null) {
      await end();
      end = null;
    }
    if (outcome instanceof Error) {
      // Refused: typed, and nothing written.
      expect(describe(outcome), context).toMatch(/^MemoryAdmissionError 53200 /u);
      const rows = await core.execute("SELECT id FROM t");
      if ("batches" in rows) expect(oracleRows(rows), `${context}: nothing written`).toEqual([]);
      reach?.reach("refused typed");
      return "refused";
    }
    const read =
      history.form === "insert"
        ? await core.execute("SELECT v FROM t WHERE id = 1")
        : (outcome as Awaited<ReturnType<StorageCore["execute"]>>);
    if (!("batches" in read)) throw new Error(`${context}: rows`);
    const [row] = oracleRows(read);
    const text = (Object.values(row ?? {})[0] ?? "") as string;
    expect(text.length, `${context}: the value`).toBe(expected);
    reach?.reach(history.held ? "served beside a held read" : "served alone");
    reach?.reach(history.form);
    if (history.separator !== "") reach?.reach("separator");
    return "served";
  } finally {
    if (end !== null) await end().catch(() => undefined);
    await core.close();
    Governor.stallAfterMs = null;
  }
}

it("writes an aggregated value beside the modification's spool (the reported refusal)", async () => {
  expect(
    await run({
      capMiB: 32,
      parts: 3,
      width: 1_000_000,
      separator: "",
      held: true,
      form: "insert",
    }),
  ).toBe("served");
}, 60_000);

it("refuses a result-owned value beyond its admission typed, never untyped", async () => {
  const governor = new Governor(8 * MiB);
  const grant = await governor.admit({ label: "result", minimumBytes: 1024, spillable: true });
  const channel = new ResultChannel([{ name: "v", type: { kind: "text" }, nullable: true }], {
    windowBytes: 64,
  });
  const sink = new ResultSink([{ kind: "text" }], grant, channel, 0, { now: () => 0 });
  const outcome = await sink.allocateOwnedResult(MiB).then(
    () => "allocated",
    (error: unknown) => describe(error),
  );
  expect(outcome).toMatch(/^MemoryAdmissionError 53200 result result-owned value/u);
  grant.close();
});

for (const seed of seedsFor("next-string-agg-sizing", [0x5a01, 0x5a02]))
  it(`string_agg is served within its bound or refused typed (seed ${String(seed)})`, async () => {
    const reach = createReach("string_agg sizing", STRING_AGG_SIZING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          capMiB: fc.constantFrom(24, 32, 48, 64),
          parts: fc.integer({ min: 1, max: 12 }),
          width: fc.constantFrom(200_000, 500_000, 1_000_000),
          separator: fc.constantFrom<"" | ",">("", ","),
          held: fc.boolean(),
          form: fc.constantFrom<"insert" | "select">("insert", "select"),
        }),
        async (history) => {
          reach.beginHistory();
          await run(history, reach);
        },
      ),
      { seed, numRuns: 30 },
    );
    reach.assertReached();
  }, 600_000);
