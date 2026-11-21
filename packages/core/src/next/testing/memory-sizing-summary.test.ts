/**
 * Memtable sizing summaries (write/memtable.ts `valueSummary`, `MemorySegment.readSummary`) bound
 * what one-row `readHint`s over the same chunk report, after inserts with nulls, updates (new
 * versions), deletes, a rolled-back transaction, an added column (catalog-only, read as its missing
 * value) and publication (re-pointed readers size row by row: no summary survives). Law (page/read-bounds.ts, one-row sizing): the
 * summary of a chunk is at least the largest one-row output, at least the sum of the one-row
 * outputs, and at least every reader term of a one-row hint; summaries only grow within a chunk.
 * Where no row was dropped or appended after the pin and one varlen column is read, the summary is
 * exact: a summary never pays for more than the widest row of that column.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MEMORY_SIZING_SUMMARY_REACH } from "../../testing/reach.js";
import type { ByteBudget } from "../access/space.js";
import {
  MEMORY_CHUNK_ROWS,
  type MemoryReadContext,
  type MemoryReadSummary,
  type MemorySegment,
  type Snapshot,
} from "../access/snapshot.js";
import type { Memtable } from "../write/memtable.js";
import { batch, ints, rules, texts } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";

class Budget implements ByteBudget {
  used = 0;
  allocate(n: number): boolean {
    this.used += n;
    return true;
  }
  free(n: number): void {
    this.used -= n;
  }
}

/** Deterministic widths from one generated salt (fast-check shrinks the salt, not the rows). */
function widths(salt: number, count: number, max: number): number[] {
  let state = salt >>> 0 || 1;
  return Array.from({ length: count }, () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state % (max + 1);
  });
}
const text = (width: number, at: number): string =>
  at % 3 === 0 ? "é".repeat(Math.floor(width / 2)) : "x".repeat(width);

function memorySegments(snapshot: Snapshot, table: number): MemorySegment[] {
  const view = snapshot.table(table, { budget: new Budget() });
  if (view instanceof Promise) throw new Error("resident table view expected");
  return view.segments.filter((s): s is MemorySegment => s.kind === "memory");
}

for (const seed of seedsFor("next-memory-sizing-summary", [52011, 52012]))
  it(`bounds every one-row hint of a chunk by its summary (${String(seed)})`, async () => {
    const reach = createReach("memory sizing summaries", MEMORY_SIZING_SUMMARY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          rows: fc.constantFrom(40, 2_500, 4_500),
          // Below the memtable's 8 MiB freeze, so the pinned rows stay in memory segments.
          maxWidth: fc.constantFrom(8, 300, 1_200),
          salt: fc.integer({ min: 1, max: 0x7fffffff }),
          nulls: fc.boolean(),
          update: fc.boolean(),
          remove: fc.boolean(),
          rollback: fc.boolean(),
          alter: fc.boolean(),
          publish: fc.boolean(),
        }),
        async (s) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          let snapshot: Snapshot | null = null;
          try {
            await core.execute("CREATE TABLE t(id INT PRIMARY KEY, a TEXT, b TEXT)");
            const id = core.harness.path.catalog.tableByName("t")?.id ?? -1;
            const a = widths(s.salt, s.rows, s.maxWidth),
              b = widths(s.salt ^ 0x5bd1e995, s.rows, s.maxWidth);
            for (let from = 0; from < s.rows; from += 1_000)
              await core.writeBatch(
                "insertBatch",
                "t",
                Array.from({ length: Math.min(1_000, s.rows - from) }, (_, k) => {
                  const at = from + k;
                  return {
                    id: at,
                    a: s.nulls && at % 4 === 1 ? null : text(a[at] ?? 0, at),
                    b: s.nulls && at % 5 === 2 ? null : text(b[at] ?? 0, at + 1),
                  };
                }),
              );
            if (s.nulls) reach.reach("null values");
            if (s.update) {
              // New versions: wider than any inserted value, appended to the memtable.
              await core.execute("UPDATE t SET a = $1 WHERE id % 7 = 3", [
                "u".repeat(s.maxWidth + 17),
              ]);
              reach.reach("updated rows");
            }
            if (s.remove) {
              await core.execute("DELETE FROM t WHERE id % 5 = 1");
              reach.reach("deleted rows");
            }
            if (s.rollback) {
              // Wide rows written and rolled back: their chunk keeps summaries that cover them.
              const tx = await core.harness.path.begin();
              const statement = tx.statement({ rules });
              const ids = Array.from({ length: 30 }, (_, k) => 1_000_000 + k);
              await statement
                .table(id)
                .insert(
                  batch(
                    ints(ids),
                    texts(ids.map(() => "r".repeat(4_000))),
                    texts(ids.map(() => "r".repeat(4_000))),
                  ),
                  null,
                );
              await statement.end();
              tx.rollback();
              reach.reach("rolled back rows");
            }
            if (s.alter) {
              await core.execute(
                `ALTER TABLE t ADD COLUMN c TEXT DEFAULT '${"d".repeat(s.maxWidth)}'`,
              );
              reach.reach("added column");
            }
            snapshot = core.harness.path.pin();
            const table = core.harness.path.catalog.table(id);
            if (table === undefined) throw new Error("missing table");
            const ids = table.columns.map((column) => column.id);
            const [pk, colA, colB] = ids;
            if (pk === undefined || colA === undefined || colB === undefined)
              throw new Error("missing columns");
            const colC = ids[3];
            if (s.publish) {
              // Rows after the pin, then publication: the pinned readers are re-pointed.
              await core.writeBatch(
                "insertBatch",
                "t",
                Array.from({ length: 50 }, (_, k) => ({
                  id: 2_000_000 + k,
                  a: "p".repeat(s.maxWidth * 2),
                  b: "p",
                })),
              );
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
            }
            const requests: number[][] = [[colA], [pk], [colA, colB], [pk, colA, colB]];
            if (colC !== undefined) requests.push([colC], [colA, colC]);
            const budget = new Budget();
            const context: MemoryReadContext = {
              budget,
              pages: snapshot.pages,
              signal: new AbortController().signal,
            };
            const segments = memorySegments(snapshot, id);
            expect(segments.length, "the pinned rows are in a memory segment").toBeGreaterThan(0);
            for (const segment of segments) {
              const memtable = (segment as unknown as { memtable: Memtable }).memtable;
              const chunks = Math.ceil(segment.rowCount / MEMORY_CHUNK_ROWS);
              if (chunks > 1) reach.reach("several chunks");
              // Publication's release drops the chunks and their summaries: re-pointed readers then
              // size row by row (the summary answers null, the caller's fallback).
              // A background flush may publish it while this history reads: checked at each read.
              const published = () => memtable.published !== null || memtable.state === "published";
              // Exact unless rows were dropped (rollback) or appended after the pin (publish).
              const exact = !s.rollback && !s.publish;
              for (const columns of requests) {
                const varlen = columns.filter((column) => column !== pk).length;
                if (varlen > 1) reach.reach("several varlen columns");
                const whole = { largest: 0, total: 0, resident: 0, scratch: 0, io: 0 };
                // A run's summary against the one-row hints of its rows.
                const check = (
                  summary: MemoryReadSummary | null | undefined,
                  hints: typeof whole,
                  where: string,
                ) => {
                  // Until its rows are released, a published memtable still summarizes them.
                  if (published()) reach.reach("published chunk");
                  if (published() && summary === null) return;
                  if (summary === undefined || summary === null)
                    throw new Error(`${where}: a memtable run lacks its summary`);
                  expect(summary.rowBytes, where).toBeGreaterThanOrEqual(hints.largest);
                  expect(summary.totalBytes, where).toBeGreaterThanOrEqual(hints.total);
                  expect(summary.residentBytes, where).toBeGreaterThanOrEqual(hints.resident);
                  expect(summary.scratchBytes, where).toBeGreaterThanOrEqual(hints.scratch);
                  expect(summary.ioBytes, where).toBeGreaterThanOrEqual(hints.io);
                  if (exact && varlen === 1) {
                    expect(summary.rowBytes, where).toBe(hints.largest);
                    expect(summary.totalBytes, where).toBe(hints.total);
                    reach.reach("exact for one varlen column");
                  }
                };
                for (let chunk = 0; chunk < chunks; chunk++) {
                  const rows = Math.min(
                    MEMORY_CHUNK_ROWS,
                    segment.rowCount - chunk * MEMORY_CHUNK_ROWS,
                  );
                  const part = { largest: 0, total: 0, resident: 0, scratch: 0, io: 0 };
                  for (let row = 0; row < rows; row++) {
                    const hint = await segment.readHint(
                      { columns, chunk, startRow: row, maxRows: 1, targetBytes: 4096 },
                      context,
                    );
                    part.largest = Math.max(part.largest, hint.outputBytes);
                    part.total += hint.outputBytes;
                    part.resident = Math.max(part.resident, hint.residentBytes);
                    part.scratch = Math.max(part.scratch, hint.scratchBytes);
                    part.io = Math.max(part.io, hint.ioBytes);
                  }
                  check(
                    await segment.readSummary?.({ columns, chunk, rows }, context),
                    part,
                    `chunk ${String(chunk)} columns ${columns.join(",")}`,
                  );
                  whole.largest = Math.max(whole.largest, part.largest);
                  whole.total += part.total;
                  whole.resident = Math.max(whole.resident, part.resident);
                  whole.scratch = Math.max(whole.scratch, part.scratch);
                  whole.io = Math.max(whole.io, part.io);
                }
                // The run read-bounds sizes: every row of the segment, in one summary.
                check(
                  await segment.readSummary?.(
                    { columns, chunk: 0, rows: segment.rowCount },
                    context,
                  ),
                  whole,
                  `whole segment columns ${columns.join(",")}`,
                );
              }
            }
            expect(budget.used).toBe(0);
          } finally {
            snapshot?.release();
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 10,
        examples: [
          [
            {
              rows: 4_500,
              maxWidth: 300,
              salt: 7,
              nulls: true,
              update: true,
              remove: true,
              rollback: true,
              alter: true,
              publish: true,
            },
          ],
          [
            {
              rows: 2_500,
              maxWidth: 1_200,
              salt: 11,
              nulls: false,
              update: true,
              remove: true,
              rollback: false,
              alter: true,
              publish: false,
            },
          ],
        ],
      },
    );
    reach.assertReached();
  }, 600_000);
