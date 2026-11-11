/** Checkpoint prefixes and unread suffixes partition one selected/dictionary input stream. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  JSONB_CONTINUATION_REACH,
  JSONB_DEFERRED_REACH,
} from "../../testing/reach.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { Batch, Vector } from "../vector/vector.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { OutputArena } from "./kernels/vectors.js";
import {
  I32,
  TEXT,
  col,
  dictionary,
  lit,
  numbers,
  texts,
  valueAt,
} from "./kernels/test-support.js";
import type { Operator } from "./pipeline.js";
import { JsonPhaseOperator, closeJsonPhase, type JsonPhase } from "./json-phase.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const expr: ScalarExpr = {
  op: "call",
  fn: "json_get_text",
  type: TEXT,
  args: [
    { op: "cast", input: col(0, TEXT), type: { kind: "json", binary: true } },
    lit(texts(["n"])),
  ],
};
/** Input vectors belong to the fixture; this helper retires every native consumer grant. */
async function drain(
  batches: readonly Batch[],
  input: readonly PhysicalType[],
  exprs: readonly ScalarExpr[],
  stopAfter = Infinity,
): Promise<{ output: unknown[][]; cuts: number; sourceRows: number }> {
  const output: unknown[][] = [];
  const sink: Operator = {
    push(batch) {
      for (let i = 0; i < (batch.selection?.count ?? batch.length); i++)
        output.push(batch.columns.map((column) => valueAt(column, batch.selection?.rows[i] ?? i)));
      return "ready";
    },
    ready: () => Promise.resolve(),
    finish: () => undefined,
    abort: () => undefined,
  };
  const governor = new Governor(32 << 20),
    spill = new MemorySpillService();
  let saved: JsonPhase | undefined,
    sourceRows = 0,
    cuts = 0;
  try {
    for (;;) {
      const prior = saved;
      saved = undefined;
      const capacity = prior?.emit ? 0 : Math.max(4096, prior?.capacity ?? 0),
        retainedBytes = prior?.retainedBytes ?? 0;
      const grant = await governor.admit({
        label: "JSON native phase",
        minimumBytes: (8 << 20) + capacity + retainedBytes,
        spillable: false,
      });
      const arena = new OutputArena(
          { allocate: (n) => grant.allocate(n), free: (n) => grant.free(n) },
          2048,
        ),
        disposers: Array<() => unknown> = [];
      let operator: JsonPhaseOperator | undefined;
      try {
        operator = new JsonPhaseOperator(
          exprs,
          { arena, input, params: { slots: [] } },
          {
            grant,
            spill,
            queryId: 1,
            capacity,
            retainedBytes,
            progressBytes: 0,
            signal: new AbortController().signal,
            // A retaining parent can communicate stop after delivery without a numeric input hint.
            stopped: () => output.length >= stopAfter,
            ...(prior ? { resume: prior } : {}),
            dispose: (release) => {
              disposers.push(release);
            },
            handoff: (phase) => {
              saved = phase;
              cuts++;
              return Promise.resolve();
            },
          },
          exprs.map((e) => e.type),
          sink,
        );
        if (prior) await operator.resume();
        else {
          for (const batch of batches) {
            operator.push(batch);
            await operator.ready();
            sourceRows += batch.selection?.count ?? batch.length;
          }
          await operator.finish();
        }
      } finally {
        operator?.abort(new Error("phase retired"));
        await operator?.closed;
        for (const dispose of disposers.reverse()) await dispose();
        arena.close();
        grant.close();
        if (prior) await closeJsonPhase(prior);
      }
      expect(governor.usedBytes).toBe(0);
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- awaited handoff callback publishes saved
      if (!saved) break;
      if (cuts > 20) throw new Error("Non-progressing JSON continuation");
    }
    return { output, cuts, sourceRows };
  } finally {
    if (saved) await closeJsonPhase(saved);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }
}
it.each(seedsFor("next-jsonb-continuations", [0xb68a, 0xb68b]))(
  "partitions unread input exactly across cuts (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB continuation stream", JSONB_CONTINUATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 128 }),
        fc.array(fc.integer({ min: 1, max: 128 }), { minLength: 1, maxLength: 4 }),
        fc.boolean(),
        fc.boolean(),
        async (first, pattern, selected, encoded) => {
          reach.beginHistory();
          const docs = Array.from({ length: 257 }, (_, i) =>
            i === first
              ? '{"n":1e5000}'
              : i === first + 1
                ? '{"n":1e10000}'
                : i % 7 === 0
                  ? null
                  : JSON.stringify({ n: `row-${String(i)}` }),
          );
          const expected = (
            await pg.query<{ n: string | null }>(
              "SELECT doc::jsonb ->> 'n' AS n FROM unnest($1::text[]) WITH ORDINALITY AS q(doc,ord) ORDER BY ord",
              [docs],
            )
          ).rows.map((r) => [r.n]);
          const batches: Batch[] = [];
          for (let at = 0, part = 0; at < docs.length; part++) {
            const size = Math.min(pattern[part % pattern.length] ?? 1, docs.length - at),
              partDocs = docs.slice(at, at + size);
            const values = selected
              ? partDocs.flatMap((d) => [d, "invalid-unselected-json"])
              : partDocs;
            const flat = texts(values),
              vector: Vector = encoded
                ? dictionary(
                    flat,
                    values.map((v, i) => (v === null ? null : i)),
                  )
                : flat;
            batches.push({
              length: values.length,
              columns: [vector],
              selection: selected
                ? { rows: Uint16Array.from(partDocs.map((_, i) => 2 * i)), count: size }
                : null,
            });
            at += size;
          }
          const actual = await drain(batches, [TEXT], [expr]);
          expect(actual.sourceRows).toBe(docs.length);
          expect(actual.output).toEqual(expected);
          expect(actual.cuts).toBeGreaterThanOrEqual(3);
          for (const label of [
            "completed prefix",
            "current row",
            "unread suffix",
            "multiple growth cuts",
            "source opened once",
          ])
            reach.reach(label);
          reach.reach(selected ? "selection" : "dense");
          reach.reach(encoded ? "dictionary" : "flat");
          if (pattern.length > 1) reach.reach("unequal boundaries");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [2, [1], false, false],
          [64, [17, 64], true, false],
          [127, [128, 1], false, true],
          [128, [1, 128, 17], true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
it.each(seedsFor("next-jsonb-deferred-errors", [0xb690, 0xb691]))(
  "preserves deferred row-error position across demand (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB deferred error position", JSONB_DEFERRED_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 0, max: 2 }),
        fc.boolean(),
        async (arithmetic, limit, grows) => {
          reach.beginHistory();
          const docs = [
              grows ? '{"n":1e5000}' : '{"n":"ok"}',
              arithmetic ? '{"n":"later"}' : "[1,]",
            ],
            ids = [1, arithmetic ? 0 : 1];
          const sql = `SELECT doc::jsonb ->> 'n' AS n,10/id AS x FROM(VALUES($1::text,$2::int4),($3::text,$4::int4))q(doc,id) LIMIT ${String(limit)}`;
          let expected: unknown;
          try {
            expected = {
              rows: (
                await pg.query<{ n: string; x: number }>(sql, [docs[0], ids[0], docs[1], ids[1]])
              ).rows.map((r) => [r.n, r.x]),
            };
          } catch (e) {
            expected = { state: (e as { code: string }).code };
          }
          const batches = docs.map((doc, i) => ({
            length: 1,
            columns: [texts([doc]), numbers(I32, [ids[i] ?? 1])],
            selection: null,
          }));
          const divide: ScalarExpr = {
            op: "call",
            fn: "div",
            args: [lit(numbers(I32, [10])), col(1, I32)],
            type: I32,
          };
          let actual: unknown;
          try {
            actual = { rows: (await drain(batches, [TEXT, I32], [expr, divide], limit)).output };
          } catch (e) {
            actual = { state: (e as { sqlState: string }).sqlState };
          }
          expect(actual).toEqual(expected);
          reach.reach(arithmetic ? "arithmetic error" : "JSON error");
          reach.reach(`limit ${String(limit)}`);
          reach.reach(grows ? "growth" : "no growth");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [false, 1, true],
          [true, 1, false],
          [false, 0, false],
          [true, 0, true],
          [false, 2, true],
          [true, 2, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
