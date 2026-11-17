/** Law: COMPLETE MODIFY input uses the root's native workspace and retains a successful
 * source prefix before a terminal data error; it never mutates or ends the statement. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_INPUT_REACH,
  NATIVE_INPUT_RETENTION_REACH,
} from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { Governor, type Grant } from "../memory/governor.js";
import { NativeInputProgram, nativeInputProgramBytes } from "../owner/native-input.js";
import { PrepaidQueryGovernor } from "../owner/prepaid-query.js";
import { numbers, I32, valueAt } from "../exec/kernels/test-support.js";
import { ChunkReader } from "../page/reader.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import type { Batch } from "../vector/vector.js";
function grant(gov: Governor, label: string, minimumBytes: number): Grant {
  const g = gov.tryAdmit({ label, minimumBytes, spillable: true });
  if (!g) throw new Error("root admission");
  return g;
}
function hasIndex(rel: Rel): boolean {
  return (
    rel.op === "keyLookup" || rel.op === "indexRange" || ("input" in rel && hasIndex(rel.input))
  );
}
for (const seed of seedsFor("next-native-input", [0xb659, 0xb65a]))
  it(`compiles typed body inputs without writes seed ${String(seed)}`, async () => {
    const reach = createReach("native input producer", NATIVE_INPUT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 3 }),
        fc.integer({ min: 1, max: 9 }),
        fc.boolean(),
        fc.boolean(),
        async (kind, n, published, dataError) => {
          reach.beginHistory();
          const core = await new StorageCore(16).open();
          let program: NativeInputProgram | null = null,
            query: PrepaidQueryGovernor | null = null;
          let metadata: Grant | null = null,
            source: Grant | null = null;
          const stop = new AbortController();
          try {
            await core.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)");
            await core.execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)");
            if (published) {
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              reach.reach("published");
            } else reach.reach("resident");
            const sql =
              kind === 0
                ? `INSERT INTO t VALUES(4,$1),(5,${dataError ? "1/0" : "$1+1"})`
                : kind === 1
                  ? "UPDATE t SET v=v+$1 WHERE id=2"
                  : kind === 2
                    ? "DELETE FROM t WHERE id=$1"
                    : "UPDATE t SET v=v+$1 WHERE v>0";
            const lowered = core.lowerer.lower(sql, core.context());
            if (lowered.kind !== "modify") throw new Error("modify");
            const plan: PhysicalPlan = lowered.plan;
            if (plan.root.op !== "modify") throw new Error("root");
            if (kind === 1 || kind === 2) {
              expect(hasIndex(plan.root)).toBe(true);
              reach.reach("index input");
            } else if (kind === 3) reach.reach("scan input");
            else reach.reach("VALUES input");
            const tx = await core.harness.path.begin(),
              statement = tx.statement({
                rules: () => {
                  throw new Error("Input-only producer requested row rules");
                },
              }),
              pin = statement.readOwn(),
              gov = core.harness.governor;
            metadata = grant(gov, "native root metadata", 2 << 20);
            source = grant(gov, "native query source", 0);
            const meta = metadata,
              src = source;
            query = new PrepaidQueryGovernor({
              governor: gov,
              metadata: meta,
              source: src,
              signal: stop.signal,
              prepare: async (bytes, signal) => {
                if (!(await src.makeRoom?.(bytes, signal))) throw new Error("source minimum");
              },
              prepareMetadata: async (bytes, signal) => {
                if (!(await meta.makeRoom?.(bytes, signal))) throw new Error("metadata minimum");
              },
            });
            const native = Object.freeze(numbers(I32, [n]));
            const seen: unknown[][] = [];
            const beforeProgram = meta.allocated;
            program = new NativeInputProgram(plan, meta);
            expect(meta.allocated - beforeProgram).toBe(nativeInputProgramBytes(plan));
            try {
              const error = await program.run(
                {
                  inputView: pin,
                  parameters: { slots: [native] },
                  signal: stop.signal,
                  metadata: meta,
                  execution: {
                    governor: query,
                    spill: core.spill,
                    queryId: 72,
                    statementTime: 123n,
                    reads: {
                      open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
                    },
                    sequences: null,
                    sequenceSession: null,
                    returning: "atExecuted",
                  },
                },
                {
                  push: (batch: Batch, transfer) => {
                    for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
                      const row = batch.selection?.rows[k] ?? k;
                      seen.push(batch.columns.map((v) => structuredClone(valueAt(v, row))));
                    }
                    transfer?.release();
                    return "ready";
                  },
                  ready: () => Promise.resolve(),
                  finish: () => undefined,
                  abort: () => undefined,
                },
              );
              const failure = kind === 0 && dataError;
              expect(error?.sqlState ?? null).toBe(failure ? "22012" : null);
              const count =
                kind === 0 ? (failure ? 1 : 2) : kind === 1 ? 1 : kind === 2 ? (n <= 3 ? 1 : 0) : 3;
              expect(seen).toHaveLength(count);
              if (kind === 0) {
                expect(seen[0]).toEqual([4, n]);
                if (!failure) expect(seen[1]).toEqual([5, n + 1]);
              }
              if (kind === 1) expect(seen[0]?.[1]).toBe(20 + n);
              if (kind === 3) expect(seen.map((row) => row[1])).toEqual([10 + n, 20 + n, 30 + n]);
              if (failure) reach.reach("terminal error prefix");
              else reach.reach("successful prefix");
              expect(src.allocated).toBe(0);
              expect(meta.allocated - beforeProgram).toBe(nativeInputProgramBytes(plan));
              expect(native.data.layout).toBe("i32");
              if (native.data.layout === "i32") expect(native.data.values[0]).toBe(n);
              expect(pin.released).toBe(false);
              reach.reach("parameters retired before apply");
            } finally {
              await program.close();
              program = null;
              await query.close();
              query = null;
              pin.release();
              statement.rollback();
              tx.rollback();
            }
            source.close();
            metadata.close();
            source = metadata = null;
            const actual = await core.execute("SELECT id,v FROM t ORDER BY id");
            if (!("batches" in actual)) throw new Error("Expected unchanged table rows");
            expect(oracleRows(actual)).toEqual([
              { id: 1, v: 10 },
              { id: 2, v: 20 },
              { id: 3, v: 30 },
            ]);
            reach.reach("no writes");
          } finally {
            stop.abort();
            await program?.close();
            await query?.close();
            metadata?.close();
            source?.close();
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-native-input-retention", [0xb667, 0xb668]))
  it(`charges shared literal payload once instead of multiplying its minimum seed ${String(seed)}`, async () => {
    const reach = createReach("native input literal retention", NATIVE_INPUT_RETENTION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: 2 << 20 }), async (width) => {
        reach.beginHistory();
        reach.reach(
          width === 0 ? "empty literal" : width >= 1 << 20 ? "large literal" : "small literal",
        );
        const { texts, TEXT } = await import("../exec/kernels/test-support.js");
        const make = (n: number): PhysicalPlan => {
          const value = texts(["x".repeat(n)]),
            literal = { op: "literal" as const, value, type: TEXT },
            columns = [
              { name: "a", type: TEXT, nullable: false },
              { name: "b", type: TEXT, nullable: false },
            ];
          return {
            root: {
              id: 2,
              op: "modify",
              kind: "insert",
              tableId: 1,
              input: { id: 1, op: "values", rows: [[literal, literal]], output: columns },
              assigned: [],
              onRepeat: "first",
              onConflict: null,
              returning: null,
              output: [],
            },
            columns: [],
            parameters: [],
            placeholderCount: 0,
            clockSlot: null,
            initPlans: [],
            catalogVersion: 0,
            volatile: false,
          };
        };
        const plan = make(width),
          bytes = nativeInputProgramBytes(plan),
          base = nativeInputProgramBytes(make(0));
        expect(bytes - base).toBe(width);
        const gov = new Governor(16 << 20),
          paid = grant(gov, "literal program", bytes),
          program = new NativeInputProgram(plan, paid);
        expect(paid.allocated).toBe(bytes);
        await program.close();
        expect(paid.allocated).toBe(0);
        paid.close();
        expect(gov.usedBytes).toBe(0);
        reach.reach("shared aliases");
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 24, examples: [[0], [1], [2 << 20]] },
    );
    reach.assertReached();
  });
