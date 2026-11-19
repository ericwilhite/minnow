/** One full-key cursor is already row-id ordered. Prefixes and multiple probes
 * retain locator sorting, including duplicate probes and non-key insertion order. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_INPUT_PROBE_ORDER_REACH } from "../../testing/reach.js";
import type { Rel, ScalarExpr } from "../ir/ir.js";
import { NativeInputProgram } from "../owner/native-input.js";
import { PrepaidQueryGovernor } from "../owner/prepaid-query.js";
import { numbers, I32, valueAt } from "../exec/kernels/test-support.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("next-native-input-probe-order", [0xb739, 0xb73a]))
  it(`preserves input row order for full and partial probes seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native probe order", NATIVE_INPUT_PROBE_ORDER_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 0, max: 2 }), fc.boolean(), async (mode, reverse) => {
          reach.beginHistory();
          const core = await new StorageCore(64).open(),
            signal = new AbortController().signal;
          const rows = [
            [1, 3, 30],
            [1, 1, 10],
            [1, 2, 20],
            [2, 1, 40],
          ];
          if (reverse) rows.reverse();
          try {
            const ddl = "CREATE TABLE t(a INTEGER,b INTEGER,v INTEGER,PRIMARY KEY(a,b))";
            await core.execute(ddl);
            await pg.exec("DROP TABLE IF EXISTS t;" + ddl);
            for (const row of rows) {
              await core.execute("INSERT INTO t VALUES($1,$2,$3)", row);
              await pg.query("INSERT INTO t VALUES($1,$2,$3)", row);
            }
            const lowered = core.lowerer.lower(
              "UPDATE t SET v=v+1 WHERE a=$1 AND b=$2",
              core.context(),
            );
            if (lowered.kind !== "modify") throw new Error("missing MODIFY");
            let found = false;
            const literal = (n: number): ScalarExpr => ({
              op: "literal",
              type: I32,
              value: numbers(I32, [n]),
            });
            const rewrite = (rel: Rel): Rel => {
              if (rel.op === "keyLookup") {
                found = true;
                return {
                  ...rel,
                  keys:
                    mode === 0
                      ? [[literal(1), literal(1)]]
                      : mode === 1
                        ? [
                            [literal(1), literal(3)],
                            [literal(1), literal(1)],
                            [literal(1), literal(3)],
                          ]
                        : [[literal(1)]],
                };
              }
              if (rel.op === "filter") return rewrite(rel.input);
              return "input" in rel ? { ...rel, input: rewrite(rel.input) } : rel;
            };
            const plan = { ...lowered.plan, root: rewrite(lowered.plan.root) };
            expect(found, "planner-selected composite lookup").toBe(true);
            const tx = await core.harness.path.begin(),
              statement = tx.statement({
                rules: () => {
                  throw new Error("input requested row rules");
                },
              }),
              pin = statement.readOwn(),
              gov = core.harness.governor;
            const meta = await gov.admit({
                label: "probe program",
                minimumBytes: 2 << 20,
                spillable: true,
              }),
              source = await gov.admit({ label: "probe source", minimumBytes: 0, spillable: true });
            const query = new PrepaidQueryGovernor({
                governor: gov,
                metadata: meta,
                source,
                signal,
                prepare: async (n, s) => {
                  expect(await source.makeRoom?.(n, s)).toBe(true);
                },
                prepareMetadata: async (n, s) => {
                  expect(await meta.makeRoom?.(n, s)).toBe(true);
                },
              }),
              program = new NativeInputProgram(plan, meta),
              seen: number[] = [];
            const admit = query.admitGroup.bind(query);
            const spy = vi.spyOn(query, "admitGroup").mockImplementation((options) => {
              if (mode !== 2 && options.label.endsWith("/fixed-buffers"))
                expect(
                  options.grants.some((p) => /\/sort#[0-9]+\/minimum$/u.test(p.label)),
                  "prefixes and multiple probes require sorting",
                ).toBe(mode !== 0);
              return admit(options);
            });
            try {
              const error = await program.run(
                {
                  inputView: pin,
                  parameters: { slots: [numbers(I32, [1]), numbers(I32, [1])] },
                  signal,
                  metadata: meta,
                  execution: {
                    governor: query,
                    spill: core.spill,
                    queryId: 90,
                    statementTime: 0n,
                    reads: {
                      open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
                    },
                    sequences: null,
                    sequenceSession: null,
                    returning: "atExecuted",
                  },
                },
                {
                  push: (batch, transfer) => {
                    for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
                      const row = batch.selection?.rows[k] ?? k;
                      const v = batch.columns[1];
                      if (!v) throw new Error("missing assigned value");
                      seen.push(Number(valueAt(v, row)));
                    }
                    transfer?.release();
                    return "ready";
                  },
                  ready: () => Promise.resolve(),
                  finish: () => undefined,
                  abort: () => undefined,
                },
              );
              expect(error).toBeNull();
              const expected = rows
                .filter(
                  (r) =>
                    r[0] === 1 &&
                    (mode === 2 || (mode === 0 ? r[1] === 1 : r[1] === 1 || r[1] === 3)),
                )
                .map((r) => (r[2] ?? 0) + 1);
              expect(seen, "physical row order and duplicate-target elimination").toEqual(expected);
              const predicate =
                mode === 0 ? "a=1 AND b=1" : mode === 1 ? "a=1 AND b IN(3,1,3)" : "a=1";
              const oracle = (
                await pg.query<{ v: number }>(
                  "UPDATE t SET v=v+1 WHERE " + predicate + " RETURNING v",
                )
              ).rows.map((r) => r.v);
              expect([...seen].sort((a, b) => a - b)).toEqual(oracle.sort((a, b) => a - b));
              reach.reach(
                mode === 0
                  ? "single complete key"
                  : mode === 1
                    ? "multiple probes"
                    : "composite prefix",
              );
              reach.reach("non-key insertion order");
            } finally {
              spy.mockRestore();
              await program.close();
              await query.close();
              pin.release();
              statement.rollback();
              tx.rollback();
              source.close();
              meta.close();
            }
          } finally {
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("zero ownership");
        }),
        {
          seed,
          numRuns: 10,
          examples: [
            [0, false],
            [1, true],
            [2, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
