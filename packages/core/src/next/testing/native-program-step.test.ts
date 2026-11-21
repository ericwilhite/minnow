/** Complete-input producers retire before root/body/RI application (native-input.ts).
 * Retain immutable plans and schemas, but fund only one compiled producer per step. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PROGRAM_STEP_REACH } from "../../testing/reach.js";
import { NativeBodyPrograms, nativeBodyProgramMinimumBytes } from "../owner/body-programs.js";
import { NATIVE_INPUT_CAPTURE_BYTES, nativeInputProgramBytes } from "../owner/native-input.js";
import { PrepaidQueryGovernor, PREPAID_QUERY_BYTES } from "../owner/prepaid-query.js";
import { nativeParameterMinimumBytes } from "../exec/native-parameters.js";
import * as compiler from "../exec/plan.js";
import { numbers, I32, valueAt } from "../exec/kernels/test-support.js";
import { ChunkReader } from "../page/reader.js";
import { NO_RULES } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("next-native-program-step", [49537, 49538]))
  it(`funds one compiled producer across retained plans (${String(seed)})`, async () => {
    const reach = createReach("native program step", NATIVE_PROGRAM_STEP_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 16 }), fc.boolean(), async (count, failure) => {
        reach.beginHistory();
        const core = await new StorageCore(32).open();
        try {
          await core.execute("CREATE TABLE t(n INT)");
          const lowered = core.lowerer.lower(
            `INSERT INTO t VALUES($1),(${failure ? "1/0" : "$1+1"})`,
            core.context(),
          );
          if (lowered.kind !== "modify") throw new Error("Missing input plan");
          const entries = Array.from({ length: count }, () => ({
            body: null,
            plan: { ...lowered.plan },
          }));
          const legacy =
            4096 +
            entries.reduce(
              (sum, { plan }) =>
                sum +
                1024 +
                256 * (plan.root.op === "modify" ? plan.root.input.output.length : 0) +
                nativeInputProgramBytes(plan),
              0,
            );
          expect(
            nativeBodyProgramMinimumBytes(entries),
            "a fitting legacy minimum never rises",
          ).toBeLessThanOrEqual(legacy);
          reach.reach("fitting legacy minimum never rises");
          const bytes =
            nativeBodyProgramMinimumBytes(entries) +
            NATIVE_INPUT_CAPTURE_BYTES +
            nativeParameterMinimumBytes(lowered.plan) +
            PREPAID_QUERY_BYTES +
            131072;
          const metadata = core.harness.governor.tryAdmit({
            label: "retained programs",
            minimumBytes: bytes,
            spillable: true,
          });
          const source = core.harness.governor.tryAdmit({
            label: "one producer",
            minimumBytes: 0,
            spillable: true,
          });
          if (!metadata || !source) throw new Error("Missing root grant");
          const compile = vi.spyOn(compiler, "compileModificationInput");
          const tx = await core.harness.path.begin(),
            statement = tx.statement({ rules: () => NO_RULES }),
            pin = statement.readOwn();
          let programs: NativeBodyPrograms | null = null;
          try {
            programs = new NativeBodyPrograms(tx.catalog, entries, metadata);
            expect(compile, "constructors retain no compiled producer").not.toHaveBeenCalled();
            const retained = metadata.allocated;
            for (const [index, entry] of entries.entries())
              for (let run = 0; run < 2; run++) {
                const query = new PrepaidQueryGovernor({
                  governor: core.harness.governor,
                  metadata,
                  source,
                  signal: new AbortController().signal,
                  prepare: async (n, signal) => {
                    if (!(await source.makeRoom?.(n, signal))) throw new Error("source step");
                  },
                  prepareMetadata: async () => {
                    throw new Error("Compiled producer exceeded its complete minimum");
                  },
                });
                const seen: unknown[] = [];
                try {
                  const result = await programs.get(entry.plan, null).input.run(
                    {
                      inputView: pin,
                      parameters: { slots: [numbers(I32, [index])] },
                      signal: new AbortController().signal,
                      metadata,
                      execution: {
                        governor: query,
                        reads: {
                          open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
                        },
                        spill: core.spill,
                        queryId: 71,
                        statementTime: 0n,
                        sequences: null,
                        sequenceSession: null,
                        returning: "atExecuted",
                      },
                    },
                    {
                      push: (batch, transfer) => {
                        const vector = batch.columns[0];
                        if (!vector) throw new Error("Missing native producer field");
                        for (let k = 0; k < (batch.selection?.count ?? batch.length); k++)
                          seen.push(valueAt(vector, batch.selection?.rows[k] ?? k));
                        transfer?.release();
                        return "ready";
                      },
                      ready: () => Promise.resolve(),
                      finish: () => undefined,
                      abort: () => undefined,
                    },
                  );
                  expect(result?.sqlState ?? null).toBe(failure ? "22012" : null);
                  expect(seen).toEqual(failure ? [index] : [index, index + 1]);
                } finally {
                  await query.close();
                }
                expect(metadata.allocated, "compiled state retired after the producer fence").toBe(
                  retained,
                );
                expect(source.allocated).toBe(0);
              }
            expect(compile).toHaveBeenCalledTimes(2 * count);
            reach.reach(count > 1 ? "many immutable plans" : "one immutable plan");
            reach.reach(failure ? "terminal error prefix" : "successful producer");
            reach.reach("repeated invocations");
            reach.reach("compiled state retired at each fence");
          } finally {
            await programs?.close();
            pin.release();
            statement.rollback();
            tx.rollback();
            compile.mockRestore();
            expect(metadata.allocated).toBe(0);
            metadata.close();
            source.close();
          }
        } finally {
          await core.close();
        }
        expect(core.harness.governor.usedBytes).toBe(0);
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, false],
          [16, true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
