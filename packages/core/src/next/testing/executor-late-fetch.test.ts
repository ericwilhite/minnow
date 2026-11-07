/** Winner payloads bypass admission to caches without increasing a sparse lease's charge. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-executor-one-pass-fetch", [0xd131, 0xd132]))(
  "reads sparse winners once and keeps only keys in the selection scan (seed %i)",
  async (seed) => {
    const reach = createReach("one-pass fetch", [
      "stored",
      "mixed",
      "payload-read",
      "exact-slab",
      "keys-only",
      "rows",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 16, max: 40 }),
        fc.integer({ min: 48, max: 200 }),
        fc.integer({ min: 1, max: 17 }),
        fc.boolean(),
        async (width, count, limit, mixed) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open(),
            governor = new Governor(64 << 20),
            fields = ["id", "k", ...Array.from({ length: width - 2 }, (_, k) => `p${String(k)}`)],
            keyRows = mixed ? count >>> 1 : count,
            input = Array.from({ length: count }, (_, id) =>
              Object.fromEntries(
                fields.map((name, k) => [
                  name,
                  k === 0
                    ? id
                    : k === 1
                      ? ((id % keyRows) * 7919) % keyRows
                      : (id + k) % 7 === 0
                        ? null
                        : `${String(id)}-${String(k)}-α-${"x".repeat(id % 31)}`,
                ]),
              ),
            );
          let snapshot: ReturnType<typeof core.harness.path.pin> | null = null;
          try {
            await core.execute(
              `CREATE TABLE t(id INTEGER,k INTEGER,${fields
                .slice(2)
                .map((n) => `${n} TEXT`)
                .join(",")})`,
            );
            const cut = mixed ? count >>> 1 : count;
            await core.writeBatch("insertBatch", "t", input.slice(0, cut));
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            if (mixed) await core.writeBatch("insertBatch", "t", input.slice(cut));
            const prepared = core.prepare(
                `SELECT * FROM t ORDER BY k DESC,id LIMIT ${String(limit)}`,
              ),
              definition = core.harness.path.catalog.tableByName("t");
            if (!definition) throw new Error("fixture table");
            snapshot = core.harness.path.pin();
            const fixture = await governor.admit({
              label: "fetch/fixture",
              minimumBytes: 4096,
              spillable: true,
            });
            const table = snapshot.table(definition.id, { budget: fixture });
            const chunks = new Map<object, number>();
            for (const segment of table.segments)
              if (segment.kind === "group") {
                await segment.root();
                for (const column of definition.columns) {
                  const chunk = segment.chunk(column.id);
                  if (chunk) chunks.set(chunk, column.id);
                }
              }
            fixture.close();
            const keys = new Set(definition.columns.slice(0, 2).map((c) => c.id)),
              selection = new Set<number>(),
              payload = new Set<number>();
            let payloadReads = 0;
            const execution = prepared.execute({
              parameters: [],
              snapshot,
              statement: null,
              statementTime: 0n,
              governor,
              spill: core.spill,
              queryId: 131,
              sequences: null,
              sequenceSession: null,
              returning: "atExecuted",
              reads: {
                open(pages, chunk, options) {
                  const id = chunks.get(chunk);
                  if (id === undefined) throw new Error("unknown chunk");
                  (options.once ? payload : selection).add(id);
                  let charged = 0;
                  const budget = {
                    allocate(bytes: number) {
                      if (!options.budget.allocate(bytes)) return false;
                      charged += bytes;
                      return true;
                    },
                    free(bytes: number) {
                      charged -= bytes;
                      options.budget.free(bytes);
                    },
                  };
                  return ChunkReader.open(
                    {
                      peek: (ref, policy) => pages.peek(ref, policy),
                      parsed: (ref) => pages.parsed?.(ref) ?? null,
                      attach: (ref, parsed, bytes, of) =>
                        pages.attach?.(ref, parsed, bytes, of) ?? false,
                      async read(refs, policy) {
                        const before = charged;
                        const lease = await pages.read(refs, policy);
                        try {
                          if (options.once) {
                            payloadReads++;
                            expect(policy.once).toBe(true);
                            if (refs.length === 1) {
                              expect(charged - before).toBe(refs[0]?.length);
                              reach.reach("exact-slab");
                            }
                          }
                        } catch (error) {
                          lease.release();
                          throw error;
                        }
                        return lease;
                      },
                    },
                    chunk,
                    { ...options, budget },
                  );
                },
              },
            });
            const rows: unknown[] = [];
            try {
              for (;;) {
                const frame = await execution.results.next();
                if (!frame) break;
                try {
                  rows.push(
                    ...oracleRows({
                      columns: prepared.columns,
                      batches: [
                        {
                          length: frame.payload.rowCount,
                          selection: null,
                          columns: frame.payload.columns.map((column, k) => {
                            const type = prepared.columns[k]?.type;
                            if (column.form !== "flat" || !type) throw new Error("flat frame");
                            return { ...column, type };
                          }),
                        },
                      ],
                    }),
                  );
                } finally {
                  frame.release();
                }
              }
              await execution.executed;
              await execution.delivered;
            } catch (error) {
              execution.cancel(error);
              await Promise.allSettled([execution.executed, execution.delivered]);
              throw error;
            }
            expect(rows).toEqual(
              [...input]
                .sort((a, b) => Number(b.k) - Number(a.k) || Number(a.id) - Number(b.id))
                .slice(0, limit),
            );
            expect(payloadReads, "winner payloads must bypass cache admission").toBeGreaterThan(0);
            expect(payload.size).toBe(width);
            expect(selection).toEqual(keys);
            reach.reach("payload-read");
            reach.reach("keys-only");
            reach.reach("rows");
            reach.reach(mixed ? "mixed" : "stored");
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("released");
          } finally {
            snapshot?.release();
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [16, 100, 11, false],
          [32, 120, 17, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
