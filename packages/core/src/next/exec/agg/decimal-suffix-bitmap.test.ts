/** PostgreSQL byte equality and COUNT(DISTINCT) are authority. Canonical suffix
 * compression must preserve every original key through exact spill/replay.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import {
  createReach,
  B9_DECIMAL_SUFFIX_REACH,
  B9_DECIMAL_SUFFIX_PRESSURE_REACH,
} from "../../../testing/reach.js";
import { Governor } from "../../memory/governor.js";
import type { Vector } from "../../vector/vector.js";
import { TEXT, texts, dictionary, constant } from "../kernels/test-support.js";
import { ScalarByteDistinct } from "./scalar-byte-distinct.js";
import { StateRow } from "./state-row.js";
import { HashHeadroomError } from "./hash-workspace.js";
import type { Rel } from "../../ir/ir.js";
import { HybridAggregateOperator, closeHybridPhase, type HybridPhase } from "./hybrid.js";
import { ReservedGrant } from "../admission.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf, type Row } from "../join-fixtures.js";
import { OutputArena } from "../kernels/vectors.js";
import { I64, col } from "../kernels/test-support.js";
import { drive } from "../pipeline.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});

it.each(seedsFor("b9-decimal-suffix", [0xb9b11, 0xb9b12]))(
  "keeps exact PG classes, first rows and members under8MiB (seed %i)",
  async (seed) => {
    const reach = createReach("B9 decimal suffix", B9_DECIMAL_SUFFIX_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("acct-", "", "π-", "\ufeffp-"),
        fc.boolean(),
        fc.integer({ min: 0, max: 255 }),
        async (prefix, generic, salt) => {
          reach.beginHistory();
          const governor = new Governor(8 << 20),
            grant = await governor.admit({
              label: "B9 suffix state",
              minimumBytes: 4 << 20,
              spillable: true,
            });
          if (!grant.allocate(6 * 2048 + 4 * 64)) throw new Error("fixture workspace admission");
          let held = 0,
            ceiling = Infinity;
          const charge = {
              allocate(bytes: number) {
                if (held + bytes > ceiling) throw new HashHeadroomError(held + bytes, ceiling);
                if (!grant.allocate(bytes)) throw new Error("complete suffix fixture exhausted");
                held += bytes;
                return true;
              },
              free(bytes: number) {
                held -= bytes;
                grant.free(bytes);
              },
            },
            set = new ScalarByteDistinct(TEXT, charge),
            copy = new ScalarByteDistinct(TEXT, grant),
            all: string[] = [],
            out = { rows: new Uint16Array(2048), count: 0 },
            gids = new Int32Array(2048);
          const apply = async (
            vector: Vector,
            values: ReadonlyArray<string | null>,
            active: number[],
            form: string,
          ) => {
            const offered = active.map((i) => values[i] ?? null),
              expected = (
                await pg.query<{ id: number }>(
                  `SELECT min(ord)::int id FROM unnest($1::text[]) WITH ORDINALITY t(v,ord) WHERE v IS NOT NULL AND NOT(v=ANY($2::text[])) GROUP BY v COLLATE "C" ORDER BY min(ord)`,
                  [offered, all],
                )
              ).rows.map((r) => active[r.id - 1]);
            ceiling = held + set.growthBytes(vector, active.length);
            const selected = { rows: Uint16Array.from(active), count: active.length };
            set.filter(vector, gids, vector.length, selected, true, out);
            expect([...out.rows.subarray(0, out.count)]).toEqual(expected);
            all.push(...offered.filter((v): v is string => v !== null));
            reach.reach(form as "flat" | "dictionary" | "constant");
            reach.reach("PG first rows");
          };
          try {
            const first = [
              "unselected-9",
              null,
              generic ? "sentinel!" : prefix + "0",
              prefix + "1",
              prefix + "31",
              prefix + "32",
              prefix + "4095",
              prefix + "4096",
              prefix + "1048575",
              prefix + "1048576",
              prefix + "16777215",
              prefix + "16777216",
              prefix + "00",
              prefix + "01",
              "other-1",
              prefix + "1",
              "z".repeat(Math.max(1, new TextEncoder().encode(prefix).length)) + "1",
              ...Array.from({ length: 12 }, (_, i) => prefix + String(i * 4096 + salt)),
              prefix + String(salt),
              prefix + "9999999999999999999999",
              "",
              null,
            ];
            const vector = texts(first);
            await apply(
              vector,
              first,
              Array.from({ length: first.length - 1 }, (_, i) => i + 1),
              "flat",
            );
            if (vector.data.layout !== "varlen") throw new Error("bytes fixture");
            vector.data.bytes.fill(255);
            const entries = [
                prefix + "4096",
                prefix + "1",
                prefix + "4294967296",
                "other-1",
                prefix + "001",
                prefix + String(4097 + salt),
              ],
              codes = [0, 1, 0, 2, null, 3, 4, 5],
              values = codes.map((c) => (c === null ? null : (entries[c] ?? null)));
            await apply(
              dictionary(texts(entries), codes),
              values,
              values.map((_, i) => i),
              "dictionary",
            );
            const reverse = [...entries].reverse(),
              reverseValues = codes.map((c) => (c === null ? null : (reverse[c] ?? null)));
            await apply(
              dictionary(texts(reverse), codes),
              reverseValues,
              reverseValues.map((_, i) => i),
              "dictionary",
            );
            await apply(
              constant(texts([prefix + "12"]), 5),
              Array.from({ length: 5 }, () => prefix + "12"),
              [0, 2, 4],
              "constant",
            );
            const expected = (
              await pg.query<{ n: number }>(
                "SELECT count(DISTINCT v)::int n FROM unnest($1::text[]) t(v)",
                [all],
              )
            ).rows[0]?.n;
            expect(set.count).toBe(expected);
            const physical: { readonly bitmapCount?: number; readonly bitmapPages?: number } = set;
            if (!generic) {
              expect(physical.bitmapCount ?? 0).toBeGreaterThan(0);
              expect(physical.bitmapPages ?? 0).toBeLessThanOrEqual(16);
              expect(physical.bitmapPages ?? 0).toBeGreaterThanOrEqual(12);
              reach.reach("bitmap");
              reach.reach("sparse pages");
            } else {
              expect(physical.bitmapCount ?? 0).toBe(0);
              reach.reach("generic first");
            }
            const members: string[] = [];
            for (const member of set.members()) {
              if (member === undefined) continue;
              const row = new StateRow(grant);
              try {
                row.value(set.memberBytes(member.id), () => set.memberValue(member.id, TEXT));
                const value = row.columns[0];
                if (value?.data.layout !== "varlen") throw new Error("member byte value");
                const data = value.data;
                members.push(
                  Buffer.from(
                    data.bytes.subarray(data.offsets[0] ?? 0, data.offsets[1] ?? 0),
                  ).toString("hex"),
                );
                copy.filter(value, gids, 1, null, true, out);
              } finally {
                row.close();
              }
            }
            const expectedMembers = (
              await pg.query<{ hex: string }>(
                `SELECT DISTINCT encode(convert_to(v,'UTF8'),'hex') hex FROM unnest($1::text[]) t(v) ORDER BY hex`,
                [all],
              )
            ).rows.map((r) => r.hex);
            expect(members.sort()).toEqual(expectedMembers);
            expect(copy.count).toBe(expected);
            reach.reach("members");
            reach.reach("restored");
            reach.reach("NULL");
            reach.reach("leading zero fallback");
            reach.reach("large suffix fallback");
            reach.reach("other prefix fallback");
            reach.reach("dictionary identity");
            reach.reach("selection");
            reach.reach("owned prefix");
            reach.reach("funded growth");
            if (prefix === "") reach.reach("empty prefix");
            if (prefix.includes("π") || prefix.includes("\ufeff")) reach.reach("UTF8 prefix");
            expect(governor.peakBytes).toBeLessThanOrEqual(8 << 20);
          } finally {
            set.close();
            copy.close();
            grant.close();
          }
          expect(held).toBe(0);
          expect(governor.usedBytes).toBe(0);
          reach.reach("released");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          ["acct-", false, 0],
          ["\ufeffp-", false, 1],
          ["", false, 2],
          ["acct-", true, 3],
        ],
      },
    );
    reach.assertReached();
  },
  180000,
);

it.each(seedsFor("b9-decimal-suffix-pressure", [0xb9b13, 0xb9b14]))(
  "spills and replays every compressed and ordinary member at16MiB (seed %i)",
  async (seed) => {
    const reach = createReach("B9 suffix pressure", B9_DECIMAL_SUFFIX_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1200, max: 1600 }),
        fc.integer({ min: 280, max: 340 }),
        async (groups, width) => {
          reach.beginHistory();
          const values = [
              ...Array.from({ length: 32 }, (_, i) => `acct-${String(i)}`),
              ...Array.from({ length: 3 * groups }, (_, i) =>
                i % 2 === 0
                  ? `acct-${String(i % groups)}`
                  : `mail-${String(i % groups)}-${"x".repeat(width)}!`,
              ),
            ],
            rows: Row[] = values.map((v) => [v]),
            expected = (
              await pg.query<{ n: number }>(
                "SELECT count(DISTINCT v)::int n FROM unnest($1::text[]) t(v)",
                [values],
              )
            ).rows[0]?.n,
            governor = new Governor(16 << 20),
            hashMinimum = 512 << 10,
            ioMinimum = 2 << 20,
            accounts = async (minimum: number) => {
              const native = await governor.admitGroup({
                label: "complete suffix pressure graph",
                grants: [
                  { label: "main", minimumBytes: minimum + ioMinimum, spillable: true },
                  ...["hash", "io"].flatMap((name) => [
                    { label: name + "/fixed", minimumBytes: 0, spillable: false },
                    { label: name + "/growth", minimumBytes: 0, spillable: true },
                  ]),
                ],
              });
              const part = (i: number) => {
                const g = native[i];
                if (!g) throw new Error("missing suffix helper");
                return g;
              };
              return {
                grant: part(0),
                extras: [1, 3].map(
                  (i) => new ReservedGrant(part(i), part(i + 1), 0, null, false, undefined, true),
                ),
              };
            },
            initial = await accounts(hashMinimum),
            filler = await governor.admit({
              label: "pinned suffix competitor",
              minimumBytes: governor.capBytes - governor.reserveBytes - governor.usedBytes,
              spillable: false,
            }),
            spill = new MemorySpillService(),
            arena = new OutputArena(initial.grant, 2048),
            sink = new CollectSink(),
            rel: Extract<Rel, { op: "aggregate" }> = {
              id: 2,
              op: "aggregate",
              input: {
                id: 1,
                op: "values",
                rows: [],
                output: [{ name: "k", type: TEXT, nullable: true }],
              },
              output: [{ name: "n", type: I64, nullable: false }],
              groupBy: [],
              groupingSets: null,
              aggregates: [
                {
                  fn: "count",
                  args: [col(0, TEXT)],
                  type: I64,
                  distinct: true,
                  filter: null,
                  orderBy: [],
                },
              ],
            },
            captured: { phase: HybridPhase | null } = { phase: null },
            operator = new HybridAggregateOperator(
              rel,
              { arena, input: [TEXT], params: { slots: [] } },
              {
                ...initial,
                hashMinimum,
                ioMinimum,
                spill,
                queryId: 92,
                handoff: (phase) => {
                  captured.phase = phase;
                },
              },
              sink,
            );
          let resumed: HybridAggregateOperator | undefined,
            next: Awaited<ReturnType<typeof accounts>> | undefined,
            nextArena: OutputArena | undefined;
          try {
            await drive(
              new ArraySource(
                batchesOf([TEXT], rows, { batchRows: 128, dictionary: true, skip: 3 }),
              ),
              operator,
            );
            await operator.closed;
            expect(captured.phase).not.toBeNull();
            reach.reach("checkpoint");
            expect(operator.profile.evictions).toBeGreaterThan(0);
            expect(operator.profile.spilledBytes).toBeGreaterThan(0);
            arena.close();
            for (const helper of initial.extras) helper.close();
            initial.grant.close();
            filler.close();
            expect(governor.usedBytes).toBe(0);
            reach.reach("no holdings between phases");
            if (captured.phase === null) throw new Error("missing exact suffix checkpoint");
            next = await accounts(8 << 20);
            nextArena = new OutputArena(next.grant, 2048);
            resumed = new HybridAggregateOperator(
              rel,
              { arena: nextArena, input: [TEXT], params: { slots: [] } },
              {
                ...next,
                hashMinimum: 8 << 20,
                ioMinimum,
                spill,
                queryId: 92,
                resume: captured.phase,
              },
              sink,
            );
            await resumed.finish();
            expect(sink.rows.map((r) => Number(r[0]))).toEqual([expected]);
            reach.reach("PG count");
            reach.reach("membership before suffix");
            reach.reach("local dictionaries");
            reach.reach("selection");
            expect(governor.peakBytes).toBeLessThanOrEqual(16 << 20);
            reach.reach("16 MiB");
          } finally {
            operator.abort(new Error("suffix pressure cleanup"));
            await operator.closed;
            resumed?.abort(new Error("suffix restored cleanup"));
            if (resumed) await resumed.closed;
            if (captured.phase) await closeHybridPhase(captured.phase);
            nextArena?.close();
            for (const helper of next?.extras ?? []) helper.close();
            next?.grant.close();
            arena.close();
            for (const helper of initial.extras) helper.close();
            initial.grant.close();
            filler.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("released");
        },
      ),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  },
  180000,
);
