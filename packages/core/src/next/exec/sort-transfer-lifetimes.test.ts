/** Real wire detachment, dictionary replay and every failure prefix obey PG and frame credit. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { Governor, OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import { isValid } from "../vector/validity.js";
import type { Batch } from "../vector/vector.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  SORT_DICTIONARY_LIFETIME_REACH,
  SORT_TRANSFER_FAILURE_REACH,
} from "../../testing/reach.js";
import {
  ArraySource,
  INT,
  BIG,
  DBL,
  TEXT,
  batchesOf,
  rowsOf,
  valueAt,
  type Row,
  type Value,
} from "./join-fixtures.js";
import { drive, type Operator } from "./pipeline.js";
import {
  ResultChannel,
  frameTransferList,
  type ResultFrame,
  type FramePayload,
  type WireVector,
} from "./result.js";
import { ResultSink } from "./result-sink.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";

const dictionariesReach = createReach(
  "next-sort-dictionary-lifetimes",
  SORT_DICTIONARY_LIFETIME_REACH,
);
const failuresReach = createReach("next-sort-transfer-failures", SORT_TRANSFER_FAILURE_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  dictionariesReach.assertReached();
  failuresReach.assertReached();
});
const expectedCache = new Map<string, Promise<Row[]>>();
const symbols = ["x", "β", "🙂", "é"];
function row(i: number, seed: number): Row {
  const wide = (1n << 62n) + BigInt(i + (seed % 17));
  return [
    i,
    i % 2 === 0 ? wide : -wide,
    i % 5 === 0 ? null : (i * 11 + (seed % 101)) % 101,
    i % 19 === 0 ? NaN : i % 17 === 0 ? Infinity : i % 13 === 0 ? -0 : (i % 23) - 11.5,
    i % 7 === 0 ? null : (symbols[(i + (seed % 97)) % 4] ?? "") + String((i + (seed % 97)) % 97),
  ];
}
function expected(seed: number, n: number, desc = false): Promise<Row[]> {
  const key = String(seed) + "/" + String(n) + "/" + String(desc);
  let cached = expectedCache.get(key);
  if (!cached) {
    cached = pg
      .query<{ i: number; w: string; m: number | null; f: number; v: string | null }>(
        `
 SELECT i::int4 AS i,
  (CASE WHEN i%2=0 THEN 4611686018427387904::numeric+i+${String(seed % 17)}
   ELSE -4611686018427387904::numeric-i-${String(seed % 17)} END)::int8::text AS w,
  CASE WHEN i%5=0 THEN NULL ELSE (i*11+${String(seed % 101)})%101 END AS m,
  (CASE WHEN i%19=0 THEN 'NaN'::float8 WHEN i%17=0 THEN 'Infinity'::float8
    WHEN i%13=0 THEN '-0'::float8 ELSE (i%23)-11.5 END)::float8 AS f,
  CASE WHEN i%7=0 THEN NULL ELSE (ARRAY['x','β','🙂','é'])[1+(i+${String(seed % 97)})%4]
    || ((i+${String(seed % 97)})%97)::text END AS v
 FROM generate_series(0,${String(n - 1)}) g(i) ORDER BY i ${desc ? "DESC" : "ASC"}
 `,
      )
      .then((result) => result.rows.map((r) => [r.i, BigInt(r.w), r.m, r.f, r.v]));
    expectedCache.set(key, cached);
  }
  return cached;
}
function exact(actual: readonly Row[], model: readonly Row[], label: string): void {
  const mismatch = actual.findIndex((r, i) => r.some((v, c) => !Object.is(v, model[i]?.[c])));
  expect({ length: actual.length, mismatch }, label).toEqual({
    length: model.length,
    mismatch: -1,
  });
}
function rememberUnions(batch: Batch, unions: WeakSet<object>): void {
  for (const vector of batch.columns)
    if (vector.form === "dictionary" && vector.dictionary.data.layout === "varlen") {
      unions.add(vector.dictionary.data.bytes.buffer);
      unions.add(vector.dictionary.data.offsets.buffer);
    }
}
function headers(
  payload: FramePayload,
  cache: Map<number, Value[]>,
  unions: WeakSet<object>,
): void {
  for (const delta of payload.dictionaries) {
    if (delta.values.data.layout !== "varlen") throw Error("unexpected dictionary delta layout");
    expect(
      unions.has(delta.values.data.bytes.buffer),
      "wire delta must not transfer borrowed union bytes",
    ).toBe(false);
    expect(
      unions.has(delta.values.data.offsets.buffer),
      "wire delta must not transfer borrowed union offsets",
    ).toBe(false);
    const received = structuredClone(delta.values, {
      transfer: frameTransferList({
        rowCount: delta.values.length,
        columns: [delta.values],
        dictionaries: [],
      }),
    });
    const entries = cache.get(delta.id) ?? [];
    expect(delta.from).toBe(entries.length);
    for (let i = 0; i < received.length; i++) entries.push(valueAt({ ...received, type: TEXT }, i));
    cache.set(delta.id, entries);
  }
}
function cell(
  vector: WireVector,
  type: PhysicalType,
  cache: Map<number, Value[]>,
  i: number,
): Value {
  if (vector.form === "dictionary") {
    if (!isValid(vector.validity, i)) return null;
    const value = cache.get(vector.dictionary)?.[vector.codes[i] ?? -1];
    if (value === undefined) throw Error("missing published dictionary entry");
    return value;
  }
  if (vector.form === "constant") return valueAt({ ...vector.value, type }, 0);
  return valueAt({ ...vector, type }, i);
}
function decoded(
  columns: readonly WireVector[],
  types: readonly PhysicalType[],
  cache: Map<number, Value[]>,
  n: number,
): Row[] {
  return Array.from({ length: n }, (_, i) =>
    columns.map((v, c) => cell(v, types[c] ?? INT, cache, i)),
  );
}
function detach(column: WireVector, n: number): WireVector {
  return structuredClone(column, {
    transfer: frameTransferList({ rowCount: n, columns: [column], dictionaries: [] }),
  });
}

for (const seed of seedsFor("next-sort-transfer-failures", [0x8a11, 0x9b24]))
  for (const width of [3, 5])
    for (const failFrame of [0, 1])
      for (const mode of ["error", "cancel"] as const)
        for (let prefix = 0; prefix <= width; prefix++)
          it(`frees each byte once after ${String(prefix)}/${String(width)} columns (${String(seed)}, frame ${String(failFrame)}, ${mode})`, async () => {
            const n = 6144,
              types = [INT, BIG, INT, DBL, TEXT].slice(0, width),
              data = Array.from({ length: n }, (_, i) => row(i, seed).slice(0, width));
            const model = (await expected(seed, n)).map((r) => r.slice(0, width));
            const governor = new Governor(32 << 20),
              spill = new MemorySpillService();
            const source = await governor.admit({
              label: "sort",
              minimumBytes: sortMinimumBytes(types),
              spillable: true,
            });
            const result = await governor.admit({
              label: "frames",
              minimumBytes: 2 << 20,
              spillable: true,
            });
            const channel = new ResultChannel(
              types.map((type, c) => ({ name: String(c), type, nullable: true })),
              { windowBytes: 2 << 20 },
            );
            const sink = new ResultSink(types, result, channel, 0),
              signal = new AbortController();
            const input = batchesOf(types, data, {
              dictionary: true,
              int64: "int64",
              batchRows: 127,
            });
            const unions = new WeakSet(),
              dictionary = new Map<number, Value[]>(),
              frames: ResultFrame[] = [];
            const failed: Array<{
              frame: ResultFrame;
              columns: WireVector[];
              from: number;
              cost: number;
            }> = [];
            const reason = new Error("generated partial transfer stop");
            let offered = 0,
              stopped = false,
              registryBytes = 0;
            const receiver: Operator = {
              reclassifiesTransfers: true,
              push(batch, token) {
                if (stopped) throw reason;
                if (!token?.detachableBuffers) throw Error("missing dedicated frame permission");
                rememberUnions(batch, unions);
                // Registry admission is outside the native-buffer handoff. Its live bytes stay pinned.
                const registryBefore = result.allocated;
                for (const v of batch.columns)
                  if (v.form === "dictionary" && v.type.kind === "text")
                    expect(sink.admitDictionary(v.dictionary)).toBe(true);
                registryBytes += result.allocated - registryBefore;
                const before = source.allocated + result.allocated;
                const status = sink.push(batch, token);
                expect(status).toBe("ready");
                const frame = channel.poll();
                if (!frame) throw Error("missing frame");
                frames.push(frame);
                const metadata = OBJECT_OVERHEAD_BYTES * (3 + 2 * width);
                const deltaBytes = frameTransferList({
                  rowCount: 0,
                  columns: [],
                  dictionaries: frame.payload.dictionaries,
                }).reduce((sum, b) => sum + b.byteLength, 0);
                const live = before + metadata + deltaBytes;
                expect(source.allocated + result.allocated, "native ownership after adoption").toBe(
                  live,
                );
                headers(frame.payload, dictionary, unions);
                expect(
                  source.allocated + result.allocated,
                  "dictionary-header transfer keeps one owner",
                ).toBe(live);
                const columns = frame.payload.columns.slice(),
                  limit = offered === failFrame ? prefix : width;
                for (let c = 0; c < limit; c++) {
                  const vector = columns[c];
                  if (!vector) throw Error("missing column");
                  const buffers = new Set(
                    frameTransferList({
                      rowCount: frame.payload.rowCount,
                      columns: [vector],
                      dictionaries: [],
                    }),
                  );
                  token.reclassify?.(buffers);
                  token.reclassify?.(buffers);
                  token.release();
                  columns[c] = detach(vector, frame.payload.rowCount);
                  expect(
                    source.allocated + result.allocated,
                    "one owner after every detached column",
                  ).toBe(live);
                }
                const from = offered * 2048;
                offered++;
                if (offered - 1 === failFrame) {
                  stopped = true;
                  failed.push({ frame, columns, from, cost: frame.byteLength + metadata });
                  if (mode === "error") throw reason;
                  signal.abort(reason);
                  return status;
                }
                exact(
                  decoded(columns, types, dictionary, frame.payload.rowCount),
                  model.slice(from, from + frame.payload.rowCount),
                  "PG earlier frame before failure",
                );
                frame.release();
                frame.release();
                expect(
                  source.allocated + result.allocated,
                  "credit releases native capacities exactly once",
                ).toBe(live - frame.byteLength - metadata);
                return status;
              },
              ready: () => sink.ready(),
              finish: () => sink.finish(),
              abort: (e) => sink.abort(e),
            };
            const sort = new SortOperator(
              {
                types,
                keys: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
                limit: null,
                offset: 0,
                withTies: false,
              },
              { grant: source, spill, queryId: 1, columnar: true, signal: signal.signal },
              receiver,
            );
            failuresReach.beginHistory();
            try {
              await expect(drive(new ArraySource(input), sort, signal.signal)).rejects.toThrow(
                reason.message,
              );
              await sort.closed;
              const held = failed[0];
              if (!held) throw Error("fault prefix was not reached");
              expect(source.allocated).toBe(0);
              expect(result.allocated).toBe(held.cost + registryBytes);
              // The coordinator ends the independently live sink; offered frame credit stays valid.
              sink.abort(reason);
              expect(result.allocated).toBe(held.cost);
              exact(
                decoded(held.columns, types, dictionary, held.frame.payload.rowCount),
                model.slice(held.from, held.from + held.frame.payload.rowCount),
                "PG partially transferred frame after source close",
              );
              sink.discard(reason);
              channel.cancel(reason);
              for (const frame of frames) {
                frame.release();
                frame.release();
              }
              expect(source.allocated + result.allocated).toBe(0);
              for (const state of [
                mode,
                prefix === 0
                  ? "zero columns"
                  : prefix === width
                    ? "all columns"
                    : "interior prefix",
                failFrame === 0 ? "first frame" : "later frame",
                "wide int64",
                "nullable validity",
                "single owner every step",
                "source close before credit",
                "idempotent credit",
              ] as const)
                failuresReach.reach(state);
              if (width === 5) {
                failuresReach.reach("non-finite payload");
                failuresReach.reach("dictionary header before failure");
              }
            } finally {
              sort.abort(reason);
              await sort.closed;
              sink.discard(reason);
              channel.cancel(reason);
              for (const f of frames) f.release();
              result.close();
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
            }
          }, 120000);

for (const seed of seedsFor("next-sort-dictionary-lifetimes", [0x6311, 0x7422]))
  for (const spillMode of [false, true])
    for (const descending of [false, true])
      it(`keeps exact dictionaries through ${spillMode ? "reclaim/spill/merge" : "resident frames"} (${String(seed)}, ${String(descending)})`, async () => {
        const n = spillMode ? 70003 : 6175,
          types = [INT, TEXT, BIG];
        const data = Array.from({ length: n }, (_, i) => {
          const r = row(i, seed);
          return [r[0] ?? null, r[4] ?? null, r[1] ?? null];
        });
        const model = (await expected(seed, n, descending)).map((r) => [
          r[0] ?? null,
          r[4] ?? null,
          r[1] ?? null,
        ]);
        const input = batchesOf(types, data, { dictionary: true, int64: "int64", batchRows: 997 });
        const governor = new Governor(32 << 20),
          spill = new MemorySpillService();
        const source = await governor.admit({
          label: "sort",
          minimumBytes: sortMinimumBytes(types),
          spillable: true,
        });
        const result = await governor.admit({
          label: "frames",
          minimumBytes: 4 << 20,
          spillable: true,
        });
        const channel = new ResultChannel(
          types.map((type, c) => ({ name: String(c), type, nullable: true })),
          { windowBytes: 4 << 20 },
        );
        const sink = new ResultSink(types, result, channel, 0),
          unions = new WeakSet();
        const receiver: Operator = {
          reclassifiesTransfers: true,
          push(b, t) {
            rememberUnions(b, unions);
            return sink.push(b, t);
          },
          ready: () => sink.ready(),
          finish: () => sink.finish(),
          abort: (e) => sink.abort(e),
        };
        const sort = new SortOperator(
          {
            types,
            keys: [
              { column: 0, direction: descending ? "desc" : "asc", nulls: "last", collation: null },
            ],
            limit: null,
            offset: 0,
            withTies: false,
          },
          { grant: source, spill, queryId: 1, columnar: true },
          receiver,
        );
        const frames: ResultFrame[] = [],
          actual: Row[] = [],
          dictionary = new Map<number, Value[]>();
        let previousId: number | undefined,
          deltaCount = 0,
          frameCount = 0;
        dictionariesReach.beginHistory();
        try {
          for (let i = 0; i < input.length; i++) {
            const batch = input[i];
            if (!batch) throw Error("missing input");
            expect(batch.columns[1]?.form).toBe("dictionary");
            if (sort.push(batch) === "blocked") await sort.ready();
            if (spillMode && (i === 2 || i === 10)) await sort.reclaim();
          }
          await sort.finish();
          await sort.closed;
          expect(source.allocated).toBe(0);
          if (spillMode) {
            expect(sort.profile.spilledBytes).toBeGreaterThan(0);
            expect(sort.profile.mergePasses).toBeGreaterThan(0);
            dictionariesReach.reach("actual spill and merge");
            dictionariesReach.reach("union replay under reclaim");
          } else {
            expect(sort.profile.columnarRows).toBe(n);
            dictionariesReach.reach("resident union frames");
          }
          for (;;) {
            const frame = await channel.next();
            if (frame === null) break;
            frames.push(frame);
            const text = frame.payload.columns[1];
            if (!spillMode) {
              expect(text?.form).toBe("dictionary");
              if (text?.form !== "dictionary") throw Error("missing preserved wire dictionary");
              if (previousId !== undefined) expect(text.dictionary).toBe(previousId);
              previousId = text.dictionary;
            }
            deltaCount += frame.payload.dictionaries.length;
            headers(frame.payload, dictionary, unions);
            const received = frame.payload.columns.map((column) =>
              detach(column, frame.payload.rowCount),
            );
            actual.push(...decoded(received, types, dictionary, frame.payload.rowCount));
            frameCount++;
            frame.release();
            frame.release();
          }
          exact(actual, model, "PG rows across dictionary lifetime");
          expect(frameCount).toBeGreaterThan(2);
          if (!spillMode) {
            expect(deltaCount).toBe(1);
            dictionariesReach.reach("stable wire id and delta");
          }
          for (const state of [
            "local dictionary codes",
            "NULL text",
            "UTF8 text",
            "exact wide int64",
            "after source close",
            "earlier frames detached",
            descending ? "desc" : "asc",
            "PG full output",
          ] as const)
            dictionariesReach.reach(state);
          exact(input.flatMap(rowsOf), data, "borrowed input remains unchanged after detachment");
        } finally {
          const reason = new Error("lifetime test finished");
          sort.abort(reason);
          await sort.closed;
          sink.discard(reason);
          channel.cancel(reason);
          for (const frame of frames) frame.release();
          result.close();
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
        }
      }, 120000);
