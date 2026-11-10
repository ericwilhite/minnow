/** Local dictionary codes are not global identities; payload bytes and NULLs stay exact. */
import { PGlite } from "@electric-sql/pglite";
import { beforeAll, afterAll, expect, it } from "vitest";
import { createReach, SORT_DICTIONARY_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  ArraySource,
  CollectSink,
  INT,
  DBL,
  BIG,
  TEXT,
  batchesOf,
  type Row,
} from "./join-fixtures.js";
import { drive, type Operator } from "./pipeline.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";
import type { FlatVector } from "../vector/vector.js";
const reach = createReach("next-sort-dictionary", SORT_DICTIONARY_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-sort-dictionary", [0x6712, 0x9813]))
  for (const desc of [false, true]) {
    it(`preserves and rebases text dictionaries exactly against PG (${String(seed)}, ${String(desc)})`, async () => {
      const random = mulberry32(seed),
        n = 2101 + Math.floor(random() * 101),
        types = [INT, DBL, TEXT, BIG];
      const ids = Array.from({ length: n }, (_, i) => i);
      for (let i = n - 1; i > 0; i--) {
        const at = Math.floor(random() * (i + 1));
        [ids[i], ids[at]] = [ids[at] ?? 0, ids[i] ?? 0];
      }
      const texts = ["", "é", "😀", "a", "aa", "prefix-é"];
      const data: Row[] = ids.map((i) => [
        i,
        i % 11 === 0 ? null : i % 7 === 0 ? NaN : (i % 19) - 9,
        i % 13 === 0 ? null : (texts[i % 6] ?? ""),
        (1n << 62n) + BigInt(i),
      ]);
      const expected = (
        await pg.query<{ id: number; v: string | null; b: string }>(
          `SELECT i AS id,CASE WHEN i%13=0 THEN NULL ELSE (ARRAY['','é','😀','a','aa','prefix-é'])[(i%6)+1] END AS v,(4611686018427387904::bigint+i)::text AS b FROM generate_series(0,${String(n - 1)}) g(i) ORDER BY CASE WHEN i%11=0 THEN NULL WHEN i%7=0 THEN 'NaN'::float8 ELSE (i%19)-9 END ${desc ? "DESC" : "ASC"} NULLS FIRST,i`,
        )
      ).rows;
      const governor = new Governor(64 << 20),
        spill = new MemorySpillService(),
        grant = await governor.admit({
          label: "dict-sort",
          minimumBytes: sortMinimumBytes(types),
          spillable: true,
        });
      const collected = new CollectSink(),
        seen = new WeakSet<FlatVector>();
      let pushes = 0;
      const sink: Operator = {
        reclassifiesTransfers: true,
        push(batch, transfer) {
          const vector = batch.columns[2];
          expect(vector?.form).toBe("dictionary");
          if (vector?.form !== "dictionary") throw Error("text dictionary flattened");
          if (pushes++) expect(seen.has(vector.dictionary)).toBe(true);
          seen.add(vector.dictionary);
          const result = collected.push(batch);
          transfer?.release();
          return result;
        },
        ready: () => collected.ready(),
        finish: () => collected.finish(),
        abort: (e) => collected.abort(e),
      };
      const op = new SortOperator(
        {
          types,
          keys: [
            { column: 1, direction: desc ? "desc" : "asc", nulls: "first", collation: null },
            { column: 0, direction: "asc", nulls: "last", collation: null },
          ],
          limit: null,
          offset: 0,
          withTies: false,
        },
        { grant, spill, queryId: 1, columnar: true },
        sink,
      );
      await drive(
        new ArraySource(
          batchesOf(types, data, { dictionary: true, int64: "int64", batchRows: 97 }),
        ),
        op,
      );
      await op.closed;
      expect(collected.rows.map((r) => ({ id: Number(r[0]), v: r[2], b: String(r[3]) }))).toEqual(
        expected,
      );
      expect(pushes).toBeGreaterThan(1);
      expect(op.profile.columnarRows).toBe(n);
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
      reach.beginHistory();
      for (const state of [
        "genuine dictionaries",
        "local code rebasing",
        "stable dictionary identity",
        "NULL payload",
        "UTF8 payload",
        "wide int64 payload",
        desc ? "desc" : "asc",
      ] as const)
        reach.reach(state);
    });
  }

for (const seed of seedsFor("next-sort-dictionary-bytes", [0x33a1, 0x4872])) {
  it(`keeps exact octets across hash collisions and appended prefixes (${String(seed)})`, async () => {
    const { SortDictionary } = await import("./sort-dictionary.js");
    const { Slice, runSliced } = await import("./slices.js");
    const { TaskClock } = await import("./pipeline.js");
    const pair = seed % 2 === 0 ? [10269, 11626] : [10268, 11627];
    const entries = [pair[0], pair[1]].map((i) =>
      Uint8Array.from([255, 0, ...Array.from(String(i), (c) => c.charCodeAt(0))]),
    );
    const bytes = new Uint8Array(14);
    entries.forEach((v, i) => bytes.set(v, i * 7));
    const source: FlatVector = {
      form: "flat",
      type: TEXT,
      length: 1,
      validity: null,
      data: { layout: "varlen", bytes, offsets: Int32Array.of(0, 7, 14) },
    };
    const governor = new Governor(8 << 20),
      grant = await governor.admit({ label: "dict-bytes", minimumBytes: 2 << 20, spillable: true });
    const dictionary = new SortDictionary(TEXT, grant);
    expect(dictionary.init()).toBe(true);
    const slice = new Slice(4, new TaskClock(() => 0));
    expect(
      Array.from(
        (await runSliced(dictionary.translate(source, slice), slice, () => undefined)) ?? [],
      ),
    ).toEqual([0]);
    source.length = 2;
    expect(
      Array.from(
        (await runSliced(dictionary.translate(source, slice), slice, () => undefined)) ?? [],
      ),
    ).toEqual([0, 1]);
    const output = dictionary.vector();
    expect(output.length).toBe(2);
    if (output.data.layout !== "varlen") throw Error("wrong layout");
    expect(Array.from(output.data.bytes.subarray(0, 14))).toEqual(
      Array.from(bytes.subarray(0, 14)),
    );
    dictionary.close();
    grant.close();
    expect(governor.usedBytes).toBe(0);
    reach.beginHistory();
    for (const state of [
      "byte hash collision",
      "appended dictionary prefix",
      "raw octets",
    ] as const)
      reach.reach(state);
  });
  it(`bounds dictionary construction and releases partial state (${String(seed)})`, async () => {
    const { SortDictionary } = await import("./sort-dictionary.js");
    const { Slice, runSliced } = await import("./slices.js");
    const { TaskClock } = await import("./pipeline.js");
    const n = 32769 + (seed % 2),
      bytes = new Uint8Array(n * 5),
      offsets = new Int32Array(n + 1);
    for (let i = 0; i < n; i++) {
      const value = String(10000 + i);
      for (let j = 0; j < 5; j++) bytes[i * 5 + j] = value.charCodeAt(j);
      offsets[i + 1] = (i + 1) * 5;
    }
    const source: FlatVector = {
      form: "flat",
      type: TEXT,
      length: n,
      validity: null,
      data: { layout: "varlen", bytes, offsets },
    };
    const governor = new Governor(8 << 20),
      grant = await governor.admit({ label: "dict-bound", minimumBytes: 2 << 20, spillable: true });
    const dictionary = new SortDictionary(TEXT, grant);
    expect(dictionary.init()).toBe(true);
    const slice = new Slice(4, new TaskClock(() => 0));
    expect(await runSliced(dictionary.translate(source, slice), slice, () => undefined)).toBeNull();
    dictionary.close();
    grant.close();
    expect(governor.usedBytes).toBe(0);
    reach.beginHistory();
    reach.reach("codebook bound");
  });
}
it("replays preserved codes as logical text before a mixed-form fallback", async () => {
  const types = [INT, DBL, TEXT],
    data: Row[] = [
      [4, 4, "four"],
      [2, 2, "two"],
      [3, 3, "three"],
      [1, 1, "one"],
      [0, 0, null],
    ];
  const encoded = batchesOf(types, data.slice(0, 3), { dictionary: true }),
    flat = batchesOf(types, data.slice(3));
  const governor = new Governor(32 << 20),
    spill = new MemorySpillService(),
    grant = await governor.admit({
      label: "mixed-dict",
      minimumBytes: sortMinimumBytes(types),
      spillable: true,
    });
  const sink = new CollectSink();
  const receiver: Operator = {
    reclassifiesTransfers: true,
    push(batch, transfer) {
      const result = sink.push(batch);
      transfer?.release();
      return result;
    },
    ready: () => sink.ready(),
    finish: () => sink.finish(),
    abort: (e) => sink.abort(e),
  };
  const op = new SortOperator(
    {
      types,
      keys: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
      limit: null,
      offset: 0,
      withTies: false,
    },
    { grant, spill, queryId: 1, columnar: true },
    receiver,
  );
  await drive(new ArraySource([...encoded, ...flat]), op);
  await op.closed;
  expect(sink.rows).toEqual(data.slice().sort((a, b) => Number(a[0]) - Number(b[0])));
  expect(op.profile.columnarRows).toBe(0);
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
  reach.beginHistory();
  reach.reach("mixed encoding fallback");
});

for (const seed of seedsFor("next-sort-dictionary-borrow", [0x83a1, 0x84a2]))
  it(`does not publish a short-lived dictionary to a borrow-only receiver (${String(seed)})`, async () => {
    const types = [INT, DBL, TEXT],
      n = 31 + (seed % 29),
      data: Row[] = Array.from({ length: n }, (_, i) => [
        i,
        (i * 17) % 11,
        i % 7 === 0 ? null : `v${String(i % 3)}`,
      ]);
    const governor = new Governor(16 << 20),
      spill = new MemorySpillService(),
      grant = await governor.admit({
        label: "borrowed-dict",
        minimumBytes: sortMinimumBytes(types),
        spillable: true,
      }),
      collected = new CollectSink();
    const sink: Operator = {
      push(batch) {
        expect(batch.columns[2]?.form).toBe("flat");
        return collected.push(batch);
      },
      ready: () => collected.ready(),
      finish: () => collected.finish(),
      abort: (e) => collected.abort(e),
    };
    const op = new SortOperator(
      {
        types,
        keys: [
          { column: 1, direction: "asc", nulls: "last", collation: null },
          { column: 0, direction: "asc", nulls: "last", collation: null },
        ],
        limit: null,
        offset: 0,
        withTies: false,
      },
      { grant, spill, queryId: 1, columnar: true },
      sink,
    );
    await drive(new ArraySource(batchesOf(types, data, { dictionary: true })), op);
    await op.closed;
    expect(collected.rows).toEqual(
      data.slice().sort((a, b) => Number(a[1]) - Number(b[1]) || Number(a[0]) - Number(b[0])),
    );
    expect(op.profile.columnarRows).toBe(n);
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    reach.beginHistory();
    reach.reach("borrow-only receiver");
  });
