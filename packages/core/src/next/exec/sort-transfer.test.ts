/** Dedicated source buffers may move only after the result account can own them. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, SORT_TRANSFER_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { OutputArena } from "./kernels/vectors.js";
import { ProjectOperator } from "./filter.js";
import { ArraySource, INT, DBL, TEXT, batchesOf, rowsOf, type Row } from "./join-fixtures.js";
import { drive, TaskClock, type Operator } from "./pipeline.js";
import { ResultChannel } from "./result.js";
import { ResultSink } from "./result-sink.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";
import type { Batch } from "../vector/vector.js";

const reach = createReach("next-sort-transfer", SORT_TRANSFER_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
for (const seed of seedsFor("next-sort-transfer", [0x7411, 0x8431]))
  for (const flip of [false, true]) {
    it(`moves dedicated sorted outputs through projections, exact against PG (${String(seed)}, ${String(flip)})`, async () => {
      const random = mulberry32(seed),
        n = 31 + Math.floor(random() * 400);
      const types = [INT, DBL, TEXT],
        columns = types.map((type, i) => ({ name: String(i), type, nullable: true }));
      const governor = new Governor(32 << 20),
        spill = new MemorySpillService();
      const source = await governor.admit({
        label: "sort",
        minimumBytes: sortMinimumBytes(types),
        spillable: true,
      });
      const result = await governor.admit({
        label: "result",
        minimumBytes: 2 << 20,
        spillable: true,
      });
      const channel = new ResultChannel(columns, { windowBytes: 2 << 20 });
      const sink = new ResultSink(types, result, channel, 0);
      const arena = new OutputArena(source, 2048);
      let output: Batch | undefined,
        marked = false;
      const observer: Operator = {
        reclassifiesTransfers: true,
        push(batch, transfer) {
          output = batch;
          marked = transfer?.detachableBuffers !== undefined;
          return sink.push(batch, transfer);
        },
        ready: () => sink.ready(),
        finish: () => sink.finish(),
        abort: (e) => sink.abort(e),
      };
      const project = new ProjectOperator(
        types.map((type, index) => ({ op: "column", index, type })),
        { arena, input: types, params: { slots: [] } },
        observer,
      );
      const op = new SortOperator(
        {
          types,
          keys: [
            { column: 1, direction: flip ? "desc" : "asc", nulls: "last", collation: null },
            { column: 0, direction: "asc", nulls: "last", collation: null },
          ],
          limit: null,
          offset: 0,
          withTies: false,
        },
        { grant: source, spill, queryId: 1, columnar: true },
        project,
      );
      const data: Row[] = Array.from({ length: n }, (_, i) => [
        i,
        i % 11 === 0 ? null : i % 7 === 0 ? NaN : (i % 19) - 9,
        i % 13 === 0 ? null : `v${String(i % 5)}`,
      ]);
      const expected = (
        await pg.query<{ id: number }>(
          `SELECT i AS id FROM generate_series(0,${String(n - 1)}) g(i) ORDER BY CASE WHEN i%11=0 THEN NULL WHEN i%7=0 THEN 'NaN'::float8 ELSE (i%19)-9 END ${flip ? "DESC" : "ASC"} NULLS LAST,i`,
        )
      ).rows.map((x) => x.id);
      await drive(new ArraySource(batchesOf(types, data)), op);
      await op.closed;
      const frame = await channel.next();
      expect(frame).not.toBeNull();
      if (!frame || !output) throw Error("missing frame");
      expect(marked).toBe(true);
      const original = output.columns[0],
        actual = frame.payload.columns[0];
      expect(original?.form).toBe("flat");
      expect(actual?.form).toBe("flat");
      if (original?.form !== "flat" || actual?.form !== "flat") throw Error("not flat");
      expect(actual.data).toBe(original.data);
      const batch = {
        length: frame.payload.rowCount,
        selection: null,
        columns: frame.payload.columns.map((v, c) => ({ ...v, type: types[c] })),
      } as Batch;
      expect(rowsOf(batch).map((x) => x[0])).toEqual(expected);
      expect(result.allocated).toBeGreaterThan(0);
      frame.release();
      expect(await channel.next()).toBeNull();
      arena.close();
      result.close();
      expect(governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
      reach.beginHistory();
      for (const state of [
        "dedicated output",
        "foreign reclassification",
        "pure projection",
        "nulls and NaNs",
        flip ? "desc" : "asc",
      ] as const)
        reach.reach(state);
    });
  }

it("moves only a complete unchanged tight source chunk", async () => {
  const { ColumnStore } = await import("./column-store.js");
  const { moveGatherColumn, gatherSource } = await import("./sort-columnar-data.js");
  const { Slice, runSliced } = await import("./slices.js");
  const governor = new Governor(8 << 20);
  const grant = await governor.admit({
    label: "chunk-move",
    minimumBytes: 1 << 20,
    spillable: true,
  });
  const store = new ColumnStore([INT], grant);
  for (const batch of batchesOf(
    [INT],
    Array.from({ length: 2048 }, (_, i) => [i]),
  ))
    store.append(batch.columns, null, 0, batch.length);
  const slice = new Slice(4, new TaskClock(() => 0));
  const source = await runSliced(gatherSource(store, 0, slice), slice, () => undefined);
  const ids = Int32Array.from({ length: 2048 }, (_, i) => i);
  const moved = moveGatherColumn(source, ids, 0, 2048, store.chunkShift, store.chunkMask);
  expect(moved).toBe(store.chunk(0, 0));
  expect(moveGatherColumn(source, ids, 0, 1024, store.chunkShift, store.chunkMask)).toBeNull();
  ids[1] = 2;
  ids[2] = 1;
  expect(moveGatherColumn(source, ids, 0, 2048, store.chunkShift, store.chunkMask)).toBeNull();
  if (!moved) throw Error("not moved");
  // Actual irreversible detach: closing must use charged capacities, never detached view sizes.
  const { frameTransferList } = await import("./frame-buffers.js");
  const buffers = frameTransferList({ rowCount: 2048, columns: [moved], dictionaries: [] });
  structuredClone(moved, { transfer: buffers });
  store.close();
  grant.close();
  expect(governor.usedBytes).toBe(0);
});

it("copies when the result account refuses the adoption charge", async () => {
  const { numbers } = await import("./kernels/test-support.js");
  const governor = new Governor(8 << 20);
  const source = await governor.admit({ label: "source", minimumBytes: 4096, spillable: true });
  const result = await governor.admit({ label: "result", minimumBytes: 1 << 20, spillable: true });
  expect(source.allocate(8)).toBe(true);
  const vector = numbers(INT, [3, 4]);
  if (vector.data.layout !== "i32") throw Error("wrong test layout");
  const channel = new ResultChannel([{ name: "v", type: INT, nullable: false }], {
    windowBytes: 1 << 20,
  });
  const refusing = new Proxy(result, {
    get(target, key) {
      if (key === "allocate")
        return (bytes: number) => (bytes === 8 ? false : target.allocate(bytes));
      const value: unknown = Reflect.get(target, key);
      return typeof value === "function" ? (value.bind(target) as unknown) : value;
    },
  });
  const sink = new ResultSink([INT], refusing, channel, 0);
  let releases = 0;
  const pushed = sink.push(
    { length: 2, columns: [vector], selection: null },
    {
      detachableBuffers: new Set([vector.data.values.buffer as ArrayBuffer]),
      detachableBudget: source,
      detachableReclassify: true,
      release: () => {
        releases++;
        source.free(8);
      },
    },
  );
  if (pushed === "blocked") await sink.ready();
  await sink.finish();
  const frame = await channel.next();
  expect(frame).not.toBeNull();
  const output = frame?.payload.columns[0];
  if (output?.form !== "flat" || output.data.layout !== "i32") throw Error("wrong frame");
  expect(output.data).not.toBe(vector.data);
  expect(Array.from(output.data.values.subarray(0, frame?.payload.rowCount))).toEqual([3, 4]);
  expect(releases).toBe(1);
  frame?.release();
  source.close();
  result.close();
  expect(governor.usedBytes).toBe(0);
});
