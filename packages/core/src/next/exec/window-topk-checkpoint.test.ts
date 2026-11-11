/** A bounded ranking prefix is sufficient for every future bounded ranking suffix. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, RETAINED_TOPK_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { WindowOperator, windowRetainingMinimumBytes, type WindowSpec } from "./window.js";
import type { WindowCheckpoint } from "./window-checkpoint.js";
import { INT, TEXT, CollectSink, batchesOf, type Row } from "./join-fixtures.js";
const reach = createReach("retained bounded ranking", RETAINED_TOPK_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});

async function history(
  seed: number,
  fn: "rank" | "row_number",
  n: number,
  pressure: boolean,
  cut: number,
  ordered: boolean,
) {
  reach.beginHistory();
  reach.reach(fn);
  reach.reach("peer ties");
  reach.reach("NULL keys");
  reach.reach("selected dictionary");
  if (!ordered && fn === "rank") reach.reach("no order");
  const random = mulberry32(seed + n),
    k = 3,
    types = [INT, INT, INT, TEXT];
  const rows: Row[] = Array.from({ length: n }, (_, id) => [
    id % 11 === 0 ? null : id % 3,
    id % 7 === 0 ? null : Math.floor(random() * 7),
    id,
    `${String(id)}${"x".repeat(pressure ? 500000 : 2)}`,
  ]);
  await pg.exec("DROP TABLE IF EXISTS ranked;CREATE TABLE ranked(p int,k int,id int,t text)");
  for (let at = 0; at < n; at += 128) {
    const part = rows.slice(at, at + 128);
    await pg.query(
      `INSERT INTO ranked VALUES ${part.map((_, i) => `(${[1, 2, 3, 4].map((c) => `$${String(i * 4 + c)}`).join(",")})`).join(",")}`,
      part.flat(),
    );
  }
  const key = ordered ? "k DESC NULLS LAST" : "";
  const windowOrder = fn === "row_number" ? `${key ? key + "," : ""}id` : key;
  const sql = `SELECT p,k,id,t,r FROM(SELECT *,${fn}() OVER(PARTITION BY p ${windowOrder ? "ORDER BY " + windowOrder : ""}) r FROM ranked)w WHERE r<=${String(k)} ORDER BY p NULLS LAST,${key ? key + "," : ""}id`;
  const expected = (await pg.query<readonly unknown[]>(sql, [], { rowMode: "array" })).rows;
  const governor = new Governor(128 << 20),
    spill = new MemorySpillService(),
    sink = new CollectSink(2);
  const busy = await governor.admit({
    label: "concurrent owner",
    minimumBytes: 0,
    spillable: true,
  });
  const spec: WindowSpec = {
    checkpointable: true,
    topK: k,
    types,
    partitionBy: [0],
    orderBy: [
      ...(ordered
        ? [{ column: 1, direction: "desc" as const, nulls: "last" as const, collation: null }]
        : []),
      ...(fn === "row_number"
        ? [{ column: 2, direction: "asc" as const, nulls: "last" as const, collation: null }]
        : []),
    ],
    functions: [{ fn, arg: null, frame: null, type: { kind: "int64" } }],
  };
  const make = async (extra = 0) =>
    new WindowOperator(
      spec,
      {
        grant: await governor.admit({
          label: "top-k",
          minimumBytes: windowRetainingMinimumBytes(types) + extra,
          spillable: true,
        }),
        spill,
        queryId: 1,
      },
      sink,
    );
  let actor = await make(),
    pending: WindowCheckpoint | undefined;
  if (pressure)
    expect(busy.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes)).toBe(
      true,
    );
  const push = async (from: number, to: number) => {
    for (const batch of batchesOf(types, rows.slice(from, to), {
      batchRows: 137,
      dictionary: true,
      skip: 3,
    }))
      if (actor.push(batch) === "blocked") await actor.ready();
  };
  const rotate = async (prefix: number) => {
    expect(sink.rows).toHaveLength(0);
    const wasBounded = actor.profile.boundedRanking;
    const compactions = actor.profile.sort.compactions;
    if (actor.profile.sort.runs > 0) reach.reach("spilled prefix");
    pending = await actor.checkpoint();
    expect(pending.nextOrdinal).toBe(BigInt(prefix));
    if (prefix === 0) {
      expect(pending.records.tape.rows).toBe(0);
      reach.reach("empty cut");
    } else {
      reach.reach("nonempty cut");
      expect(pending.records.tape.rows).toBeLessThanOrEqual(prefix);
    }
    if (wasBounded && compactions > 0) {
      expect(pending.records.tape.rows).toBeLessThan(prefix);
      reach.reach("compacted prefix");
      if (n > 131074) reach.reach("generated compacted prefix");
    }
    const minimum = pending.minimumBytes;
    actor.abort(Error("retire"));
    await actor.closed;
    if (pressure) {
      const bytes = Math.min(busy.allocated, minimum);
      busy.free(bytes);
      busy.release(bytes);
    }
    actor = await make(minimum);
    expect(actor.profile.boundedRanking).toBe(true);
    await actor.restore(pending);
    pending = undefined;
    expect(actor.profile.boundedRanking).toBe(true);
  };
  try {
    expect(actor.profile.boundedRanking).toBe(true);
    reach.reach("fast before cut");
    await push(0, cut);
    await rotate(cut);
    const second = Math.min(n, cut + 5);
    await push(cut, second);
    await rotate(second);
    reach.reach("two cuts");
    await push(second, n);
    await actor.finish();
    await actor.closed;
    expect(actor.profile.rowsIn).toBe(n);
    // The proven predicate remains above the general continuation, as it does in SQL.
    const actual = sink.rows
      .map((row) => row.map((value) => (typeof value === "bigint" ? Number(value) : value)))
      .filter((row) => Number(row[4]) <= k);
    // Apply the same outer ORDER BY as the authority query. RANK does not order peers.
    actual.sort((a, b) => {
      const pa = a[0],
        pb = b[0];
      if (pa === null && pb !== null) return 1;
      if (pa !== null && pb === null) return -1;
      if (pa !== pb) return Number(pa) - Number(pb);
      if (ordered) {
        const ka = a[1],
          kb = b[1];
        if (ka === null && kb !== null) return 1;
        if (ka !== null && kb === null) return -1;
        if (ka !== kb) return Number(kb) - Number(ka);
      }
      return Number(a[2]) - Number(b[2]);
    });
    expect(actual.length).toBe(expected.length);
    for (let i = 0; i < actual.length; i++) {
      const row = actual[i],
        authority = expected[i];
      if (row === undefined || authority === undefined) throw Error("missing ranking row");
      expect(row.length).toBe(authority.length);
      for (let column = 0; column < row.length; column++)
        expect(
          Object.is(row[column], authority[column]),
          `row ${String(i)}, column ${String(column)}`,
        ).toBe(true);
    }
  } finally {
    actor.abort(Error("cleanup"));
    await actor.closed;
    await pending?.close();
    busy.close();
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    reach.reach("zero resources");
  }
}
for (const seed of seedsFor("next-retaining-topk", [0xb7d0, 0xb7d1]))
  for (const fn of ["rank", "row_number"] as const)
    it(`keeps ${fn} fast until a real cut (${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 20, max: 60 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          (n, pressure, empty, ordered) =>
            history(seed, fn, n, pressure, empty ? 0 : Math.floor(n / 2), ordered),
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [30, true, false, true],
            [40, false, true, true],
            [131074, false, false, true],
            [40, false, false, false],
          ],
        },
      );
    }, 120000);

// Vary sizes beyond the real compaction threshold, not only its fixed regression anchor.
for (const seed of seedsFor("next-retaining-topk-large", [0xb7d6, 0xb7d7]))
  it(`restores generated compacted prefixes (${String(seed)})`, async () => {
    for (const fn of ["rank", "row_number"] as const)
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 131076, max: 131194 }), (n) =>
          history(seed, fn, n, false, Math.floor(n / 2), true),
        ),
        { seed, numRuns: 2, endOnFailure: true },
      );
  }, 120000);

for (const seed of seedsFor("next-retaining-topk-empty-schema", [0xb7d2, 0xb7d3]))
  it(`preserves zero-payload ranking rows through cuts (${String(seed)})`, async () => {
    for (const route of [
      "bounded",
      "grouped",
      "general",
      "grouped-stream",
      "general-stream",
      "grouped-stream-no-cut",
      "general-stream-no-cut",
    ] as const)
      for (const fn of ["rank", "row_number"] as const) {
        const n = seed % 2 === 0 ? 4099 : 13,
          k = 3,
          governor = new Governor(32 << 20),
          spill = new MemorySpillService(),
          sink = new CollectSink();
        const make = async (extra = 0) =>
          new WindowOperator(
            {
              checkpointable: true,
              ...(route === "bounded" ? { topK: k } : {}),
              ...(route.startsWith("general") ? { grouped: false } : {}),
              ...(route.includes("stream") ? { streamRows: n > 2048 ? 2048 : 5 } : {}),
              types: [],
              partitionBy: [],
              orderBy: [],
              functions: [{ fn, arg: null, frame: null, type: { kind: "int64" } }],
            },
            {
              grant: await governor.admit({
                label: "zero-payload ranking",
                minimumBytes: windowRetainingMinimumBytes([]) + extra,
                spillable: true,
              }),
              spill,
              queryId: 1,
            },
            sink,
          );
        let actor = await make(),
          pending: WindowCheckpoint | undefined;
        try {
          let consumed = 0;
          const first = Math.min(2051, n - 5);
          for (const count of route.endsWith("-no-cut") ? [] : [first, 2]) {
            for (let at = 0; at < count; at += 2048)
              if (
                actor.push({ length: Math.min(2048, count - at), columns: [], selection: null }) ===
                "blocked"
              )
                await actor.ready();
            consumed += count;
            pending = await actor.checkpoint();
            expect(pending.records.tape.rows).toBe(consumed);
            actor.abort(Error("retire"));
            await actor.closed;
            actor = await make(pending.minimumBytes);
            await actor.restore(pending);
            pending = undefined;
          }
          for (let at = consumed; at < n; at += 2048)
            if (
              actor.push({ length: Math.min(2048, n - at), columns: [], selection: null }) ===
              "blocked"
            )
              await actor.ready();
          await actor.finish();
          await actor.closed;
          const sql = `SELECT r FROM(SELECT ${fn}() OVER() r FROM generate_series(1,${String(n)}))q WHERE r<=${String(k)}`;
          expect(
            sink.rows.map((row) => row.map(Number)).filter((row) => Number(row[0]) <= k),
          ).toEqual((await pg.query(sql, [], { rowMode: "array" })).rows);
          reach.beginHistory();
          reach.reach("zero-payload rows");
          if (route.startsWith("grouped")) reach.reach("zero-payload grouped");
          if (route.startsWith("general")) reach.reach("zero-payload general");
          if (route.includes("stream")) {
            expect(actor.profile.streamedPartitions).toBeGreaterThan(0);
            expect(actor.profile.spilledPartitions).toBeGreaterThan(0);
            reach.reach("zero-payload stream");
          }
          if (route === "grouped-stream-no-cut")
            reach.reach("zero-payload grouped stream without cut");
          if (first > 2048) reach.reach("zero-payload multichunk");
        } finally {
          actor.abort(Error("cleanup"));
          await actor.closed;
          await pending?.close();
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
        }
      }
  });
