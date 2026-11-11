/** Window ancestors checkpoint before evaluation: peers stay peers and ROWS keeps input ties. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, it, expect } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, RETAINED_WINDOW_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { WindowOperator, windowRetainingMinimumBytes, type WindowSpec } from "./window.js";
import { type WindowCheckpoint } from "./window-checkpoint.js";
import { INT, DBL, TEXT, batchesOf, CollectSink, type Row } from "./join-fixtures.js";
const reach = createReach("window checkpoint", RETAINED_WINDOW_REACH);
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
  n: number,
  pressure: boolean,
  special: boolean,
  route: "grouped" | "sort" | "columnar",
  generatedLarge = false,
  batchRows = 9,
): Promise<void> {
  reach.beginHistory();
  const gov = new Governor(128 * 1024 * 1024),
    spill = new MemorySpillService(),
    sink = new CollectSink(2);
  const types = [INT, INT, DBL, TEXT];
  const rows: Row[] = Array.from({ length: n }, (_, i) => [
    i % 11 === 0 ? null : i % 2,
    i % 7 === 0 ? null : i % 3,
    special && i % 17 === 0
      ? NaN
      : special && i % 19 === 0
        ? Infinity
        : ([1e16, 1, -1e16, 2][i % 4] ?? 0),
    `${String(i)}${"x".repeat(pressure ? 500000 : 2)}`,
  ]);
  await pg.exec("DROP TABLE IF EXISTS w;CREATE TABLE w(id int,p int,k int,v float8,t text)");
  for (let from = 0; from < rows.length; from += 256) {
    const batch = rows.slice(from, from + 256);
    await pg.query(
      `INSERT INTO w VALUES ${batch.map((_, i) => `(${Array.from({ length: 5 }, (_, c) => "$" + String(i * 5 + c + 1)).join(",")})`).join(",")}`,
      batch.flatMap((row, i) => [from + i, ...row]),
    );
  }
  const expected = (
    await pg.query(
      "SELECT p,k,v,t,rank() OVER(PARTITION BY p ORDER BY k),sum(v) OVER(PARTITION BY p ORDER BY k,id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  ,sum(v) OVER(PARTITION BY p ORDER BY k),lag(k) OVER(PARTITION BY p ORDER BY k,id) FROM w ORDER BY p,k,id",
      [],
      { rowMode: "array" },
    )
  ).rows;
  const spec: WindowSpec = {
    checkpointable: true,
    ...(route === "grouped" ? {} : { sortPath: "sort" }),
    types,
    partitionBy: [0],
    orderBy: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
    functions: [
      { fn: "rank", arg: null, frame: null, type: { kind: "int64" } },
      {
        fn: "sum",
        arg: 2,
        type: DBL,
        frame: {
          unit: "rows",
          start: { kind: "unbounded-preceding", offset: null },
          end: { kind: "current-row", offset: null },
          exclude: "no-others",
        },
      },
      { fn: "sum", arg: 2, type: DBL, frame: null },
      { fn: "lag", arg: 1, type: INT, frame: null },
    ],
  };
  const busy = await gov.admit({ label: "busy", minimumBytes: 0, spillable: true });
  const pending: { saved: WindowCheckpoint | undefined } = { saved: undefined };
  let spilledPrefix = false;
  const make = async (add = 0) => {
    const grant = await gov.admit({
      label: "window",
      minimumBytes: windowRetainingMinimumBytes(types, 4) + add,
      spillable: true,
    });
    return new WindowOperator(
      spec,
      { grant, spill, queryId: 1, columnar: route === "columnar" },
      sink,
    );
  };
  let win = await make();
  if (pressure) expect(busy.allocate(gov.capBytes - gov.reserveBytes - gov.usedBytes)).toBe(true);
  const push = async (part: readonly Row[]) => {
    for (const b of batchesOf(types, part, { batchRows, dictionary: true, skip: 3 }))
      if (win.push(b) === "blocked") await win.ready();
  };
  const rotate = async () => {
    spilledPrefix ||= win.profile.sort.runs > 0;
    if (win.profile.sort.runs > 0) reach.reach("spilled prefix");
    pending.saved = await win.checkpoint();
    if (route === "grouped" && n > 8192) {
      expect(pending.saved.records.tape.rows).toBeGreaterThan(4096);
      expect(pending.saved.hasOrdinals).toBe(true);
      reach.reach("multichunk grouped prefix");
      if (generatedLarge) reach.reach("generated multichunk prefix");
    }
    reach.reach(
      route === "grouped"
        ? "lazy grouped cut"
        : route === "columnar"
          ? "lazy columnar cut"
          : "lazy general cut",
    );
    win.abort(new Error("retire"));
    await win.closed;
    if (pressure) {
      const bytes = Math.min(busy.allocated, pending.saved.minimumBytes);
      busy.free(bytes);
      busy.release(bytes);
    }
    win = await make(pending.saved.minimumBytes);
    await win.restore(pending.saved);
    pending.saved = undefined;
  };
  try {
    await push(rows.slice(0, Math.floor(n / 2)));
    await rotate();
    await push(rows.slice(Math.floor(n / 2), Math.floor(n / 2) + 5));
    await rotate();
    await push(rows.slice(Math.floor(n / 2) + 5));
    await win.finish();
    await win.closed;
    expect(sink.rows.map((r) => r.map((v) => (typeof v === "bigint" ? Number(v) : v)))).toEqual(
      expected,
    );
    reach.reach("two cuts");
    reach.reach("peer groups");
    reach.reach("nonassociative floats");
    reach.reach("selected dictionary input");
    reach.reach("NULL partition/order keys");
    reach.reach("RANGE peers and lag");
    if (special) reach.reach("non-finite floats");

    if (pressure) expect(spilledPrefix).toBe(true);
  } finally {
    win.abort(new Error("cleanup"));
    await win.closed;
    await pending.saved?.close();
    busy.close();
    expect(gov.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
    reach.reach("zero resources");
  }
}

for (const seed of seedsFor("next-window-checkpoints", [0xb791, 0xb792])) {
  it(`restores stable window rows without restarting frames (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 20, max: 80 }),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom("grouped", "sort", "columnar"),
        (n, pressure, special, route) => history(n, pressure, special, route),
      ),
      {
        seed,
        numRuns: 13,
        examples: [
          [30, true, false, "grouped"],
          [30, false, true, "grouped"],
          [40, true, true, "sort"],
          [40, false, true, "columnar"],
          [40, true, true, "columnar"],
          [8200, false, true, "grouped"],
          [8200, false, true, "sort"],
          [8200, false, true, "columnar"],
        ],
      },
    );
  }, 120000);

  it(`restores generated multichunk windows (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 8194, max: 9000 }),
        fc.boolean(),
        fc.integer({ min: 1, max: 64 }),
        (n, special, batchRows) => history(n, false, special, "grouped", true, batchRows),
      ),
      { seed, numRuns: 3 },
    );
  }, 120000);
}

// Plentiful memory must never pay for a possible future cut. Observe the actual routes.
for (const route of ["grouped", "columnar"] as const)
  it(`keeps the ${route} route until a real cut`, async () => {
    const governor = new Governor(64 * 1024 * 1024),
      spill = new MemorySpillService();
    const types = [INT, INT, DBL],
      sink = new CollectSink();
    const grant = await governor.admit({
      label: "lazy window",
      minimumBytes: windowRetainingMinimumBytes(types),
      spillable: true,
    });
    const win = new WindowOperator(
      {
        checkpointable: true,
        types,
        partitionBy: [0],
        ...(route === "columnar" ? { sortPath: "sort" } : {}),
        orderBy: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
        functions: [{ fn: "rank", arg: null, frame: null, type: { kind: "int64" } }],
      },
      { grant, spill, queryId: 1, columnar: route === "columnar" },
      sink,
    );
    try {
      for (const batch of batchesOf(types, [
        [0, 2, 1],
        [0, 1, 2],
        [0, 1, 3],
      ]))
        if (win.push(batch) === "blocked") await win.ready();
      await win.finish();
      await win.closed;
      expect(sink.rows.map((row) => row[3])).toEqual([1n, 1n, 3n]);
      if (route === "grouped") expect(win.profile.groupedRows).toBe(3);
      else expect(win.profile.sort).toMatchObject({ columnarRows: 3 });
      expect(win.profile.groupedFallbacks).toBe(0);
      expect(win.profile.sort.spilledBytes).toBe(0);
    } finally {
      win.abort(new Error("cleanup"));
      await win.closed;
    }
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  });
