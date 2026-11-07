/** PostgreSQL JOIN semantics and ColumnStore's row-count contract include zero-column tuples. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { ColumnStore } from "./column-store.js";
import { INT, canonical, runJoin, type Row } from "./join-fixtures.js";
const kinds = ["inner", "left", "right", "full", "semi", "anti", "mark"] as const;
const reach = createReach("next-join-key-only", [
  ...kinds,
  "zero columns",
  "single-chunk limit",
  "unbounded rows",
]);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});

for (const seed of seedsFor("next-join-key-only", [0xb701, 0xb702]))
  it(`generated key-only joins and zero-payload gathers seed ${String(seed)}`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed);
    const descriptor = Object.getOwnPropertyDescriptor(ColumnStore.prototype, "chunkCount");
    if (!descriptor?.get) throw new Error("chunk count getter required");
    let reads = 0;
    // A bad implementation fails for non-progress before it can trap this test worker.
    const spy = vi.spyOn(ColumnStore.prototype, "chunkCount", "get").mockImplementation(function (
      this: ColumnStore,
    ) {
      if (this.types.length === 0 && ++reads > 64)
        throw new Error("empty-schema append made no row progress");
      return descriptor.get?.call(this) as number;
    });
    try {
      for (const finite of [false, true]) {
        const store = new ColumnStore(
          [],
          { allocate: () => true, free: () => undefined },
          { chunkRows: 4, ...(finite ? { targetBytes: 64 } : {}) },
        );
        try {
          reads = 0;
          expect(store.append([], null, 0, 13)).toBe(finite ? 4 : 13);
          expect(store.rows).toBe(finite ? 4 : 13);
          expect(store.chunkCount).toBe(finite ? 1 : 4);
          expect(store.vectors()).toEqual([]);
          store.truncate(0);
          reads = 0;
          const source = new ColumnStore([], { allocate: () => true, free: () => undefined });
          source.append([], null, 0, 1);
          try {
            expect(store.appendFrom(source, [], Int32Array.of(-1, 0, 0), 0, 3)).toBe(3);
          } finally {
            source.close();
          }
          expect(store.rows).toBe(3);
          store.reset();
          expect(store.rows).toBe(0);
          reach.reach(finite ? "single-chunk limit" : "unbounded rows");
          reach.reach("zero columns");
        } finally {
          store.close();
        }
      }
      const b: Row[] = [
        [1],
        ...Array.from({ length: 8 }, () => [random() < 0.2 ? null : Math.floor(random() * 4)]),
      ];
      const p: Row[] = [
        [1],
        [9],
        [null],
        ...Array.from({ length: 5 }, () => [Math.floor(random() * 4)]),
      ];
      const vals = (rows: Row[]) =>
        rows.map((r) => `(${r[0] === null ? "NULL::int" : String(r[0])})`).join(",");
      for (const kind of kinds)
        for (const cap of [8 << 20, "tight"] as const) {
          reads = 0;
          const ours = await runJoin(
            {
              kind,
              probeTypes: [INT],
              buildTypes: [INT],
              probeKeys: [0],
              buildKeys: [0],
              nullSafe: [false],
              residual: null,
              publishes: [],
            },
            b,
            p,
            { cap, slack: 0, batchRows: 1, blockEvery: 1 },
          );
          const sql =
            `WITH b(k) AS (VALUES ${vals(b)}),p(k) AS (VALUES ${vals(p)}) ` +
            (kind === "mark"
              ? "SELECT p.k,p.k IN(SELECT k FROM b) FROM p"
              : kind === "semi" || kind === "anti"
                ? `SELECT p.k FROM p WHERE ${kind === "anti" ? "NOT " : ""}EXISTS(SELECT 1 FROM b WHERE b.k=p.k)`
                : `SELECT p.k,b.k FROM p ${kind.toUpperCase()} JOIN b ON b.k=p.k`);
          const expected = await pg.query(sql, [], { rowMode: "array" } as never);
          expect(canonical(ours.rows)).toEqual(canonical(expected.rows as unknown as Row[]));
          expect(ours.governor.usedBytes).toBe(0);
          expect(ours.spill.bytesInUse).toBe(0);
          reach.reach(kind);
        }
    } finally {
      spy.mockRestore();
    }
  }, 60000);
