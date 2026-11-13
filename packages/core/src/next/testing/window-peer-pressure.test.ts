import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, WINDOW_PEER_PRESSURE_REACH } from "../../testing/reach.js";
import { peerPressure } from "./window-peer-pressure-fixture.js";
for (const seed of seedsFor("next-window-peer-pressure", [0xb7b0, 0xb7b1]))
  it(`funds peer emission with all credit occupied (${String(seed)})`, async () => {
    const pg = new PGlite(),
      random = mulberry32(seed),
      reach = createReach(`peer pressure ${String(seed)}`, WINDOW_PEER_PRESSURE_REACH);
    try {
      await pg.waitReady;
      for (const array of [false, true])
        for (const width of array
          ? [6144, 6656]
          : [60000, 65536, 60000 + Math.floor(random() * 5537)])
          for (const pressure of [false, true]) {
            reach.beginHistory();
            reach.reach(array ? "array" : "jsonb");
            reach.reach(pressure ? "all credit occupied" : "unpressured");
            const result = await peerPressure(array, width, pressure);
            const type = array ? "text[]" : "jsonb";
            const expected = (
              await pg.query(
                `SELECT n,row_number() OVER(ORDER BY k)::int4 rn FROM(VALUES(1::float8,$1::${type}),(2::float8,$2::${type}))t(n,k)`,
                result.values,
              )
            ).rows;
            expect(result.rows).toEqual(expected);
            if (pressure) expect(result.occupied).toBeGreaterThan(0);
            expect(result.peak).toBeLessThanOrEqual(128 << 20);
          }
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
