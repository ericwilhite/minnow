/** One window emit step shares the prepaid pool: persistent flags are counted once. */
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { Pool } from "./spill-pool.js";
for (const seed of seedsFor("window-emit-peak", [0xb7b2, 0xb7b3]))
  it(`never raises the parent step peak (${String(seed)})`, () => {
    const random = mulberry32(seed),
      base = 1 << 20,
      flags = 4096;
    const run = (shared: boolean, sizes: number[]) => {
      let used = 0,
        peak = 0;
      const charge = {
        allocate(n: number) {
          used += n;
          peak = Math.max(used, peak);
          return true;
        },
        free(n: number) {
          used -= n;
        },
      };
      const pool = new Pool(charge, base, shared);
      if (shared) expect(pool.allocate(flags)).toBe(true);
      else charge.allocate(flags);
      for (const size of sizes) expect(pool.allocate(size)).toBe(true);
      for (const size of [...sizes].reverse()) pool.free(size);
      if (shared) pool.free(flags);
      else charge.free(flags);
      pool.close();
      expect(used).toBe(0);
      return peak;
    };
    for (const size of [1, base - flags, base - flags + 1, base, base + 1, 2 * base])
      expect(run(true, [size])).toBeLessThanOrEqual(run(false, [size]));
    for (let i = 0; i < 256; i++) {
      const sizes = Array.from(
        { length: 1 + Math.floor(random() * 5) },
        () => 1 + Math.floor(random() * 2 * base),
      );
      expect(run(true, sizes)).toBeLessThanOrEqual(run(false, sizes));
    }
  });
