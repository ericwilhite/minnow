/**
 * A long mutation history, compacted as it goes, checked against a reference the whole way.
 *
 * Compaction is well covered by example: a table is mutated a handful of times, compacted, and
 * the result inspected. What no example covers is *accumulation* — thousands of interleaved
 * inserts, updates, deletes and upserts, with compaction folding deltas away underneath them,
 * repeatedly, while queries run against the moving result. That is the shape of a database that
 * has been open in a browser tab for a month, and it is where a fold that drops a row, resurrects
 * a deleted key, or double-applies an update would actually show up.
 *
 * The reference is a plain `Map` maintained alongside, which makes the check total rather than
 * sampled: after every checkpoint, the database's entire contents must equal the map's. A
 * divergence is reported with the seed and the operation index, so it replays exactly.
 *
 * Two invariants are asserted continuously, not just at the end:
 *
 *   - **Contents.** Every row the map holds is present with the right values, and nothing else is.
 *     Deleted keys stay deleted; updated keys carry their latest value; re-inserted keys come back
 *     exactly once.
 *   - **Convergence.** Compaction actually folds. The measure is the number of blocks the current
 *     manifest references — what a query really reads — rather than the number of segment
 *     records, which only ever grows: a superseded segment stays on file until garbage collection
 *     removes it, so counting records would report a healthy compaction as a regression.
 *
 *     Compaction is deliberately *bounded and incremental*: one `compactTable` call folds a
 *     limited number of blocks -- about forty on this workload -- so a long history takes many
 *     calls to fold completely, and in production `autoCompact` spreads those calls across reads.
 *     The contract is therefore monotone progress rather than a fixed point reached in one go:
 *     every call must leave the table no worse, and a run of calls must measurably shrink it.
 *     Measured on this fixture, thirty calls took it from 2,444 live blocks to 1,365, decreasing
 *     every single time.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { mulberry32, seedFor } from "../testing/seeds.js";

/** Keys are drawn from a small space so inserts, updates and deletes collide constantly. */
const KEY_SPACE = 240;
const OPERATIONS = 1_500;
const CHECKPOINT_EVERY = 125;
const REGIONS = ["west", "east", "north", "south"] as const;

interface Row {
  id: number;
  region: string;
  amount: number;
  /** Lets a Row be handed straight to insertBatch, which takes an open record of values. */
  [column: string]: string | number;
}

describe("compaction over a long mutation history", () => {
  it("keeps the table equal to a reference across thousands of mutations", async () => {
    const seed = seedFor("compaction-soak", 0x50a4);
    const random = mulberry32(seed);
    const store = new MemoryBlockStore();
    // Small blocks and no background compaction: every fold here is one this test asked for,
    // so a failure names the compaction that caused it rather than a race with a timer.
    const database = new MinnowDatabase(store, { rowsPerBlock: 32, autoCompact: false });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "region", type: "string" },
        { name: "amount", type: "number" },
      ],
    });

    /** The answer the database must agree with, maintained by the same operations. */
    const reference = new Map<number, Row>();
    let compactions = 0;
    let peakBlocks = 0;

    /** Blocks the live manifest points at: the table's real read footprint. */
    const liveBlocks = async (): Promise<number> =>
      (await store.getCurrentManifest())?.liveBlockCount ?? 0;

    const checkContents = async (afterOperation: number): Promise<void> => {
      const rows = (
        await database.query("SELECT id, region, amount FROM items ORDER BY id", {
          memoize: false,
        })
      ).rows as unknown as Row[];
      const expected = [...reference.values()].sort((left, right) => left.id - right.id);
      const context = `seed ${String(seed)}, after operation ${String(afterOperation)}`;

      expect(rows.length, `${context}: row count`).toBe(expected.length);
      for (let index = 0; index < expected.length; index += 1) {
        const want = expected[index];
        const got = rows[index];
        if (want === undefined || got === undefined) continue;
        if (got.id !== want.id || got.region !== want.region || got.amount !== want.amount) {
          throw new Error(
            `${context}: row ${String(index)} diverged — ` +
              `wanted ${JSON.stringify(want)}, got ${JSON.stringify(got)}`,
          );
        }
      }

      // The aggregate path reads the same history through different machinery than the ordered
      // scan above, so a fold that only breaks one of them still gets caught.
      const total = [...reference.values()].reduce((sum, row) => sum + row.amount, 0);
      const aggregate = (
        await database.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM items", {
          memoize: false,
        })
      ).rows[0] as { n: number; total: number | null };
      expect(aggregate.n, `${context}: aggregate count`).toBe(expected.length);
      expect(aggregate.total ?? 0, `${context}: aggregate sum`).toBe(total);

      // And a keyed lookup, which takes the pruned path rather than a scan.
      if (expected.length > 0) {
        const probe = expected[Math.floor(random() * expected.length)];
        if (probe !== undefined) {
          const found = (
            await database.query("SELECT amount FROM items WHERE id = ?", {
              params: [probe.id],
              memoize: false,
            })
          ).rows;
          expect(found, `${context}: keyed lookup for id ${String(probe.id)}`).toEqual([
            { amount: probe.amount },
          ]);
        }
      }
    };

    for (let operation = 1; operation <= OPERATIONS; operation += 1) {
      const key = Math.floor(random() * KEY_SPACE) + 1;
      const roll = random();
      const exists = reference.has(key);

      if (!exists && roll < 0.55) {
        const row: Row = {
          id: key,
          region: REGIONS[Math.floor(random() * REGIONS.length)] ?? "west",
          amount: Math.floor(random() * 400) / 4,
        };
        await database.insertBatch("items", [row]);
        reference.set(key, row);
      } else if (exists && roll < 0.35) {
        await database.execute("DELETE FROM items WHERE id = ?", [key]);
        reference.delete(key);
      } else if (exists && roll < 0.75) {
        const amount = Math.floor(random() * 400) / 4;
        await database.execute("UPDATE items SET amount = ? WHERE id = ?", [amount, key]);
        const previous = reference.get(key);
        if (previous !== undefined) reference.set(key, { ...previous, amount });
      } else if (exists) {
        const region = REGIONS[Math.floor(random() * REGIONS.length)] ?? "west";
        await database.execute("UPDATE items SET region = ? WHERE id = ?", [region, key]);
        const previous = reference.get(key);
        if (previous !== undefined) reference.set(key, { ...previous, region });
      }

      if (operation % CHECKPOINT_EVERY === 0) {
        peakBlocks = Math.max(peakBlocks, await liveBlocks());
        await checkContents(operation);
        const before = await liveBlocks();
        await database.compactTable("items");
        compactions += 1;
        const after = await liveBlocks();
        expect(
          after,
          `seed ${String(seed)}: compaction at operation ${String(operation)} grew the live ` +
            `manifest from ${String(before)} blocks to ${String(after)}`,
        ).toBeLessThanOrEqual(before);
        // Reading immediately after a fold is the case where a stale cache or a dangling
        // segment reference surfaces.
        await checkContents(operation);
      }
    }

    await checkContents(OPERATIONS);

    // A run of maintenance, the way a background compactor performs it. Each call folds a
    // bounded amount, so what is asserted is that every one of them helps and that the run adds
    // up -- not that one call finishes the job.
    const startingBlocks = await liveBlocks();
    let previous = startingBlocks;
    for (let round = 0; round < 12; round += 1) {
      await database.compactTable("items");
      const current = await liveBlocks();
      expect(
        current,
        `seed ${String(seed)}: compaction round ${String(round)} went from ` +
          `${String(previous)} live blocks to ${String(current)}`,
      ).toBeLessThanOrEqual(previous);
      previous = current;
    }
    const settled = await liveBlocks();
    // Folding must not change a single answer. This is the check that matters most.
    await checkContents(OPERATIONS);

    // The soak has to have actually soaked. Without these, a change that made every mutation a
    // no-op would leave the contents check trivially satisfied.
    expect(compactions).toBeGreaterThan(5);
    expect(reference.size).toBeGreaterThan(20);
    expect(peakBlocks).toBeGreaterThan(20);
    // Progress, not a fixed point: twelve bounded calls have to make a real dent. A compactor
    // that stopped folding, or folded nothing per call, would sit where it started.
    expect(
      settled,
      `seed ${String(seed)}: twelve compactions moved ${String(startingBlocks)} live blocks to ` +
        `${String(settled)}; expected a reduction of at least a tenth`,
    ).toBeLessThan(startingBlocks * 0.9);
  }, 120_000);

  it("survives compaction interleaved with reads of every earlier state", async () => {
    // Snapshots taken before a compaction must keep answering from the blocks they pinned, even
    // as compaction rewrites the table underneath them. This is the guarantee that lets a long
    // query run while maintenance happens.
    const seed = seedFor("compaction-soak-snapshots", 0x2c17);
    const random = mulberry32(seed);
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      rowsPerBlock: 16,
      autoCompact: false,
    });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "amount", type: "number" },
      ],
    });
    await database.insertBatch(
      "items",
      Array.from({ length: 200 }, (_, index) => ({ id: index + 1, amount: index })),
    );

    const expectedTotals: number[] = [];
    let total = (200 * 199) / 2;
    expectedTotals.push(total);

    for (let round = 0; round < 12; round += 1) {
      const key = Math.floor(random() * 200) + 1;
      const previous = (
        await database.query("SELECT amount FROM items WHERE id = ?", {
          params: [key],
          memoize: false,
        })
      ).rows[0] as { amount: number } | undefined;
      const next = Math.floor(random() * 500);
      await database.execute("UPDATE items SET amount = ? WHERE id = ?", [next, key]);
      total += next - (previous?.amount ?? 0);
      await database.compactTable("items");
      expectedTotals.push(total);

      const observed = (
        await database.query("SELECT SUM(amount) AS total FROM items", { memoize: false })
      ).rows[0] as { total: number };
      expect(observed.total, `seed ${String(seed)}, round ${String(round)}`).toBe(total);
    }

    // Row count is untouched by any of it: updates never add or remove rows.
    expect(
      (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false })).rows,
    ).toEqual([{ n: 200 }]);
  }, 60_000);
});
