/**
 * A long mutation history with the background maintenance on, checked against a reference the
 * whole way.
 *
 * compaction-soak.test.ts folds on demand, so a failure names the fold that caused it. This is
 * the other half: the folds and collection passes a database schedules for itself, landing
 * while inserts, updates, deletes and queries keep coming — the shape of a tab open all day. It
 * answers three questions the on-demand soak cannot: that a fold or a pass interleaved with
 * writes never changes an answer, that the table actually converges (a scan reads a bounded
 * number of segments, the store holds a bounded number of blocks), and that what was written
 * during a fold is still there after it.
 *
 * The reference is a plain Map maintained alongside; after every checkpoint, the database's
 * contents must equal it. A divergence reports the seed and the operation index.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { allVisibleSegments, heavyTestTimeout } from "./storage-test-helpers.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";

vi.setConfig({ testTimeout: heavyTestTimeout(180_000) });

/** Keys are drawn from a small space so inserts, updates and deletes collide constantly. */
const KEY_SPACE = 400;
const OPERATIONS = 1_800;
const CHECKPOINT_EVERY = 150;
const REGIONS = ["west", "east", "north", "south"] as const;
// These pin the public maintenance behaviour exercised by the soak: a fold is due at either
// threshold. A mixed insert/mutation tail can therefore legitimately contain 32-47 segments
// when fewer than 32 of them are deltas.
const AUTO_COMPACT_SCAN_SEGMENTS = 48;
const AUTO_COMPACT_DELTA_SEGMENTS = 32;

interface Row {
  id: number;
  region: string;
  amount: number;
  [column: string]: string | number;
}

describe.each(seedsFor("auto-compaction-soak", [0x7a5c]))(
  "background maintenance over a long mutation history (seed %s)",
  (seed) => {
    it("keeps the table equal to a reference while folds and collection passes land", async () => {
      const random = mulberry32(seed);
      const store = new MemoryBlockStore();
      // Small blocks so the table is many blocks wide; maintenance on, as it ships. The clock is
      // the test's, so the age bound on retained versions can be crossed without waiting.
      let clock = Date.parse("2026-01-01T00:00:00Z");
      const database = new MinnowDatabase(store, {
        rowsPerBlock: 32,
        now: () => new Date(clock),
      });
      await database.createTable({
        name: "items",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "region", type: "string" },
          { name: "amount", type: "number" },
        ],
      });
      const reference = new Map<number, Row>();
      const liveBlocks = async (): Promise<number> =>
        (await store.getCurrentManifest())?.liveBlockCount ?? 0;
      const visibleRecords = async () =>
        Promise.all(
          (await allVisibleSegments(database, "items")).map(async (segment) => {
            const record = await store.getSegment(segment.id);
            if (record === undefined) throw new Error(`Visible segment is missing: ${segment.id}`);
            return record;
          }),
        );
      const autoCompactionDue = async (): Promise<boolean> => {
        const records = await visibleRecords();
        const levelZero = records.filter((segment) => segment.level === 0).length;
        const deltas = records.filter((segment) => {
          const kind = segment.kind;
          return kind !== "insert" && kind !== "base";
        }).length;
        return levelZero >= AUTO_COMPACT_SCAN_SEGMENTS || deltas >= AUTO_COMPACT_DELTA_SEGMENTS;
      };

      const checkContents = async (afterOperation: number): Promise<void> => {
        const context = `seed ${String(seed)}, after operation ${String(afterOperation)}`;
        const rows = (
          await database.query("SELECT id, region, amount FROM items ORDER BY id", {
            memoize: false,
          })
        ).rows as unknown as Row[];
        const expected = [...reference.values()].sort((left, right) => left.id - right.id);
        expect(rows.length, `${context}: row count`).toBe(expected.length);
        for (let index = 0; index < expected.length; index += 1) {
          const want = expected[index];
          const got = rows[index];
          if (want === undefined || got === undefined) continue;
          if (got.id !== want.id || got.region !== want.region || got.amount !== want.amount) {
            throw new Error(
              `${context}: row ${String(index)} diverged — wanted ${JSON.stringify(want)}, got ${JSON.stringify(got)}`,
            );
          }
        }
        const total = expected.reduce((sum, row) => sum + row.amount, 0);
        const aggregate = (
          await database.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM items", {
            memoize: false,
          })
        ).rows[0] as { n: number; total: number | null };
        expect(aggregate.n, `${context}: aggregate count`).toBe(expected.length);
        expect(aggregate.total ?? 0, `${context}: aggregate sum`).toBe(total);
        if (expected.length > 0) {
          const probe = expected[Math.floor(random() * expected.length)];
          if (probe !== undefined) {
            expect(
              (
                await database.query("SELECT amount FROM items WHERE id = ?", {
                  params: [probe.id],
                  memoize: false,
                })
              ).rows,
              `${context}: keyed lookup for id ${String(probe.id)}`,
            ).toEqual([{ amount: probe.amount }]);
          }
        }
      };

      /** Lets the background loops run: real time for the timers, clock time for the age bound. */
      const breathe = async (ms: number): Promise<void> => {
        await new Promise((resolve) => setTimeout(resolve, ms));
        clock += ms;
      };

      let peakVisible = 0;
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
        // A read every few writes, like an application, so folds land under queries too.
        if (operation % 7 === 0) {
          await database.query("SELECT COUNT(*) AS n FROM items WHERE amount > 50", {
            memoize: false,
          });
        }
        // Give the background loops room every so often; a tight loop starves their timers.
        if (operation % 25 === 0) await breathe(5);
        if (operation % CHECKPOINT_EVERY === 0) {
          peakVisible = Math.max(peakVisible, (await allVisibleSegments(database, "items")).length);
          await checkContents(operation);
        }
      }
      await checkContents(OPERATIONS);

      /** Waits for the loops to go quiet: no active job, the table not due, the footprint still. */
      const settle = async (): Promise<void> => {
        let quiet = 0;
        let previous = "";
        // Twenty quiet polls: a run's planning has no job record to show for itself until it ends.
        for (let attempt = 0; attempt < 600 && quiet < 20; attempt += 1) {
          await breathe(50);
          const active =
            (await database.listCompactionJobs()).some(
              (job) =>
                job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
            ) ||
            (await database.listGarbageCollectionJobs()).some(
              (job) => job.state === "planned" || job.state === "running",
            );
          const visible = (await allVisibleSegments(database, "items")).length;
          const storageStats = await store.getStorageStats();
          const current = JSON.stringify([
            visible,
            await liveBlocks(),
            storageStats.liveBlockCount + storageStats.obsoleteBlockCount,
          ]);
          quiet = !active && !(await autoCompactionDue()) && current === previous ? quiet + 1 : 0;
          previous = current;
        }
        expect(quiet, `seed ${String(seed)}: background maintenance did not settle`).toBe(20);
      };
      /**
       * A quiet minute on the database's clock and one commit to wake the collector, as an idle
       * tab's timer would. Twice: the first commit lands while the folds are still draining the
       * backlog, so its own manifest — kept readable for a minute — still references the pre-fold
       * blocks; the second, a minute later, lets that one go too.
       */
      const quietMinute = async (): Promise<void> => {
        clock += 61_000;
        await database.execute("UPDATE items SET amount = amount + 0 WHERE id = ?", [
          [...reference.keys()][0] ?? 1,
        ]);
        await settle();
      };
      await settle();
      await quietMinute();
      await quietMinute();
      await checkContents(OPERATIONS + 1);
      // Background planning intentionally has no half-built job record. Join its serialized
      // queue with one explicit pass before measuring the final bytes, so slow coverage
      // instrumentation cannot mistake a planner that has not persisted its job yet for quiet.
      await database.collectGarbage();

      // Convergence: the history was long, and what is left of it is small.
      const records = await visibleRecords();
      const levelOne = records.filter((segment) => segment.level === 1);
      const levelZero = records.filter((segment) => segment.level === 0);
      const deltas = records.filter((segment) => {
        const kind = segment.kind;
        return kind !== "insert" && kind !== "base";
      });
      const visible = records.length;
      const storageStats = await store.getStorageStats();
      const stored = storageStats.liveBlockCount + storageStats.obsoleteBlockCount;
      expect(peakVisible, `seed ${String(seed)}: the soak has to have soaked`).toBeGreaterThan(32);
      expect(reference.size).toBeGreaterThan(40);
      // The folded table fits in one L1 partition. A mixed tail can stop below the 48-segment
      // scan threshold even when it is longer than the 32-delta threshold, because inserts are
      // not deltas. Pin both actual due conditions instead of assuming every tail segment is one.
      expect(levelOne, `seed ${String(seed)}: folded partitions after settling`).toHaveLength(1);
      expect(levelZero.length, `seed ${String(seed)}: L0 segments after settling`).toBeLessThan(
        AUTO_COMPACT_SCAN_SEGMENTS,
      );
      expect(deltas.length, `seed ${String(seed)}: delta segments after settling`).toBeLessThan(
        AUTO_COMPACT_DELTA_SEGMENTS,
      );
      expect(visible, `seed ${String(seed)}: visible segments after settling`).toBeLessThanOrEqual(
        AUTO_COMPACT_SCAN_SEGMENTS,
      );
      expect(stored, `seed ${String(seed)}: collection left non-live blocks`).toBe(
        await liveBlocks(),
      );
      expect(storageStats.obsoleteBlockCount, `seed ${String(seed)}: obsolete payloads`).toBe(0);
      expect((await database.listCompactionJobs()).length).toBeGreaterThan(0);
      expect((await database.listGarbageCollectionJobs()).length).toBeGreaterThan(0);
    });
  },
);
