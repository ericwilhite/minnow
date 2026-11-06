/**
 * Persisted planning statistics (`TableRoot.statistics`, ruling on Gate A reopen): an open reads
 * one statistics blob per table and no group root, and the planning summary after a reopen is the
 * one maintained before it, across generated flushes of int and text columns with NULLs.
 */
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { ColumnSummary } from "../access/write-path.js";
import type { BlobRef } from "../storage/extents.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  TEXT,
  tableInput,
  texts,
} from "./test-support.js";

/** Group-root blobs start with the codec's magic "MGR1" (little-endian u32 0x3152474d). */
const isGroupRoot = (bytes: Uint8Array) =>
  bytes[0] === 0x4d && bytes[1] === 0x47 && bytes[2] === 0x52 && bytes[3] === 0x31;

function summary(path: WritePathImpl, t: number): Array<ColumnSummary | null> {
  const def = path.catalog.table(t);
  const stats = path.statistics(t);
  return (def?.columns ?? []).map((c) => stats?.column(c.id) ?? null);
}

describe("persisted planning statistics", () => {
  const SEEDS = seedsFor("next-write-statistics", [401, 402, 403, 404]);
  for (const seed of SEEDS)
    it(`a reopen keeps the maintained summary and reads no group root (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      const harness = new Harness("strict", "sim", 4 + int(12));
      let path = await harness.open();
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [
                column("id", INT, false),
                column("n", INT),
                column("s", TEXT),
              ]),
            })
          ).id ?? 0;
      });
      let next = 0;
      for (let step = 0; step < 30; step++) {
        const count = 1 + int(20);
        const ids = Array.from({ length: count }, () => next++);
        const nulls = random() < 0.3;
        await autocommit(path, (s) =>
          s
            .table(t)
            .insert(
              batch(
                ints(ids),
                ints(ids.map((id) => (nulls && id % 3 === 0 ? null : int(1000) - 500))),
                texts(ids.map((id) => (id % 4 === 0 ? null : "x".repeat(int(30))))),
              ),
              null,
            ),
        );
        if (random() < 0.15) {
          await path.quiesce();
          const before = summary(path, t);
          const rows = path.statistics(t)?.rowCount;
          const reads: BlobRef[] = [];
          harness.onSpace = (space) => {
            const read = space.read.bind(space);
            space.read = async (refs, options) => {
              const lease = await read(refs, options);
              for (const [k, bytes] of lease.bytes.entries()) {
                const ref = refs[k];
                if (ref !== undefined && isGroupRoot(bytes)) reads.push(ref);
              }
              return lease;
            };
            const peek = space.peek.bind(space);
            space.peek = (ref) => {
              const bytes = peek(ref);
              if (bytes !== null && isGroupRoot(bytes)) reads.push(ref);
              return bytes;
            };
          };
          path = random() < 0.5 ? await harness.reopen() : await harness.crash();
          harness.onSpace = null;
          expect(
            reads,
            `seed ${String(seed)} step ${String(step)}: group roots read at open`,
          ).toEqual([]);
          await path.quiesce();
          // A crash may replay rows into memtables (not yet in groups); a reopen after quiesce has
          // the same groups, so its summary is the one maintained before.
          expect(path.statistics(t)?.rowCount).toBe(rows);
          expect(summary(path, t)).toEqual(before);
        }
      }
      await path.quiesce();
      await harness.space.audit();
      await harness.close();
    }, 300_000);
});
