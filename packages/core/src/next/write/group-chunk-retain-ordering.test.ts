/**
 * Group chunks are refcounted by the write states that list them (write/groups.ts "Ownership"). A
 * flush derives its next directory from the directory it saw when it started (`before.with`),
 * which retains every chunk it shares - after it has awaited the writes of its groups. A DROP TABLE
 * committed during those awaits publishes a state without the table; the old state was the last
 * holder of the table's directory, so its chunks are released before the flush retains them:
 * `RangeError: A released group chunk was retained`, which fails the write path.
 *
 * Seen in CI (coverage shard 4/7) in extent-deltas.test.ts, where a drop commits while a flush
 * writes. This file forces the order: a directory of two chunks (the first is full, so any flush
 * shares it), a flush held at its first extent write, DROP TABLE, then the flush continues. The
 * flush must abandon at its publish (the table is gone) without failing the write path.
 */
import { describe, expect, it } from "vitest";
import { CHUNK_GROUPS } from "./groups.js";
import { PIN_HEADER_BYTES } from "../storage/root-pins.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  tableInput,
  watchGrants,
} from "./test-support.js";

describe("a flush of a table dropped while it writes", () => {
  it("does not retain chunks the drop released", async () => {
    let table = 0;
    let armed = false;
    let held!: () => void;
    let release!: () => void;
    const atWrite = new Promise<void>((resolve) => {
      held = resolve;
    });
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    // One-row memtables: every flush makes a group of its own.
    const harness = new Harness("strict", "memory", 1, undefined, { merges: { fanIn: 8 } });
    harness.onSpace = (space) => {
      const beginWrite = space.beginWrite.bind(space);
      space.beginWrite = (label, options) => {
        const writer = beginWrite(label, options);
        if (!armed || label !== `flush t${String(table)}`) return writer;
        armed = false;
        const write = writer.write.bind(writer);
        let first = true;
        writer.write = async (...args) => {
          if (first) {
            first = false;
            held();
            await gate;
          }
          return write(...args);
        };
        return writer;
      };
    };
    // The engine's resident-metadata grant: what the flush pins is given back.
    const metadata: { grant: { readonly allocated: number } | null } = { grant: null };
    watchGrants(harness.governor, (grant) => {
      if (grant.label === "write path metadata") metadata.grant = grant;
    });
    const path = await harness.open();
    try {
      await autocommit(path, async (s) => {
        table =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("dropped", [column("id", INT, false)]),
            })
          ).id ?? 0;
      });
      // More groups than one chunk holds: chunk 0 is full and a later flush shares it.
      const filled = CHUNK_GROUPS + 8;
      for (let id = 0; id < filled; id++)
        await autocommit(path, (s) => s.table(table).insert(batch(ints([id])), null));
      await path.quiesce();
      expect(path.state().tables.get(table)?.groups.chunks.length).toBeGreaterThan(1);

      // A flush held before it derives its directory.
      armed = true;
      const next = filled;
      await autocommit(path, (s) => s.table(table).insert(batch(ints([next])), null));
      await autocommit(path, (s) => s.table(table).insert(batch(ints([next + 1])), null));
      await atWrite;

      // The drop publishes a state without the table: nothing holds its directory now.
      await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: table }));
      expect(path.state().tables.has(table)).toBe(false);

      release();
      await path.quiesce();
      expect(path.failure).toBeNull();
      expect(path.maintenance.flushesAbandoned, "the flush abandoned its dropped table").toBe(1);
      await harness.space.audit();
      // Nothing of the dropped table stays charged, the flush's reference included: the grant
      // holds the root's bytes and the current state's pin only.
      const credit = PIN_HEADER_BYTES + 2 * "current state".length;
      expect(metadata.grant?.allocated).toBe(path.residentMetadataBytes + credit);
    } finally {
      release();
      await harness.close();
    }
  }, 60_000);
});
