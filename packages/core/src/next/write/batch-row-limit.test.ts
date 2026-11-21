/**
 * Write batches above 65,536 rows. Row positions and selections are 16-bit, so a longer batch
 * wrapped (row 65,536 read as row 0): an insert of 65,537 distinct keys failed with a false
 * duplicate key at statement end, and a delete of 65,537 locators with a misleading "cannot
 * affect a row twice". Such a batch is now refused with BatchTooLargeError (54000) before anything
 * is written, and the statement stays usable; batches up to the limit are unchanged. (Delete and
 * update also read their old rows at most one 2,048-row chunk at a time, BATCH_ROWS, and refuse
 * more with a RangeError, unchanged here.)
 */
import { expect, it } from "vitest";
import { BatchTooLargeError, MAX_WRITE_BATCH_ROWS } from "../access/modify.js";
import { BATCH_ROWS } from "../vector/vector.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  tableInput,
} from "./test-support.js";

const SIZES = [MAX_WRITE_BATCH_ROWS, MAX_WRITE_BATCH_ROWS + 1, 2 * MAX_WRITE_BATCH_ROWS + 1];

async function table(h: Harness, indexed = true) {
  const path = await h.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          // Indexed: a primary key and a second unique index, both checked on every insert.
          // Unindexed: the plainest delete. Update and delete read their old rows, at most one
          // 2,048-row chunk (BATCH_ROWS), and refuse more themselves (RangeError, nothing
          // written); a batch past 65,536 rows is refused before that, by the batch limit.
          table: tableInput(
            "b",
            [column("id", INT, false), column("u", INT, false)],
            indexed ? { unique: [{ name: "b_u", columns: [1] }] } : { primaryKey: null },
          ),
        })
      ).id ?? 0;
  });
  return { path, t };
}

const keys = (n: number, from = 0) => Array.from({ length: n }, (_, i) => from + i);

it.each(SIZES)(
  "inserts a batch of %i rows, or refuses it whole and keeps the statement",
  async (n) => {
    const h = new Harness("strict", "memory");
    const { path, t } = await table(h);
    try {
      const tx = await path.begin();
      const statement = tx.statement({
        rules: () => ({ check: () => null, generate: () => undefined }),
      });
      const ids = keys(n);
      let outcome: unknown;
      try {
        outcome = await statement.table(t).insert(batch(ints(ids), ints(ids)), null);
      } catch (error) {
        outcome = error;
      }
      if (n <= MAX_WRITE_BATCH_ROWS) expect(outcome).toBe(n);
      else {
        expect(outcome).toBeInstanceOf(BatchTooLargeError);
        expect(outcome).toMatchObject({ sqlState: "54000", rows: n, limit: MAX_WRITE_BATCH_ROWS });
        // Nothing was written, and the statement takes the same rows in batches within the limit.
        for (let from = 0; from < n; from += MAX_WRITE_BATCH_ROWS) {
          const part = keys(Math.min(MAX_WRITE_BATCH_ROWS, n - from), from);
          expect(await statement.table(t).insert(batch(ints(part), ints(part)), null)).toBe(
            part.length,
          );
        }
      }
      await statement.end();
      await (
        await tx.commit(null)
      ).durable;
      const view = path.pin();
      try {
        const rows = await rowsOf(view, t);
        expect(rows).toHaveLength(n);
        expect(new Set(rows.map((row) => row[0])).size).toBe(n);
      } finally {
        view.release();
      }
    } finally {
      await h.close();
    }
  },
  120_000,
);

it.each([BATCH_ROWS, ...SIZES.slice(1)])(
  "deletes a batch of %i locators, or refuses it and deletes nothing",
  async (n) => {
    const h = new Harness("strict", "memory");
    const { path, t } = await table(h, false);
    try {
      const rows = n + 1;
      for (let from = 0; from < rows; from += MAX_WRITE_BATCH_ROWS) {
        const part = keys(Math.min(MAX_WRITE_BATCH_ROWS, rows - from), from);
        await autocommit(path, (s) => s.table(t).insert(batch(ints(part), ints(part)), null));
      }
      // Row ids follow insertion order from 0 in a fresh table.
      let outcome: unknown;
      try {
        await autocommit(path, (s) => s.table(t).delete(batch(locators(keys(n), t)), null));
        outcome = "deleted";
      } catch (error) {
        outcome = error;
      }
      const view = path.pin();
      try {
        const left = await rowsOf(view, t);
        if (n <= MAX_WRITE_BATCH_ROWS) {
          expect(outcome).toBe("deleted");
          expect(left).toEqual([[n, n]]);
        } else {
          expect(outcome).toBeInstanceOf(BatchTooLargeError);
          expect(left).toHaveLength(rows);
        }
      } finally {
        view.release();
      }
    } finally {
      await h.close();
    }
  },
  120_000,
);
