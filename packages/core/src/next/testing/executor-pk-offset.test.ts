/** PK OFFSET counts the pinned index's visible rows before fetching any payload. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { ViewIndex } from "../write/view.js";
import { Governor, type Grant } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const kinds = [
  "memory",
  "stored",
  "filter",
  "expression",
  "other-key",
  "plain",
  "composite",
  "mixed",
] as const;
const offsets = [0, 1, 100, 257, 10000, null, -1, 9223372036854775808n] as const;
const limits = [0, 7, null] as const;

it.each(seedsFor("next-executor-pk-offset", [0xe141, 0xe142]))(
  "routes exact PK order and retains generic semantics otherwise (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    const reach = createReach("admitted PK OFFSET", [
      "ascending",
      "descending",
      "memory",
      "stored",
      "composite",
      "offset only",
      "NULL offset",
      "past end",
      "exact end",
      "negative",
      "overflow",
      "zero validates offset",
      "zero unopened",
      "constant",
      "parameter",
      "filter fallback",
      "expression fallback",
      "other-key fallback",
      "plain fallback",
      "mixed fallback",
      "admitted",
      "skip once",
      "survivors only",
      "PG exact",
      "released",
    ]);
    let governor = new Governor(64 << 20);
    const prepare = core.executor.prepare.bind(core.executor);
    core.executor.prepare = (plan) => {
      const prepared = prepare(plan);
      return { ...prepared, execute: (request) => prepared.execute({ ...request, governor }) };
    };
    const stats = {
      opens: 0,
      closes: 0,
      rows: 0,
      nexts: 0,
      skips: [] as number[],
      grants: [] as Grant[],
    };
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with .call(this).
    const nativePlan = ViewIndex.prototype.planRange;
    const plans = vi.spyOn(ViewIndex.prototype, "planRange").mockImplementation(function (
      this: ViewIndex,
      ...args
    ) {
      const plan = nativePlan.call(this, ...args);
      return {
        minimumBytes: plan.minimumBytes,
        open(options) {
          stats.opens++;
          stats.grants.push(options.budget);
          const cursor = plan.open(options);
          return {
            skip(count) {
              stats.skips.push(count);
              return cursor.skip(count);
            },
            next(out) {
              stats.nexts++;
              const work = cursor.next(out);
              const record = (count: number) => {
                stats.rows += count;
                return count;
              };
              return work instanceof Promise ? work.then(record) : record(work);
            },
            close() {
              stats.closes++;
              cursor.close();
              expect(options.budget.held).toBeGreaterThanOrEqual(plan.minimumBytes);
            },
          };
        },
      };
    });
    await pg.waitReady;
    try {
      const rows = Array.from({ length: 257 }, (_, i) => {
        const id = (i * 73) % 257;
        return { id, a: id % 7, b: id, p: id % 13 === 0 ? null : `v${String(id % 5)}` };
      });
      for (const name of ["s", "plain", "c", "m"]) {
        const primary =
          name === "plain" ? "" : name === "c" ? ", PRIMARY KEY(a,b)" : ", PRIMARY KEY(id)";
        const ddl = `CREATE TABLE ${name}(id INTEGER,a INTEGER,b INTEGER,p TEXT${primary})`;
        await core.execute(ddl);
        await pg.exec(ddl);
        await pg.exec(
          `INSERT INTO ${name} SELECT i,i%7,i,CASE WHEN i%13=0 THEN NULL ELSE 'v'||(i%5)::text END FROM generate_series(0,256) i`,
        );
        if (name !== "m") await core.writeBatch("insertBatch", name, rows);
      }
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await core.writeBatch("insertBatch", "m", rows);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...kinds),
          fc.integer({ min: 0, max: offsets.length - 1 }),
          fc.integer({ min: 0, max: limits.length - 1 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (kind, oi, li, omitLimit, descending, parameters) => {
            reach.beginHistory();
            governor = new Governor(64 << 20);
            Object.assign(stats, { opens: 0, closes: 0, rows: 0, nexts: 0, skips: [], grants: [] });
            const offset = offsets[oi],
              limit = limits[li];
            if (offset === undefined || limit === undefined) throw new Error("Bound dimension");
            const composite = kind === "composite" || kind === "mixed";
            const table =
              kind === "plain" ? "plain" : composite ? "c" : kind === "memory" ? "m" : "s";
            const direction = descending ? "DESC" : "ASC";
            const order = composite
              ? kind === "mixed"
                ? "a ASC,b DESC"
                : `a ${direction},b ${direction}`
              : kind === "expression"
                ? `-id ${direction}`
                : kind === "other-key"
                  ? `p ${direction},id ${direction}`
                  : `id ${direction}`;
            const bound =
              parameters ||
              limit === null ||
              offset === null ||
              typeof offset === "bigint" ||
              offset < 0;
            const sql = `SELECT p AS value,id FROM ${table}${kind === "filter" ? " WHERE id%2=0" : ""} ORDER BY ${order}${omitLimit ? "" : ` LIMIT ${bound ? "$1" : String(limit)}`} OFFSET ${bound ? (omitLimit ? "$1" : "$2") : String(offset)}`;
            const args = bound ? (omitLimit ? [offset] : [limit, offset]) : [];
            let expected: unknown, code: unknown;
            try {
              expected = (await pg.query(sql, args)).rows;
            } catch (error) {
              code = (error as { code: unknown }).code;
            }
            if (code !== undefined) {
              await expect(core.execute(sql, args)).rejects.toMatchObject({ sqlState: code });
              expect(stats.opens).toBe(0);
              reach.reach(code === "22003" ? "overflow" : "negative");
              if (!omitLimit && limit === 0 && offset === -1) reach.reach("zero validates offset");
            } else {
              const result = await core.execute(sql, args);
              if (!("batches" in result)) throw new Error("Query result expected");
              const actual = oracleRows(result);
              expect(actual).toEqual(expected);
              const eligible = kind === "memory" || kind === "stored" || kind === "composite";
              if (!omitLimit && limit === 0) {
                expect(stats.opens).toBe(0);
                reach.reach("zero unopened");
              } else if (eligible) {
                expect(stats.opens).toBe(1);
                expect(stats.closes).toBe(1);
                expect(stats.rows).toBe(actual.length);
                expect(stats.skips).toEqual(offset === null || offset === 0 ? [] : [offset]);
                if (typeof offset === "number" && offset > rows.length) {
                  expect(stats.nexts).toBe(0);
                  reach.reach("past end");
                } else if (offset === rows.length) {
                  // A full skip count carries no EOF flag. One empty next is
                  // permitted by IndexCursor; no locator may be materialized.
                  expect(stats.nexts).toBeLessThanOrEqual(1);
                  reach.reach("exact end");
                }
                if (stats.skips.length) reach.reach("skip once");
                if (omitLimit) reach.reach("offset only");
                if (offset === null) reach.reach("NULL offset");
                reach.reach(kind);
                reach.reach("admitted");
                reach.reach("survivors only");
              } else {
                expect(stats.opens).toBe(0);
                reach.reach(`${kind} fallback`);
              }
              reach.reach("PG exact");
            }
            reach.reach(descending ? "descending" : "ascending");
            reach.reach(bound ? "parameter" : "constant");
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            for (const grant of stats.grants) {
              expect(grant.allocated).toBe(0);
              expect(grant.held).toBe(0);
            }
            reach.reach("released");
          },
        ),
        {
          seed,
          numRuns: 31,
          examples: [
            ["stored", 2, 1, false, false, false],
            ["memory", 1, 1, false, true, true],
            ["stored", 2, 1, true, false, true],
            ["composite", 1, 1, true, true, false],
            ["mixed", 1, 1, false, false, false],
            ["filter", 1, 1, false, true, false],
            ["expression", 1, 1, false, false, false],
            ["plain", 1, 1, false, false, false],
            ["other-key", 1, 1, false, false, false],
            ["stored", 5, 1, true, false, true],
            ["stored", 6, 1, true, false, true],
            ["stored", 7, 1, true, false, true],
            ["stored", 6, 0, false, true, true],
            ["memory", 5, 0, false, false, true],
            ["stored", 4, 1, false, true, true],
            ["stored", 1, 2, false, false, true],
            ["memory", 0, 1, false, false, false],
            ["stored", 1, 0, false, false, false],
            ["memory", 3, 1, false, false, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      plans.mockRestore();
      await core.close();
      await pg.close();
    }
  },
  120000,
);
