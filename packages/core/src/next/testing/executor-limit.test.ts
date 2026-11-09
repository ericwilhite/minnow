/** Streaming LIMIT bounds scan demand; OFFSET and filters cannot discard a borrowed suffix. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-executor-limit-demand", [0xc901, 0xc902]))(
  "bounds streaming input demand over memory, stored and mixed scans (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    const reach = createReach("scan demand", [
      "memory",
      "stored",
      "mixed",
      "parameter",
      "constant",
      "offset",
      "nested",
      "filtered",
      "breaker",
      "nested-batch",
      "sparse-vectorized",
      "simple-range",
    ]);
    await pg.waitReady;
    const rows = Array.from({ length: 128 }, (_, v) => ({
      v,
      s: `row${String(v)}-${"x".repeat(32)}`,
    }));
    try {
      for (const name of ["m", "s", "x"]) {
        await core.execute(`CREATE TABLE ${name}(v INTEGER NOT NULL,s TEXT NOT NULL)`);
        await pg.exec(`CREATE TABLE ${name}(v INTEGER NOT NULL,s TEXT NOT NULL)`);
        await pg.query(
          `INSERT INTO ${name} VALUES ${rows.map((_, i) => `($${String(i * 2 + 1)},$${String(i * 2 + 2)})`).join(",")}`,
          rows.flatMap((row) => [row.v, row.s]),
        );
      }
      await core.writeBatch("insertBatch", "s", rows);
      await core.writeBatch("insertBatch", "x", rows.slice(0, 64));
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await core.writeBatch("insertBatch", "m", rows);
      await core.writeBatch("insertBatch", "x", rows.slice(64));
      // Credit actual physical states, rather than merely labelling three identical inputs.
      for (const [name, kind] of [
        ["m", "memory"],
        ["s", "stored"],
        ["x", "mixed"],
      ] as const) {
        const tableDef = core.harness.path.catalog.tableByName(name);
        if (!tableDef) throw new Error("missing fixture table");
        const pin = core.harness.path.pin(),
          grant = await core.harness.governor.admit({
            label: "fixture/state",
            minimumBytes: 1 << 20,
            spillable: true,
          });
        const table = pin.table(tableDef.id, { budget: grant });
        try {
          const kinds = new Set(
            table.segments.filter((segment) => segment.rowCount > 0).map((segment) => segment.kind),
          );
          expect(kinds.has("memory")).toBe(kind !== "stored");
          expect(kinds.has("group")).toBe(kind !== "memory");
        } finally {
          pin.release();
          grant.close();
        }
      }
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 2 }),
          fc.integer({ min: 1, max: 127 }),
          fc.integer({ min: 1, max: 12 }),
          fc.integer({ min: 0, max: 8 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (state, mod, limit, offset, parameter, nested, ordered, simpleRange) => {
            reach.beginHistory();
            const table = ["m", "s", "x"][state];
            if (!table) throw new Error("missing state");
            const innerLimit = limit + 5,
              innerOffset = offset + 2;
            const innerBounds = parameter
              ? "LIMIT $1 OFFSET $2"
              : `LIMIT ${String(nested ? innerLimit : limit)} OFFSET ${String(nested ? innerOffset : offset)}`;
            const threshold = mod % 64;
            const predicate = simpleRange ? `v >= ${String(threshold)}` : `v % ${String(mod)} = 0`;
            const inner = `SELECT v,s FROM ${table} WHERE ${predicate}${ordered ? " ORDER BY v" : ""} ${innerBounds}`;
            const sql = nested
              ? `SELECT v,s FROM (${inner}) q${ordered ? " ORDER BY v" : ""} ${parameter ? "LIMIT $3 OFFSET $4" : `LIMIT ${String(limit)} OFFSET ${String(offset)}`}`
              : inner;
            const args = parameter
              ? nested
                ? [innerLimit, innerOffset, limit, offset]
                : [limit, offset]
              : [];
            const expected = (await pg.query(sql, args)).rows;
            const result = await core.execute(sql, args);
            if (!("batches" in result)) throw new Error("expected SELECT");
            const actual = oracleRows(result);
            // Unordered LIMIT may choose any qualifying subset. Count and membership are invariant;
            // ordered cases additionally compare the actual rows, with a total ordering in this fixture.
            expect(actual).toHaveLength(expected.length);
            expect(new Set(actual.map((row) => row.v)).size).toBe(actual.length);
            for (const row of actual) {
              const v = Number(row.v);
              expect(simpleRange ? v >= threshold : v % mod === 0).toBe(true);
              expect(row.s).toBe(rows[v]?.s);
            }
            if (ordered) expect(actual).toEqual(expected);
            const scans = core.lastProfile?.operators.filter((op) => op.op === "scan") ?? [];
            expect(scans).toHaveLength(1);
            if (!ordered) {
              const budget = nested
                ? innerOffset + Math.min(innerLimit, offset + limit)
                : offset + limit;
              expect(scans[0]?.rowsOut).toBeLessThanOrEqual(budget);
              if (!simpleRange && mod >= 64 && state !== 2) {
                expect(scans[0]?.batchesOut).toBe(1);
                reach.reach("sparse-vectorized");
              }
              if (mod === 1) {
                expect(scans[0]?.batchesOut).toBe(1);
                if (nested) reach.reach("nested-batch");
              }
            } else {
              expect(scans[0]?.rowsOut).toBe(simpleRange ? 128 - threshold : Math.ceil(128 / mod));
              expect(scans[0]?.batchesOut).toBeLessThanOrEqual(state === 2 ? 2 : 1);
              reach.reach("breaker");
            }
            reach.reach((["memory", "stored", "mixed"] as const)[state] ?? "memory");
            reach.reach(parameter ? "parameter" : "constant");
            if (offset > 0) reach.reach("offset");
            if (nested) reach.reach("nested");
            if (mod > 1) reach.reach("filtered");
            if (simpleRange) reach.reach("simple-range");
            expect(core.spill.bytesInUse).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [0, 7, 2, 1, false, false, false, false],
            [1, 3, 7, 4, true, false, false, false],
            [2, 5, 5, 3, false, true, false, false],
            [0, 1, 3, 1, true, true, true, false],
            [2, 1, 12, 8, false, true, false, false],
            [0, 127, 1, 1, false, false, false, false],
            [1, 7, 4, 3, false, false, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
  120000,
);
