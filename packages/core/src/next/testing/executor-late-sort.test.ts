/** SQL -> real write/scan -> narrow sort -> locator fetch, against independent engines. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
import { OutputArena } from "../exec/kernels/vectors.js";
import { packLocator } from "../vector/locator.js";

const fields = [
  "id",
  "v",
  ...Array.from({ length: 22 }, (_, k) => `p${String(k).padStart(2, "0")}`),
];
const payloadTypes = ["INTEGER", "TEXT", "NUMERIC(12,2)", "DOUBLE PRECISION"];
const schema = `id INTEGER PRIMARY KEY,v DOUBLE PRECISION,${fields
  .slice(2)
  .map((name, k) => {
    const type = payloadTypes[k % 4];
    if (type === undefined) throw new Error("fixture payload type");
    return `${name} ${type}`;
  })
  .join(",")}`;
const rows = Array.from({ length: 2305 }, (_, k) => {
  const id = (k * 7919) % 2305;
  const row: Record<string, number | string | null> = {
    id,
    v: id % 17 === 0 ? null : (id % 37) - 18,
  };
  fields.slice(2).forEach((name, j) => {
    row[name] =
      (id + j) % 23 === 0
        ? null
        : j % 4 === 0
          ? id * 7 - j * 37
          : j % 4 === 1
            ? `p${String(j % 8 === 1 ? id % 5 : id)}-α-${"q".repeat(j % 8 === 1 ? 3 : id % 31)}`
            : j % 4 === 2
              ? `${String(id * 19 + j)}.${String(id % 100).padStart(2, "0")}`
              : id * 0.75 + j * 0.125;
  });
  return row;
});

it.each(seedsFor("next-executor-late-sort", [0xd101, 0xd102]))(
  "materializes only wide-sort winners in their SQL order (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite(),
      current = await currentEngine("memory");
    const reach = createReach("late-sort", [
      "memory",
      "stored",
      "mixed",
      "deleted-stored",
      "profile-ids",
      "late",
      "control",
      "zero",
      "offset",
      "descending",
      "ascending",
      "nulls-first",
      "nulls-last",
      "multiple-input-batches",
      "payload-pages",
      "dictionary-payload",
      "empty-table",
      "empty-winners",
    ]);
    await pg.waitReady;
    try {
      for (const name of ["m", "s", "x", "e", "d"]) {
        const sql = `CREATE TABLE ${name}(${schema})`;
        await core.execute(sql);
        await pg.exec(sql);
        await current.run({ kind: "sql", sql });
        if (name === "e") continue;
        await current.run({ kind: "insertBatch", table: name, rows });
        for (let from = 0; from < rows.length; from += 128) {
          const part = rows.slice(from, from + 128);
          await pg.query(
            `INSERT INTO ${name} VALUES ${part.map((_, k) => `(${fields.map((_, c) => `$${String(k * fields.length + c + 1)}`).join(",")})`).join(",")}`,
            part.flatMap((row) => fields.map((name) => row[name])),
          );
        }
      }
      await core.writeBatch("insertBatch", "s", rows);
      await core.writeBatch("insertBatch", "d", rows);
      await core.writeBatch("insertBatch", "x", rows.slice(0, 1152));
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await core.writeBatch("insertBatch", "m", rows);
      await core.writeBatch("insertBatch", "x", rows.slice(1152));
      // Use the ordinary writer's physical locators; the SQL oracles delete by the same ids.
      const deleted = core.harness.path.catalog.tableByName("d");
      if (!deleted) throw new Error("deleted fixture table");
      const deletionGrant = await core.harness.governor.admit({
        label: "late-sort/delete-fixture",
        minimumBytes: 1 << 20,
        spillable: true,
      });
      const deletionArena = new OutputArena(deletionGrant, 2048);
      try {
        await core.write(async (statement) => {
          const table = await statement.view.table(deleted.id, { budget: deletionGrant });
          const vector = deletionArena.flat({ kind: "locator", tableId: deleted.id }).vector;
          if (vector.data.layout !== "f64") throw new Error("locator layout");
          let ordinal = 0,
            count = 0;
          for (const segment of table.segments)
            for (let position = 0; position < segment.rowCount; position++, ordinal++) {
              const id = rows[ordinal]?.id;
              if (typeof id === "number" && id % 13 === 0)
                vector.data.values[count++] = packLocator(segment.index, position);
            }
          vector.length = count;
          expect(
            await statement
              .table(deleted.id)
              .delete({ length: count, columns: [vector], selection: null }, null),
          ).toBe(count);
        });
      } finally {
        deletionArena.close();
        deletionGrant.close();
      }
      await pg.exec("DELETE FROM d WHERE id % 13 = 0");
      await current.run({ kind: "sql", sql: "DELETE FROM d WHERE id % 13 = 0" });
      // Observe actual segment states before crediting their generator labels.
      for (const [name, kind] of [
        ["m", "memory"],
        ["s", "stored"],
        ["x", "mixed"],
        ["d", "stored"],
      ] as const) {
        const def = core.harness.path.catalog.tableByName(name);
        if (!def) throw new Error("fixture table");
        const pin = core.harness.path.pin(),
          grant = await core.harness.governor.admit({
            label: "late-sort/fixture",
            minimumBytes: 1 << 20,
            spillable: true,
          });
        try {
          const table = pin.table(def.id, { budget: grant });
          const kinds = new Set(table.segments.filter((s) => s.rowCount > 0).map((s) => s.kind));
          expect(kinds.has("memory")).toBe(kind !== "stored");
          expect(kinds.has("group")).toBe(kind !== "memory");
          if (name === "d") expect(table.liveRowCount).toBeLessThan(rows.length);
          if (kind === "stored") {
            const column = table.columns.find((field) => field.name === "p01");
            if (!column) throw new Error("dictionary fixture column");
            const groups = table.segments.filter((segment) => segment.kind === "group");
            expect(groups.length).toBeGreaterThan(0);
            for (const group of groups) {
              await group.root();
              expect(group.chunk(column.id)?.statistics.dictionarySize).toBeGreaterThan(0);
            }
          }
        } finally {
          pin.release();
          grant.close();
        }
      }
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 4 }),
          fc.integer({ min: 0, max: 64 }),
          fc.integer({ min: 0, max: 2500 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (state, limit, offset, parameter, descending, nullsFirst) => {
            reach.beginHistory();
            const table = ["m", "s", "x", "e", "d"][state];
            if (!table) throw new Error("state");
            const sql = `SELECT * FROM ${table} ORDER BY v ${descending ? "DESC" : "ASC"} NULLS ${nullsFirst ? "FIRST" : "LAST"},id ${parameter ? "LIMIT $1 OFFSET $2" : `LIMIT ${String(limit)} OFFSET ${String(offset)}`}`;
            const args = parameter ? [limit, offset] : [];
            const expected = (await pg.query<Record<string, unknown>>(sql, args)).rows;
            const legacy = await current.run({ kind: "sql", sql, params: args });
            expect(legacy.kind).toBe("rows");
            expect(legacy.rows).toEqual(expected);
            const result = await core.execute(sql, args);
            if (!("batches" in result)) throw new Error("SELECT result");
            expect(oracleRows(result)).toEqual(expected);
            const profile = core.lastProfile;
            if (!profile) throw new Error("query profile");
            const plan = core.prepare(sql).plan;
            const ids = new Set<number>();
            const visit = (rel: typeof plan.root): void => {
              ids.add(rel.id);
              if ("input" in rel) visit(rel.input);
            };
            visit(plan.root);
            for (const op of profile.operators) expect(ids.has(op.relId)).toBe(true);
            if (limit > 0 && !parameter && expected.length > 0) reach.reach("profile-ids");
            const sorts = profile.operators.filter((op) => op.op === "sort"),
              scans = profile.operators.filter((op) => op.op === "scan");
            if (limit > 0 && !parameter) {
              expect(sorts).toHaveLength(1);
              if (expected.length > 0) reach.reach("late");
              else reach.reach("empty-winners");
              expect(sorts.every((op) => op.rowsOut === expected.length)).toBe(true);
              expect(profile.pipelines).toBe(expected.length > 0 ? 2 : 1);
              const project = profile.operators.find((op) => op.op === "project");
              if ((state === 1 || state === 2 || state === 4) && expected.length > 0) {
                expect(project?.pagesRead).toBeGreaterThan(0);
                reach.reach("payload-pages");
              }
            } else {
              expect(sorts).toHaveLength(limit === 0 ? 0 : 1);
              reach.reach(limit === 0 ? "zero" : "control");
            }
            if (limit > 0 && scans.some((op) => op.batchesOut > 1))
              reach.reach("multiple-input-batches");
            if (state === 3) {
              expect(scans.every((op) => op.rowsOut === 0)).toBe(true);
              reach.reach("empty-table");
            } else if (state === 4) reach.reach("deleted-stored");
            else reach.reach((["memory", "stored", "mixed"] as const)[state] ?? "memory");
            reach.reach(descending ? "descending" : "ascending");
            reach.reach(nullsFirst ? "nulls-first" : "nulls-last");
            if (offset > 0) reach.reach("offset");
            // Low-cardinality payload is checked against both oracles, including NULL slots.
            if (
              state === 1 &&
              limit > 0 &&
              !parameter &&
              expected.some((row) => typeof row.p01 === "string")
            )
              reach.reach("dictionary-payload");
            expect(core.spill.bytesInUse).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [0, 64, 3, false, true, false],
            [1, 7, 1, false, false, true],
            [2, 31, 5, false, true, true],
            [0, 8, 0, true, false, false],
            [1, 0, 3, false, true, false],
            [1, 7, 3000, false, true, false],
            [3, 8, 0, false, true, false],
            [4, 31, 3, false, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await current.close();
      await pg.close();
    }
  },
  120000,
);
