/**
 * Foreign-key actions against PostgreSQL (PGlite), generated (review B3-p2b-1 #1 and #2):
 * - **Follow.** Parent keys shifted by one UPDATE so that old and new key sets overlap (a key one
 *   row leaves, another takes): children must follow their parent's identity (ON UPDATE CASCADE).
 *   Only statements PostgreSQL accepts are compared (it checks uniqueness row by row, so the rows
 *   are inserted in the order that makes the shift valid there).
 * - **Two paths.** A child with two foreign keys to one parent, each with its own ON DELETE action
 *   (every pair of RESTRICT, CASCADE, SET NULL), parents deleted in one statement, with 1-3-row
 *   memtables so an action freezes a memtable between two others.
 * Rows (or the refusal) must equal PostgreSQL's.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, describe, expect, it } from "vitest";
import type { TableInput } from "../access/catalog.js";
import type { Statement } from "../access/modify.js";
import { ForeignKeyViolationError } from "../access/modify.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
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

const FREE = { allocate: () => true, free: () => undefined };
type Action = "restrict" | "cascade" | "setNull";
const ACTIONS: readonly Action[] = ["restrict", "cascade", "setNull"];
const SQL_ACTION: Record<Action, string> = {
  restrict: "RESTRICT",
  cascade: "CASCADE",
  setNull: "SET NULL",
};

const REACH = [
  "follow-overlap",
  "follow-swap-chain",
  "follow-shared-column",
  "follow-two-columns",
  "two-paths-mixed",
  "two-paths-refused",
  "two-paths-freeze",
] as const;
const reach = createReach("next-write-fk-generated", REACH);

/** One PostgreSQL per test file (each round drops its tables): starting PGlite is the cost. */
let instance: PGlite | null = null;
async function postgres(): Promise<PGlite> {
  instance ??= await PGlite.create();
  return instance;
}

afterAll(async () => {
  await instance?.close();
  reach.assertReached();
});

async function create(
  path: WritePathImpl,
  name: string,
  input: TableInput["columns"],
  extra: Partial<TableInput> = {},
): Promise<number> {
  let id = 0;
  await autocommit(path, async (s) => {
    id =
      (await s.changeCatalog({ kind: "createTable", table: tableInput(name, input, extra) })).id ??
      0;
  });
  return id;
}

function fk(parent: number, onDelete: Action, columns: number[], name: string) {
  return {
    name,
    columns,
    parentTableId: parent,
    parentIndexId: "primary" as const,
    onDelete,
    enforced: true,
  };
}

/** Locators of `keys` in the statement's own view. */
async function locate(statement: Statement, table: number, keys: number[]): Promise<number[]> {
  const encoded = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(keys), direction: "asc", nulls: "first" }],
    { length: keys.length, selection: null },
    encoded,
  );
  const out = {
    probes: new Int32Array(keys.length),
    locators: new Float64Array(keys.length),
    count: 0,
  };
  const index = statement.view.index(table, statement.view.catalog.table(table)?.primaryKey ?? 0);
  await index.probe(encoded, out);
  encoded.close();
  return Array.from(out.locators.subarray(0, out.count));
}

async function sorted(path: WritePathImpl, table: number) {
  const view = path.pin();
  const rows = await rowsOf(view, table);
  view.release();
  return rows.sort((a, b) => Number(a[0]) - Number(b[0]));
}

const pgRows = async (pg: PGlite, sql: string) =>
  (await pg.query<Record<string, number | null>>(sql)).rows.map((row) => Object.values(row));

describe("foreign-key actions vs PostgreSQL (generated)", () => {
  const SEEDS = seedsFor("next-write-fk-generated", [501, 502, 503, 504, 505, 506, 507, 508]);
  for (const seed of SEEDS)
    it(`children follow their parent's identity across overlapping keys (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed);
      const int = (n: number) => Math.floor(random() * n);
      reach.beginHistory();
      let compared = 0;
      for (let round = 0; round < 12; round++) {
        const ids = [...new Set(Array.from({ length: 3 + int(6) }, () => 1 + int(12)))];
        const shift = (random() < 0.5 ? 1 : -1) * (1 + int(2));
        const moved = ids.filter(() => random() < 0.7);
        if (moved.length === 0) continue;
        // PostgreSQL updates rows in insertion order and checks each at once: insert so that every
        // moved row's target is already vacated (descending for an upward shift).
        const order = [...ids].sort((a, b) => (shift > 0 ? b - a : a - b));
        // The child's shape (review B3-p2b-2 B1): one foreign key; two sharing one column; two on
        // two columns, each following its own parent.
        const shape = (["single", "shared", "two"] as const)[int(3)] ?? "single";
        const children = ids.flatMap((id) => Array.from({ length: int(3) }, () => id));
        const second = children.map(() => need(ids[int(ids.length)]));
        const pg = await postgres();
        try {
          await pg.exec(
            "CREATE TABLE p(id int PRIMARY KEY);" +
              (shape === "single"
                ? "CREATE TABLE c(id int PRIMARY KEY, ref int REFERENCES p ON UPDATE CASCADE ON DELETE CASCADE);"
                : shape === "shared"
                  ? "CREATE TABLE c(id int PRIMARY KEY, ref int REFERENCES p ON UPDATE CASCADE ON DELETE CASCADE, FOREIGN KEY (ref) REFERENCES p ON UPDATE CASCADE ON DELETE CASCADE);"
                  : "CREATE TABLE c(id int PRIMARY KEY, ref int REFERENCES p ON UPDATE CASCADE ON DELETE CASCADE, ref2 int REFERENCES p ON UPDATE CASCADE ON DELETE CASCADE);") +
              `INSERT INTO p VALUES ${order.map((id) => `(${String(id)})`).join(",")};` +
              (children.length > 0
                ? `INSERT INTO c VALUES ${children
                    .map(
                      (ref, k) =>
                        `(${String(k + 1)}, ${String(ref)}${shape === "two" ? `, ${String(second[k])}` : ""})`,
                    )
                    .join(",")};`
                : ""),
          );
          try {
            await pg.exec(
              `UPDATE p SET id = id + (${String(shift)}) WHERE id IN (${moved.join(",")})`,
            );
          } catch {
            continue; // a statement PostgreSQL refuses is not compared
          }
          const expected = await pgRows(
            pg,
            shape === "two"
              ? "SELECT id, ref, ref2 FROM c ORDER BY id"
              : "SELECT id, ref FROM c ORDER BY id",
          );
          const harness = new Harness("strict", "memory", 1 + int(3));
          const path = await harness.open();
          try {
            const p = await create(path, "p", [column("id", INT, false)]);
            const c = await create(
              path,
              "c",
              shape === "two"
                ? [column("id", INT, false), column("ref", INT), column("ref2", INT)]
                : [column("id", INT, false), column("ref", INT)],
              {
                foreignKeys:
                  shape === "single"
                    ? [fk(p, "cascade", [1], "c_ref")]
                    : shape === "shared"
                      ? [fk(p, "cascade", [1], "c_ref"), fk(p, "cascade", [1], "c_ref_again")]
                      : [fk(p, "cascade", [1], "c_ref"), fk(p, "cascade", [2], "c_ref2")],
              },
            );
            await autocommit(path, (s) => s.table(p).insert(batch(ints(order)), null));
            if (children.length > 0)
              await autocommit(path, (s) =>
                s
                  .table(c)
                  .insert(
                    shape === "two"
                      ? batch(ints(children.map((_, k) => k + 1)), ints(children), ints(second))
                      : batch(ints(children.map((_, k) => k + 1)), ints(children)),
                    null,
                  ),
              );
            const keyColumn = path.catalog.table(p)?.columns[0]?.id ?? 0;
            await autocommit(path, async (s) =>
              s
                .table(p, { assigned: [keyColumn] })
                .update(
                  batch(
                    locators(await locate(s, p, moved), p),
                    ints(moved.map((id) => id + shift)),
                  ),
                  null,
                ),
            );
            expect(await sorted(path, c), `seed ${String(seed)} round ${String(round)}`).toEqual(
              expected,
            );
            compared += 1;
            const after = new Set(moved.map((id) => id + shift));
            if (moved.some((id) => after.has(id))) reach.reach("follow-overlap");
            if (moved.filter((id) => after.has(id)).length >= 2) reach.reach("follow-swap-chain");
            if (shape === "shared" && moved.some((id) => after.has(id)))
              reach.reach("follow-shared-column");
            if (shape === "two" && moved.some((id) => after.has(id)))
              reach.reach("follow-two-columns");
          } finally {
            await harness.close();
          }
        } finally {
          await pg.exec("DROP TABLE IF EXISTS c; DROP TABLE IF EXISTS p;");
        }
      }
      expect(compared).toBeGreaterThan(0);
    }, 300_000);

  for (const seed of SEEDS)
    it(`two foreign-key paths act like PostgreSQL across memtable freezes (seed ${String(seed)})`, async () => {
      const random = mulberry32(seed + 1000);
      const int = (n: number) => Math.floor(random() * n);
      reach.beginHistory();
      for (let round = 0; round < 6; round++) {
        const first = ACTIONS[int(3)] ?? "cascade";
        const second = ACTIONS[int(3)] ?? "setNull";
        const parents = [1, 2, 3, 4];
        const children = Array.from({ length: 2 + int(5) }, (_, k) => [
          k + 10,
          random() < 0.8 ? (parents[int(4)] ?? 1) : null,
          random() < 0.8 ? (parents[int(4)] ?? 1) : null,
        ]);
        const doomed = parents.filter(() => random() < 0.5);
        if (doomed.length === 0) continue;
        const sql = (value: number | null | undefined) => (value == null ? "NULL" : String(value));
        const pg = await postgres();
        let refused = false;
        let expected: unknown[][];
        try {
          await pg.exec(
            "CREATE TABLE p(id int PRIMARY KEY);" +
              `CREATE TABLE c(id int PRIMARY KEY, r1 int REFERENCES p ON DELETE ${SQL_ACTION[first]}, r2 int REFERENCES p ON DELETE ${SQL_ACTION[second]});` +
              `INSERT INTO p VALUES ${parents.map((id) => `(${String(id)})`).join(",")};` +
              `INSERT INTO c VALUES ${children.map((row) => `(${row.map(sql).join(",")})`).join(",")};`,
          );
          try {
            await pg.exec(`DELETE FROM p WHERE id IN (${doomed.join(",")})`);
          } catch (error) {
            // 23503 (NO ACTION) or 23001 (RESTRICT): both are refusals by a foreign key.
            expect(["23503", "23001"]).toContain((error as { code?: string }).code);
            refused = true;
          }
          expected = await pgRows(pg, "SELECT id, r1, r2 FROM c ORDER BY id");
        } finally {
          await pg.exec("DROP TABLE IF EXISTS c; DROP TABLE IF EXISTS p;");
        }
        const rows = 1 + int(3);
        const harness = new Harness("strict", "memory", rows);
        const path = await harness.open();
        try {
          const p = await create(path, "p", [column("id", INT, false)]);
          const c = await create(
            path,
            "c",
            [column("id", INT, false), column("r1", INT), column("r2", INT)],
            { foreignKeys: [fk(p, first, [1], "c_r1"), fk(p, second, [2], "c_r2")] },
          );
          await autocommit(path, (s) => s.table(p).insert(batch(ints(parents)), null));
          await autocommit(path, (s) =>
            s
              .table(c)
              .insert(
                batch(
                  ints(children.map((row) => row[0] ?? 0)),
                  ints(children.map((row) => row[1] ?? null)),
                  ints(children.map((row) => row[2] ?? null)),
                ),
                null,
              ),
          );
          let actual: "ok" | "refused" = "ok";
          try {
            await autocommit(path, async (s) =>
              s.table(p).delete(batch(locators(await locate(s, p, doomed), p)), null),
            );
          } catch (error) {
            expect(error).toBeInstanceOf(ForeignKeyViolationError);
            actual = "refused";
          }
          const label = `seed ${String(seed)} round ${String(round)}: ${first}/${second}, delete ${doomed.join(",")}`;
          expect(actual, label).toBe(refused ? "refused" : "ok");
          expect(await sorted(path, c), label).toEqual(expected);
          if (first !== second) reach.reach("two-paths-mixed");
          if (refused) reach.reach("two-paths-refused");
          if (!refused && rows === 1 && first !== "restrict" && second !== "restrict")
            reach.reach("two-paths-freeze");
        } finally {
          await harness.close();
        }
      }
    }, 300_000);
});
