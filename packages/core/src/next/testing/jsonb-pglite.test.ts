/** JSONB text output, not JSON.parse's number/object approximation, is the PG authority. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_SQL_REACH } from "../../testing/reach.js";
import { ExceptOperator } from "../exec/except.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
let rowId = 0;
let spillWritten = 0;
let restoreSpill: (() => void) | undefined;
let rejectDescendantCheckpoint = false;
// eslint-disable-next-line @typescript-eslint/unbound-method -- called with its actual receiver below
const checkpoint = ExceptOperator.prototype.checkpoint;
const checkpointSpy = vi.spyOn(ExceptOperator.prototype, "checkpoint").mockImplementation(function (
  this: ExceptOperator,
) {
  if (rejectDescendantCheckpoint)
    throw new Error("A completed descendant cannot checkpoint its pending finish");
  return checkpoint.call(this);
});
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
  const create = core.spill.create.bind(core.spill);
  const spillSpy = vi.spyOn(core.spill, "create").mockImplementation(async (id) => {
    const file = await create(id),
      append = file.append.bind(file);
    file.append = async (page) => {
      const ref = await append(page);
      spillWritten += page.length;
      return ref;
    };
    return file;
  });
  restoreSpill = () => spillSpy.mockRestore();
  await core.execute("CREATE TABLE jsonb_documents(id INTEGER PRIMARY KEY,doc TEXT)");
  await pg.exec("CREATE TABLE jsonb_documents(id INTEGER PRIMARY KEY,doc TEXT)");
}, 60000);
afterAll(async () => {
  checkpointSpy.mockRestore();
  restoreSpill?.();
  await core.close();
  await pg.close();
});
const quote = (s: string): string => "'" + s.replace(/'/g, "''") + "'";
it.each(seedsFor("next-jsonb-sql", [0xb680, 0xb681]))(
  "matches PG JSONB canonical text and extraction (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB canonical extraction", JSONB_SQL_REACH);
    const samples = [
      '{"zz":1.00,"b":2.00,"aa":3.000,"b":4.00}',
      '{"n":9007199254740993.0100,"z":-0.00,"n":9007199254740994.0200}',
      '{"汉":"\\u0061/\\n","a":"\\uD83D\\uDE03","escaped\\u006bey":null}',
      '[1e3,1.00e-3,1e-20,-0.000,true,null,{"zz":1,"a":2}]',
      '{"n":1e1000}',
      "null",
      "true",
      '"scalar"',
      '{"n":null}',
      "[]",
      "{}",
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: samples.length - 1 }),
        fc.integer({ min: -5, max: 8 }),
        fc.integer({ min: 0, max: 7 }),
        fc.integer({ min: -999999, max: 999999 }),
        fc.integer({ min: 0, max: 12 }),
        async (shape, index, scope, coefficient, scale) => {
          reach.beginHistory();
          const text = samples[shape] ?? "null";
          const number = `${String(coefficient)}.${"0".repeat(scale)}10`;
          const doc = shape === 0 ? text.replace("3.000", number) : text;
          const input = `${quote(doc)}::jsonb`;
          const selector =
            shape === 3 || index === -1 || index === 0
              ? String(index)
              : quote(shape === 2 ? "汉" : "n");
          const projection = `(d -> ${selector})::text AS j,d ->> ${selector} AS t,d::text AS full`;
          const select = `SELECT ${input} AS d`;
          let sql: string;
          if (scope === 0) sql = `SELECT ${projection} FROM(${select}) q`;
          else if (scope === 1) sql = `WITH q AS(${select}) SELECT ${projection} FROM q`;
          else if (scope === 2) {
            await core.execute("DROP VIEW IF EXISTS jsonb_view");
            await pg.exec("DROP VIEW IF EXISTS jsonb_view");
            await core.execute(`CREATE VIEW jsonb_view AS ${select}`);
            await pg.exec(`CREATE VIEW jsonb_view AS ${select}`);
            sql = `SELECT ${projection} FROM jsonb_view`;
          } else if (scope === 4) {
            const id = ++rowId;
            await core.writeBatch("insertBatch", "jsonb_documents", [{ id, doc }]);
            await pg.query("INSERT INTO jsonb_documents VALUES ($1,$2)", [id, doc]);
            if (id % 9 === 0) {
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
            }
            sql = `SELECT ${projection} FROM(SELECT doc::jsonb AS d FROM jsonb_documents WHERE id+0=${String(id)}) q`;
          } else if (scope === 7) {
            sql = `SELECT ${projection} FROM(SELECT doc::jsonb AS d FROM(SELECT ${quote(doc)}::text AS doc EXCEPT SELECT NULL::text AS doc) s) q`;
          } else
            sql = `SELECT (${input} -> ${selector})::text AS j,${input} ->> ${selector} AS t,${input}::text AS full`;
          if (scope === 5)
            sql = `(${sql}) EXCEPT SELECT NULL::text AS j,NULL::text AS t,NULL::text AS full`;
          if (scope === 6) {
            const row = (
              await pg.query<{ j: string | null; t: string | null; full: string | null }>(sql)
            ).rows[0];
            if (!row) throw new Error("Missing independent EXCEPT witness");
            const left = (["j", "t", "full"] as const)
              .map((name) => `${row[name] === null ? "NULL" : quote(row[name])}::text AS ${name}`)
              .join(",");
            sql = `SELECT ${left} EXCEPT (${sql})`;
          }
          const authority = await pg.query<Record<string, unknown>>(sql);
          rejectDescendantCheckpoint = scope === 7;
          try {
            const before = spillWritten;
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected JSONB rows");
            expect(oracleRows(result), sql).toEqual(authority.rows);
            if (scope < 5) {
              // These plans have no other spilling operator: profile bytes must equal actual
              // backend appends across every JSON producer/delivery epoch, not just the last.
              expect(core.lastProfile?.operators.reduce((n, p) => n + p.spilledBytes, 0)).toBe(
                spillWritten - before,
              );
              reach.reach("profile bytes");
            }
          } finally {
            rejectDescendantCheckpoint = false;
          }
          reach.reach(
            [
              "derived",
              "CTE",
              "view",
              "direct",
              "stored TEXT",
              "EXCEPT left",
              "EXCEPT right",
              "EXCEPT below JSON",
            ][scope] ?? "direct",
          );
          if (shape === 0) reach.reach("key order");
          if (shape === 1) {
            reach.reach("exact number");
            reach.reach("last duplicate");
          }
          if (shape === 2) reach.reach("Unicode");
          if (shape === 3) {
            reach.reach("array");
            if (index < 0) reach.reach("negative index");
          }
          if (shape === 4) reach.reach("number expansion");
          if (shape === 5 || shape === 8) reach.reach("JSON null");
          if (shape >= 5 && shape <= 7) reach.reach("scalar");
          if (authority.rows[0]?.j === null) reach.reach("missing or mismatch");
        },
      ),
      {
        seed,
        numRuns: 100,
        examples: [
          [0, -1, 7, -12, 3],
          ...samples.map((_, shape) => [shape, -1, shape % 8, -12, 3]),
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each([
  "SELECT NULL::text::jsonb::text AS j, ('{\"n\":1}'::jsonb -> NULL::text)::text AS k",
  "SELECT (NULL::text::jsonb ->> 0) AS j, '{\"n\":null}'::jsonb ->> 'n' AS k",
  "SELECT n AS n,j::text AS j FROM(SELECT 99999999999999999999999999999999999999999999999999999999999999999::numeric AS n,'{\"n\":1e1000}'::jsonb AS j) q",
  "SELECT coalesce(j, '') AS j FROM (SELECT '{\"n\":1e1000}'::jsonb ->> 'n' AS j) q",
])("keeps NULLs and mixed native checkpoint cells: %s", async (sql) => {
  const result = await core.execute(sql);
  if (!("batches" in result)) throw new Error("Expected JSONB rows");
  expect(oracleRows(result)).toEqual((await pg.query(sql)).rows);
});
it.each(["[1,]", '"\\u0000"', '"\\uD800"', "1e131072", "1e-16384", '{"a":1e131072,"a":1}'])(
  "preserves JSONB SQLSTATE for %s",
  async (text) => {
    const sql = `SELECT ${quote(text)}::jsonb::text AS j`;
    let state: string | undefined;
    try {
      await pg.query(sql);
    } catch (e) {
      state = (e as { code: string }).code;
    }
    expect(state).toBeDefined();
    await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: state });
    expect(core.spill.bytesInUse).toBe(0);
  },
);

it.each([
  `SELECT sum(CASE WHEN d::jsonb ->> 'n' IS NULL THEN 0 ELSE 1 END)::float8 AS n FROM(VALUES ('{"n":1e5000}'::text),('{"n":"汉"}'::text),(NULL::text)) q(d)`,
  `SELECT d::jsonb::text AS j,count(*)::float8 AS n FROM(VALUES ('{"n":1}'::text),('{"n":1}'::text),('{"n":1.0}'::text)) q(d) GROUP BY d::jsonb::text ORDER BY j`,
])("feeds exact JSONB rows into aggregation: %s", async (sql) => {
  const result = await core.execute(sql);
  if (!("batches" in result)) throw new Error("Expected aggregate rows");
  expect(oracleRows(result)).toEqual((await pg.query(sql)).rows);
});
