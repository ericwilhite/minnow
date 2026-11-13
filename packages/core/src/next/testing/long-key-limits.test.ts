/** Cutover capability authority is the shipped index-path policy, not PostgreSQL's btree cap. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/memory.js";
import { keyToken, secondaryTupleComponent, secondaryIndexTerm } from "../../engine/index-terms.js";
import { createReach, LONG_KEY_LIMIT_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { normalizeSqlDomainValue } from "../../engine/sql-domains.js";
import { Catalog } from "../write/catalog.js";
import { need, UNCHARGED, decodeJson, encodeJson } from "../write/bytes.js";
import {
  batch,
  column,
  TEXT,
  INT,
  ints,
  texts,
  tableInput,
  locators,
  rules,
} from "../write/test-support.js";
import { indexLimitViolation, indexMaximumKeyBytes } from "../write/index-limits.js";
import type { PhysicalType } from "../types.js";
import {
  encodeEqualityKey,
  type KeyValue,
  KeyBatch,
  encodeKeyBatch,
} from "../keys/key-encoding.js";
import { readRows } from "../write/rows.js";
import { StorageCore } from "./storage-core.js";
import { composeEngine } from "../owner/compose.js";
import { ProtocolClient } from "../owner/client.js";
import { OwnerServer } from "../owner/server.js";
import { chunkReads } from "../exec/scan.js";
import type { FlatVector, Vector } from "../vector/vector.js";
const reach = createReach("catalog long-key limits", LONG_KEY_LIMIT_STATES);
afterAll(() => reach.assertReached());
function text(units: number, glyph: string) {
  return glyph.repeat(Math.floor(units / glyph.length)) + "x".repeat(units % glyph.length);
}
async function connection(core: StorageCore) {
  const engine = composeEngine({
    planner: core.lowerer,
    writePath: core.harness.path,
    governor: core.harness.governor,
    spill: core.spill,
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "long-key", epoch: 1, build: "test" }),
    ports = new MessageChannel(),
    client = new ProtocolClient();
  server.accept(ports.port1);
  await client.connect(ports.port2);
  return {
    client,
    async close() {
      client.close();
      const stop = server.quiesce("closing");
      await stop.drained;
      stop.finish();
      ports.port1.close();
      ports.port2.close();
      await engine.close();
    },
  };
}
async function locate(core: StorageCore, n: number) {
  const view = core.harness.path.pin();
  try {
    const def = need(view.catalog.tableByName("t")),
      table = view.internalTable(def.id),
      position = def.columns.findIndex((c) => c.name === "n");
    for (const segment of table.segments) {
      const deleted = await segment.deletions();
      for (let at = 0; at < segment.rowCount; at++) {
        if (deleted?.isDeleted(at)) continue;
        const locator = segment.index * 65536 + at,
          vectors = await readRows(table, [table.rowId(locator)]),
          value = need(vectors[position]).data;
        if (value.layout === "i32" && value.values[0] === n)
          return {
            tableId: def.id,
            columnId: need(def.columns.find((c) => c.name === "k")).id,
            locator,
          };
      }
    }
    throw new Error("missing mutation target");
  } finally {
    view.release();
  }
}
async function rekey(core: StorageCore, n: number, key: string) {
  const target = await locate(core, n);
  return await core.write((s) =>
    s
      .table(target.tableId, { assigned: [target.columnId] })
      .update(batch(locators([target.locator], target.tableId), texts([key])), null),
  );
}
async function remove(core: StorageCore, n: number) {
  const target = await locate(core, n);
  return await core.write((s) =>
    s.table(target.tableId).delete(batch(locators([target.locator], target.tableId)), null),
  );
}
for (const seed of seedsFor("next-catalog-key-limit-policy", [0xb512]))
  it(`matches released scalar and tuple term policies across vector forms (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("scalar", "primaryTuple", "tuple", "legacyScalar"),
        fc.oneof(
          fc.constantFrom(0, 1, 3272, 3273, 4096, 13103, 13104, 13106, 13107, 16384, 16385),
          fc.integer({ min: 0, max: 16385 }),
        ),
        fc.constantFrom("x", "\u0800", "😀", "\0"),
        fc.constantFrom("flat", "constant", "dictionary"),
        async (shape, units, glyph, form) => {
          reach.beginHistory();
          const value = text(units, glyph);
          const created = Catalog.empty().change(
            {
              kind: "createTable",
              table: tableInput("t", [column("k", TEXT, false), column("n", INT, false)], {
                primaryKey: shape === "primaryTuple" ? [0, 1] : [0],
              }),
            },
            1,
          );
          let catalog = created.catalog;
          let index = need(catalog.table(1)?.indexes[0]);
          if (shape === "tuple" || shape === "legacyScalar") {
            catalog = catalog.change(
              {
                kind: "createIndex",
                index: {
                  tableId: 1,
                  name: "k_n",
                  columnIds: shape === "tuple" ? [1, 2] : [1],
                  unique: false,
                  role: "secondary",
                  ...(shape === "legacyScalar" ? { keyLimit: "scalar" as const } : {}),
                },
              },
              2,
            ).catalog;
            index = need(catalog.table(1)?.indexes.at(-1));
          }
          const table = need(catalog.table(1)),
            flat = texts([value]);
          let vector: Vector = flat;
          if (form === "constant")
            vector = { form: "constant", type: TEXT, value: flat, length: 1 };
          else if (form === "dictionary")
            vector = {
              form: "dictionary",
              type: TEXT,
              dictionary: flat,
              codes: Int32Array.of(0),
              validity: null,
              dictionaryId: 1,
              length: 1,
            };
          const columns = [vector, ints([1])];
          let refused = false;
          try {
            if (shape === "scalar") keyToken("string", value);
            else if (shape === "legacyScalar") secondaryIndexTerm("string", value);
            else {
              const term =
                secondaryTupleComponent("string", value) + secondaryTupleComponent("number", 1);
              if (shape === "primaryTuple") keyToken("string", term);
              else if (term.length > 65536) throw new RangeError("term limit");
            }
          } catch (error) {
            if (!(error instanceof RangeError)) throw error;
            refused = true;
          }
          const violation = indexLimitViolation(index, table, columns, null, 0, 1);
          expect(violation !== null).toBe(refused);
          if (violation !== null) {
            expect(violation).toBeInstanceOf(RangeError);
            expect(violation.code).toBe("MINNOW_INDEX_KEY_TOO_LONG");
            reach.reach("typed semantic refusal");
          } else {
            const keys = new KeyBatch(UNCHARGED);
            try {
              encodeKeyBatch(
                "equality",
                index.columnIds.map((id) => ({
                  vector: need(columns[table.columns.findIndex((c) => c.id === id)]),
                  direction: "asc",
                  nulls: "first",
                })),
                { length: 1, selection: null },
                keys,
              );
              expect(keys.key(0).length).toBeLessThanOrEqual(indexMaximumKeyBytes(index, table));
            } finally {
              keys.close();
            }
          }
          reach.reach(
            shape === "legacyScalar"
              ? "legacy scalar secondary"
              : shape === "primaryTuple"
                ? "composite primary budget"
                : shape === "tuple"
                  ? "secondary tuple budget"
                  : "scalar primary budget",
          );
          reach.reach(form);
          reach.reach(
            glyph === "😀" ? "UTF16 astral" : glyph === "\0" ? "embedded NUL" : "other Unicode",
          );
          if (glyph === "\u0800" && units <= 16384 && units > 13107)
            reach.reach("UTF8 width is not units");
        },
      ),
      { seed, numRuns: 96 },
    );
  }, 120_000);

for (const seed of seedsFor("next-catalog-long-key-sql", [0xb513]))
  it(`keeps ordinary point reads and real SQL invalid-index scans exact (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("primary", "unique", "secondary"),
        fc.constantFrom(4096, 13106, 13107, 16384, 16385),
        fc.constantFrom("x", "\u0800", "😀"),
        async (shape, units, glyph) => {
          reach.beginHistory();
          const core = await new StorageCore(4).open();
          let active = await connection(core);
          const store = new MemoryBlockStore(),
            released = new MinnowDatabase(store);
          const value = text(units, glyph);
          try {
            const ddl =
              shape === "primary"
                ? "CREATE TABLE t(k TEXT PRIMARY KEY,n INTEGER)"
                : "CREATE TABLE t(k TEXT,n INTEGER PRIMARY KEY)";
            await active.client.query(ddl);
            await released.execute(ddl);
            if (shape !== "primary") {
              const index = `CREATE ${shape === "unique" ? "UNIQUE " : ""}INDEX by_k ON t(k)`;
              await active.client.query(index);
              await released.execute(index);
            }
            await core.writeBatch("insertBatch", "t", [{ k: "a", n: 1 }]);
            await released.execute("INSERT INTO t VALUES('a',1)");
            const prepared = await active.client.prepare(
              "SELECT n FROM t WHERE k >= $1 AND k <= $1",
            );

            const old = core.harness.path.pin();
            try {
              let authorityError = false;
              try {
                await released.execute("INSERT INTO t VALUES($1,2)", [value]);
              } catch (error) {
                if (!(error instanceof RangeError)) throw error;
                authorityError = true;
              }
              if (authorityError) {
                await expect(
                  core.writeBatch("insertBatch", "t", [{ k: value, n: 2 }]),
                ).rejects.toMatchObject({ code: "MINNOW_INDEX_KEY_TOO_LONG" });
                expect((await active.client.query("SELECT n FROM t ORDER BY n")).rows()).toEqual([
                  [1],
                ]);
                reach.reach("write refusal atomic");
              } else {
                await core.writeBatch("insertBatch", "t", [{ k: value, n: 2 }]);
                expect(
                  (
                    await active.client.query(
                      "SELECT n FROM t WHERE CASE WHEN k=$1 THEN true ELSE false END",
                      [value],
                    )
                  ).rows(),
                ).toEqual([[2]]);
                const def = need(core.harness.path.catalog.tableByName("t")),
                  index = need(
                    def.indexes.find((i) =>
                      shape === "primary" ? i.role === "primary" : i.name === "by_k",
                    ),
                  );
                const legacyIndex = (await released.introspect()).tables[0]?.indexes?.find(
                  (i) => i.name === "by_k",
                );
                expect(index.invalid === true).toBe(legacyIndex?.state === "invalid");
                if (index.invalid === true) {
                  const unavailable = core.harness.path.pin();
                  try {
                    expect(() => unavailable.index(def.id, index.id)).toThrow();
                  } finally {
                    unavailable.release();
                  }
                  const replanned = core.lowerer.lower(
                    "SELECT n FROM t WHERE k=$1",
                    core.context(),
                  );
                  if (replanned.kind !== "query") throw new Error("query expected");
                  expect(JSON.stringify(replanned.plan.root)).not.toMatch(
                    /"op":"(?:keyLookup|indexRange)"/,
                  );
                  expect((await active.client.execute(prepared.id, [value])).rows()).toEqual([[2]]);
                  reach.reach("invalid secondary scans");
                  reach.reach("prepared replan");
                }
                if (index.invalid !== true) {
                  const pinned = core.harness.path.pin(),
                    keys = new KeyBatch(UNCHARGED);
                  try {
                    encodeKeyBatch(
                      "equality",
                      [{ vector: texts([value]), direction: "asc", nulls: "first" }],
                      { length: 1, selection: null },
                      keys,
                    );
                    const found: number[] = [];
                    await pinned.index(def.id, index.id).resolve(keys.key(0), found);
                    expect(found).toHaveLength(1);
                    const row = await readRows(pinned.internalTable(def.id), found);
                    const n = need(row[def.columns.findIndex((c) => c.name === "n")]).data;
                    expect(n.layout === "i32" ? n.values[0] : null).toBe(2);
                  } finally {
                    keys.close();
                    pinned.release();
                  }
                }
                const changed = text(units - 1, glyph) + "z";
                await rekey(core, 2, changed);
                expect(
                  (
                    await active.client.query(
                      "SELECT n FROM t WHERE CASE WHEN k=$1 THEN true ELSE false END",
                      [changed],
                    )
                  ).rows(),
                ).toEqual([[2]]);
                await remove(core, 2);
                expect((await active.client.query("SELECT n FROM t ORDER BY n")).rows()).toEqual([
                  [1],
                ]);
                reach.reach("update and delete");
              }
              const def = need(old.catalog.tableByName("t")),
                key = need(
                  def.indexes.find((i) =>
                    shape === "primary" ? i.role === "primary" : i.name === "by_k",
                  ),
                );
              const keys = new KeyBatch(UNCHARGED);
              try {
                encodeKeyBatch(
                  "equality",
                  [{ vector: texts(["a"]), direction: "asc", nulls: "first" }],
                  { length: 1, selection: null },
                  keys,
                );
                const found: number[] = [];
                await old.index(def.id, key.id).resolve(keys.key(0), found);
                expect(found).toHaveLength(1);
              } finally {
                keys.close();
              }
              reach.reach("old ready snapshot");
            } finally {
              old.release();
            }
            const policy = core.harness.path.catalog
              .tableByName("t")
              ?.indexes.map((i) => [i.id, i.invalid]);
            await active.close();
            await core.harness.reopen();
            active = await connection(core);
            expect(
              (
                await active.client.query(
                  "SELECT n FROM t WHERE CASE WHEN k='a' THEN true ELSE false END",
                )
              ).rows(),
            ).toEqual([[1]]);
            expect(
              core.harness.path.catalog.tableByName("t")?.indexes.map((i) => [i.id, i.invalid]),
            ).toEqual(policy);
            reach.reach("durable catalog policy");
          } finally {
            await active.close();
            await core.close();
            await released.close();
            store.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 24 },
    );
  }, 180_000);

for (const seed of seedsFor("next-catalog-limit-rollback-backfill", [0xb514]))
  it(`undoes invalidation and backfills without planner-ready omissions (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(13107, 16384), fc.boolean(), async (units, rollback) => {
        reach.beginHistory();
        const core = await new StorageCore(4).open();
        let active = await connection(core);
        const c = active.client,
          value = text(units, "\u0800");
        try {
          await c.query("CREATE TABLE t(id INTEGER PRIMARY KEY,k TEXT)");
          await c.query("CREATE INDEX by_k ON t(k)");
          await core.writeBatch("insertBatch", "t", [{ id: 1, k: "a" }]);
          const tx = await core.harness.path.begin(),
            savepoint = tx.savepoint(),
            s = tx.statement({ rules }),
            tid = need(tx.catalog.tableByName("t")).id;
          try {
            await s.table(tid).insert(batch(ints([2]), texts([value])), null);
            await s.end();
            expect(
              tx.catalog.tableByName("t")?.indexes.find((i) => i.name === "by_k")?.invalid,
            ).toBe(true);
            if (rollback) {
              tx.rollback();
              reach.reach("transaction undo");
            } else {
              tx.rollbackTo(savepoint);
              const receipt = await tx.commit(null);
              await receipt.durable;
              reach.reach("savepoint undo");
            }
          } catch (error) {
            s.rollback();
            tx.rollback();
            throw error;
          }
          const def = need(core.harness.path.catalog.tableByName("t"));
          expect(def.indexes.find((i) => i.name === "by_k")?.invalid).toBeUndefined();
          await c.query("DROP INDEX by_k");
          await core.writeBatch("insertBatch", "t", [{ id: 2, k: value }]);
          await c.query("CREATE INDEX by_k ON t(k)");
          expect(
            core.harness.path.catalog.tableByName("t")?.indexes.find((i) => i.name === "by_k")
              ?.invalid,
          ).toBe(true);
          expect((await c.query("SELECT id FROM t WHERE k=$1", [value])).rows()).toEqual([[2]]);
          reach.reach("backfill invalidation");
          await c.query("DROP INDEX by_k");
          await expect(c.query("CREATE UNIQUE INDEX unique_k ON t(k)")).rejects.toMatchObject({
            code: "MINNOW_INDEX_KEY_TOO_LONG",
          });
          expect(
            core.harness.path.catalog.tableByName("t")?.indexes.some((i) => i.name === "unique_k"),
          ).toBe(false);
          await c.query("CREATE INDEX by_k ON t(k)");
          await active.close();
          await core.harness.reopen();
          active = await connection(core);
          expect(
            (await active.client.query("SELECT id FROM t WHERE k=$1", [value])).rows(),
          ).toEqual([[2]]);
          reach.reach("backfill replay");
        } finally {
          await active.close();
          await core.close();
        }
        expect(core.harness.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 8 },
    );
  }, 180_000);

for (const seed of seedsFor("next-catalog-domain-term-costs", [0xb517]))
  it(`counts canonical legacy domain components at tuple boundaries (seed ${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.constantFrom("numeric", "uuid", "date", "time", "interval", "enum"),
        fc.integer({ min: -1, max: 1 }),
        fc.boolean(),
        (family, extra, primary) => {
          reach.beginHistory();
          let vector: FlatVector, legacy: string;
          switch (family) {
            case "numeric": {
              const coefficient = 123456789012345678901234567890n;
              vector = {
                form: "flat",
                type: { kind: "decimal", precision: null, scale: null },
                length: 1,
                validity: null,
                data: {
                  layout: "decimal",
                  scale: 6,
                  scales: null,
                  specials: null,
                  coefficients: { repr: "wide", values: [coefficient] },
                },
              };
              legacy = need(
                normalizeSqlDomainValue({ kind: "numeric" }, "123456789012345678901234.567890"),
              );
              break;
            }
            case "uuid":
              vector = {
                form: "flat",
                type: { kind: "uuid" },
                length: 1,
                validity: null,
                data: { layout: "fixed16", bytes: new Uint8Array(16).fill(17) },
              };
              legacy = need(
                normalizeSqlDomainValue({ kind: "uuid" }, "11111111-1111-1111-1111-111111111111"),
              );
              break;
            case "date":
              vector = ints([0], { kind: "date" });
              legacy = need(normalizeSqlDomainValue({ kind: "date" }, "2000-01-01"));
              break;
            case "time":
              vector = {
                form: "flat",
                type: { kind: "time" },
                length: 1,
                validity: null,
                data: {
                  layout: "int64",
                  values: { repr: "int64", values: BigInt64Array.of(1234567890n) },
                },
              };
              legacy = need(normalizeSqlDomainValue({ kind: "time" }, "00:20:34.56789"));
              break;
            case "interval":
              vector = {
                form: "flat",
                type: { kind: "interval" },
                length: 1,
                validity: null,
                data: {
                  layout: "interval",
                  months: Int32Array.of(2),
                  days: Int32Array.of(-3),
                  micros: { repr: "int64", values: BigInt64Array.of(4500n) },
                },
              };
              legacy = need(
                normalizeSqlDomainValue({ kind: "interval" }, "2 months -3 days 4500 microseconds"),
              );
              break;
            default: {
              const name = 'm"\\\n',
                label = '😀"\\\n水',
                type = { kind: "enum" as const, name, labels: ["other", label] };
              vector = { ...texts([label]), type };
              legacy = need(
                normalizeSqlDomainValue({ kind: "enum", name, values: type.labels }, label),
              );
              break;
            }
          }
          const budget = primary ? 16384 : 65536,
            term = secondaryTupleComponent("string", legacy);
          const units = Math.floor((budget - term.length - 5) / 5) + extra,
            value = "\u0800".repeat(units);
          let catalog = Catalog.empty().change(
            {
              kind: "createTable",
              table: tableInput(
                "domain",
                [column("k", TEXT, false), column("v", vector.type, false)],
                { primaryKey: primary ? [0, 1] : null },
              ),
            },
            1,
          ).catalog;
          if (!primary)
            catalog = catalog.change(
              {
                kind: "createIndex",
                index: {
                  tableId: 1,
                  name: "term",
                  columnIds: [1, 2],
                  unique: true,
                  role: "secondary",
                },
              },
              2,
            ).catalog;
          const table = need(catalog.table(1)),
            index = need(table.indexes[0]);
          let refused = false;
          try {
            const full = secondaryTupleComponent("string", value) + term;
            if (primary) keyToken("string", full);
            else if (full.length > 65536) throw new RangeError("whole term");
          } catch (error) {
            if (!(error instanceof RangeError)) throw error;
            refused = true;
          }
          expect(
            indexLimitViolation(index, table, [texts([value]), vector], null, 0, 1) !== null,
          ).toBe(refused);
          reach.reach("canonical domain term costs");
          reach.reach(family === "enum" ? "enum without native cache" : "other domain components");
        },
      ),
      { seed, numRuns: 48 },
    );
  });

for (const seed of seedsFor("next-index-policy-corruption", [0xb519]))
  it(`refuses malformed catalog policy and availability metadata (seed ${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.constantFrom("unknown", "uniqueInvalid", "falseFlag", "scalarTuple"),
        (mode) => {
          reach.beginHistory();
          const catalog = Catalog.empty().change(
            {
              kind: "createTable",
              table: tableInput("t", [column("k", TEXT, false), column("n", INT, false)]),
            },
            1,
          ).catalog;
          const data = decodeJson(catalog.encode()) as {
            tables: Array<{ indexes: Array<Record<string, unknown>> }>;
          };
          const index = need(data.tables[0]?.indexes[0]);
          if (mode === "unknown") index.keyLimit = "future";
          else if (mode === "uniqueInvalid") index.invalid = true;
          else if (mode === "falseFlag") index.invalid = false;
          else {
            index.keyLimit = "scalar";
            index.columnIds = [1, 2];
          }
          expect(() => Catalog.decode(encodeJson(data))).toThrow(
            expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
          );
          reach.reach("catalog policy corruption refusal");
        },
      ),
      { seed, numRuns: 16 },
    );
  });

for (const seed of seedsFor("next-fixed-index-capacity", [0xb51d]))
  it(`bounds every fixed scalar's actual equality encoding (seed ${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.constantFrom(
          "bool",
          "int32",
          "float32",
          "float64",
          "int64",
          "date",
          "time",
          "timestamp",
          "uuid",
          "interval",
        ),
        fc.integer({ min: -100, max: 100 }),
        (kind, n) => {
          reach.beginHistory();
          let type: PhysicalType,
            value: KeyValue = n;
          switch (kind) {
            case "bool":
              type = { kind: "bool" };
              value = n > 0;
              break;
            case "int32":
              type = INT;
              break;
            case "float32":
              type = { kind: "float32" };
              value = n / 4;
              break;
            case "float64":
              type = { kind: "float64" };
              value = n / 4;
              break;
            case "int64":
              type = { kind: "int64" };
              value = BigInt(n);
              break;
            case "date":
              type = { kind: "date" };
              break;
            case "time":
              type = { kind: "time" };
              value = BigInt(Math.abs(n));
              break;
            case "timestamp":
              type = { kind: "timestamp", zoned: false };
              value = BigInt(n);
              break;
            case "uuid":
              type = { kind: "uuid" };
              value = new Uint8Array(16).fill(n & 255);
              break;
            default:
              type = { kind: "interval" };
              value = { months: n, days: 0, micros: 0n };
          }
          const catalog = Catalog.empty().change(
            { kind: "createTable", table: tableInput("fixed", [column("k", type, false)]) },
            1,
          ).catalog;
          const table = need(catalog.table(1)),
            index = need(table.indexes[0]);
          const key = encodeEqualityKey([{ type, value }]);
          expect(indexMaximumKeyBytes(index, table)).toBeGreaterThanOrEqual(key.length);
          reach.reach(
            kind === "float32" ? "float32 full equality width" : "other fixed equality widths",
          );
        },
      ),
      { seed, numRuns: 48 },
    );
  });
