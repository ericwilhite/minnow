/** Native scan FTS is exact to the shipped documented FTS extension, including corpus scope. */
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_SCAN_REACH } from "../../testing/reach.js";
import { allocateFlat } from "../vector/vector.js";
import { parseDate, timestampFromDate } from "../temporal.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { StorageCore } from "./storage-core.js";
import { checkPlan } from "./ir-check.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-fts-scan", [0xb7f3, 0xb7f4]))
  it(`matches shipped SQL on unfiltered snapshot statistics (${String(seed)})`, async () => {
    const core = await new StorageCore(16).open();
    const db = new MinnowDatabase(new MemoryBlockStore(), { ftsAutoIndexRows: 1_000_000 });
    const random = mulberry32(seed),
      reach = createReach(`FTS scan ${String(seed)}`, FTS_SCAN_REACH);
    try {
      const ddl = "CREATE TABLE docs(id INTEGER PRIMARY KEY,a TEXT,b TEXT,flag BOOLEAN)";
      await core.execute(ddl);
      await db.execute(ddl);
      const vocab = ["alpha", "alpine", "beta", "ﬁle", "東京タワー", "Straße", "ＡＢＣ", "!!!"];
      const pick = () => vocab[Math.floor(random() * vocab.length)] ?? "alpha";
      for (let round = 0; round < 4; round++) {
        const rows = [
          { id: round * 100, a: null, b: null, flag: false },
          { id: round * 100 + 1, a: "", b: null, flag: true },
          { id: round * 100 + 2, a: "alpha alpha", b: "alpine", flag: true },
          { id: round * 100 + 3, a: "alpha beta", b: "alpha", flag: false },
          ...Array.from({ length: 28 }, (_, i) => ({
            id: round * 100 + 4 + i,
            a: random() < 0.2 ? null : `${pick()} ${pick()}`,
            b: random() < 0.2 ? null : pick(),
            flag: random() < 0.5,
          })),
        ];
        await core.writeBatch("insertBatch", "docs", rows);
        await db.insertBatch("docs", rows);
        for (const query of ["alpha al*", "beta", "東京", "ﬁle", "", "!!!", "alpha alpha"]) {
          reach.beginHistory();
          reach.reach("whole corpus");
          reach.reach("NULL empty");
          reach.reach("prefix union");
          reach.reach("Unicode");
          reach.reach("duplicate terms and columns");
          const q = `'${query}'`;
          const sqls = [
            `SELECT id,MATCH(a,b) AGAINST ${q} m FROM docs ORDER BY id`,
            `SELECT id FROM docs WHERE NOT MATCH(a,b) AGAINST ${q} OR id%3=0 ORDER BY id`,
            `SELECT id,MATCH(*) AGAINST ${q} m FROM docs ORDER BY id`,
            `SELECT id,BM25(a,b) AGAINST ${q} s FROM docs WHERE id%3=0 ORDER BY s DESC NULLS LAST,id LIMIT 5 OFFSET 1`,
            `SELECT id,BM25(a,a,b) AGAINST ${q} s,BM25(b) AGAINST ${q} t FROM docs ORDER BY id`,
            `SELECT count(*) n FROM docs WHERE BM25(a,b) AGAINST ${q}>0`,
            `SELECT id,CASE WHEN flag THEN MATCH(a,b) AGAINST ${q} ELSE NULL END m FROM docs ORDER BY id`,
          ];
          for (const sql of sqls) {
            const expected = await db.query(sql, { memoize: false }),
              actual = await core.execute(sql);
            if (!("rows" in expected) || !("batches" in actual)) throw Error("query");
            expect(oracleRows(actual), sql).toEqual(expected.rows);
          }
        }
      }
      reach.reach("parameters");
      const sql =
        "SELECT id,BM25(a,b) AGAINST $1 s FROM docs WHERE MATCH(a,b) AGAINST $1 ORDER BY s DESC NULLS LAST,id";
      const lowered = core.lowerer.lower(sql, core.context());
      if (lowered.kind !== "query") throw Error("query plan");
      checkPlan(lowered.plan, core.harness.path.catalog);
      const prepared = core.prepare(sql),
        prepare = core.executor.prepare.bind(core.executor);
      core.executor.prepare = () => prepared;
      try {
        for (const q of ["al*", "beta", ""]) {
          const expected = await db.query(sql, { params: [q], memoize: false }),
            actual = await core.execute(sql, [q]);
          if (!("rows" in expected) || !("batches" in actual)) throw Error("query");
          expect(oracleRows(actual)).toEqual(expected.rows);
        }
      } finally {
        core.executor.prepare = prepare;
      }
      await core.execute("CREATE TABLE heap(a TEXT,flag BOOLEAN)");
      await db.execute("CREATE TABLE heap(a TEXT,flag BOOLEAN)");
      const heap = [
        { a: "alpha", flag: true },
        { a: null, flag: false },
      ];
      await core.writeBatch("insertBatch", "heap", heap);
      await db.insertBatch("heap", heap);
      const wildcard = "SELECT MATCH(*) AGAINST '1' m FROM heap";
      const before = await core.execute(wildcard);
      if (!("batches" in before)) throw Error("heap query");
      expect(oracleRows(before)).toEqual((await db.query(wildcard)).rows);
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      const missing = "anchor " + (random() < 0.5 ? "東京".repeat(2048) : "ﬁle".repeat(2048));
      const repeated = "SELECT MATCH(*) AGAINST 'anchor' m FROM heap";
      const previous = await core.execute(repeated);
      if (!("batches" in previous)) throw Error("heap query");
      expect(oracleRows(previous)).toEqual((await db.query(repeated, { memoize: false })).rows);
      // ALTER execution belongs to B6 and is refused on this stage's base. DROP/recreate
      // exercises the real catalog-version/cache boundary without inventing that route.
      await core.execute("DROP TABLE heap");
      await db.execute("DROP TABLE heap");
      const replacement = `CREATE TABLE heap(a TEXT,flag BOOLEAN,extra TEXT DEFAULT '${missing}')`;
      await core.execute(replacement);
      await db.execute(replacement);
      const replacementRows = heap.map((row) => ({ ...row, extra: missing }));
      await core.writeBatch("insertBatch", "heap", replacementRows);
      await db.insertBatch("heap", replacementRows);
      const current = await core.execute(repeated);
      if (!("batches" in current)) throw Error("heap query");
      expect(oracleRows(current)).toEqual((await db.query(repeated, { memoize: false })).rows);
      const rebound = "SELECT BM25(*) AGAINST 'anchor' s,MATCH(*) AGAINST 'anchor' m FROM heap";
      const after = await core.execute(rebound);
      if (!("batches" in after)) throw Error("heap query");
      expect(oracleRows(after)).toEqual((await db.query(rebound, { memoize: false })).rows);
      // Newly searchable wide text must enter preflight even when two output rows fit
      // in one source batch. The compiled wildcard must use the current declaration.
      expect(
        Math.max(
          ...(core.lastProfile?.operators
            .filter((op) => op.op === "fts")
            .map((op) => op.peakHeldBytes) ?? [0]),
        ),
      ).toBeGreaterThan(5 << 20);
      reach.reach("schema and wildcard rebind");
      await core.execute("CREATE TABLE nums(id INTEGER PRIMARY KEY,n NUMERIC)");
      await db.execute("CREATE TABLE nums(id INTEGER PRIMARY KEY,n NUMERIC)");
      const nums = [
        { id: 0, n: "1e-1000" },
        { id: 1, n: "0" },
        { id: 2, n: "-1.50" },
      ];
      await core.writeBatch("insertBatch", "nums", nums);
      await db.insertBatch("nums", nums);
      const numeric =
        "SELECT id,MATCH(n) AGAINST '0' m,BM25(n) AGAINST '0' s FROM nums ORDER BY id";
      const numericRows = await core.execute(numeric);
      if (!("batches" in numericRows)) throw Error("numeric query");
      expect(oracleRows(numericRows)).toEqual((await db.query(numeric, { memoize: false })).rows);
      // An unconstrained NUMERIC's compact coefficient is not a bound on display scale.
      // Until a per-value scale proof exists, fund the legal document ceiling once.
      expect(
        Math.max(
          ...(core.lastProfile?.operators
            .filter((op) => op.op === "fts")
            .map((op) => op.peakHeldBytes) ?? [0]),
        ),
      ).toBeGreaterThan(8 << 20);
      reach.reach("numeric display width");
      reach.assertReached();
    } finally {
      await core.close();
      await db.close();
    }
  }, 60000);

it("characterizes shipped FTS renderings across logical types", async () => {
  const core = await new StorageCore().open();
  const db = new MinnowDatabase(new MemoryBlockStore(), { ftsAutoIndexRows: 1_000_000 });
  try {
    const ddl =
      "CREATE TABLE typed(id INTEGER PRIMARY KEY,n NUMERIC(8,2),d DATE,t TIMESTAMP,z TIMESTAMPTZ,tm TIME,iv INTERVAL,j JSONB,u UUID,a TEXT[])";
    await core.execute(ddl);
    await db.execute(ddl);
    const rows = [
      {
        id: 1,
        n: "1.50",
        d: "2026-01-02",
        t: new Date("2026-01-02T03:04:05.000Z"),
        z: new Date("2026-01-02T03:04:05.000Z"),
        tm: "10:00:00.5",
        iv: "1 day 2 hours",
        j: '{"foo":"alpha"}',
        u: "123e4567-e89b-12d3-a456-426614174000",
        a: '["alpha","beta"]',
      },
    ];
    const table = core.harness.path.catalog.tableByName("typed");
    if (!table) throw Error("typed table missing");
    const encoded = new TextEncoder();
    const arrayBytes = encodeArrayValue(
      { kind: "text" },
      {
        dimensions: [2],
        lowerBounds: [1],
        elements: [encoded.encode("alpha"), encoded.encode("beta")],
      },
    );
    const columns = table.columns.map((column) => {
      const bytes =
        column.name === "a"
          ? arrayBytes
          : encoded.encode(column.name === "j" ? '{"foo":"alpha"}' : "");
      const source = allocateFlat(column.type, 1);
      const vector = {
        ...source,
        data:
          source.data.layout === "varlen"
            ? { layout: "varlen" as const, bytes, offsets: Int32Array.of(0, bytes.length) }
            : source.data,
      };
      vector.length = 1;
      const data = vector.data;
      if (data.layout === "i32")
        data.values[0] = column.name === "id" ? 1 : parseDate("2026-01-02");
      else if (data.layout === "int64" && data.values.repr === "safe")
        data.values.values[0] =
          column.name === "tm"
            ? 36_000_500_000
            : Number(timestampFromDate(new Date("2026-01-02T03:04:05Z")));
      else if (data.layout === "decimal" && data.coefficients.repr === "safe") {
        data.coefficients.values[0] = 150;
        if (data.scales) data.scales[0] = 2;
      } else if (data.layout === "interval") {
        data.months[0] = 0;
        data.days[0] = 1;
        if (data.micros.repr === "safe") data.micros.values[0] = 7_200_000_000;
      } else if (data.layout === "varlen") {
        expect(data.bytes.length).toBe(bytes.length);
      } else if (data.layout === "fixed16")
        data.bytes.set(
          Uint8Array.from("123e4567e89b12d3a456426614174000".match(/../gu) ?? [], (value) =>
            parseInt(value, 16),
          ),
        );
      else throw Error("typed fixture");
      return vector;
    });
    await core.insertVectors(table, { length: 1, selection: null, columns });
    await db.insertBatch("typed", rows);
    const differences: unknown[] = [];
    for (const column of ["n", "d", "t", "z", "tm", "iv", "j", "u", "a"])
      for (const query of [
        "50",
        "5",
        "500",
        "000z",
        "2026",
        "alpha",
        "beta",
        "days",
        "7200000000",
        "null",
      ]) {
        const sql = `SELECT MATCH(${column}) AGAINST '${query}' m,BM25(${column}) AGAINST '${query}' s FROM typed`;
        const expected = await db.query(sql, { memoize: false }),
          actual = await core.execute(sql);
        if (!("batches" in actual)) throw Error("query");
        if (JSON.stringify(oracleRows(actual)) !== JSON.stringify(expected.rows))
          differences.push({ column, query, actual: oracleRows(actual), expected: expected.rows });
      }
    expect(differences).toEqual([]);
  } finally {
    await core.close();
    await db.close();
  }
}, 60000);

for (const seed of seedsFor("next-fts-scan-shapes", [0xb7f9, 0xb7fa]))
  it(`preserves join residuals, grouping, parameter validation and empty input (${String(seed)})`, async () => {
    const core = await new StorageCore(8).open(),
      db = new MinnowDatabase(new MemoryBlockStore());
    try {
      for (const ddl of [
        "CREATE TABLE l(id INTEGER PRIMARY KEY,a TEXT)",
        "CREATE TABLE r(id INTEGER PRIMARY KEY,b TEXT)",
      ]) {
        await core.execute(ddl);
        await db.execute(ddl);
      }
      const random = mulberry32(seed);
      for (const [name, key] of [
        ["l", "a"],
        ["r", "b"],
      ] as const) {
        const rows = Array.from({ length: 12 }, (_, id) => ({
          id,
          [key]: id % 4 === 0 ? null : random() < 0.5 ? "alpha" : "beta",
        }));
        await core.writeBatch("insertBatch", name, rows);
        await db.insertBatch(name, rows);
      }
      for (const sql of [
        "SELECT l.id,r.id j FROM l LEFT JOIN r ON l.id=r.id AND MATCH(l.a,r.b) AGAINST 'alpha beta' ORDER BY l.id,j",
        "SELECT l.id,r.id j FROM l FULL JOIN r ON l.id=r.id WHERE MATCH(l.a,r.b) AGAINST 'alpha' ORDER BY l.id NULLS LAST,j NULLS LAST",
        "SELECT a,count(*) n FROM l WHERE MATCH(a) AGAINST 'al*' GROUP BY a ORDER BY a",
        "SELECT sum(BM25(a) AGAINST 'alpha') s FROM l WHERE id>2",
        "SELECT id FROM l WHERE CASE WHEN id%2=0 THEN MATCH(a) AGAINST '' ELSE MATCH(a) AGAINST 'beta' END ORDER BY id",
      ]) {
        const expected = await db.query(sql, { memoize: false }),
          actual = await core.execute(sql);
        if (!("batches" in actual)) throw Error("query");
        expect(oracleRows(actual), sql).toEqual(expected.rows);
      }
      const grouped = "SELECT a,MATCH(a) AGAINST 'al*' m FROM l GROUP BY a ORDER BY a NULLS LAST";
      await expect(db.query(grouped)).rejects.toThrow("GROUP BY");
      await expect(core.execute(grouped)).rejects.toThrow();
      const ddl = "CREATE TABLE mixed(id INTEGER PRIMARY KEY,n NUMERIC,a TEXT)";
      await core.execute(ddl);
      await db.execute(ddl);
      const mixed = [
        { id: 0, n: "1.50", a: "001 50" },
        { id: 1, n: "1.50", a: "1 5" },
        { id: 2, n: "2", a: "2 00" },
      ];
      await core.writeBatch("insertBatch", "mixed", mixed);
      await db.insertBatch("mixed", mixed);
      for (const q of ["001.50", "1.5", "2.00"]) {
        const mixedSql =
          "SELECT id,MATCH(a) AGAINST $1 m,BM25(a) AGAINST $1 s FROM mixed WHERE n=$1 ORDER BY id";
        const result = await core.execute(mixedSql, [q]);
        if (!("batches" in result)) throw Error("mixed query");
        expect(oracleRows(result)).toEqual(
          (await db.query(mixedSql, { params: [q], memoize: false })).rows,
        );
      }
      for (const op of ["MATCH", "BM25"])
        for (const value of [null, 1, false, "a ".repeat(33), "a".repeat(16385)])
          for (const suffix of ["WHERE false", "LIMIT 0"]) {
            const sql = `SELECT ${op}(a) AGAINST $1 s FROM l ${suffix}`;
            await expect(core.execute(sql, [value])).rejects.toThrow();
            await expect(db.query(sql, { params: [value], memoize: false })).rejects.toThrow();
          }
      await core.execute("CREATE TABLE empty(a TEXT)");
      await db.execute("CREATE TABLE empty(a TEXT)");
      for (const sql of [
        "SELECT MATCH(a) AGAINST 'alpha' m,BM25(a) AGAINST 'alpha' s FROM empty",
        "SELECT count(*) n FROM empty WHERE BM25(a) AGAINST 'alpha'>0",
      ]) {
        const expected = await db.query(sql),
          actual = await core.execute(sql);
        if (!("batches" in actual)) throw Error("query");
        expect(oracleRows(actual)).toEqual(expected.rows);
      }
    } finally {
      await core.close();
      await db.close();
    }
  }, 60000);
