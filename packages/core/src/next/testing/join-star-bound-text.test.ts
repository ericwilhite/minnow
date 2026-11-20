/** Owner literal normalization is a production entry: bounded TEXT values may
 * select exact-order star state, while invalid bindings keep old demand/errors. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  B9_STAR_BOUND_TEXT_REACH,
  B9_STAR_TEXT_DEMAND_REACH,
  B9_STAR_TEXT_SCOPE_REACH,
} from "../../testing/reach.js";
import { createExecutor } from "../exec/create.js";
import { QueryRuntime } from "../exec/run.js";
import { prepareParameters } from "../exec/parameters.js";
import { Governor } from "../memory/governor.js";
import { StarLookups } from "../exec/join-star-lookups.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { ProtocolClient, type ClientResult } from "../owner/client.js";
import type { WireValue } from "../owner/protocol.js";
import { chunkReads } from "../exec/scan.js";
import { StorageCore } from "./storage-core.js";
const VALUES = [null, "t1", "t0", "", "λ", "\ufefft1", "quote'", "x".repeat(1024)] as const;
const statement = (tier: string) =>
  `SELECT a.tier AS tier_a,b.tier AS tier_b,c.label AS label_c,COUNT(*) AS n,SUM(f.v) AS s FROM sf f JOIN sa a ON a.k=f.a JOIN sb b ON b.k=f.b JOIN sc c ON c.k=f.c WHERE a.active AND b.weight>1 AND c.tier=${tier} GROUP BY a.tier,b.tier,c.label ORDER BY s DESC,tier_a,tier_b,label_c LIMIT 50`;
const quoted = (s: string) => `'${s.replaceAll("'", "''")}'`;
async function owner(shared?: StorageCore, queryGovernor?: Governor, planCacheBytes = 0) {
  const core = shared ?? (await new StorageCore(32).open()),
    candidate = createExecutor(),
    control = createExecutor({ starLookups: false, starDictionary: false });
  let enabled = true,
    prepares = 0;
  const engine = composeEngine({
      planner: core.lowerer,
      writePath: core.harness.path,
      governor: queryGovernor ?? core.harness.governor,
      spill: core.spill,
      reads: chunkReads,
      planCacheBytes,
      executor: {
        prepare: (plan) => {
          prepares++;
          return (enabled ? candidate : control).prepare(plan);
        },
      },
    }),
    server = new OwnerServer(engine, { ownerId: "bound-star", epoch: 1, build: "test" }),
    channel = new MessageChannel(),
    client = new ProtocolClient();
  server.accept(channel.port2);
  await client.connect(channel.port1);
  return {
    core,
    engine,
    governor: queryGovernor ?? core.harness.governor,
    get prepares() {
      return prepares;
    },
    query: (fast: boolean, sql: string, params: readonly WireValue[]) => {
      enabled = fast;
      return client.query(sql, params);
    },
    async close() {
      const draining = server.quiesce("closing");
      try {
        await draining.drained;
        client.close();
        await engine.close();
      } finally {
        await core.close();
        draining.finish();
        channel.port1.close();
        channel.port2.close();
      }
    },
  };
}
type Owner = Awaited<ReturnType<typeof owner>>;
async function fixture(o: Owner, pg?: PGlite) {
  const ddl = [
    "CREATE TABLE sf(id INTEGER PRIMARY KEY,a TEXT,b TEXT,c TEXT,v DOUBLE PRECISION)",
    ...["sa", "sb", "sc"].map(
      (name) =>
        `CREATE TABLE ${name}(k TEXT PRIMARY KEY,tier TEXT,label TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
    ),
  ];
  if (pg) await pg.exec("DROP TABLE IF EXISTS sf,sa,sb,sc");
  for (const sql of ddl) {
    await o.core.execute(sql);
    if (pg) await pg.exec(sql);
  }
  for (const name of ["sa", "sb", "sc"]) {
    const rows = Array.from({ length: 8 }, (_, id) => {
      const tier = VALUES[id];
      if (tier === undefined) throw Error("Missing fixture tier");
      return {
        k: `k${String(id)}`,
        tier,
        label: id % 3 === 0 ? null : id % 3 === 1 ? "\ufefflabel" : "λ",
        weight: 2,
        active: true,
      };
    });
    await o.core.writeBatch("insertBatch", name, rows);
    if (pg)
      await pg.query(
        `INSERT INTO ${name} SELECT * FROM json_to_recordset($1::json) x(k text,tier text,label text,weight double precision,active boolean)`,
        [JSON.stringify(rows)],
      );
  }
  const rows = Array.from({ length: 128 }, (_, id) => ({
    id,
    a: id % 13 === 0 ? null : `k${String(id % 8)}`,
    b: `k${String(Math.floor(id / 3) % 8)}`,
    c: `k${String(Math.floor(id / 5) % 8)}`,
    v: id % 11 === 0 ? null : ((id % 31) - 15) / 4,
  }));
  await o.core.writeBatch("insertBatch", "sf", rows);
  if (pg)
    await pg.query(
      "INSERT INTO sf SELECT * FROM json_to_recordset($1::json) x(id int,a text,b text,c text,v double precision)",
      [JSON.stringify(rows)],
    );
  await o.core.harness.path.recycleLog();
  await o.core.harness.path.quiesce();
}
const bits = (result: ClientResult) =>
  result
    .rows()
    .map((row) =>
      row.map((v) =>
        typeof v === "number"
          ? { bits: Buffer.from(new Float64Array([v]).buffer).toString("hex") }
          : v,
      ),
    );
function proofCounter(o: Owner) {
  let count = 0;
  const governor = o.governor,
    single = governor.tryAdmit.bind(governor),
    group = governor.admitGroup.bind(governor);
  const a = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
    if (options.label.includes("/star-domain-planning")) count++;
    return single(options);
  });
  const b = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
    if (options.label.includes("/star-domain-planning")) count++;
    return group(options);
  });
  return {
    get count() {
      return count;
    },
    close() {
      a.mockRestore();
      b.mockRestore();
    },
  };
}
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("b9-bound-text-star", [0xb9e71, 0xb9e72]))(
  "ordinary owner binds TEXT once with exact PG keys/native bits seed %i",
  async (seed) => {
    const reach = createReach("B9 ordinary bound TEXT star", B9_STAR_BOUND_TEXT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(...VALUES), fc.boolean(), async (value, explicit) => {
        reach.beginHistory();
        const o = await owner();
        try {
          await fixture(o, pg);
          const parameter = explicit || value === null,
            sql = statement(parameter ? "$1" : quoted(value)),
            params = parameter ? [value] : [],
            planned = o.engine.plan(sql).lowered;
          expect(planned.kind).toBe("query");
          if (planned.kind !== "query") throw Error("Not a query");
          expect(planned.plan.parameters).toHaveLength(1);
          expect(planned.plan.parameters[0]?.kind).toBe("text");
          reach.reach(parameter ? "explicit TEXT slot" : "owner normalized literal");
          const before = await o.query(false, sql, params),
            replay = vi.spyOn(StarLookups.prototype, "probe"),
            proofs = proofCounter(o);
          let after: ClientResult;
          try {
            after = await o.query(true, sql, params);
            expect(replay).toHaveBeenCalledTimes(1);
            expect(proofs.count).toBeGreaterThan(0);
          } finally {
            replay.mockRestore();
            proofs.close();
          }
          expect(bits(after)).toEqual(bits(before));
          expect(after.columns[3]?.type.kind).toBe("int64");
          const reference = await pg.query<unknown[]>(
            `SELECT encode(convert_to(q.tier_a,'UTF8'),'hex'),encode(convert_to(q.tier_b,'UTF8'),'hex'),encode(convert_to(q.label_c,'UTF8'),'hex'),q.n,q.s FROM (${statement("$1")}) q ORDER BY q.s DESC,q.tier_a,q.tier_b,q.label_c`,
            [value],
            { rowMode: "array" } as never,
          );
          const actual = after.rows().map((row) =>
              row.map((v, i) => {
                if (i === 3) {
                  if (typeof v === "bigint") return v;
                  if (typeof v !== "number" || !Number.isSafeInteger(v))
                    throw Error("Expected exact int64 count");
                  return BigInt(v);
                }
                if (i >= 3 || v === null) return v;
                if (typeof v !== "string") throw Error("Expected TEXT group key");
                return Buffer.from(v).toString("hex");
              }),
            ),
            expected = reference.rows.map((row) =>
              row.map((v, i) => (i === 3 ? BigInt(v as string) : v)),
            );
          expect(actual).toEqual(expected);
          reach.reach("PGlite exact ordered bags");
          reach.reach("same native bits");
          reach.reach("lookup and proof route reached");
          reach.reach(value === null ? "NULL binding" : "string binding");
          if (value === "") reach.reach("empty binding");
          if (value?.includes("λ") || value?.includes("\ufeff")) reach.reach("Unicode BOM binding");
          if (value?.length === 1024) reach.reach("1024-unit boundary");
        } finally {
          await o.close();
          expect(o.core.harness.governor.usedBytes).toBe(0);
          reach.reach("zero final credits");
        }
      }),
      {
        seed,
        numRuns: 12,
        examples: [
          ["t1", false],
          [null, true],
          ["", false],
          ["λ", true],
          ["\ufefft1", false],
          ["x".repeat(1024), true],
        ],
      },
    );
    reach.assertReached();
  },
  180000,
);
it.each(seedsFor("b9-bound-text-demand", [0xb9e73, 0xb9e74]))(
  "invalid or over-bound TEXT keeps old binding demand seed %i",
  async (seed) => {
    const o = await owner(),
      reach = createReach("B9 TEXT binding demand", B9_STAR_TEXT_DEMAND_REACH);
    try {
      await fixture(o);
      const sql = statement("$1");
      const result = async (fast: boolean, params: readonly WireValue[]) => {
        try {
          return { bits: bits(await o.query(fast, sql, params)) };
        } catch (error) {
          return {
            error: error instanceof Error ? error.message : String(error),
            code: (error as { code?: unknown }).code,
          };
        }
      };
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("missing", "NUL", "bool", "number", "overbound", "extra"),
          fc.integer({ min: 0, max: 64 }),
          fc.boolean(),
          async (mode, width, flag) => {
            reach.beginHistory();
            const params: WireValue[] =
              mode === "missing"
                ? []
                : mode === "NUL"
                  ? ["λ".repeat(width) + "\0tail"]
                  : mode === "bool"
                    ? [flag]
                    : mode === "number"
                      ? [width]
                      : mode === "extra"
                        ? ["t1", "t0"]
                        : ["x".repeat(1025 + width)];
            const before = await result(false, params),
              proofs = proofCounter(o),
              replay = vi.spyOn(StarLookups.prototype, "probe");
            try {
              expect(await result(true, params)).toEqual(before);
              expect(proofs.count).toBe(0);
              expect(replay).not.toHaveBeenCalled();
            } finally {
              replay.mockRestore();
              proofs.close();
            }
            reach.reach(mode);
            reach.reach("old binding outcome");
            reach.reach("no optional proof demand");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            ["missing", 0, false],
            ["NUL", 0, false],
            ["bool", 0, false],
            ["number", 0, false],
            ["overbound", 0, false],
            ["extra", 0, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await o.close();
      expect(o.core.harness.governor.usedBytes).toBe(0);
    }
  },
  120000,
);

/** The writer fixture keeps its original governor; query-only caps isolate the
 * optional strategy decision. Recalibrate after real batch-size changes, without
 * replacing a budget or allocation. Native32/64MiB checks remain separate. */
it("a selected resident star includes bound TEXT in its complete cap decision", async () => {
  const large = await owner(),
    smallOwners: Owner[] = [];
  // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with .call(this).
  const reserve = QueryRuntime.prototype.reserve;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with .call(this).
  const minimum = QueryRuntime.prototype.minimumBytes;
  type Entries = Parameters<QueryRuntime["reserve"]>[0];
  const hasBook = (entries: Entries) =>
    entries.some(
      (e) => e.rel.op === "project" && e.rel.profileAs?.op === "aggregate" && e.minimum > 0,
    );
  const observation: {
    current: {
      complete: number;
      bytes: number;
      largestSlot: number;
      slotBytes: readonly number[];
    } | null;
  } = { current: null };
  try {
    await fixture(large);
    const sql = statement("$1"),
      params = ["x".repeat(1024)],
      expected = bits(await large.query(false, sql, params));
    const capture = vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(async function (
      this: QueryRuntime,
      entries,
    ) {
      if (hasBook(entries)) {
        const input = await prepareParameters(this.plan, this.request.parameters, this.signal);
        observation.current = {
          complete: minimum.call(
            this,
            entries,
            input.bytes,
            input.largestSlot,
            input.slotBytes ?? [],
          ),
          bytes: input.bytes,
          largestSlot: input.largestSlot,
          slotBytes: input.slotBytes ?? [],
        };
      }
      return reserve.call(this, entries);
    });
    try {
      expect(bits(await large.query(true, sql, params))).toEqual(expected);
    } finally {
      capture.mockRestore();
    }
    const input = observation.current;
    if (input === null) throw Error("No actual resident admission witness");
    let cap = input.complete - 1,
      witnessed = false;
    const violations: number[] = [];
    for (let step = 0; step < 12 && !witnessed; step++) {
      const governor = new Governor(cap),
        small = await owner(large.core, governor);
      smallOwners.push(small);
      const sizing: { current: { complete: number; without: number } | null } = { current: null };
      const sizes = vi.spyOn(QueryRuntime.prototype, "minimumBytes").mockImplementation(function (
        this: QueryRuntime,
        entries,
        ...args
      ) {
        const value = minimum.call(this, entries, ...args);
        if (this.request.governor === governor && hasBook(entries)) {
          sizing.current = {
            complete: minimum.call(this, entries, input.bytes, input.largestSlot, input.slotBytes),
            without: minimum.call(this, entries),
          };
        }
        return value;
      });
      const selected = vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(function (
        this: QueryRuntime,
        entries,
      ) {
        expect(this.request.governor).toBe(governor);
        if (hasBook(entries)) {
          const complete = minimum.call(
            this,
            entries,
            input.bytes,
            input.largestSlot,
            input.slotBytes,
          );
          if (complete > governor.capBytes) violations.push(complete);
        }
        return reserve.call(this, entries);
      });
      try {
        let answer: ClientResult | null = null;
        try {
          answer = await small.query(true, sql, params);
        } catch (error) {
          expect(error).toMatchObject({ code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" });
        }
        if (answer !== null) expect(bits(answer)).toEqual(expected);
      } finally {
        selected.mockRestore();
        sizes.mockRestore();
      }
      const sized = sizing.current;
      if (sized === null) throw Error("Missing optional strategy sizing attempt");
      witnessed = sized.without <= cap && sized.complete > cap;
      if (!witnessed) {
        expect(sized.complete - 1).toBeLessThan(cap);
        cap = sized.complete - 1;
      }
    }
    expect(witnessed, "a real cap separates operator-only and complete parameter minima").toBe(
      true,
    );
    expect(violations, "optional resident minimum includes all parameters").toEqual([]);
  } finally {
    for (const small of smallOwners.reverse()) {
      await small.close();
      expect(small.governor.usedBytes).toBe(0);
      expect(small.governor.peakBytes).toBeLessThanOrEqual(small.governor.capBytes);
    }
    await large.close();
    expect(large.core.harness.governor.usedBytes).toBe(0);
  }
}, 60000);

it("one cached ordinary-owner plan rechecks each normalized TEXT binding", async () => {
  const cached = await owner(undefined, undefined, 1 << 20);
  let control: Owner | null = null;
  try {
    await fixture(cached);
    control = await owner(cached.core);
    for (const value of ["t1", "λ", "\ufefft1", "", "x".repeat(1024), "x".repeat(1025), "t1"]) {
      const sql = statement(quoted(value)),
        before = await control.query(false, sql, []),
        replay = vi.spyOn(StarLookups.prototype, "probe"),
        proofs = proofCounter(cached);
      try {
        expect(bits(await cached.query(true, sql, []))).toEqual(bits(before));
        expect(replay).toHaveBeenCalledTimes(value.length <= 1024 ? 1 : 0);
        expect(proofs.count > 0).toBe(value.length <= 1024);
      } finally {
        replay.mockRestore();
        proofs.close();
      }
    }
    expect(cached.prepares, "one physical cached plan handles all literal values").toBe(1);
  } finally {
    await control?.close();
    await cached.close();
    expect(cached.core.harness.governor.usedBytes).toBe(0);
  }
}, 60000);

it.each(seedsFor("b9-star-text-scope", [0xb9e75, 0xb9e76]))(
  "bound TEXT keeps unsupported dictionary and ancestor families generic seed %i",
  async (seed) => {
    const o = await owner(),
      reach = createReach("B9 bound TEXT scope", B9_STAR_TEXT_SCOPE_REACH);
    try {
      await fixture(o);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(
            "one join",
            "two joins",
            "AVG",
            "COUNT only",
            "parameter LIMIT",
            "computed sort",
          ),
          async (mode) => {
            reach.beginHistory();
            const sql =
              mode === "one join"
                ? "SELECT a.tier AS tier_a,COUNT(*) AS n,SUM(f.v) AS s FROM sf f JOIN sa a ON a.k=f.a WHERE a.tier=$1 GROUP BY a.tier ORDER BY s DESC,tier_a LIMIT 50"
                : mode === "two joins"
                  ? "SELECT a.tier AS tier_a,b.tier AS tier_b,COUNT(*) AS n,SUM(f.v) AS s FROM sf f JOIN sa a ON a.k=f.a JOIN sb b ON b.k=f.b WHERE b.tier=$1 GROUP BY a.tier,b.tier ORDER BY s DESC,tier_a,tier_b LIMIT 50"
                  : mode === "AVG"
                    ? statement("$1").replace("SUM(f.v)", "AVG(f.v)")
                    : mode === "COUNT only"
                      ? statement("$1")
                          .replace(",SUM(f.v) AS s", "")
                          .replace("ORDER BY s DESC,", "ORDER BY n DESC,")
                      : mode === "parameter LIMIT"
                        ? statement("$1").replace("LIMIT 50", "LIMIT $2")
                        : statement("$1").replace("ORDER BY s DESC,", "ORDER BY SUM(f.v)+1 DESC,");
            const params: WireValue[] = mode === "parameter LIMIT" ? ["t1", 50] : ["t1"],
              before = await o.query(false, sql, params),
              proofs = proofCounter(o),
              replay = vi.spyOn(StarLookups.prototype, "probe");
            try {
              expect(bits(await o.query(true, sql, params))).toEqual(bits(before));
              expect(
                proofs.count,
                "unsupported bound family must not open optional proof readers",
              ).toBe(0);
              expect(replay).not.toHaveBeenCalled();
            } finally {
              proofs.close();
              replay.mockRestore();
            }
            reach.reach(mode);
            reach.reach("exact native fallback");
            reach.reach("no optional proof demand");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            ["one join"],
            ["two joins"],
            ["AVG"],
            ["COUNT only"],
            ["parameter LIMIT"],
            ["computed sort"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await o.close();
      expect(o.core.harness.governor.usedBytes).toBe(0);
    }
  },
  120000,
);
