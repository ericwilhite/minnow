/** SQL-level evidence for one shared JSON/HybridPhase retaining-parent controller. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  RETAINING_CONSUMER_REACH,
  RETAINING_AGGREGATE_REACH,
  RETAINING_ERROR_REACH,
} from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { HybridAggregateOperator } from "../exec/agg/hybrid.js";
import { QueryRuntime } from "../exec/run.js";
import { ExceptOperator } from "../exec/except.js";
import type { Rel } from "../ir/ir.js";
import { JsonPhaseOperator } from "../exec/json-phase.js";
import { HashJoin } from "../exec/join.js";
import { WindowOperator } from "../exec/window.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
let core: StorageCore, pg: PGlite;
beforeAll(async () => {
  core = await new StorageCore().open();
  pg = new PGlite();
  await pg.waitReady;
  for (const ddl of [
    "CREATE TABLE t(k INTEGER,doc TEXT,x DOUBLE PRECISION)",
    "CREATE TABLE u(k INTEGER)",
  ]) {
    await core.execute(ddl);
    await pg.exec(ddl);
  }
  await core.writeBatch(
    "insertBatch",
    "u",
    [0, 0, 1, 4, null].map((k) => ({ k })),
  );
  await pg.exec("INSERT INTO u VALUES (0),(0),(1),(4),(NULL)");
  const rows = Array.from({ length: 6 }, (_, i) => ({
    k: i === 5 ? null : i % 3,
    doc: `1e${String(6000 + i * 2000)}`,
    x: i === 0 ? 1e16 : i === 1 ? 1 : i === 2 ? -1e16 : i,
  }));
  await core.writeBatch("insertBatch", "t", rows);
  for (const row of rows)
    await pg.query("INSERT INTO t VALUES ($1,$2,$3)", [row.k, row.doc, row.x]);
  await core.execute("CREATE TABLE agg(k INTEGER,v TEXT)");
  await pg.exec("CREATE TABLE agg(k INTEGER,v TEXT)");
  const aggregateRows = Array.from({ length: 8192 }, (_, i) => ({
    k: i % 4,
    v: String(i) + "x".repeat(240),
  }));
  await core.writeBatch("insertBatch", "agg", aggregateRows);
  for (let from = 0; from < aggregateRows.length; from += 256) {
    const batch = aggregateRows.slice(from, from + 256);
    await pg.query(
      `INSERT INTO agg VALUES ${batch.map((_, i) => `($${String(i * 2 + 1)},$${String(i * 2 + 2)})`).join(",")}`,
      batch.flatMap((r) => [r.k, r.v]),
    );
  }
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});

/** IR right is the build input; observe actual lowered placement, not SQL text order. */
function cutJoinSides(root: Rel, id: number): Set<string> {
  const sides = new Set<string>();
  const visit = (rel: Rel): boolean => {
    if (rel.id === id) return true;
    if ("left" in rel) {
      const left = visit(rel.left),
        right = visit(rel.right);
      if (left) sides.add("actual probe");
      if (right) sides.add("actual build");
      return left || right;
    }
    if ("input" in rel) return visit(rel.input);
    if ("inputs" in rel) {
      let found = false;
      for (const child of rel.inputs) found = visit(child) || found;
      return found;
    }
    return false;
  };
  expect(visit(root), "handoff relation remains in the prepared physical plan").toBe(true);
  return sides;
}

/** Keep the SQL itself as the PG authority, including semi/anti/three-valued mark output. */
function joinSql(
  kind: string,
  build: boolean,
  source: string,
  other: string,
  value: string,
  otherJson = false,
): string {
  const guard = source.includes("count(DISTINCT") ? "a.n>=0" : "a.j IS NOT NULL";
  const otherGuard = otherJson ? " AND b.j IS NOT NULL" : "";
  const left = build ? other + " b" : source + " a",
    right = build ? source + " a" : other + " b";
  const projection = build ? "b.k" + (otherJson ? ",b.j::text j" : "") : "a.k," + value;
  const probe = build ? other + " b" : source + " a",
    sub = build ? source + " a" : other + " b";
  const probeKey = build ? "b.k" : "a.k",
    subKey = build ? "a.k" : "b.k";
  const predicate = "a.k=b.k" + (build ? " AND " + guard : otherGuard);
  const order = build
    ? "b.k NULLS FIRST" + (otherJson ? ",j" : "")
    : "a.k NULLS FIRST" + (value.includes("::text") ? ",j" : "");
  if (kind === "semi" || kind === "anti")
    return `SELECT ${projection} FROM ${probe} WHERE ${kind === "anti" ? "NOT " : ""}EXISTS(SELECT 1 FROM ${sub} WHERE ${predicate}) ORDER BY ${order}`;
  if (kind === "mark")
    return `SELECT ${projection},${probeKey} IN(SELECT ${subKey} FROM ${sub} WHERE ${build ? guard : otherJson ? "b.j IS NOT NULL" : "TRUE"}) m FROM ${probe} ORDER BY ${order}`;
  return `SELECT a.k,${value},b.k b${otherJson ? ",b.j::text j" : ""} FROM ${left} ${kind} JOIN ${right} ${kind === "CROSS" ? "" : "ON a.k=b.k"} ORDER BY a.k NULLS FIRST,b.k NULLS FIRST${value.includes("::text") || otherJson ? ",j" : ""}`;
}

it.each(seedsFor("next-retaining-consumer", [0xb6a0, 0xb6a1]))(
  "preserves JSON cuts under SQL joins and windows (seed %i)",
  async (seed) => {
    const reach = createReach<string>("retaining consumer", RETAINING_CONSUMER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(
          "INNER",
          "LEFT",
          "RIGHT",
          "FULL",
          "CROSS",
          "semi",
          "anti",
          "mark",
          "window",
          "descendant window",
          "descendant join",
        ),
        fc.boolean(),
        async (kind, build) => {
          reach.beginHistory();
          const producer = "(SELECT k,doc::jsonb AS j,x FROM t)";
          const descendant = kind.startsWith("descendant");
          const sql =
            kind === "descendant window"
              ? "SELECT k,'1e12000'::jsonb::text j FROM(SELECT k,rank() OVER(ORDER BY k) r FROM u) w WHERE r>0 ORDER BY k NULLS FIRST"
              : kind === "descendant join"
                ? "SELECT a.k,'1e12000'::jsonb::text j FROM u a FULL JOIN u b ON a.k=b.k ORDER BY a.k NULLS FIRST"
                : kind === "window"
                  ? `SELECT k,j::text AS j,rank() OVER(ORDER BY k NULLS FIRST) AS r FROM ${producer} a ORDER BY k NULLS FIRST,j`
                  : joinSql(kind, build, producer, "u", "a.j::text j");
          let preparedRoot: Rel | undefined;
          const actualSides = new Set<string>();
          const prepareNative = core.executor.prepare.bind(core.executor);
          const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
            const result = prepareNative(plan);
            preparedRoot = result.plan.root;
            return result;
          });
          const contexts = new WeakSet(),
            handoffs: Array<() => void> = [];
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the JSON producer
          const jsonPush = JsonPhaseOperator.prototype.push;
          const observedJSON = vi
            .spyOn(JsonPhaseOperator.prototype, "push")
            .mockImplementation(function (this: JsonPhaseOperator, ...args) {
              if (!contexts.has(this.shared)) {
                contexts.add(this.shared);
                const original = this.shared.handoff;
                const spy = vi.spyOn(this.shared, "handoff").mockImplementation((phase) => {
                  const match = /(?:project|filter)#(\d+)/.exec(this.shared.grant.label);
                  if (!preparedRoot || !match)
                    throw new Error("Missing prepared JSON relation identity");
                  for (const side of cutJoinSides(preparedRoot, Number(match[1])))
                    actualSides.add(side);
                  return original(phase);
                });
                handoffs.push(() => spy.mockRestore());
              }
              return jsonPush.apply(this, args);
            });
          let cuts = 0,
            parents = 0;
          const cut = vi.spyOn(QueryRuntime.prototype, "beginHandoff").mockImplementation(function (
            this: QueryRuntime,
          ) {
            cuts++;
            return originalCut.call(this);
          });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the original receiver
          const nativeJoin = HashJoin.prototype.checkpoint;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual window
          const nativeWindow = WindowOperator.prototype.checkpoint;
          const join = vi.spyOn(HashJoin.prototype, "checkpoint").mockImplementation(function (
            this: HashJoin,
          ) {
            parents++;
            return nativeJoin.call(this);
          });
          const window = vi
            .spyOn(WindowOperator.prototype, "checkpoint")
            .mockImplementation(function (this: WindowOperator) {
              parents++;
              return nativeWindow.call(this);
            });
          try {
            const expected = (await pg.query(sql)).rows;
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected rows");
            expect(oracleRows(result)).toEqual(expected);
            expect(cuts).toBeGreaterThan(0);
            if (descendant) {
              expect(parents).toBe(0);
              reach.reach("descendant excluded");
            } else expect(parents).toBeGreaterThan(0);
            reach.reach(kind === "window" ? "window" : "join");
            if (kind !== "window" && !descendant) reach.reach("kind " + kind);
            reach.reach(build ? "build" : "probe");
            for (const side of actualSides) reach.reach(side);
            if (kind !== "window" && !descendant) expect(actualSides.size).toBeGreaterThan(0);
            reach.reach("JSON cut");
            reach.reach("native checkpoint");
            reach.reach("PG exact");
            expect(core.spill.bytesInUse).toBe(0);
          } finally {
            join.mockRestore();
            window.mockRestore();
            cut.mockRestore();
            prepare.mockRestore();
            observedJSON.mockRestore();
            for (const restore of handoffs) restore();
          }
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          ["INNER", false],
          ["INNER", true],
          ["FULL", false],
          ["FULL", true],
          ["semi", false],
          ["semi", true],
          ["anti", false],
          ["anti", true],
          ["mark", false],
          ["mark", true],
          ["LEFT", false],
          ["LEFT", true],
          ["RIGHT", false],
          ["RIGHT", true],
          ["CROSS", false],
          ["CROSS", true],
          ["descendant window", false],
          ["descendant join", false],
          ["window", false],
          ["window", true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
// eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with its original receiver
const originalCut = QueryRuntime.prototype.beginHandoff;

it.each(seedsFor("next-retaining-aggregate-consumer", [0xb6a2, 0xb6a3]))(
  "preserves aggregate and mixed SQL phases under retaining pressure (seed %i)",
  async (seed) => {
    const reach = createReach<string>("aggregate retaining consumer", RETAINING_AGGREGATE_REACH);
    const source = "(SELECT k,count(DISTINCT v) n FROM agg GROUP BY k)";
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(
          "window",
          "join build",
          "join probe",
          "mixed window",
          "mixed build",
          "mixed probe",
          "except window",
        ),
        fc.constantFrom("INNER", "LEFT", "RIGHT", "FULL", "CROSS", "semi", "anti", "mark"),
        async (shape, kind) => {
          reach.beginHistory();
          const mixed = shape.startsWith("mixed");
          const build = shape.endsWith("build");
          const window = shape.endsWith("window");
          const json = "(SELECT k,doc::jsonb j FROM t)";
          const windowInput =
            shape === "except window"
              ? "(SELECT k,0::bigint n FROM u EXCEPT SELECT k,count(DISTINCT v) n FROM agg GROUP BY k)"
              : source;
          const sql = window
            ? mixed
              ? `SELECT k,n,j::text j,rank() OVER(ORDER BY k) r FROM (SELECT k,n,'1e12000'::jsonb j FROM ${source} z) a ORDER BY k`
              : `SELECT k,n,rank() OVER(ORDER BY k) r FROM ${windowInput} a ORDER BY k`
            : joinSql(kind, build, source, mixed ? json : "u", "a.n", mixed);
          let preparedRoot: Rel | undefined;
          const actualSides = new Set<string>();
          const governor = new Governor(128 << 20),
            pinned = await governor.admit({
              label: "retaining competitor",
              minimumBytes: 0,
              spillable: true,
            });
          let pressured = false,
            cuts = 0,
            restores = 0,
            aggregateCuts = 0,
            parents = 0;
          const idleAccounts = new Set<Grant>();
          let excepts = 0;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with its native receiver
          const nativeExcept = ExceptOperator.prototype.checkpoint;
          const ex = vi
            .spyOn(ExceptOperator.prototype, "checkpoint")
            .mockImplementation(async function (this: ExceptOperator) {
              const saved = await nativeExcept.call(this);
              excepts++;
              expect(saved.leftDone).toBe(true);
              return saved;
            });
          const observed = new WeakSet();
          const handoffs: Array<() => void> = [];
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual receiver
          const push = HybridAggregateOperator.prototype.push;
          const observer = vi
            .spyOn(HybridAggregateOperator.prototype, "push")
            .mockImplementation(function (this: HybridAggregateOperator, batch) {
              // EXISTS/IN may insert a separate tiny dedup aggregate. Apply pressure
              // specifically to the DISTINCT payload producer whose continuation is tested.
              if (!this.rel.aggregates.some((call) => call.fn === "count" && call.distinct))
                return push.call(this, batch);
              if (this.shared.handoff && !observed.has(this.shared)) {
                observed.add(this.shared);
                const original = this.shared.handoff;
                const spy = vi.spyOn(this.shared, "handoff").mockImplementation((phase) => {
                  aggregateCuts++;
                  if (!preparedRoot) throw new Error("Missing aggregate prepared plan");
                  for (const side of cutJoinSides(preparedRoot, this.rel.id)) actualSides.add(side);
                  return original(phase);
                });
                handoffs.push(() => spy.mockRestore());
              }
              if (!pressured) {
                const free = Math.max(
                  0,
                  governor.capBytes - governor.reserveBytes - governor.usedBytes,
                );
                expect(pinned.allocate(free)).toBe(true);
                pressured = true;
              }
              return push.call(this, batch);
            });
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the original receiver
          const nativeJoin = HashJoin.prototype.checkpoint;
          // eslint-disable-next-line @typescript-eslint/unbound-method -- invoked with the actual window
          const nativeWindow = WindowOperator.prototype.checkpoint;
          const join = vi.spyOn(HashJoin.prototype, "checkpoint").mockImplementation(function (
            this: HashJoin,
          ) {
            parents++;
            return nativeJoin.call(this);
          });
          const win = vi.spyOn(WindowOperator.prototype, "checkpoint").mockImplementation(function (
            this: WindowOperator,
          ) {
            parents++;
            return nativeWindow.call(this);
          });
          const original = core.executor.prepare.bind(core.executor);
          const prepared = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
            const value = original(plan);
            preparedRoot = value.plan.root;
            return { ...value, execute: (request) => value.execute({ ...request, governor }) };
          });
          const native = governor.admitGroup.bind(governor);
          governor.admitGroup = (options) => {
            if (options.idleCredit) idleAccounts.add(options.idleCredit);
            if (options.idleCredit !== undefined && pressured) {
              restores++;
              queueMicrotask(() => pinned.close());
            }
            return native(options);
          };
          const cut = vi.spyOn(QueryRuntime.prototype, "beginHandoff").mockImplementation(function (
            this: QueryRuntime,
          ) {
            cuts++;
            return originalCut.call(this);
          });
          try {
            const expected = (await pg.query(sql)).rows;
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected rows");
            expect(oracleRows(result)).toEqual(expected);
            expect(aggregateCuts).toBeGreaterThan(0);
            expect(parents).toBeGreaterThan(0);
            expect(restores).toBeGreaterThan(0);
            if (shape === "except window") {
              expect(excepts).toBeGreaterThan(0);
              reach.reach("EXCEPT left retained");
            }
            if (mixed) {
              expect(cuts).toBeGreaterThan(aggregateCuts);
              reach.reach("mixed producers");
              reach.reach("JSON cut");
            }
            expect(idleAccounts.size).toBe(1);
            expect(core.lastProfile?.operators.some((op) => op.spilledBytes > 0)).toBe(true);
            expect(core.spill.bytesInUse).toBe(0);
            expect(governor.usedBytes).toBe(0);
            reach.reach(window ? "window" : "join");
            if (!window) reach.reach("kind " + kind);
            reach.reach(build ? "build" : "probe");
            for (const side of actualSides) reach.reach(side);
            if (!window) expect(actualSides.size).toBeGreaterThan(0);
            reach.reach("aggregate cut");
            reach.reach("observed spill");
            reach.reach("shared idle account");
            reach.reach("PG exact");
            reach.reach("zero grants/files");
          } finally {
            pinned.close();
            cut.mockRestore();
            observer.mockRestore();
            prepared.mockRestore();
            join.mockRestore();
            win.mockRestore();
            ex.mockRestore();
            for (const restore of handoffs) restore();
          }
        },
      ),
      {
        seed,
        numRuns: 48,
        examples: [
          ["window", "INNER"],
          ["join build", "FULL"],
          ["join probe", "FULL"],
          ["mixed window", "INNER"],
          ["mixed build", "FULL"],
          ["mixed probe", "FULL"],
          ["except window", "INNER"],
          ["join build", "semi"],
          ["join probe", "semi"],
          ["join build", "anti"],
          ["join probe", "anti"],
          ["join build", "mark"],
          ["join probe", "mark"],
          ["join build", "LEFT"],
          ["join probe", "LEFT"],
          ["join build", "RIGHT"],
          ["join probe", "RIGHT"],
          ["join build", "CROSS"],
          ["join probe", "CROSS"],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-retaining-row-error", [0xb6a6, 0xb6a7]))(
  "keeps a saved terminal data error behind demanded join rows (seed %i)",
  async (seed) => {
    const reach = createReach<string>("retaining row error", RETAINING_ERROR_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(0, 1, 5, 6),
        fc.constantFrom("bad", "1e131072", '"\\u0000"'),
        fc.boolean(),
        async (limit, bad, late) => {
          reach.beginHistory();
          const sql = late
            ? `SELECT a.doc::jsonb::text j FROM(SELECT v.i,v.doc FROM(VALUES(0,'1e6000'),(1,'${bad}'))v(i,doc) CROSS JOIN u b ORDER BY v.i LIMIT 100) a LIMIT ${String(limit)}`
            : `SELECT a.j::text j FROM(SELECT doc::jsonb j FROM(VALUES(0,'1e6000'),(1,'${bad}'))v(i,doc) ORDER BY i LIMIT 100) a CROSS JOIN u b LIMIT ${String(limit)}`;
          reach.reach(late ? "streaming projection" : "eager barrier");
          let state: string | undefined, expected: unknown;
          try {
            expected = (await pg.query(sql)).rows;
          } catch (error) {
            state = (error as { code: string }).code;
          }
          if (state) {
            await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: state });
            reach.reach("terminal error");
            reach.reach("state " + state);
          } else {
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected rows");
            expect(oracleRows(result)).toEqual(expected);
            reach.reach(limit === 0 ? "zero demand" : "valid prefix");
          }
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("limit " + String(limit));
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, "bad", false],
          [1, "bad", true],
          [5, "bad", true],
          [6, "bad", true],
          [6, "1e131072", true],
          [6, '"\\u0000"', true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
