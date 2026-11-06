/**
 * Window functions against PostgreSQL itself (PGlite): generated specs over every function and
 * frame (ROWS / RANGE / GROUPS with offsets, every EXCLUDE), NULL keys, resident and streamed
 * partitions. The law is PostgreSQL's result per row. ROWS and GROUPS queries order by (k, id),
 * a total order, so every value must match exactly (float sums add in the same order). RANGE
 * offsets need one ORDER BY key, so those queries order by k alone and compare only functions
 * whose value does not depend on the order among peers.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import { BIG, DBL, INT, TEXT, type Row, type Value } from "./join-fixtures.js";
import type { SortColumn } from "./sort-keys.js";
import type { Frame, FrameBound } from "./window-frames.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { ArraySource, CollectSink, batchesOf } from "./join-fixtures.js";
import { drive } from "./pipeline.js";
import { streamable } from "./window-stream.js";
import {
  WindowOperator,
  windowMinimumBytes,
  type WindowFunctionSpec,
  type WindowSpec,
} from "./window.js";

async function runWindow(spec: WindowSpec, data: readonly Row[]) {
  const minimum = windowMinimumBytes(spec.types, spec.functions.length);
  const governor = new Governor(256 << 20);
  const grant = await governor.admit({ label: "window", minimumBytes: minimum, spillable: true });
  const spill = new MemorySpillService();
  const sink = new CollectSink();
  const op = new WindowOperator(spec, { grant, spill, queryId: 3 }, sink);
  await drive(new ArraySource(batchesOf(spec.types, data, { batchRows: 97 })), op);
  await op.closed;
  expect(governor.usedBytes).toBe(0);
  expect(spill.bytesInUse).toBe(0);
  return { rows: sink.rows, profile: op.profile };
}

const SEEDS = seedsFor("next-window-pglite", [0x3d2, 0x77]);
// p, k, x (float8), y (int), t (text "C"), id.
const TYPES: PhysicalType[] = [INT, INT, DBL, INT, TEXT, INT];
const NAMES = ["p", "k", "x", "y", "t", "id"];
const [P, K, X, Y, T, ID] = [0, 1, 2, 3, 4, 5];
let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});

function rows(random: () => number, n: number): Row[] {
  return Array.from({ length: n }, (_, i): Row => [
    Math.floor(random() * 4),
    random() < 0.08 ? null : Math.floor(random() * 12),
    random() < 0.1 ? null : Math.round(random() * 1e4) / 64,
    random() < 0.1 ? null : Math.floor(random() * 2000) - 1000,
    random() < 0.1 ? null : `t${String(Math.floor(random() * 30))}`,
    i,
  ]);
}

async function load(data: readonly Row[]): Promise<void> {
  await pg.exec(
    'DROP TABLE IF EXISTS w; CREATE TABLE w (p int, k int, x float8, y int, t text COLLATE "C", id int);',
  );
  for (let at = 0; at < data.length; at += 200) {
    const part = data.slice(at, at + 200);
    const values = part
      .map(
        (r) =>
          `(${r.map((v) => (v === null ? "NULL" : typeof v === "string" ? `'${v}'` : String(v))).join(",")})`,
      )
      .join(",");
    await pg.exec(`INSERT INTO w VALUES ${values}`);
  }
}

const bound = (b: FrameBound): string => {
  switch (b.kind) {
    case "unbounded-preceding":
      return "UNBOUNDED PRECEDING";
    case "unbounded-following":
      return "UNBOUNDED FOLLOWING";
    case "current-row":
      return "CURRENT ROW";
    default:
      return `${String(b.offset)} ${b.kind === "preceding" ? "PRECEDING" : "FOLLOWING"}`;
  }
};
const frameSql = (f: Frame): string =>
  `${f.unit.toUpperCase()} BETWEEN ${bound(f.start)} AND ${bound(f.end)} EXCLUDE ${f.exclude === "no-others" ? "NO OTHERS" : f.exclude === "current-row" ? "CURRENT ROW" : f.exclude.toUpperCase()}`;

/** A constant amount (the generator makes no column amounts). */
function amountSql(call: WindowFunctionSpec): string {
  const amount = call.amount;
  if (typeof amount !== "number") throw new TypeError("Only constant amounts");
  return String(amount);
}

function callSql(call: WindowFunctionSpec, over: string): string {
  const arg = call.arg === null ? "" : (NAMES[call.arg] ?? "");
  const frame = call.frame === null ? "" : ` ${frameSql(call.frame)}`;
  switch (call.fn) {
    case "row_number":
    case "rank":
    case "dense_rank":
    case "percent_rank":
    case "cume_dist":
      return `${call.fn}() OVER (${over})`;
    case "ntile":
      return `ntile(${amountSql(call)}) OVER (${over})`;
    case "lag":
    case "lead":
      return `${call.fn}(${arg}, ${call.amount === undefined || call.amount === null ? "1" : amountSql(call)}) OVER (${over})`;
    case "nth_value":
      return `nth_value(${arg}, ${amountSql(call)}) OVER (${over}${frame})`;
    case "count_star":
      return `count(*) OVER (${over}${frame})`;
    case "avg":
      // Lowering's avg of integers is double precision (C11); PostgreSQL's is numeric.
      return `avg(${arg}::float8) OVER (${over}${frame})`;
    default:
      return `${call.fn}(${arg}) OVER (${over}${frame})`;
  }
}

const KINDS = [
  "unbounded-preceding",
  "preceding",
  "current-row",
  "following",
  "unbounded-following",
] as const;

function randomFrame(random: () => number, unit: Frame["unit"]): Frame {
  let a = Math.floor(random() * 4);
  let b = 1 + Math.floor(random() * 4);
  if (a > b) [a, b] = [b, a];
  const make = (i: number): FrameBound => {
    const kind = KINDS[i] ?? "current-row";
    return {
      kind,
      offset: kind === "preceding" || kind === "following" ? Math.floor(random() * 4) : null,
    };
  };
  const excludes = ["no-others", "current-row", "group", "ties"] as const;
  return {
    unit,
    start: make(a),
    end: make(b),
    exclude: excludes[Math.floor(random() * 4)] ?? "no-others",
  };
}

function randomCall(random: () => number, range: boolean): WindowFunctionSpec {
  const pick = <V>(items: readonly V[]): V => items[Math.floor(random() * items.length)] as V;
  // Under RANGE (one key, ties free) only order-independent functions.
  const fns = range
    ? [
        "rank",
        "dense_rank",
        "percent_rank",
        "cume_dist",
        "count_star",
        "count",
        "sum",
        "min",
        "max",
      ]
    : [
        "row_number",
        "rank",
        "dense_rank",
        "percent_rank",
        "cume_dist",
        "ntile",
        "lag",
        "lead",
        "first_value",
        "last_value",
        "nth_value",
        "count_star",
        "count",
        "sum",
        "avg",
        "min",
        "max",
      ];
  const fn = pick(fns);
  const unit: Frame["unit"] = range ? "range" : random() < 0.5 ? "rows" : "groups";
  const frame = random() < 0.15 ? null : randomFrame(random, unit);
  switch (fn) {
    case "row_number":
    case "rank":
    case "dense_rank":
      return { fn, arg: null, frame: null, type: BIG };
    case "percent_rank":
    case "cume_dist":
      return { fn, arg: null, frame: null, type: DBL };
    case "ntile":
      return { fn, arg: null, amount: 1 + Math.floor(random() * 5), frame: null, type: INT };
    case "lag":
    case "lead":
      return { fn, arg: pick([Y, T]), amount: Math.floor(random() * 3), frame: null, type: INT };
    case "first_value":
    case "last_value":
    case "nth_value": {
      const arg = pick([X, T]);
      return {
        fn,
        arg,
        ...(fn === "nth_value" ? { amount: 1 + Math.floor(random() * 3) } : {}),
        frame,
        type: arg === X ? DBL : TEXT,
      };
    }
    case "count_star":
      return { fn, arg: null, frame, type: BIG };
    case "count":
      return { fn, arg: pick([X, Y, T]), frame, type: BIG };
  }
  // Under RANGE, integer arguments only: float sums would add peers in a free order.
  const arg = range ? Y : pick([X, Y]);
  const type: PhysicalType =
    fn === "avg" ? DBL : fn === "sum" && arg === Y ? BIG : arg === X ? DBL : INT;
  return { fn, arg, frame, type };
}

function fixType(call: WindowFunctionSpec): WindowFunctionSpec {
  if (call.fn === "lag" || call.fn === "lead")
    return { ...call, type: call.arg === T ? TEXT : INT };
  return call;
}

function same(a: Value, b: unknown, float: boolean): boolean {
  if (a === null || b === null || b === undefined)
    return a === null && (b === null || b === undefined);
  if (float) {
    const x = Number(a);
    const y = Number(b);
    return x === y || (Number.isNaN(x) && Number.isNaN(y));
  }
  return (
    String(a) ===
    (typeof b === "string" || typeof b === "number" || typeof b === "bigint"
      ? String(b)
      : JSON.stringify(b))
  );
}

describe("window functions vs PostgreSQL", () => {
  for (const seed of SEEDS)
    for (const streamed of [false, true])
      it(`seed ${String(seed)}${streamed ? ", streamed" : ""}`, async () => {
        const random = mulberry32(seed + (streamed ? 7 : 0));
        const data = rows(random, 400);
        await load(data);
        for (let round = 0; round < 16; round++) {
          const range = random() < 0.35;
          const direction = random() < 0.5 ? "asc" : "desc";
          const nulls = random() < 0.5 ? "first" : "last";
          const orderBy: SortColumn[] = [
            { column: K, direction, nulls, collation: null },
            ...(range
              ? []
              : [
                  {
                    column: ID,
                    direction: "asc" as const,
                    nulls: "last" as const,
                    collation: null,
                  },
                ]),
          ];
          let functions = Array.from({ length: 1 + Math.floor(random() * 3) }, () =>
            fixType(randomCall(random, range)),
          );
          if (streamed) {
            functions = functions.filter(
              (call) => streamable({ types: TYPES, orderBy, functions }, call) === null,
            );
            if (functions.length === 0) continue;
          }
          const spec: WindowSpec = {
            types: TYPES,
            partitionBy: [P],
            orderBy,
            functions,
            ...(streamed ? { streamRows: 16 } : {}),
          };
          const over = `PARTITION BY p ORDER BY ${orderBy.map((key) => `${NAMES[key.column] ?? ""} ${key.direction.toUpperCase()} NULLS ${key.nulls.toUpperCase()}`).join(", ")}`;
          const sql = `SELECT id, ${functions.map((call, f) => `${callSql(call, over)} AS f${String(f)}`).join(", ")} FROM w`;
          const expected = new Map<number, Record<string, unknown>>();
          for (const row of (await pg.query<Record<string, unknown>>(sql)).rows)
            expected.set(Number(row.id), row);
          const result = await runWindow(spec, data);
          for (const row of result.rows) {
            const want = expected.get(Number(row[ID]));
            if (want === undefined) throw new Error(`no row ${String(row[ID])}`);
            functions.forEach((call, f) => {
              const value = row[TYPES.length + f] ?? null;
              const float = call.type.kind === "float64";
              if (!same(value, want[`f${String(f)}`], float))
                expect(value, `${sql}\nrow ${String(row[ID])} f${String(f)}`).toEqual(
                  want[`f${String(f)}`],
                );
            });
          }
          if (streamed) expect(result.profile.streamedPartitions).toBeGreaterThan(0);
        }
      }, 180_000);
});
