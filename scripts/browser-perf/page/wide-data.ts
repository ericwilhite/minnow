/**
 * The deterministic wide fact table (`events_wide`) and its three small dimensions.
 *
 * Every value is a pure function of (row, column, total rows), so each engine receives exactly
 * the same data. This is the generator measurements.md used; changing any value here changes
 * what every cached database holds, so bump GENERATOR_VERSION in ../catalogue.ts with it.
 */
export function mix(a: number, b: number): number {
  let h = Math.imul(a ^ 0x5bd1e995, 0x9e3779b1) ^ Math.imul(b + 0x7f4a7c15, 0x85ebca77);
  h ^= h >>> 16;
  h = Math.imul(h, 0x7feb352d);
  h ^= h >>> 15;
  h = Math.imul(h, 0x846ca68b);
  h ^= h >>> 16;
  return h >>> 0;
}

const U = 4294967296;
const WORDS =
  "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee zulu order ship return refund cart price stock store item user click view page search filter sort report daily weekly monthly basket promo coupon delivery invoice account login logout signup profile review rating cancel update mobile desktop tablet browser region warehouse supplier".split(
    " ",
  );
const DCARDS = [5, 10, 20, 50, 100, 200, 500, 1000];
const BASE_TS = 1704067200000; // 2024-01-01T00:00:00Z
export const DAY_MS = 86_400_000;

const word = (index: number): string => WORDS[index & 63] ?? "";

interface FamilyCounts {
  d: number;
  h: number;
  i: number;
  f: number;
  n: number;
  b: number;
  x: number;
}

function counts(columns: 200 | 500): FamilyCounts {
  return columns === 200
    ? { d: 20, h: 10, i: 40, f: 40, n: 20, b: 20, x: 47 }
    : { d: 50, h: 25, i: 100, f: 100, n: 50, b: 50, x: 122 };
}

export type WideFamily = "id" | "ts" | "day" | "d" | "h" | "i" | "f" | "n" | "b" | "x";

export interface WideColumn {
  readonly name: string;
  readonly family: WideFamily;
  readonly k: number;
  /** For the nullable mixed family: 0 INTEGER, 1 DOUBLE, 2 TEXT, 3 BOOLEAN. */
  readonly mixed: number;
  /** Declared types: Minnow (PostgreSQL), DuckDB, SQLite. */
  readonly minnow: string;
  readonly duckdb: string;
  readonly sqlite: string;
}

const MIXED_TYPES: ReadonlyArray<readonly [string, string, string]> = [
  ["INTEGER", "INTEGER", "INTEGER"],
  ["DOUBLE PRECISION", "DOUBLE", "REAL"],
  ["TEXT", "VARCHAR", "TEXT"],
  ["BOOLEAN", "BOOLEAN", "INTEGER"],
];

export function wideSpec(columns: 200 | 500): WideColumn[] {
  const c = counts(columns);
  const pad = (k: number): string => (k < 100 ? String(k).padStart(2, "0") : String(k));
  const spec: WideColumn[] = [];
  const add = (
    name: string,
    family: WideFamily,
    k: number,
    types: readonly [string, string, string],
    mixed = -1,
  ): void => {
    spec.push({ name, family, k, mixed, minnow: types[0], duckdb: types[1], sqlite: types[2] });
  };
  add("id", "id", 0, ["BIGINT PRIMARY KEY", "BIGINT PRIMARY KEY", "INTEGER PRIMARY KEY"]);
  add("ts", "ts", 0, ["TIMESTAMP NOT NULL", "TIMESTAMP NOT NULL", "INTEGER NOT NULL"]);
  add("day", "day", 0, ["DATE NOT NULL", "DATE NOT NULL", "TEXT NOT NULL"]);
  const text = ["TEXT NOT NULL", "VARCHAR NOT NULL", "TEXT NOT NULL"] as const;
  for (let k = 0; k < c.d; k++) add(`d${pad(k)}`, "d", k, text);
  for (let k = 0; k < c.h; k++) add(`h${pad(k)}`, "h", k, text);
  const int = ["INTEGER NOT NULL", "INTEGER NOT NULL", "INTEGER NOT NULL"] as const;
  for (let k = 0; k < c.i; k++) add(`i${pad(k)}`, "i", k, int);
  const dbl = ["DOUBLE PRECISION NOT NULL", "DOUBLE NOT NULL", "REAL NOT NULL"] as const;
  for (let k = 0; k < c.f; k++) add(`f${pad(k)}`, "f", k, dbl);
  const num = ["NUMERIC(12,2) NOT NULL", "DECIMAL(12,2) NOT NULL", "NUMERIC NOT NULL"] as const;
  for (let k = 0; k < c.n; k++) add(`n${pad(k)}`, "n", k, num);
  const bool = ["BOOLEAN NOT NULL", "BOOLEAN NOT NULL", "INTEGER NOT NULL"] as const;
  for (let k = 0; k < c.b; k++) add(`b${pad(k)}`, "b", k, bool);
  for (let k = 0; k < c.x; k++) {
    const mixed = k % 4;
    add(`x${pad(k)}`, "x", k, MIXED_TYPES[mixed] ?? ["INTEGER", "INTEGER", "INTEGER"], mixed);
  }
  if (spec.length !== columns) throw new Error(`wide spec has ${String(spec.length)} columns`);
  return spec;
}

function uuid(i: number, salt: number): string {
  const a = mix(i, salt);
  const b = mix(i, salt + 1);
  const c = mix(i, salt + 2);
  const d = mix(i, salt + 3);
  const h = (v: number, n: number): string => v.toString(16).padStart(8, "0").slice(0, n);
  return `${h(a, 8)}-${h(b, 4)}-4${h(b >>> 16, 3)}-${h(c, 4)}-${h(c >>> 8, 4)}${h(d, 8)}`;
}

function freeText(i: number, salt: number, minLength: number, maxLength: number): string {
  const target = minLength + (mix(i, salt) % (maxLength - minLength + 1));
  let s = "";
  let j = 1;
  while (s.length < target) s += (s ? " " : "") + word(mix(i, salt + j++));
  return s.slice(0, target);
}

export const tsOf = (i: number): number => BASE_TS + i * 30000 + (mix(i, 1) % 30000);
const dayString = (ms: number): string => new Date(ms - (ms % DAY_MS)).toISOString().slice(0, 10);

export type WideValue = number | string | boolean | null;

/** The value of `column` at 0-based row `i` of a `rows`-row table. NUMERIC is decimal text. */
export function wideValue(column: WideColumn, i: number, rows: number): WideValue {
  const k = column.k;
  switch (column.family) {
    case "id":
      return i + 1;
    case "ts":
      return tsOf(i);
    case "day":
      return dayString(tsOf(i));
    case "d": {
      const card = DCARDS[k % 8] ?? 5;
      const u = mix(i, 10 + k);
      const v = k % 3 === 0 ? Math.floor((u / U) * (u / U) * card) : u % card;
      return `d${String(k)}_${String(v)}`;
    }
    case "h": {
      const salt = 200 + k * 10;
      switch (k % 10) {
        case 0:
          return uuid(i, salt);
        case 1:
          return `user${String(mix(i, salt) % 800000)}@mail${String(mix(i, salt + 1) % 50)}.example.com`;
        case 2:
          return freeText(i, salt, 20, 200);
        case 3:
          return uuid(i >>> 2, salt);
        case 4:
          return `/c/${word(mix(i, salt))}/${String(mix(i, salt + 1) % 100000)}?ref=${word(mix(i, salt + 2))}`;
        case 5:
          return `acct-${String(mix(i, salt) % Math.max(1, Math.floor(rows / 2)))}`;
        case 6: {
          const u = mix(i, salt);
          return `10.${String(u >>> 24)}.${String((u >>> 16) & 255)}.${String((u >>> 8) & 255)}`;
        }
        case 7:
          return uuid(mix(i, salt) % rows, 200); // some row's h00
        case 8:
          return freeText(i, salt, 20, 60);
        default:
          return `SKU-${String(mix(i, salt) % 200000)}`;
      }
    }
    case "i": {
      const u = mix(i, 1000 + k);
      if (k === 20) return (u % rows) + 1; // foreign key to id
      switch (k % 10) {
        case 0:
          return u % 100;
        case 1:
          return u % 1000;
        case 2:
          return u % 1000000;
        case 3:
          return Math.floor(i / 100);
        case 4:
          return (u % 2000000001) - 1000000000;
        case 5:
          return u % 10;
        case 6:
          return u % 65536;
        case 7:
          return u % 1000000;
        case 8:
          return (u % 10000) - 5000;
        default:
          return u % 1000;
      }
    }
    case "f": {
      const u = mix(i, 2000 + k);
      switch (k % 4) {
        case 0:
          return (u % 1000000) / 1000;
        case 1:
          return ((u % 10000) + (mix(i, 2500 + k) % 10000)) / 100 - 100;
        case 2:
          return (u / U) * 1e6;
        default:
          return (u % 100000) / 100;
      }
    }
    case "n": {
      const u = mix(i, 3000 + k);
      let cents = u % (k % 2 ? 100000000 : 1000000);
      if (k % 5 === 4) cents -= k % 2 ? 50000000 : 500000;
      const a = Math.abs(cents);
      return `${cents < 0 ? "-" : ""}${String(Math.floor(a / 100))}.${String(a % 100).padStart(2, "0")}`;
    }
    case "b":
      return mix(i, 4000 + k) / U < ((k % 5) + 1) / 6;
    case "x": {
      const rate = 0.1 + (0.8 * ((k * 7) % 9)) / 8;
      if (mix(i, 5000 + k) / U < rate) return null;
      const u = mix(i, 6000 + k);
      switch (column.mixed) {
        case 0:
          return u % 100000;
        case 1:
          return (u % 1000000) / 100;
        case 2:
          return `x${String(k)}_${String(u % 50)}`;
        default:
          return (u & 1) === 1;
      }
    }
  }
}

/** Rows [start, start + count) as one array per column. */
export function wideBatch(
  spec: readonly WideColumn[],
  start: number,
  count: number,
  rows: number,
): Record<string, WideValue[]> {
  const out: Record<string, WideValue[]> = {};
  for (const column of spec) {
    const values = new Array<WideValue>(count);
    for (let j = 0; j < count; j++) values[j] = wideValue(column, start + j, rows);
    out[column.name] = values;
  }
  return out;
}

export interface DimensionRow {
  readonly key: string;
  readonly label: string;
  readonly tier: string;
  readonly weight: number;
  readonly active: boolean;
}

/** Small dimension tables joined to the fact on d03 (50 rows), d04 (100) and d07 (1,000). */
export function dimensionTables(): Array<{ name: string; rows: DimensionRow[] }> {
  const make = (k: number, cardinality: number): { name: string; rows: DimensionRow[] } => {
    const rows: DimensionRow[] = [];
    for (let v = 0; v < cardinality; v++) {
      rows.push({
        key: `d${String(k)}_${String(v)}`,
        label: `${word(v)}-${String(v)}`,
        tier: `t${String(v % 4)}`,
        weight: (v % 17) / 4,
        active: v % 7 !== 0,
      });
    }
    return { name: `dim_d${String(k).padStart(2, "0")}`, rows };
  };
  return [make(3, 50), make(4, 100), make(7, 1000)];
}
