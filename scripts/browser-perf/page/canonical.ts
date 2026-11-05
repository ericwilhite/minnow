/**
 * One canonical form for a result value from any engine, so results can be hashed and compared.
 *
 * Numbers keep 10 significant digits (integers below 1e9 stay exact), DATE and TIMESTAMP become
 * epoch milliseconds, booleans become 1/0, and NUMERIC text becomes a number. That absorbs the
 * representation differences between Minnow (decimal text, ISO dates), DuckDB (Arrow numbers,
 * Dates) and SQLite (REAL, 0/1, ISO text) without hiding a real difference in value.
 */
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;
const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}(:?\d{2})?)?$/;
const DECIMAL = /^-?\d+\.\d+$/;

function canonicalNumber(value: number): string {
  if (Number.isInteger(value) && Math.abs(value) < 1e9) return String(value);
  if (!Number.isFinite(value)) return String(value);
  return String(parseFloat(value.toPrecision(10)));
}

export function canonicalValue(value: unknown): string {
  if (value === null || value === undefined) return "N";
  switch (typeof value) {
    case "number":
      return canonicalNumber(value);
    case "bigint":
      return canonicalNumber(Number(value));
    case "boolean":
      return value ? "1" : "0";
    case "string": {
      if (DECIMAL.test(value)) return canonicalNumber(Number(value));
      if (ISO_DATE.test(value)) return canonicalNumber(Date.parse(`${value}T00:00:00Z`));
      if (ISO_TIMESTAMP.test(value)) {
        const iso =
          value.includes("T") || value.includes("Z") ? value : `${value.replace(" ", "T")}Z`;
        const time = Date.parse(iso);
        if (!Number.isNaN(time)) return canonicalNumber(time);
      }
      return `s${value}`;
    }
    default:
      if (value instanceof Date) return canonicalNumber(value.getTime());
      return `?${String(value)}`;
  }
}

/** The raw comparable value kept in a result dump: numbers stay numbers for tolerance checks. */
export function comparableValue(value: unknown): number | string {
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  if (value instanceof Date) return value.getTime();
  if (typeof value === "string") {
    if (DECIMAL.test(value)) return Number(value);
    if (ISO_DATE.test(value)) return Date.parse(`${value}T00:00:00Z`);
  }
  return canonicalValue(value);
}

function fnv(text: string, seed = 0x811c9dc5): number {
  let h = seed;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export interface ResultView {
  readonly rows: number;
  readonly columns: number;
  get(row: number, column: number): unknown;
}

/** Result rows up to this count are kept in the record, for tolerance comparison and review. */
export const DUMP_ROW_LIMIT = 2_000;

export interface ResultDigest {
  readonly hash: string;
  readonly rows: number;
  readonly columns: number;
  readonly dump?: Array<Array<number | string>>;
}

/** Two 32-bit FNV lanes: order-sensitive when the ORDER BY is total, a multiset hash otherwise. */
export function digestResult(result: ResultView, ordered: boolean): ResultDigest {
  let a = 0;
  let b = 0;
  for (let r = 0; r < result.rows; r++) {
    let line = "";
    for (let c = 0; c < result.columns; c++) line += `${canonicalValue(result.get(r, c))}\u001f`;
    const h1 = fnv(line);
    const h2 = fnv(line, 0x12345678);
    if (ordered) {
      a = (Math.imul(a ^ h1, 0x9e3779b1) + r) >>> 0;
      b = (Math.imul(b ^ h2, 0x85ebca77) + r) >>> 0;
    } else {
      a = (a + h1) >>> 0;
      b = (b + h2) >>> 0;
    }
  }
  const hash = a.toString(16).padStart(8, "0") + b.toString(16).padStart(8, "0");
  if (result.rows > DUMP_ROW_LIMIT) return { hash, rows: result.rows, columns: result.columns };
  const dump: Array<Array<number | string>> = [];
  for (let r = 0; r < result.rows; r++) {
    const row: Array<number | string> = [];
    for (let c = 0; c < result.columns; c++) row.push(comparableValue(result.get(r, c)));
    dump.push(row);
  }
  return { hash, rows: result.rows, columns: result.columns, dump };
}
