// Deterministic generator for the wide fact table W (events_wide) and its small dims.
// Pure function of (row index, column, total rows): every engine receives identical values.
export function mix(a, b) {
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

export function counts(ncols) {
  if (ncols === 200) return { d: 20, h: 10, i: 40, f: 40, n: 20, b: 20, x: 47 };
  if (ncols === 500) return { d: 50, h: 25, i: 100, f: 100, n: 50, b: 50, x: 122 };
  throw new Error("ncols must be 200 or 500");
}
const p2 = (k) => String(k).padStart(2, "0");

/** Column spec: name, family, SQL types per engine, and a generator into a column array. */
export function wideSpec(ncols) {
  const c = counts(ncols);
  const pad = (k) => (k < 100 ? p2(k) : String(k));
  const cols = [];
  cols.push({
    name: "id",
    fam: "id",
    pg: "BIGINT PRIMARY KEY",
    duck: "BIGINT PRIMARY KEY",
    lite: "INTEGER PRIMARY KEY",
  });
  cols.push({
    name: "ts",
    fam: "ts",
    pg: "TIMESTAMP NOT NULL",
    duck: "TIMESTAMP NOT NULL",
    lite: "INTEGER NOT NULL",
  });
  cols.push({
    name: "day",
    fam: "day",
    pg: "DATE NOT NULL",
    duck: "DATE NOT NULL",
    lite: "TEXT NOT NULL",
  });
  for (let k = 0; k < c.d; k++)
    cols.push({
      name: "d" + pad(k),
      fam: "d",
      k,
      pg: "TEXT NOT NULL",
      duck: "VARCHAR NOT NULL",
      lite: "TEXT NOT NULL",
    });
  for (let k = 0; k < c.h; k++)
    cols.push({
      name: "h" + pad(k),
      fam: "h",
      k,
      pg: "TEXT NOT NULL",
      duck: "VARCHAR NOT NULL",
      lite: "TEXT NOT NULL",
    });
  for (let k = 0; k < c.i; k++)
    cols.push({
      name: "i" + pad(k),
      fam: "i",
      k,
      pg: "INTEGER NOT NULL",
      duck: "INTEGER NOT NULL",
      lite: "INTEGER NOT NULL",
    });
  for (let k = 0; k < c.f; k++)
    cols.push({
      name: "f" + pad(k),
      fam: "f",
      k,
      pg: "DOUBLE PRECISION NOT NULL",
      duck: "DOUBLE NOT NULL",
      lite: "REAL NOT NULL",
    });
  for (let k = 0; k < c.n; k++)
    cols.push({
      name: "n" + pad(k),
      fam: "n",
      k,
      pg: "NUMERIC(12,2) NOT NULL",
      duck: "DECIMAL(12,2) NOT NULL",
      lite: "NUMERIC NOT NULL",
    });
  for (let k = 0; k < c.b; k++)
    cols.push({
      name: "b" + pad(k),
      fam: "b",
      k,
      pg: "BOOLEAN NOT NULL",
      duck: "BOOLEAN NOT NULL",
      lite: "INTEGER NOT NULL",
    });
  for (let k = 0; k < c.x; k++) {
    const t = k % 4;
    const sql = [
      ["INTEGER", "INTEGER", "INTEGER"],
      ["DOUBLE PRECISION", "DOUBLE", "REAL"],
      ["TEXT", "VARCHAR", "TEXT"],
      ["BOOLEAN", "BOOLEAN", "INTEGER"],
    ][t];
    cols.push({ name: "x" + pad(k), fam: "x", k, xt: t, pg: sql[0], duck: sql[1], lite: sql[2] });
  }
  if (cols.length !== ncols) throw new Error("spec size " + cols.length);
  return cols;
}

function uuid(i, salt) {
  const a = mix(i, salt),
    b = mix(i, salt + 1),
    c = mix(i, salt + 2),
    d = mix(i, salt + 3);
  const h = (v, n) => v.toString(16).padStart(8, "0").slice(0, n);
  return `${h(a, 8)}-${h(b, 4)}-4${h(b >>> 16, 3)}-${h(c, 4)}-${h(c >>> 8, 4)}${h(d, 8)}`;
}
function text(i, salt, minLen, maxLen) {
  const target = minLen + (mix(i, salt) % (maxLen - minLen + 1));
  let s = "";
  let j = 1;
  while (s.length < target) {
    s += (s ? " " : "") + WORDS[mix(i, salt + j++) & 63];
  }
  return s.slice(0, target);
}
function dayString(ms) {
  return new Date(ms - (ms % 86400000)).toISOString().slice(0, 10);
}
export const tsOf = (i) => BASE_TS + i * 30000 + (mix(i, 1) % 30000);

/** Value of column `col` for 0-based row `i` in a table of `rows` rows. Strings for NUMERIC. */
export function value(col, i, rows) {
  switch (col.fam) {
    case "id":
      return i + 1;
    case "ts":
      return tsOf(i); // epoch ms; adapters convert
    case "day":
      return dayString(tsOf(i));
    case "d": {
      const card = DCARDS[col.k % 8];
      const u = mix(i, 10 + col.k);
      const v = col.k % 3 === 0 ? Math.floor((u / U) * (u / U) * card) : u % card;
      return `d${col.k}_${v}`;
    }
    case "h": {
      const t = col.k % 10;
      const salt = 200 + col.k * 10;
      switch (t) {
        case 0:
          return uuid(i, salt);
        case 1:
          return `user${mix(i, salt) % 800000}@mail${mix(i, salt + 1) % 50}.example.com`;
        case 2:
          return text(i, salt, 20, 200);
        case 3:
          return uuid(i >>> 2, salt);
        case 4:
          return `/c/${WORDS[mix(i, salt) & 63]}/${mix(i, salt + 1) % 100000}?ref=${WORDS[mix(i, salt + 2) & 63]}`;
        case 5:
          return `acct-${mix(i, salt) % Math.max(1, Math.floor(rows / 2))}`;
        case 6: {
          const u = mix(i, salt);
          return `10.${u >>> 24}.${(u >>> 16) & 255}.${(u >>> 8) & 255}`;
        }
        case 7:
          return uuid(mix(i, salt) % rows, 200); // references some row's h00
        case 8:
          return text(i, salt, 20, 60);
        default:
          return `SKU-${mix(i, salt) % 200000}`;
      }
    }
    case "i": {
      const u = mix(i, 1000 + col.k);
      if (col.k === 20) return (u % rows) + 1; // foreign key to id
      switch (col.k % 10) {
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
      const u = mix(i, 2000 + col.k);
      switch (col.k % 4) {
        case 0:
          return (u % 1000000) / 1000;
        case 1:
          return ((u % 10000) + (mix(i, 2500 + col.k) % 10000)) / 100 - 100;
        case 2:
          return (u / U) * 1e6;
        default:
          return (u % 100000) / 100;
      }
    }
    case "n": {
      const u = mix(i, 3000 + col.k);
      let cents = u % (col.k % 2 ? 100000000 : 1000000);
      if (col.k % 5 === 4) cents -= col.k % 2 ? 50000000 : 500000;
      const neg = cents < 0;
      const a = Math.abs(cents);
      return `${neg ? "-" : ""}${Math.floor(a / 100)}.${String(a % 100).padStart(2, "0")}`;
    }
    case "b":
      return mix(i, 4000 + col.k) / U < ((col.k % 5) + 1) / 6;
    case "x": {
      const rate = 0.1 + (0.8 * ((col.k * 7) % 9)) / 8;
      if (mix(i, 5000 + col.k) / U < rate) return null;
      const u = mix(i, 6000 + col.k);
      switch (col.xt) {
        case 0:
          return u % 100000;
        case 1:
          return (u % 1000000) / 100;
        case 2:
          return `x${col.k}_${u % 50}`;
        default:
          return (u & 1) === 1;
      }
    }
  }
  throw new Error("bad col " + col.name);
}

/** Columnar batch: { name: Array } for rows [start, start+n). */
export function wideBatch(spec, start, n, rows) {
  const out = {};
  for (const col of spec) {
    const a = new Array(n);
    for (let j = 0; j < n; j++) a[j] = value(col, start + j, rows);
    out[col.name] = a;
  }
  return out;
}

/** Small dimension tables joined to W on d03 (50), d04 (100), d07 (1000). */
export function dimTables() {
  const mk = (k, card) => {
    const rows = [];
    for (let v = 0; v < card; v++)
      rows.push({
        key: `d${k}_${v}`,
        label: `${WORDS[v & 63]}-${v}`,
        tier: `t${v % 4}`,
        weight: (v % 17) / 4,
        active: v % 7 !== 0,
      });
    return { name: `dim_d${p2(k)}`, rows };
  };
  return [mk(3, 50), mk(4, 100), mk(7, 1000)];
}
