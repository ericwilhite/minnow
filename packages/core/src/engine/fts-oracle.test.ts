/**
 * Full-text oracle. `MATCH ... AGAINST` over one column, two columns and `*`, with AND terms,
 * prefixes and an extra predicate, runs against a keyed table and an append-only table through
 * inserts, updates, deletes, rekeys, upserts, a MATCH-predicated UPDATE, folds (which stop a keyed
 * table's index, so it scans from then on), index rebuilds, reopens and ADD/DROP COLUMN. Every
 * answer must equal a JavaScript token model; `explain` confirms the append-only table's index
 * stays ready through folds.
 *
 * `MINNOW_SEED=<n>` replays one seed; `MINNOW_ORACLE_STEPS` widens a run.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/** A map entry the model guarantees is present. */
function must<T>(value: T | undefined, what: string): T {
  if (value === undefined) throw new Error(`${what} is missing`);
  return value;
}

const int = (r: () => number, n: number): number => Math.floor(r() * n);
const pick = <T>(r: () => number, xs: readonly T[]): T => xs[int(r, xs.length)] as T;
const VOCAB = [
  "alpha",
  "alpine",
  "beta",
  "gamma",
  "delta",
  "echo",
  "fox",
  "golf",
  "hotel",
  "india",
  "juliet",
  "kilo",
  "lima",
  "mike",
];
const TAGS = ["red", "green", "blue", "alpha"];
interface Row {
  id: number;
  body: string;
  tag: string | null;
  extra?: string | null;
}

const tokens = (text: string | null | undefined): string[] =>
  (text ?? "")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length > 0);

function matches(row: Row, columns: string[], query: string): boolean {
  const doc = columns.flatMap((c) =>
    tokens((row as unknown as Record<string, string | null | undefined>)[c]),
  );
  return query
    .split(/\s+/)
    .filter((t) => t.length > 0)
    .every((term) =>
      term.endsWith("*") ? doc.some((t) => t.startsWith(term.slice(0, -1))) : doc.includes(term),
    );
}
const body = (r: () => number): string =>
  Array.from({ length: 2 + int(r, 4) }, () => pick(r, VOCAB)).join(" ");

interface Outcome {
  readonly problems: string[];
  readonly checks: number;
  readonly indexedPlans: number;
}

async function runStream(seed: number, steps: number): Promise<Outcome> {
  const r = mulberry32(seed);
  const store = new MemoryBlockStore();
  const open = (): MinnowDatabase =>
    new MinnowDatabase(store, { rowsPerBlock: 8, ftsAutoIndexRows: 4 });
  let db = open();
  const problems: string[] = [];
  const keyed = new Map<number, Row>();
  const log: Row[] = [];
  let next = 1;
  let logNext = 1;
  let hasExtra = false;
  await db.execute("CREATE TABLE n (id INTEGER PRIMARY KEY, body TEXT NOT NULL, tag TEXT)");
  await db.execute("CREATE TABLE l (lid INTEGER, body TEXT NOT NULL, tag TEXT)");
  const lit = (v: number | string | null | undefined): string =>
    v === null || v === undefined ? "NULL" : typeof v === "number" ? String(v) : `'${v}'`;
  const insertKeyed = async (count: number): Promise<void> => {
    const rows: Row[] = Array.from({ length: count }, () => ({
      id: next++,
      body: body(r),
      tag: r() < 0.2 ? null : pick(r, TAGS),
      ...(hasExtra ? { extra: r() < 0.5 ? pick(r, VOCAB) : null } : {}),
    }));
    await db.execute(
      `INSERT INTO n (id, body, tag${hasExtra ? ", extra" : ""}) VALUES ${rows
        .map(
          (x) =>
            `(${lit(x.id)}, ${lit(x.body)}, ${lit(x.tag)}${hasExtra ? `, ${lit(x.extra)}` : ""})`,
        )
        .join(",")}`,
    );
    for (const x of rows) keyed.set(x.id, x);
  };
  const insertLog = async (count: number): Promise<void> => {
    const rows: Row[] = Array.from({ length: count }, () => ({
      id: logNext++,
      body: body(r),
      tag: r() < 0.2 ? null : pick(r, TAGS),
    }));
    await db.execute(
      `INSERT INTO l (lid, body, tag) VALUES ${rows
        .map((x) => `(${lit(x.id)}, ${lit(x.body)}, ${lit(x.tag)})`)
        .join(",")}`,
    );
    log.push(...rows);
  };
  await insertKeyed(30);
  await insertLog(30);
  for (const [table, column] of [
    ["n", "body"],
    ["n", "tag"],
    ["l", "body"],
    ["l", "tag"],
  ] as const) {
    await db.buildFtsIndex(table, column);
  }
  let indexedPlans = 0;
  let checks = 0;

  const verify = async (label: string): Promise<void> => {
    const queries = [
      pick(r, VOCAB),
      `${pick(r, VOCAB)} ${pick(r, VOCAB)}`,
      `${pick(r, VOCAB).slice(0, 2)}*`,
      `${pick(r, TAGS)} ${pick(r, VOCAB).slice(0, 3)}*`,
      "alpha",
    ];
    for (const q of queries) {
      for (const columns of [["body"], ["tag"], ["body", "tag"], ["*"]]) {
        const spelled = columns[0] === "*" ? "*" : columns.join(", ");
        const effective = (table: "n" | "l"): string[] =>
          columns[0] === "*"
            ? table === "n" && hasExtra
              ? ["body", "tag", "extra"]
              : ["body", "tag"]
            : columns;
        const idGt = r() < 0.3 ? int(r, next) : null;
        const and = idGt === null ? "" : ` AND id > ${String(idGt)}`;
        const sqlN = `SELECT id FROM n WHERE MATCH(${spelled}) AGAINST ${lit(q)}${and} ORDER BY id`;
        const sqlL = `SELECT lid FROM l WHERE MATCH(${spelled}) AGAINST ${lit(q)} ORDER BY lid`;
        checks += 1;
        const gotN = (await db.query(sqlN, { memoize: false })).rows.map((x) => x.id);
        const wantN = [...keyed.values()]
          .filter((x) => matches(x, effective("n"), q) && (idGt === null || x.id > idGt))
          .map((x) => x.id)
          .sort((a, b) => a - b);
        if (JSON.stringify(gotN) !== JSON.stringify(wantN)) {
          problems.push(
            `seed ${String(seed)} ${label}: ${sqlN}\n  got=${JSON.stringify(gotN)}\n  want=${JSON.stringify(wantN)}`,
          );
          return;
        }
        const gotL = (await db.query(sqlL, { memoize: false })).rows.map((x) => x.lid);
        const wantL = log
          .filter((x) => matches(x, effective("l"), q))
          .map((x) => x.id)
          .sort((a, b) => a - b);
        if (JSON.stringify(gotL) !== JSON.stringify(wantL)) {
          problems.push(
            `seed ${String(seed)} ${label}: ${sqlL}\n  got=${JSON.stringify(gotL)}\n  want=${JSON.stringify(wantL)}`,
          );
          return;
        }
      }
    }
    const plan = await db.explain("SELECT lid FROM l WHERE MATCH(body) AGAINST 'alpha'");
    if (/ready full-text index/i.test(plan)) indexedPlans += 1;
  };

  for (let step = 0; step < steps && problems.length === 0; step += 1) {
    const roll = r();
    let label: string;
    const ids = [...keyed.keys()];
    if (roll < 0.2) {
      label = "insert";
      await insertKeyed(1 + int(r, 3));
      await insertLog(1 + int(r, 3));
    } else if (roll < 0.4 && ids.length > 0) {
      const id = pick(r, ids);
      const b = body(r);
      const t = r() < 0.3 ? null : pick(r, TAGS);
      label = `update ${String(id)}`;
      await db.execute(`UPDATE n SET body = ${lit(b)}, tag = ${lit(t)} WHERE id = ${String(id)}`);
      keyed.set(id, { ...must(keyed.get(id), "row"), body: b, tag: t });
    } else if (roll < 0.5 && ids.length > 0) {
      const id = pick(r, ids);
      label = `delete ${String(id)}`;
      await db.execute(`DELETE FROM n WHERE id = ${String(id)}`);
      keyed.delete(id);
    } else if (roll < 0.58 && ids.length > 0) {
      const id = pick(r, ids);
      const to = next++;
      label = `rekey ${String(id)}->${String(to)}`;
      await db.execute(`UPDATE n SET id = ${String(to)} WHERE id = ${String(id)}`);
      const row = must(keyed.get(id), "row");
      keyed.delete(id);
      keyed.set(to, { ...row, id: to });
    } else if (roll < 0.66) {
      const id = ids.length > 0 && r() < 0.5 ? pick(r, ids) : next++;
      const b = body(r);
      label = `upsert ${String(id)}`;
      await db.execute(
        `INSERT INTO n (id, body, tag) VALUES (${String(id)}, ${lit(b)}, 'blue') ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body`,
      );
      const prev = keyed.get(id);
      keyed.set(
        id,
        prev === undefined
          ? { id, body: b, tag: "blue", ...(hasExtra ? { extra: null } : {}) }
          : { ...prev, body: b },
      );
      if (id >= next) next = id + 1;
    } else if (roll < 0.74) {
      label = "compact n+l";
      await db.compactTable("n");
      await db.compactTable("l");
    } else if (roll < 0.8) {
      label = "rebuild fts";
      for (const [t, c] of [
        ["n", "body"],
        ["n", "tag"],
        ["l", "body"],
        ["l", "tag"],
      ] as const) {
        // A keyed table with keyed mutations refuses a rebuild and scans; that is the contract.
        await db.buildFtsIndex(t, c).catch(() => undefined);
      }
    } else if (roll < 0.86) {
      label = "reopen";
      await db.close();
      db = open();
    } else if (roll < 0.93) {
      const v = pick(r, VOCAB);
      label = `UPDATE n SET tag='alpha' WHERE MATCH(body) AGAINST '${v}'`;
      await db.execute(`UPDATE n SET tag = 'alpha' WHERE MATCH(body) AGAINST ${lit(v)}`);
      for (const row of keyed.values()) if (matches(row, ["body"], v)) row.tag = "alpha";
    } else if (!hasExtra) {
      label = "ADD COLUMN extra";
      await db.execute("ALTER TABLE n ADD COLUMN extra TEXT");
      hasExtra = true;
      for (const row of keyed.values()) row.extra = null;
    } else {
      label = "DROP COLUMN extra";
      await db.execute("ALTER TABLE n DROP COLUMN extra");
      hasExtra = false;
      for (const row of keyed.values()) delete row.extra;
    }
    await verify(`step ${String(step)} (${label})`);
  }
  await db.close();
  return { problems, checks, indexedPlans };
}

const STEPS = Number(process.env.MINNOW_ORACLE_STEPS ?? 40);

describe("full-text MATCH agrees with the token model", () => {
  it.each(seedsFor("fts-oracle", [0x6f3d, 0x704e]))(
    "keyed and append-only tables (seed %s)",
    async (seed) => {
      const outcome = await runStream(seed, STEPS);
      expect(outcome.problems).toEqual([]);
      expect(outcome.checks).toBeGreaterThan(0);
      expect(outcome.indexedPlans).toBeGreaterThan(0);
    },
  );
});
