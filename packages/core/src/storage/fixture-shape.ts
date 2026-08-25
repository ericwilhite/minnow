/**
 * The database every compatibility fixture holds, and the questions asked of it.
 *
 * Shared by the generator (scripts/write-format-fixture.mts) and the test that reads the
 * fixtures back, so the two cannot drift: a fixture written today and a fixture written in two
 * years describe the same database, and any difference in the answers is a difference in the
 * engine rather than in what was asked.
 *
 * The shape is chosen to touch every part of the persisted format that a change could break,
 * because a fixture only protects what it happens to contain:
 *
 *   - every column type, and a nullable column of each that actually holds nulls
 *   - a low-cardinality string column, which encodes through the dictionary, alongside a
 *     high-cardinality one that cannot
 *   - a column of near-identical text, which compresses, beside random text that does not, so
 *     both codec paths appear in the blocks
 *   - enough rows to span several blocks per column, so multi-block columns and their zone maps
 *     are covered rather than a single degenerate block
 *   - a mutation history: updates and deletes leave delta segments, which are a different
 *     segment kind with a different block layout from a plain append
 *   - both a string and a numeric unique key, since key encoding differs between them
 *   - a table with no unique key at all, which stores no key column
 *
 * Adding to this shape is safe and welcome. Removing from it silently narrows what every future
 * fixture proves, so treat a deletion here as a decision to stop protecting that surface.
 */
import { MinnowDatabase } from "../engine/database.js";
import { MemoryBlockStore } from "./memory.js";

/** Stable clock and ID source shared by the fixture writer and its writer-shape regression. */
export const FORMAT_FIXTURE_CREATED_AT = "2026-01-01T00:00:00.000Z";

export function createFormatFixtureId(): () => string {
  let nextId = 0;
  return () => `fixture-${String(nextId++).padStart(6, "0")}`;
}

/** Deterministic pseudo-randomness, so a regenerated fixture differs only when the format does. */
function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

const REGIONS = ["west", "east", "north", "south"] as const;
const LETTERS = "abcdefghijklmnopqrstuvwxyz";

/** Text with almost no entropy, which the compressing codec will take. */
function compressible(index: number): string {
  return `the quick brown fox jumps over the lazy dog ${String(index % 3)}`;
}

/** Text with high entropy, which it will not. */
function incompressible(random: () => number): string {
  let out = "";
  for (let index = 0; index < 24; index += 1) {
    out += LETTERS[Math.floor(random() * LETTERS.length)] ?? "a";
  }
  return out;
}

/** Rows enough to span several blocks at the small block size the generator uses. */
const ROWS = 400;

/**
 * Builds the fixture database. The caller supplies the database so the generator and the test
 * can each choose their own store; nothing here depends on which.
 */
export async function buildFixtureDatabase(database: MinnowDatabase): Promise<void> {
  const random = mulberry32(0x0f1a7);
  await database.createTable({
    name: "records",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string" },
      { name: "note", type: "string" },
      { name: "noise", type: "string" },
      { name: "amount", type: "number" },
      { name: "active", type: "boolean" },
      { name: "seen_at", type: "datetime" },
      { name: "maybe_text", type: "string", nullable: true },
      { name: "maybe_number", type: "number", nullable: true },
      { name: "maybe_flag", type: "boolean", nullable: true },
      { name: "maybe_time", type: "datetime", nullable: true },
    ],
  });
  await database.insertBatch(
    "records",
    Array.from({ length: ROWS }, (_, index) => ({
      id: index + 1,
      region: REGIONS[index % REGIONS.length] ?? "west",
      note: compressible(index),
      noise: incompressible(random),
      // Quarters stay exact in doubles, so a rounding change shows up as a difference rather
      // than as noise nobody can attribute.
      amount: Math.floor(random() * 400) / 4,
      active: index % 3 === 0,
      seen_at: new Date(Date.UTC(2026, 0, 1 + (index % 28), index % 24, 0, 0)),
      maybe_text: index % 5 === 0 ? null : `t${String(index)}`,
      maybe_number: index % 7 === 0 ? null : index * 2,
      maybe_flag: index % 11 === 0 ? null : index % 2 === 0,
      maybe_time: index % 13 === 0 ? null : new Date(Date.UTC(2025, 5, 1 + (index % 27))),
    })),
  );

  // A string unique key, which encodes differently from a numeric one.
  await database.createTable({
    name: "keyed_by_name",
    uniqueKey: "name",
    columns: [
      { name: "name", type: "string" },
      { name: "score", type: "number" },
    ],
  });
  await database.insertBatch(
    "keyed_by_name",
    Array.from({ length: 120 }, (_, index) => ({
      name: `name-${String(index).padStart(4, "0")}`,
      score: index % 50,
    })),
  );

  // No unique key at all: this table stores no key column, which is its own layout.
  await database.createTable({
    name: "keyless",
    columns: [
      { name: "label", type: "string" },
      { name: "value", type: "number" },
    ],
  });
  await database.insertBatch(
    "keyless",
    Array.from({ length: 90 }, (_, index) => ({
      label: REGIONS[index % REGIONS.length] ?? "west",
      value: index,
    })),
  );

  // The mutation history. Updates and deletes write delta segments, whose layout differs from an
  // append -- and a fixture with only appends would prove nothing about reading them back.
  await database.execute("UPDATE records SET amount = ? WHERE id = ?", [1234.5, 3]);
  await database.execute("UPDATE records SET maybe_text = ? WHERE id = ?", [null, 4]);
  await database.execute("UPDATE records SET region = ? WHERE id = ?", ["central", 5]);
  await database.execute("DELETE FROM records WHERE id = ?", [6]);
  await database.execute("DELETE FROM records WHERE id > ?", [396]);
  await database.execute("UPDATE keyed_by_name SET score = ? WHERE name = ?", [999, "name-0007"]);
  await database.execute("DELETE FROM keyed_by_name WHERE name = ?", ["name-0008"]);
}

/**
 * What every fixture is asked. Ordered so the answers are stable, and chosen to read each part
 * of the shape above back out: every type, both codecs, the dictionary, the deltas, both key
 * kinds, and the keyless table.
 *
 * Only ever append here. Changing an existing query changes the question every historical
 * fixture is answering, and the recorded answer no longer means what it meant.
 */
export const FIXTURE_QUERIES: readonly string[] = [
  "SELECT COUNT(*) AS n FROM records",
  "SELECT COUNT(*) AS n FROM keyed_by_name",
  "SELECT COUNT(*) AS n FROM keyless",
  // Every column of one row, after the updates that touched its neighbours.
  "SELECT * FROM records WHERE id = 1",
  // The updated rows, which live in delta segments rather than the base.
  "SELECT id, amount FROM records WHERE id = 3",
  "SELECT id, maybe_text FROM records WHERE id = 4",
  "SELECT id, region FROM records WHERE id = 5",
  // The deleted rows must be gone, both the single and the ranged delete.
  "SELECT id FROM records WHERE id IN (6, 397, 398, 399, 400) ORDER BY id",
  // Nullability across every type, counted rather than listed so the answer stays small.
  "SELECT COUNT(*) AS n FROM records WHERE maybe_text IS NULL",
  "SELECT COUNT(*) AS n FROM records WHERE maybe_number IS NULL",
  "SELECT COUNT(*) AS n FROM records WHERE maybe_flag IS NULL",
  "SELECT COUNT(*) AS n FROM records WHERE maybe_time IS NULL",
  // The dictionary-coded column, and an aggregate that has to decode every value of it.
  "SELECT region, COUNT(*) AS n FROM records GROUP BY region ORDER BY region",
  // Both text columns, which took different codecs.
  "SELECT note FROM records WHERE id = 42",
  "SELECT noise FROM records WHERE id = 42",
  // Numeric and datetime round-trips, including the boundary rows of the range.
  "SELECT SUM(amount) AS total, MIN(amount) AS low, MAX(amount) AS high FROM records",
  "SELECT MIN(seen_at) AS first, MAX(seen_at) AS last FROM records",
  "SELECT COUNT(*) AS n FROM records WHERE active",
  // A scan that spans blocks rather than resolving inside one.
  "SELECT id FROM records WHERE amount > 90 ORDER BY id LIMIT 10",
  // The string key, updated and deleted through.
  "SELECT name, score FROM keyed_by_name WHERE name = 'name-0007'",
  "SELECT name FROM keyed_by_name WHERE name = 'name-0008'",
  "SELECT name, score FROM keyed_by_name ORDER BY name LIMIT 3",
  // The keyless table, which stores no key column.
  "SELECT label, SUM(value) AS total FROM keyless GROUP BY label ORDER BY label",
  // A join, which reads two layouts at once.
  "SELECT r.region, COUNT(*) AS n FROM records r JOIN keyless k ON r.region = k.label GROUP BY r.region ORDER BY r.region",
];

/** Builds the exact deterministic writer artifact frozen by the current-version fixture. */
export async function createFormatFixtureArtifact(): Promise<{
  bytes: Uint8Array;
  expectations: Array<{ sql: string; rows: unknown }>;
}> {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 64,
    autoCompact: false,
    autoCollect: false,
    now: () => new Date(FORMAT_FIXTURE_CREATED_AT),
    createId: createFormatFixtureId(),
  });
  try {
    await buildFixtureDatabase(database);
    const expectations: Array<{ sql: string; rows: unknown }> = [];
    for (const sql of FIXTURE_QUERIES) {
      const result = await database.query(sql, { memoize: false });
      expectations.push({ sql, rows: result.rows });
    }
    return { bytes: await database.exportSnapshot(), expectations };
  } finally {
    await database.close();
  }
}
