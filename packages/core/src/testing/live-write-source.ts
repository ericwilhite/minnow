import fc from "fast-check";
export type Write =
  | { kind: "insert"; table: "a" | "b"; id: number; x: number | null; g: string; ref: number }
  | { kind: "upsert"; id: number; x: number | null; g: string }
  | { kind: "updateKey"; id: number; x: number | null }
  | { kind: "updateWhere"; low: number; by: number }
  | { kind: "deleteKey"; table: "a" | "b"; id: number }
  | { kind: "deleteWhere"; low: number }
  | { kind: "rekey"; from: number; to: number }
  | { kind: "shift"; low: number; by: number }
  | { kind: "transaction"; writes: Write[]; commit: boolean }
  | { kind: "other"; write: Write }
  | { kind: "compact"; table: "a" | "b" }
  | { kind: "collect" }
  | { kind: "index" };

const key = fc.integer({ min: -8, max: 12 });
const value = fc.option(fc.integer({ min: -20, max: 30 }), { nil: null, freq: 6 });
const group = fc.constantFrom("k", "m", "q");

export const simpleWrite: fc.Arbitrary<Write> = fc.oneof(
  {
    weight: 4,
    arbitrary: fc.record({
      kind: fc.constant("insert" as const),
      table: fc.constantFrom("a" as const, "b" as const),
      id: key,
      x: value,
      g: group,
      ref: key,
    }),
  },
  {
    weight: 3,
    arbitrary: fc.record({ kind: fc.constant("upsert" as const), id: key, x: value, g: group }),
  },
  {
    weight: 3,
    arbitrary: fc.record({ kind: fc.constant("updateKey" as const), id: key, x: value }),
  },
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant("updateWhere" as const),
      low: fc.integer({ min: -20, max: 30 }),
      by: fc.integer({ min: -9, max: 9 }),
    }),
  },
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant("deleteKey" as const),
      table: fc.constantFrom("a" as const, "b" as const),
      id: key,
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant("deleteWhere" as const),
      low: fc.integer({ min: -20, max: 30 }),
    }),
  },
  { weight: 3, arbitrary: fc.record({ kind: fc.constant("rekey" as const), from: key, to: key }) },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant("shift" as const),
      low: key,
      by: fc.constantFrom(-2, -1, 1, 2),
    }),
  },
);

export const write: fc.Arbitrary<Write> = fc.oneof(
  { weight: 12, arbitrary: simpleWrite },
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant("transaction" as const),
      writes: fc.array(simpleWrite, { minLength: 1, maxLength: 3 }),
      commit: fc.boolean(),
    }),
  },
  { weight: 3, arbitrary: fc.record({ kind: fc.constant("other" as const), write: simpleWrite }) },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant("compact" as const),
      table: fc.constantFrom("a" as const, "b" as const),
    }),
  },
  { weight: 1, arbitrary: fc.constant({ kind: "collect" as const }) },
  { weight: 1, arbitrary: fc.constant({ kind: "index" as const }) },
);

function literal(value: number | null): string {
  return value === null ? "NULL" : String(value);
}

export function sqlOf(entry: Write): string[] {
  switch (entry.kind) {
    case "insert":
      return [
        entry.table === "a"
          ? `INSERT INTO a VALUES (${String(entry.id)}, '${entry.g}', ${literal(entry.x)}, ${String(entry.ref)})`
          : `INSERT INTO b VALUES (${String(entry.id)}, ${String(entry.ref)}, ${literal(entry.x)})`,
      ];
    case "upsert":
      return [
        `INSERT INTO a VALUES (${String(entry.id)}, '${entry.g}', ${literal(entry.x)}, NULL) ON CONFLICT (id) DO UPDATE SET g = excluded.g, x = excluded.x`,
      ];
    case "updateKey":
      return [`UPDATE a SET x = ${literal(entry.x)} WHERE id = ${String(entry.id)}`];
    case "updateWhere":
      return [`UPDATE a SET x = x + ${String(entry.by)} WHERE x >= ${String(entry.low)}`];
    case "deleteKey":
      return [`DELETE FROM ${entry.table} WHERE id = ${String(entry.id)}`];
    case "deleteWhere":
      return [`DELETE FROM a WHERE x < ${String(entry.low)}`];
    case "rekey":
      return [`UPDATE a SET id = ${String(entry.to)} WHERE id = ${String(entry.from)}`];
    case "shift":
      return [`UPDATE a SET id = id + ${String(entry.by)} WHERE id >= ${String(entry.low)}`];
    default:
      return [];
  }
}
