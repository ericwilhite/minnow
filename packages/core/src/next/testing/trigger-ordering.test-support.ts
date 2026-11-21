/** Reusable strict PGlite ordering campaign. Native provider adapters consume nativeSetup,
 * seed, mutation and traceSql; they must propagate refusals/errors, never turn them into rows.
 * Native callers use actual row writes to a trace accumulator; no sequence kernel is needed. */
import type { PGlite } from "@electric-sql/pglite";
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;

export interface TriggerOrderingHistory {
  readonly rows: number;
  readonly event: "update" | "delete";
  readonly beforeNames: readonly string[];
  readonly afterNames: readonly string[];
  readonly twoBodies: boolean;
  readonly reverseCreation: boolean;
  readonly published?: boolean;
}
export interface TriggerOrderingScript {
  readonly nativeSetup: readonly string[];
  readonly postgresSetup: readonly string[];
  readonly seed: readonly string[];
  readonly mutation: string;
  readonly traceSql: string;
  readonly traceKind: "rows" | "accumulator";
}
export interface TriggerOrderingAdapter {
  /** Resolve only after root AFTER work/verification; native SQL adapter executes script. */
  run(): Promise<readonly string[]>;
  /** Must fence pending work, including failure. Factory owns cleanup before delivery. */
  close(): Promise<void>;
  readonly stateQueries?: readonly string[];
  readState?(sql: string): Promise<ReadonlyArray<Record<string, unknown>>>;
}
export type TriggerOrderingFactory = (
  history: TriggerOrderingHistory,
  script: TriggerOrderingScript,
) => Promise<TriggerOrderingAdapter>;

export function orderingScript(
  h: TriggerOrderingHistory,
  traceKind: TriggerOrderingScript["traceKind"] = "rows",
): TriggerOrderingScript {
  const schema = [
    ...(traceKind === "rows" ? ["CREATE SEQUENCE event_seq"] : []),
    "CREATE TABLE p(id INT PRIMARY KEY)",
    "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
    `CREATE TABLE log(label TEXT,n INT${traceKind === "rows" ? ",ord BIGINT DEFAULT nextval('event_seq')" : ""})`,
    `CREATE TABLE audit(label TEXT,n INT${traceKind === "rows" ? ",ord BIGINT DEFAULT nextval('event_seq')" : ""})`,
    ...(traceKind === "accumulator"
      ? ["CREATE TABLE trace(id INT PRIMARY KEY,v TEXT)", "INSERT INTO trace VALUES(1,'')"]
      : []),
  ];
  const specs = [
    ...h.beforeNames.map((name, i) => ({
      name,
      table: "p",
      timing: "before",
      event: h.event,
      label: i === 0 ? "pB" : `pB${String(i)}`,
      double: h.twoBodies,
    })),
    ...h.afterNames.map((name, i) => ({
      name,
      table: "p",
      timing: "after",
      event: h.event,
      label: `p${String(i)}`,
      double: h.twoBodies,
    })),
    {
      name: "childBefore",
      table: "c",
      timing: "before",
      event: h.event,
      label: "cB",
      double: false,
    },
    {
      name: "childAfter",
      table: "c",
      timing: "after",
      event: h.event,
      label: "cA",
      double: h.twoBodies,
    },
    { name: "echo", table: "log", timing: "after", event: "insert", label: "echo", double: false },
  ];
  if (h.reverseCreation) specs.reverse();
  const nativeSetup = [...schema],
    postgresSetup = [...schema];
  for (let i = 0; i < specs.length; i++) {
    const spec = specs[i];
    if (!spec) throw new Error("Missing ordering spec");
    const source = spec.table === "log" ? "NEW.n" : "OLD.id",
      target = spec.table === "log" ? "audit" : "log",
      // This is actual SQL executed independently by both engines, not a synthetic event
      // recorder. Trace has no triggers; the existing log/audit writes still exercise nested
      // body frames. Recording before each INSERT preserves the sequenced harness's trace.
      mark = (label: string) =>
        traceKind === "accumulator"
          ? `UPDATE trace SET v=v || '${label}:' || CAST(${source} AS TEXT) || '|' WHERE id=1; `
          : "",
      body = `${mark(spec.label)}INSERT INTO ${target}(label,n) VALUES ('${spec.label}',${source});${spec.double ? ` ${mark("tail")}INSERT INTO audit(label,n) VALUES ('tail',${source});` : ""}`;
    nativeSetup.push(
      `CREATE TRIGGER ${quote(spec.name)} ${spec.timing} ${spec.event} ON ${spec.table} BEGIN ${body} END`,
    );
    // Independent dialect compilation: PG actually fires the program, never a shadow trace.
    postgresSetup.push(
      `CREATE OR REPLACE FUNCTION ordering_f${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body} RETURN ${spec.event === "delete" ? "OLD" : "NEW"}; END $$`,
      `CREATE TRIGGER ${quote(spec.name)} ${spec.timing} ${spec.event} ON ${spec.table} FOR EACH ROW EXECUTE FUNCTION ordering_f${String(i)}()`,
    );
  }
  return {
    nativeSetup,
    postgresSetup,
    seed: [
      `INSERT INTO p SELECT n FROM generate_series(1,${String(h.rows)})n`,
      `INSERT INTO c SELECT n+1000,n FROM generate_series(1,${String(h.rows)})n`,
    ],
    mutation: h.event === "update" ? "UPDATE p SET id=id+100" : "DELETE FROM p",
    traceKind,
    traceSql:
      traceKind === "rows"
        ? "SELECT label||':'||n::text s FROM (SELECT * FROM log UNION ALL SELECT * FROM audit)q ORDER BY ord"
        : "SELECT v AS s FROM trace",
  };
}
export async function orderingDifferential(
  pg: PGlite,
  history: TriggerOrderingHistory,
  make: TriggerOrderingFactory,
  script = orderingScript(history),
) {
  await pg.exec(
    "DROP TABLE IF EXISTS c,p,log,audit,trace CASCADE; DROP SEQUENCE IF EXISTS event_seq",
  );
  for (const sql of [...script.postgresSetup, ...script.seed, script.mutation]) await pg.exec(sql);
  const expected = orderingTrace(script, (await pg.query<{ s: string }>(script.traceSql)).rows);
  const adapter = await make(history, script);
  try {
    const actual = await adapter.run();
    const states: Array<{
      sql: string;
      expected: ReadonlyArray<Record<string, unknown>>;
      actual: ReadonlyArray<Record<string, unknown>>;
    }> = [];
    for (const sql of adapter.stateQueries ?? []) {
      if (!adapter.readState) throw new TypeError("Missing native state reader");
      states.push({
        sql,
        expected: (await pg.query<Record<string, unknown>>(sql)).rows,
        actual: await adapter.readState(sql),
      });
    }
    return { expected, actual, states };
  } finally {
    await adapter.close();
  }
}
/** Exact observation decoding, shared across the independent PG and native queries. Invalid
 * or missing trace rows reject; they are never interpreted as successful empty work. */
export function orderingTrace(
  script: TriggerOrderingScript,
  rows: ReadonlyArray<{ s: unknown }>,
): readonly string[] {
  if (script.traceKind === "rows")
    return rows.map((row) => {
      if (typeof row.s !== "string") throw new TypeError("Invalid trigger trace row");
      return row.s;
    });
  if (rows.length !== 1 || typeof rows[0]?.s !== "string")
    throw new TypeError("Missing trigger accumulator");
  const text = rows[0].s;
  if (text === "") return [];
  if (!text.endsWith("|")) throw new TypeError("Incomplete trigger accumulator");
  return text.slice(0, -1).split("|");
}
