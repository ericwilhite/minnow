/** Reusable strict PGlite ordering campaign. Native provider adapters consume nativeSetup,
 * seed, mutation and traceSql; they must propagate refusals/errors, never turn them into rows.
 * The controller adapter is deliberately synthetic for native row/RI/source execution. */
import type { PGlite } from "@electric-sql/pglite";
import { fixture, quote } from "./trigger-controller.test-support.js";

export interface TriggerOrderingHistory {
  readonly rows: number;
  readonly event: "update" | "delete";
  readonly beforeNames: readonly string[];
  readonly afterNames: readonly string[];
  readonly twoBodies: boolean;
  readonly reverseCreation: boolean;
}
export interface TriggerOrderingScript {
  readonly nativeSetup: readonly string[];
  readonly postgresSetup: readonly string[];
  readonly seed: readonly string[];
  readonly mutation: string;
  readonly traceSql: string;
}
export interface TriggerOrderingAdapter {
  /** Resolve only after root AFTER work/verification; native SQL adapter executes script. */
  run(): Promise<readonly string[]>;
  /** Must fence pending work, including failure. Factory owns cleanup before delivery. */
  close(): Promise<void>;
}
export type TriggerOrderingFactory = (
  history: TriggerOrderingHistory,
  script: TriggerOrderingScript,
) => Promise<TriggerOrderingAdapter>;

export function orderingScript(h: TriggerOrderingHistory): TriggerOrderingScript {
  const schema = [
    "CREATE SEQUENCE event_seq",
    "CREATE TABLE p(id INT PRIMARY KEY)",
    "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
    "CREATE TABLE log(label TEXT,n INT,ord BIGINT DEFAULT nextval('event_seq'))",
    "CREATE TABLE audit(label TEXT,n INT,ord BIGINT DEFAULT nextval('event_seq'))",
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
      body = `INSERT INTO ${target}(label,n) VALUES ('${spec.label}',${source});${spec.double ? ` INSERT INTO audit(label,n) VALUES ('tail',${source});` : ""}`;
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
    traceSql:
      "SELECT label||':'||n::text s FROM (SELECT * FROM log UNION ALL SELECT * FROM audit)q ORDER BY ord",
  };
}
export async function orderingDifferential(
  pg: PGlite,
  history: TriggerOrderingHistory,
  make: TriggerOrderingFactory,
) {
  const script = orderingScript(history);
  await pg.exec("DROP TABLE IF EXISTS c,p,log,audit CASCADE; DROP SEQUENCE IF EXISTS event_seq");
  for (const sql of [...script.postgresSetup, ...script.seed, script.mutation]) await pg.exec(sql);
  const expected = (await pg.query<{ s: string }>(script.traceSql)).rows.map((r) => r.s);
  const adapter = await make(history, script);
  try {
    return { expected, actual: await adapter.run() };
  } finally {
    await adapter.close();
  }
}
/** Real controller/queues/bindings/body protocol; injected scalar source and RI adapters.
 * Replace ONLY this factory with B5b's verified native SQL entry point when it lands. */
export const controllerOrderingFactory: TriggerOrderingFactory = async (h) => {
  const f = await fixture(h.event, false, h.twoBodies, "none", null, false, 2, h);
  return {
    async run() {
      for (let n = 1; n <= h.rows; n++) await f.row(n);
      await f.runtime.finishRoot();
      return [...f.trace];
    },
    close: () => f.close(),
  };
};
