/** Native trigger preparation is admitted step by step: every lowered rule program and trigger
 * body is charged before the next planning step or any wait, and a step that does not fit is
 * dropped and planned again after its credit is granted. Large generated bodies and wide
 * default rule programs run under a competing grant that fills the cap. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_COMMAND_PREPARATION_REACH } from "../../testing/reach.js";
import { executeNativeCommand } from "../owner/native-command.js";
import { retainedBytes } from "../owner/rule-cache.js";
import { Governor, MemoryAdmissionError, type Grant } from "../memory/governor.js";
import type { Lowerer } from "../lower/lowerer.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

type Event =
  | { readonly kind: "plan"; readonly key: string; readonly bytes: number }
  | { readonly kind: "allocate"; readonly bytes: number; readonly ok: boolean }
  | { readonly kind: "wait" };

interface History {
  readonly rows: number;
  readonly bodies: ReadonlyArray<readonly number[]>;
  readonly wide: number;
  readonly mode: "free" | "release" | "cancel";
}

function script(h: History) {
  const columns = Array.from(
    { length: h.wide },
    (_, i) => `,c${String(i)} INT DEFAULT ${String(i)}`,
  );
  const schema = [
    "CREATE TABLE p(id INT PRIMARY KEY)",
    "CREATE TABLE log(t INT,b INT,n INT)",
    `CREATE TABLE w(id INT${columns.join("")})`,
  ];
  const native = [...schema],
    postgres = [...schema];
  h.bodies.forEach((terms, t) => {
    const statements = terms.map(
      (k, b) =>
        `INSERT INTO log(t,b,n) VALUES (${String(t)},${String(b)},OLD.id${" + 1".repeat(k)});`,
    );
    if (t === 0 && h.wide > 0) statements.push("INSERT INTO w(id) VALUES (OLD.id);");
    const body = statements.join(" ");
    native.push(`CREATE TRIGGER t${String(t)} AFTER UPDATE ON p BEGIN ${body} END`);
    postgres.push(
      `CREATE OR REPLACE FUNCTION prep_f${String(t)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body} RETURN NEW; END $$`,
      `CREATE TRIGGER t${String(t)} AFTER UPDATE ON p FOR EACH ROW EXECUTE FUNCTION prep_f${String(t)}()`,
    );
  });
  return {
    native,
    postgres,
    seed: `INSERT INTO p SELECT n FROM generate_series(1,${String(h.rows)})n`,
    mutation: "UPDATE p SET id=id+100",
    queries: [
      "SELECT * FROM p ORDER BY id",
      "SELECT * FROM log ORDER BY t,b,n",
      "SELECT * FROM w ORDER BY id",
    ],
  };
}

/** Event-order law for the preparation phase (before the first producer): a plan's result is
 * charged before the next plan; a wait follows only a failed charge, and the step that failed
 * is planned again after the wait rather than kept. Charges cover the planner's own output. */
function checkPreparation(
  events: readonly Event[],
  complete = true,
): { waits: number; replans: number } {
  let pending: Extract<Event, { kind: "plan" }> | null = null,
    failed: Extract<Event, { kind: "plan" }> | null = null,
    waits = 0,
    replans = 0;
  for (const event of events) {
    if (event.kind === "plan") {
      expect(pending, "a planning result was left uncharged before the next step").toBeNull();
      if (failed) {
        expect(event.key, "the step that did not fit is planned again").toBe(failed.key);
        replans++;
        failed = null;
      }
      pending = event;
    } else if (event.kind === "allocate") {
      if (!pending) continue;
      if (event.ok) {
        expect(event.bytes).toBeGreaterThanOrEqual(pending.bytes);
        pending = null;
      } else {
        failed = pending;
        pending = null;
      }
    } else {
      expect(pending, "a planning result was held across a wait").toBeNull();
      if (failed) waits++;
    }
  }
  if (complete) {
    expect(pending).toBeNull();
    expect(failed).toBeNull();
  }
  return { waits, replans };
}

for (const seed of seedsFor("next-native-command-preparation", [49701, 49702]))
  it(`admits native trigger preparation before retaining it (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("native command preparation", NATIVE_COMMAND_PREPARATION_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 3 }),
          fc.array(fc.array(fc.integer({ min: 1, max: 1200 }), { minLength: 1, maxLength: 2 }), {
            minLength: 1,
            maxLength: 4,
          }),
          fc.integer({ min: 0, max: 60 }),
          fc.constantFrom("free" as const, "release" as const, "cancel" as const),
          async (rows, bodies, wide, mode) => {
            reach.beginHistory();
            const h: History = { rows, bodies, wide, mode },
              s = script(h),
              core = new StorageCore(64),
              governor = new Governor(
                (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20,
              ),
              stop = new AbortController(),
              events: Event[] = [];
            Object.defineProperty(core.harness, "governor", { value: governor });
            await core.open();
            let tx: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null,
              foreign: Grant | null = null,
              preparing = false;
            try {
              await pg.exec("DROP TABLE IF EXISTS p,log,w CASCADE");
              for (const sql of [...s.postgres, s.seed]) await pg.exec(sql);
              for (const sql of [...s.native, s.seed]) await core.execute(sql);
              tx = await core.harness.path.begin();
              const context = { ...core.context(), catalog: tx.catalog },
                lowered = core.lowerer.lower(s.mutation, context);
              if (lowered.kind !== "modify") throw new TypeError("Missing mutation plan");
              let waiting: () => void = () => undefined;
              const waited = new Promise<void>((resolve) => {
                waiting = resolve;
              });
              const fill = async () => {
                if (mode === "free" || foreign) return;
                const [grant] = await governor.admitGroup({
                  label: "foreign result",
                  includesProgress: true,
                  grants: [
                    {
                      label: "foreign result",
                      minimumBytes: governor.capBytes - governor.usedBytes,
                      spillable: false,
                    },
                  ],
                  signal: new AbortController().signal,
                });
                if (!grant?.allocate(grant.held)) throw new Error("Missing foreign grant");
                foreign = grant;
              };
              // Instrument the real planner and the root's real metadata account.
              const planner: Lowerer = Object.create(core.lowerer) as Lowerer;
              planner.lower = (text, ctx) => {
                const out = core.lowerer.lower(text, ctx);
                if (preparing)
                  events.push({
                    kind: "plan",
                    key: `body:${text}`,
                    bytes: retainedBytes(out.kind === "modify" ? out.plan : out),
                  });
                return out;
              };
              planner.lowerTableRules = (table, catalog) => {
                const out = core.lowerer.lowerTableRules(table, catalog);
                if (preparing)
                  events.push({
                    kind: "plan",
                    key: `rules:${String(table.id)}`,
                    bytes: retainedBytes(out),
                  });
                return out;
              };
              // Every admission while preparing is a potential wait (the root's growth and the
              // writer's own step growth). Until a planning step has failed to fit, the
              // competing grant yields to each and then refills what is left, so the wait the
              // test releases or cancels is the one a planning step caused.
              let planWaited = false;
              const admit = governor.admitGroup.bind(governor);
              vi.spyOn(governor, "admitGroup").mockImplementation(async (request) => {
                const step = preparing && request.label !== "foreign result";
                if (step) {
                  events.push({ kind: "wait" });
                  if (planWaited) waiting();
                  else {
                    foreign?.close();
                    foreign = null;
                  }
                }
                const grants = await admit(request);
                if (request.label === "native root open") {
                  const metadata = grants[0];
                  if (!metadata) throw new Error("Missing metadata grant");
                  const allocate = metadata.allocate.bind(metadata);
                  metadata.allocate = (bytes: number) => {
                    const ok = allocate(bytes);
                    if (preparing) {
                      if (!ok && events.at(-1)?.kind === "plan") planWaited = true;
                      events.push({ kind: "allocate", bytes, ok });
                    }
                    return ok;
                  };
                  preparing = true;
                }
                if (request.label === "native root open" || (step && !planWaited)) await fill();
                return grants;
              });
              const pending = executeNativeCommand({
                transaction: tx,
                rootId: 1,
                plan: lowered.plan,
                parameters: [],
                planner,
                context,
                governor,
                signal: stop.signal,
                execution: {
                  reads: {
                    open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
                  },
                  spill: core.spill,
                  queryId: 1,
                  statementTime: 0n,
                  sequences: null,
                  sequenceSession: null,
                  returning: "atExecuted",
                },
              });
              const outcome = pending.then(
                (value) => ({ value }),
                (error: unknown) => ({ error }),
              );
              if (mode !== "free") {
                await Promise.race([waited, outcome]);
                // The root waits for preparation credit; its preparation is charged or dropped.
                checkPreparation(events, false);
                if (mode === "cancel") stop.abort(new Error("cancel native preparation"));
                else {
                  (foreign as Grant | null)?.close();
                  foreign = null;
                }
              }
              const completed = await outcome;
              (foreign as Grant | null)?.close();
              foreign = null;
              preparing = false;
              vi.restoreAllMocks();
              // Body program workspace grows with body size, so the largest generated bodies
              // can need more than a 32 MiB cap. That is a typed refusal above the cap, never
              // below it, and it leaves no effects.
              const refusal =
                "error" in completed && completed.error instanceof MemoryAdmissionError
                  ? completed.error
                  : null;
              const refused = refusal !== null;
              if (refusal) {
                expect(refusal.sqlState).toBe("53200");
                expect(refusal.bytes).toBeGreaterThan(refusal.capacity);
                tx.rollback();
                tx = null;
              } else if (mode === "cancel") {
                expect(completed).toHaveProperty("error", stop.signal.reason);
                tx.rollback();
                tx = null;
                reach.reach("cancelled while waiting");
              } else {
                if ("error" in completed) throw completed.error;
                expect(completed.value.rowCount).toBe(rows);
                await pg.exec(s.mutation);
                const receipt = await tx.commit(null);
                await receipt.durable;
                tx = null;
                reach.reach(mode === "release" ? "granted after release" : "no competing credit");
              }
              const { waits, replans } = checkPreparation(events, mode !== "cancel" && !refused);
              if (mode !== "free" && !refused) {
                expect(waits).toBeGreaterThan(0);
                reach.reach("waited before retaining");
              }
              if (replans > 0) reach.reach("replanned after wait");
              for (const query of s.queries) {
                const actual = await core.execute(query);
                if (!("batches" in actual)) throw new Error("Missing rows");
                expect(oracleRows(actual), query).toEqual((await pg.query(query)).rows);
              }
              reach.reach("exact PG rows");
              if (bodies.some((terms) => terms.some((k) => k >= 600))) reach.reach("large body");
              if (bodies.length > 1) reach.reach("several triggers");
              if (wide >= 30) reach.reach("wide default rules");
            } finally {
              (foreign as Grant | null)?.close();
              vi.restoreAllMocks();
              tx?.rollback();
              await core.close();
              expect(core.spill.bytesInUse).toBe(0);
              expect(governor.usedBytes).toBe(0);
            }
            reach.reach("zero retained ownership");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [1, [[650]], 0, "free"],
            [2, [[300], [200]], 60, "release"],
            [3, [[200], [1], [300, 2]], 40, "cancel"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 300000);
