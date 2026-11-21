/** Native trigger preparation is metered under a growing reservation. The real allocation sites
 * (rule and body lowering) are instrumented: each runs only after its item's base share was
 * charged, every copy inside is charged by the copy meter before it is allocated, and every
 * governor admission during preparation must begin with no preparation charge held. Attempts
 * stay within ceil(log2(cap / initial)) + 1, a reused plan admits once, and 53200 appears only
 * above the cap. Large generated bodies and wide default rules run free, under a competing grant
 * that fills the cap (released or cancelled while the root waits), and above the cap. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_COMMAND_PREPARATION_REACH } from "../../testing/reach.js";
import { executeNativeCommand, PREPARATION_INITIAL_FACTOR } from "../owner/native-command.js";
import { NativeCommandReach } from "../owner/command-reach.js";
import { NativeCommandRoot } from "../owner/command-root.js";
import { NATIVE_ROOT_CREDIT_BYTES } from "../owner/native-root-credits.js";
import {
  bodyPreparationBound,
  closurePreparationBound,
  rulePreparationBound,
} from "../owner/native-preparation-bound.js";
import { nativeCommandReachBytes } from "../owner/command-reach.js";
import { Governor, MemoryAdmissionError, type Grant } from "../memory/governor.js";
import type { Lowerer } from "../lower/lowerer.js";
import { ChunkReader } from "../page/reader.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

type Mode = "free" | "release" | "cancel" | "oversize";
interface History {
  readonly rows: number;
  readonly bodies: ReadonlyArray<readonly number[]>;
  readonly wide: number;
  readonly mode: Mode;
}

const capBytes = (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20;

function script(h: History) {
  // Above the cap: enough 1000-term bodies that their static bound alone exceeds the cap.
  const bodies =
    h.mode === "oversize"
      ? Array.from({ length: Math.ceil(capBytes / (4 << 20)) + 2 }, () => [1000, 1000])
      : h.bodies;
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
  bodies.forEach((terms, t) => {
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
    // Above the cap nothing runs, so PostgreSQL only needs the unchanged rows.
    postgres: h.mode === "oversize" ? schema : postgres,
    seed: `INSERT INTO p SELECT n FROM generate_series(1,${String(h.rows)})n`,
    mutation: "UPDATE p SET id=id+100",
    queries: [
      "SELECT * FROM p ORDER BY id",
      "SELECT * FROM log ORDER BY t,b,n",
      "SELECT * FROM w ORDER BY id",
    ],
  };
}

for (const seed of seedsFor("next-native-command-preparation", [49701, 49702]))
  it(`admits native trigger preparation before building it (${String(seed)})`, async () => {
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
          fc.constantFrom<Mode>("free", "release", "cancel", "oversize"),
          async (rows, bodies, wide, mode) => {
            reach.beginHistory();
            const h: History = { rows, bodies, wide, mode },
              s = script(h),
              core = new StorageCore(64),
              governor = new Governor(capBytes),
              stop = new AbortController(),
              violations: string[] = [];
            Object.defineProperty(core.harness, "governor", { value: governor });
            await core.open();
            let tx: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null,
              foreign: Grant | null = null,
              accounts: { metadata: Grant; rules: Grant; parameters: Grant } | null = null,
              preparing = false,
              builds = 0,
              waits = 0,
              reaches = 0,
              firstAdmitted: number | undefined = undefined,
              settled = false;
            try {
              await pg.exec("DROP TABLE IF EXISTS p,log,w CASCADE");
              for (const sql of [...s.postgres, s.seed]) await pg.exec(sql);
              for (const sql of [...s.native, s.seed]) await core.execute(sql);
              tx = await core.harness.path.begin();
              const catalog = tx.catalog,
                context = { ...core.context(), catalog },
                lowered = core.lowerer.lower(s.mutation, context);
              if (lowered.kind !== "modify") throw new TypeError("Missing mutation plan");
              const rootTableId = (lowered.plan.root as { readonly tableId: number }).tableId;
              // An allocation site runs only after its item's base share was charged to admitted
              // credit; the copies it makes are charged by the meter as they happen.
              let lastCharge = 0;
              const site = (what: string, bound: number) => {
                if (!preparing) return;
                builds++;
                if (!accounts) {
                  violations.push(`${what} before the root opened`);
                  return;
                }
                if (lastCharge < bound)
                  violations.push(
                    `${what} built after a ${String(lastCharge)}B charge < base ${String(bound)}B`,
                  );
              };
              const bodyBound = (text: string) => {
                for (const definition of catalog.triggers)
                  for (const body of definition.statements)
                    if (body.sql === text) return bodyPreparationBound(body, catalog);
                return Infinity;
              };
              const planner: Lowerer = Object.create(core.lowerer) as Lowerer;
              planner.lower = (text, ctx) => {
                site("body plan", bodyBound(text));
                return core.lowerer.lower(text, ctx);
              };
              planner.lowerTableRules = (table, view) => {
                site("rule expressions", rulePreparationBound(table));
                return core.lowerer.lowerTableRules(table, view);
              };
              const prepareReach = NativeCommandReach.prepare.bind(NativeCommandReach);
              vi.spyOn(NativeCommandReach, "prepare").mockImplementation(
                (view, root, charge, work) => {
                  reaches++;
                  firstAdmitted ??= accounts?.metadata.held;
                  return prepareReach(view, root, charge, work);
                },
              );
              const create = NativeCommandRoot.create.bind(NativeCommandRoot);
              vi.spyOn(NativeCommandRoot, "create").mockImplementation((scope) => {
                preparing = false;
                return create(scope);
              });
              // Every admission during preparation must begin with nothing of it held.
              const admit = governor.admitGroup.bind(governor);
              vi.spyOn(governor, "admitGroup").mockImplementation(async (request) => {
                if (preparing && request.label !== "foreign result" && accounts) {
                  waits++;
                  const held =
                    accounts.metadata.allocated -
                    NATIVE_ROOT_CREDIT_BYTES +
                    accounts.rules.allocated +
                    accounts.parameters.allocated;
                  if (held !== 0)
                    violations.push(`${request.label} admission began holding ${String(held)}B`);
                }
                const pending = admit(request);
                if (preparing && foreign && request.label !== "foreign result") {
                  // The root now waits holding nothing; release or cancel from outside.
                  const grant: Grant = foreign;
                  foreign = null;
                  queueMicrotask(() => {
                    if (mode === "cancel") stop.abort(new Error("cancel native preparation"));
                    grant.close();
                  });
                  reach.reach("waited holding nothing");
                }
                const grants = await pending;
                if (request.label === "native root open") {
                  const [metadata, , ruleGrant, parameters] = grants;
                  if (!metadata || !ruleGrant || !parameters) throw new Error("Missing accounts");
                  accounts = { metadata, rules: ruleGrant, parameters };
                  const allocate = metadata.allocate.bind(metadata);
                  metadata.allocate = (bytes: number) => {
                    const ok = allocate(bytes);
                    if (ok) lastCharge = bytes;
                    return ok;
                  };
                  preparing = true;
                  if (mode === "release" || mode === "cancel") {
                    const [grant] = await admit({
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
                  }
                }
                return grants;
              });
              const request = (transaction: NonNullable<typeof tx>, queryId: number) => ({
                transaction,
                rootId: queryId,
                plan: lowered.plan,
                parameters: [],
                planner,
                context,
                governor,
                signal: stop.signal,
                execution: {
                  reads: {
                    open: (
                      pages: Parameters<typeof ChunkReader.open>[0],
                      chunk: Parameters<typeof ChunkReader.open>[1],
                      options: Parameters<typeof ChunkReader.open>[2],
                    ) => ChunkReader.open(pages, chunk, options),
                  },
                  spill: core.spill,
                  queryId,
                  statementTime: 0n,
                  sequences: null,
                  sequenceSession: null,
                  returning: "atExecuted" as const,
                },
              });
              const completed = await executeNativeCommand(request(tx, 1)).then(
                (value) => ({ value }),
                (error: unknown) => ({ error }),
              );
              (foreign as Grant | null)?.close();
              foreign = null;
              preparing = false;
              vi.restoreAllMocks();
              expect(violations.join("\n"), "preparation admission laws").toBe("");
              // Doubling from the first admitted reservation (the static estimate, or the largest
              // reservation that fits beside the writer) reaches the cap within this many attempts.
              const estimate = Math.min(
                capBytes,
                NATIVE_ROOT_CREDIT_BYTES +
                  nativeCommandReachBytes(catalog) +
                  PREPARATION_INITIAL_FACTOR * closurePreparationBound(catalog, rootTableId).bytes,
              );
              const initial = (firstAdmitted as number | undefined) ?? estimate;
              expect(initial).toBeLessThanOrEqual(estimate);
              expect(reaches).toBeLessThanOrEqual(Math.ceil(Math.log2(capBytes / initial)) + 1);
              const refusal =
                "error" in completed && completed.error instanceof MemoryAdmissionError
                  ? completed.error
                  : null;
              if (mode === "oversize") {
                // The metered need grows past the cap: a typed 53200 only once it is above it.
                expect(refusal?.sqlState).toBe("53200");
                expect(refusal?.bytes ?? 0).toBeGreaterThan(refusal?.capacity ?? Infinity);
                tx.rollback();
                tx = null;
                reach.reach("above-cap refusal at the cap");
              } else if (refusal) {
                // Only the largest generated bodies at 32 MiB; never below the cap.
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
                expect(builds).toBeGreaterThan(0);
                reach.reach(mode === "release" ? "granted after release" : "no competing credit");
                // The same plan shape admits its learned working set in one attempt.
                tx = await core.harness.path.begin();
                let again = 0;
                const prepareAgain = NativeCommandReach.prepare.bind(NativeCommandReach);
                vi.spyOn(NativeCommandReach, "prepare").mockImplementation(
                  (view, root, charge, work) => {
                    again++;
                    return prepareAgain(view, root, charge, work);
                  },
                );
                const second = await executeNativeCommand(request(tx, 2));
                vi.restoreAllMocks();
                expect(second.rowCount).toBe(rows);
                expect(again).toBe(1);
                await pg.exec(s.mutation);
                const again2 = await tx.commit(null);
                await again2.durable;
                tx = null;
                reach.reach("learned working set in one attempt");
              }
              if (waits > 0) reach.reach("admissions checked");
              if (reaches > 1) reach.reach("released and retried");
              for (const query of s.queries) {
                const actual = await core.execute(query);
                if (!("batches" in actual)) throw new Error("Missing rows");
                expect(oracleRows(actual), query).toEqual((await pg.query(query)).rows);
              }
              reach.reach("exact PG rows");
              settled = true;
              if (mode !== "oversize") {
                if (bodies.some((terms) => terms.some((k) => k >= 600))) reach.reach("large body");
                if (bodies.length > 1) reach.reach("several triggers");
                if (wide >= 30) reach.reach("wide default rules");
              }
            } finally {
              (foreign as Grant | null)?.close();
              vi.restoreAllMocks();
              tx?.rollback();
              await core.close();
              // Only after a settled history: an earlier failure must surface, not its leak.
              if (settled) {
                expect(core.spill.bytesInUse).toBe(0);
                expect(governor.usedBytes).toBe(0);
              }
            }
            reach.reach("zero retained ownership");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            // The smallest body: its preparation plus the writer's floor fits even at 32 MiB, so
            // the free state is reached there; larger bodies may refuse, typed, above the cap.
            [1, [[1]], 0, "free"],
            // Seed 49701's statement-end deadlock (shrunk): the root held its preparation while
            // the statement end's FTS minimum waited for the writer (hold-and-wait), at 32 MiB.
            [1, [[243], [195]], 60, "free"],
            [1, [[243], [195]], 60, "release"],
            [1, [[650]], 0, "free"],
            [2, [[300], [200]], 60, "release"],
            [3, [[200], [1], [300, 2]], 40, "cancel"],
            [1, [[1]], 0, "oversize"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 600000);

// At 32 MiB the writer's next-step floor (19.5 MB: rows 4 MiB, commit buffers 6 MiB, merge
// 4.73 MB, metadata headroom, fixed caches) beside a large generated body's preparation (14.5 MB)
// exceeds the cap, so those histories refuse with 53200. Sizing the floor from the statement is
// the writer-floors follow-up; then every generated free history fits at 32 MiB.
it.todo("fits every generated free history at 32 MiB once writer floors are sized per statement");
