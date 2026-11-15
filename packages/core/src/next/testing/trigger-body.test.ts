/** Real prefix pins, native writers/row rules and PG rollback around an injected source host.
 * Source execution/registry remains B5b's separate implementation; this tests B6's body protocol.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_BODY_REACH } from "../../testing/reach.js";
import type { Snapshot } from "../access/snapshot.js";
import type { TableDef } from "../access/catalog.js";
import type { BoundParameters } from "../exec/executor.js";
import type { Statement } from "../access/modify.js";
import type { BoundTableRules } from "../exec/row-rules.js";
import { compileTableRules } from "../exec/rules.js";
import { ROW_IMAGE_UNREAD, type NativeRowHooks } from "../access/statement-command.js";
import { ColumnStore } from "../exec/column-store.js";
import { TriggerBodyRunner, type TriggerBodyHost } from "../exec/trigger-body.js";
import { constant } from "../exec/kernels/test-support.js";
import { isValidSlot } from "../exec/kernels/vectors.js";
import { batch, ints, locators, rowsOf } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { yieldTask } from "../task.js";

const MODES = [
  "ok",
  "source error",
  "cancel source",
  "apply error",
  "cancel apply",
  "retire error",
  "apply and retire error",
  "cancel image",
  "cancel before",
  "check",
  "cancel scope",
  "pending source cancel",
  "pending retirement cancel",
  "generated null",
] as const;
async function locateAll(pin: Snapshot, table: TableDef): Promise<Float64Array> {
  if (table.primaryKey === null) throw new Error("missing keyed fixture");
  const cursor = pin.index(table.id, table.primaryKey).range({ lower: null, upper: null }, "asc"),
    part = new Float64Array(16),
    out: number[] = [];
  try {
    for (;;) {
      const n = await cursor.next(part);
      if (n === 0) break;
      for (let i = 0; i < n; i++) out.push(part[i] ?? 0);
    }
  } finally {
    cursor.close();
  }
  return Float64Array.from(out);
}
const hooks: NativeRowHooks = { beforeRow: async () => undefined, afterRow: async () => undefined };
for (const seed of seedsFor("next-trigger-body", [0xb7d8, 0xb7d9]))
  it(`retires body input and pins without ending the root seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("bounded trigger body protocol", TRIGGER_BODY_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 12 }),
          fc.integer({ min: 1, max: 3 }),
          fc.integer({ min: 1, max: 23 }),
          fc.constantFrom(...MODES),
          async (count, size, n, mode) => {
            reach.beginHistory();
            const core = await new StorageCore(16).open(),
              cancel = new AbortController(),
              reason = new Error(mode),
              retireError = new Error("retire injected"),
              pins: Snapshot[] = [],
              value = mode === "check" ? -n - 1 : mode === "generated null" ? null : n;
            let store: ColumnStore | null = null,
              parameterScope: BoundParameters | null = null,
              held = 0,
              peak = 0,
              imageReads = 0,
              imageReleases = 0,
              spooled = false,
              applied = false,
              batches = 0,
              retired = 0;
            const closeInput = () => {
              store?.close();
              store = null;
            };
            const charge = {
              allocate: (bytes: number) => {
                held += bytes;
                peak = Math.max(held, peak);
                return true;
              },
              free: (bytes: number) => {
                held -= bytes;
                if (mode === "cancel scope" && spooled && !applied) cancel.abort(reason);
              },
            };
            let ruleHeld = 0;
            const ruleCharge = {
              allocate: (bytes: number) => {
                ruleHeld += bytes;
                return true;
              },
              free: (bytes: number) => {
                ruleHeld -= bytes;
              },
            };
            let activeRules: ((id: number) => BoundTableRules) | null = null;
            const nativeWrite = async (work: (statement: Statement) => Promise<void>) => {
              const tx = await core.harness.path.begin(),
                bound = new Map<number, BoundTableRules>();
              const rules = (id: number): BoundTableRules => {
                let rule = bound.get(id);
                if (!rule) {
                  const table = statement.catalog.table(id);
                  if (!table) throw new Error("missing row-rule table");
                  rule = compileTableRules(
                    table,
                    statement.catalog.version,
                    core.lowerer.lowerTableRules(table, statement.catalog),
                    { charge: ruleCharge, capacity: 16 },
                  );
                  bound.set(id, rule);
                }
                return rule;
              };
              const statement = tx.statement({ rules });
              activeRules = rules;
              try {
                await work(statement);
                await statement.end();
                await (
                  await tx.commit(null)
                ).durable;
              } catch (error) {
                statement.rollback();
                tx.rollback();
                throw error;
              } finally {
                for (const rule of bound.values()) rule.close();
                activeRules = null;
              }
            };
            try {
              const ddl = [
                "CREATE TABLE p(id INT PRIMARY KEY,n INT)",
                "CREATE TABLE t(id INT PRIMARY KEY,v INT,g INT GENERATED ALWAYS AS(v+1) STORED NOT NULL,CONSTRAINT ck CHECK(g>0))",
              ];
              await pg.exec(`DROP TABLE IF EXISTS t,p;${ddl.join(";")}`);
              for (const sql of ddl) await core.execute(sql);
              const data = Array.from({ length: count }, (_, i) => ({ id: i + 1, v: 0 }));
              await nativeWrite(async (statement) => {
                const parent = statement.catalog.tableByName("p"),
                  target = statement.catalog.tableByName("t");
                if (!parent || !target || !activeRules) throw new Error("missing native setup");
                await statement.table(parent.id).insert(batch(ints([1]), ints([0])), null);
                const input = batch(
                  ints(data.map((r) => r.id)),
                  ints(data.map((r) => r.v)),
                  ints(data.map(() => 0)),
                );
                activeRules(target.id).generate(
                  input,
                  target.columns.filter((c) => c.generatedSql === null).map((c) => c.id),
                );
                await statement.table(target.id).insert(input, null);
              });
              await pg.exec("INSERT INTO p VALUES(1,0)");
              await pg.query("INSERT INTO t(id,v) SELECT n,0 FROM generate_series(1,$1::int)n", [
                count,
              ]);
              const declaration = core.lowerer.lower(
                "CREATE TRIGGER tr AFTER UPDATE ON p BEGIN UPDATE t SET v=NEW.n; END",
                core.context(),
              );
              if (declaration.kind !== "ddl" || declaration.change?.kind !== "createTrigger")
                throw new Error("missing body declaration");
              const body = declaration.change.trigger.statements[0],
                p = core.harness.path.catalog.tableByName("p"),
                t = core.harness.path.catalog.tableByName("t");
              if (!body || !p || !t) throw new Error("missing body tables");
              const lowered = core.lowerer.lower(body.sql, {
                ...core.context(),
                parameterTypes: body.bindings.map((b) => b.type),
              });
              if (lowered.kind !== "modify" || lowered.plan.root.op !== "modify")
                throw new Error("missing lowered body");
              const marker = { terminalErrorPosition: 2 },
                marked = { ...lowered.plan, root: { ...lowered.plan.root, bodyProbe: marker } },
                runner = new TriggerBodyRunner(p, body, marked),
                command = { rootId: 7, id: 2, frameId: 2 },
                v = t.columns.find((c) => c.name === "v"),
                source = ints([value]);
              if (!v) throw new Error("missing v");
              // Every generated history checks all finite preparation corruptions. None may
              // reach a source factory, pin or writer; the native provider receives typed IR.
              for (const [candidateBody, candidatePlan] of [
                [{ ...body, targetTableId: p.id }, marked],
                [body, { ...marked, placeholderCount: marked.placeholderCount + 1 }],
                [body, { ...marked, parameters: [{ kind: "text" } as const] }],
                [body, { ...marked, root: marked.root.input }],
              ] as const)
                expect(() => new TriggerBodyRunner(p, candidateBody, candidatePlan)).toThrow(
                  expect.objectContaining({ code: "MINNOW_TRIGGER_BODY_STATE", sqlState: "XX000" }),
                );
              reach.reach("invalid programs refuse before effects");
              const host: TriggerBodyHost = {
                spoolBodyInput: async (request) => {
                  expect(request.command).toBe(command);
                  expect(request.body).toBe(body);
                  expect(imageReleases).toBe(1);
                  expect(request.inputView.released).toBe(false);
                  if (request.plan.root.op !== "modify")
                    throw new Error("lost complete modification");
                  expect(request.plan.root.onRepeat).toBe("error");
                  expect(request.plan.root.input).toBe(marked.root.input);
                  expect("bodyProbe" in request.plan.root && request.plan.root.bodyProbe).toBe(
                    marker,
                  );
                  expect(await rowsOf(request.inputView, p.id)).toEqual([[1, value]]);
                  parameterScope = request.parameters;
                  const scalar = parameterScope.slots[0];
                  if (!scalar) throw new Error("missing binding");
                  const at = await locateAll(request.inputView, t);
                  store = new ColumnStore([{ kind: "locator", tableId: t.id }, v.type], charge, {
                    chunkRows: 16,
                    varlenBytes: 64,
                  });
                  expect(
                    store.append(
                      [locators(Array.from(at), t.id), constant(scalar, count)],
                      null,
                      0,
                      count,
                    ),
                  ).toBe(count);
                  spooled = true;
                  reach.reach("prefix input sealed");
                  reach.reach("complete modify metadata");
                  if (mode === "source error") {
                    store.close();
                    store = null;
                    throw reason;
                  }
                  if (mode === "cancel source") cancel.abort(reason);
                  if (mode === "pending source cancel") {
                    await yieldTask();
                    cancel.abort(reason);
                    expect(request.inputView.released).toBe(false);
                    expect(request.parameters.slots.length).toBe(1);
                    expect(held).toBeGreaterThan(runner.minimumBytes);
                    reach.reach("pending source cancellation fenced");
                  }
                  return { rootId: command.rootId, id: 1 };
                },
                applyBodyInput: async (request) => {
                  expect(parameterScope).not.toBeNull();
                  const closed = parameterScope;
                  if (!closed) throw new Error("missing parameter scope");
                  expect(() => closed.slots).toThrow("closed");
                  reach.reach("binding copy retired before write");
                  expect(request.hooks).toBe(hooks);
                  expect(request.inputView.released).toBe(false);
                  if (!store) throw new Error("missing input");
                  applied = true;
                  const columns = store.vectors(),
                    writer = root.table(t.id, {
                      assigned: t.columns
                        .filter(
                          (c) => body.assignedColumnIds.includes(c.id) || c.generatedSql !== null,
                        )
                        .map((c) => c.id),
                      inputView: request.inputView,
                      onRepeat: "error",
                    });
                  for (let from = 0; from < count; from += size) {
                    request.signal.throwIfAborted();
                    const end = Math.min(count, from + size),
                      at = columns[0],
                      values = columns[1];
                    if (at?.data.layout !== "f64" || values?.data.layout !== "i32")
                      throw new Error("bad sealed input");
                    if (!activeRules) throw new Error("row rules not bound");
                    const valueData = values.data;
                    const candidate = batch(
                      ints(Array.from({ length: end - from }, (_, i) => from + i + 1)),
                      ints(
                        Array.from({ length: end - from }, (_, i) =>
                          isValidSlot(values, from + i) ? (valueData.values[from + i] ?? 0) : null,
                        ),
                      ),
                      ints(Array.from({ length: end - from }, () => 0)),
                    );
                    activeRules(t.id).generate(candidate, body.assignedColumnIds);
                    const updated = candidate.columns[1],
                      generated = candidate.columns[2];
                    if (updated?.form !== "flat" || generated?.form !== "flat")
                      throw new Error("missing generated result");
                    await writer.update(
                      batch(
                        locators(Array.from(at.data.values.subarray(from, end)), t.id),
                        updated,
                        generated,
                      ),
                      null,
                    );
                    batches++;
                    expect(() => request.inputView.index(t.id, t.primaryKey ?? 0)).toThrow(
                      "locator-only",
                    );
                    if (mode === "apply error" || mode === "apply and retire error") throw reason;
                    if (mode === "cancel apply") cancel.abort(reason);
                  }
                  request.signal.throwIfAborted();
                },
                releaseBodyInput: async (token, input) => {
                  expect(token).toBe(command);
                  expect(input).toEqual({ rootId: 7, id: 1 });
                  expect(pins.at(-1)?.released).toBe(false);
                  retired++;
                  if (mode === "pending retirement cancel") {
                    await yieldTask();
                    cancel.abort(reason);
                    expect(pins.at(-1)?.released).toBe(false);
                    expect(held).toBeGreaterThan(0);
                    reach.reach("pending retirement cancellation fenced");
                  }
                  store?.close();
                  store = null;
                  if (mode === "retire error" || mode === "apply and retire error")
                    throw retireError;
                },
              };
              let root: Parameters<Parameters<StorageCore["write"]>[0]>[0];
              await pg.exec("BEGIN");
              await pg.query("UPDATE p SET n=$1", [value]);
              let pgState: string | undefined;
              try {
                await pg.query("UPDATE t SET v=$1", [value]);
              } catch (error) {
                pgState = (error as { code?: string }).code;
              }
              let failure: unknown;
              try {
                await nativeWrite(async (statement) => {
                  root = statement;
                  const at = await locateAll(root.view, p);
                  const nc = p.columns.find((c) => c.name === "n");
                  if (!nc) throw new Error("missing n");
                  await root
                    .table(p.id, { assigned: [nc.id] })
                    .update(batch(locators(Array.from(at), p.id), ints([value])), null);
                  if (mode === "cancel before") cancel.abort(reason);
                  try {
                    await runner.run(
                      command,
                      { spoolId: 1, row: 0 },
                      {
                        statement: {
                          catalog: root.catalog,
                          readOwn: () => {
                            const pin = root.readOwn();
                            pins.push(pin);
                            return pin;
                          },
                        },
                        native: {
                          readRowBindings: async (_ref, projection) => {
                            imageReads++;
                            expect(projection.new).toEqual(body.bindings.map((b) => b.columnId));
                            return {
                              tableId: p.id,
                              old: null,
                              new: p.columns.map((c) =>
                                c.name === "n" ? source : ROW_IMAGE_UNREAD,
                              ),
                              release: () => {
                                imageReleases++;
                                if (source.data.layout === "i32") source.data.values.fill(0);
                                if (mode === "cancel image") cancel.abort(reason);
                              },
                            };
                          },
                        },
                        host,
                        hooks,
                        charge,
                        work: { signal: cancel.signal },
                      },
                    );
                  } finally {
                    expect(root.view.released).toBe(false);
                  }
                });
              } catch (error) {
                failure = error;
              }
              if (mode === "ok") expect(failure).toBeUndefined();
              else {
                expect(failure).toBeDefined();
                if (mode === "check" || mode === "generated null") {
                  expect(failure).toMatchObject({ sqlState: pgState });
                  expect(pgState).toBe(mode === "check" ? "23514" : "23502");
                  if (mode === "generated null") {
                    expect(failure).toMatchObject({
                      columnId: t.columns.find((c) => c.name === "g")?.id,
                    });
                    reach.reach("native generated NOT NULL matches PG");
                  } else reach.reach("native rules and PG constraint");
                } else if (mode === "retire error" || mode === "apply and retire error") {
                  expect(failure).toBeInstanceOf(AggregateError);
                  const aggregate = failure as AggregateError;
                  expect(aggregate.errors).toContain(retireError);
                  if (mode === "apply and retire error") {
                    expect(aggregate.cause, "earlier body failure remains cause").toBe(reason);
                    reach.reach("primary and cleanup errors retained");
                  }
                } else expect(failure).toBe(reason);
              }
              await pg.exec(failure ? "ROLLBACK" : "COMMIT");
              for (const table of ["p", "t"]) {
                const sql = `SELECT * FROM ${table} ORDER BY id`,
                  actual = await core.execute(sql);
                if (!("batches" in actual)) throw new Error("missing rows");
                expect(oracleRows(actual)).toEqual((await pg.query(sql)).rows);
              }
              for (const pin of pins) expect(pin.released).toBe(true);
              expect(held, "body copy/input credit returned").toBe(0);
              expect(ruleHeld).toBe(0);
              expect(peak).toBeLessThan(32768);
              expect(retired).toBe(
                mode === "source error" || mode === "cancel before" || mode === "cancel image"
                  ? 0
                  : 1,
              );
              expect(imageReads).toBe(mode === "cancel before" ? 0 : 1);
              expect(imageReleases).toBe(imageReads);
              if (mode === "ok") {
                reach.reach("PG commit");
                if (batches > 1) reach.reach("multiple writer batches");
              } else reach.reach("PG root rollback");
              if (mode === "source error") reach.reach("source failure");
              if (mode === "apply error") reach.reach("apply failure");
              if (mode === "cancel source") reach.reach("cancel sealed input");
              if (mode === "cancel apply") reach.reach("cancel partial write");
              if (mode === "cancel image") reach.reach("cancel image release");
              if (mode === "cancel scope") reach.reach("cancel binding retirement");
              if (mode === "cancel before") reach.reach("cancel before source");
              reach.reach("pin survives input retirement");
              reach.reach("zero owned copies and readers");
              expect(core.harness.path.shared.readers.count).toBe(0);
            } finally {
              closeInput();
              for (const pin of pins) pin.release();
              await core.close();
              expect(held, "body copy/input credit returned").toBe(0);
              await pg.exec("ROLLBACK");
            }
          },
        ),
        // fast-check counts explicit examples inside numRuns. Fund every control plus
        // eight random histories, so the later fault modes cannot crowd out random generation.
        { seed, numRuns: MODES.length + 8, examples: MODES.map((mode) => [8, 2, 3, mode]) },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);

it("refuses a stale body before opening a pin or prepaid helper", async () => {
  const core = await new StorageCore().open();
  try {
    await core.execute("CREATE TABLE p(id INT PRIMARY KEY,n INT)");
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT)");
    const declaration = core.lowerer.lower(
      "CREATE TRIGGER tr AFTER UPDATE ON p BEGIN UPDATE t SET v=NEW.n; END",
      core.context(),
    );
    if (declaration.kind !== "ddl" || declaration.change?.kind !== "createTrigger")
      throw new Error("missing declaration");
    const body = declaration.change.trigger.statements[0],
      owner = core.harness.path.catalog.tableByName("p");
    if (!body || !owner) throw new Error("missing owner/body");
    const lowered = core.lowerer.lower(body.sql, {
      ...core.context(),
      parameterTypes: body.bindings.map((b) => b.type),
    });
    if (lowered.kind !== "modify") throw new Error("missing modification");
    const runner = new TriggerBodyRunner(owner, body, lowered.plan);
    await core.execute("CREATE TABLE later(n INT)");
    let calls = 0;
    const unexpected = async () => {
      calls++;
      throw new Error("stale body reached provider");
    };
    await core.write(async (root) => {
      await expect(
        runner.run(
          { rootId: 1, id: 2, frameId: 2 },
          { spoolId: 1, row: 0 },
          {
            statement: {
              catalog: root.catalog,
              readOwn: () => {
                calls++;
                throw new Error("stale body opened pin");
              },
            },
            native: { readRowBindings: unexpected },
            host: {
              spoolBodyInput: unexpected,
              applyBodyInput: unexpected,
              releaseBodyInput: unexpected,
            },
            hooks,
            charge: {
              allocate: () => {
                calls++;
                return false;
              },
              free: () => {
                calls++;
              },
            },
            work: { signal: new AbortController().signal },
          },
        ),
      ).rejects.toMatchObject({ code: "MINNOW_PLAN_STALE", sqlState: "0A000" });
    });
    expect(calls).toBe(0);
    expect(core.harness.path.shared.readers.count).toBe(0);
  } finally {
    await core.close();
  }
});
