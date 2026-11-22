/**
 * A publication's point of no return is the durable switch to its new root. After it, the state
 * that names that root must publish, so nothing there may be refused: a refused metadata charge
 * after the switch cannot be retried, and it failed the write path ("The log reset after a
 * recycle's publish failed" → WritePathFailedError: a recycle pinned its new state under metadata
 * pressure). Every publication funds its post-switch pins before the switch
 * (write/engine.ts `#fundPins`).
 *
 * The tests refuse every metadata charge a publication makes after its switch, the worst
 * metadata pressure it can meet there, while workloads drive each publication path: flushes,
 * index merges, log recycles, FTS staging and the idle unchanged publish. Nothing may fail, and
 * the path must stay usable.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createReach, PUBLICATION_PRESSURE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ftsDef } from "../access/fts-def.js";
import { MetadataBudget, MetadataPressure } from "../write/budget.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  tableInput,
} from "../write/test-support.js";
import type { WritePathImpl } from "../write/engine.js";
import { IDLE_FLUSH_MS } from "../write/log.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

// The original method, restored after each test; it is always called with its budget as `this`.
const allocate = Object.getOwnPropertyDescriptor(MetadataBudget.prototype, "allocate")?.value as (
  this: MetadataBudget,
  bytes: number,
) => boolean;
afterEach(() => {
  MetadataBudget.prototype.allocate = allocate;
});

/** The write path's publications (engine.ts): each switches to a new root, then publishes. */
const PUBLICATIONS = [
  "#recycleLog",
  "#writeFlush",
  "#mergePinned",
  "#publishUnchanged",
  "#stageFts",
] as const;

/**
 * Refuses every metadata charge a publication makes between its durable switch and the end of
 * its post-switch section (engine tuning `publicationSwitch`). Publications take one lock to
 * switch, so at most one is past its switch at a time; its own allocations are refused, while
 * other work running meanwhile (another publication's preparation) meets ordinary pressure. The
 * first refusal rejects `firstRefusal`, naming the publication: a refused post-switch step can
 * otherwise leave its publication retrying forever.
 */
function refuseAfterSwitch() {
  const switchedNow: string[] = [];
  const refused: string[] = [];
  let refuse: (error: Error) => void = () => undefined;
  const firstRefusal = new Promise<never>((_, reject) => {
    refuse = reject;
  });
  firstRefusal.catch(() => undefined);
  const counts = { switches: 0 };
  MetadataBudget.prototype.allocate = function (bytes: number) {
    const current = switchedNow[switchedNow.length - 1];
    if (current !== undefined && bytes > 0 && (new Error().stack ?? "").includes(current)) {
      refused.push(current);
      refuse(new Error(`${current} allocated ${String(bytes)} bytes after its switch`));
      throw new MetadataPressure(bytes, bytes);
    }
    return allocate.call(this, bytes);
  };
  const publicationSwitch = (phase: "switched" | "settled"): void => {
    if (phase === "settled") {
      switchedNow.pop();
      return;
    }
    const stack = new Error().stack ?? "";
    const publication = PUBLICATIONS.find((name) => stack.includes(name));
    if (publication === undefined) throw new Error(`An unknown publication switched: ${stack}`);
    counts.switches++;
    switchedNow.push(publication);
  };
  const step = <T>(work: Promise<T>): Promise<T> => Promise.race([work, firstRefusal]);
  return { refused, counts, publicationSwitch, step };
}

it("funds every post-switch pin before the switch: no publication fails after its root is durable", async () => {
  const { refused, counts, publicationSwitch, step } = refuseAfterSwitch();
  const core = new StorageCore(8, {
    logRecycleBytes: 32 * 1024,
    merges: { mode: "auto" },
    publicationSwitch,
  });
  await core.open();
  const sql = (text: string) => step(core.execute(text));
  try {
    await sql("CREATE TABLE t(id INT PRIMARY KEY, v TEXT, n INT)");
    await sql("CREATE INDEX t_n ON t(n)");
    await sql("CREATE TABLE docs(id INT PRIMARY KEY, body TEXT)");
    const docs = core.harness.path.catalog.tableByName("docs");
    const body = docs?.columns.find((column) => column.name === "body");
    if (docs === undefined || body === undefined) throw new Error("missing docs");
    const { id: _id, ...fts } = ftsDef(1, docs.id, body.id);
    void _id;
    await step(
      autocommit(core.harness.path, (statement) =>
        statement.changeCatalog({ kind: "createFts", fts }),
      ),
    );
    for (let i = 0; i < 300; i++) {
      await sql(`INSERT INTO t VALUES (${String(i)}, 'v${String(i)}', ${String(i % 17)})`);
      if (i % 5 === 0) await sql(`UPDATE t SET n = n + 1 WHERE id = ${String(i - 3)}`);
      if (i % 7 === 0) await sql(`DELETE FROM t WHERE id = ${String(i - 6)}`);
      if (i % 3 === 0)
        await sql(`INSERT INTO docs VALUES (${String(i)}, 'word${String(i % 11)} other')`);
    }
    await step(core.harness.path.runMaintenance());
    const maintenance = core.harness.path.maintenance;
    // Every publication path ran, under refusal after its switch.
    expect(maintenance.flushes).toBeGreaterThan(0);
    expect(maintenance.merges).toBeGreaterThan(0);
    expect(maintenance.recycles).toBeGreaterThan(0);
    expect(counts.switches).toBeGreaterThan(maintenance.flushes);
    // Nothing a publication did after its switch was ever refused.
    expect(refused).toEqual([]);
    // The path is healthy and every acknowledged write is there.
    const rows = await sql("SELECT count(*) AS n FROM t");
    if (!("batches" in rows)) throw new Error("rows");
    const deleted = Array.from({ length: 300 }, (_, i) => i).filter((i) => i % 7 === 0 && i >= 6);
    expect(oracleRows(rows).map((row) => Number(row.n))).toEqual([300 - deleted.length]);
    await sql("INSERT INTO t VALUES (1000, 'after', 1)");
  } finally {
    MetadataBudget.prototype.allocate = allocate;
    await core.close();
  }
}, 120_000);

async function oneTable(path: WritePathImpl): Promise<number> {
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  return t;
}

it("funds the idle publication's state pin before its switch", async () => {
  const { refused, publicationSwitch, step } = refuseAfterSwitch();
  // As write/extent-deltas.test.ts drives an idle publish: any retired space is over the bound.
  const harness = new Harness("strict", "memory", 4, undefined, {
    merges: { mode: "manual", fanIn: 2 },
    idleGarbageBytes: 1,
    publicationSwitch,
  });
  const path = await harness.open();
  try {
    const t = await oneTable(path);
    const insert = (ids: number[]) =>
      step(autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null)));
    await insert([1, 2, 3, 4]);
    await insert([5]);
    await step(path.quiesce());
    await insert([6, 7, 8]);
    await insert([9]);
    await step(path.quiesce());
    await step(new Promise((resolve) => setTimeout(resolve, 3 * IDLE_FLUSH_MS)));
    await step(path.quiesce());
    expect(path.maintenance.idlePublishes).toBeGreaterThan(0);
    expect(refused).toEqual([]);
    expect(path.failure).toBeNull();
  } finally {
    MetadataBudget.prototype.allocate = allocate;
    await harness.close();
  }
});

/**
 * Value widths, in characters: small, page-sized and a 1 MB row. Wider rows meet a separate,
 * pre-existing defect first: from about 1.3 MB a statement's fixed buffers are sized to nearly
 * all the memory free at sizing (about 30 MB here), and once the writer's floors grow by a few KB
 * that request can never be served (a self-wait, reported with item 3). They would test that
 * instead of the publications.
 */
const WIDTHS = [8, 4096, 200_000, 1_000_000] as const;
const op = fc.oneof(
  fc.record({ kind: fc.constant("insert" as const), width: fc.constantFrom(...WIDTHS) }),
  fc.record({ kind: fc.constant("update" as const), width: fc.constantFrom(...WIDTHS) }),
  fc.record({ kind: fc.constant("delete" as const) }),
  fc.record({ kind: fc.constant("recycle" as const) }),
  fc.record({ kind: fc.constant("maintenance" as const) }),
);

for (const seed of seedsFor("next-publication-pressure", [0x9b11, 0x9b12]))
  it(`keeps every acknowledged write under metadata pressure on each publication (seed ${String(seed)})`, async () => {
    const reach = createReach("publication pressure", PUBLICATION_PRESSURE_REACH);
    // A wait no holder will ever end becomes a named failure, not a hang.
    const stall = Governor.stallAfterMs;
    Governor.stallAfterMs = 10_000;
    await fc
      .assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.array(op, { minLength: 4, maxLength: 24 }),
          async (refuse, ops) => {
            reach.beginHistory();
            reach.reach(refuse ? "refused after switch" : "small cap only");
            const pressure = refuseAfterSwitch();
            if (!refuse) MetadataBudget.prototype.allocate = allocate;
            const core = new StorageCore(8, {
              logRecycleBytes: 64 * 1024,
              merges: { mode: "manual", fanIn: 2 },
              publicationSwitch: pressure.publicationSwitch,
            });
            // A small cap: large rows leave the metadata budget little room to grow.
            Object.defineProperty(core.harness, "governor", { value: new Governor(32 << 20) });
            await core.open();
            const model = new Map<number, string>();
            let next = 0;
            try {
              await pressure.step(
                core.execute("CREATE TABLE t(id INT PRIMARY KEY, n INT, v TEXT)"),
              );
              // Index runs (merges) on a narrow column: PostgreSQL refuses btree keys above
              // about 2.7 KB, so a wide text column is never an index key here either.
              await pressure.step(core.execute("CREATE INDEX t_n ON t(n)"));
              for (const step of ops) {
                const before = core.harness.path.maintenance;
                const counts = [before.flushes, before.recycles, before.merges];
                let sql: string | null = null;
                let params: unknown[] = [];
                let apply: () => void = () => undefined;
                if (step.kind === "insert") {
                  const id = next++,
                    value = String.fromCharCode(97 + (id % 26)).repeat(step.width);
                  sql = "INSERT INTO t VALUES ($1, $1 % 5, $2)";
                  params = [id, value];
                  apply = () => model.set(id, value);
                } else if (step.kind === "update" && model.size > 0) {
                  const id = [...model.keys()][next % model.size] ?? 0,
                    value = "u".repeat(step.width);
                  sql = "UPDATE t SET v = $2, n = n + 1 WHERE id = $1";
                  params = [id, value];
                  apply = () => model.set(id, value);
                } else if (step.kind === "delete" && model.size > 0) {
                  const id = [...model.keys()][0] ?? 0;
                  sql = "DELETE FROM t WHERE id = $1";
                  params = [id];
                  apply = () => model.delete(id);
                }
                if (step.kind === "recycle") await pressure.step(core.harness.path.recycleLog());
                else if (step.kind === "maintenance")
                  await pressure.step(core.harness.path.runMaintenance());
                else if (sql !== null) {
                  if ("width" in step && step.width >= 200_000) reach.reach("wide row");
                  const outcome = await pressure.step(core.execute(sql, params as never)).then(
                    () => null,
                    (error: unknown) => error,
                  );
                  if (outcome === null) apply();
                  else {
                    // Only a typed out-of-memory refusal; the statement then wrote nothing.
                    expect(outcome, sql).toMatchObject({ sqlState: "53200" });
                  }
                }
                const after = core.harness.path.maintenance;
                if (after.flushes > (counts[0] ?? 0)) reach.reach("flush");
                if (after.recycles > (counts[1] ?? 0)) reach.reach("recycle");
                if (after.merges > (counts[2] ?? 0)) reach.reach("merge");
                expect(core.harness.path.failure).toBeNull();
                // Every acknowledged write, and nothing else, is there.
                const rows = await pressure.step(core.execute("SELECT id, v FROM t ORDER BY id"));
                if (!("batches" in rows)) throw new Error("rows");
                const stored = oracleRows(rows).map((row) => [Number(row.id), String(row.v)]);
                const wanted = [...model].sort((a, b) => a[0] - b[0]);
                expect(stored.length).toBe(wanted.length);
                for (const [k, [id, value]] of stored.entries()) {
                  expect(id).toBe(wanted[k]?.[0]);
                  expect(value === wanted[k]?.[1], `row ${String(id)}`).toBe(true);
                }
              }
              // Under the same pressure: two flushes leave two index runs, which a merge publishes.
              const merges = core.harness.path.maintenance.merges;
              for (let round = 0; round < 2; round++) {
                const rows = Array.from({ length: 9 }, () => {
                  const id = next++;
                  return `(${String(id)}, ${String(id % 5)}, 'm')`;
                });
                await pressure.step(core.execute(`INSERT INTO t VALUES ${rows.join(", ")}`));
                for (let k = rows.length; k > 0; k--) model.set(next - k, "m");
                await pressure.step(core.harness.path.quiesce());
              }
              await pressure.step(core.harness.path.runMaintenance());
              if (core.harness.path.maintenance.merges > merges) reach.reach("merge");
              expect(core.harness.path.failure).toBeNull();
              const final = await pressure.step(core.execute("SELECT count(*) AS n FROM t"));
              if (!("batches" in final)) throw new Error("rows");
              expect(oracleRows(final).map((row) => Number(row.n))).toEqual([model.size]);
              expect(pressure.refused).toEqual([]);
            } finally {
              MetadataBudget.prototype.allocate = allocate;
              await core.close();
            }
          },
        ),
        { seed, numRuns: 12 },
      )
      .finally(() => {
        Governor.stallAfterMs = stall;
      });
    reach.assertReached();
  }, 600_000);
