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
import { AsyncLocalStorage } from "node:async_hooks";
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createReach, PUBLICATION_PRESSURE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ftsDef } from "../access/fts-def.js";
import { MetadataBudget, MetadataPressure } from "../write/budget.js";
import { pinBytes } from "../storage/root-pins.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  tableInput,
} from "../write/test-support.js";
import type { PublicationKind, WritePathImpl } from "../write/engine.js";
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
/**
 * Refuses every metadata charge a publication makes between its durable switch and the end of
 * its post-switch section. The engine names both ends and the publication (tuning
 * `publicationSwitch`); the "switched" call enters that publication's async context here, so only
 * charges made in its own continuation are refused, while other work running meanwhile (another
 * publication's preparation, a statement) meets ordinary pressure. Each publication's "started"
 * call clears the context it inherited from whatever scheduled it. The first refusal rejects
 * `firstRefusal`, naming the publication: a refused post-switch step can otherwise leave its
 * publication retrying forever. Each publication's first pin funding is also refused once
 * (tuning `beforeFundPins`): it must unwind and retry, leaving no charge behind.
 */
function refuseAfterSwitch() {
  const inside = new AsyncLocalStorage<PublicationKind | undefined>();
  const switchedNow = new Set<PublicationKind>();
  const refused: string[] = [];
  let refuse: (error: Error) => void = () => undefined;
  const firstRefusal = new Promise<never>((_, reject) => {
    refuse = reject;
  });
  firstRefusal.catch(() => undefined);
  const counts = { switches: 0 };
  const fundingRefused = new Set<PublicationKind>();
  const metadata = {
    budget: null as MetadataBudget | null,
    /** At rest the grant holds the resident metadata plus the current state's pin, exactly. */
    measure(path: { readonly residentMetadataBytes: number }): number {
      if (this.budget === null) throw new Error("no metadata budget seen");
      return this.budget.grant.allocated - path.residentMetadataBytes;
    },
  };
  MetadataBudget.prototype.allocate = function (bytes: number) {
    // The write path's one metadata budget.
    metadata.budget ??= this;
    const kind = inside.getStore();
    if (kind !== undefined && switchedNow.has(kind) && bytes > 0) {
      refused.push(kind);
      refuse(
        new Error(`the ${kind} publication allocated ${String(bytes)} bytes after its switch`),
      );
      throw new MetadataPressure(bytes, bytes);
    }
    return allocate.call(this, bytes);
  };
  const publicationSwitch = (
    phase: "started" | "switched" | "settled",
    kind: PublicationKind,
  ): void => {
    // A publication's own work is not the one that spawned it: a flush scheduled from inside
    // another publication's post-switch section starts here with no inherited attribution.
    if (phase === "started") {
      inside.enterWith(undefined);
      return;
    }
    if (phase === "settled") {
      switchedNow.delete(kind);
      inside.enterWith(undefined);
      return;
    }
    counts.switches++;
    switchedNow.add(kind);
    inside.enterWith(kind);
  };
  const beforeFundPins = (kind: PublicationKind): void => {
    if (fundingRefused.has(kind)) return;
    fundingRefused.add(kind);
    throw new MetadataPressure(1, 1);
  };
  const step = <T>(work: Promise<T>): Promise<T> => Promise.race([work, firstRefusal]);
  return { refused, counts, publicationSwitch, beforeFundPins, step, fundingRefused, metadata };
}

it("funds every post-switch pin before the switch: no publication fails after its root is durable", async () => {
  const { refused, counts, publicationSwitch, beforeFundPins, step, fundingRefused, metadata } =
    refuseAfterSwitch();
  const core = new StorageCore(8, {
    logRecycleBytes: 32 * 1024,
    merges: { mode: "auto" },
    publicationSwitch,
    beforeFundPins,
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
    // Each refused funding unwound completely: at rest the metadata grant holds exactly the
    // resident metadata plus the current state's pin, as before any publication.
    expect([...fundingRefused].sort()).toEqual(["flush", "fts", "merge", "recycle"]);
    await step(core.harness.path.quiesce());
    expect(metadata.measure(core.harness.path)).toBe(pinBytes("current state"));
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
  const { refused, publicationSwitch, beforeFundPins, step, fundingRefused, metadata } =
    refuseAfterSwitch();
  // As write/extent-deltas.test.ts drives an idle publish: any retired space is over the bound.
  const harness = new Harness("strict", "memory", 4, undefined, {
    merges: { mode: "manual", fanIn: 2 },
    idleGarbageBytes: 1,
    publicationSwitch,
    beforeFundPins,
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
    // The idle publish runs on a timer once the path has been idle for IDLE_FLUSH_MS: wait for
    // it (bounded), not for a fixed time a slow runner may not meet.
    for (let waited = 0; path.maintenance.idlePublishes === 0 && waited < 100; waited++)
      await step(new Promise((resolve) => setTimeout(resolve, IDLE_FLUSH_MS)));
    await step(path.quiesce());
    expect(path.maintenance.idlePublishes).toBeGreaterThan(0);
    expect(refused).toEqual([]);
    expect(path.failure).toBeNull();
    expect(fundingRefused.has("idle")).toBe(true);
    await step(path.quiesce());
    expect(metadata.measure(path)).toBe(pinBytes("current state"));
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
              ...(refuse ? { beforeFundPins: pressure.beforeFundPins } : {}),
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

/**
 * Astra's reproducer (#50 review): a flush's page-index growth was charged before its pin
 * funding, so a refused funding left that growth charged. Identical workloads, one with the
 * flush's first funding refused, must end with identical metadata holdings.
 */
it("refunds a flush's page-index charge when its pin funding is refused", async () => {
  const run = async (refuse: boolean) => {
    const seen: { budget: MetadataBudget | null } = { budget: null };
    let refused = 0;
    MetadataBudget.prototype.allocate = function (bytes: number) {
      // The write path's one metadata budget.
      seen.budget ??= this;
      return allocate.call(this, bytes);
    };
    const core = new StorageCore(8, {
      merges: { mode: "manual" },
      // The first flush's pin funding is refused, as metadata pressure refuses it.
      beforeFundPins: (kind) => {
        if (refuse && kind === "flush" && refused++ === 0) throw new MetadataPressure(1, 1);
      },
    });
    try {
      await core.open();
      await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)");
      for (let i = 0; i < 40; i++)
        await core.execute(`INSERT INTO t VALUES (${String(i)}, ${String(i)})`);
      await core.harness.path.quiesce();
      if (seen.budget === null) throw new Error("no metadata was charged");
      return { allocated: seen.budget.grant.allocated, refused };
    } finally {
      MetadataBudget.prototype.allocate = allocate;
      await core.close();
    }
  };
  const base = await run(false);
  const pressure = await run(true);
  expect(pressure.refused).toBeGreaterThan(0);
  expect(pressure.allocated).toBe(base.allocated);
});
