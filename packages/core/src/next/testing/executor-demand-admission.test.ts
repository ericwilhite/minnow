/** governor.ts laws 2–5: an enforced read target and a retained native recipe must
 * admit a complete progress step, without a fictitious maximum-publication workspace. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  EXECUTOR_DEMAND_ADMISSION_REACH,
  EXECUTOR_DEMAND_PUBLICATION_REACH,
  EXECUTOR_LEGACY_DEMAND_REACH,
} from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { createExecutor } from "../exec/create.js";
import type { Snapshot } from "../access/snapshot.js";
import { QueryRuntime } from "../exec/run.js";
import { oracleRows } from "./differential-adapter.js";
import { ints, texts } from "../write/test-support.js";
import type { FlatVector } from "../vector/vector.js";
import { StorageCore } from "./storage-core.js";

/** Observe charged allocations separately from unused admitted credit. */
function measure(governor: Governor) {
  const active = new Set<Grant>();
  let minimum = 0,
    peak = 0;
  const observe = () => {
    let used = 0;
    for (const grant of active) used += grant.allocated;
    peak = Math.max(peak, used);
  };
  const meter = (grant: Grant | null): Grant | null => {
    if (grant === null || active.has(grant) || grant.label === "demand competitor") return grant;
    active.add(grant);
    const allocate = grant.allocate.bind(grant),
      free = grant.free.bind(grant),
      close = grant.close.bind(grant);
    grant.allocate = (bytes) => {
      const accepted = allocate(bytes);
      observe();
      return accepted;
    };
    grant.free = (bytes) => {
      free(bytes);
      observe();
    };
    grant.close = () => {
      close();
      observe();
    };
    return grant;
  };
  const admit = governor.tryAdmit.bind(governor);
  const spy = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
    if (options.label.endsWith("/fixed-buffers")) minimum = Math.max(minimum, options.minimumBytes);
    return meter(admit(options));
  });
  const admitGroup = governor.admitGroup.bind(governor);
  const groups = vi.spyOn(governor, "admitGroup").mockImplementation(async (options) => {
    if (options.label.endsWith("/fixed-buffers"))
      minimum = Math.max(
        minimum,
        options.grants.reduce((sum, part) => sum + part.minimumBytes, 0),
      );
    const grants = await admitGroup(options);
    for (const grant of grants) meter(grant);
    return grants;
  });
  return {
    get minimum() {
      return minimum;
    },
    get peak() {
      observe();
      return peak;
    },
    close: () => {
      spy.mockRestore();
      groups.mockRestore();
    },
  };
}

it("peak meter excludes unused loan credit and counts its allocated buffers once", async () => {
  const governor = new Governor(8 << 20),
    observed = measure(governor);
  const grant = governor.tryAdmit({ label: "meter witness", minimumBytes: 0, spillable: true });
  if (!grant) throw Error("meter fixture admission");
  try {
    const loan = await grant.borrow(1 << 20);
    try {
      expect(observed.peak, "unused loan credit is not allocated buffer memory").toBe(0);
      expect(grant.allocate(4096)).toBe(true);
      expect(observed.peak).toBe(4096);
      grant.free(4096);
    } finally {
      loan.repay();
    }
  } finally {
    grant.close();
    observed.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it("peak meter observes native group grants without counting their held minimum", async () => {
  const governor = new Governor(8 << 20),
    observed = measure(governor);
  const grants = await governor.admitGroup({
    label: "async meter witness",
    includesProgress: true,
    grants: [{ label: "group meter", minimumBytes: 1 << 20, spillable: false }],
  });
  const grant = grants[0];
  if (!grant) throw Error("meter group admission");
  try {
    expect(observed.peak).toBe(0);
    expect(grant.allocate(4096)).toBe(true);
    const bytes = new Uint8Array(4096);
    expect(bytes.byteLength).toBe(4096);
    expect(observed.peak).toBe(4096);
    grant.free(4096);
  } finally {
    grant.close();
    observed.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it.each(seedsFor("next-demand-admission", [0xb9e01, 0xb9e02]))(
  "native key-order demand is sound and proportional under pinned pressure seed %i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      reach = createReach("executor demand admission", EXECUTOR_DEMAND_ADMISSION_REACH);
    const ownerGovernor = core.harness.governor;
    const evidence: Array<Record<string, unknown>> = [];
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 4 }),
          fc.constantFrom(0, 1, 17, 4096, 65535),
          fc.integer({ min: 2, max: 29 }),
          fc.constantFrom(0, 1, 2, 7, null),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (columns, width, count, limit, descending, declared, nulls) => {
            reach.beginHistory();
            const names = Array.from({ length: columns }, (_, i) => `s${String(i)}`),
              ddl = `CREATE TABLE demand_rows(id INT PRIMARY KEY,${names.map((n) => `${n} ${declared ? `VARCHAR(${String(Math.max(1, width))})` : "TEXT"}`).join(",")},amount ${declared ? "NUMERIC(30,6)" : "NUMERIC"})`;
            await core.execute("DROP TABLE IF EXISTS demand_rows");
            await pg.exec("DROP TABLE IF EXISTS demand_rows");
            await core.execute(ddl);
            await pg.exec(ddl);
            const rows: Array<{
              id: number;
              amount: string | null;
              [key: string]: number | string | null;
            }> = Array.from({ length: count }, (_, i) => {
              const id = count - i;
              return {
                id,
                ...Object.fromEntries(
                  names.map((name, c) => [
                    name,
                    nulls && (i + c) % 7 === 0 ? null : (c % 2 ? "λ" : "x").repeat(width),
                  ]),
                ),
                amount: i % 5 === 0 ? null : `${String(id)}.123400`,
              };
            });
            // Native typed input avoids the unrelated unsupported VARCHAR parameter/cast route.
            const table = core.harness.path.catalog.tableByName("demand_rows");
            if (!table) throw Error("table");
            const amountType = table.columns.at(-1)?.type;
            if (!amountType) throw Error("amount type");
            const coefficients = ints(
              rows.map((row, i) => (i % 5 === 0 ? null : row.id * 1000000 + 123400)),
            );
            const amount: FlatVector = {
              form: "flat",
              type: amountType,
              length: count,
              validity: coefficients.validity,
              data: {
                layout: "decimal",
                scale: 6,
                scales: null,
                specials: null,
                coefficients: {
                  repr: "safe",
                  values: Float64Array.from(rows, (row) => row.id * 1000000 + 123400),
                },
              },
            };
            const vectors = [
              ints(rows.map((row) => row.id)),
              ...names.map((name, i) => ({
                ...texts(rows.map((row) => row[name] as string | null)),
                type: table.columns[i + 1]?.type ?? { kind: "text" as const },
              })),
              amount,
            ];
            await core.insertVectors(table, { length: count, selection: null, columns: vectors });
            for (const row of rows)
              await pg.query(
                `INSERT INTO demand_rows VALUES(${Array.from({ length: columns + 2 }, (_, i) => `$${String(i + 1)}`).join(",")})`,
                [row.id, ...names.map((n) => row[n]), row.amount],
              );
            const sql = `SELECT * FROM demand_rows ORDER BY id ${descending ? "DESC" : "ASC"} LIMIT $1`,
              governor = new Governor(64 << 20),
              observed = measure(governor);
            const competitor: { grant: Grant | null } = { grant: null };
            // Deny speculative growth after complete native admission. A sound recipe must
            // finish using its own fixed/progress credit, or existing spill, without waiting.
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the intercepted runtime via apply below.
            const reserve = QueryRuntime.prototype.reserve;
            const mock = vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(function (
              this: QueryRuntime,
              ...args
            ) {
              const work = reserve.apply(this, args);
              const pin = () => {
                if (this.request.governor !== governor || competitor.grant) return;
                competitor.grant = governor.tryAdmit({
                  label: "demand competitor",
                  minimumBytes: 0,
                  spillable: true,
                });
                if (!competitor.grant) throw Error("competitor admission");
                expect(
                  competitor.grant.allocate(
                    governor.capBytes - governor.reserveBytes - governor.usedBytes,
                  ),
                ).toBe(true);
              };
              return work instanceof Promise ? work.then(pin) : pin();
            });
            Object.assign(core.harness, { governor });
            try {
              const actual = await core.execute(sql, [limit]);
              if (!("batches" in actual)) throw Error("expected rows");
              expect(oracleRows(actual)).toEqual((await pg.query(sql, [limit])).rows);
              if (limit !== 0) {
                expect(observed.minimum).toBeGreaterThan(0);
                expect(observed.peak).toBeGreaterThan(0);
                expect(observed.minimum).toBeLessThanOrEqual(4 * observed.peak + (1 << 20));
              }
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              evidence.push({
                columns,
                width,
                count,
                limit,
                descending,
                declared,
                nulls,
                minimumBytes: observed.minimum,
                chargedPeakBytes: observed.peak,
                capBytes: governor.capBytes,
              });
              reach.reach("pinned competitor denies optional growth");
              reach.reach("PGlite exact ordered rows");
              reach.reach(declared ? "declared widths" : "unknown widths");
              reach.reach(
                descending ? "descending physical reorder" : "ascending physical reorder",
              );
              reach.reach(
                limit === 0 ? "LIMIT zero" : limit === null ? "unbounded LIMIT" : "bound LIMIT",
              );
              if (width >= 65535) reach.reach("oversized row");
              if (nulls) reach.reach("nullable input");
              reach.reach("minimum versus charged peak");
            } finally {
              mock.mockRestore();
              competitor.grant?.close();
              observed.close();
              Object.assign(core.harness, { governor: ownerGovernor });
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero final query credit");
          },
        ),
        { seed, numRuns: 24 },
      );
      reach.assertReached();
      if (process.env.MINNOW_B9_DEMAND_EVIDENCE)
        process.stdout.write(
          `B9_DEMAND_EVIDENCE ${JSON.stringify({ seed, histories: evidence })}\n`,
        );
    } finally {
      Object.assign(core.harness, { governor: ownerGovernor });
      await core.close();
      await pg.close();
    }
  },
  120000,
);

it("two-row INT TEXT NUMERIC ordered LIMIT fits an8MiB query governor", async () => {
  const core = await new StorageCore().open(),
    ownerGovernor = core.harness.governor,
    governor = new Governor(8 << 20),
    observed = measure(governor);
  try {
    await core.execute(
      "CREATE TABLE person_record(id INT PRIMARY KEY,first_name TEXT,amount NUMERIC)",
    );
    await core.execute("INSERT INTO person_record VALUES(1,'before',12.3400),(2,'later',22.0000)");
    Object.assign(core.harness, { governor });
    const result = await core.execute("SELECT * FROM person_record ORDER BY id LIMIT $1", [1]);
    if (!("batches" in result)) throw Error("expected rows");
    expect(oracleRows(result)).toEqual([{ id: 1, first_name: "before", amount: "12.3400" }]);
    expect(observed.minimum).toBeLessThan(8 << 20);
    expect(observed.minimum).toBeLessThanOrEqual(4 * observed.peak + (1 << 20));
    expect(governor.usedBytes).toBe(0);
  } finally {
    observed.close();
    Object.assign(core.harness, { governor: ownerGovernor });
    await core.close();
  }
});

it.each(seedsFor("next-demand-publication", [0xb9e31, 0xb9e32]))(
  "retains the sized resident source through publication after admission seed %i",
  async (seed) => {
    const core = await new StorageCore().open(),
      ownerGovernor = core.harness.governor;
    const reach = createReach("admitted source publication", EXECUTOR_DEMAND_PUBLICATION_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 29 }),
          fc.integer({ min: 0, max: 8192 }),
          fc.boolean(),
          async (count, width, descending) => {
            reach.beginHistory();
            await core.execute("DROP TABLE IF EXISTS demand_publish");
            await core.execute("CREATE TABLE demand_publish(id INT PRIMARY KEY,v TEXT,n NUMERIC)");
            const value = "λ".repeat(width);
            await core.writeBatch(
              "insertBatch",
              "demand_publish",
              Array.from({ length: count }, (_, i) => ({
                id: i + 1,
                v: value,
                n: `${String(i + 1)}.1200`,
              })),
            );
            const tableId = core.harness.path.catalog.tableByName("demand_publish")?.id;
            if (tableId === undefined) throw Error("publication table");
            const governor = new Governor(8 << 20);
            let published = false;
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the intercepted runtime via apply below.
            const reserve = QueryRuntime.prototype.reserve;
            const spy = vi
              .spyOn(QueryRuntime.prototype, "reserve")
              .mockImplementation(async function (this: QueryRuntime, ...args) {
                await reserve.apply(this, args);
                if (this.request.governor === governor && !published) {
                  expect(this.residentSourceRows(tableId)).toBe(count);
                  reach.reach("resident source retained before publication");
                  published = true;
                  await core.harness.path.recycleLog();
                  await core.harness.path.quiesce();
                  expect(
                    core.harness.path.state().tables.get(tableId)?.groups.count,
                  ).toBeGreaterThan(0);
                  reach.reach("publication after complete admission");
                }
              });
            Object.assign(core.harness, { governor });
            try {
              const result = await core.execute(
                `SELECT * FROM demand_publish ORDER BY id ${descending ? "DESC" : "ASC"} LIMIT $1`,
                [1],
              );
              if (!("batches" in result)) throw Error("expected rows");
              const id = descending ? count : 1;
              expect(oracleRows(result)).toEqual([{ id, v: value, n: `${String(id)}.1200` }]);
              expect(published).toBe(true);
              reach.reach("old snapshot exact values after publication");
              expect(governor.usedBytes).toBe(0);
              reach.reach("8MiB query zero final credit");
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            } finally {
              spy.mockRestore();
              Object.assign(core.harness, { governor: ownerGovernor });
            }
          },
        ),
        { seed, numRuns: 12 },
      );
      reach.assertReached();
    } finally {
      Object.assign(core.harness, { governor: ownerGovernor });
      await core.close();
    }
  },
  120000,
);

it.each(seedsFor("next-demand-legacy-cursor", [0xb9e51, 0xb9e52]))(
  "diagnostic cursor requests fit their complete reserved capacity seed %i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      owner = core.harness.governor,
      reach = createReach("legacy cursor demand", EXECUTOR_LEGACY_DEMAND_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 29 }),
          fc.boolean(),
          async (count, descending) => {
            reach.beginHistory();
            await core.execute("DROP TABLE IF EXISTS legacy_demand");
            await pg.exec("DROP TABLE IF EXISTS legacy_demand");
            const ddl = "CREATE TABLE legacy_demand(id INT PRIMARY KEY,s TEXT,amount NUMERIC)";
            const insert = `INSERT INTO legacy_demand VALUES ${Array.from({ length: count }, (_, i) => `(${String(count - i)},'x${String(i)}',1.2500)`).join(",")}`;
            for (const sql of [ddl, insert]) {
              await core.execute(sql);
              await pg.exec(sql);
            }
            const governor = new Governor(8 << 20),
              legacy = createExecutor({ keyOrder: true }),
              prepare = vi
                .spyOn(core.executor, "prepare")
                .mockImplementation((plan) => legacy.prepare(plan)),
              originalPin = core.harness.path.pin.bind(core.harness.path);
            let unadmitted = 0;
            const pin = vi.spyOn(core.harness.path, "pin").mockImplementation(
              (...args) =>
                new Proxy(originalPin(...args), {
                  get(target, name) {
                    if (name === "index")
                      return (...indexArgs: Parameters<Snapshot["index"]>) => {
                        const reader = target.index(...indexArgs);
                        // A legal access provider need not offer either optional capability.
                        return {
                          def: reader.def,
                          probe: reader.probe.bind(reader),
                          range(...range: Parameters<typeof reader.range>) {
                            unadmitted++;
                            return reader.range(...range);
                          },
                        };
                      };
                    const value: unknown = Reflect.get(target, name, target);
                    return typeof value === "function" ? (value.bind(target) as unknown) : value;
                  },
                }),
            );
            const reserved = new Map<number, number>();
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to each intercepted runtime via call below.
            const reserve = QueryRuntime.prototype.reserve;
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to each intercepted runtime via apply below.
            const admit = QueryRuntime.prototype.admit;
            const reservation = vi
              .spyOn(QueryRuntime.prototype, "reserve")
              .mockImplementation(function (this: QueryRuntime, entries) {
                if (this.request.governor === governor)
                  for (const entry of entries) reserved.set(entry.rel.id, entry.minimum);
                return reserve.call(this, entries);
              });
            const request = vi.spyOn(QueryRuntime.prototype, "admit").mockImplementation(function (
              this: QueryRuntime,
              ...args
            ) {
              const [, minimum, , rel] = args;
              const bound = rel ? reserved.get(rel.id) : undefined;
              if (this.request.governor === governor && bound !== undefined) {
                expect(
                  minimum,
                  "operator request must fit its completely reserved capacity",
                ).toBeLessThanOrEqual(bound);
                reach.reach("operator requests within reserved capacity");
              }
              return admit.apply(this, args);
            });
            Object.assign(core.harness, { governor });
            try {
              const sql = `SELECT * FROM legacy_demand ORDER BY id ${descending ? "DESC" : "ASC"} LIMIT $1`,
                actual = await core.execute(sql, [1]);
              if (!("batches" in actual)) throw Error("rows");
              expect(oracleRows(actual)).toEqual((await pg.query(sql, [1])).rows);
              expect(unadmitted).toBe(1);
              expect(governor.usedBytes).toBe(0);
              reach.reach("optional range capability absent");
              reach.reach("complete query fits8MiB");
              reach.reach("PGlite exact direct projection");
              reach.reach(descending ? "descending" : "ascending");
              reach.reach("legacy diagnostic cursor exercised");
              reach.reach("zero final credits");
            } finally {
              request.mockRestore();
              reservation.mockRestore();
              pin.mockRestore();
              prepare.mockRestore();
              Object.assign(core.harness, { governor: owner });
            }
          },
        ),
        { seed, numRuns: 12 },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  },
  120000,
);
