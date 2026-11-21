/** Governor complete-minimum law: a one-locator fetch still visits physical slots.
 * PostgreSQL owns SQL rows; independent constructor accounting owns backing bytes. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXECUTOR_KEY_ORDER_DEMAND_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { QueryRuntime } from "../exec/run.js";
import { ViewIndex } from "../write/view.js";
import { allocationAudit } from "./allocation-audit.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

it.each(seedsFor("next-executor-key-order-demand", [0xb9f01, 0xb9f02]))(
  "funds fixed physical-slot buffers beside one-row demand under pressure (seed %i)",
  async (seed) => {
    const pg = new PGlite();
    const reach = createReach("key-order physical-slot demand", EXECUTOR_KEY_ORDER_DEMAND_REACH);
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(2, 257, 2049),
          fc.constantFrom("int", "text", "numeric"),
          fc.boolean(),
          fc.boolean(),
          fc.constantFrom(0, 1),
          async (count, kind, descending, stored, offset) => {
            reach.beginHistory();
            const core = await new StorageCore(8192).open();
            // Keep the existing 8 MiB small-table contract. Multi-chunk native
            // sources retain the established 64 MiB boundary-suite cap: their
            // publication recipe is independent of this one-locator repair.
            const governor = new Governor((count > 2048 ? 64 : 8) << 20);
            const active = new Set<Grant>();
            const competitor: { grant: Grant | null } = { grant: null };
            const nativePrepare = core.executor.prepare.bind(core.executor);
            const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
              const prepared = nativePrepare(plan);
              return {
                ...prepared,
                execute: (request) => prepared.execute({ ...request, governor }),
              };
            });
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound below.
            const reserve = QueryRuntime.prototype.reserve;
            const pin = vi
              .spyOn(QueryRuntime.prototype, "reserve")
              .mockImplementation(async function (this: QueryRuntime, ...args) {
                await reserve.apply(this, args);
                if (this.request.governor !== governor || competitor.grant) return;
                competitor.grant = governor.tryAdmit({
                  label: "demand competitor",
                  minimumBytes: 0,
                  spillable: true,
                });
                if (!competitor.grant) throw Error("pressure pin");
                expect(
                  competitor.grant.allocate(
                    governor.capBytes - governor.reserveBytes - governor.usedBytes,
                  ),
                ).toBe(true);
              });
            const admit = governor.tryAdmit.bind(governor);
            const direct = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
              const grant = admit(options);
              if (grant && options.label !== "demand competitor") active.add(grant);
              return grant;
            });
            const grouped = governor.admitGroup.bind(governor);
            const groups = vi.spyOn(governor, "admitGroup").mockImplementation(async (options) => {
              const grants = await grouped(options);
              for (const grant of grants) active.add(grant);
              return grants;
            });
            const ordinary = vi.spyOn(ViewIndex.prototype, "planRange");
            const resident = vi.spyOn(ViewIndex.prototype, "prepareResidentRange");
            let audit: ReturnType<typeof allocationAudit> | undefined;
            try {
              const type = kind === "int" ? "INTEGER" : kind === "text" ? "TEXT" : "NUMERIC(38,6)";
              const ddl = `CREATE TABLE physical_demand(id INTEGER PRIMARY KEY,p ${type})`;
              await core.execute(ddl);
              await pg.exec(`DROP TABLE IF EXISTS physical_demand; ${ddl}`);
              const rows = Array.from({ length: count }, (_, i) => ({
                id: count - i,
                p:
                  kind === "int"
                    ? i
                    : kind === "text"
                      ? `value-${String(i)}`
                      : `${String(i)}.123456`,
              }));
              await core.writeBatch("insertBatch", "physical_demand", rows);
              await pg.query(
                "INSERT INTO physical_demand SELECT i, " +
                  (kind === "int"
                    ? "$1::int-i"
                    : kind === "text"
                      ? "'value-'||($1::int-i)::text"
                      : "($1::int-i)::numeric+0.123456") +
                  " FROM generate_series(1,$1::int) i",
                [count],
              );
              if (stored) {
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
              }
              ordinary.mockClear();
              resident.mockClear();
              audit = allocationAudit(() => [...active].reduce((n, grant) => n + grant.held, 0));
              const sql = `SELECT p FROM physical_demand ORDER BY id ${descending ? "DESC" : "ASC"} LIMIT $1 OFFSET $2`;
              const result = await core.execute(sql, [1, offset]);
              if (!("batches" in result)) throw Error("query rows");
              expect(oracleRows(result)).toEqual((await pg.query(sql, [1, offset])).rows);
              // Lead design decision under the governor law (size before admission, release,
              // then admit): LIMIT 1 over a resident one-chunk table keeps its single-use
              // resident range, unsized; otherwise its own planned cursor sizes the window first.
              expect([ordinary.mock.calls.length, resident.mock.calls.length]).toEqual(
                !stored && count <= 2048 ? [0, 1] : [2, 0],
              );
              expect(audit.observedBytes).toBeGreaterThan(0);
              expect(audit.completed.every((step) => step.actual <= step.credit)).toBe(true);
              expect(competitor.grant).not.toBeNull();
              reach.reach(kind);
              reach.reach(count === 2 ? "2" : count === 257 ? "257" : "2049");
              reach.reach(stored ? "stored" : "resident");
              reach.reach(descending ? "DESC" : "ASC");
              reach.reach(offset ? "OFFSET" : "no OFFSET");
              reach.reach("PG exact");
              reach.reach("independent backing");
            } finally {
              audit?.close();
              competitor.grant?.close();
              pin.mockRestore();
              prepare.mockRestore();
              direct.mockRestore();
              groups.mockRestore();
              ordinary.mockRestore();
              resident.mockRestore();
              await core.close();
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("released");
          },
        ),
        {
          seed,
          numRuns: 18,
          examples: [
            [2, "int", false, false, 0],
            [257, "text", true, false, 1],
            [2049, "numeric", false, true, 0],
            [2, "int", true, true, 1],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  240000,
);
