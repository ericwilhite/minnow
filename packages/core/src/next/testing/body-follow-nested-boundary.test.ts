/** Follow deferral retains signed budgets and exact original membership, under pressure. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  FOLLOW_NESTED_PRESSURE_REACH,
  FOLLOW_NESTED_BOUNDARY_REACH,
} from "../../testing/reach.js";
import { NativePrimaryAdapter } from "../write/primary-command.js";
import { commandRootFixture } from "./command-root.test-support.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-follow-nested-pressure", [0xb705, 0xb706]))
  it(`spills nested AFTER without retaining one frame/file per child seed ${String(seed)}`, async () => {
    const reach = createReach("nested follow pressure", FOLLOW_NESTED_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 32, max: 96 }), fc.boolean(), async (count, published) => {
        reach.beginHistory();
        const payload = "x".repeat(8192),
          f = await commandRootFixture(
            [
              "CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE c(id INT PRIMARY KEY,pid INT UNIQUE REFERENCES p(id),payload TEXT)",
              "CREATE TABLE audit(id INT PRIMARY KEY,v INT)",
              "CREATE TABLE trace(id INT PRIMARY KEY,v TEXT)",
              "INSERT INTO audit VALUES(1,0)",
              "INSERT INTO trace VALUES(1,'')",
              `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`).join(",")}`,
              `INSERT INTO c VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)},'${payload}')`).join(",")}`,
              "CREATE TRIGGER child_before BEFORE UPDATE ON c BEGIN UPDATE audit SET v=v+1 WHERE id=1; END",
              "CREATE TRIGGER audit_after AFTER UPDATE ON audit BEGIN UPDATE trace SET v=v||CAST(NEW.v AS TEXT)||';' WHERE id=1; END",
            ],
            `UPDATE p SET id=(id%${String(count)})+1`,
            published,
            64 << 20,
          ),
          gov = f.core.harness.governor,
          competitor = await gov.admit({
            label: "nested follow competitor",
            minimumBytes: Math.max(0, gov.capBytes - gov.reserveBytes - gov.usedBytes - (1 << 20)),
            spillable: false,
          });
        expect(competitor.allocate(competitor.held)).toBe(true);
        let active = 0,
          peak = 0,
          total = 0;
        const create = f.spill.create.bind(f.spill),
          files = vi.spyOn(f.spill, "create").mockImplementation(async (id) => {
            const file = await create(id);
            active++;
            total++;
            peak = Math.max(peak, active);
            let closed = false;
            const close = file.close.bind(file);
            file.close = async () => {
              await close();
              if (!closed) {
                closed = true;
                active--;
              }
            };
            return file;
          });
        try {
          expect(await f.root.run({ slots: [] })).toBe(count);
          await f.commit();
          competitor.close();
          await f.retire();
          expect(peak).toBeLessThan(12);
          expect(active).toBe(0);
          expect(total).toBeGreaterThan(0);
          expect(gov.peakBytes).toBeLessThanOrEqual(gov.capBytes);
          const trace = await f.core.execute("SELECT v FROM trace");
          if (!("batches" in trace)) throw Error("trace");
          expect(oracleRows(trace)).toEqual([
            { v: Array.from({ length: count }, (_, i) => `${String(i + 1)};`).join("") },
          ]);
          const children = await f.core.execute("SELECT id,pid,payload FROM c ORDER BY id");
          if (!("batches" in children)) throw Error("children");
          expect(oracleRows(children)).toEqual(
            Array.from({ length: count }, (_, i) => ({
              id: i + 1,
              pid: ((i + 1) % count) + 1,
              payload,
            })),
          );
          reach.reach(published ? "published" : "resident");
          reach.reach("64MiB competing credit");
          reach.reach("wide unchanged payload");
          reach.reach("bounded active files");
          reach.reach("deferred FIFO exact");
        } finally {
          files.mockRestore();
          competitor.close();
          await f.close();
        }
        reach.reach("zero native charge");
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [32, false],
          [96, true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-follow-nested-boundary", [0xb707, 0xb708]))
  it(`retains spent budget and immediate nonmember checks seed ${String(seed)}`, async () => {
    const reach = createReach("follow nested boundaries", FOLLOW_NESTED_BOUNDARY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 7 }),
        fc.boolean(),
        fc.boolean(),
        async (count, outsider, published) => {
          reach.beginHistory();
          const setup = [
              "CREATE TABLE p(id INT PRIMARY KEY)",
              "CREATE TABLE audit(id INT PRIMARY KEY,v INT)",
              "INSERT INTO audit VALUES(1,0)",
              "CREATE TABLE trace(id INT PRIMARY KEY,n INT)",
              "INSERT INTO trace VALUES(1,0)",
              ...(outsider
                ? [
                    "CREATE TABLE s(id INT PRIMARY KEY)",
                    `INSERT INTO s VALUES${Array.from(
                      { length: count },
                      (_, i) => `(${String(i + 1)})`,
                    )
                      .concat(["(77)", "(99)"])
                      .join(",")}`,
                  ]
                : ["CREATE TABLE sink(n INT)"]),
              `CREATE TABLE c(id INT PRIMARY KEY,pid INT UNIQUE REFERENCES p(id)${outsider ? ",k INT DEFAULT 99 REFERENCES s(id) ON DELETE SET DEFAULT,CONSTRAINT outsider_unique UNIQUE(k)" : ""})`,
              `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`)
                .concat(outsider ? ["(99)", "(100)"] : [])
                .join(",")}`,
              `INSERT INTO c VALUES${Array.from(
                { length: count },
                (_, i) =>
                  `(${String(i + 1)},${String(i + 1)}${outsider ? `,${String(i + 1)}` : ""})`,
              )
                .concat(outsider ? ["(101,99,77)", "(102,100,99)"] : [])
                .join(",")}`,
              `CREATE TRIGGER child_before BEFORE UPDATE ON c BEGIN ${outsider ? "DELETE FROM s WHERE id=77" : "UPDATE audit SET v=v+1 WHERE id=1"} AND OLD.id=${String(count)}; END`,
              ...(!outsider
                ? [
                    "CREATE TRIGGER audit_after AFTER UPDATE ON audit BEGIN UPDATE trace SET n=n+1 WHERE id=1; END",
                    "CREATE TRIGGER trace_after AFTER UPDATE ON trace BEGIN INSERT INTO sink VALUES(NEW.n); END",
                  ]
                : []),
            ],
            f = await commandRootFixture(
              setup,
              `UPDATE p SET id=(id%${String(count)})+1 WHERE id<=${String(count)}`,
              published,
            ),
            child = f.statement.catalog.tableByName("c");
          if (!child) throw Error("child");
          let root = 0,
            accepted = 0;
          const apply = Reflect.get(NativePrimaryAdapter.prototype, "apply"),
            writes = vi
              .spyOn(NativePrimaryAdapter.prototype, "apply")
              .mockImplementation(async function (
                this: NativePrimaryAdapter,
                ...args: Parameters<NativePrimaryAdapter["apply"]>
              ) {
                const out = await apply.apply(this, args),
                  t = args[0];
                if (t.tableId === f.statement.catalog.tableByName("p")?.id) root = t.command.id;
                if (t.tableId === child.id && t.command.id === root) accepted++;
                return out;
              });
          try {
            const error = await (async () => {
              try {
                await f.root.run({ slots: [] });
                await f.root.end();
                return null;
              } catch (error) {
                return error;
              }
            })();
            expect(
              error && typeof error === "object" && "sqlState" in error ? error.sqlState : null,
            ).toBe(outsider ? "23505" : "54001");
            if (outsider) {
              expect(error).toMatchObject({
                sqlState: "23505",
                tableId: child.id,
                indexId: child.indexes.find((i) => i.name === "outsider_unique")?.id,
              });
              expect(accepted).toBe(count - 1);
              reach.reach("nonmember immediate uniqueness");
            } else {
              expect(error).toMatchObject({ sqlState: "54001", code: "MINNOW_TRIGGER_DEPTH" });
              expect(accepted).toBe(count);
              reach.reach("spent budget preserved");
              reach.reach("depth after completed group");
            }
            await f.retire();
            const rows = await f.core.execute("SELECT id,pid FROM c ORDER BY id");
            if (!("batches" in rows)) throw Error("rows");
            expect(oracleRows(rows)).toEqual(
              Array.from({ length: count }, (_, i) => ({ id: i + 1, pid: i + 1 })).concat(
                outsider
                  ? [
                      { id: 101, pid: 99 },
                      { id: 102, pid: 100 },
                    ]
                  : [],
              ),
            );
            reach.reach(published ? "published" : "resident");
            reach.reach("atomic rollback");
          } finally {
            writes.mockRestore();
            await f.close();
          }
          reach.reach("zero native charge");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [3, false, false],
          [3, true, true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
