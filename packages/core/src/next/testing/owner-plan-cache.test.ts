/** The public Lowerer cache contract: normalized keys hold templates/markers, never raw constants. */
import fc from "fast-check";
import { describe, expect, it, vi } from "vitest";
import { OWNER_PLAN_CACHE_STATES, createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { createLowerer } from "../lower/create.js";
import { cleanups, harness } from "./owner-harness.js";

function subject(planCacheSize = 8) {
  const planner = createLowerer();
  return { ...harness({ planner, planCacheSize }), planner };
}
const quote = (value: string) => `'${value.replaceAll("'", "''")}'`;
async function seed(h: ReturnType<typeof subject>, ids: readonly number[], text = false) {
  const client = await h.connect();
  await client.query(`CREATE TABLE t (id ${text ? "TEXT" : "INTEGER"} PRIMARY KEY, v TEXT)`);
  await client.query(
    `INSERT INTO t VALUES ${ids.map((id) => `(${text ? quote(`k${String(id)}`) : String(id)}, ${quote(`v${String(id)}`)})`).join(", ")}`,
  );
  return client;
}
async function finish(h: ReturnType<typeof subject>) {
  h.server.stepDown("closing");
  await h.engine.close();
  for (const close of cleanups.splice(0).reverse()) close();
}

describe("owner auto-parameterized plan cache", () => {
  it("shares a template and prepared plan across more literal texts than cache entries", async () => {
    const h = subject(2);
    const ids = Array.from({ length: 24 }, (_, i) => i);
    const c = await seed(h, ids);
    const lower = vi.spyOn(h.planner, "lower");
    const templates = vi.spyOn(h.planner, "lowerParameterized");
    const prepared = h.executor.prepared;
    try {
      for (let pass = 0; pass < 2; pass++)
        for (const id of ids)
          expect((await c.query(`SELECT id, v FROM t WHERE id = ${String(id)}`)).rows()).toEqual([
            [id, `v${String(id)}`],
          ]);
      expect(lower).not.toHaveBeenCalled();
      expect(templates).toHaveBeenCalledTimes(1);
      expect(h.executor.prepared - prepared).toBe(1);
    } finally {
      await finish(h);
    }
  });

  it("caches the refusal marker once while keeping each fallback's constants under its own text", async () => {
    const h = subject(8);
    const c = await seed(h, [1, 2, 3]);
    const lower = vi.spyOn(h.planner, "lower");
    const templates = vi.spyOn(h.planner, "lowerParameterized");
    try {
      for (let pass = 0; pass < 2; pass++)
        for (const id of [1, 2, 3])
          expect((await c.query(`SELECT id, v FROM t WHERE id = '${String(id)}'`)).rows()).toEqual([
            [id, `v${String(id)}`],
          ]);
      expect(templates).toHaveBeenCalledTimes(1);
      expect(templates.mock.results[0]).toMatchObject({
        type: "return",
        value: { kind: "notParameterizable" },
      });
      expect(lower).toHaveBeenCalledTimes(3);
    } finally {
      await finish(h);
    }
  });

  it("keeps prepared literals separate and appends lifted values after client parameters", async () => {
    const h = subject();
    const c = await seed(h, [1, 2]);
    const templates = vi.spyOn(h.planner, "lowerParameterized");
    try {
      const a = await c.prepare("SELECT id, v FROM t WHERE id = 1 AND v = $1");
      const b = await c.prepare("SELECT id, v FROM t WHERE id = 2 AND v = $1");
      for (let i = 0; i < 3; i++) {
        expect((await c.execute(a.id, ["v1"])).rows()).toEqual([[1, "v1"]]);
        expect((await c.execute(b.id, ["v2"])).rows()).toEqual([[2, "v2"]]);
      }
      expect(templates).toHaveBeenCalledTimes(1);
      await expect(c.execute(a.id, [])).rejects.toBeInstanceOf(Error);
    } finally {
      await finish(h);
    }
  });

  it("replans a prepared literal after its table is replaced at a new catalog version", async () => {
    const h = subject(16);
    const c = await seed(h, [1]);
    try {
      const p = await c.prepare("SELECT id, v FROM t WHERE id = 1");
      expect((await c.execute(p.id)).rows()).toEqual([[1, "v1"]]);
      await c.query("DROP TABLE t");
      await c.query("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
      await c.query("INSERT INTO t VALUES (1, 11)");
      expect((await c.execute(p.id)).rows()).toEqual([[1, 11]]);
    } finally {
      await finish(h);
    }
  });
});

const SEEDS = seedsFor("next-owner-plan-cache", [0xca23e]);
it.each(SEEDS)(
  "generated template, marker, binding and eviction histories (seed %i)",
  async (seedValue) => {
    const reach = createReach("next-owner-plan-cache", OWNER_PLAN_CACHE_STATES, { minimum: 2 });
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          ids: fc.uniqueArray(fc.integer({ min: -5, max: 20 }), { minLength: 3, maxLength: 7 }),
          family: fc.constantFrom("integer", "text", "coerced"),
          clientParams: fc.boolean(),
          evict: fc.boolean(),
          transaction: fc.boolean(),
          write: fc.boolean(),
        }),
        async ({ ids, family, clientParams, evict, transaction, write }) => {
          const h = subject(evict ? 2 : ids.length + 3);
          const c = await seed(h, ids, family === "text");
          const lower = vi.spyOn(h.planner, "lower");
          const templates = vi.spyOn(h.planner, "lowerParameterized");
          const entered = new Set<string>();
          const literal = (id: number) =>
            family === "integer"
              ? String(id)
              : quote(family === "text" ? `k${String(id)}` : String(id));
          const sql = (id: number, columns = "id, v") =>
            `SELECT ${columns} FROM t WHERE id = ${literal(id)}${clientParams ? " AND v = $1" : ""}`;
          const args = (id: number) => (clientParams ? [`v${String(id)}`] : []);
          const row = (id: number) => [family === "text" ? `k${String(id)}` : id, `v${String(id)}`];
          try {
            if (transaction) await c.query("BEGIN");
            lower.mockClear();
            templates.mockClear();
            for (let pass = 0; pass < 2; pass++)
              for (const id of ids)
                expect((await c.query(sql(id), args(id))).rows()).toEqual([row(id)]);
            if (transaction) entered.add("transaction query");
            if (family === "coerced") {
              expect(
                templates.mock.results.some(
                  (r) => r.type === "return" && r.value.kind === "notParameterizable",
                ),
              ).toBe(true);
              if (!evict) {
                expect(templates).toHaveBeenCalledTimes(1);
                expect(lower).toHaveBeenCalledTimes(ids.length);
              }
              entered.add("marker fallback");
            } else {
              expect(templates).toHaveBeenCalledTimes(1);
              expect(lower).not.toHaveBeenCalled();
              entered.add(family === "integer" ? "integer template" : "text template");
              if (clientParams) entered.add("client parameters");
            }
            if (evict) {
              const first = ids[0];
              if (first === undefined) throw new Error("history needs a key");
              await c.query(sql(first, "v, id"), args(first));
              await c.query(sql(first, "id"), args(first));
              const calls = lower.mock.calls.length + templates.mock.calls.length;
              expect((await c.query(sql(first), args(first))).rows()).toEqual([row(first)]);
              expect(lower.mock.calls.length + templates.mock.calls.length).toBeGreaterThan(calls);
              entered.add("cache eviction");
            }
            if (write) {
              for (const id of ids) {
                const updated = `updated${String(id)}`;
                expect(
                  (
                    await c.query(`UPDATE t SET v = $1 WHERE id = ${literal(id)} RETURNING id, v`, [
                      updated,
                    ])
                  ).rows(),
                ).toEqual([[row(id)[0], updated]]);
              }
              entered.add("bound write");
            }
            reach.recordHistory(entered);
          } finally {
            await finish(h);
          }
        },
      ),
      { seed: seedValue, numRuns: 100 },
    );
    reach.assertReached();
  },
  120_000,
);
