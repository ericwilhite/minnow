/** Governor rule 6 and the owner cache's LRU/byte-bound contract; SQL semantics use real lowering. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  OWNER_PLAN_CACHE_POLICY_STATES,
  OWNER_PRIVATE_PLAN_CACHE_STATES,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { stringBytes } from "../memory/governor.js";
import { harness } from "./owner-harness.js";

function subject(entries = 64, bytes = 256 * 1024) {
  const h = harness({ planCacheSize: entries, planCacheBytes: bytes });
  return {
    ...h,
    lower: vi.spyOn(h.planner, "lower"),
    templates: vi.spyOn(h.planner, "lowerParameterized"),
  };
}
async function close(h: ReturnType<typeof subject>) {
  h.server.stepDown("closing");
  await h.engine.close();
}
const sqlFor = (kind: "raw" | "template" | "marker", alias: string, payload: number) =>
  `${kind === "raw" ? "SELECT 'payload'" : kind === "template" ? "SELECT 1 = 1" : "SELECT 1 = '1'"} AS ${alias} /*${"x".repeat(payload)}*/`;
const calls = (h: ReturnType<typeof subject>) =>
  h.lower.mock.calls.length + h.templates.mock.calls.length;

it("review P5: A,B,A,C,A compiles three templates, preserving the hot family", async () => {
  const h = subject(2);
  try {
    for (const [alias, value] of [
      ["a", 1],
      ["b", 2],
      ["a", 3],
      ["c", 4],
      ["a", 5],
    ] as const)
      h.engine.plan(`SELECT 1 = ${String(value)} AS ${alias}`);
    expect(h.templates).toHaveBeenCalledTimes(3);
    expect(h.lower).not.toHaveBeenCalled();
  } finally {
    await close(h);
  }
});

it("review P4: retained plans are charged, then released even if write-path close fails", async () => {
  const h = subject();
  const registrations = vi.spyOn(h.governor, "registerCache");
  h.engine.plan("SELECT 'retained sql and value' AS value");
  expect(h.governor.usedBytes).toBeGreaterThan(0);
  expect(registrations).toHaveBeenCalledTimes(1);
  const registered = registrations.mock.results[0];
  if (registered?.type !== "return") throw new Error("The plan cache did not register");
  const unregister = vi.spyOn(registered.value, "unregister");
  h.writePath.close = () => Promise.reject(new Error("close failed"));
  h.server.stepDown("closing");
  await expect(h.engine.close()).rejects.toThrow("close failed");
  expect(h.governor.usedBytes).toBe(0);
  expect(unregister).toHaveBeenCalledTimes(1);
});

it.each(seedsFor("next-owner-plan-cache-policy", [0xcac4e]))(
  "generated size, reclaim and mixed recency histories (seed %i)",
  async (seed) => {
    const reach = createReach("next-owner-plan-cache-policy", OWNER_PLAN_CACHE_POLICY_STATES, {
      minimum: 2,
    });
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          kind: fc.constantFrom("raw" as const, "template" as const, "marker" as const),
          payload: fc.integer({ min: 64, max: 4096 }),
          policy: fc.constantFrom("size", "reclaim", "lru"),
        }),
        async ({ kind, payload, policy }) => {
          const entered = new Set<string>();
          const sql = sqlFor(kind, "a", payload);
          const h = subject();
          let measured: number;
          try {
            h.engine.plan(sql);
            const normalized = h.planner.parameterize(sql);
            const minimum =
              kind === "raw"
                ? 2 * stringBytes(sql)
                : kind === "template"
                  ? 2 * stringBytes(normalized?.key ?? "")
                  : 2 * stringBytes(normalized?.key ?? "") + 2 * stringBytes(sql);
            measured = h.governor.usedBytes;
            expect(measured).toBeGreaterThanOrEqual(minimum);
            entered.add(`${kind} charged`);
            if (policy === "reclaim") {
              const normal = h.governor.capBytes - h.governor.reserveBytes;
              const grant = h.governor.tryAdmit({
                label: "plan-cache-reclaim",
                minimumBytes: normal,
                spillable: true,
              });
              expect(grant).not.toBeNull();
              try {
                expect(h.governor.usedBytes).toBe(normal);
              } finally {
                grant?.close();
              }
              expect(h.governor.usedBytes).toBe(0);
              const before = calls(h);
              h.engine.plan(sql);
              expect(calls(h)).toBeGreaterThan(before);
              entered.add("governor reclaim");
            }
          } finally {
            await close(h);
          }
          expect(h.governor.usedBytes).toBe(0);
          entered.add("close releases");
          if (policy === "size") {
            const small = subject(64, 128);
            try {
              small.engine.plan(sql);
              small.engine.plan(sql);
              expect(small.governor.usedBytes).toBe(0);
              expect(
                kind === "raw" ? small.lower.mock.calls.length : small.templates.mock.calls.length,
              ).toBe(2);
              entered.add("oversized refused");
            } finally {
              await close(small);
            }
            const bounded = subject(64, measured);
            try {
              bounded.engine.plan(sql);
              bounded.engine.plan(sqlFor(kind, "b", payload));
              expect(bounded.governor.usedBytes).toBeLessThanOrEqual(measured);
              const before = calls(bounded);
              bounded.engine.plan(sql);
              expect(calls(bounded)).toBeGreaterThan(before);
              entered.add("byte eviction");
            } finally {
              await close(bounded);
            }
          }
          if (policy === "lru") {
            const mixed = kind === "marker";
            const recent = subject(mixed ? 3 : 2);
            try {
              const a = sqlFor(kind === "raw" ? "raw" : "template", "a", 0);
              const b = sqlFor(kind === "raw" ? "raw" : "template", "b", 0);
              const c = sqlFor(mixed ? "marker" : kind, "c", 0);
              for (const text of [a, b, a, c, a]) recent.engine.plan(text);
              if (mixed) {
                expect(recent.templates).toHaveBeenCalledTimes(3);
                expect(recent.lower).toHaveBeenCalledTimes(1);
                entered.add("mixed LRU");
              } else if (kind === "raw") {
                expect(recent.lower).toHaveBeenCalledTimes(3);
                entered.add("raw LRU");
              } else {
                expect(recent.templates).toHaveBeenCalledTimes(3);
                entered.add("template LRU");
              }
            } finally {
              await close(recent);
            }
          }
          reach.recordHistory(entered);
        },
      ),
      { seed, numRuns: 100 },
    );
    reach.assertReached();
  },
  120_000,
);

it.each(seedsFor("next-owner-private-plan-cache", [0xca9e]))(
  "generated private-plan lifetime accounting (seed %i)",
  async (seed) => {
    const reach = createReach("next-owner-private-plan-cache", OWNER_PRIVATE_PLAN_CACHE_STATES, {
      minimum: 2,
    });
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          end: fc.constantFrom("COMMIT", "ROLLBACK", "close"),
          payload: fc.integer({ min: 64, max: 2048 }),
        }),
        async ({ end, payload }) => {
          const h = subject();
          const c = await h.connect();
          const entered = new Set<string>();
          try {
            await c.query("BEGIN");
            await c.query("CREATE TABLE p(id INT PRIMARY KEY)");
            const before = h.governor.usedBytes;
            const sql = sqlFor("raw", "private_plan", payload);
            await c.query(sql);
            expect(h.governor.usedBytes - before).toBeGreaterThanOrEqual(2 * stringBytes(sql));
            entered.add("private charged");
            if (end !== "close") {
              await c.query(end);
              expect(h.governor.usedBytes).toBe(before);
              entered.add(end === "COMMIT" ? "private commit" : "private rollback");
            } else {
              c.close();
              entered.add("private close");
            }
          } finally {
            await close(h);
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.recordHistory(entered);
        },
      ),
      { seed, numRuns: 60 },
    );
    reach.assertReached();
  },
  120_000,
);
