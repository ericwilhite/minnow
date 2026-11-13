/** PostgreSQL cursor FETCH 1 is the authority for error order and the delivered prefix. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXPRESSION_ERROR_REACH } from "../../testing/reach.js";
import { VOLATILE_FUNCTIONS } from "../ir/functions.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { Batch } from "../vector/vector.js";
import { Governor } from "../memory/governor.js";
import { FilterOperator, ProjectOperator } from "./filter.js";
import { OutputArena } from "./kernels/vectors.js";
import { compileExpr } from "./kernels/compile.js";
import {
  BOOL,
  I32,
  TEXT,
  call,
  cmp,
  col,
  lit,
  numbers,
  texts,
  valueAt,
} from "./kernels/test-support.js";
import type { Operator } from "./pipeline.js";

const integer = (n: number) => lit(numbers(I32, [n]));
const state = (error: unknown) =>
  (error as { sqlState?: string; code?: string }).sqlState ?? (error as { code?: string }).code;

it.each(seedsFor("expression-error-order", [0xb7b7, 0xb7b8]))(
  "delivers PostgreSQL's first error and prefix at every batch size seed%i",
  async (seed) => {
    const pg = new PGlite();
    const reach = createReach("expression error order", EXPRESSION_ERROR_REACH);
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 8 }),
          fc.integer({ min: 1, max: 8 }),
          fc.integer({ min: 1, max: 9 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (n, division, overflow, pressure, filter, lazy, keyFirst) => {
            reach.beginHistory();
            const a = (division % n) + 1,
              b = (overflow % (n + 1)) + 1;
            const rawX = `10/(${String(a)}-i)`;
            const x = lazy ? `coalesce(CASE WHEN i>0 THEN ${rawX} ELSE 1/(i-i) END,1/(i-i))` : rawX;
            const y = `2147483647+(i-${String(b - 1)})`;
            const key = "CASE WHEN i=1 THEN repeat('x',1024) ELSE 'k' END";
            const sql = filter
              ? `SELECT i, ${key} k FROM generate_series(1,${String(n)})g(i) WHERE CASE WHEN ${x}>0 THEN ${y}>0 ELSE false END`
              : `SELECT ${keyFirst ? `${key} k, ${x} x, ${y} y` : `${x} x, ${y} y, ${key} k`} FROM generate_series(1,${String(n)})g(i)`;
            const expected: unknown[][] = [];
            let expectedError: string | undefined;
            await pg.exec(`BEGIN; DECLARE c CURSOR FOR ${sql}`);
            try {
              for (;;) {
                const rows = (await pg.query<Record<string, unknown>>("FETCH 1 FROM c")).rows;
                if (!rows.length) break;
                expected.push(...rows.map((row) => Object.values(row)));
              }
            } catch (error) {
              expectedError = state(error);
            } finally {
              await pg.exec("ROLLBACK");
            }
            expect(expectedError).toMatch(/^22/);
            const i = col(0, I32),
              rawDX = call("div", I32, integer(10), call("sub", I32, integer(a), i)),
              dy = call("add", I32, integer(2147483647), call("sub", I32, i, integer(b - 1)));
            const branch: ScalarExpr = {
              op: "case",
              type: TEXT,
              branches: [
                { when: cmp("=", i, integer(1)), then: { op: "param", slot: 0, type: TEXT } },
              ],
              otherwise: lit(texts(["k"])),
            };
            const bad = call("div", I32, integer(1), call("sub", I32, i, i));
            const dx = lazy
              ? call(
                  "coalesce",
                  I32,
                  {
                    op: "case",
                    type: I32,
                    branches: [{ when: cmp(">", i, integer(0)), then: rawDX }],
                    otherwise: bad,
                  },
                  bad,
                )
              : rawDX;
            const k = branch;
            // Every partition of this same history must expose the same prefix and error.
            for (let size = 1; size <= n; size++) {
              const governor = new Governor(16 << 20);
              const grant = await governor.admit({
                label: "expressions",
                minimumBytes: 0,
                spillable: true,
              });
              const competitor = await governor.admit({
                label: "pinned",
                minimumBytes: 0,
                spillable: true,
              });
              const arena = new OutputArena(grant, n);
              const context = {
                arena,
                input: [I32],
                params: { slots: [texts(["x".repeat(1024)])] },
              };
              const actual: unknown[][] = [];
              let released = 0;
              const sink: Operator = {
                push: (batch) => {
                  for (let row = 0; row < (batch.selection?.count ?? batch.length); row++)
                    actual.push(
                      batch.columns.map((v) => valueAt(v, batch.selection?.rows[row] ?? row)),
                    );
                  return "blocked";
                },
                ready: () => Promise.resolve(),
                finish: () => undefined,
                abort: () => undefined,
              };
              const project = new ProjectOperator(
                filter ? [i, k] : keyFirst ? [k, dx, dy] : [dx, dy, k],
                context,
                sink,
                {
                  signal: new AbortController().signal,
                  available: () => {
                    if (pressure) reach.reach("denied prefix error");
                    return pressure ? 0 : 1e12;
                  },
                },
              );
              const operator = filter
                ? new FilterOperator(
                    {
                      op: "case",
                      type: BOOL,
                      branches: [
                        { when: cmp(">", dx, integer(0)), then: cmp(">", dy, integer(0)) },
                      ],
                      otherwise: lit(numbers(BOOL, [0])),
                    },
                    context,
                    project,
                  )
                : project;
              let actualError: string | undefined;
              try {
                if (pressure)
                  expect(
                    competitor.allocate(
                      governor.capBytes - governor.reserveBytes - governor.usedBytes,
                    ),
                  ).toBe(true);
                for (let start = 0; start < n; start += size) {
                  const length = Math.min(size, n - start);
                  const batch: Batch = {
                    length,
                    selection: null,
                    columns: [
                      numbers(
                        I32,
                        Array.from({ length }, (_, j) => start + j + 1),
                      ),
                    ],
                  };
                  if (operator.push(batch, { release: () => released++ }) === "blocked")
                    await operator.ready();
                }
              } catch (error) {
                actualError = state(error);
              } finally {
                operator.abort(new Error("cleanup"));
                await operator.closed;
                await project.closed;
                arena.close();
                grant.close();
                competitor.close();
                expect(governor.usedBytes).toBe(0);
              }
              expect(actualError, `size=${String(size)} ${sql}`).toBe(expectedError);
              expect(actual, `size=${String(size)} ${sql}`).toEqual(expected);
              expect(released).toBeGreaterThan(0);
              if (size === 1) reach.reach("single row");
              if (size === n) reach.reach("full batch");
            }
            reach.reach(filter ? "filter" : "target list");
            reach.reach(pressure ? "pinned competitor" : "ordinary");
            reach.reach(expected.length ? "emitted prefix" : "first row error");
            reach.reach(expectedError === "22012" ? "division first" : "overflow first");
            if (lazy) reach.reach("lazy COALESCE CASE");
          },
        ),
        {
          seed,
          numRuns: 12,
          endOnFailure: true,
          examples: [
            [3, 2, 8, false, false, true, false],
            [3, 2, 8, true, false, false, false],
            [2, 1, 0, false, false, false, false],
            [2, 0, 0, false, false, false, false],
            [2, 1, 0, true, false, true, false],
            [3, 2, 8, false, true, true, false],
            [3, 2, 8, true, true, false, false],
            [3, 2, 8, true, false, true, true],
            [2, 1, 0, true, false, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  60000,
);

it("keeps volatile functions refused: precise replay must be revisited before allowing them", () => {
  expect([...VOLATILE_FUNCTIONS].sort()).toEqual(["gen_random_uuid", "nextval", "random"]);
  const arena = new OutputArena({ allocate: () => true, free: () => undefined }, 8);
  try {
    expect(() =>
      compileExpr(
        {
          op: "call",
          fn: "nextval",
          args: [{ op: "param", type: { kind: "int64" }, slot: 0 }],
          type: { kind: "int64" },
        },
        { arena, input: [], params: { slots: [] } },
      ),
    ).toThrow(/sequences/);
    for (const [fn, type] of [
      ["random", { kind: "float64" }],
      ["gen_random_uuid", { kind: "uuid" }],
    ] as const)
      expect(() =>
        compileExpr(
          { op: "call", fn, args: [], type },
          { arena, input: [], params: { slots: [] } },
        ),
      ).toThrow(/volatile/);
  } finally {
    arena.close();
  }
});

it("never sizes a successful ordinary allocation", async () => {
  const arena = new OutputArena({ allocate: () => true, free: () => undefined }, 8);
  const expr: ScalarExpr = {
    op: "case",
    type: TEXT,
    branches: [{ when: col(0, BOOL), then: col(1, TEXT) }],
    otherwise: col(1, TEXT),
  };
  const operator = new ProjectOperator(
    [expr],
    { arena, input: [BOOL, TEXT], params: { slots: [] } },
    {
      push: () => "ready",
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    },
    {
      available: () => {
        throw new Error("ordinary path consulted sizing");
      },
      signal: new AbortController().signal,
    },
  );
  try {
    expect(
      operator.push({
        length: 2,
        selection: null,
        columns: [numbers(BOOL, [1, 0]), texts(["a", "b"])],
      }),
    ).toBe("ready");
  } finally {
    await operator.closed;
    arena.close();
  }
});
