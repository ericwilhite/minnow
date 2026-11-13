/** Warmed, already-decoded CASE operator; each observation owns a fresh VM. */
import { strict as assert } from "node:assert";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import type { ScalarExpr } from "../packages/core/dist/next/ir/ir.js";

export default async function create(context: CPUWorkerContext): Promise<CPUWorkerHarness> {
  const config = context.data as { runtimeRoot: string; pushes: number; warmups: number };
  const load = (name: string) =>
    import(pathToFileURL(join(config.runtimeRoot, "packages/core/dist/next", name + ".js")).href);
  const [
    { ProjectOperator },
    { OutputArena },
    { BOOL, TEXT, col, texts, numbers, UNCHARGED, valueAt },
  ] = (await Promise.all([
    load("exec/filter"),
    load("exec/kernels/vectors"),
    load("exec/kernels/test-support"),
  ])) as [
    typeof import("../packages/core/dist/next/exec/filter.js"),
    typeof import("../packages/core/dist/next/exec/kernels/vectors.js"),
    typeof import("../packages/core/dist/next/exec/kernels/test-support.js"),
  ];
  const rows = 1024;
  const batch = {
    length: rows,
    selection: null,
    columns: [
      numbers(
        BOOL,
        Array.from({ length: rows }, (_, i) => i % 2),
      ),
      texts(Array.from({ length: rows }, (_, i) => "value" + String(i))),
    ],
  };
  const expr: ScalarExpr = {
    op: "case",
    type: TEXT,
    branches: [{ when: col(0, BOOL), then: col(1, TEXT) }],
    otherwise: col(1, TEXT),
  };
  const arena = new OutputArena(UNCHARGED, rows);
  let seen = 0,
    checking = false,
    preflights = 0;
  const operator = new ProjectOperator(
    [expr],
    { arena, params: { slots: [] }, input: [BOOL, TEXT] },
    {
      push: (b) => {
        seen += b.selection?.count ?? b.length;
        if (checking)
          for (let i = 0; i < rows; i++)
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion -- the fixed fixture projects exactly one column
            assert.equal(valueAt(b.columns[0]!, i), "value" + String(i));
        return "ready";
      },
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    },
    {
      available: () => {
        preflights++;
        return 1e12;
      },
      signal: context.signal,
    },
  );
  let warm = false;
  return {
    revision: context.revision,
    identity: { ...config, rows, fixture: "B4 review CASE, alternating bool, short text" },
    workloads: ["case1024"],
    boundaries: ["operator"],
    async verify() {
      checking = true;
      assert.equal(operator.push(batch), "ready");
      checking = false;
      if (!warm) {
        for (let i = 0; i < config.warmups; i++) operator.push(batch);
        warm = true;
      }
      return { fingerprint: "CASE:1024:value0..value1023", rows: rows * config.pushes };
    },
    async run() {
      seen = 0;
      const before = preflights;
      for (let i = 0; i < config.pushes; i++) assert.equal(operator.push(batch), "ready");
      assert.equal(seen, rows * config.pushes);
      return { rows: seen, detail: { preflightCalls: preflights - before } };
    },
    async close() {
      await operator.closed;
      arena.close();
    },
  };
}
