// eslint-disable-next-line no-restricted-imports -- Repository-only corpus IO, excluded from tarballs.
import { readFileSync } from "node:fs";
import { afterAll, expect, it } from "vitest";
import { currentEngine } from "./differential-current.js";
import {
  assertIdentical,
  compareEngines,
  type EngineUnderTest,
  canonicalResult,
  encodeValue,
  replay,
  runWorkload,
  type CanonicalOutcome,
  type EngineFactory,
  type Workload,
} from "./differential-harness.js";
import { seedsFor } from "./seeds.js";
import { PROFILE_SEEDS, workloads } from "./differential-sources.js";
import { createReach, DIFFERENTIAL_REACH } from "./reach.js";

const reach = createReach("differential-harness", DIFFERENTIAL_REACH);
afterAll(() => reach.assertReached());
const profile = process.env.MINNOW_DIFFERENTIAL_PROFILE === "nightly" ? "nightly" : "fast";
const sources = await workloads(profile, seedsFor("differential-harness", PROFILE_SEEDS[profile]));
for (const workload of sources) {
  it(`cross-store ${workload.name}`, async () => {
    const engines: EngineUnderTest[] = [];
    const entered = new Set<string>([workload.name.split(":")[0] ?? ""]);
    try {
      for (const store of ["memory", "indexeddb", "opfs"] as const)
        engines.push(await currentEngine(store));
      await compareEngines(workload, engines, (outcome) => {
        entered.add(outcome.ok ? "success" : "refusal");
        if (outcome.ok) {
          entered.add(outcome.kind);
          if (outcome.kind !== "rows" && outcome.columns.length > 0) entered.add("returning");
        }
      });
      reach.recordHistory(entered);
    } finally {
      await Promise.all(engines.map((engine) => engine.close()));
    }
  }, 300_000);
}

it("matches the frozen evidence through the ordinary engine API", async () => {
  const frozen = JSON.parse(
    readFileSync(new URL("../../testdata/differential-corpus.json", import.meta.url), "utf8"),
    (_key, value: unknown) => {
      if (value && typeof value === "object" && "$date" in value)
        return new Date(String(value.$date));
      return value;
    },
  ) as { version: number; corpus: Array<{ workload: Workload; outcomes: CanonicalOutcome[] }> };
  expect(frozen.version).toBe(1);
  for (const { workload, outcomes } of frozen.corpus)
    assertIdentical(workload, outcomes, await runWorkload(currentEngine, "memory", workload));
}, 120_000);

const probe: Workload = {
  name: "mutant",
  operations: [
    { kind: "sql", sql: "SELECT 1 AS n UNION ALL SELECT 2 AS n" },
    { kind: "sql", sql: "SELECT CAST('1.20' AS NUMERIC) AS n" },
    { kind: "sql", sql: "SELECT 0.0 AS n" },
    { kind: "sql", sql: "SELECT * FROM absent" },
  ],
};
for (const mutation of ["drop", "reorder", "scale", "error", "negative-zero"] as const)
  it(`mutant ${mutation}: ${mutation === "reorder" ? "passes" : "fails"} for its intended difference`, async () => {
    const factory: EngineFactory = async (store) => {
      const inner = await currentEngine(store);
      let step = 0;
      return {
        close: () => inner.close(),
        async run(operation) {
          const index = step++;
          if (mutation === "error" && index === 3) throw new TypeError("mutant");
          const result = await inner.run(operation);
          if (mutation === "drop" && index === 0)
            return { ...result, rows: (result.rows ?? []).slice(1) };
          if (mutation === "reorder" && index === 0)
            return { ...result, rows: [...(result.rows ?? [])].reverse() };
          if (mutation === "scale" && index === 1) return { ...result, rows: [{ n: "1.200" }] };
          if (mutation === "negative-zero" && index === 2) return { ...result, rows: [{ n: -0 }] };
          return result;
        },
      };
    };
    const expected = await runWorkload(currentEngine, "memory", probe);
    const actual = await runWorkload(factory, "memory", probe);
    if (mutation === "reorder") assertIdentical(probe, expected, actual);
    else {
      const index =
        mutation === "drop" ? 0 : mutation === "scale" ? 1 : mutation === "negative-zero" ? 2 : 3;
      expect(
        actual.filter((value, i) => JSON.stringify(value) !== JSON.stringify(expected[i])),
      ).toHaveLength(1);
      expect(() => assertIdentical(probe, expected, actual)).toThrow(`"step":${String(index)}`);
    }
    const engines = [await currentEngine("memory"), await factory("memory")];
    try {
      if (mutation === "reorder") await compareEngines(probe, engines);
      else {
        const index =
          mutation === "drop" ? 0 : mutation === "scale" ? 1 : mutation === "negative-zero" ? 2 : 3;
        await expect(compareEngines(probe, engines)).rejects.toThrow(
          `mutant:step-${String(index)}`,
        );
      }
    } finally {
      await Promise.all(engines.map((engine) => engine.close()));
    }
  });

it("preserves exact domains, multiplicity, total ordering, codes and counts", async () => {
  expect(encodeValue(-0)).not.toBe(encodeValue(0));
  expect(encodeValue("1.20")).not.toBe(encodeValue("1.2"));
  expect(encodeValue(new Uint8Array([1, 2]))).not.toBe(encodeValue([1, 2]));
  expect(encodeValue({ a: 1, b: true })).toBe(encodeValue({ b: true, a: 1 }));
  expect(encodeValue(null)).not.toBe(encodeValue("null"));
  const operation = {
    kind: "sql",
    sql: "SELECT n ORDER BY n",
    totalOrder: "all projected values",
  } as const;
  expect(
    canonicalResult({ kind: "rows", columns: ["n"], rows: [{ n: 1 }, { n: 2 }] }, operation),
  ).not.toEqual(
    canonicalResult({ kind: "rows", columns: ["n"], rows: [{ n: 2 }, { n: 1 }] }, operation),
  );
  const outcomes = await replay(
    {
      run: () => Promise.reject(Object.assign(new RangeError("ignored"), { code: "22003" })),
      close: () => Promise.resolve(),
    },
    probe,
  );
  expect(outcomes[0]).toEqual({ ok: false, error: "RangeError", code: "22003" });
});

it("rejects column, domain, count and duplicate changes independently", () => {
  const operation = { kind: "sql", sql: "SELECT 1 AS n" } as const;
  const base = {
    kind: "rows",
    columns: ["n"],
    domains: [{ kind: "integer" }],
    rows: [{ n: 1 }, { n: 1 }],
    counts: { rowCount: 2 },
  };
  const expected = canonicalResult(base, operation);
  for (const mutant of [
    { ...base, columns: ["other"], rows: [{ other: 1 }, { other: 1 }] },
    { ...base, domains: [{ kind: "text" }] },
    { ...base, counts: { rowCount: 1 } },
    { ...base, rows: [{ n: 1 }] },
  ])
    expect(canonicalResult(mutant, operation)).not.toEqual(expected);
  expect(encodeValue([null, true, "2026-01-01", "1.20", new Uint8Array([0, 255])])).not.toBe(
    encodeValue([null, true, "2026-01-01", "1.2", new Uint8Array([0, 255])]),
  );
  expect(encodeValue(new Date("2026-01-01T00:00:00Z"))).not.toBe(
    encodeValue("2026-01-01T00:00:00.000Z"),
  );
  expect(encodeValue(Infinity)).not.toBe(encodeValue(-Infinity));
  expect(encodeValue(NaN)).toBe(encodeValue(Number("not a number")));
  expect(() => encodeValue(undefined)).toThrow("Unsupported differential value");
});

it("snapshots adapter-owned columns before the next operation reuses them", () => {
  const columns = ["n"];
  const outcome = canonicalResult(
    { kind: "rows", columns, rows: [{ n: 1 }] },
    { kind: "sql", sql: "SELECT 1 AS n" },
  );
  columns[0] = "reused";
  expect(outcome.ok && outcome.columns).toEqual(["n"]);
});
