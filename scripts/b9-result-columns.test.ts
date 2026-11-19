/** Law: SELECT/frame columns keep ordinal position regardless of output aliases.
 * PGlite array rows independently preserve numeric, duplicate and prototype names.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, B9_RESULT_COLUMNS_REACH } from "../packages/core/src/testing/reach.js";
import { startCPUWorker, type Verification } from "./lib/independent-cpu.mts";
import { rawRow, exactJSON, compareFloatResults, type RawResult } from "./b9-float-law.mts";
const root = fileURLToPath(new URL("..", import.meta.url)),
  names = ["2", "1", "same", "__proto__", "constructor", "é"],
  cases = names.flatMap((a) => names.flatMap((b) => names.map((c) => [a, b, c]))),
  quoted = (s: string) => '"' + s.replaceAll('"', '""') + '"';
const query = (aliases: readonly string[], from: string) =>
  `SELECT MIN(id::float8) AS ${quoted(aliases[0] ?? "")},SUM(f00) AS ${quoted(aliases[1] ?? "")},COUNT(*) AS ${quoted(aliases[2] ?? "")} FROM ${from}`;
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("b9-result-columns", [0xb9b41, 0xb9b42]))(
  "keeps exact key/count and tolerant SUM at their PG column ordinal (seed%i)",
  async (seed) => {
    const directory = await mkdtemp(join(tmpdir(), "b9-result-columns-")),
      generator = new URL("./next-executor-workload/gen.mjs", import.meta.url),
      reach = createReach("B9 result columns", B9_RESULT_COLUMNS_REACH);
    const gen = (await import(generator.href)) as {
      wideSpec(n: number): Array<{ name: string }>;
      value(column: unknown, index: number, rows: number): number;
    };
    const spec = gen.wideSpec(200).find((c) => c.name === "f00");
    if (!spec) throw new Error("f00");
    const values = Array.from({ length: 1000 }, (_, i) => gen.value(spec, i, 1000));
    await writeFile(
      join(directory, "gen.mjs"),
      `export * from ${JSON.stringify(generator.href)};\n`,
    );
    await writeFile(
      join(directory, "queries.mjs"),
      `export const W_QUERIES=${JSON.stringify(cases.map((aliases, i) => ({ id: "alias_" + String(i), sql: query(aliases, "events_wide"), ordered: true })))};export const S_QUERIES=[];export const S_TABLES=[];export const sqlFor=(q)=>q.sql;\n`,
    );
    const worker = await startCPUWorker(
      {
        module: fileURLToPath(new URL("./b9-perf-worker.mts", import.meta.url)),
        cwd: root,
        revision: "alias-main",
        data: {
          runtimeRoot: root,
          workloadRoot: directory,
          rows: 1000,
          imageIdentity: "b9-alias-law",
          capBytes: 256 * 2 ** 20,
        },
        execArgv: ["--max-old-space-size=1536", "--v8-pool-size=1"],
      },
      { arm: "control", cohort: -1, workload: "alias_0", boundary: "frames" },
    );
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.tuple(...[0, 1, 2].map(() => fc.integer({ min: 0, max: names.length - 1 }))),
          async (indexes) => {
            reach.beginHistory();
            const index =
                (indexes[0] ?? 0) * names.length ** 2 +
                (indexes[1] ?? 0) * names.length +
                (indexes[2] ?? 0),
              aliases = cases[index];
            if (!aliases) throw new Error("alias case");
            const actual = await worker.ask<Verification>({
                op: "verify",
                workload: "alias_" + String(index),
              }),
              row = (
                await pg.query(
                  query(aliases, "unnest($1::float8[]) WITH ORDINALITY t(f00,id)"),
                  [values],
                  { rowMode: "array" } as never,
                )
              ).rows[0] as unknown as [number, number, string | number | bigint];
            if (!row) throw new Error("PG result");
            const kinds = ["float64", "float64", "int64"],
              expected: Verification = {
                fingerprint: "postgres",
                determinismFingerprint: "postgres",
                rows: 1,
                detail: {
                  raw: {
                    version: 1,
                    schema: aliases.map((_, i) => ({
                      name: `column${String(i)}`,
                      kind: kinds[i],
                      type: exactJSON({ kind: kinds[i] }),
                    })),
                    eligible: [false, true, false],
                    rows: [rawRow([row[0], row[1], Number(row[2])], kinds)],
                    planIdentity: "postgres",
                  },
                },
              };
            const raw = (actual.detail as { raw: RawResult }).raw;
            expect(raw.schema.map((c) => c.kind)).toEqual(kinds);
            // IR/output vectors are positional. Output-name uniquification is
            // outside this law; the production comparison still checks names exactly.
            const byOrdinal = {
              ...actual,
              detail: {
                raw: {
                  ...raw,
                  schema: raw.schema.map((c, i) => ({ ...c, name: `column${String(i)}` })),
                },
              },
            };
            expect(
              compareFloatResults(byOrdinal, expected),
              exactJSON({ aliases, actual: raw, expected: expected.detail }),
            ).toBe(true);
            if (aliases.some((a) => /^\d+$/.test(a))) reach.reach("numeric aliases");
            if (new Set(aliases).size < aliases.length) reach.reach("duplicate aliases");
            if (aliases.includes("__proto__") || aliases.includes("constructor"))
              reach.reach("prototype aliases");
            if (aliases.includes("é")) reach.reach("UTF8 aliases");
            reach.reach("ordinal PG values");
          },
        ),
        { seed, numRuns: 16, examples: [[[0, 1, 2]], [[2, 2, 2]], [[3, 4, 0]], [[5, 0, 1]]] },
      );
      reach.assertReached();
    } finally {
      await worker.close();
      await rm(directory, { recursive: true, force: true });
    }
  },
  180000,
);
