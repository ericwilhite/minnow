import { expect, it } from "vitest";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { QuietLoad, assessGateA } from "./browser-perf/gate-a-policy.mjs";
import { prepareFixture, verifyFixture } from "./browser-perf/gate-a-fixture.mjs";
import { fixtureReplacer, fixtureReviver } from "./browser-perf/page/fixture-json.js";
import type { ExportGroup } from "./browser-perf/page/export-stream.js";
import { openFileSet } from "./node-sync-file.mjs";
import { FileStoreSession } from "../packages/core/src/next/storage/file-store.js";
import { PageCache } from "../packages/core/src/next/storage/page-cache.js";
import { ChunkReader } from "../packages/core/src/next/page/reader.js";
import {
  countingBudget,
  countingMemory,
} from "../packages/core/src/next/storage/space-test-support.js";
import { ColumnTarget } from "../packages/core/src/next/exec/column-target.js";
import { valueAt } from "../packages/core/src/next/page/test-support.js";

it("accepts <=1 and permanently refuses a busy or unavailable sample", () => {
  const allowed = new QuietLoad();
  for (const n of [0, 0.1, 0.999, 1]) allowed.observe(n, "pass");
  expect(() => allowed.assert()).not.toThrow();
  for (const n of [1.001, 2, NaN, Infinity, -1]) {
    const load = new QuietLoad();
    load.observe(n, "busy");
    load.observe(0, "later");
    expect(() => load.assert()).toThrow("REFUSED");
  }
});
function report() {
  return {
    built: { groups: 17, generatedGroups: 17 },
    exportedRows: 1048576,
    results: Array.from({ length: 5 }, (_, i) => ({
      rows: 1048576,
      mode: "scan",
      label: `cold persistent candidate scan round ${i}`,
      residentBeforeMiB: 0,
      rowsPerSecond: 200000,
      exportCpuSeconds: 5,
      rendererCampaignGrowthMiB: 200,
      executorStats: { path: "production-owner-api" },
      host: { before: { load: [0.2, 0, 0] }, after: { load: [0.3, 0, 0] } },
      fixture: { seed: 42, hash: "hash", independent: true },
    })),
  };
}
function pass0(r: ReturnType<typeof report>) {
  const p = r.results[0];
  if (!p) throw new Error("Missing pass");
  return p;
}
it("only judges five independent cold production API passes with matching identity", () => {
  expect(assessGateA(report(), 42, "hash", new QuietLoad()).allRatesMeetTarget).toBe(true);
  const changes = [
    (r: ReturnType<typeof report>) => {
      r.results.pop();
    },
    (r: ReturnType<typeof report>) => {
      r.built.generatedGroups = 1;
    },
    (r: ReturnType<typeof report>) => {
      pass0(r).fixture.hash = "wrong";
    },
    (r: ReturnType<typeof report>) => {
      pass0(r).host.after.load[0] = 1.1;
    },
    (r: ReturnType<typeof report>) => {
      pass0(r).executorStats.path = "production-executor";
    },
    (r: ReturnType<typeof report>) => {
      pass0(r).residentBeforeMiB = 2;
    },
  ];
  for (const mutate of changes) {
    const r = report();
    mutate(r);
    expect(() => assessGateA(r, 42, "hash", new QuietLoad())).toThrow("REFUSED");
  }
  const slow = report();
  const first = slow.results[0];
  if (!first) throw new Error("Missing pass");
  first.rowsPerSecond = 189999;
  expect(assessGateA(slow, 42, "hash", new QuietLoad()).allRatesMeetTarget).toBe(false);
});
it("preserves descriptor statistics, including byte arrays and special float bits", () => {
  const original = { a: 123n, b: Uint8Array.of(0, 128, 255), c: -0, d: NaN, e: Infinity };
  const copy = JSON.parse(
    JSON.stringify(original, fixtureReplacer),
    fixtureReviver,
  ) as typeof original;
  expect(copy).toEqual(original);
  expect(Object.is(copy.c, -0)).toBe(true);
  expect(() => JSON.parse('{"$bytes":[999]}', fixtureReviver)).toThrow();
});
it("builds distinct seeded groups, reuses a verified image and refuses changed bytes", async () => {
  const scratch = mkdtempSync(join(tmpdir(), "gate-a-fixture-test-"));
  const caches: string[] = [];
  try {
    const seed = 42,
      sourceHash = scratch.replaceAll("/", "_");
    const first = await prepareFixture(seed, 16, 8, sourceHash),
      second = await prepareFixture(seed, 16, 8, sourceHash);
    caches.push(join(first.dir, ".."));
    const replay = await prepareFixture(seed, 16, 8, sourceHash + "_replay");
    caches.push(join(replay.dir, ".."));
    expect(replay.manifest.files).toEqual(first.manifest.files);
    const different = await prepareFixture(seed + 1, 16, 8, sourceHash);
    caches.push(join(different.dir, ".."));
    expect(different.manifest.hash).not.toBe(first.manifest.hash);
    expect(different.manifest.files.find((f) => f.name === "data")?.sha256).not.toBe(
      first.manifest.files.find((f) => f.name === "data")?.sha256,
    );
    expect(first.reused).toBe(false);
    expect(second.reused).toBe(true);
    expect(first.manifest.hash).toBe(second.manifest.hash);
    expect(first.manifest.generatedGroups).toBe(3);
    const groups = JSON.parse(
      readFileSync(join(first.dir, "groups.json"), "utf8"),
      fixtureReviver,
    ) as ExportGroup[];
    const files = openFileSet(first.dir, false),
      session = new FileStoreSession(files, 1, () => undefined),
      cache = new PageCache(session),
      memory = countingMemory(Infinity);
    cache.attach(memory);
    const budget = countingBudget(Infinity),
      ids: bigint[] = [];
    try {
      for (const group of groups) {
        const chunk = group.chunks[0];
        if (!chunk) throw new Error("Missing ID column");
        const reader = await ChunkReader.open(
            {
              read: cache.read.bind(cache),
              peek: cache.peek.bind(cache),
              parsed: cache.parsed.bind(cache),
              attach: cache.attachParsed.bind(cache),
            },
            chunk,
            { budget },
          ),
          target = new ColumnTarget(chunk.type, 8, {
            take: () => undefined,
            give: () => undefined,
          });
        try {
          await reader.load(reader.directory.pages.map((_, i) => i));
          const out = target.decode(reader, { from: 0, to: 8 });
          if (out.form !== "flat") throw new Error("Expected flat IDs");
          for (let row = 0; row < 8; row++) ids.push(valueAt(out, row) as bigint);
        } finally {
          target.close();
          reader.close();
        }
      }
    } finally {
      cache.close();
      await session.close();
      for (const file of [files.log, files.data, ...files.roots]) file.close();
    }
    expect(ids).toEqual(Array.from({ length: 24 }, (_, r) => BigInt(((r + (seed % 24)) % 24) + 1)));
    expect(new Set(ids).size).toBe(24);
    const bytes = readFileSync(join(first.dir, "data"));
    bytes[bytes.length - 1] = (bytes[bytes.length - 1] ?? 0) ^ 1;
    writeFileSync(join(first.dir, "data"), bytes);
    await expect(
      verifyFixture(first.dir, { seed, exportedRows: 16, groupRows: 8, sourceHash }),
    ).rejects.toThrow("hash mismatch");
  } finally {
    for (const cache of caches) rmSync(cache, { recursive: true, force: true });
    rmSync(scratch, { recursive: true, force: true });
  }
}, 60000);
