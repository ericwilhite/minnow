/** Native W200 publication cost. Each arm owns an image written by its own build.
 * CPU includes normalization, inserts, WAL durability, root publication and log recycling.
 * Logical generation, DDL, verification, inspection and close are outside that clock.
 * Memory-file write bytes cover all native files; they do not measure fsync or browser RSS. */
import { strict as assert } from "node:assert";
import { execFileSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { wideBatch, wideSpec } from "./next-executor-workload/gen.mjs";
import { pairedCPU } from "./lib/paired-cpu.mts";
import type { InputValue } from "../packages/core/src/next/testing/executor-input.js";
import type { ColumnTotals } from "../packages/core/src/next/format/manifest.js";
// Probe scaffolding also compiles on a parent whose codec has no inline advice.
type ProbeColumn = ColumnTotals & { readonly ndv?: unknown };
const option = (name: string, fallback = "") => {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const rows = Number(option("--rows", "4096")),
  groupRows = Number(option("--group-rows", "1024")),
  transactionMode = option("--transactions", "batch");
assert(transactionMode === "batch" || transactionMode === "all");
assert(Number.isSafeInteger(rows) && rows > 0);
assert(Number.isSafeInteger(groupRows) && groupRows > 0);
interface Sample {
  cpuMs: number;
  generatorCpuMs: number;
  elapsedMs: number;
  physicalWriteBytes: number;
  chunkDataBytes: number;
  finalStatisticsBytes: number;
  finalExtraStatisticsBytes: number;
  knownColumns: number;
  groups: number;
  governorPeakBytes: number;
  nativeDataHighWaterBytes: number;
  nativeFreeBytes: number;
  nativeUntrackedFreeBytes: number;
  nativeWaitingBytes: number;
}
const cpu = (start: NodeJS.CpuUsage) => {
  const elapsed = process.cpuUsage(start);
  return (elapsed.user + elapsed.system) / 1000;
};
if (process.argv.includes("--worker")) {
  const root = resolve(option("--worker")),
    url = (relative: string) => pathToFileURL(join(root, "next", relative)).href;
  const { oracleRows } = (await import(
    url("testing/differential-adapter.js")
  )) as typeof import("../packages/core/src/next/testing/differential-adapter.js");
  const { StorageCore } = (await import(
    url("testing/storage-core.js")
  )) as typeof import("../packages/core/src/next/testing/storage-core.js");
  const { compileTableRules } = (await import(
    url("exec/rules.js")
  )) as typeof import("../packages/core/src/next/exec/rules.js");
  const { inputBatch } = (await import(
    url("testing/executor-input.js")
  )) as typeof import("../packages/core/src/next/testing/executor-input.js");
  const { MemoryFile } = (await import(
    url("storage/memory-store.js")
  )) as typeof import("../packages/core/src/next/storage/memory-store.js");
  const codec = (await import(
    url("format/manifest-codec.js")
  )) as typeof import("../packages/core/src/next/format/manifest-codec.js");
  const core = await new StorageCore(groupRows).open(),
    spec = wideSpec(200),
    write = MemoryFile.prototype.write;
  let physicalWriteBytes = 0,
    counted = false,
    cpuMs = 0,
    generatorCpuMs = 0;
  MemoryFile.prototype.write = function (buffer, options) {
    const n = write.call(this, buffer, options);
    if (counted) physicalWriteBytes += n;
    return n;
  };
  let result: Sample;
  try {
    await core.execute(
      `CREATE TABLE ndv_wide(${spec
        .map((c) => {
          assert(c.pg);
          return `${c.name} ${c.pg}`;
        })
        .join(",")})`,
    );
    const p = core.harness.path,
      table = p.catalog.tableByName("ndv_wide");
    assert(table);
    await p.quiesce();
    const [inputGrant] = await core.harness.governor.admitGroup({
      label: "B9 one W input batch",
      grants: [{ label: "input", minimumBytes: 8 << 20, spillable: true }],
    });
    assert(inputGrant);
    let sum = 0;
    const started = performance.now();
    counted = true;
    const begin = process.cpuUsage(),
      tx = transactionMode === "all" ? await p.begin() : null;
    cpuMs += cpu(begin);
    try {
      for (let at = 0; at < rows; at += 2048) {
        const generation = process.cpuUsage();
        const source = wideBatch(spec, at, Math.min(2048, rows - at), rows) as Record<
          string,
          InputValue[]
        >;
        for (const n of source.i00 ?? []) {
          assert(typeof n === "number");
          sum += n;
        }
        generatorCpuMs += cpu(generation);
        const work = process.cpuUsage();
        const input = inputBatch(table, source, inputGrant);
        const statement = tx?.statement({
          rules: (id) => {
            const definition = p.catalog.table(id);
            assert(definition);
            return compileTableRules(
              definition,
              p.catalog.version,
              core.lowerer.lowerTableRules(definition, p.catalog),
            );
          },
        });
        try {
          if (statement) {
            await statement.table(table.id).insert(input.batch, null);
            await statement.end();
          } else await core.insertVectors(table, input.batch);
        } catch (error) {
          statement?.rollback();
          throw error;
        } finally {
          input.close();
        }
        cpuMs += cpu(work);
      }
      const commit = process.cpuUsage();
      inputGrant.close();
      if (tx) {
        const receipt = await tx.commit(null);
        await receipt.durable;
      }
      await p.recycleLog();
      await p.quiesce();
      cpuMs += cpu(commit);
    } catch (error) {
      tx?.rollback();
      throw error;
    } finally {
      inputGrant.close();
      counted = false;
    }
    const elapsedMs = performance.now() - started;
    const check = await core.execute(
      "SELECT COUNT(*) AS n,SUM(i00) AS s,MIN(id) AS lo,MAX(id) AS hi FROM ndv_wide",
    );
    assert("batches" in check);
    const actual = oracleRows(check)[0];
    assert(actual);
    assert.deepEqual(Object.values(actual).map(String), [
      String(rows),
      String(sum),
      "1",
      String(rows),
    ]);
    const tableRoot = p.state().root.tables.find((t) => t.tableId === table.id);
    assert(tableRoot?.statistics);
    const [inspect] = await core.harness.governor.admitGroup({
      label: "B9 statistics inspection",
      grants: [{ label: "inspection", minimumBytes: 16 << 20, spillable: true }],
    });
    assert(inspect);
    try {
      const held = await core.harness.space.read([tableRoot.statistics], {
        budget: inspect,
        once: true,
      });
      try {
        const bytes = held.bytes[0];
        assert(bytes);
        const parsed = codec.decodeTableStatistics(bytes, inspect);
        try {
          const columns: readonly ProbeColumn[] = parsed.value.columns;
          const plain = {
            ...parsed.value,
            columns: columns.map((column) => {
              const { ndv, ...rest } = column;
              void ndv;
              return rest;
            }),
          };
          result = {
            cpuMs,
            generatorCpuMs,
            elapsedMs,
            physicalWriteBytes,
            chunkDataBytes: parsed.value.columns.reduce((n, c) => n + c.bytes, 0),
            finalStatisticsBytes: bytes.length,
            finalExtraStatisticsBytes: bytes.length - codec.encodeTableStatistics(plain).length,
            knownColumns: columns.filter((c) => c.ndv !== undefined).length,
            groups: parsed.value.groupCount,
            governorPeakBytes: core.harness.governor.peakBytes,
            nativeDataHighWaterBytes: core.harness.space.stats.dataBytes,
            nativeFreeBytes: core.harness.space.stats.freeBytes,
            nativeUntrackedFreeBytes: core.harness.space.stats.untrackedFreeBytes,
            nativeWaitingBytes: core.harness.space.stats.waitingBytes,
          };
        } finally {
          inspect.free(parsed.charge);
        }
      } finally {
        held.release();
      }
    } finally {
      inspect.close();
    }
  } finally {
    MemoryFile.prototype.write = write;
    await core.close();
  }
  assert.equal(core.harness.governor.usedBytes, 0);
  console.log(JSON.stringify(result));
} else {
  const rounds = Number(option("--rounds", "9"));
  assert(Number.isSafeInteger(rounds) && rounds >= 9);
  const roots = { parent: resolve(option("--parent")), candidate: resolve(option("--candidate")) };
  const profileDirectory = option("--profile-dir");
  if (profileDirectory) await mkdir(profileDirectory, { recursive: true });
  const run = (arm: keyof typeof roots, profile = false): Sample =>
    JSON.parse(
      execFileSync(
        process.execPath,
        [
          "--v8-pool-size=1",
          ...(profile && profileDirectory
            ? [
                "--cpu-prof",
                `--cpu-prof-dir=${profileDirectory}`,
                `--cpu-prof-name=ndv-${arm}.cpuprofile`,
              ]
            : []),
          fileURLToPath(import.meta.url),
          "--worker",
          roots[arm],
          "--rows",
          String(rows),
          "--group-rows",
          String(groupRows),
          "--transactions",
          transactionMode,
        ],
        { encoding: "utf8", maxBuffer: 1 << 20, env: { ...process.env, UV_THREADPOOL_SIZE: "1" } },
      ),
    ) as Sample;
  const warmups = [];
  for (const arm of ["parent", "candidate"] as const) warmups.push({ arm, ...run(arm, true) });
  const pairs = [];
  for (let round = 0; round < rounds; round++) {
    const order =
      round % 2 ? (["candidate", "parent"] as const) : (["parent", "candidate"] as const);
    const samples = {} as Record<keyof typeof roots, Sample>;
    for (const arm of order) samples[arm] = run(arm);
    assert.equal(samples.parent.chunkDataBytes, samples.candidate.chunkDataBytes);
    assert.equal(samples.parent.knownColumns, 0, "MST1 control has no persisted NDV");
    assert.equal(
      samples.candidate.knownColumns,
      200,
      "complete published NDV in the measured workload",
    );
    const pair = { round, order, ...samples };
    pairs.push(pair);
    console.error(JSON.stringify(pair));
  }
  const output = resolve(option("--output"));
  await writeFile(
    output,
    JSON.stringify(
      {
        rows,
        columns: 200,
        groupRows,
        transactionMode,
        inputReserveBytes: 8 << 20,
        warmups,
        roots,
        rounds,
        boundary:
          "native normalization/insert/durable/root publication/log recycle; generator CPU reported separately; all memory-file writes; inspection/setup/close excluded",
        limits:
          "local small cells only unless rows=1000000 on hosted runner; no fsync/browser RSS measurement; prototype runtime requires ordinary build hash confirmation",
        comparison: pairedCPU(
          pairs.map((p) => p.parent.cpuMs),
          pairs.map((p) => p.candidate.cpuMs),
        ),
        pairs,
      },
      null,
      2,
    ) + "\n",
  );
}
