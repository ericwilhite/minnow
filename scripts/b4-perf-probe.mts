/** Branch-only hosted W1M probe, serial fresh VMs. Never merge this campaign into main.
 * Nine three-way cohorts, ten COMPLETE native queries per all-thread CPU bracket.
 * A/A controls precede comparisons; failure/incomplete evidence is retained, never discarded.
 */
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { openSync, closeSync } from "node:fs";
import { createHash } from "node:crypto";
import { appendFile, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { cpus, hostname, totalmem } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { pairedCPU } from "./lib/paired-cpu.mts";
import { startCPUWorker, type CPUArm, type CPUValue } from "./lib/independent-cpu.mts";
import type { B4Capture, B4Verification, B4WorkerData } from "./b4-perf-worker.mts";

const option = (name: string, fallback: string) => {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const root = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  output = resolve(option("--output", join(process.env.HOME ?? ".", ".cache/b4-probe/results"))),
  roots = {
    main: resolve(option("--main", "")),
    candidate: resolve(option("--candidate", "")),
    frozen: resolve(option("--frozen", "")),
  },
  componentParentRoot = resolve(option("--component-parent", "")),
  frozenSHA = "5811568dbe5a1c6c3756e3df19789deb75a7ffbf",
  cells = [
    "scan_proj3_agg",
    "scan_selective_30col",
    "agg_numeric_sum",
    "join_star_3dims",
    "join_fact_fact_int",
    "join_semi_exists",
    "join_anti_not_exists",
    "scan_plain3",
  ],
  aaCells = ["scan_proj3_agg", "scan_selective_30col", "agg_numeric_sum"],
  nodeFlags = ["--max-old-space-size=3072", "--v8-pool-size=1"],
  orders = [
    ["frozen", "main", "candidate"],
    ["main", "candidate", "frozen"],
    ["candidate", "frozen", "main"],
    ["frozen", "candidate", "main"],
    ["candidate", "main", "frozen"],
    ["main", "frozen", "candidate"],
  ] as const;
type Runtime = keyof typeof roots;
const git = (path: string, ...args: string[]) =>
  execFileSync("git", ["-C", path, ...args], { encoding: "utf8" }).trim();
const json = (file: string, data: unknown) =>
  writeFile(join(output, file), JSON.stringify(data, null, 2));
assert.equal(process.version, "v26.12.0");
assert(
  process.env.GITHUB_ACTIONS === "true" && process.env.GITHUB_RUN_ID,
  "Workflow-only W1M probe; do not run on the shared development host",
);
assert(totalmem() >= 14 * 2 ** 30, "Dedicated 16 GB runner required");
assert(process.env.TMPDIR?.includes("/.cache/"), "Scratch must stay in ~/.cache");
for (const name of Object.keys(roots) as Runtime[]) {
  assert(roots[name].includes("/.cache/"), `${name} checkout must be cache-only`);
  assert.equal(
    git(roots[name], "status", "--porcelain"),
    "",
    `${name} source must be committed and clean`,
  );
}
const revisions = Object.fromEntries(
  (Object.keys(roots) as Runtime[]).map((name) => [name, git(roots[name], "rev-parse", "HEAD")]),
) as Record<Runtime, string>;
assert.equal(revisions.frozen, frozenSHA);
assert(componentParentRoot.includes("/.cache/"), "Component parent must be cache-only");
assert.equal(
  git(componentParentRoot, "status", "--porcelain"),
  "",
  "Clean component parent required",
);
const componentParentRevision = git(componentParentRoot, "rev-parse", "HEAD");
git(roots.candidate, "merge-base", "--is-ancestor", componentParentRevision, revisions.candidate);
// Applying the isolated candidate onto fresh main is the caller's responsibility. Enforce
// ancestry and record the complete diff so an older base cannot masquerade as current main.
git(roots.candidate, "merge-base", "--is-ancestor", revisions.main, revisions.candidate);
const controller = new AbortController();
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.once(signal, () => controller.abort(new Error(signal)));
await mkdir(output, { recursive: true });
const events = (event: unknown) => {
  writes = writes.then(() =>
    appendFile(join(output, "events.jsonl"), JSON.stringify(event) + "\n"),
  );
};
let writes = Promise.resolve();
async function treeHashes(path: string) {
  const files = (await readdir(path, { recursive: true, withFileTypes: true }))
      .filter((entry) => entry.isFile())
      .map((entry) => join(entry.parentPath, entry.name))
      .sort(),
    hashes: Record<string, string> = {};
  for (const file of files)
    hashes[relative(path, file)] = createHash("sha256")
      .update(await readFile(file))
      .digest("hex");
  return hashes;
}
const sourceHashes = {
    main: await treeHashes(join(roots.main, "packages/core/dist/next")),
    candidate: await treeHashes(join(roots.candidate, "packages/core/dist/next")),
    frozen: await treeHashes(join(roots.frozen, "packages/core/dist/next")),
  },
  componentParentSources = await treeHashes(join(componentParentRoot, "packages/core/dist/next")),
  workloadRoot = join(root, "scripts/next-executor-workload"),
  workload = await treeHashes(workloadRoot),
  identities = {
    main: `b4-main-${revisions.main}`,
    frozen: `b4-frozen-${revisions.frozen}`,
  },
  images = {
    main: join(output, "main-writer-image"),
    frozen: join(output, "frozen-writer-image"),
  },
  pins: Record<Runtime, Record<string, string>> = {
    main: {},
    candidate: {},
    frozen: {},
  };
assert.equal(
  workload["gen.mjs"],
  "8b3d70312ac54a87e69f2a0672ed49b024baab7cf9097d92cf6e11da0189fd95",
  "frozen W1M generator changed",
);
assert.equal(
  workload["queries.mjs"],
  "abcc9f2809d04270569fe6b9e37867978d0c4c82e8a5ea331461d6f3a37347f2",
  "frozen query catalogue changed",
);
await json("protocol.json", {
  started: new Date().toISOString(),
  node: process.version,
  nodeFlags,
  hostname: hostname(),
  cpus: cpus(),
  totalMemory: totalmem(),
  revisions,
  roots,
  sourceHashes,
  component: {
    parentRoot: componentParentRoot,
    parentRevision: componentParentRevision,
    parentSources: componentParentSources,
    rounds: 9,
    repeats: 512,
    warmups: 4,
    capBytes: 4 * 2 ** 20,
    fixtureRows: 65_536,
    windows: [2048, 8192, 32768],
    transitionRows: 2048,
    heapMiB: 2048,
    boundary:
      "selection-first loaded-reader reductions; setup/verification outside CPU; all threads/natural GC",
    acceptance:
      "all six A/A neutral; all six A/B accepted gain and median saving above full paired spread",
    limit: "repeated 65,536-row fixture, not W1M/W5M or whole-query/renderer memory evidence",
  },
  workload,
  candidateChanges: git(roots.candidate, "diff", "--stat", revisions.main, revisions.candidate),
  workloadChangesFromFrozen: git(
    root,
    "diff",
    frozenSHA,
    "HEAD",
    "--",
    "scripts/next-executor-workload/gen.mjs",
    "scripts/next-executor-workload/queries.mjs",
  ),
  cells,
  aaCells,
  rows: 1_000_000,
  columns: 200,
  capBytes: 256 * 2 ** 20,
  cohorts: 9,
  repeats: 10,
  orders,
  boundary:
    "native released frames; lower/prepare/execute/release inside CPU, no IPC/renderer or forced GC",
  verification:
    "lossless full fresh/candidate values; separate frozen ten-significant-digit canonical hashes; plain3 unordered multiset with multiplicity and generator proof",
  limits: [
    "Node MemoryBackend, not browser/OPFS",
    "W5M not run",
    "fsync, renderer RSS, cold I/O not measured",
    "historical canonical hashes are not exact NUMERIC proof",
  ],
});
const arm = (name: Runtime, restore = true): CPUArm => {
  const imageName = name === "frozen" ? "frozen" : "main",
    data: B4WorkerData = {
      runtimeRoot: roots[name],
      workloadRoot,
      rows: 1_000_000,
      capBytes: 256 * 2 ** 20,
      repeats: 10,
      imageIdentity: identities[imageName],
      ...(restore ? { image: images[imageName], expected: pins[name] } : {}),
    };
  return {
    module: fileURLToPath(new URL("./b4-perf-worker.mts", import.meta.url)),
    cwd: roots[name],
    revision: revisions[name],
    data,
    execArgv: nodeFlags,
    env: { UV_THREADPOOL_SIZE: "1" },
  };
};
const open = (name: Runtime, workload: string, cohort: number, restore = true) =>
  startCPUWorker(
    arm(name, restore),
    {
      arm: name === "candidate" ? "candidate" : "control",
      cohort,
      workload,
      boundary: "frames",
    },
    events,
    controller.signal,
  );
interface ComponentResult {
  rounds: number;
  repeats: number;
  warmups: number;
  capBytes: number;
  records: Array<{
    nullable: boolean;
    selectionWindowRows: number;
    inputRowsPerSample: number;
    result: ReturnType<typeof pairedCPU>;
    medianSavingBeatsFullRange: boolean;
  }>;
}
async function confirmComponents() {
  const affinity = execFileSync(
    "python3",
    ["-c", "import os; print(min(os.sched_getaffinity(0)))"],
    { encoding: "utf8" },
  ).trim();
  assert(/^\d+$/.test(affinity), "Invalid dedicated component CPU affinity");
  await json("component-protocol.json", {
    affinity,
    parent: componentParentRevision,
    candidate: revisions.candidate,
    repeats: 512,
    rounds: 9,
    warmups: 4,
    capBytes: 4 * 2 ** 20,
  });
  for (const label of ["AA", "AB"] as const) {
    const directory = join(output, `component-${label}`),
      log = openSync(join(output, `component-${label}.log`), "w"),
      args = [
        "-c",
        affinity,
        process.execPath,
        "--v8-pool-size=1",
        join(root, "scripts/next-encoded-page-bench.mts"),
        "--control-root",
        componentParentRoot,
        "--candidate-root",
        label === "AA" ? componentParentRoot : roots.candidate,
        "--candidate-mode",
        label === "AA" ? "decoded" : "encoded",
        "--rounds",
        "9",
        "--repeats",
        "512",
        "--warmups",
        "4",
        "--cap-bytes",
        String(4 * 2 ** 20),
        "--output",
        directory,
      ];
    await json("active.json", { phase: "component", label, affinity, args });
    let executionError: Error | undefined;
    try {
      const child = spawn("taskset", args, {
        cwd: root,
        env: { ...process.env, UV_THREADPOOL_SIZE: "1", NODE_OPTIONS: "--max-old-space-size=2048" },
        stdio: ["ignore", log, log],
        signal: controller.signal,
      });
      await new Promise<void>((yes, no) => {
        let launchError: Error | undefined;
        child.once("error", (error) => {
          launchError = error;
        });
        child.once("close", (code, signal) => {
          if (code === 0 && signal === null && launchError === undefined) yes();
          else
            no(
              launchError ??
                new Error(`Component ${label} exited ${String(code)}/${String(signal)}`),
            );
        });
      });
    } catch (error) {
      executionError = error instanceof Error ? error : new Error(String(error));
    } finally {
      closeSync(log);
    }
    // Preserve partial records without hiding a failed or incomplete component attempt.
    try {
      await writeFile(
        join(output, `component-${label}.json`),
        await readFile(join(directory, "result.json")),
      );
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) {
        if (executionError)
          throw new AggregateError(
            [executionError, error],
            "Component and evidence archival failed",
            { cause: error },
          );
        throw error;
      }
    }
    if (executionError) throw executionError;
    const measured = JSON.parse(
      await readFile(join(directory, "result.json"), "utf8"),
    ) as ComponentResult;
    assert.equal(measured.rounds, 9);
    assert.equal(measured.repeats, 512);
    assert.equal(measured.warmups, 4);
    assert.equal(measured.capBytes, 4 * 2 ** 20);
    assert.deepEqual(
      measured.records.map((record) => [record.nullable, record.selectionWindowRows]),
      [
        [false, 2048],
        [false, 8192],
        [false, 32768],
        [true, 2048],
        [true, 8192],
        [true, 32768],
      ],
    );
    for (const record of measured.records) {
      assert.equal(record.result.pairs.length, 9);
      assert.equal(record.inputRowsPerSample, 65_536 * 512);
      if (label === "AA")
        assert(
          !record.result.acceptedGain && record.result.noRegression,
          "Non-neutral identical-build component control; reject timing claims",
        );
      else
        assert(
          record.result.acceptedGain && record.medianSavingBeatsFullRange,
          "Component gain must beat both paired IQR and full spread",
        );
    }
  }
}
try {
  await confirmComponents();
  const firstCell = cells[0];
  assert(firstCell, "At least one cell is required");
  for (const name of ["frozen", "main"] as const) {
    const worker = await open(name, firstCell, -1, false);
    try {
      await json("active.json", {
        phase: "capture",
        runtime: name,
        identity: worker.hello,
      });
      const capture = await worker.ask<B4Capture>({
        op: "command",
        name: "capture",
        data: { directory: images[name], cells },
      });
      pins[name] = capture.fingerprints;
      await json(`${name}-capture.json`, capture);
    } finally {
      await worker.close();
    }
  }
  pins.candidate = pins.main;
  // Independent full plain3 generator proof for BOTH writer-native images before timing.
  let expectedPlain: { fingerprint: string; exactFingerprint: string; rows: number } | undefined;
  for (const name of ["frozen", "main", "candidate"] as const) {
    const worker = await open(name, "scan_plain3", -1);
    try {
      await json("active.json", {
        phase: "plain-generator",
        runtime: name,
        identity: worker.hello,
      });
      if (!expectedPlain)
        expectedPlain = await worker.ask({
          op: "command",
          name: "plain-generator",
        });
      assert(expectedPlain);
      const observed = await worker.ask<B4Verification>({
        op: "verify",
        workload: "scan_plain3",
      });
      await json(`${name}-plain-generator.json`, {
        expected: expectedPlain,
        observed,
      });
      assert.equal(observed.rows, expectedPlain.rows);
      assert.equal(observed.fingerprint, expectedPlain.fingerprint);
      assert.equal(
        observed.exactFingerprint,
        expectedPlain.exactFingerprint,
        "plain3 lossless generator mismatch",
      );
    } finally {
      await worker.close();
    }
  }
  async function sample(name: Runtime, workload: string, cohort: number, label: string) {
    const worker = await open(name, workload, cohort);
    try {
      await json("active.json", {
        phase: label,
        runtime: name,
        workload,
        cohort,
        identity: worker.hello,
      });
      const before = await worker.ask<B4Verification>({
          op: "verify",
          workload,
        }),
        measured = await worker.ask<CPUValue>({
          op: "run",
          workload,
          boundary: "frames",
        }),
        after = await worker.ask<B4Verification>({ op: "verify", workload });
      assert.deepEqual(after, before, "output changed across measurement");
      assert.equal(measured.rows, before.rows * 10);
      assert(Number.isFinite(measured.cpuMs) && measured.cpuMs >= 0);
      assert(Math.abs(measured.cpuMs - measured.userMs - measured.systemMs) < 1e-6);
      const result = {
        name,
        workload,
        cohort,
        label,
        identity: worker.hello,
        verification: before,
        ...measured,
        cpuMsPerQuery: measured.cpuMs / 10,
      };
      await appendFile(join(output, "samples.jsonl"), JSON.stringify(result) + "\n");
      console.log(
        JSON.stringify({
          name,
          workload,
          cohort,
          label,
          cpuMs: result.cpuMsPerQuery,
        }),
      );
      return result;
    } finally {
      await worker.close();
    }
  }
  for (const workload of aaCells) {
    const first: number[] = [],
      second: number[] = [];
    for (let cohort = 0; cohort < 9; cohort++) {
      const order = cohort % 2 === 0 ? ["A", "B"] : ["B", "A"];
      for (const label of order) {
        const measured = await sample("main", workload, cohort, `AA-${label}`);
        (label === "A" ? first : second).push(measured.cpuMsPerQuery);
      }
    }
    const result = pairedCPU(first, second);
    await json(`AA-${workload}.json`, result);
    assert(
      !result.acceptedGain && result.noRegression,
      "identical-build calibration is noisy; retain attempt and reject timing claims",
    );
  }
  for (const workload of cells) {
    const samples: Array<Awaited<ReturnType<typeof sample>>> = [],
      comparisons: Array<{
        cohort: number;
        frozenLosslessMatchesMain: boolean;
      }> = [];
    for (let cohort = 0; cohort < 9; cohort++) {
      const observations = new Map<Runtime, B4Verification>();
      const order = orders[cohort % orders.length];
      assert(order, "Missing balanced cohort order");
      for (const name of order) {
        const measured = await sample(name, workload, cohort, "three-way");
        samples.push(measured);
        observations.set(name, measured.verification);
      }
      const frozen = observations.get("frozen"),
        main = observations.get("main"),
        candidate = observations.get("candidate");
      assert(frozen && main && candidate, "Missing complete three-way observations");
      assert.equal(candidate.fingerprint, main.fingerprint);
      assert.equal(main.fingerprint, frozen.fingerprint);
      assert.equal(candidate.rows, main.rows);
      assert.equal(main.rows, frozen.rows);
      assert.deepEqual(
        candidate.columns,
        main.columns,
        "every fresh/candidate output column must match",
      );
      assert.equal(
        candidate.exactFingerprint,
        main.exactFingerprint,
        "lossless fresh/candidate drift",
      );
      comparisons.push({
        cohort,
        frozenLosslessMatchesMain: frozen.exactFingerprint === main.exactFingerprint,
      });
      await json(`${workload}-progress.json`, { samples, comparisons });
    }
    const values = (name: Runtime) =>
        samples.filter((sample) => sample.name === name).map((sample) => sample.cpuMsPerQuery),
      result = {
        workload,
        mainVsFrozen: pairedCPU(values("frozen"), values("main")),
        candidateVsMain: pairedCPU(values("main"), values("candidate")),
        candidateVsFrozen: pairedCPU(values("frozen"), values("candidate")),
        samples,
        comparisons,
      };
    await json(`${workload}.json`, result);
    assert(result.candidateVsMain.noRegression, `${workload}: accepted whole-cell regression`);
  }
  for (const name of ["main", "candidate"] as const)
    for (const workload of ["scan_proj3_agg", "agg_numeric_sum"]) {
      const worker = await open(name, workload, -1);
      try {
        await worker.ask({ op: "verify", workload });
        await json(
          `${name}-${workload}-profile.json`,
          await worker.ask({
            op: "command",
            name: "profile",
            data: {
              workload,
              output: join(output, `${name}-${workload}.cpuprofile`),
            },
          }),
        );
      } finally {
        await worker.close();
      }
    }
  for (const name of Object.keys(roots) as Runtime[])
    assert.deepEqual(
      await treeHashes(join(roots[name], "packages/core/dist/next")),
      sourceHashes[name],
      `${name} changed during campaign`,
    );
  assert.deepEqual(
    await treeHashes(join(componentParentRoot, "packages/core/dist/next")),
    componentParentSources,
    "Component parent changed",
  );
  assert.deepEqual(await treeHashes(workloadRoot), workload);
  await writes;
  await json("complete.json", {
    completed: new Date().toISOString(),
    revisions,
  });
  await json("active.json", { complete: true });
} catch (error) {
  await writes;
  await json("failure.json", {
    failed: new Date().toISOString(),
    error: error instanceof Error ? error.stack : String(error),
    revisions,
  });
  throw error;
}
