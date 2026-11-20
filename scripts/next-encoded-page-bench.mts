/** Real loaded page filter -> ordered float SUM + exact int32 SUM, selection first.
 * All setup, I/O, state/output allocation and verification are outside CPU brackets. Existing
 * 2048-row transition barriers remain inside 2048/8192/32768-row selection windows. No query,
 * browser, OPFS or wider production error-demand equivalence is inferred from this component.
 * Nine AB/BA pairs count all process threads and natural GC; run serially with an A/A first.
 */
import { strict as assert } from "node:assert";
import { cpuUsage } from "node:process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { loadavg, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { ByteBudget } from "../packages/core/dist/next/storage/extents.js";
import type { FlatVector } from "../packages/core/dist/next/vector/vector.js";
import type { Accumulator } from "../packages/core/dist/next/exec/agg/states.js";
import {
  defined,
  targetFor,
  valueAt,
  writeGroup,
} from "../packages/core/dist/next/page/test-support.js";
import { pairedCPU } from "./lib/paired-cpu.mts";

type ReaderModule = typeof import("../packages/core/dist/next/page/reader.js");
type StateModule = typeof import("../packages/core/dist/next/exec/agg/states.js");
type Reader = Awaited<ReturnType<ReaderModule["ChunkReader"]["open"]>>;
const EMPTY16 = new Uint16Array(0),
  EMPTY32 = new Int32Array(0);
const option = (name: string, fallback: string) => {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const repo = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  controlRoot = resolve(option("--control-root", repo)),
  candidateRoot = resolve(option("--candidate-root", repo)),
  output = resolve(option("--output", join(tmpdir(), "next-encoded-pages"))),
  candidateMode = option("--candidate-mode", "encoded"),
  rounds = Number(option("--rounds", "9")),
  repeats = Number(option("--repeats", "64")),
  warmups = Number(option("--warmups", "4")),
  capBytes = Number(option("--cap-bytes", String(4 * 1024 * 1024)));
assert(["encoded", "decoded"].includes(candidateMode));
assert(Number.isSafeInteger(capBytes) && capBytes > 0, "positive byte cap");
for (const [name, count] of Object.entries({ rounds, repeats, warmups }))
  assert(Number.isSafeInteger(count) && count >= (name === "rounds" ? 9 : 1), name);
const F64 = { kind: "float64" } as const,
  I32 = { kind: "int32", bits: 32 } as const,
  I64 = { kind: "int64" } as const,
  ROWS = 65_536,
  STEP = 2048;
class Budget implements ByteBudget {
  used = 0;
  peak = 0;
  allocate(bytes: number): boolean {
    if (this.used + bytes > capBytes) return false;
    this.used += bytes;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(bytes: number): void {
    this.used -= bytes;
    assert(this.used >= 0);
  }
}
const moduleURL = (root: string, name: string) =>
  pathToFileURL(join(root, "packages/core/dist/next", name)).href;
async function load(root: string, encoded: boolean) {
  const reader = (await import(moduleURL(root, "page/reader.js"))) as ReaderModule,
    states = (await import(moduleURL(root, "exec/agg/states.js"))) as StateModule,
    metadata = encoded
      ? ((await import(moduleURL(root, "exec/page-consumer.js"))) as {
          PAGE_CONSUMER_METADATA_BYTES: number;
        })
      : null;
  return { reader, states, encoded, metadataBytes: metadata?.PAGE_CONSUMER_METADATA_BYTES ?? 0 };
}
const apis = {
  control: await load(controlRoot, false),
  candidate: await load(candidateRoot, candidateMode === "encoded"),
};
const sourceFiles = ["page/reader.js", "page/decode.js", "page/select.js", "exec/agg/states.js"];
if (candidateMode === "encoded")
  sourceFiles.push(
    "page/consume.js",
    "exec/agg/page-count.js",
    "exec/agg/page-float.js",
    "exec/agg/page-scaled.js",
    "exec/agg/page-int32.js",
    "exec/page-consumer.js",
  );
async function hashes(root: string) {
  const result: Record<string, string> = {};
  for (const file of sourceFiles) {
    try {
      result[file] = createHash("sha256")
        .update(await readFile(fileURLToPath(moduleURL(root, file))))
        .digest("hex");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
  return result;
}
const sources = { control: await hashes(controlRoot), candidate: await hashes(candidateRoot) },
  records = [];
await mkdir(output, { recursive: true });
for (const nullable of [false, true]) {
  const filter = Array.from(
      { length: ROWS },
      (_, row) => (Math.imul(row, 0x9e37_79b1) >>> 0) % 1_000_000,
    ),
    floats = filter.map((value, row) =>
      nullable && row % 13 === 0 ? null : row % 127 === 0 ? Math.PI : ((value % 2000) - 1000) / 100,
    ),
    integers = filter.map((value, row) => (nullable && row % 17 === 0 ? null : value - 500_000)),
    fixture = await writeGroup(
      [
        { type: I32, values: filter },
        { type: F64, values: floats },
        { type: I32, values: integers },
      ],
      { gzip: false, pageBytes: 16 * 1024, rawPageBytes: 32 * 1024 },
    ),
    chunks = fixture.group.columns;
  let expectedFloat = -0,
    expectedInteger = 0n,
    expectedRows = 0;
  for (let row = 0; row < ROWS; row++)
    if ((filter[row] ?? 0) < 500_000) {
      expectedRows++;
      const f = floats[row],
        i = integers[row];
      if (f !== null && f !== undefined) expectedFloat += f;
      if (i !== null && i !== undefined) expectedInteger += BigInt(i);
    }
  for (const window of [2048, 8192, 32768]) {
    async function setup(api: Awaited<ReturnType<typeof load>>) {
      const budget = new Budget(),
        readers: Reader[] = [];
      // The component's state/descriptor objects and result-verification views share a
      // conservative fixed metadata envelope; payload arrays are additionally charged.
      const fixedBytes = window * 2 + STEP * 6 + api.metadataBytes + repeats * 4096 + 8 * 64;
      assert(budget.allocate(fixedBytes));
      const positions = new Uint16Array(window),
        gids = new Int32Array(STEP),
        identity = Uint16Array.from({ length: STEP }, (_, row) => row),
        passes: Array<{ float: Accumulator; integer: Accumulator; rows: number }> = [];
      let targetBytes = 0;
      try {
        for (const chunk of chunks) {
          const reader = await api.reader.ChunkReader.open(fixture.space, chunk, { budget });
          readers.push(reader);
          const [first, stop] = reader.pagesInRange(0, ROWS);
          await reader.load(Array.from({ length: stop - first }, (_, k) => first + k));
        }
        const targets: FlatVector[] = [];
        if (!api.encoded)
          for (let c = 1; c < readers.length; c++) {
            // f64/i32 plus worst-case validity. Admission precedes caller-owned output buffers.
            targetBytes += STEP * (c === 1 ? 8 : 4) + Math.ceil(STEP / 32) * 4;
          }
        assert(budget.allocate(targetBytes));
        if (!api.encoded)
          for (let c = 1; c < readers.length; c++)
            targets.push(targetFor(defined(readers[c]), STEP, 0));
        for (let pass = 0; pass < repeats; pass++) {
          const float = api.states.accumulatorFor("sum", F64, F64, budget),
            integer = api.states.accumulatorFor("sum", I64, I32, budget);
          passes.push({ float, integer, rows: 0 });
          float.grow(1);
          integer.grow(1);
        }
        return {
          budget,
          readers,
          passes,
          positions,
          gids,
          identity,
          targets,
          fixedBytes,
          targetBytes,
          compiled: defined(
            defined(readers[0]).compilePredicate({ op: "<", value: 500_000 }) ?? undefined,
          ),
        };
      } catch (error) {
        for (const reader of readers) reader.close();
        for (const pass of passes) {
          pass.float.close();
          pass.integer.close();
        }
        throw error;
      }
    }
    type Run = Awaited<ReturnType<typeof setup>>;
    function run(state: Run, api: Awaited<ReturnType<typeof load>>) {
      for (const pass of state.passes)
        for (let from = 0; from < ROWS; from += window) {
          const count = defined(state.readers[0]).select(
            state.compiled,
            from,
            Math.min(ROWS, from + window),
            state.positions,
          );
          pass.rows += count;
          for (let start = 0; start < count; start += STEP) {
            const end = Math.min(count, start + STEP);
            for (let column = 1; column < 3; column++) {
              const reader = defined(state.readers[column]),
                accumulator = column === 1 ? pass.float : pass.integer;
              if (api.encoded) {
                const [first, stop] = reader.pagesInRange(
                  state.positions[start] ?? 0,
                  (state.positions[end - 1] ?? 0) + 1,
                );
                let k = start;
                for (let page = first; page < stop && k < end; page++) {
                  const entry = defined(reader.directory.pages[page]),
                    limit = entry.firstRow + entry.rowCount,
                    begin = k;
                  while (k < end && (state.positions[k] ?? 0) < limit) k++;
                  if (begin === k) continue;
                  const kernel = accumulator.pageKernel?.(column === 1 ? F64 : I32, entry);
                  assert(kernel, "fixture must use a supported encoded kernel");
                  if (accumulator.pageCheck)
                    assert(
                      reader.checkPage(page, accumulator.pageCheck),
                      "fixture must pass payload eligibility",
                    );
                  reader.consumePage(page, state.positions, begin, k, accumulator, kernel);
                }
              } else {
                const target = defined(state.targets[column - 1]);
                reader.gather(state.positions, target, 0, start, end);
                accumulator.update(
                  { ...target, length: end - start },
                  state.gids,
                  state.identity,
                  end - start,
                  true,
                );
              }
            }
          }
        }
    }
    function verifyAndClose(state: Run) {
      try {
        for (const pass of state.passes) {
          const bytes = pass.float.outputBytes(0, 1) + pass.integer.outputBytes(0, 1);
          assert(state.budget.allocate(bytes));
          try {
            assert.equal(valueAt(pass.float.emit(0, 1), 0), expectedFloat);
            assert.equal(valueAt(pass.integer.emit(0, 1), 0), expectedInteger);
            assert.equal(pass.rows, expectedRows);
          } finally {
            state.budget.free(bytes);
          }
        }
      } finally {
        for (const reader of state.readers) reader.close();
        for (const pass of state.passes) {
          pass.float.close();
          pass.integer.close();
        }
        state.readers.length = 0;
        state.passes.length = 0;
        state.targets.length = 0;
        state.positions = EMPTY16;
        state.identity = EMPTY16;
        state.gids = EMPTY32;
        state.budget.free(state.fixedBytes + state.targetBytes);
      }
      assert.equal(state.budget.used, 0, "component retained credit");
    }
    for (let warmup = 0; warmup < warmups; warmup++)
      for (const api of Object.values(apis)) {
        const state = await setup(api);
        run(state, api);
        verifyAndClose(state);
      }
    const samples: Array<{
      round: number;
      arm: "control" | "candidate";
      cpuMs: number;
      peakBytes: number;
      load: number[];
    }> = [];
    for (let round = 0; round < rounds; round++)
      for (const arm of round % 2 === 0
        ? (["control", "candidate"] as const)
        : (["candidate", "control"] as const)) {
        const api = apis[arm],
          state = await setup(api),
          start = cpuUsage();
        run(state, api);
        const used = cpuUsage(start);
        verifyAndClose(state);
        samples.push({
          round,
          arm,
          cpuMs: (used.user + used.system) / 1000,
          peakBytes: state.budget.peak,
          load: loadavg(),
        });
      }
    const result = pairedCPU(
      samples.filter((x) => x.arm === "control").map((x) => x.cpuMs),
      samples.filter((x) => x.arm === "candidate").map((x) => x.cpuMs),
    );
    records.push({
      nullable,
      selectionWindowRows: window,
      transitionRows: STEP,
      inputRowsPerSample: ROWS * repeats,
      result,
      samples,
      medianSavingBeatsFullRange: -result.medianDeltaMs > result.rangeDeltaMs,
    });
    await writeFile(
      join(output, "result.json"),
      JSON.stringify(
        {
          node: process.version,
          execArgv: process.execArgv,
          nodeOptions: process.env.NODE_OPTIONS ?? null,
          uvThreadpoolSize: process.env.UV_THREADPOOL_SIZE ?? null,
          scriptSHA256: createHash("sha256")
            .update(await readFile(fileURLToPath(import.meta.url)))
            .digest("hex"),
          controlRoot,
          candidateRoot,
          candidateMode,
          rounds,
          repeats,
          warmups,
          capBytes,
          sources,
          records,
        },
        null,
        2,
      ),
    );
    console.log(JSON.stringify({ nullable, window, ...result, pairs: undefined }));
  }
}
assert.deepEqual(await hashes(controlRoot), sources.control, "control changed during comparison");
assert.deepEqual(
  await hashes(candidateRoot),
  sources.candidate,
  "candidate changed during comparison",
);
