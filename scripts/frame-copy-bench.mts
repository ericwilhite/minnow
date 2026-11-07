/** Full bench assembler, same borrowed 200-column scan layouts; isolate offset-copy policy.
 * Five interleaved fresh-process rounds, process CPU + wall, setup/warmup outside timing.
 * The baseline is frozen from 1a32d1f0; the candidate only changes varlen offset ownership.
 * npx vite-node scripts/frame-copy-bench.mts
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { serialize } from "node:v8";
import { build } from "esbuild";
import { MemoryExtentSpace } from "../packages/core/src/next/page/memory-extents.js";
import { GroupWriter } from "../packages/core/src/next/page/writer.js";
import { ChunkReader } from "../packages/core/src/next/page/reader.js";
import { ColumnTarget } from "../packages/core/src/next/exec/column-target.js";
import { countingBudget } from "../packages/core/src/next/storage/space-test-support.js";
import { wideSpec } from "./browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.js";

const candidate = readFileSync("scripts/browser-perf/page/owner-frames.ts", "utf8");
const anchor = "offsets: ownedOffsets(d.offsets, n, first),";
if (!candidate.includes(anchor)) throw new Error("Offset-copy candidate anchor moved");
// Reconstruct the frozen parent formula without requiring an unmerged Git object in a fresh clone.
const baseline = candidate.replace(
  anchor,
  "offsets: Int32Array.from(d.offsets.subarray(0, n + 1), (x) => x - first),",
);
const scratch = mkdtempSync(join(homedir(), ".cache/minnow-frame-copy-"));
try {
  const space = new MemoryExtentSpace(),
    spec = wideSpec(200),
    types = spec.map(typeOf);
  const writer = space.beginWrite("fixture"),
    group = new GroupWriter({
      columns: types.map((type, k) => ({ columnId: k + 1, type })),
      writer,
    });
  await group.append(
    spec.map((s, k) => vectorOf(s, types[k] ?? { kind: "int64" }, 0, 65536, 1048576)),
    65536,
  );
  const made = await group.finish(),
    budget = countingBudget(Infinity);
  const columns = [];
  for (let k = 0; k < made.columns.length; k++) {
    const chunk = made.columns[k];
    if (!chunk) throw new Error("Missing chunk");
    const reader = await ChunkReader.open(space, chunk, { budget });
    const [lo, hi] = reader.pagesInRange(0, 768);
    await reader.load(Array.from({ length: hi - lo }, (_, i) => lo + i));
    await reader.loadValues(0, 768);
    const target = new ColumnTarget(types[k] ?? { kind: "int64" }, 2048, {
      take(n) {
        if (!budget.allocate(n)) throw new Error("Budget");
      },
      give: (n) => budget.free(n),
    });
    columns.push(structuredClone(target.decode(reader, { from: 0, to: 768 })));
    target.close();
    reader.close();
  }
  if (budget.used !== 0) throw new Error("Fixture lease leak");
  writeFileSync(join(scratch, "batch"), serialize({ length: 768, columns, selection: null }));
  const worker = `
import {readFileSync} from "node:fs";
import {deserialize} from "node:v8";
import {createHash} from "node:crypto";
import {BenchFrameAssembler} from "./scripts/browser-perf/page/owner-frames.ts";
import {countingBudget} from "./packages/core/src/next/storage/space-test-support.ts";
import {frameTransferList} from "./packages/core/src/next/exec/result.ts";
const batch=deserialize(readFileSync(process.argv[2]+"/batch")),budget=countingBudget(Infinity);
const assembler=new BenchFrameAssembler(budget);
let bytes=0;
function one(){const f=assembler.offer(batch);const b=frameTransferList(f.payload);let n=0;for(const x of b)n+=x.byteLength;f.free();return n;}
// Settle the stream dictionary policy before either timing or comparing output.
one();const f=assembler.offer(batch),hash=createHash("sha256");
hash.update(JSON.stringify(f.payload,(_k,v)=>ArrayBuffer.isView(v)?{type:v.constructor.name,length:v.length}:typeof v==="bigint"?String(v):v));
for(const b of frameTransferList(f.payload))hash.update(new Uint8Array(b));
const digest=hash.digest("hex");f.free();
for(let k=0;k<300;k++)one();
const before=process.cpuUsage(),start=performance.now(),iterations=2000;
for(let k=0;k<iterations;k++)bytes+=one();
const wallMs=performance.now()-start,cpu=process.cpuUsage(before);
assembler.close();if(budget.used!==0)throw new Error("Leaked result charge");
console.log(JSON.stringify({cpuMs:(cpu.user+cpu.system)/1000,wallMs,iterations,bytes,digest}));
`;
  for (const [name, source] of [
    ["baseline", baseline],
    ["candidate", candidate],
  ] as const)
    await build({
      stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: join(scratch, name + ".mjs"),
      plugins: [
        {
          name: "frame-copy-variant",
          setup(b) {
            b.onLoad({ filter: /\/owner-frames\.ts$/ }, () => ({ contents: source, loader: "ts" }));
          },
        },
      ],
    });
  const rows: Array<{
    variant: string;
    round: number;
    cpuMs: number;
    wallMs: number;
    digest: string;
  }> = [];
  for (let round = 0; round < 5; round++)
    for (const variant of round % 2 === 0 ? ["baseline", "candidate"] : ["candidate", "baseline"]) {
      const row = {
        round,
        variant,
        ...(JSON.parse(
          execFileSync(process.execPath, [join(scratch, variant + ".mjs"), scratch], {
            encoding: "utf8",
            timeout: 120000,
          }),
        ) as { cpuMs: number; wallMs: number; digest: string }),
      };
      rows.push(row);
      console.log(JSON.stringify(row));
    }
  if (new Set(rows.map((r) => r.digest)).size !== 1) throw new Error("Frame bytes changed");
  const summary = ["baseline", "candidate"].map((variant) => {
    const stats = (key: "cpuMs" | "wallMs") => {
      const xs = rows
        .filter((r) => r.variant === variant)
        .map((r) => r[key])
        .sort((a, b) => a - b);
      return { median: xs[2], min: xs[0], max: xs[4] };
    };
    return { variant, cpuMs: stats("cpuMs"), wallMs: stats("wallMs") };
  });
  console.log(
    JSON.stringify({
      rounds: 5,
      summary,
      baseline: "1a32d1f0",
      candidate: "typed slice, bias loop only for nonzero offset",
    }),
  );
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
