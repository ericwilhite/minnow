/** Loaded-page vector materialization, 200-column wide fixture, 512-row batches, 32 passes.
 * Includes dedicated output allocation, shape selection, sizing and decode. Excludes writing,
 * page loading/parsing, framing and transport. One warmup + GC, then five interleaved fresh-process
 * rounds per revision. Scratch is under ~/.cache; invoke vite-node with TMPDIR there too.
 * npx vite-node scripts/vector-materialize-bench.mts --compare=<revision>
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { loadavg, tmpdir } from "node:os";
import { join } from "node:path";
import { serialize } from "node:v8";
import type { PhysicalType } from "../packages/core/src/next/types.js";
import type { FlatVector } from "../packages/core/src/next/vector/vector.js";
import { executorSnapshot } from "./browser-perf/executor-comparison.js";
import { build } from "esbuild";
import { MemoryExtentSpace } from "../packages/core/src/next/page/memory-extents.js";
import { GroupWriter } from "../packages/core/src/next/page/writer.js";
import { wideSpec } from "./browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.js";

const reference = process.argv.find((a) => a.startsWith("--compare="))?.slice("--compare=".length);
if (!reference) throw new Error("Use --compare=<revision>");
const variants = ["baseline", "candidate"];
const reuseTarget = process.argv.includes("--reused");
const batchComparison = process.argv.includes("--batch");
const offsetsFixture = process.argv.includes("--offsets");
const scratch = mkdtempSync(join(tmpdir(), "minnow-materialize-"));
try {
  const count = 8192,
    space = new MemoryExtentSpace(),
    spec = wideSpec(200),
    types: PhysicalType[] = offsetsFixture ? spec.map(() => ({ kind: "bytes" })) : spec.map(typeOf);
  const group = new GroupWriter({
    columns: types.map((type, k) => ({ columnId: k + 1, type })),
    writer: space.beginWrite("fixture"),
    ...(offsetsFixture ? { dictionaryEntries: 0 } : {}),
  });
  await group.append(
    spec.map((s, k): FlatVector =>
      offsetsFixture
        ? {
            form: "flat",
            type: { kind: "bytes" },
            length: count,
            validity: null,
            data: {
              layout: "varlen",
              offsets: Int32Array.from({ length: count + 1 }, (_, i) => i),
              bytes: Uint8Array.from({ length: count }, (_, i) => (i * 31 + k) % 251),
            },
          }
        : vectorOf(s, types[k] ?? { kind: "int64" }, 0, count, 1048576),
    ),
    count,
  );
  const made = await group.finish();
  writeFileSync(join(scratch, "data"), space.bytes.subarray(0, space.allocator.end));
  writeFileSync(join(scratch, "group"), serialize({ rows: count, chunks: made.columns }));
  const worker = `
import {readFileSync} from "node:fs";
import {deserialize} from "node:v8";
import {MemoryExtentSpace,UNLIMITED} from "./packages/core/src/next/page/memory-extents.ts";
import {ChunkReader} from "./packages/core/src/next/page/reader.ts";
import {ColumnTarget} from "./packages/core/src/next/exec/column-target.ts";
const dir=process.argv[2], variant=process.argv[3], group=deserialize(readFileSync(dir+"/group"));
const space=new MemoryExtentSpace();space.bytes=new Uint8Array(readFileSync(dir+"/data"));
const readers=[];for(const chunk of group.chunks){const reader=await ChunkReader.open(space,chunk,{budget:UNLIMITED});await reader.load(Array.from({length:reader.directory.pages.length},(_,i)=>i));readers.push(reader);}
let charged=0, decodedBytes=0, vectors=0;const memory={take(n){charged+=n;},give(n){charged-=n;}};
const reuseTarget=process.argv[4]==="reused";
const batch=process.argv[5]==="batch"&&variant==="candidate"?1024:512;
const targets=group.chunks.map(c=>new ColumnTarget(c.type,batch,memory));
const plainPages=readers.reduce((n,r)=>n+r.directory.pages.filter(p=>(p.encoding&127)===1).length,0);
const dictionary=readers.map((r,c)=>c<23&&r.isDictionaryRange(0,group.rows));
function run(){
 let checksum=0; decodedBytes=0;vectors=0;
 const before=process.cpuUsage(),threadBefore=process.threadCpuUsage(),start=performance.now();
 for(let repeat=0;repeat<32;repeat++)for(let from=0;from<group.rows;from+=batch)for(let c=0;c<targets.length;c++){
  if(reuseTarget){
    const output=targets[c].decode(readers[c],{from,to:Math.min(group.rows,from+batch)});
    if(output.form!=="flat"||output.data.layout!=="varlen")throw new Error("Reused requires plain varlen");
    checksum+=output.data.bytes[0]??0;vectors++;
  } else {
    const output=targets[c].decodeOwned(readers[c],{from,to:Math.min(group.rows,from+batch)},dictionary[c]);
    decodedBytes+=output.bytes;vectors++;for(const buffer of output.buffers)checksum+=new Uint8Array(buffer)[0]??0;
    memory.give(output.bytes);
  }
 }
 const wallMs=performance.now()-start,cpu=process.cpuUsage(before),thread=process.threadCpuUsage(threadBefore);
 if(charged!==targets.reduce((n,t)=>n+t.chargedBytes,0))throw new Error("Materialization charge leak");
 return {variant,batch,plainPages,decodedBytes,vectors,checksum,cpuMs:(cpu.user+cpu.system)/1000,threadCpuMs:(thread.user+thread.system)/1000,wallMs};
}
run();if(typeof global.gc!=="function")throw new Error("expose GC");global.gc();console.log(JSON.stringify(run()));
for(const target of targets)target.close();
for(const reader of readers)reader.close();
if(charged!==0)throw new Error("Closing charge leak");
`;
  await build({
    stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: join(scratch, "worker.mjs"),
  });
  if (reference)
    await build({
      stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: join(scratch, "worker-baseline.mjs"),
      plugins: [executorSnapshot(reference).esbuild],
    });
  const results: Array<{
    round: number;
    variant: string;
    cpuMs: number;
    threadCpuMs: number;
    wallMs: number;
  }> = [];
  for (let round = 0; round < 5; round++)
    for (const variant of round % 2 === 0 ? variants : [...variants].reverse()) {
      const row = {
        round,
        mode: reuseTarget ? "reused targets (copy isolation)" : "owned allocation and decode",
        fixture: offsetsFixture
          ? "plain one-byte varlen, isolates offset materialization"
          : "mixed wide",
        hostLoad: loadavg(),
        ...(JSON.parse(
          execFileSync(
            process.execPath,
            [
              "--expose-gc",
              join(
                scratch,
                reference && variant === "baseline" ? "worker-baseline.mjs" : "worker.mjs",
              ),
              scratch,
              variant,
              reuseTarget ? "reused" : "owned",
              batchComparison ? "batch" : "fixed",
            ],
            { encoding: "utf8", timeout: 120000 },
          ),
        ) as { variant: string; cpuMs: number; threadCpuMs: number; wallMs: number }),
      };
      results.push(row);
      console.log(JSON.stringify(row));
    }
  for (const variant of variants) {
    const rows = results.filter((r) => r.variant === variant);
    const stats = (key: "cpuMs" | "threadCpuMs" | "wallMs") => {
      const sorted = rows.map((r) => r[key]).sort((a, b) => a - b);
      return { median: sorted[2], min: sorted[0], max: sorted[4] };
    };
    console.log(
      JSON.stringify({
        variant,
        cpuMs: stats("cpuMs"),
        threadCpuMs: stats("threadCpuMs"),
        wallMs: stats("wallMs"),
      }),
    );
  }
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
