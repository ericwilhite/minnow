/** Production executor/result path versus the historical adapter over the same memory-backed
 * PageCache and real in-process MessagePort transfers. This isolates worker export CPU without
 * OPFS/renderer cost; it does not justify replacing the production path with the adapter.
 * Five interleaved fresh-process rounds, one excluded warmup and GC per child.
 * npx vite-node scripts/executor-export-bench.mts (scratch/TMPDIR must be under ~/.cache).
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { homedir, loadavg } from "node:os";
import { join } from "node:path";
import { serialize } from "node:v8";
import { executorSnapshot } from "./browser-perf/executor-comparison.js";
import { build } from "esbuild";
import { MemoryExtentSpace } from "../packages/core/src/next/page/memory-extents.js";
import { GroupWriter } from "../packages/core/src/next/page/writer.js";
import { wideSpec } from "./browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.js";

const reference = process.argv.find((a) => a.startsWith("--compare="))?.slice("--compare=".length);
const variants = reference ? ["baseline", "candidate"] : ["adapter", "executor"];
const scratch = mkdtempSync(join(homedir(), ".cache", "minnow-executor-export-"));
try {
  const count = 8192,
    space = new MemoryExtentSpace(),
    spec = wideSpec(200),
    types = spec.map(typeOf);
  const group = new GroupWriter({
    columns: types.map((type, k) => ({ columnId: k + 1, type })),
    writer: space.beginWrite("fixture"),
  });
  await group.append(
    spec.map((s, k) => vectorOf(s, types[k] ?? { kind: "int64" }, 0, count, 1048576)),
    count,
  );
  const made = await group.finish();
  writeFileSync(join(scratch, "data"), space.bytes.subarray(0, space.allocator.end));
  writeFileSync(join(scratch, "group"), serialize({ rows: count, chunks: made.columns }));
  const worker = `
import {readFileSync} from "node:fs";
import {deserialize} from "node:v8";
import {PageCache} from "./packages/core/src/next/storage/page-cache.ts";
import {Governor} from "./packages/core/src/next/memory/governor.ts";
import {prepareExecutorExport} from "./scripts/browser-perf/page/executor-export.ts";
import {scanExport} from "./scripts/browser-perf/page/scan-export.ts";
import {rebaseExtents} from "./scripts/browser-perf/page/export-stream.ts";
const dir=process.argv[2],variant=process.argv[3],g=deserialize(readFileSync(dir+"/group")),data=readFileSync(dir+"/data");
const moved=new Map(g.chunks.map(c=>[c.data.extent.offset,{...c.data.extent,offset:c.data.extent.offset+data.length}]));
const groups=[g,{rows:g.rows,chunks:rebaseExtents(g.chunks,moved)}];
async function run(){
 const governor=new Governor(512*1048576),cache=new PageCache({readDataInto(at,out){let done=0;while(done<out.length){const from=(at+done)%data.length,n=Math.min(out.length-done,data.length-from);out.set(data.subarray(from,from+n),done);done+=n;}return Promise.resolve();}});
 cache.attach(governor.registerCache(cache));
 const pages={read:cache.read.bind(cache),peek:cache.peek.bind(cache),parsed:cache.parsed.bind(cache),attach:cache.attachParsed.bind(cache)};
 const ports=new MessageChannel();let received=0;
 ports.port2.onmessage=e=>{for(const frame of e.data.frames){received+=frame.rowCount;ports.port2.postMessage({t:"release",target:1,through:frame.sequence});}};ports.port2.start();
 let query=null,result=null,execute;
 if(variant!=="adapter")execute=prepareExecutorExport(pages,groups,{governor,port:ports.port1});
 else{query=await governor.admit({label:"adapter query",minimumBytes:32*1048576,spillable:true});result=await governor.admit({label:"adapter result",minimumBytes:4*1048576,spillable:true});execute=()=>scanExport(pages,groups,{budget:query,resultBudget:result,port:ports.port1,readWindowBytes:0});}
 const before=process.cpuUsage(),threadBefore=process.threadCpuUsage(),start=performance.now();
 let output;try{output=await execute();}finally{ports.port2.close();query?.close();result?.close();}
 const wallMs=performance.now()-start,cpu=process.cpuUsage(before),thread=process.threadCpuUsage(threadBefore);
 const reads=cache.stats.platformReads;cache.close();
 if(received!==2*g.rows||governor.usedBytes!==0)throw new Error("Export rows/charge mismatch");
 const stats=output.executorStats??output.scanStats;
 return{variant,rows:received,cpuMs:(cpu.user+cpu.system)/1000,threadCpuMs:(thread.user+thread.system)/1000,wallMs,reads,frames:stats.frames,wireBytes:stats.wireBytes};
}
await run();if(typeof global.gc!=="function")throw new Error("expose GC");global.gc();
console.log(JSON.stringify(await run()));
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
