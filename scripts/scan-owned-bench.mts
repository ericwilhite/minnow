/** Terminal decode + BENCH frame assembly: borrowed/copy versus dedicated/adopt.
 * Four wide groups, fresh PageCache per pass, memory-backed IO. Setup/warmup excluded.
 * Five interleaved fresh processes; CPU, wall, platform calls and memory charges recorded.
 * Uses the real Governor and separate query/result grants (same 512 MiB cap as the browser).
 * Includes real in-process structuredClone transfers and four held frames; excludes MessagePort IPC.
 * --profile is one diagnostic pass per variant (never a performance comparison).
 * npx vite-node scripts/scan-owned-bench.mts
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { serialize } from "node:v8";
import { build } from "esbuild";
import { MemoryExtentSpace } from "../packages/core/src/next/page/memory-extents.js";
import { GroupWriter } from "../packages/core/src/next/page/writer.js";
import { wideSpec } from "./browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.js";

const profiling = process.argv.includes("--profile");
const rounds = profiling ? 1 : 5;
const scratch = mkdtempSync(join(tmpdir(), "minnow-scan-owned-"));
try {
  const space = new MemoryExtentSpace(),
    spec = wideSpec(200),
    types = spec.map(typeOf),
    writer = space.beginWrite("fixture");
  const group = new GroupWriter({
    columns: types.map((type, k) => ({ columnId: k + 1, type })),
    writer,
  });
  await group.append(
    spec.map((s, k) => vectorOf(s, types[k] ?? { kind: "int64" }, 0, 65536, 1048576)),
    65536,
  );
  const made = await group.finish();
  writeFileSync(join(scratch, "data"), space.bytes.subarray(0, space.allocator.end));
  writeFileSync(join(scratch, "group"), serialize({ rows: 65536, chunks: made.columns }));
  const worker = `
import {readFileSync,writeFileSync} from "node:fs";
import {Session} from "node:inspector";
import {deserialize} from "node:v8";
import {PageCache} from "./packages/core/src/next/storage/page-cache.ts";
import {TableScan} from "./packages/core/src/next/exec/table-scan.ts";
import {chunkReads} from "./packages/core/src/next/exec/scan.ts";
import {OutputArena} from "./packages/core/src/next/exec/kernels/vectors.ts";
import {Governor} from "./packages/core/src/next/memory/governor.ts";
import {BenchFrameAssembler} from "./scripts/browser-perf/page/owner-frames.ts";
import {transferFor} from "./packages/core/src/next/owner/frame.ts";
import {ResultChannel} from "./packages/core/src/next/exec/result.ts";
import {rebaseExtents} from "./scripts/browser-perf/page/export-stream.ts";
const dir=process.argv[2],variant=process.argv[3],g=deserialize(readFileSync(dir+"/group")),data=readFileSync(dir+"/data");
const columns=g.chunks.map(c=>({id:c.columnId,name:"c"+c.columnId,type:c.type,nullable:true,missing:null}));
const segments=Array.from({length:4},(_,index)=>{const chunks=rebaseExtents(g.chunks,new Map(g.chunks.map(c=>[c.data.extent.offset,{...c.data.extent,offset:c.data.extent.offset+index*data.length}])));return{kind:"group",index,rowCount:g.rows,liveRowCount:g.rows,staged:false,root:()=>({rowCount:g.rows,rowIds:{kind:"dense"},columns:chunks}),chunk:id=>chunks.find(c=>c.columnId===id)??null,deletions:()=>null,statistics:()=>null};});
async function context(){let charged=0,peak=0,reads=0;const governor=new Governor(512*1048576),cache=new PageCache({readDataInto(at,out){reads++;let done=0;while(done<out.length){const from=(at+done)%data.length,n=Math.min(out.length-done,data.length-from);out.set(data.subarray(from,from+n),done);done+=n;}return Promise.resolve();}}),handle=governor.registerCache(cache);
cache.attach({charge(n){if(!handle.charge(n))return false;charged+=n;peak=Math.max(peak,charged);return true;},release(n){charged-=n;handle.release(n);}});
const queryBudget=await governor.admit({label:"scan",minimumBytes:32*1048576,spillable:true}),resultBudget=await governor.admit({label:"result",minimumBytes:4*1048576,spillable:true});
return{cache,queryBudget,resultBudget,pages:{read:cache.read.bind(cache),peek:cache.peek.bind(cache),parsed:cache.parsed.bind(cache),attach:cache.attachParsed.bind(cache)},summary:()=>({peak,governorPeak:governor.peakBytes,reads,pool:cache.stats.poolBytes,resident:cache.stats.residentBytes}),close(){cache.close();handle.unregister();queryBudget.close();resultBudget.close();if(charged!==0||governor.usedBytes!==0)throw new Error("Cache/grant charge leak");}};}

async function run(ctx){const resultBudget=ctx.resultBudget,budget=ctx.queryBudget,arena=new OutputArena(budget,2048),scan=new TableScan({table:{tableId:1,schemaVersion:1,columns,segments,liveRowCount:4*g.rows},pages:ctx.pages,reads:chunkReads,readWindowBytes:64*1048576,outputBudget:resultBudget,transferOutputs:variant==="dictionary"?"dictionary":variant==="flat",budget,columns:columns.map(c=>c.id),filter:null,emitLocator:false,context:{arena,params:{slots:[]},input:[]}});const assembler=new BenchFrameAssembler(resultBudget),channel=new ResultChannel([]);let rows=0,sum=0,batches=0,wireBytes=0;const receiver=[];try{for(let item=await scan.next();item!==null;item=await scan.next()){const b=item.batch,d=b.columns[0].data;if(d.layout!=="int64"||d.values.repr!=="safe"||b.selection!==null)throw new Error("Bad key");for(let k=0;k<b.length;k++)sum+=d.values.values[k];rows+=b.length;batches++;channel.offer(assembler.offer(b,item.transfer));const frame=channel.poll();if(!frame)throw new Error("Missing frame");wireBytes+=frame.byteLength;const received=structuredClone(frame.payload,{transfer:transferFor(frame.transfer)});receiver.push({received,release:frame.release});if(receiver.length>=4)receiver.shift().release();}}finally{for(const held of receiver)held.release();receiver.length=0;scan.close();arena.close();assembler.close();if(resultBudget.allocated!==0)throw new Error("Result charge leak");}if(rows!==4*g.rows||budget.allocated!==0)throw new Error("Rows/lease balance");return{rows,sum,batches,wireBytes,...assembler.copyStats,...ctx.summary()};}
const warm=await context();await run(warm);warm.close();
if(typeof global.gc!=="function")throw new Error("Expose GC for untimed warmup cleanup");global.gc();
const profiler=new Session();const post=(method,args={})=>new Promise((resolve,reject)=>profiler.post(method,args,(e,v)=>e?reject(e):resolve(v)));
if(process.env.MINNOW_COMPONENT_PROFILE){profiler.connect();await post("Profiler.enable");await post("Profiler.setSamplingInterval",{interval:500});await post("Profiler.start");}
const ctx=await context(),before=process.cpuUsage(),threadBefore=typeof process.threadCpuUsage==="function"?process.threadCpuUsage():null,start=performance.now(),result=await run(ctx),wallMs=performance.now()-start,cpu=process.cpuUsage(before),thread=threadBefore===null?null:process.threadCpuUsage(threadBefore);ctx.close();
if(process.env.MINNOW_COMPONENT_PROFILE){const result=await post("Profiler.stop");writeFileSync(process.env.MINNOW_COMPONENT_PROFILE+"-"+variant+".cpuprofile",JSON.stringify(result.profile));profiler.disconnect();}
console.log(JSON.stringify({variant,cpuMs:(cpu.user+cpu.system)/1000,...(thread===null?{}:{threadCpuMs:(thread.user+thread.system)/1000}),wallMs,...result}));
`;
  await build({
    stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: join(scratch, "worker.mjs"),
  });
  const rows: Array<{
    variant: string;
    round: number;
    cpuMs: number;
    threadCpuMs?: number;
    wallMs: number;
    sum: number;
    reads: number;
  }> = [];
  for (let round = 0; round < rounds; round++)
    for (const variant of round % 2 === 0
      ? ["borrowed", "dictionary", "flat"]
      : ["flat", "dictionary", "borrowed"]) {
      const row = {
        round,
        ...(JSON.parse(
          execFileSync(
            process.execPath,
            ["--expose-gc", join(scratch, "worker.mjs"), scratch, variant],
            {
              encoding: "utf8",
              env: {
                ...process.env,
                MINNOW_COMPONENT_PROFILE: profiling
                  ? (process.env.MINNOW_COMPONENT_PROFILE ?? join(scratch, "profile"))
                  : "",
              },
              timeout: 120000,
            },
          ),
        ) as {
          variant: string;
          cpuMs: number;
          threadCpuMs?: number;
          wallMs: number;
          sum: number;
          reads: number;
        }),
      };
      rows.push(row);
      console.log(JSON.stringify(row));
    }
  if (new Set(rows.map((r) => r.sum)).size !== 1) throw new Error("Key checksum changed");
  console.log(
    JSON.stringify({
      rounds,
      diagnosticProfile: profiling,
      summary: ["borrowed", "dictionary", "flat"].map((variant) => {
        const stats = (k: "cpuMs" | "threadCpuMs" | "wallMs" | "reads") => {
          const xs = rows
            .filter((r) => r.variant === variant)
            .map((r) => r[k])
            .filter((value): value is number => typeof value === "number")
            .sort((a, b) => a - b);
          return { median: xs[Math.floor(xs.length / 2)], min: xs[0], max: xs.at(-1) };
        };
        return {
          variant,
          cpuMs: stats("cpuMs"),
          threadCpuMs: stats("threadCpuMs"),
          wallMs: stats("wallMs"),
          reads: stats("reads"),
        };
      }),
    }),
  );
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
