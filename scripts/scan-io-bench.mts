/** Real TableScan: cached per-batch reads versus one-pass coalesced windows.
 * Four wide groups, fresh PageCache per pass, memory-backed IO. Setup/warmup excluded.
 * Five interleaved fresh processes; CPU, wall, platform calls and memory charges recorded.
 * npx vite-node scripts/scan-io-bench.mts
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

const scratch = mkdtempSync(join(tmpdir(), "minnow-scan-io-"));
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
import {readFileSync} from "node:fs";
import {deserialize} from "node:v8";
import {PageCache} from "./packages/core/src/next/storage/page-cache.ts";
import {TableScan} from "./packages/core/src/next/exec/table-scan.ts";
import {chunkReads} from "./packages/core/src/next/exec/scan.ts";
import {OutputArena} from "./packages/core/src/next/exec/kernels/vectors.ts";
import {countingBudget} from "./packages/core/src/next/storage/space-test-support.ts";
import {rebaseExtents} from "./scripts/browser-perf/page/export-stream.ts";
const dir=process.argv[2],variant=process.argv[3],g=deserialize(readFileSync(dir+"/group")),data=readFileSync(dir+"/data");
const columns=g.chunks.map(c=>({id:c.columnId,name:"c"+c.columnId,type:c.type,nullable:true,missing:null}));
const segments=Array.from({length:4},(_,index)=>{const chunks=rebaseExtents(g.chunks,new Map(g.chunks.map(c=>[c.data.extent.offset,{...c.data.extent,offset:c.data.extent.offset+index*data.length}])));return{kind:"group",index,rowCount:g.rows,liveRowCount:g.rows,staged:false,root:()=>({rowCount:g.rows,rowIds:{kind:"dense"},columns:chunks}),chunk:id=>chunks.find(c=>c.columnId===id)??null,deletions:()=>null,statistics:()=>null};});
function context(){let charged=0,peak=0,reads=0;const cache=new PageCache({readDataInto(at,out){reads++;let done=0;while(done<out.length){const from=(at+done)%data.length,n=Math.min(out.length-done,data.length-from);out.set(data.subarray(from,from+n),done);done+=n;}return Promise.resolve();}});cache.attach({charge(n){charged+=n;peak=Math.max(peak,charged);return true;},release(n){charged-=n;}});return{cache,pages:{read:cache.read.bind(cache),peek:cache.peek.bind(cache),parsed:cache.parsed.bind(cache),attach:cache.attachParsed.bind(cache)},summary:()=>({peak,reads,pool:cache.stats.poolBytes,resident:cache.stats.residentBytes}),close(){cache.close();if(charged!==0)throw new Error("Cache charge leak");}};}
async function run(ctx){const budget=countingBudget(Infinity),arena=new OutputArena(budget,2048),scan=new TableScan({table:{tableId:1,schemaVersion:1,columns,segments,liveRowCount:4*g.rows},pages:ctx.pages,reads:chunkReads,readWindowBytes:variant==="window"?64*1048576:0,budget,columns:columns.map(c=>c.id),filter:null,emitLocator:false,context:{arena,params:{slots:[]},input:[]}});let rows=0,sum=0,batches=0;try{for(let item=await scan.next();item!==null;item=await scan.next()){const b=item.batch,d=b.columns[0].data;if(d.layout!=="int64"||d.values.repr!=="safe"||b.selection!==null)throw new Error("Bad key");sum+=d.values.values[0]+d.values.values[b.length-1];rows+=b.length;batches++;}}finally{scan.close();arena.close();}if(rows!==4*g.rows||budget.used!==0)throw new Error("Rows/lease balance");return{rows,sum,batches,...ctx.summary()};}
const warm=context();await run(warm);warm.close();
if(typeof global.gc!=="function")throw new Error("Expose GC for untimed warmup cleanup");global.gc();
const ctx=context(),before=process.cpuUsage(),threadBefore=typeof process.threadCpuUsage==="function"?process.threadCpuUsage():null,start=performance.now(),result=await run(ctx),wallMs=performance.now()-start,cpu=process.cpuUsage(before),thread=threadBefore===null?null:process.threadCpuUsage(threadBefore);ctx.close();
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
  for (let round = 0; round < 5; round++)
    for (const variant of round % 2 === 0 ? ["batch", "window"] : ["window", "batch"]) {
      const row = {
        round,
        ...(JSON.parse(
          execFileSync(
            process.execPath,
            ["--expose-gc", join(scratch, "worker.mjs"), scratch, variant],
            {
              encoding: "utf8",
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
      rounds: 5,
      summary: ["batch", "window"].map((variant) => {
        const stats = (k: "cpuMs" | "threadCpuMs" | "wallMs" | "reads") => {
          const xs = rows
            .filter((r) => r.variant === variant)
            .map((r) => r[k])
            .filter((value): value is number => typeof value === "number")
            .sort((a, b) => a - b);
          return { median: xs[2], min: xs[0], max: xs[4] };
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
