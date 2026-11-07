/** Loaded-page vector materialization, 200-column wide fixture, 512-row batches, 32 passes.
 * Includes dedicated output allocation, shape selection, sizing and decode. Source dictionaries
 * remain codes per range. --returned times decode plus charged pool take/clear per batch; real
 * clone returns and issue/release are outside those timers (not a transport benchmark). Excludes writing,
 * page loading/parsing, framing and transport. One warmup + GC, then five interleaved fresh-process
 * rounds per revision. Scratch uses os.tmpdir(); set TMPDIR to choose its location.
 * npx vite-node scripts/vector-materialize-bench.mts --compare=<revision>
 */
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { loadavg, tmpdir } from "node:os";
import { join, resolve } from "node:path";
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
const profileDirArg = process.argv
  .find((a) => a.startsWith("--profile-dir="))
  ?.slice("--profile-dir=".length);
const profileDir = profileDirArg ? resolve(profileDirArg) : undefined;
if (profileDir) {
  mkdirSync(profileDir, { recursive: true });
}
const returned = process.argv.includes("--returned");
if (returned && profileDir)
  throw new Error(
    "Returned mode excludes handovers from timers; profile fresh materialization separately",
  );
const variants = ["baseline", "candidate"];
const reuseTarget = process.argv.includes("--reused");
const batchComparison = process.argv.includes("--batch");
const legacyDictionaries = process.argv.includes("--legacy-dictionaries");
if (returned && reuseTarget)
  throw new Error("Choose borrowed targets or dedicated returned outputs");
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
import {readFileSync,writeFileSync} from "node:fs";
import {Session} from "node:inspector";
import {deserialize} from "node:v8";
import {MemoryExtentSpace,UNLIMITED} from "./packages/core/src/next/page/memory-extents.ts";
import {ChunkReader} from "./packages/core/src/next/page/reader.ts";
import {ColumnTarget} from "./packages/core/src/next/exec/column-target.ts";
import {ReturnPool} from "./packages/core/src/next/exec/return-pool.ts";
import {Governor} from "./packages/core/src/next/memory/governor.ts";
const dir=process.argv[2], variant=process.argv[3], group=deserialize(readFileSync(dir+"/group"));
const space=new MemoryExtentSpace();space.bytes=new Uint8Array(readFileSync(dir+"/data"));
const readers=[];for(const chunk of group.chunks){const reader=await ChunkReader.open(space,chunk,{budget:UNLIMITED});await reader.load(Array.from({length:reader.directory.pages.length},(_,i)=>i));readers.push(reader);}
const returned=process.argv[8]==="returned", governor=new Governor(32*1048576), pool=returned?new ReturnPool(governor,4*1048576):null;
let charged=0, decodedBytes=0, vectors=0, dictionaryVectors=0;const memory={take(n){charged+=n;},give(n){charged-=n;},...(pool?{reuse:n=>pool.take(n)}:{})};
const reuseTarget=process.argv[4]==="reused";
const batch=process.argv[5]==="batch"&&variant==="candidate"?1024:512;
const targets=group.chunks.map(c=>new ColumnTarget(c.type,batch,memory));
const plainPages=readers.reduce((n,r)=>n+r.directory.pages.filter(p=>(p.encoding&127)===1).length,0);
const dictionary=readers.map((r,c)=>process.argv[7]!=="legacy"||(c<23&&r.isDictionaryRange(0,group.rows)));
function run(){
 let checksum=0; decodedBytes=0;vectors=0;dictionaryVectors=0;
 let cpuMs=0,threadCpuMs=0,wallMs=0,frame=0,handoverMs=0;
 const hits=pool?.hits??0,accepted=pool?.accepted??0;
 const before=process.cpuUsage(),threadBefore=process.threadCpuUsage(),start=performance.now();
 for(let repeat=0;repeat<32;repeat++)for(let from=0;from<group.rows;from+=batch){
  const sent=pool?[]:null, bp=pool?process.cpuUsage():null,bt=pool?process.threadCpuUsage():null,bs=pool?performance.now():0;
  for(let c=0;c<targets.length;c++){
   if(reuseTarget){
    const output=targets[c].decode(readers[c],{from,to:Math.min(group.rows,from+batch)});
    if(output.form!=="flat"||output.data.layout!=="varlen")throw new Error("Reused requires plain varlen");
    checksum+=output.data.bytes[0]??0;vectors++;
   }else{
    const output=targets[c].decodeOwned(readers[c],{from,to:Math.min(group.rows,from+batch)},dictionary[c]);
    decodedBytes+=output.bytes;vectors++;if(output.vector.form==="dictionary")dictionaryVectors++;
    for(const buffer of output.buffers){checksum+=new Uint8Array(buffer)[0]??0;sent?.push(buffer);}
    memory.give(output.bytes);
   }
  }
  if(pool){
   const elapsed=performance.now()-bs,p=process.cpuUsage(bp),t=process.threadCpuUsage(bt);
   wallMs+=elapsed;cpuMs+=(p.user+p.system)/1000;threadCpuMs+=(t.user+t.system)/1000;
   const h=performance.now(),ids=pool.issue(frame,sent),received=structuredClone(sent,{transfer:sent});
   const back=structuredClone(received,{transfer:received});
   pool.release(frame++,back.map((buffer,i)=>({id:ids[i],buffer})));handoverMs+=performance.now()-h;
  }
 }
 if(!pool){const p=process.cpuUsage(before),t=process.threadCpuUsage(threadBefore);wallMs=performance.now()-start;cpuMs=(p.user+p.system)/1000;threadCpuMs=(t.user+t.system)/1000;}
 if(charged!==targets.reduce((n,t)=>n+t.chargedBytes,0))throw new Error("Materialization charge leak");
 if(pool&&pool.hits===hits)throw new Error("No output buffer was reused");
 return {variant,batch,plainPages,decodedBytes,vectors,dictionaryVectors,checksum,cpuMs,threadCpuMs,wallMs,
  returned,returnHits:(pool?.hits??0)-hits,returnAccepted:(pool?.accepted??0)-accepted,returnPoolBytes:pool?.evictableBytes??0,handoverMs};
}
run();if(typeof global.gc!=="function")throw new Error("expose GC");global.gc();
const profilePath=process.argv[6], session=new Session();
const call=(method,params={})=>new Promise((resolve,reject)=>session.post(method,params,(error,result)=>error?reject(error):resolve(result)));
if(profilePath){session.connect();await call("Profiler.enable");await call("Profiler.setSamplingInterval",{interval:500});await call("Profiler.start");}
const measured=run();
if(profilePath){const result=await call("Profiler.stop");writeFileSync(profilePath,JSON.stringify(result.profile));session.disconnect();}
console.log(JSON.stringify({...measured,profiled:Boolean(profilePath)}));
for(const target of targets)target.close();
for(const reader of readers)reader.close();
pool?.close();if(charged!==0||governor.usedBytes!==0)throw new Error("Closing charge leak");
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
        timing: returned
          ? "decodeOwned plus charged ReturnPool.take/clear; two real clone transfers and issue/release outside per-batch timers"
          : "resident decode and fresh output allocation; complete loop timer",
        dictionaryPolicy: legacyDictionaries
          ? "historical first-23-only diagnostic"
          : "allow source dictionary codes per range; excludes ResultSink stream cap",
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
              profileDir
                ? join(profileDir, `materialize-${String(round)}-${variant}.cpuprofile`)
                : "",
              legacyDictionaries ? "legacy" : "source",
              returned ? "returned" : "fresh",
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
