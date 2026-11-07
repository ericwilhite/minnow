/** Isolated production ResultSink -> ResultChannel -> MessagePort cost. Setup, warmup and GC
 * excluded. Five interleaved fresh-process rounds; immutable reference dependencies from Git.
 * npx vite-node scripts/result-sink-bench.mts --compare=REV
 * This measures framing of 200 columns (40 dictionary text, 160 fixed-width), not storage/OPFS.
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { homedir, loadavg } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";
import { executorSnapshot } from "./browser-perf/executor-comparison.js";

const revision = process.argv.find((a) => a.startsWith("--compare="))?.slice(10);
if (!revision) throw new Error("--compare=REV is required");
const scratch = mkdtempSync(join(homedir(), ".cache", "minnow-result-sink-"));
const worker = `
import {ResultSink} from "./packages/core/src/next/exec/result-sink.ts";
import {ResultChannel} from "./packages/core/src/next/exec/result.ts";
import {Governor} from "./packages/core/src/next/memory/governor.ts";
const n=2048, repeats=64, types=Array.from({length:200},(_,c)=>c<40?{kind:"text"}:{kind:"int32",bits:32});
function fixture(){return types.map((type,c)=>{
 if(c>=40)return {form:"flat",type,length:n,validity:null,data:{layout:"i32",values:Int32Array.from({length:n},(_,r)=>r+c)}};
 const offsets=Int32Array.from({length:65},(_,r)=>r*48),bytes=new Uint8Array(64*48);for(let r=0;r<64;r++)bytes.fill(65+r%26,r*48,(r+1)*48);
 return {form:"dictionary",type,length:n,validity:null,codes:Int32Array.from({length:n},(_,r)=>r%64),dictionaryId:c+1,dictionary:{form:"flat",type,length:64,validity:null,data:{layout:"varlen",offsets,bytes}}};
});}
async function run(columns){
 const governor=new Governor(64*1048576),grant=await governor.admit({label:"result",minimumBytes:4*1048576,spillable:true});
 const channel=new ResultChannel(types.map((type,c)=>({type,name:String(c),nullable:true})),{windowBytes:4*1048576});
 const sink=new ResultSink(types,grant,channel,0);const ports=new MessageChannel();let received=0,wireBytes=0,frames=0;
 ports.port2.onmessage=event=>{const {payload}=event.data;received+=payload.rowCount;ports.port2.postMessage(payload.sequence);};ports.port2.start();
 const outstanding=new Map();ports.port1.onmessage=event=>{const frame=outstanding.get(event.data);if(!frame)throw new Error("credit");outstanding.delete(event.data);frame.release();};ports.port1.start();
 const processBefore=process.cpuUsage(),threadBefore=process.threadCpuUsage(),start=performance.now();
 const producer=(async()=>{for(let k=0;k<repeats;k++)if(sink.push({length:n,columns,selection:null})==="blocked")await sink.ready();await sink.finish();})();
 try{for(;;){const frame=await channel.next();if(!frame)break;outstanding.set(frame.payload.sequence,frame);frames++;wireBytes+=frame.transfer.reduce((s,b)=>s+b.byteLength,0);ports.port1.postMessage({payload:frame.payload},[...frame.transfer]);}await producer;await channel.delivered;
 const wallMs=performance.now()-start,cpu=process.cpuUsage(processBefore),thread=process.threadCpuUsage(threadBefore);
 if(received!==n*repeats||grant.allocated!==0)throw new Error("rows/charges");
 return {rows:received,frames,wireBytes,wallMs,cpuMs:(cpu.user+cpu.system)/1000,threadCpuMs:(thread.user+thread.system)/1000};
 }finally{ports.port1.close();ports.port2.close();grant.close();}
}
await run(fixture());const input=fixture();globalThis.gc?.();console.log(JSON.stringify(await run(input)));
`;
try {
  for (const baseline of [false, true])
    await build({
      stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: join(scratch, baseline ? "baseline.mjs" : "candidate.mjs"),
      plugins: baseline ? [executorSnapshot(revision).esbuild] : [],
    });
  const results: Array<{ variant: string; cpuMs: number; threadCpuMs: number; wallMs: number }> =
    [];
  for (let round = 0; round < 5; round++)
    for (const variant of round % 2 === 0 ? ["baseline", "candidate"] : ["candidate", "baseline"]) {
      const row = {
        round,
        variant,
        hostLoad: loadavg(),
        ...(JSON.parse(
          execFileSync(process.execPath, ["--expose-gc", join(scratch, variant + ".mjs")], {
            encoding: "utf8",
            env: process.env,
          }),
        ) as { cpuMs: number; threadCpuMs: number; wallMs: number }),
      };
      results.push(row);
      console.log(JSON.stringify(row));
    }
  for (const variant of ["baseline", "candidate"]) {
    const summary: Record<string, unknown> = { variant };
    for (const metric of ["cpuMs", "threadCpuMs", "wallMs"] as const) {
      const values = results
        .filter((r) => r.variant === variant)
        .map((r) => r[metric])
        .sort((a, b) => a - b);
      summary[metric] = { median: values[2], min: values[0], max: values[4] };
    }
    console.log(JSON.stringify(summary));
  }
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
