/** Actual page-export loop at one/two retained groups, five interleaved CPU/wall rounds.
 * One wide group generated outside timing, repeated eight times, real PageCache over memory.
 * This isolates decoding, read planning, pooling and retention, not OPFS stalls.
 * npx vite-node scripts/readahead-bench.mts
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";
import { serialize } from "node:v8";
import { MemoryExtentSpace } from "../packages/core/src/next/page/memory-extents.js";
import { GroupWriter } from "../packages/core/src/next/page/writer.js";
import { wideSpec } from "./browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.js";

const scratch = mkdtempSync(join(homedir(), ".cache/minnow-readahead-"));
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
  const made = await group.finish();
  writeFileSync(join(scratch, "data"), space.bytes.subarray(0, space.allocator.end));
  writeFileSync(join(scratch, "group.bin"), serialize({ rows: 65536, chunks: made.columns }));
  const worker = `
import {readFileSync} from "node:fs";
import {deserialize} from "node:v8";
import {PageCache} from "./packages/core/src/next/storage/page-cache.ts";
import {countingBudget} from "./packages/core/src/next/storage/space-test-support.ts";
import {BlobCache} from "./packages/core/src/next/format/blob-cache.ts";
import {exportStream,rebaseExtents} from "./scripts/browser-perf/page/export-stream.ts";
const dir=process.argv[2],group=deserialize(readFileSync(dir+"/group.bin")),data=readFileSync(dir+"/data");
let charged=0,peak=0;
const memory={charge(n){charged+=n;peak=Math.max(peak,charged);return true;},release(n){charged-=n;}};
const cache=new PageCache({readDataInto(at,out){let done=0;while(done<out.length){const from=(at+done)%data.length,n=Math.min(out.length-done,data.length-from);out.set(data.subarray(from,from+n),done);done+=n;}return Promise.resolve();}});cache.attach(memory);
const pages={read:cache.read.bind(cache),peek:cache.peek.bind(cache),parsed:cache.parsed.bind(cache),attach:cache.attachParsed.bind(cache)};
const budget=countingBudget(Infinity),directories=new BlobCache(()=>memory);
const groups=Array.from({length:8},(_,i)=>({rows:group.rows,chunks:rebaseExtents(group.chunks,new Map(group.chunks.map(c=>[c.data.extent.offset,{...c.data.extent,offset:c.data.extent.offset+i*data.length}])))})),depth=Number(process.argv[3]);
async function run(){const result=await exportStream(pages,groups,{budget,directories,once:true,readahead:262144,windowBytes:64*1048576,groupDepth:depth});if(result.rows!==8*65536||budget.used!==0)throw new Error("Rows or lease charge mismatch");return result;}
await run();const before=process.cpuUsage(),start=performance.now();const result=await run();const wallMs=performance.now()-start,cpu=process.cpuUsage(before);
const poolBytes=cache.stats.poolBytes;cache.close();if(charged!==0)throw new Error("Leaked cache");
console.log(JSON.stringify({depth,cpuMs:(cpu.user+cpu.system)/1000,wallMs,rows:result.rows,loads:result.loads,poolBytes,peak}));
`;
  await build({
    stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: join(scratch, "worker.mjs"),
  });
  const samples: Array<Record<string, number>> = [];
  for (let round = 0; round < 5; round++)
    for (const depth of round % 2 === 0 ? [1, 2] : [2, 1]) {
      const result = JSON.parse(
        execFileSync(process.execPath, [join(scratch, "worker.mjs"), scratch, String(depth)], {
          encoding: "utf8",
          timeout: 120000,
        }),
      ) as Record<string, number>;
      samples.push(result);
      console.log(JSON.stringify({ round, ...result }));
    }
  const summary = [1, 2].map((depth) => {
    const rows = samples.filter((r) => r.depth === depth);
    const stats = (k: string) => {
      const v = rows
        .map((r) => {
          const n = r[k];
          if (n === undefined) throw new Error("Missing sample");
          return n;
        })
        .sort((a, b) => a - b);
      return { median: v[2], min: v[0], max: v[4] };
    };
    return { depth, cpuMs: stats("cpuMs"), wallMs: stats("wallMs"), peak: stats("peak") };
  });
  console.log(JSON.stringify({ rounds: 5, summary }));
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
