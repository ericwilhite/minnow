/** TableScan component CPU, one lever: contiguous selected prefixes use range APIs.
 * Real 200-column encoded fixture, four groups, real cache/targets/scan; no frames or IO.
 * Five interleaved fresh processes. Setup, one warmup, row/checksum validation outside timing.
 * npx vite-node scripts/scan-range-bench.mts
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { serialize } from "node:v8";
import { build } from "esbuild";
import { MemoryExtentSpace } from "../packages/core/src/next/page/memory-extents.js";
import { GroupWriter } from "../packages/core/src/next/page/writer.js";
import { wideSpec } from "./browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.js";

const candidate = readFileSync("packages/core/src/next/exec/table-scan.ts", "utf8");
const anchor =
  /function specForRows\(rows: Uint16Array, count: number\): DecodeSpec \{[\s\S]*?\n\}/u;
if (!anchor.test(candidate)) throw new Error("Range candidate anchor moved");
// Parent gather policy, with the same constant-size dispatch call as the candidate.
const baseline = candidate.replace(
  anchor,
  "function specForRows(rows: Uint16Array, count: number): DecodeSpec { return { rows, count }; }",
);
const scratch = mkdtempSync(join(homedir(), ".cache/minnow-scan-range-"));
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
  writeFileSync(join(scratch, "group"), serialize({ rows: 65536, chunks: made.columns }));
  const worker = `
import {readFileSync} from "node:fs";
import {deserialize} from "node:v8";
import {PageCache} from "./packages/core/src/next/storage/page-cache.ts";
import {ChunkReader} from "./packages/core/src/next/page/reader.ts";
import {TableScan} from "./packages/core/src/next/exec/table-scan.ts";
import {OutputArena} from "./packages/core/src/next/exec/kernels/vectors.ts";
import {countingBudget} from "./packages/core/src/next/storage/space-test-support.ts";
import {rebaseExtents} from "./scripts/browser-perf/page/export-stream.ts";
const dir=process.argv[2],group=deserialize(readFileSync(dir+"/group")),data=readFileSync(dir+"/data");
let charged=0,peak=0;
const memory={charge(n){charged+=n;peak=Math.max(peak,charged);return true;},release(n){charged-=n;}};
const cache=new PageCache({readDataInto(at,out){let done=0;while(done<out.length){const from=(at+done)%data.length,n=Math.min(out.length-done,data.length-from);out.set(data.subarray(from,from+n),done);done+=n;}return Promise.resolve();}});cache.attach(memory);
const pages={read:cache.read.bind(cache),peek:cache.peek.bind(cache),parsed:cache.parsed.bind(cache),attach:cache.attachParsed.bind(cache)};
const budget=countingBudget(Infinity),arena=new OutputArena(budget,2048);
const columns=group.chunks.map(c=>({id:c.columnId,name:"c"+c.columnId,type:c.type,declared:null,nullable:true,missing:null}));
const segments=Array.from({length:4},(_,index)=>{const chunks=rebaseExtents(group.chunks,new Map(group.chunks.map(c=>[c.data.extent.offset,{...c.data.extent,offset:c.data.extent.offset+index*data.length}])));return {kind:"group",index,rowCount:group.rows,liveRowCount:group.rows,staged:false,root:()=>({rowCount:group.rows,rowIds:{kind:"dense"},columns:chunks}),chunk:id=>chunks.find(c=>c.columnId===id)??null,deletions:()=>null,statistics:()=>null};});
async function run(){const scan=new TableScan({table:{tableId:1,schemaVersion:1,columns,segments,liveRowCount:4*group.rows},pages,reads:{open:(p,c,o)=>ChunkReader.open(p,c,o)},budget,columns:columns.map(c=>c.id),filter:null,emitLocator:false,context:{arena,params:{slots:[]},input:[]}});let rows=0,sum=0,batches=0;try{for(let item=await scan.next();item!==null;item=await scan.next()){const b=item.batch,d=b.columns[0].data;if(d.layout!=="int64"||d.values.repr!=="safe"||b.selection!==null)throw new Error("Fixture key layout");sum+=d.values.values[0]+d.values.values[b.length-1];rows+=b.length;batches++;}}finally{scan.close();}if(rows!==4*group.rows||budget.used!==0)throw new Error("Rows/lease balance");return{rows,sum,batches,stats:scan.stats};}
const warm=await run(),before=process.cpuUsage(),start=performance.now(),result=await run(),wallMs=performance.now()-start,cpu=process.cpuUsage(before);
if(result.sum!==warm.sum||result.batches!==warm.batches)throw new Error("Warm result changed");
arena.close();cache.close();if(charged!==0||budget.used!==0)throw new Error("Leaked charge");
console.log(JSON.stringify({cpuMs:(cpu.user+cpu.system)/1000,wallMs,peak,...result}));
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
          name: "range-variant",
          setup(b) {
            b.onLoad({ filter: /\/exec\/table-scan\.ts$/ }, () => ({
              contents: source,
              loader: "ts",
            }));
          },
        },
      ],
    });
  const rows: Array<{
    variant: string;
    round: number;
    cpuMs: number;
    wallMs: number;
    sum: number;
    batches: number;
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
        ) as { cpuMs: number; wallMs: number; sum: number; batches: number }),
      };
      rows.push(row);
      console.log(JSON.stringify(row));
    }
  if (new Set(rows.map((r) => r.sum + ":" + r.batches)).size !== 1)
    throw new Error("Variants differ");
  console.log(
    JSON.stringify({
      rounds: 5,
      baseline: "1a32d1f0",
      summary: ["baseline", "candidate"].map((variant) => {
        const stats = (key: "cpuMs" | "wallMs") => {
          const xs = rows
            .filter((r) => r.variant === variant)
            .map((r) => r[key])
            .sort((a, b) => a - b);
          return { median: xs[2], min: xs[0], max: xs[4] };
        };
        return { variant, cpuMs: stats("cpuMs"), wallMs: stats("wallMs") };
      }),
    }),
  );
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
