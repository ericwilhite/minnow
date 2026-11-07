/** TableScan component CPU: reuse decode shape/header setup across sizing, reservation, decode.
 * Real 200-column encoded fixture, four groups, real cache/targets/scan; no frames or IO.
 * Five interleaved fresh processes. Setup, one warmup, row/checksum validation outside timing.
 * --planning isolates sizing/reservation over real loaded pages; default runs the whole scan.
 * npx vite-node scripts/scan-shape-bench.mts
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { loadavg, tmpdir } from "node:os";
import { join } from "node:path";
import { serialize } from "node:v8";
import { build } from "esbuild";
import { MemoryExtentSpace } from "../packages/core/src/next/page/memory-extents.js";
import { GroupWriter } from "../packages/core/src/next/page/writer.js";
import { wideSpec } from "./browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.js";

const PLANNING = process.argv.includes("--planning");
const REFERENCE = "74c79c9b";
const files = ["exec/table-scan", "exec/column-target", "page/reader"];
const sources = new Map(
  files.map((file) => [
    file + ".ts",
    {
      candidate: readFileSync("packages/core/src/next/" + file + ".ts", "utf8"),
      baseline: execFileSync(
        "git",
        ["show", REFERENCE + ":packages/core/src/next/" + file + ".ts"],
        { encoding: "utf8" },
      ),
    },
  ]),
);
const scratch = mkdtempSync(join(tmpdir(), "minnow-scan-shape-"));
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
  const planningWorker = `
import {readFileSync} from "node:fs";
import {deserialize} from "node:v8";
import {ColumnTarget} from "./packages/core/src/next/exec/column-target.ts";
import {ChunkReader} from "./packages/core/src/next/page/reader.ts";
import {MemoryExtentSpace} from "./packages/core/src/next/page/memory-extents.ts";
import {countingBudget} from "./packages/core/src/next/storage/space-test-support.ts";
const dir=process.argv[2],group=deserialize(readFileSync(dir+"/group")),data=readFileSync(dir+"/data");
const space=new MemoryExtentSpace();space.bytes=new Uint8Array(data);space.allocator.end=data.length;
const budget=countingBudget(Infinity),readers=[],targets=[],sizers=[];
let allocated=0;
const memory={take(n){allocated+=n},give(n){allocated-=n}};
for(const chunk of group.chunks){const r=await ChunkReader.open(space,chunk,{budget});await r.load(r.directory.pages.map((_,p)=>p));const t=new ColumnTarget(chunk.type,2048,memory);readers.push(r);targets.push(t);sizers.push(typeof t.outputBytes==="function"?(spec)=>t.outputBytes(r,spec,false,true):(spec)=>ColumnTarget.outputBytes(r,chunk.type,spec,false,true));}
function run(){let sum=0;for(let pass=0;pass<3000;pass++)for(let col=0;col<readers.length;col++){const spec={from:(pass*786)%64000,to:Math.min(65536,(pass*786)%64000+786)},r=readers[col],t=targets[col];sum+=sizers[col](spec);sum+=t.growthFor(r,spec);sum+=sizers[col](spec);}return sum;}
const warm=run(),before=process.cpuUsage(),start=performance.now(),sum=run(),wallMs=performance.now()-start,cpu=process.cpuUsage(before);
if(sum!==warm)throw new Error("Checksum changed");for(const t of targets)t.close();for(const r of readers)r.close();if(budget.used!==0||allocated!==0)throw new Error("Leaked planning charge");
console.log(JSON.stringify({cpuMs:(cpu.user+cpu.system)/1000,wallMs,sum,batches:3000*200,rows:0}));
`;
  const scanWorker = `
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
  for (const name of ["baseline", "candidate"] as const)
    await build({
      stdin: {
        contents: PLANNING ? planningWorker : scanWorker,
        resolveDir: process.cwd(),
        loader: "ts",
      },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: join(scratch, name + ".mjs"),
      plugins: [
        {
          name: "shape-variant",
          setup(b) {
            b.onLoad(
              { filter: /\/(exec\/(table-scan|column-target)|page\/reader)\.ts$/ },
              (args) => {
                const file = args.path.split("/next/")[1];
                const source = file === undefined ? undefined : sources.get(file);
                if (!source) throw new Error("Unrecognized variant module: " + args.path);
                return { contents: source[name], loader: "ts" };
              },
            );
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
      const loadBefore = loadavg();
      const row = {
        round,
        loadBefore,
        variant,
        ...(JSON.parse(
          execFileSync(process.execPath, [join(scratch, variant + ".mjs"), scratch], {
            encoding: "utf8",
            timeout: 300000,
          }),
        ) as { cpuMs: number; wallMs: number; sum: number; batches: number }),
      };
      Object.assign(row, { loadAfter: loadavg() });
      rows.push(row);
      console.log(JSON.stringify(row));
    }
  if (new Set(rows.map((r) => r.sum + ":" + r.batches)).size !== 1)
    throw new Error("Variants differ");
  console.log(
    JSON.stringify({
      rounds: 5,
      component: PLANNING ? "descriptor planning only" : "TableScan without IO or frames",
      baseline: REFERENCE,
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
