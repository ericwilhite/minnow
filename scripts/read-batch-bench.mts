/** Full one-pass PageCache read/verify/release CPU kernel; no OPFS latency claim.
 * npx vite-node scripts/read-batch-bench.mts
 * Five interleaved fresh-process rounds. Larger-call candidate remains bench-only.
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";

const scratch = mkdtempSync(join(homedir(), ".cache/minnow-read-batch-"));
const source = `
import { PageCache } from "./packages/core/src/next/storage/page-cache.ts";
import { xxHash32 } from "./packages/core/src/next/hash/xxhash32.ts";
import { MAX_READ_BYTES } from "./packages/core/src/next/storage/extents.ts";
import { countingBudget } from "./packages/core/src/next/storage/space-test-support.ts";
const input = new Uint8Array(16*1024*1024+256).map((_, k)=>k&255);
const results=[];
for (const gap of [0,32768]) {
 const refs=Array.from({length:128},(_,i)=>({extent:{offset:0,length:32*1024*1024},offset:i*(65536+gap),length:65536,checksum:xxHash32(input.subarray(0,65536))}));
 let calls=0,cacheCharge=0;
 const cache=new PageCache({readDataInto(at,out){calls++;out.set(input.subarray(at&255,(at&255)+out.length));return Promise.resolve();}});
 cache.attach({charge(n){cacheCharge+=n;return true;},release(n){cacheCharge-=n;}});
 const budget=countingBudget(Infinity); let sink=0;
 async function run(){
  for(let n=0;n<128;n++) {
   const held=[];
   for(let at=0;at<refs.length;at+=MAX_READ_BYTES/65536) {
    const lease=await cache.read(refs.slice(at,at+MAX_READ_BYTES/65536),{budget,once:true,reuseBuffers:true});
    sink^=lease.bytes[lease.bytes.length-1][65535];held.push(lease);
   }
   for(const lease of held)lease.release();
   if(budget.used!==0)throw new Error("leaked lease budget");
  }
 }
 await run();calls=0;
 const cpu=process.cpuUsage(),start=performance.now();await run();
 const wallMs=performance.now()-start,used=process.cpuUsage(cpu);
 const cpuMs=(used.user+used.system)/1000;
 const poolBytes=cache.stats.poolBytes;cache.close();
 if(cacheCharge!==0||budget.used!==0)throw new Error("leaked cache charge");
 results.push({gap,wallMs,cpuMs,calls,poolBytes,sink,logicalBytes:128*128*65536});
}
console.log(JSON.stringify(results));
`;
const samples: Array<{ round: number; readMiB: number; result: Array<Record<string, number>> }> =
  [];
try {
  for (const readMiB of [4, 8])
    await build({
      stdin: { contents: source, resolveDir: process.cwd(), loader: "ts" },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: join(scratch, `${readMiB}.mjs`),
      plugins: [
        {
          name: "read-limit",
          setup(b) {
            b.onLoad({ filter: /[/\\]storage[/\\]extents\.ts$/ }, ({ path }) => {
              const old = readFileSync(path, "utf8");
              const contents = old.replace(
                "export const MAX_READ_BYTES = 4 * MAX_SLAB;",
                `export const MAX_READ_BYTES = ${readMiB} * MAX_SLAB;`,
              );
              if (readMiB !== 4 && contents === old) throw new Error("Read limit anchor moved");
              return { contents, loader: "ts" };
            });
          },
        },
      ],
    });
  for (let round = 0; round < 5; round++)
    for (const readMiB of round % 2 === 0 ? [4, 8] : [8, 4]) {
      const result = JSON.parse(
        execFileSync(process.execPath, [join(scratch, `${readMiB}.mjs`)], {
          encoding: "utf8",
          timeout: 120000,
        }),
      ) as Array<Record<string, number>>;
      samples.push({ round, readMiB, result });
      console.log(JSON.stringify({ round, readMiB, result }));
    }
  const summary = [4, 8].flatMap((readMiB) =>
    [0, 32768].map((gap) => {
      const rows = samples
        .filter((s) => s.readMiB === readMiB)
        .map((s) => s.result.find((r) => r.gap === gap));
      const stats = (key: string) => {
        const v = rows
          .map((r) => {
            const n = r?.[key];
            if (n === undefined) throw new Error("missing sample");
            return n;
          })
          .sort((a, b) => a - b);
        return { median: v[2], min: v[0], max: v[4] };
      };
      return {
        readMiB,
        gap,
        cpuMs: stats("cpuMs"),
        wallMs: stats("wallMs"),
        calls: rows[0]?.calls,
        poolBytes: rows[0]?.poolBytes,
      };
    }),
  );
  console.log(JSON.stringify({ rounds: 5, summary }));
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
