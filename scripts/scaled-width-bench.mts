/** Dense scaled-float decode: shared typed-array IC vs width-specialized loops.
 * Five interleaved fresh-process rounds; full existing decode function, bit checks.
 * npx vite-node scripts/scaled-width-bench.mts
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";

const scratch = mkdtempSync(join(homedir(), ".cache/minnow-scaled-width-"));
const anchor =
  "else for (let k = 0; k < count; k++) out[at + k] = (base + (deltas[from + k] ?? 0)) / power;";
export function specializeScaledWidth(source: string): string {
  if (!source.includes(anchor)) throw new Error("Scaled decode anchor moved");
  let changed = source.replace(
    anchor,
    "else scaledWidth(deltas, from, count, base, power, out, at);",
  );
  changed += `\nfunction scaledWidth(d,from,count,base,power,out,at) {
   if(d instanceof Uint8Array) scaled8(d,from,count,base,power,out,at);
   else if(d instanceof Uint16Array) scaled16(d,from,count,base,power,out,at);
   else scaled32(d,from,count,base,power,out,at);
  }\n`;
  for (const width of [8, 16, 32])
    changed += `function scaled${width}(d,from,count,base,power,out,at) {for(let k=0;k<count;k++)out[at+k]=(base+(d[from+k]??0))/power;}\n`;
  return changed;
}
const worker = `
import { scaledInto } from "./packages/core/src/next/page/decode.ts";
const results=[];
for(const patched of [false,true]) {
 const views=[Uint8Array,Uint16Array,Uint32Array].map((Type,i)=>({aux:2+i,baseNumber:-10000,unsigned:Type.from({length:2048},(_,k)=>(k*131+29)>>>0), patches:patched?new Uint32Array([137,999,1150]):null,patchBytes:patched?new Uint8Array(new Uint32Array([0x12345678,0x7ff00000,0,0x80000000,0x87654321,0xfff80000]).buffer):null}));
 const out=new Float64Array(1100),words=new Uint32Array(out.buffer);
 let sink=0;
 function run(){for(let k=0;k<120000;k++){scaledInto(views[k%3],null,137,1161,0,out,3);sink^=words[3+(k%1024)];}}
 run();const before=process.cpuUsage(),start=performance.now();run();
 const wallMs=performance.now()-start,cpu=process.cpuUsage(before);
 const bits=views.map(v=>{scaledInto(v,null,137,1161,0,out,3);return Buffer.from(out.buffer).toString("base64");});
 results.push({patched,cpuMs:(cpu.user+cpu.system)/1000,wallMs,bits,sink,values:120000*1024});
}
console.log(JSON.stringify(results));
`;
try {
  for (const variant of ["shared", "specialized"])
    await build({
      stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: join(scratch, variant + ".mjs"),
      plugins: [
        {
          name: "scaled-width",
          setup(b) {
            b.onLoad({ filter: /[/\\]page[/\\]decode\.ts$/ }, ({ path }) => {
              let contents = readFileSync(path, "utf8");
              if (variant === "specialized") contents = specializeScaledWidth(contents);
              return { contents: contents + "\nexport {scaledInto};", loader: "ts" };
            });
          },
        },
      ],
    });
  const samples: Array<{
    round: number;
    variant: string;
    result: Array<{
      patched: boolean;
      cpuMs: number;
      wallMs: number;
      bits: string[];
      sink: number;
    }>;
  }> = [];
  let expected: string | undefined;
  for (let round = 0; round < 5; round++)
    for (const variant of round % 2 === 0 ? ["shared", "specialized"] : ["specialized", "shared"]) {
      const result = JSON.parse(
        execFileSync(process.execPath, [join(scratch, variant + ".mjs")], {
          encoding: "utf8",
          timeout: 120000,
        }),
      ) as (typeof samples)[number]["result"];
      const check = JSON.stringify(result.map((r) => [r.bits, r.sink]));
      expected ??= check;
      if (check !== expected) throw new Error("Decode bits differ");
      samples.push({ round, variant, result });
      console.log(
        JSON.stringify({
          round,
          variant,
          result: result.map(({ bits, ...metrics }) => ({
            ...metrics,
            bitCheckedBytes: bits.reduce((n, b) => n + Buffer.from(b, "base64").length, 0),
          })),
        }),
      );
    }
  const summary = ["shared", "specialized"].flatMap((variant) =>
    [false, true].map((patched) => {
      const rows = samples
        .filter((s) => s.variant === variant)
        .map((s) => s.result.find((r) => r.patched === patched));
      const stats = (key: "cpuMs" | "wallMs") => {
        const a = rows
          .map((r) => {
            if (!r) throw new Error("Missing sample");
            return r[key];
          })
          .sort((a, b) => a - b);
        return { median: a[2], min: a[0], max: a[4] };
      };
      return { variant, patched, cpuMs: stats("cpuMs"), wallMs: stats("wallMs") };
    }),
  );
  console.log(JSON.stringify({ rounds: 5, summary }));
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
