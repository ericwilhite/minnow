/**
 * Candidate for the checksum CPU seen in OPFS profiles: aligned native words vs DataView.
 * Five interleaved rounds, fresh processes; CPU/wall ns/byte, medians and min/max.
 * An isolated full XXH32 call, with range/seed checks and consumed/compared results; no I/O.
 * The candidate remains in the harness until it clears the performance rule.
 *   npx vite-node scripts/hash-load-bench.mts
 */
import { execFileSync } from "node:child_process";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";

const rounds = 5;
const scratch = mkdtempSync(join(tmpdir(), "minnow-hash-load-"));
const variants = ["dataview", "aligned-words", "aligned-signed"] as const;
interface Sample {
  size: number;
  alignment: number;
  cpuSeconds: number;
  cpuNsPerByte: number;
  wallNsPerByte: number;
  checksum: number;
}
const samples: Record<(typeof variants)[number], Sample[][]> = {
  dataview: [],
  "aligned-words": [],
  "aligned-signed": [],
};
const worker = `
import { xxHash32 } from "./packages/core/src/next/hash/xxhash32.ts";
const raw = new Uint8Array(4 * 1024 * 1024 + 16);
for (let k=0;k<raw.length;k++) raw[k]=(k*131+(k>>>9))&255;
const input = new Uint8Array(raw.buffer,8,4*1024*1024);
const results=[];
for (const size of [4096,8195,65536,1048576]) for (const alignment of [0,1]) {
 const calls=Math.ceil(128*1024*1024/size);
 const stride=Math.ceil(size/4)*4+16;
 function run() {
  let checksum=0, at=alignment;
  for(let k=0;k<calls;k++) {
   checksum^=xxHash32(input,k*7919,at,at+size);
   at+=stride;
   if(at+size>input.length)at=alignment;
  }
  return checksum;
 }
 run();
 const before=process.cpuUsage(); const start=performance.now();
 const checksum=run();
 const wallMs=performance.now()-start;const used=process.cpuUsage(before);
 const cpuSeconds=(used.user+used.system)/1e6;
 results.push({size,alignment,cpuSeconds,cpuNsPerByte:cpuSeconds*1e9/(size*calls),wallNsPerByte:wallMs*1e6/(size*calls),checksum});
}
console.log(JSON.stringify(results));
`;
try {
  for (const variant of variants)
    await build({
      stdin: {
        contents: worker,
        resolveDir: process.cwd(),
        sourcefile: "hash-load-worker.mts",
        loader: "ts",
      },
      outfile: join(scratch, `${variant}.mjs`),
      bundle: true,
      platform: "node",
      format: "esm",
      target: "node24",
      plugins: [
        {
          name: "hash-load",
          setup(builder) {
            builder.onLoad({ filter: /[/\\]hash[/\\]xxhash32\.ts$/ }, ({ path }) => {
              let contents = readFileSync(path, "utf8");
              if (variant !== "dataview") {
                const a = contents.indexOf("      const view = new DataView");
                const b =
                  contents.indexOf("      index = start + at;", a) +
                  "      index = start + at;".length;
                if (a < 0 || b < a) throw new Error("Hash candidate anchor moved");
                const baseline = contents.slice(a, b);
                const candidate = `
      if (NATIVE_LITTLE_ENDIAN && ((bytes.byteOffset + start) & 3) === 0) {
        const words = new ${variant === "aligned-signed" ? "Int32Array" : "Uint32Array"}(bytes.buffer, bytes.byteOffset + start, Math.floor(length / 4));
        const stripes = ${variant === "aligned-signed" ? "words.length - 4" : "Math.floor((length - 16) / 4)"};
        let at = 0;
        for (; at <= stripes; at += 4) {
          v1 = round(v1, words[at] ?? 0);
          v2 = round(v2, words[at+1] ?? 0);
          v3 = round(v3, words[at+2] ?? 0);
          v4 = round(v4, words[at+3] ?? 0);
        }
        index = start + at * 4;
      } else { ${baseline} }
`;
                contents = contents.slice(0, a) + candidate + contents.slice(b);
                contents = contents.replace(
                  "const PRIME1",
                  "const NATIVE_LITTLE_ENDIAN = new Uint8Array(new Uint32Array([1]).buffer)[0] === 1;\nconst PRIME1",
                );
              }
              return { contents, loader: "ts" };
            });
          },
        },
      ],
    });
  let expected: number[] | undefined;
  for (let round = 0; round < rounds; round++)
    for (let offset = 0; offset < variants.length; offset++) {
      const variant = variants[(round + offset) % variants.length];
      if (!variant) throw new Error("no variant");
      const result = JSON.parse(
        execFileSync(process.execPath, [join(scratch, `${variant}.mjs`)], {
          encoding: "utf8",
          timeout: 120000,
        }),
      ) as Sample[];
      const checks = result.map((r) => r.checksum);
      expected ??= checks;
      if (JSON.stringify(checks) !== JSON.stringify(expected))
        throw new Error("Hash results differ");
      samples[variant].push(result);
      console.log(JSON.stringify({ round, variant, result }));
    }
  const summary = variants.flatMap((variant) =>
    [4096, 8195, 65536, 1048576].flatMap((size) =>
      [0, 1].map((alignment) => {
        const rows = samples[variant].map((round) =>
          round.find((r) => r.size === size && r.alignment === alignment),
        );
        const metric = (key: "cpuNsPerByte" | "wallNsPerByte") => {
          const values = rows
            .map((r) => {
              if (!r) throw new Error("no sample");
              return r[key];
            })
            .sort((a, b) => a - b);
          return { median: values[2], min: values[0], max: values.at(-1) };
        };
        return {
          variant,
          size,
          alignment,
          cpu: metric("cpuNsPerByte"),
          wall: metric("wallNsPerByte"),
        };
      }),
    ),
  );
  console.log(JSON.stringify({ rounds, summary }));
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
