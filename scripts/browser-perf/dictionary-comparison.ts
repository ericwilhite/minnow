/** BENCH only: select the old/new sizing kernel in one worker; production has no selector. */
import { execFileSync } from "node:child_process";
import type { Plugin } from "vite";
export function dictionaryComparison(): Plugin {
  const old = execFileSync("git", ["show", "53ec3457:packages/core/src/next/page/decode.ts"], {
    encoding: "utf8",
  });
  const first = old.indexOf("function codeBytes8("),
    end = old.indexOf("function codeBytes16(", first);
  if (first < 0 || end < first) throw new Error("Dictionary kernel anchor moved");
  const baseline = old
    .slice(first, end)
    .replace("function codeBytes8(", "function baselineCodeBytes8(");
  return {
    name: "dictionary-size-comparison",
    enforce: "pre",
    transform(source, id) {
      if (!id.endsWith("/next/page/decode.ts")) return;
      if (!source.includes("function codeBytes8(")) throw new Error("Dictionary kernel missing");
      return (
        source.replace("function codeBytes8(", "function candidateCodeBytes8(") +
        `\n${baseline}\nlet codeBytes8=candidateCodeBytes8;
(globalThis as typeof globalThis & {__minnowDictionarySizing?:(variant:string)=>void}).__minnowDictionarySizing=(variant)=>{codeBytes8=variant==="baseline"?baselineCodeBytes8:candidateCodeBytes8;};\n`
      );
    },
  };
}
